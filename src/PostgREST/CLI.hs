{-|
Module      : PostgREST.CLI
Description : Command line interface.
-}
{-# LANGUAGE LambdaCase     #-}
{-# LANGUAGE NamedFieldPuns #-}

module PostgREST.CLI (app) where

import qualified Data.Aeson                 as JSON
import qualified Data.ByteString.Lazy       as LBS
import qualified Data.Map.Strict            as M
import qualified Hasql.Pool                 as SQL (Pool, acquire,
                                                    release, use)
import qualified Hasql.Transaction          as SQL
import qualified Hasql.Transaction.Sessions as SQL

import Control.AutoUpdate       (defaultUpdateSettings, mkAutoUpdate,
                                 updateAction)
import Data.IORef               (IORef, atomicWriteIORef, newIORef,
                                 readIORef)
import Data.String              (IsString (..))
import Data.Text                (pack)
import Data.Text.IO             (hPutStrLn)
import Data.Time.Clock          (getCurrentTime)
import Network.Wai              (Application)
import Network.Wai.Handler.Warp (Settings, defaultSettings,
                                 runSettings, setHost, setPort,
                                 setServerName)
import Options.Applicative      (Parser, customExecParser, flag,
                                 footer, fullDesc, help, helper, info,
                                 infoOption, long, metavar, prefs,
                                 progDesc, short, showHelpOnEmpty,
                                 showHelpOnError, strArgument)
import System.CPUTime           (getCPUTime)
import System.Environment       (getEnvironment)
import System.IO                (BufferMode (..), hSetBuffering)
import System.Posix.Types       (FileMode)
import Text.Printf              (hPrintf)

import PostgREST.App         (postgrest)
import PostgREST.DbStructure (getDbStructure, getPgVersion)
import PostgREST.Statements  (dbSettingsStatement)

import PostgREST.Config

import qualified PostgREST.Connection as Connection

import Protolude      hiding (hPutStrLn, toS)
import Protolude.Conv (toS)

-- | Command line interface options
data Options =
  Options
    { command    :: Command
    , configPath :: Maybe FilePath
    }

data Command
  = CmdRun
  | CmdDumpConfig
  | CmdDumpSchema

-- | Read command line interface options. Also prints help.
readOptionsShowHelp :: Environment -> IO Options
readOptionsShowHelp env = customExecParser parserPrefs opts
  where
    parserPrefs = prefs $ showHelpOnError <> showHelpOnEmpty

    opts = info (helper <*> exampleParser <*> cliParser) $
             fullDesc
             <> progDesc (
                 "PostgREST "
                 <> toS prettyVersion
                 <> " / create a REST API to an existing Postgres database"
               )
             <> footer "To run PostgREST, please pass the FILENAME argument or set PGRST_ environment variables."

    cliParser :: Parser Options
    cliParser = Options <$>
      (
        flag CmdRun CmdDumpConfig (
          long "dump-config" <>
          help "Dump loaded configuration and exit"
        )
        <|>
        flag CmdRun CmdDumpSchema (
          long "dump-schema" <>
          help "Dump loaded schema as JSON and exit (for debugging, output structure is unstable)"
        )
      )
      <*>
      optionalWithEnvironment (strArgument (
        metavar "FILENAME" <>
        help "Path to configuration file (optional with PGRST_ environment variables)"
      ))

    optionalWithEnvironment :: Alternative f => f a -> f (Maybe a)
    optionalWithEnvironment v
      | M.null env = Just <$> v
      | otherwise  = optional v

    exampleParser :: Parser (a -> a)
    exampleParser =
      infoOption example (
        long "example" <>
        short 'e' <>
        help "Show an example configuration file"
      )

readEnvironment :: IO Environment
readEnvironment = getEnvironment <&> pgrst
  where
    pgrst env = M.filterWithKey (\k _ -> "PGRST_" `isPrefixOf` k) $ M.map pack $ M.fromList env

-- | Dump DbStructure schema to JSON
dumpSchema :: SQL.Pool -> AppConfig -> IO LBS.ByteString
dumpSchema pool conf = do
  result <-
    timeToStderr "Loaded schema in %.3f seconds" $
      SQL.use pool $ do
        pgVersion <- getPgVersion
        SQL.transaction SQL.ReadCommitted SQL.Read $
          getDbStructure
            (toList $ configDbSchemas conf)
            (configDbExtraSearchPath conf)
            pgVersion
            (configDbPreparedStatements conf)
  SQL.release pool
  case result of
    Left e -> do
      hPutStrLn stderr $ "An error ocurred when loading the schema cache:\n" <> show e
      exitFailure
    Right dbStructure -> return $ JSON.encode dbStructure

-- | Print the time taken to run an IO action to stderr with the given printf string
timeToStderr :: [Char] -> IO (Either a b) -> IO (Either a b)
timeToStderr fmtString a =
  do
    start <- getCPUTime
    result <- a
    end <- getCPUTime
    let
      duration :: Double
      duration = fromIntegral (end - start) / picoseconds
    when (isRight result) $
      hPrintf stderr (fmtString ++ "\n") duration
    return result


-- | 10^12 picoseconds per second
picoseconds :: Double
picoseconds = 1000000000000


type HandlerInstaller =
  ThreadId -> SQL.Pool -> IO () -> IO () -> IO ()

type SocketRunner =
  Settings -> Application -> FileMode -> FilePath -> IO ()

app :: Maybe HandlerInstaller -> Maybe SocketRunner -> IO ()
app installHandlers runWithSocket =
  do
    --
    -- LineBuffering: the entire output buffer is flushed whenever a newline is
    -- output, the buffer overflows, a hFlush is issued or the handle is closed
    --
    -- NoBuffering: output is written immediately and never stored in the buffer
    hSetBuffering stdout LineBuffering
    hSetBuffering stdin LineBuffering
    hSetBuffering stderr NoBuffering

    -- read PGRST_ env variables
    env <- readEnvironment

    -- read command/path from commad line
    Options{command, configPath} <- readOptionsShowHelp env

    -- build the 'AppConfig' from the config file path
    conf <- either panic identity <$> readConfig mempty env configPath

    -- These are config values that can't be reloaded at runtime. Reloading some of them would imply restarting the web server.
    let
      host = configServerHost conf
      port = configServerPort conf
      maybeSocketAddr = configServerUnixSocket conf
      dbUri = toS (configDbUri conf)
      (dbChannelEnabled, dbChannel) = (configDbChannelEnabled conf, toS $ configDbChannel conf)
      serverSettings =
          setHost ((fromString . toS) host) -- Warp settings
        . setPort port
        . setServerName (toS $ "postgrest/" <> prettyVersion) $
        defaultSettings
      poolSize = configDbPoolSize conf
      poolTimeout = configDbPoolTimeout' conf
      logLevel = configLogLevel conf
      gucConfigEnabled = configDbLoadGucConfig conf

    -- create connection pool with the provided settings, returns either a 'Connection' or a 'ConnectionError'. Does not throw.
    pool <- SQL.acquire (poolSize, poolTimeout, dbUri)

    -- Used to sync the listener(NOTIFY reload) with the connectionWorker. No connection for the listener at first. Only used if dbChannelEnabled=true.
    mvarConnectionStatus <- newEmptyMVar

    -- No schema cache at the start. Will be filled in by the connectionWorker
    refDbStructure <- newIORef Nothing

    -- Helper ref to make sure just one connectionWorker can run at a time
    refIsWorkerOn <- newIORef False

    -- Config that can change at runtime
    refConf <- newIORef conf

    let configRereader startingUp = reReadConfig startingUp pool gucConfigEnabled env configPath refConf

    -- re-read and override the config if db-load-guc-config is true
    when gucConfigEnabled $ configRereader True

    case command of
      CmdDumpConfig ->
        do
          dumpedConfig <- dumpAppConfig <$> readIORef refConf
          putStr dumpedConfig
          exitSuccess
      CmdDumpSchema ->
        do
          dumpedSchema <- dumpSchema pool =<< readIORef refConf
          putStrLn dumpedSchema
          exitSuccess
      CmdRun ->
        pass

    -- This is passed to the connectionWorker method so it can kill the main thread if the PostgreSQL's version is not supported.
    mainTid <- myThreadId

    let connWorker = Connection.worker mainTid pool refConf refDbStructure refIsWorkerOn (dbChannelEnabled, mvarConnectionStatus)

    -- Sets the initial refDbStructure
    connWorker

    case installHandlers of
      Just install ->
        install mainTid pool connWorker $ configRereader False
      Nothing ->
        pass

    -- reload schema cache + config on NOTIFY
    when dbChannelEnabled $
      Connection.listener dbUri dbChannel pool refConf refDbStructure mvarConnectionStatus connWorker $ configRereader False

    -- ask for the OS time at most once per second
    getTime <- mkAutoUpdate defaultUpdateSettings {updateAction = getCurrentTime}

    let postgrestApplication =
          postgrest
            logLevel
            refConf
            refDbStructure
            pool
            getTime
            connWorker

    case runWithSocket of
      Just run ->
        forM_ maybeSocketAddr $
          run
            serverSettings
            postgrestApplication
            (configServerUnixSocketMode conf)
      Nothing ->
        pass

    -- run the postgrest application
    case maybeSocketAddr of
      Nothing ->
        do
          putStrLn $ ("Listening on port " :: Text) <> show port
          runSettings serverSettings postgrestApplication
      Just _ ->
        pass


loadDbSettings :: SQL.Pool -> IO [(Text, Text)]
loadDbSettings pool = do
  result <- SQL.use pool $ SQL.transaction SQL.ReadCommitted SQL.Read $ SQL.statement mempty dbSettingsStatement
  case result of
    Left  e -> do
      hPutStrLn stderr ("An error ocurred when trying to query database settings for the config parameters:\n" <> show e :: Text)
      pure []
    Right x -> pure x


-- | Re-reads the config at runtime.
reReadConfig :: Bool -> SQL.Pool -> Bool -> Environment -> Maybe FilePath -> IORef AppConfig -> IO ()
reReadConfig startingUp pool gucConfigEnabled env path refConf = do
  dbSettings <- if gucConfigEnabled then loadDbSettings pool else pure []
  readConfig dbSettings env path >>= \case
    Left err   ->
      if startingUp
        then panic err -- die on invalid config if the program is starting up
        else hPutStrLn stderr $ "Failed config reload. " <> err
    Right conf -> do
      atomicWriteIORef refConf conf
      if startingUp
        then pass
        else putStrLn ("Config reloaded" :: Text)
