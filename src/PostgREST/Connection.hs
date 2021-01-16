{-# LANGUAGE MultiWayIf #-}
module PostgREST.Connection (worker, listener) where

import qualified Data.ByteString            as BS
import qualified Hasql.Connection           as C
import qualified Hasql.Notifications        as N
import qualified Hasql.Pool                 as P
import qualified Hasql.Transaction.Sessions as HT

import Control.Retry (RetryStatus, capDelay, exponentialBackoff,
                      retrying, rsPreviousDelay)
import Data.IORef    (IORef, atomicWriteIORef, readIORef)
import Data.Text.IO  (hPutStrLn)

import PostgREST.Config
import PostgREST.DbStructure (getDbStructure, getPgVersion)
import PostgREST.Error       (PgError (PgError), checkIsFatal,
                              errorPayload)
import PostgREST.Types       (ConnectionStatus (..), DbStructure,
                              PgVersion (..), SCacheStatus (..),
                              minimumPgVersion)

import Protolude      hiding (hPutStrLn, head, toS)
import Protolude.Conv (toS)

{-|
  The purpose of this worker is to obtain a healthy connection to pg and an up-to-date schema cache(DbStructure).
  This method is meant to be called by multiple times by the same thread, but does nothing if
  the previous invocation has not terminated. In all cases this method does not
  halt the calling thread, the work is preformed in a separate thread.

  Note: 'atomicWriteIORef' is essentially a lazy semaphore that prevents two
  threads from running 'connectionWorker' at the same time.

  Background thread that does the following :
  1. Tries to connect to pg server and will keep trying until success.
  2. Checks if the pg version is supported and if it's not it kills the main
     program.
  3. Obtains the dbStructure. If this fails, it goes back to 1.
-}
worker
  :: ThreadId                      -- ^ Main thread id. Killed if pg version is unsupported
  -> P.Pool                        -- ^ The pg connection pool
  -> IORef AppConfig               -- ^ mutable reference to AppConfig
  -> IORef (Maybe DbStructure)     -- ^ mutable reference to 'DbStructure'
  -> IORef Bool                    -- ^ Used as a binary Semaphore
  -> (Bool, MVar ConnectionStatus) -- ^ For interacting with the LISTEN channel
  -> IO ()
worker mainTid pool refConf refDbStructure refIsWorkerOn (dbChannelEnabled, mvarConnectionStatus) = do
  isWorkerOn <- readIORef refIsWorkerOn
  unless isWorkerOn $ do -- Prevents multiple workers to be running at the same time. Could happen on too many SIGUSR1s.
    atomicWriteIORef refIsWorkerOn True
    void $ forkIO work
  where
    work = do
      putStrLn ("Attempting to connect to the database..." :: Text)
      connected <- connectionStatus pool
      when dbChannelEnabled $
        void $ tryPutMVar mvarConnectionStatus connected -- tryPutMVar doesn't lock the thread. It should always succeed since the worker is the only mvar producer.
      case connected of
        FatalConnectionError reason -> hPutStrLn stderr reason >> killThread mainTid -- Fatal error when connecting
        NotConnected                -> return ()                                     -- Unreachable because connectionStatus will keep trying to connect
        Connected actualPgVersion   -> do                                            -- Procede with initialization
          putStrLn ("Connection successful" :: Text)
          scStatus <- loadSchemaCache pool actualPgVersion refConf refDbStructure
          case scStatus of
            SCLoaded    -> pure ()            -- do nothing and proceed if the load was successful
            SCOnRetry   -> work               -- retry
            SCFatalFail -> killThread mainTid -- die if our schema cache query has an error
          liftIO $ atomicWriteIORef refIsWorkerOn False

-- Time constants
_32s :: Int
_32s = 32000000 :: Int -- 32 seconds

_1s :: Int
_1s  = 1000000  :: Int -- 1 second

{-|
  Check if a connection from the pool allows access to the PostgreSQL database.
  If not, the pool connections are released and a new connection is tried.
  Releasing the pool is key for rapid recovery. Otherwise, the pool timeout would have to be reached for new healthy connections to be acquired.
  Which might not happen if the server is busy with requests. No idle connection, no pool timeout.

  The connection tries are capped, but if the connection times out no error is thrown, just 'False' is returned.
-}
connectionStatus :: P.Pool -> IO ConnectionStatus
connectionStatus pool =
  retrying (capDelay _32s $ exponentialBackoff _1s)
           shouldRetry
           (const $ P.release pool >> getConnectionStatus)
  where
    getConnectionStatus :: IO ConnectionStatus
    getConnectionStatus = do
      pgVersion <- P.use pool getPgVersion
      case pgVersion of
        Left e -> do
          let err = PgError False e
          hPutStrLn stderr . toS $ errorPayload err
          case checkIsFatal err of
            Just reason -> return $ FatalConnectionError reason
            Nothing     -> return NotConnected

        Right version ->
          if version < minimumPgVersion
             then return . FatalConnectionError $ "Cannot run in this PostgreSQL version, PostgREST needs at least " <> pgvName minimumPgVersion
             else return . Connected  $ version

    shouldRetry :: RetryStatus -> ConnectionStatus -> IO Bool
    shouldRetry rs isConnSucc = do
      let delay    = fromMaybe 0 (rsPreviousDelay rs) `div` _1s
          itShould = NotConnected == isConnSucc
      when itShould $
        putStrLn $ "Attempting to reconnect to the database in " <> (show delay::Text) <> " seconds..."
      return itShould

-- | Load the DbStructure by using a connection from the pool.
loadSchemaCache :: P.Pool -> PgVersion -> IORef AppConfig -> IORef (Maybe DbStructure) -> IO SCacheStatus
loadSchemaCache pool actualPgVersion refConf refDbStructure = do
  conf <- readIORef refConf
  result <- P.use pool $ HT.transaction HT.ReadCommitted HT.Read $ getDbStructure (toList $ configDbSchemas conf) (configDbExtraSearchPath conf) actualPgVersion (configDbPreparedStatements conf)
  case result of
    Left e -> do
      let err = PgError False e
          putErr = hPutStrLn stderr . toS . errorPayload $ err
      case checkIsFatal err of
        Just _  -> do
          hPutStrLn stderr ("A fatal error ocurred when loading the schema cache" :: Text)
          putErr
          hPutStrLn stderr ("This is probably a bug in PostgREST, please report it at https://github.com/PostgREST/postgrest/issues" :: Text)
          return SCFatalFail
        Nothing -> do
          hPutStrLn stderr ("An error ocurred when loading the schema cache" :: Text) >> putErr
          return SCOnRetry

    Right dbStructure -> do
      atomicWriteIORef refDbStructure $ Just dbStructure
      putStrLn ("Schema cache loaded" :: Text)
      return SCLoaded

{-|
  Starts a dedicated pg connection to LISTEN for notifications.
  When a NOTIFY <db-channel> - with an empty payload - is done, it refills the schema cache.
  It uses the connectionWorker in case the LISTEN connection dies.
-}
listener :: ByteString -> Text -> P.Pool -> IORef AppConfig -> IORef (Maybe DbStructure) -> MVar ConnectionStatus -> IO () -> IO () -> IO ()
listener dbUri dbChannel pool refConf refDbStructure mvarConnectionStatus connWorker configLoader = start
  where
    start = do
      connStatus <- takeMVar mvarConnectionStatus -- takeMVar makes the thread wait if the MVar is empty(until there's a connection).
      case connStatus of
        Connected actualPgVersion -> void $ forkFinally (do -- forkFinally allows to detect if the thread dies
          dbOrError <- C.acquire dbUri
          case dbOrError of
            Right db -> do
              putStrLn $ "Listening for notifications on the " <> dbChannel <> " channel"
              let channelToListen = N.toPgIdentifier dbChannel
                  scLoader = void $ loadSchemaCache pool actualPgVersion refConf refDbStructure -- It's not necessary to check the loadSchemaCache success here. If the connection drops, the thread will die and proceed to recover below.
              N.listen db channelToListen
              N.waitForNotifications (\_ msg ->
                if | BS.null msg            -> scLoader      -- reload the schema cache
                   | msg == "reload schema" -> scLoader      -- reload the schema cache
                   | msg == "reload config" -> configLoader  -- reload the config
                   | otherwise              -> pure ()       -- Do nothing if anything else than an empty message is sent
                ) db
            _ -> die errorMessage)
          (\_ -> do -- if the thread dies, we try to recover
            putStrLn retryMessage
            connWorker -- assume the pool connection was also lost, call the connection worker
            start)     -- retry the listener
        _ ->
          putStrLn errorMessage -- Should be unreachable. connectionStatus will retry until there's a connection.
    errorMessage = "Could not listen for notifications on the " <> dbChannel <> " channel" :: Text
    retryMessage = "Retrying listening for notifications on the " <> dbChannel <> " channel.." :: Text
