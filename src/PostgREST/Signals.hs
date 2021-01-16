module PostgREST.Signals (installHandlers) where

import qualified Hasql.Pool           as P
import           System.Posix.Signals

import Protolude hiding (hPutStrLn, head, toS)


installHandlers :: ThreadId -> P.Pool -> IO () -> IO () -> IO ()
installHandlers mainTid pool connWorker configRereader =
  do
    -- Only for systems with signals:
    --
    -- releases the connection pool whenever the program is terminated,
    -- see https://github.com/PostgREST/postgrest/issues/268
    forM_ [sigINT, sigTERM] $ \sig ->
      void $ installHandler sig (Catch $ do
          P.release pool
          throwTo mainTid UserInterrupt
        ) Nothing

    -- The SIGUSR1 signal updates the internal 'DbStructure' by running 'connectionWorker' exactly as before.
    void $ installHandler sigUSR1 (
      Catch connWorker
      ) Nothing

    -- Re-read the config on SIGUSR2
    void $ installHandler sigUSR2 (
      Catch configRereader
      ) Nothing
