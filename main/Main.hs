{-# LANGUAGE CPP #-}
module Main (main) where

import qualified PostgREST.CLI as CLI
#ifndef mingw32_HOST_OS
import qualified PostgREST.Signals    as Signals
import qualified PostgREST.UnixSocket as UnixSocket
#endif

import Protolude

main :: IO ()
main =
#ifdef mingw32_HOST_OS
  CLI.app Nothing Nothing
#else
  CLI.app
    (Just Signals.installHandlers)
    (Just UnixSocket.runAppInSocket)
#endif
