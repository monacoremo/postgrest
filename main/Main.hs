{-# LANGUAGE CPP #-}
module Main (main) where

import qualified PostgREST.CLI as CLI
#ifndef mingw32_HOST_OS
import PostgREST.Signals    (installHandlers)
import PostgREST.UnixSocket (runWithSocket)
#endif

import Protolude

main :: IO ()
main =
#ifdef mingw32_HOST_OS
  CLI.app Nothing Nothing
#else
  CLI.app (Just installHandlers) (Just runWithSocket)
#endif
