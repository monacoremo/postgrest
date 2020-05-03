module Main where

import qualified Data.Aeson as Aeson
import Data.String (String)
import Protolude
import Text.Pretty.Simple (pPrint)
import qualified Data.ByteString.Lazy as B
import System.IO (stderr)
import System.Exit (exitWith, ExitCode(ExitFailure))


import PostgREST.Types as Types
import PostgREST.DbStructure as DbStructure


main :: IO ()
main =
  let
    printResult = False
  in
  do
    line <- B.getContents

    case Aeson.eitherDecode line :: Either String Types.RawDbStructure of
      Right structure ->
        if printResult then
          pPrint $ DbStructure.parseDbStructure structure
        else
          putStrLn ("Done"::Text)

      Left err ->
        do
          hPutStrLn stderr err
          exitWith (ExitFailure 1)
