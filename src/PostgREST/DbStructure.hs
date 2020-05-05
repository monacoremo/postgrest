{-|
Module      : PostgREST.DbStructure
Description : PostgREST schema cache

This module contains queries that target PostgreSQL system catalogs, these are used to build the schema cache(DbStructure).

The schema cache is necessary for resource embedding, foreign keys are used for inferring the relationships between tables.

These queries are executed once at startup or when PostgREST is reloaded.
-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns        #-}
{-# LANGUAGE QuasiQuotes           #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeSynonymInstances  #-}

module PostgREST.DbStructure (
  getDbStructure
, accessibleTables
, accessibleProcs
, parseDbStructure
, getPgVersion
) where

import           Control.Exception
import qualified Data.Aeson                    as Aeson
import qualified Data.FileEmbed                as FileEmbed
import qualified Data.HashMap.Strict           as M
import qualified Data.List                     as L
import           Data.String
import qualified Hasql.Decoders                as HD
import qualified Hasql.Encoders                as HE
import qualified Hasql.Session                 as H
import qualified Hasql.Statement               as H
import qualified Hasql.Transaction             as HT
import           PostgREST.Private.Common
import           PostgREST.Types
import           Protolude

getDbStructure :: [Schema] -> PgVersion -> HT.Transaction DbStructure
getDbStructure schemas _ = do
  -- This voids the search path. The following queries need this for getting the
  -- fully qualified name(schema.name) of every db object.
  HT.sql "set local schema ''"
  raw <- getRawDbStructure schemas

  return $ parseDbStructure raw

parseDbStructure :: RawDbStructure -> DbStructure
parseDbStructure raw =
  let
    tabs = rawDbTables raw
    cols = rawDbColumns raw
    rels = rawDbRels raw
    keys = rawDbPrimaryKeys raw
    procs = procsMap $ fmap loadProc $ rawDbProcs raw
    cols' = addForeignKeys rels cols
  in
  DbStructure
    { dbTables = tabs
    , dbColumns = cols'
    , dbRelations = rels
    , dbPrimaryKeys = keys
    , dbProcs = procs
    , pgVersion = rawDbPgVer raw
    , dbSchemas = rawDbSchemas raw
    }

accessibleTables :: DbStructure -> [Table]
accessibleTables structure =
   filter tableIsAccessible (dbTables structure)

accessibleProcs :: DbStructure -> ProcsMap
accessibleProcs structure =
  fmap (filter pdIsAccessible) (dbProcs structure)

procsMap :: [ProcDescription] -> ProcsMap
procsMap procs =
  M.fromListWith (++) . map (\(x,y) -> (x, [y])) . sort $ map addKey procs

loadProc :: RawProcDescription -> ProcDescription
loadProc raw =
  ProcDescription
    { pdSchema = procSchema raw
    , pdName = procName raw
    , pdDescription = procDescription raw
    , pdArgs = procArgs raw
    , pdReturnType =
        parseRetType
          (procReturnTypeQi raw)
          (procReturnTypeIsSetof raw)
          (procReturnTypeIsComposite raw)
    , pdVolatility = procVolatility raw
    , pdIsAccessible = procIsAccessible raw
    }

addKey :: ProcDescription -> (QualifiedIdentifier, ProcDescription)
addKey pd = (QualifiedIdentifier (pdSchema pd) (pdName pd), pd)

parseRetType :: QualifiedIdentifier -> Bool -> Bool -> RetType
parseRetType qi isSetOf isComposite
  | isSetOf = SetOf pgType
  | otherwise = Single pgType
  where
    pgType = if isComposite then Composite qi else Scalar qi

addForeignKeys :: [Relation] -> [Column] -> [Column]
addForeignKeys rels = map addFk
  where
    addFk col = col { colFK = fk col }
    fk col = find (lookupFn col) rels >>= relToFk col
    lookupFn :: Column -> Relation -> Bool
    lookupFn c Relation{relColumns=cs, relType=rty} = c `elem` cs && rty==M2O
    relToFk col Relation{relColumns=cols, relFColumns=colsF} = do
      pos <- L.elemIndex col cols
      colF <- atMay colsF pos
      return $ ForeignKey colF

getPgVersion :: H.Session PgVersion
getPgVersion = H.statement () $ H.Statement sql HE.noParams versionRow False
  where
    sql = "SELECT current_setting('server_version_num')::integer, current_setting('server_version')"
    versionRow = HD.singleRow $ PgVersion <$> column HD.int4 <*> column HD.text



-- RAW DB STRUCTURE


getRawDbStructure :: [Schema] -> HT.Transaction RawDbStructure
getRawDbStructure schemas =
    do
        value <- HT.statement schemas rawDbStructureQuery

        case Aeson.fromJSON value of
          Aeson.Success m ->
              return m
          Aeson.Error err ->
              throw $ DbStructureDecodeException err

data DbStructureDecodeException =
    DbStructureDecodeException String
    deriving Show

instance Exception DbStructureDecodeException

rawDbStructureQuery :: H.Statement [Schema] Aeson.Value
rawDbStructureQuery =
  let
    sql =
      $(FileEmbed.embedFile "dbstructure/query.sql")

    decode =
      HD.singleRow $ column HD.json
  in
  H.Statement sql (arrayParam HE.text) decode True
