{-# LANGUAGE QuasiQuotes #-}
{-|
Module      : PostgREST.Statements
Description : PostgREST single SQL statements.

This module constructs single SQL statements that can be parametrized and prepared.

- It consumes the SqlQuery types generated by the QueryBuilder module.
- It generates the body format and some headers of the final HTTP response.

TODO: Currently, createReadStatement is not using prepared statements. See https://github.com/PostgREST/postgrest/issues/718.
-}
module PostgREST.Statements
  ( createWriteStatement
  , createReadStatement
  , callProcStatement
  , createExplainStatement
  , dbSettingsStatement
  ) where

import qualified Data.Aeson                        as JSON
import qualified Data.Aeson.Lens                   as L
import qualified Data.ByteString.Char8             as BS
import qualified Hasql.Decoders                    as HD
import qualified Hasql.DynamicStatements.Snippet   as H
import qualified Hasql.DynamicStatements.Statement as H
import qualified Hasql.Encoders                    as HE
import qualified Hasql.Statement                   as H

import Control.Lens                  ((^?))
import Data.Maybe                    (fromJust)
import Data.Text.Read                (decimal)
import Network.HTTP.Types.Status     (Status)
import Text.InterpolatedString.Perl6 (q)

import PostgREST.Error      (Error (..))
import PostgREST.Headers    (GucHeader)
import PostgREST.PgVersions (PgVersion)

import PostgREST.ApiRequest.Preferences
import PostgREST.DbStructure.Identifiers (FieldName)
import PostgREST.SqlFragment

import Protolude      hiding (toS)
import Protolude.Conv (toS)


{-| The generic query result format used by API responses. The location header
    is represented as a list of strings containing variable bindings like
    @"k1=eq.42"@, or the empty list if there is no location header.
-}
type ResultsWithCount = (Maybe Int64, Int64, [BS.ByteString], BS.ByteString, Either Error [GucHeader], Either Error (Maybe Status))

createWriteStatement :: H.Snippet -> H.Snippet -> Bool -> Bool -> Bool ->
                        PreferRepresentation -> [Text] -> PgVersion -> Bool ->
                        H.Statement () ResultsWithCount
createWriteStatement selectQuery mutateQuery wantSingle isInsert asCsv rep pKeys pgVer =
  H.dynamicallyParameterized snippet decodeStandard
 where
  snippet =
    "WITH " <> H.sql sourceCTEName <> " AS (" <> mutateQuery <> ") " <>
    H.sql (
    "SELECT " <>
      "'' AS total_result_set, " <>
      "pg_catalog.count(_postgrest_t) AS page_total, " <>
      locF <> " AS header, " <>
      bodyF <> " AS body, " <>
      responseHeadersF pgVer <> " AS response_headers, " <>
      responseStatusF pgVer  <> " AS response_status "
    ) <>
    "FROM (" <> selectF <> ") _postgrest_t"

  locF =
    if isInsert && rep `elem` [Full, HeadersOnly]
      then BS.unwords [
        "CASE WHEN pg_catalog.count(_postgrest_t) = 1",
          "THEN coalesce(" <> locationF pKeys <> ", " <> noLocationF <> ")",
          "ELSE " <> noLocationF,
        "END"]
      else noLocationF

  bodyF
    | rep `elem` [None, HeadersOnly] = "''"
    | asCsv = asCsvF
    | wantSingle = asJsonSingleF False
    | otherwise = asJsonF False

  selectF
    -- prevent using any of the column names in ?select= when no response is returned from the CTE
    | rep `elem` [None, HeadersOnly] = H.sql ("SELECT * FROM " <> sourceCTEName)
    | otherwise                      = selectQuery

  decodeStandard :: HD.Result ResultsWithCount
  decodeStandard =
   fromMaybe (Nothing, 0, [], mempty, Right [], Right Nothing) <$> HD.rowMaybe standardRow

createReadStatement :: H.Snippet -> H.Snippet -> Bool -> Bool -> Bool -> Maybe FieldName -> PgVersion -> Bool ->
                       H.Statement () ResultsWithCount
createReadStatement selectQuery countQuery isSingle countTotal asCsv binaryField pgVer =
  H.dynamicallyParameterized snippet decodeStandard
 where
  snippet =
    "WITH " <>
    H.sql sourceCTEName <> " AS ( " <> selectQuery <> " ) " <>
    countCTEF <> " " <>
    H.sql ("SELECT " <>
      countResultF <> " AS total_result_set, " <>
      "pg_catalog.count(_postgrest_t) AS page_total, " <>
      noLocationF <> " AS header, " <>
      bodyF <> " AS body, " <>
      responseHeadersF pgVer <> " AS response_headers, " <>
      responseStatusF pgVer <> " AS response_status " <>
    "FROM ( SELECT * FROM " <> sourceCTEName <> " ) _postgrest_t")

  (countCTEF, countResultF) = countF countQuery countTotal

  bodyF
    | asCsv = asCsvF
    | isSingle = asJsonSingleF False
    | isJust binaryField = asBinaryF $ fromJust binaryField
    | otherwise = asJsonF False

  decodeStandard :: HD.Result ResultsWithCount
  decodeStandard =
    HD.singleRow standardRow

{-| Read and Write api requests use a similar response format which includes
    various record counts and possible location header. This is the decoder
    for that common type of query.
-}
standardRow :: HD.Row ResultsWithCount
standardRow = (,,,,,) <$> nullableColumn HD.int8 <*> column HD.int8
                      <*> arrayColumn HD.bytea <*> column HD.bytea
                      <*> (fromMaybe (Right []) <$> nullableColumn decodeGucHeaders)
                      <*> (fromMaybe (Right Nothing) <$> nullableColumn decodeGucStatus)

type ProcResults = (Maybe Int64, Int64, ByteString, Either Error [GucHeader], Either Error (Maybe Status))

callProcStatement :: Bool -> Bool -> H.Snippet -> H.Snippet -> H.Snippet -> Bool ->
                     Bool -> Bool -> Bool -> Maybe FieldName -> PgVersion -> Bool ->
                     H.Statement () ProcResults
callProcStatement returnsScalar returnsSingle callProcQuery selectQuery countQuery countTotal asSingle asCsv multObjects binaryField pgVer =
  H.dynamicallyParameterized snippet decodeProc
  where
    snippet =
      "WITH " <> H.sql sourceCTEName <> " AS (" <> callProcQuery <> ") " <>
      countCTEF <>
      H.sql (
      "SELECT " <>
        countResultF <> " AS total_result_set, " <>
        "pg_catalog.count(_postgrest_t) AS page_total, " <>
        bodyF <> " AS body, " <>
        responseHeadersF pgVer <> " AS response_headers, " <>
        responseStatusF pgVer <> " AS response_status ") <>
      "FROM (" <> selectQuery <> ") _postgrest_t"

    (countCTEF, countResultF) = countF countQuery countTotal

    bodyF
     | asSingle           = asJsonSingleF returnsScalar
     | asCsv              = asCsvF
     | isJust binaryField = asBinaryF $ fromJust binaryField
     | returnsSingle
       && not multObjects = asJsonSingleF returnsScalar
     | otherwise          = asJsonF returnsScalar

    decodeProc :: HD.Result ProcResults
    decodeProc =
      fromMaybe (Just 0, 0, mempty, defGucHeaders, defGucStatus) <$> HD.rowMaybe procRow
      where
        defGucHeaders = Right []
        defGucStatus  = Right Nothing
        procRow = (,,,,) <$> nullableColumn HD.int8 <*> column HD.int8
                         <*> column HD.bytea
                         <*> (fromMaybe defGucHeaders <$> nullableColumn decodeGucHeaders)
                         <*> (fromMaybe defGucStatus <$> nullableColumn decodeGucStatus)

createExplainStatement :: H.Snippet -> Bool -> H.Statement () (Maybe Int64)
createExplainStatement countQuery =
  H.dynamicallyParameterized snippet decodeExplain
  where
    snippet = "EXPLAIN (FORMAT JSON) " <> countQuery
    -- |
    -- An `EXPLAIN (FORMAT JSON) select * from items;` output looks like this:
    -- [{
    --   "Plan": {
    --     "Node Type": "Seq Scan", "Parallel Aware": false, "Relation Name": "items",
    --     "Alias": "items", "Startup Cost": 0.00, "Total Cost": 32.60,
    --     "Plan Rows": 2260,"Plan Width": 8} }]
    -- We only obtain the Plan Rows here.
    decodeExplain :: HD.Result (Maybe Int64)
    decodeExplain =
      let row = HD.singleRow $ column HD.bytea in
      (^? L.nth 0 . L.key "Plan" .  L.key "Plan Rows" . L._Integral) <$> row

decodeGucHeaders :: HD.Value (Either Error [GucHeader])
decodeGucHeaders = first (const GucHeadersError) . JSON.eitherDecode . toS <$> HD.bytea

decodeGucStatus :: HD.Value (Either Error (Maybe Status))
decodeGucStatus = first (const GucStatusError) . fmap (Just . toEnum . fst) . decimal <$> HD.text

-- | Get db settings from the connection role. Global settings will be overridden by database specific settings.
dbSettingsStatement :: H.Statement () [(Text, Text)]
dbSettingsStatement = H.Statement sql HE.noParams decodeSettings False
  where
    sql = [q|
      with
      role_setting as (
        select setdatabase, unnest(setconfig) as setting from pg_catalog.pg_db_role_setting
        where setrole = current_user::regrole::oid
          and setdatabase in (0, (select oid from pg_catalog.pg_database where datname = current_catalog))
      ),
      kv_settings as (
        select setdatabase, split_part(setting, '=', 1) as k, split_part(setting, '=', 2) as value from role_setting
        where setting like 'pgrst.%'
      )
      select distinct on (key) replace(k, 'pgrst.', '') as key, value
      from kv_settings
      order by key, setdatabase desc;
    |]
    decodeSettings = HD.rowList $ (,) <$> column HD.text <*> column HD.text

column :: HD.Value a -> HD.Row a
column = HD.column . HD.nonNullable

nullableColumn :: HD.Value a -> HD.Row (Maybe a)
nullableColumn = HD.column . HD.nullable

arrayColumn :: HD.Value a -> HD.Row [a]
arrayColumn = column . HD.listArray . HD.nonNullable
