-- | Module : PostgREST.App
--   Description : PostgREST main application
--
--   This module is in charge of mapping HTTP requests to PostgreSQL queries.
--
--   Some of its functionality includes:
--
--   - Mapping HTTP request methods to proper SQL statements. For example, a GET
--     request is translated to executing a SELECT query in a read-only transaction
--   - Producing HTTP Headers according to RFCs
--   - Content Negotiation
module PostgREST.App (postgrest) where

import Control.Monad.Except (liftEither)
import Data.Either.Combinators (mapLeft)
import Data.IORef (IORef, readIORef)
import Data.Time.Clock (UTCTime)

import qualified Data.ByteString.Char8 as Char8ByteString
import qualified Data.ByteString.Lazy as LazyByteString
import qualified Data.HashMap.Strict as HashMap
import qualified Data.List as List (union)
import qualified Data.Set as Set
import qualified Hasql.DynamicStatements.Snippet as Hasql
import qualified Hasql.Pool as Hasql
import qualified Hasql.Transaction as Hasql
import qualified Hasql.Transaction.Sessions as Hasql
import qualified Network.HTTP.Types.Header as HTTP
import qualified Network.HTTP.Types.Status as HTTP
import qualified Network.HTTP.Types.URI as HTTP
import qualified Network.Wai as Wai

import qualified PostgREST.ApiRequest as ApiRequest
import PostgREST.ApiRequest (ApiRequest)
import qualified PostgREST.Auth as Auth
import qualified PostgREST.Config as Config
import PostgREST.Config (AppConfig)
import qualified PostgREST.DbRequestBuilder as DbRequestBuilder
import qualified PostgREST.DbStructure as DbStructure
import qualified PostgREST.Error as Error
import qualified PostgREST.Middleware as Middleware
import qualified PostgREST.OpenAPI as OpenAPI
import qualified PostgREST.QueryBuilder as QueryBuilder
import qualified PostgREST.RangeQuery as RangeQuery
import qualified PostgREST.Statements as Statements
import qualified PostgREST.Types as Types
import PostgREST.Types (ContentType, DbStructure, ReadRequest, QualifiedIdentifier)

import Protolude hiding (toS, Handler)
import Protolude.Conv (toS)



data RequestContext =
  RequestContext
    { rConfig :: AppConfig
    , rDbStructure :: DbStructure
    , rApiRequest :: ApiRequest
    , rContentType :: ContentType
    }


type Handler = ExceptT Error


type DbHandler = Handler Hasql.Transaction


-- TODO: Replace Error with a type from the Error module, instead of opaque
-- Wai.Response.
type Error = Wai.Response


-- | PostgREST application
postgrest
  :: Types.LogLevel
  -> IORef AppConfig
  -> IORef (Maybe DbStructure)
  -> Hasql.Pool
  -> IO UTCTime
  -> IO () -- ^ Lauch connection worker in a separate thread
  -> Wai.Application
postgrest logLev refConf refDbStructure pool getTime connWorker =
  Middleware.pgrstMiddleware logLev $
    \req respond ->
      do
        time <- getTime
        conf <- readIORef refConf
        maybeDbStructure <- readIORef refDbStructure

        let
          eitherResponse :: IO (Either Error Wai.Response)
          eitherResponse =
            runExceptT $ postgrestResponse conf maybeDbStructure pool time req

        response <- either errorToResponse identity <$> eitherResponse

        -- Launch the connWorker when the connection is down.  The postgrest
        -- function can respond successfully (with a stale schema cache) before
        -- the connWorker is done.
        when (Wai.responseStatus response == HTTP.status503) connWorker

        respond response


errorToResponse :: Error -> Wai.Response
errorToResponse =
  -- TODO: Replace with Error.errorResponseFor or similar once Error is refactored
  identity


postgrestResponse
  :: AppConfig
  -> Maybe DbStructure
  -> Hasql.Pool
  -> UTCTime
  -> Wai.Request
  -> Handler IO Wai.Response
postgrestResponse conf maybeDbStructure pool time req =
  do
    body <- lift $ Wai.strictRequestBody req

    dbStructure <-
      case maybeDbStructure of
        Just dbStructure ->
          return dbStructure

        Nothing ->
          throwError $ Error.errorResponseFor Error.ConnectionLostError

    apiRequest <-
      liftEither . mapLeft Error.errorResponseFor $
        ApiRequest.userApiRequest
          (Config.configDbSchemas conf)
          (Config.configDbRootSpec conf)
          dbStructure
          req
          body

    -- The JWT must be checked before touching the db
    eitherJwtClaims <-
      lift $
        Auth.attemptJwtClaims
          (Config.configJWKS conf)
          (Config.configJwtAudience conf)
          (toS $ ApiRequest.iJWT apiRequest)
          time
          (rightToMaybe $ Config.configJwtRoleClaimKey conf)

    jwtClaims <-
      liftEither . mapLeft Error.errorResponseFor $ Auth.jwtClaims eitherJwtClaims

    contentType <-
      liftEither . maybeToRight (contentTypeError apiRequest) $
        ApiRequest.mutuallyAgreeable
          (requestContentTypes conf apiRequest)
          (ApiRequest.iAccepts apiRequest)

    let
      context apiReq =
        RequestContext conf dbStructure apiReq contentType

      handleReq apiReq =
        either identity identity <$> runExceptT (handleRequest (context apiReq))

    dbResp <-
      lift . Hasql.use pool .
        Hasql.transaction Hasql.ReadCommitted (txMode apiRequest) .
          optionalRollback conf apiRequest $
            Middleware.runPgLocals conf jwtClaims handleReq apiRequest

    liftEither $
      mapLeft (Error.errorResponseFor . Error.PgError (Auth.containsRole jwtClaims))
      dbResp


handleRequest :: RequestContext -> DbHandler Wai.Response
handleRequest context@(RequestContext _ dbStructure apiRequest _) =
  case (ApiRequest.iAction apiRequest, ApiRequest.iTarget apiRequest) of
    (ApiRequest.ActionRead headersOnly, ApiRequest.TargetIdent identifier) ->
      handleRead headersOnly identifier context

    (ApiRequest.ActionCreate, ApiRequest.TargetIdent identifier) ->
      handleCreate identifier context

    (ApiRequest.ActionUpdate, ApiRequest.TargetIdent identifier) ->
      handleUpdate identifier context

    (ApiRequest.ActionSingleUpsert, ApiRequest.TargetIdent identifier) ->
      handleSingleUpsert identifier context

    (ApiRequest.ActionDelete, ApiRequest.TargetIdent identifier) ->
      handleDelete identifier context

    (ApiRequest.ActionInfo, ApiRequest.TargetIdent identifier) ->
      handleInfo identifier dbStructure

    (ApiRequest.ActionInvoke invMethod, ApiRequest.TargetProc proc _) ->
      handleInvoke invMethod proc context

    (ApiRequest.ActionInspect headersOnly, ApiRequest.TargetDefaultSpec tSchema) ->
      handleOpenApi headersOnly tSchema context

    _ ->
      throwError $ Wai.responseLBS  HTTP.status404 mempty mempty


handleRead :: Bool -> QualifiedIdentifier -> RequestContext -> DbHandler Wai.Response
handleRead headersOnly identifier context@(RequestContext conf dbStructure apiRequest contentType) =
  do
    req <- readRequest identifier context
    bField <- binaryField context req

    let
      countQuery =
        QueryBuilder.readRequestToCountQuery req

    (tableTotal, queryTotal, _ , body, gucHeaders, gucStatus) <-
      lift $ Hasql.statement mempty $
        Statements.createReadStatement
          (QueryBuilder.readRequestToQuery req)
          ( if ApiRequest.iPreferCount apiRequest == Just Types.EstimatedCount then
              -- LIMIT maxRows + 1 so we can determine below that maxRows was surpassed
              QueryBuilder.limitedQuery countQuery ((+ 1) <$> Config.configDbMaxRows conf)
            else
              countQuery
          )
          (contentType == Types.CTSingularJSON)
          (shouldCount apiRequest)
          (contentType == Types.CTTextCSV)
          bField
          (Types.pgVersion dbStructure)
          (Config.configDbPreparedStatements conf)

    total <- readTotal conf apiRequest tableTotal countQuery

    gucs <-
      liftEither . mapLeft Error.errorResponseFor $
        maybeGucs <$> gucHeaders <*> gucStatus

    let
      (status, contentRange) =
        RangeQuery.rangeStatusHeader
          (ApiRequest.iTopLevelRange apiRequest)
          queryTotal
          total

      qString =
        ApiRequest.iCanonicalQS apiRequest

      headers =
        [ contentRange
        , ( "Content-Location"
          , "/"
              <> toS (Types.qiName identifier)
              <> if Char8ByteString.null qString then
                   mempty
                 else
                   "?" <> toS qString
          )
        ]
        ++ contentTypeHeaders context

    failNotSingular contentType queryTotal . gucResponse gucs status headers $
      if headersOnly then mempty else toS body


readTotal
  :: AppConfig
  -> ApiRequest
  -> Maybe Int64
  -> Hasql.Snippet
  -> DbHandler (Maybe Int64)
readTotal conf apiRequest tableTotal countQuery =
  let
    explain =
      lift . Hasql.statement mempty $
        Statements.createExplainStatement countQuery
          (Config.configDbPreparedStatements conf)
  in
  case ApiRequest.iPreferCount apiRequest of
    Just Types.PlannedCount ->
      explain

    Just Types.EstimatedCount ->
      if tableTotal > (fromIntegral <$> Config.configDbMaxRows conf) then
        do
          estTotal <- explain
          return $
            if estTotal > tableTotal then
              estTotal
            else
              tableTotal
      else
        return tableTotal

    _ ->
      return tableTotal


handleCreate :: QualifiedIdentifier -> RequestContext -> DbHandler Wai.Response
handleCreate identifier context@(RequestContext _ dbStructure apiRequest contentType) =
  let
    pkCols =
      Types.tablePKCols dbStructure (Types.qiSchema identifier) (Types.qiName identifier)
  in
  do
    result <- writeQuery identifier True pkCols context

    let
      headers =
        catMaybes
          [ if null $ rFields result then
              Nothing
            else
              Just
                ( HTTP.hLocation
                , "/"
                    <> toS (Types.qiName identifier)
                    <> HTTP.renderSimpleQuery True (splitKeyValue <$> rFields result)
                )
          , Just $
              RangeQuery.contentRangeH 1 0
                (if shouldCount apiRequest then Just (rQueryTotal result) else Nothing)
          , if null pkCols && isNothing (ApiRequest.iOnConflict apiRequest) then
              Nothing
            else
              (\x -> ("Preference-Applied", Char8ByteString.pack (show x))) <$>
                ApiRequest.iPreferResolution apiRequest
          ]

    failNotSingular contentType (rQueryTotal result) $
      if ApiRequest.iPreferRepresentation apiRequest == Types.Full then
        gucResponse (rGucs result) HTTP.status201
          (headers ++ contentTypeHeaders context)
          (toS $ rBody result)
      else
        gucResponse (rGucs result) HTTP.status201 headers mempty


handleUpdate :: QualifiedIdentifier -> RequestContext -> DbHandler Wai.Response
handleUpdate identifier context@(RequestContext _ _ apiRequest contentType) =
  do
    result <- writeQuery identifier False mempty context

    let
      fullRepr =
        ApiRequest.iPreferRepresentation apiRequest == Types.Full

      updateIsNoOp =
        Set.null (ApiRequest.iColumns apiRequest)

      status
        | rQueryTotal result == 0 && not updateIsNoOp = HTTP.status404
        | fullRepr = HTTP.status200
        | otherwise = HTTP.status204

      contentRangeHeader =
        RangeQuery.contentRangeH 0 (rQueryTotal result - 1)
          (if shouldCount apiRequest then Just $ rQueryTotal result else Nothing)

    failNotSingular contentType (rQueryTotal result) $
      if fullRepr then
        gucResponse (rGucs result) status
          (contentTypeHeaders context ++ [contentRangeHeader])
          (toS $ rBody result)
      else
        gucResponse (rGucs result) status [contentRangeHeader] mempty


handleSingleUpsert :: QualifiedIdentifier -> RequestContext-> DbHandler Wai.Response
handleSingleUpsert identifier context@(RequestContext _ _ apiRequest _) =
  if ApiRequest.iTopLevelRange apiRequest /= RangeQuery.allRange then
    throwError $ Error.errorResponseFor Error.PutRangeNotAllowedError
  else
    do
      result <- writeQuery identifier False mempty context

      -- Makes sure the querystring pk matches the payload pk
      -- e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
      -- PUT /items?id=eq.14 { "id" : 2, .. } is rejected.
      -- If this condition is not satisfied then nothing is inserted,
      -- check the WHERE for INSERT in QueryBuilder.hs to see how it's done
      if rQueryTotal result /= 1 then
        do
          lift Hasql.condemn
          throwError $ Error.errorResponseFor Error.PutMatchingPkError
      else
        return $
          if ApiRequest.iPreferRepresentation apiRequest == Types.Full then
            gucResponse (rGucs result) HTTP.status200
              (contentTypeHeaders context)
              (toS $ rBody result)
          else
            gucResponse (rGucs result) HTTP.status204 (contentTypeHeaders context) mempty


handleDelete :: QualifiedIdentifier -> RequestContext -> DbHandler Wai.Response
handleDelete identifier context@(RequestContext _ _ apiRequest contentType) =
  do
    result <- writeQuery identifier False mempty context

    let
      contentRangeHeader =
        RangeQuery.contentRangeH 1 0 $
          if shouldCount apiRequest then Just (rQueryTotal result) else Nothing

    failNotSingular contentType (rQueryTotal result) $
      if ApiRequest.iPreferRepresentation apiRequest == Types.Full then
        gucResponse (rGucs result) HTTP.status200
          (contentTypeHeaders context ++ [contentRangeHeader])
          (toS $ rBody result)
      else
        gucResponse (rGucs result) HTTP.status204 [contentRangeHeader] mempty


handleInfo :: Monad m =>
  QualifiedIdentifier -> DbStructure -> Handler m Wai.Response
handleInfo identifier dbStructure =
  let
    allowH table =
      ( HTTP.hAllow
      , if Types.tableInsertable table then "GET,POST,PATCH,DELETE" else "GET"
      )

    allOrigins =
      ("Access-Control-Allow-Origin", "*")
  in
  case findTable dbStructure identifier of
    Just table ->
      return $ Wai.responseLBS HTTP.status200 [allOrigins, allowH table] mempty

    Nothing ->
      throwError $ Wai.responseLBS HTTP.status404 mempty mempty


handleInvoke
  :: ApiRequest.InvokeMethod
  -> Types.ProcDescription
  -> RequestContext
  -> DbHandler Wai.Response
handleInvoke invMethod proc context@(RequestContext conf dbStructure apiRequest contentType) =
  let
    identifier =
      Types.QualifiedIdentifier
        (Types.pdSchema proc)
        (fromMaybe (Types.pdName proc) $ Types.procTableName proc)

    returnsSingle =
      case ApiRequest.iTarget apiRequest of
        ApiRequest.TargetProc targetProc _ ->
          Types.procReturnsSingle targetProc

        _ ->
          False
  in
  do
    req <- readRequest identifier context
    bField <- binaryField context req

    (tableTotal, queryTotal, body, gucHeaders, gucStatus) <-
      lift . Hasql.statement mempty $
        Statements.callProcStatement
          (returnsScalar apiRequest)
          returnsSingle
          (QueryBuilder.requestToCallProcQuery
            (Types.QualifiedIdentifier (Types.pdSchema proc) (Types.pdName proc))
            (Types.specifiedProcArgs (ApiRequest.iColumns apiRequest) proc)
            (ApiRequest.iPayload apiRequest)
            (returnsScalar apiRequest)
            (ApiRequest.iPreferParameters apiRequest)
            (DbRequestBuilder.returningCols req [])
          )
          (QueryBuilder.readRequestToQuery req)
          (QueryBuilder.readRequestToCountQuery req)
          (shouldCount apiRequest)
          (contentType == Types.CTSingularJSON)
          (contentType == Types.CTTextCSV)
          (ApiRequest.iPreferParameters apiRequest == Just Types.MultipleObjects)
          bField
          (Types.pgVersion dbStructure)
          (Config.configDbPreparedStatements conf)

    gucs <-
      liftEither . mapLeft Error.errorResponseFor $
        maybeGucs <$> gucHeaders <*> gucStatus

    let
      (status, contentRange) =
        RangeQuery.rangeStatusHeader
          (ApiRequest.iTopLevelRange apiRequest)
          queryTotal
          tableTotal

    failNotSingular contentType queryTotal $
      gucResponse gucs status
        (contentTypeHeaders context ++ [contentRange])
        (if invMethod == ApiRequest.InvHead then mempty else toS body)


handleOpenApi :: Bool -> Types.Schema -> RequestContext -> DbHandler Wai.Response
handleOpenApi headersOnly tSchema (RequestContext conf dbStructure apiRequest _) =
  let
    encodeApi tables schemaDescription procs =
      OpenAPI.encodeOpenAPI
        (concat $ HashMap.elems procs)
        (fmap (openApiTableInfo dbStructure) tables)
        (openApiUri conf)
        schemaDescription
        (Types.dbPrimaryKeys dbStructure)
  in
  do
    body <-
      lift $ encodeApi
        <$> Hasql.statement tSchema DbStructure.accessibleTables
        <*> Hasql.statement tSchema DbStructure.schemaDescription
        <*> Hasql.statement tSchema DbStructure.accessibleProcs

    return $
      Wai.responseLBS HTTP.status200
        ([Types.toHeader Types.CTOpenAPI] ++ maybeToList (profileHeader apiRequest))
        (if headersOnly then mempty else toS body)



-- HELPERS


txMode :: ApiRequest -> Hasql.Mode
txMode apiRequest =
  case (ApiRequest.iAction apiRequest, ApiRequest.iTarget apiRequest) of
    (ApiRequest.ActionRead _, _) ->
      Hasql.Read

    (ApiRequest.ActionInfo, _) ->
      Hasql.Read

    (ApiRequest.ActionInspect _, _) ->
      Hasql.Read

    (ApiRequest.ActionInvoke ApiRequest.InvGet, _) ->
      Hasql.Read

    (ApiRequest.ActionInvoke ApiRequest.InvHead, _) ->
      Hasql.Read

    (ApiRequest.ActionInvoke ApiRequest.InvPost, ApiRequest.TargetProc Types.ProcDescription{Types.pdVolatility=Types.Stable} _) ->
      Hasql.Read

    (ApiRequest.ActionInvoke ApiRequest.InvPost, ApiRequest.TargetProc Types.ProcDescription{Types.pdVolatility=Types.Immutable} _) ->
      Hasql.Read

    _ ->
      Hasql.Write


data CreateResult =
  CreateResult
    { rQueryTotal :: Int64
    , rFields :: [ByteString]
    , rBody :: ByteString
    , rGucs :: Maybe ([Types.GucHeader], HTTP.Status)
    }


writeQuery
  :: QualifiedIdentifier
  -> Bool
  -> [Text]
  -> RequestContext
  -> DbHandler CreateResult
writeQuery identifier isInsert pkCols context =
  do
    readReq <- readRequest identifier context

    mutateReq <-
      liftEither $
        DbRequestBuilder.mutateRequest
          (Types.qiSchema identifier)
          (Types.qiName identifier)
          (rApiRequest context)
          (Types.tablePKCols
            (rDbStructure context)
            (Types.qiSchema identifier)
            (Types.qiName identifier)
          )
          readReq

    (_, queryTotal, fields, body, gucHeaders, gucStatus) <-
      lift . Hasql.statement mempty $
        Statements.createWriteStatement
          (QueryBuilder.readRequestToQuery readReq)
          (QueryBuilder.mutateRequestToQuery mutateReq)
          (rContentType context == Types.CTSingularJSON)
          isInsert
          (rContentType context == Types.CTTextCSV)
          (ApiRequest.iPreferRepresentation $ rApiRequest context)
          pkCols
          (Types.pgVersion $ rDbStructure context)
          (Config.configDbPreparedStatements $ rConfig context)

    gucs <-
      liftEither . mapLeft Error.errorResponseFor $
        maybeGucs <$> gucHeaders <*> gucStatus

    return $ CreateResult queryTotal fields body gucs


maybeGucs :: [Types.GucHeader] -> Maybe HTTP.Status -> Maybe ([Types.GucHeader], HTTP.Status)
maybeGucs headers status =
  (,) <$> pure headers <*> status


-- | Response with headers and status overridden from GUCs.
gucResponse
  :: Maybe ([Types.GucHeader], HTTP.Status)
  -> HTTP.Status
  -> [HTTP.Header]
  -> LazyByteString.ByteString
  -> Wai.Response
gucResponse mGucs status headers body =
  case mGucs of
    Just (gucHeaders, gucStatus) ->
      Wai.responseLBS gucStatus
        (Types.addHeadersIfNotIncluded headers (Types.unwrapGucHeader <$> gucHeaders))
        body

    Nothing ->
      Wai.responseLBS status headers body


-- |
-- Fail a response if a single JSON object was requested and not exactly one
-- was found.
failNotSingular :: ContentType -> Int64 -> Wai.Response -> DbHandler Wai.Response
failNotSingular contentType queryTotal response =
  if contentType == Types.CTSingularJSON && queryTotal /= 1 then
    do
      lift Hasql.condemn
      throwError . Error.errorResponseFor . Error.singularityError $ queryTotal
  else
    return response


shouldCount :: ApiRequest -> Bool
shouldCount apiRequest =
  ApiRequest.iPreferCount apiRequest == Just Types.ExactCount
  || ApiRequest.iPreferCount apiRequest == Just Types.EstimatedCount


returnsScalar :: ApiRequest -> Bool
returnsScalar apiRequest =
  case ApiRequest.iTarget apiRequest of
    ApiRequest.TargetProc proc _ ->
      Types.procReturnsScalar proc

    _ ->
      False


readRequest :: Monad m =>
  QualifiedIdentifier -> RequestContext -> Handler m Types.ReadRequest
readRequest identifier (RequestContext conf dbStructure apiRequest _) =
  liftEither $
    DbRequestBuilder.readRequest
      (Types.qiSchema identifier)
      (Types.qiName identifier)
      (Config.configDbMaxRows conf)
      (Types.dbRelations dbStructure)
      apiRequest


contentTypeHeaders :: RequestContext -> [HTTP.Header]
contentTypeHeaders (RequestContext _ _ apiRequest contentType) =
  [Types.toHeader contentType] ++ maybeToList (profileHeader apiRequest)


requestContentTypes :: AppConfig -> ApiRequest.ApiRequest -> [ContentType]
requestContentTypes conf apiRequest =
  case ApiRequest.iAction apiRequest of
    ApiRequest.ActionRead _ ->
      defaultContentTypes ++ rawContentTypes conf

    ApiRequest.ActionCreate ->
      defaultContentTypes

    ApiRequest.ActionUpdate ->
      defaultContentTypes

    ApiRequest.ActionDelete ->
      defaultContentTypes

    ApiRequest.ActionInvoke _ ->
      defaultContentTypes
        ++ rawContentTypes conf
        ++ [Types.CTOpenAPI | ApiRequest.tpIsRootSpec (ApiRequest.iTarget apiRequest)]

    ApiRequest.ActionInspect _ ->
      [Types.CTOpenAPI, Types.CTApplicationJSON]

    ApiRequest.ActionInfo ->
      [Types.CTTextCSV]

    ApiRequest.ActionSingleUpsert ->
      defaultContentTypes


defaultContentTypes :: [ContentType]
defaultContentTypes =
  [Types.CTApplicationJSON, Types.CTSingularJSON, Types.CTTextCSV]


-- |
-- If raw(binary) output is requested, check that ContentType is one of the admitted
-- rawContentTypes and that`?select=...` contains only one field other than `*`
binaryField :: Monad m =>
  RequestContext -> ReadRequest -> Handler m (Maybe Types.FieldName)
binaryField (RequestContext conf _ apiRequest contentType) readReq
  | returnsScalar apiRequest && contentType `elem` rawContentTypes conf =
      return $ Just "pgrst_scalar"
  | contentType `elem` rawContentTypes conf =
      let
        fldNames = Types.fstFieldNames readReq
        fieldName = headMay fldNames
      in
      if length fldNames == 1 && fieldName /= Just "*" then
        return fieldName
      else
        throwError . Error.errorResponseFor $ Error.BinaryFieldError contentType
  | otherwise =
      return Nothing


rawContentTypes :: AppConfig -> [ContentType]
rawContentTypes conf =
  List.union
    (Types.decodeContentType <$> Config.configRawMediaTypes conf)
    [Types.CTOctetStream, Types.CTTextPlain]


profileHeader :: ApiRequest -> Maybe HTTP.Header
profileHeader apiRequest =
  (,) <$> pure "Content-Profile" <*> (toS <$> ApiRequest.iProfile apiRequest)


contentTypeError :: ApiRequest -> Wai.Response
contentTypeError apiRequest =
  Error.errorResponseFor . Error.ContentTypeError $
    map Types.toMime (ApiRequest.iAccepts apiRequest)



-- MIDDLEWARE


-- |
-- Set a transaction to eventually roll back if requested and set respective
-- headers on the response.
optionalRollback
  :: AppConfig
  -> ApiRequest
  -> Hasql.Transaction Wai.Response
  -> Hasql.Transaction Wai.Response
optionalRollback conf apiRequest transaction =
  let
    shouldCommit =
      Config.configDbTxAllowOverride conf
      && ApiRequest.iPreferTransaction apiRequest == Just Types.Commit

    shouldRollback =
      Config.configDbTxAllowOverride conf
      && ApiRequest.iPreferTransaction apiRequest == Just Types.Rollback

    preferenceApplied
      | shouldCommit =
          Types.addHeadersIfNotIncluded
            [(HTTP.hPreferenceApplied, Char8ByteString.pack (show Types.Commit))]
      | shouldRollback =
          Types.addHeadersIfNotIncluded
            [(HTTP.hPreferenceApplied, Char8ByteString.pack (show Types.Rollback))]
      | otherwise =
          identity
  in
  do
    when (shouldRollback || (Config.configDbTxRollbackAll conf && not shouldCommit))
      Hasql.condemn

    Wai.mapResponseHeaders preferenceApplied <$> transaction


openApiUri :: AppConfig -> (Text, Text, Integer, Text)
openApiUri conf =
  case OpenAPI.pickProxy $ toS <$> Config.configOpenApiServerProxyUri conf of
    Just proxy ->
      ( Types.proxyScheme proxy
      , Types.proxyHost proxy
      , Types.proxyPort proxy
      , Types.proxyPath proxy
      )

    Nothing ->
      ("http"
      , Config.configServerHost conf
      , toInteger $ Config.configServerPort conf
      , "/"
      )


findTable :: DbStructure -> Types.QualifiedIdentifier -> Maybe Types.Table
findTable dbStructure identifier =
  find tableMatches (Types.dbTables dbStructure)
    where
      tableMatches table =
        Types.tableName table == Types.qiName identifier
        && Types.tableSchema table == Types.qiSchema identifier


splitKeyValue :: ByteString -> (ByteString, ByteString)
splitKeyValue kv =
  let
    (k, v) = Char8ByteString.break (== '=') kv
  in
    (k, Char8ByteString.tail v)


openApiTableInfo :: DbStructure -> Types.Table -> (Types.Table, [Types.Column], [Text])
openApiTableInfo dbStructure table =
  let
    schema = Types.tableSchema table
    name = Types.tableName table
  in
    ( table
    , Types.tableCols dbStructure schema name
    , Types.tablePKCols dbStructure schema name
    )
