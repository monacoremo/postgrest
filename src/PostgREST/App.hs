-- |
-- Module : PostgREST.App
-- Description : PostgREST main application
--
-- This module is in charge of mapping HTTP requests to PostgreSQL queries.
--
-- Some of its functionality includes:
--
-- - Mapping HTTP request methods to proper SQL statements. For example, a GET
--   request is translated to executing a SELECT query in a read-only transaction
-- - Producing HTTP Headers according to RFCs
-- - Content type negotiation
module PostgREST.App (postgrest) where

import Control.Monad.Except    (liftEither)
import Data.Either.Combinators (mapLeft)
import Data.IORef              (IORef, readIORef)
import Data.List               (union)
import Data.Time.Clock         (UTCTime)

import qualified Data.ByteString.Char8           as BS8
import qualified Data.ByteString.Lazy            as LazyBS
import qualified Data.Set                        as Set
import qualified Hasql.DynamicStatements.Snippet as SQL
import qualified Hasql.Pool                      as SQL
import qualified Hasql.Transaction               as SQL
import qualified Hasql.Transaction.Sessions      as SQL
import qualified Network.HTTP.Types.Header       as HTTP
import qualified Network.HTTP.Types.Status       as HTTP
import qualified Network.HTTP.Types.URI          as HTTP
import qualified Network.Wai                     as Wai

import qualified PostgREST.ApiRequest       as Req
import qualified PostgREST.Auth             as Auth
import qualified PostgREST.Config           as Config
import qualified PostgREST.DbRequestBuilder as ReqBuilder
import qualified PostgREST.DbStructure      as DbStructure
import qualified PostgREST.Error            as Error
import qualified PostgREST.Middleware       as Middleware
import qualified PostgREST.OpenAPI          as OpenAPI
import qualified PostgREST.QueryBuilder     as QueryBuilder
import qualified PostgREST.RangeQuery       as RangeQuery
import qualified PostgREST.Statements       as Statements
import qualified PostgREST.Types            as Types

import PostgREST.ApiRequest (ApiRequest)
import PostgREST.Config     (AppConfig)
import PostgREST.Error      (Error)
import PostgREST.Types      (ContentType, DbStructure,
                             QualifiedIdentifier, ReadRequest)

import Protolude      hiding (Handler, toS)
import Protolude.Conv (toS)


data RequestContext =
  RequestContext
    { rConfig      :: AppConfig
    , rDbStructure :: DbStructure
    , rApiRequest  :: ApiRequest
    , rContentType :: ContentType
    }

type Handler = ExceptT Error

type DbHandler = Handler SQL.Transaction

-- | PostgREST application
postgrest
  :: Types.LogLevel
  -> IORef AppConfig
  -> IORef (Maybe DbStructure)
  -> SQL.Pool
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

        response <- either Error.errorResponseFor identity <$> eitherResponse

        -- Launch the connWorker when the connection is down.  The postgrest
        -- function can respond successfully (with a stale schema cache) before
        -- the connWorker is done.
        when (Wai.responseStatus response == HTTP.status503) connWorker

        respond response

postgrestResponse
  :: AppConfig
  -> Maybe DbStructure
  -> SQL.Pool
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
          throwError Error.ConnectionLostError

    apiRequest <-
      liftEither . mapLeft Error.ApiRequestError $
        Req.userApiRequest
          (Config.configDbSchemas conf)
          (Config.configDbRootSpec conf)
          dbStructure
          req
          body

    -- The JWT must be checked before touching the db
    jwtClaims <- Auth.jwtClaims conf (toS $ Req.iJWT apiRequest) time

    let
      reqContentTypes = requestContentTypes conf apiRequest
      acceptContentType = Req.iAccepts apiRequest

    contentType <-
      case Req.mutuallyAgreeable reqContentTypes acceptContentType of
        Just ct ->
          return ct

        Nothing ->
          throwError . Error.ContentTypeError $ map Types.toMime acceptContentType

    let
      context apiReq =
        RequestContext conf dbStructure apiReq contentType

      handleReq apiReq =
        handleRequest (context apiReq)

    runDbHandler pool (txMode apiRequest) jwtClaims $
      Middleware.optionalRollback conf apiRequest $
        Middleware.runPgLocals conf jwtClaims handleReq apiRequest

runDbHandler :: SQL.Pool -> SQL.Mode -> Auth.JWTClaims -> DbHandler a -> Handler IO a
runDbHandler pool mode jwtClaims handler =
  do
    dbResp <-
      lift . SQL.use pool . SQL.transaction SQL.ReadCommitted mode $
        runExceptT handler

    resp <-
      liftEither . mapLeft Error.PgErr $
        mapLeft (Error.PgError $ Auth.containsRole jwtClaims) dbResp

    liftEither resp

handleRequest :: RequestContext -> DbHandler Wai.Response
handleRequest context@(RequestContext _ dbStructure apiRequest _) =
  case (Req.iAction apiRequest, Req.iTarget apiRequest) of
    (Req.ActionRead headersOnly, Req.TargetIdent identifier) ->
      handleRead headersOnly identifier context
    (Req.ActionCreate, Req.TargetIdent identifier) ->
      handleCreate identifier context
    (Req.ActionUpdate, Req.TargetIdent identifier) ->
      handleUpdate identifier context
    (Req.ActionSingleUpsert, Req.TargetIdent identifier) ->
      handleSingleUpsert identifier context
    (Req.ActionDelete, Req.TargetIdent identifier) ->
      handleDelete identifier context
    (Req.ActionInfo, Req.TargetIdent identifier) ->
      handleInfo identifier dbStructure
    (Req.ActionInvoke invMethod, Req.TargetProc proc _) ->
      handleInvoke invMethod proc context
    (Req.ActionInspect headersOnly, Req.TargetDefaultSpec tSchema) ->
      handleOpenApi headersOnly tSchema context
    _ ->
      throwError Error.NotFound

handleRead :: Bool -> QualifiedIdentifier -> RequestContext -> DbHandler Wai.Response
handleRead headersOnly identifier context@(RequestContext conf dbStructure apiRequest contentType) =
  do
    req <- readRequest identifier context
    bField <- binaryField context req

    let
      countQuery =
        QueryBuilder.readRequestToCountQuery req

    (tableTotal, queryTotal, _ , body, gucHeaders, gucStatus) <-
      lift $ SQL.statement mempty $
        Statements.createReadStatement
          (QueryBuilder.readRequestToQuery req)
          ( if Req.iPreferCount apiRequest == Just Types.EstimatedCount then
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

    response <-
      liftEither $ gucResponse <$> gucStatus <*> gucHeaders

    let
      (status, contentRange) =
        RangeQuery.rangeStatusHeader
          (Req.iTopLevelRange apiRequest)
          queryTotal
          total

      qString =
        Req.iCanonicalQS apiRequest

      headers =
        [ contentRange
        , ( "Content-Location"
          , "/"
              <> toS (Types.qiName identifier)
              <> if BS8.null qString then
                   mempty
                 else
                   "?" <> toS qString
          )
        ]
        ++ contentTypeHeaders context

    failNotSingular contentType queryTotal . response status headers $
      if headersOnly then mempty else toS body

readTotal
  :: AppConfig
  -> ApiRequest
  -> Maybe Int64
  -> SQL.Snippet
  -> DbHandler (Maybe Int64)
readTotal conf apiRequest tableTotal countQuery =
  case Req.iPreferCount apiRequest of
    Just Types.PlannedCount ->
      explain

    Just Types.EstimatedCount ->
      if tableTotal > (fromIntegral <$> Config.configDbMaxRows conf) then
        max tableTotal <$> explain
      else
        return tableTotal

    _ ->
      return tableTotal
  where
    explain =
      lift . SQL.statement mempty . Statements.createExplainStatement countQuery $
        Config.configDbPreparedStatements conf

handleCreate :: QualifiedIdentifier -> RequestContext -> DbHandler Wai.Response
handleCreate identifier context@(RequestContext _ dbStructure apiRequest contentType) =
  let
    pkCols =
      Types.tablePKCols dbStructure (Types.qiSchema identifier) (Types.qiName identifier)
  in
  do
    result <- writeQuery identifier True pkCols context

    let
      response =
        gucResponse (rGucStatus result) (rGucHeaders result)

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
          , if null pkCols && isNothing (Req.iOnConflict apiRequest) then
              Nothing
            else
              (\x -> ("Preference-Applied", BS8.pack (show x))) <$>
                Req.iPreferResolution apiRequest
          ]

    failNotSingular contentType (rQueryTotal result) $
      if Req.iPreferRepresentation apiRequest == Types.Full then
        response HTTP.status201
          (headers ++ contentTypeHeaders context)
          (toS $ rBody result)
      else
        response HTTP.status201 headers mempty

handleUpdate :: QualifiedIdentifier -> RequestContext -> DbHandler Wai.Response
handleUpdate identifier context@(RequestContext _ _ apiRequest contentType) =
  do
    result <- writeQuery identifier False mempty context

    let
      response =
        gucResponse (rGucStatus result) (rGucHeaders result)

      fullRepr =
        Req.iPreferRepresentation apiRequest == Types.Full

      updateIsNoOp =
        Set.null (Req.iColumns apiRequest)

      status
        | rQueryTotal result == 0 && not updateIsNoOp = HTTP.status404
        | fullRepr = HTTP.status200
        | otherwise = HTTP.status204

      contentRangeHeader =
        RangeQuery.contentRangeH 0 (rQueryTotal result - 1)
          (if shouldCount apiRequest then Just $ rQueryTotal result else Nothing)

    failNotSingular contentType (rQueryTotal result) $
      if fullRepr then
        response status
          (contentTypeHeaders context ++ [contentRangeHeader])
          (toS $ rBody result)
      else
        response status [contentRangeHeader] mempty

handleSingleUpsert :: QualifiedIdentifier -> RequestContext-> DbHandler Wai.Response
handleSingleUpsert identifier context@(RequestContext _ _ apiRequest _) =
  if Req.iTopLevelRange apiRequest /= RangeQuery.allRange then
    throwError Error.PutRangeNotAllowedError
  else
    do
      result <- writeQuery identifier False mempty context

      let response = gucResponse (rGucStatus result) (rGucHeaders result)

      -- Makes sure the querystring pk matches the payload pk
      -- e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
      -- PUT /items?id=eq.14 { "id" : 2, .. } is rejected.
      -- If this condition is not satisfied then nothing is inserted,
      -- check the WHERE for INSERT in QueryBuilder.hs to see how it's done
      if rQueryTotal result /= 1 then
        do
          lift SQL.condemn
          throwError Error.PutMatchingPkError
      else
        return $
          if Req.iPreferRepresentation apiRequest == Types.Full then
            response HTTP.status200 (contentTypeHeaders context) (toS $ rBody result)
          else
            response HTTP.status204 (contentTypeHeaders context) mempty

handleDelete :: QualifiedIdentifier -> RequestContext -> DbHandler Wai.Response
handleDelete identifier context@(RequestContext _ _ apiRequest contentType) =
  do
    result <- writeQuery identifier False mempty context

    let
      response =
        gucResponse (rGucStatus result) (rGucHeaders result)

      contentRangeHeader =
        RangeQuery.contentRangeH 1 0 $
          if shouldCount apiRequest then Just (rQueryTotal result) else Nothing

    failNotSingular contentType (rQueryTotal result) $
      if Req.iPreferRepresentation apiRequest == Types.Full then
        response HTTP.status200
          (contentTypeHeaders context ++ [contentRangeHeader])
          (toS $ rBody result)
      else
        response HTTP.status204 [contentRangeHeader] mempty

handleInfo :: Monad m => QualifiedIdentifier -> DbStructure -> Handler m Wai.Response
handleInfo identifier dbStructure =
  let
    allowH table =
      ( HTTP.hAllow
      , if Types.tableInsertable table then "GET,POST,PATCH,DELETE" else "GET"
      )

    allOrigins =
      ("Access-Control-Allow-Origin", "*")

    tableMatches table =
      Types.tableName table == Types.qiName identifier
      && Types.tableSchema table == Types.qiSchema identifier
  in
  case find tableMatches (Types.dbTables dbStructure) of
    Just table ->
      return $ Wai.responseLBS HTTP.status200 [allOrigins, allowH table] mempty

    Nothing ->
      throwError Error.NotFound

handleInvoke
  :: Req.InvokeMethod
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
      case Req.iTarget apiRequest of
        Req.TargetProc targetProc _ ->
          Types.procReturnsSingle targetProc
        _ ->
          False
  in
  do
    req <- readRequest identifier context
    bField <- binaryField context req

    (tableTotal, queryTotal, body, gucHeaders, gucStatus) <-
      lift . SQL.statement mempty $
        Statements.callProcStatement
          (returnsScalar apiRequest)
          returnsSingle
          (QueryBuilder.requestToCallProcQuery
            (Types.QualifiedIdentifier (Types.pdSchema proc) (Types.pdName proc))
            (Types.specifiedProcArgs (Req.iColumns apiRequest) proc)
            (Req.iPayload apiRequest)
            (returnsScalar apiRequest)
            (Req.iPreferParameters apiRequest)
            (ReqBuilder.returningCols req [])
          )
          (QueryBuilder.readRequestToQuery req)
          (QueryBuilder.readRequestToCountQuery req)
          (shouldCount apiRequest)
          (contentType == Types.CTSingularJSON)
          (contentType == Types.CTTextCSV)
          (Req.iPreferParameters apiRequest == Just Types.MultipleObjects)
          bField
          (Types.pgVersion dbStructure)
          (Config.configDbPreparedStatements conf)

    response <-
      liftEither $ gucResponse <$> gucStatus <*> gucHeaders

    let
      (status, contentRange) =
        RangeQuery.rangeStatusHeader
          (Req.iTopLevelRange apiRequest)
          queryTotal
          tableTotal

    failNotSingular contentType queryTotal $
      response status
        (contentTypeHeaders context ++ [contentRange])
        (if invMethod == Req.InvHead then mempty else toS body)

handleOpenApi :: Bool -> Types.Schema -> RequestContext -> DbHandler Wai.Response
handleOpenApi headersOnly tSchema (RequestContext conf dbStructure apiRequest _) =
  let
    prepared =
      Config.configDbPreparedStatements conf
  in
  do
    body <-
      lift $
        OpenAPI.encode conf dbStructure
          <$> SQL.statement tSchema (DbStructure.accessibleTables prepared)
          <*> SQL.statement tSchema (DbStructure.schemaDescription prepared)
          <*> SQL.statement tSchema (DbStructure.accessibleProcs prepared)

    return $
      Wai.responseLBS HTTP.status200
        (Types.toHeader Types.CTOpenAPI : maybeToList (profileHeader apiRequest))
        (if headersOnly then mempty else toS body)

txMode :: ApiRequest -> SQL.Mode
txMode apiRequest =
  case (Req.iAction apiRequest, Req.iTarget apiRequest) of
    (Req.ActionRead _, _) ->
      SQL.Read
    (Req.ActionInfo, _) ->
      SQL.Read
    (Req.ActionInspect _, _) ->
      SQL.Read
    (Req.ActionInvoke Req.InvGet, _) ->
      SQL.Read
    (Req.ActionInvoke Req.InvHead, _) ->
      SQL.Read
    (Req.ActionInvoke Req.InvPost, Req.TargetProc Types.ProcDescription{Types.pdVolatility=Types.Stable} _) ->
      SQL.Read
    (Req.ActionInvoke Req.InvPost, Req.TargetProc Types.ProcDescription{Types.pdVolatility=Types.Immutable} _) ->
      SQL.Read
    _ ->
      SQL.Write

data CreateResult =
  CreateResult
    { rQueryTotal :: Int64
    , rFields     :: [ByteString]
    , rBody       :: ByteString
    , rGucStatus  :: Maybe HTTP.Status
    , rGucHeaders :: [Types.GucHeader]
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
        ReqBuilder.mutateRequest
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
      lift . SQL.statement mempty $
        Statements.createWriteStatement
          (QueryBuilder.readRequestToQuery readReq)
          (QueryBuilder.mutateRequestToQuery mutateReq)
          (rContentType context == Types.CTSingularJSON)
          isInsert
          (rContentType context == Types.CTTextCSV)
          (Req.iPreferRepresentation $ rApiRequest context)
          pkCols
          (Types.pgVersion $ rDbStructure context)
          (Config.configDbPreparedStatements $ rConfig context)

    liftEither $
      CreateResult queryTotal fields body <$> gucStatus <*> gucHeaders

-- | Response with headers and status overridden from GUCs.
gucResponse
  :: Maybe HTTP.Status
  -> [Types.GucHeader]
  -> HTTP.Status
  -> [HTTP.Header]
  -> LazyBS.ByteString
  -> Wai.Response
gucResponse gucStatus gucHeaders status headers =
  Wai.responseLBS (fromMaybe status gucStatus)
    (Types.addHeadersIfNotIncluded headers (map Types.unwrapGucHeader gucHeaders))

-- |
-- Fail a response if a single JSON object was requested and not exactly one
-- was found.
failNotSingular :: ContentType -> Int64 -> Wai.Response -> DbHandler Wai.Response
failNotSingular contentType queryTotal response =
  if contentType == Types.CTSingularJSON && queryTotal /= 1 then
    do
      lift SQL.condemn
      throwError $ Error.singularityError queryTotal
  else
    return response

shouldCount :: ApiRequest -> Bool
shouldCount apiRequest =
  Req.iPreferCount apiRequest == Just Types.ExactCount
  || Req.iPreferCount apiRequest == Just Types.EstimatedCount

returnsScalar :: ApiRequest -> Bool
returnsScalar apiRequest =
  case Req.iTarget apiRequest of
    Req.TargetProc proc _ ->
      Types.procReturnsScalar proc

    _ ->
      False

readRequest :: Monad m =>
  QualifiedIdentifier -> RequestContext -> Handler m Types.ReadRequest
readRequest identifier (RequestContext conf dbStructure apiRequest _) =
  liftEither $
    ReqBuilder.readRequest
      (Types.qiSchema identifier)
      (Types.qiName identifier)
      (Config.configDbMaxRows conf)
      (Types.dbRelations dbStructure)
      apiRequest

contentTypeHeaders :: RequestContext -> [HTTP.Header]
contentTypeHeaders (RequestContext _ _ apiRequest contentType) =
  Types.toHeader contentType : maybeToList (profileHeader apiRequest)

requestContentTypes :: AppConfig -> Req.ApiRequest -> [ContentType]
requestContentTypes conf apiRequest =
  case Req.iAction apiRequest of
    Req.ActionRead _ ->
      defaultContentTypes ++ rawContentTypes conf
    Req.ActionCreate ->
      defaultContentTypes
    Req.ActionUpdate ->
      defaultContentTypes
    Req.ActionDelete ->
      defaultContentTypes
    Req.ActionInvoke _ ->
      defaultContentTypes
        ++ rawContentTypes conf
        ++ [Types.CTOpenAPI | Req.tpIsRootSpec (Req.iTarget apiRequest)]
    Req.ActionInspect _ ->
      [Types.CTOpenAPI, Types.CTApplicationJSON]
    Req.ActionInfo ->
      [Types.CTTextCSV]
    Req.ActionSingleUpsert ->
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
        throwError $ Error.BinaryFieldError contentType
  | otherwise =
      return Nothing

rawContentTypes :: AppConfig -> [ContentType]
rawContentTypes conf =
  (Types.decodeContentType <$> Config.configRawMediaTypes conf)
    `union` [Types.CTOctetStream, Types.CTTextPlain]

profileHeader :: ApiRequest -> Maybe HTTP.Header
profileHeader apiRequest =
  (,) "Content-Profile" <$> (toS <$> Req.iProfile apiRequest)

splitKeyValue :: ByteString -> (ByteString, ByteString)
splitKeyValue kv =
  (k, BS8.tail v)
  where
    (k, v) = BS8.break (== '=') kv
