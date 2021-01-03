-- | Module : PostgREST.App
--   Description : PostgREST main application
--
--   This module is in charge of mapping HTTP requests to PostgreSQL queries.
--
--   Some of its functionality includes:
--
--   - Mapping HTTP request methods to proper SQL statements. For example, a GET
--     request is translated to executing a SELECT query in a read-only TRANSACTION.
--   - Producing HTTP Headers according to RFCs.
--   - Content Negotiation
module PostgREST.App (postgrest, RequestContext(..)) where

import Data.IORef (IORef, readIORef)
import Data.Time.Clock (UTCTime)
import Data.Either.Combinators (mapLeft)
import Control.Monad.Except (liftEither)

import qualified Data.ByteString.Char8 as Char8ByteString
import qualified Data.ByteString.Lazy as LazyByteString
import qualified Data.HashMap.Strict as HashMap
import qualified Data.List as List (union)
import qualified Data.Set as Set
import qualified Hasql.Pool as Hasql
import qualified Hasql.Transaction as Hasql
import qualified Hasql.Transaction.Sessions as Hasql
import qualified Hasql.DynamicStatements.Snippet as Hasql
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
import PostgREST.Types (DbStructure, ContentType, QualifiedIdentifier)

import Protolude hiding (toS, Handler)
import Protolude.Conv (toS)



data RequestContext =
  RequestContext
    { rConfig :: AppConfig
    , rDbStructure :: DbStructure
    , rApiRequest :: ApiRequest
    , rContentType :: ContentType
    }


data CreateResult =
  CreateResult
    { rQueryTotal :: Int64
    , rFields :: [ByteString]
    , rBody :: ByteString
    , rGucHeaders :: Either Error.SimpleError [Types.GucHeader]
    , rGucStatus :: Either Error.SimpleError (Maybe HTTP.Status)
    }


type Handler =
  ExceptT Error


-- | To handle PostgREST requests, we need to query the database and handle
--   errors. With ExceptT, we combine the properties of Except with the ones of
--   Hasql.Transactions.
type DbHandler =
  Handler Hasql.Transaction


-- TODO: Replace Error with a type from the Error module, instead of opaque
-- Wai.Response.
type Error =
  Wai.Response


-- | PostgREST
postgrest
  :: Types.LogLevel
  -> IORef AppConfig
  -> IORef (Maybe DbStructure)
  -> Hasql.Pool
  -> IO UTCTime
  -> IO () -- ^ Connection worker that runs in a separate thread
  -> Wai.Application
postgrest logLev refConf refDbStructure pool getTime connWorker =
  Middleware.pgrstMiddleware logLev $
    \req respond ->
      do
        time <- getTime
        conf <- readIORef refConf
        maybeDbStructure <- readIORef refDbStructure

        response <-
          either errorToResponse identity <$>
            postgrestResponse conf maybeDbStructure pool time req

        -- Launch the connWorker when the connection is down.
        -- The postgrest function can respond successfully (with a stale schema
        -- cache) before the connWorker is done.
        when (Wai.responseStatus response == HTTP.status503) connWorker

        respond response


errorToResponse :: Error -> Wai.Response
errorToResponse =
  identity


postgrestResponse
  :: AppConfig
  -> Maybe DbStructure
  -> Hasql.Pool
  -> UTCTime
  -> Wai.Request
  -> IO (Either Error Wai.Response)
postgrestResponse conf maybeDbStructure pool time req =
  runExceptT $
    do
      body <- lift $ Wai.strictRequestBody req

      dbStructure <-
        liftEither $ maybeToRight
          (Error.errorResponseFor Error.ConnectionLostError)
          maybeDbStructure

      apiRequest <-
        liftEither . mapLeft Error.errorResponseFor $
          ApiRequest.userApiRequest
            (Config.configDbSchemas conf)
            (Config.configDbRootSpec conf)
            dbStructure
            req
            body

      -- The jwt must be checked before touching the db.
      rawClaims <-
        lift $ Auth.jwtClaims <$>
          Auth.attemptJwtClaims
            (Config.configJWKS conf)
            (Config.configJwtAudience conf)
            (toS $ ApiRequest.iJWT apiRequest)
            time
            (rightToMaybe $ Config.configJwtRoleClaimKey conf)

      claims <-
        liftEither $ mapLeft Error.errorResponseFor rawClaims

      contentType <-
        liftEither . maybeToRight (contentTypeError apiRequest) $
          ApiRequest.mutuallyAgreeable
            (requestContentTypes conf apiRequest)
            (ApiRequest.iAccepts apiRequest)

      dbResp <-
        lift $ Hasql.use pool $
          Hasql.transaction Hasql.ReadCommitted (txMode apiRequest) $
            optionalRollback conf apiRequest $
              Middleware.runPgLocals
                conf
                claims
                (\request -> either identity identity <$>
                  runExceptT (handleRequest (RequestContext conf dbStructure request contentType))
                )
                apiRequest

      liftEither $
        mapLeft (Error.errorResponseFor . Error.PgError (Auth.containsRole claims))
        dbResp


contentTypeError :: ApiRequest -> Wai.Response
contentTypeError apiRequest =
  Error.errorResponseFor . Error.ContentTypeError $
    map Types.toMime (ApiRequest.iAccepts apiRequest)


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

    ( ApiRequest.ActionInvoke ApiRequest.InvPost, ApiRequest.TargetProc Types.ProcDescription{Types.pdVolatility=Types.Stable} _) ->
      Hasql.Read

    ( ApiRequest.ActionInvoke ApiRequest.InvPost, ApiRequest.TargetProc Types.ProcDescription{Types.pdVolatility=Types.Immutable} _) ->
      Hasql.Read

    _ ->
      Hasql.Write


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
    req <- liftEither $ readRequest conf dbStructure identifier apiRequest

    let
      cq =
        QueryBuilder.readRequestToCountQuery req

      cQuery =
        if ApiRequest.iPreferCount apiRequest == Just Types.EstimatedCount then
          -- LIMIT maxRows + 1 so we can determine below that maxRows
          -- was surpassed
          QueryBuilder.limitedQuery cq ((+ 1) <$> Config.configDbMaxRows conf)
        else
          cq

    bField <- binaryField context req

    (tableTotal, queryTotal, _ , body, gucHeaders, gucStatus) <-
      lift $ Hasql.statement mempty $
        Statements.createReadStatement
          (QueryBuilder.readRequestToQuery req)
          cQuery
          (contentType == Types.CTSingularJSON)
          (shouldCount apiRequest)
          (contentType == Types.CTTextCSV)
          bField
          (Types.pgVersion dbStructure)
          (Config.configDbPreparedStatements conf)

    total <- readTotal conf apiRequest tableTotal cq

    let
      (rangeStatus, contentRange) =
        RangeQuery.rangeStatusHeader
          (ApiRequest.iTopLevelRange apiRequest)
          queryTotal
          total

      qString =
        ApiRequest.iCanonicalQS apiRequest

      headers =
        [ Types.toHeader contentType
        , contentRange
        , ( "Content-Location"
          , "/"
              <> toS (Types.qiName identifier)
              <> if Char8ByteString.null qString then
                   mempty
                 else
                   "?" <> toS qString
          )
        ] ++ maybeToList (profileHeader apiRequest)

    response <-
      liftEither . mapLeft Error.errorResponseFor $
        gucResponse rangeStatus headers (if headersOnly then mempty else toS body)
          <$> gucHeaders
          <*> gucStatus

    failNotSingular contentType queryTotal response


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


writeStatement
  :: QualifiedIdentifier
  -> Bool
  -> [Text]
  -> RequestContext
  -> DbHandler CreateResult
writeStatement identifier isInsert pkCols context =
  do
    sq <- readQuery identifier context
    mq <- mutateQuery identifier context

    (_, queryTotal, fields, body, gucHeaders, gucStatus) <-
      lift . Hasql.statement mempty $
        Statements.createWriteStatement sq mq
          (rContentType context == Types.CTSingularJSON)
          isInsert
          (rContentType context == Types.CTTextCSV)
          (ApiRequest.iPreferRepresentation $ rApiRequest context)
          pkCols
          (Types.pgVersion $ rDbStructure context)
          (Config.configDbPreparedStatements $ rConfig context)

    return $ CreateResult queryTotal fields body gucHeaders gucStatus


handleCreate :: QualifiedIdentifier -> RequestContext -> DbHandler Wai.Response
handleCreate identifier context@(RequestContext _ dbStructure apiRequest contentType) =
  let
    pkCols =
      Types.tablePKCols dbStructure (Types.qiSchema identifier) (Types.qiName identifier)
  in
  do
    result <- writeStatement identifier True pkCols context

    let
      ctHeaders =
        [Types.toHeader contentType] ++ maybeToList (profileHeader apiRequest)

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

      resp =
        if ApiRequest.iPreferRepresentation apiRequest == Types.Full then
          gucResponse HTTP.status201 (headers ++ ctHeaders) (toS $ rBody result)
        else
          gucResponse HTTP.status201 headers mempty

    response <-
      liftEither . mapLeft Error.errorResponseFor $
        resp <$> rGucHeaders result <*> rGucStatus result

    failNotSingular contentType (rQueryTotal result) response


handleUpdate :: QualifiedIdentifier -> RequestContext -> DbHandler Wai.Response
handleUpdate identifier context@(RequestContext _ _ apiRequest contentType) =
  do
    result <- writeStatement identifier False mempty context

    let
      fullRepr =
        ApiRequest.iPreferRepresentation apiRequest == Types.Full

      updateIsNoOp =
        Set.null (ApiRequest.iColumns apiRequest)

      defStatus
        | rQueryTotal result == 0 && not updateIsNoOp
            = HTTP.status404
        | fullRepr
            = HTTP.status200
        | otherwise
            = HTTP.status204

      contentRangeHeader =
        RangeQuery.contentRangeH
          0
          (rQueryTotal result - 1)
          (if shouldCount apiRequest then Just $ rQueryTotal result else Nothing)

      ctHeaders =
        [Types.toHeader contentType] ++ maybeToList (profileHeader apiRequest)

      fBody =
        if fullRepr then toS (rBody result) else mempty

      headers =
        (if fullRepr then ctHeaders else []) ++ [contentRangeHeader]

    response <-
      liftEither $ mapLeft Error.errorResponseFor $
        gucResponse defStatus headers fBody <$> rGucHeaders result <*> rGucStatus result

    failNotSingular contentType (rQueryTotal result) response


handleSingleUpsert :: QualifiedIdentifier -> RequestContext-> DbHandler Wai.Response
handleSingleUpsert identifier context@(RequestContext _ _ apiRequest contentType) =
  if ApiRequest.iTopLevelRange apiRequest /= RangeQuery.allRange then
    throwError $ Error.errorResponseFor Error.PutRangeNotAllowedError
  else
    do
      result <- writeStatement identifier False mempty context

      let
        headers =
          [Types.toHeader contentType] ++ maybeToList (profileHeader apiRequest)

        response =
          if ApiRequest.iPreferRepresentation apiRequest == Types.Full then
            gucResponse HTTP.status200 headers (toS $ rBody result)
          else
            gucResponse HTTP.status204 headers mempty

      -- Makes sure the querystring pk matches the payload pk
      -- e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
      -- PUT /items?id=eq.14 { "id" : 2, .. } is rejected
      -- If this condition is not satisfied then nothing is inserted,
      -- check the WHERE for INSERT in QueryBuilder.hs to see how it's done
      if rQueryTotal result /= 1 then
        do
          lift Hasql.condemn
          throwError $ Error.errorResponseFor Error.PutMatchingPkError
      else
        liftEither . mapLeft Error.errorResponseFor $
          response <$> rGucHeaders result <*> rGucStatus result


handleDelete :: QualifiedIdentifier -> RequestContext -> DbHandler Wai.Response
handleDelete identifier context@(RequestContext _ _ apiRequest contentType) =
  do
    result <- writeStatement identifier False mempty context

    let
      contentRangeHeader =
        RangeQuery.contentRangeH 1 0 $
          if shouldCount apiRequest then Just (rQueryTotal result) else Nothing

      ctHeaders =
        [Types.toHeader contentType] ++ maybeToList (profileHeader apiRequest)

      response =
        if ApiRequest.iPreferRepresentation apiRequest == Types.Full then
          gucResponse HTTP.status200 (ctHeaders ++ [contentRangeHeader]) (toS $ rBody result)
        else
          gucResponse HTTP.status204 [contentRangeHeader] mempty

    resp <-
      liftEither . mapLeft Error.errorResponseFor $
        response <$> rGucHeaders result <*> rGucStatus result

    failNotSingular contentType (rQueryTotal result) resp


handleInfo :: Monad m => QualifiedIdentifier -> DbStructure -> Handler m Wai.Response
handleInfo identifier dbStructure =
  let
    allowH table =
      ( HTTP.hAllow
      , if Types.tableInsertable table then
          "GET,POST,PATCH,DELETE"
        else
          "GET"
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
  in
  do
    req <- liftEither $ readRequest conf dbStructure identifier apiRequest
    bField <- binaryField context req

    (tableTotal, queryTotal, body, gucHeaders, gucStatus) <-
      lift $
        Hasql.statement mempty $
          Statements.callProcStatement
            (returnsScalar apiRequest)
            (returnsSingle apiRequest)
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

    let
      (rangeStatus, contentRange) =
        RangeQuery.rangeStatusHeader
          (ApiRequest.iTopLevelRange apiRequest)
          queryTotal
          tableTotal

      headers =
        [ Types.toHeader contentType
        , contentRange
        ]
        ++ maybeToList (profileHeader apiRequest)

      fBody =
        if invMethod == ApiRequest.InvHead then mempty else toS body

    response <-
      liftEither $ mapLeft Error.errorResponseFor $
        gucResponse rangeStatus headers fBody <$> gucHeaders <*> gucStatus

    failNotSingular contentType queryTotal response


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
      Wai.responseLBS
        HTTP.status200
        (catMaybes [Just $ Types.toHeader Types.CTOpenAPI, profileHeader apiRequest])
        (if headersOnly then mempty else toS body)



-- HELPERS


-- | Response with overrides from GUCs.
gucResponse
  :: HTTP.Status
  -> [HTTP.Header]
  -> LazyByteString.ByteString
  -> [Types.GucHeader]
  -> Maybe HTTP.Status
  -> Wai.Response
gucResponse status headers body ghdrs gstatus =
  Wai.responseLBS
    (fromMaybe status gstatus)
    (Types.addHeadersIfNotIncluded headers (Types.unwrapGucHeader <$> ghdrs))
    body


-- | Fail a response if a single JSON object was requested and not exactly one
--   was found.
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


returnsSingle :: ApiRequest -> Bool
returnsSingle apiRequest =
  case ApiRequest.iTarget apiRequest of
    ApiRequest.TargetProc proc _ ->
      Types.procReturnsSingle proc
    _ ->
      False


readRequest
  :: AppConfig
  -> DbStructure
  -> Types.QualifiedIdentifier
  -> ApiRequest
  -> Either Wai.Response Types.ReadRequest
readRequest conf dbStructure identifier =
  DbRequestBuilder.readRequest
    (Types.qiSchema identifier)
    (Types.qiName identifier)
    (Config.configDbMaxRows conf)
    (Types.dbRelations dbStructure)


readQuery :: Monad m => QualifiedIdentifier -> RequestContext -> Handler m Hasql.Snippet
readQuery identifier (RequestContext conf dbStructure apiRequest _) =
  liftEither $
    QueryBuilder.readRequestToQuery <$>
      readRequest conf dbStructure identifier apiRequest


mutateQuery :: Monad m => QualifiedIdentifier -> RequestContext -> Handler m Hasql.Snippet
mutateQuery identifier (RequestContext conf dbStructure apiRequest _) =
  liftEither $
    QueryBuilder.mutateRequestToQuery <$>
      (DbRequestBuilder.mutateRequest
        (Types.qiSchema identifier)
        (Types.qiName identifier)
        apiRequest
        (Types.tablePKCols dbStructure (Types.qiSchema identifier) (Types.qiName identifier))
        =<< readRequest conf dbStructure identifier apiRequest
      )



-- CONTENT TYPES


rawContentTypes :: AppConfig -> [ContentType]
rawContentTypes conf =
  List.union
    (Types.decodeContentType <$> Config.configRawMediaTypes conf)
    [ Types.CTOctetStream, Types.CTTextPlain ]


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


-- | If raw(binary) output is requested, check that ContentType is one of the admitted
--   rawContentTypes and that`?select=...` contains only one field other than `*`
binaryField :: Monad m => RequestContext -> Types.ReadRequest -> Handler m (Maybe Types.FieldName)
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



-- HEADERS


profileHeader :: ApiRequest -> Maybe HTTP.Header
profileHeader apiRequest =
  contentProfileH <$> ApiRequest.iProfile apiRequest
  where
    contentProfileH schema =
      ("Content-Profile", toS schema)



-- MIDDLEWARE


-- | Set a transaction to eventually roll back if requested and set respective
--   headers on the response.
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



-- CONFIG


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


-- TYPES helpers


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
    (k, v) =
      Char8ByteString.break (== '=') kv
  in
    (k, Char8ByteString.tail v)


-- TODO: Obsolete when Table and Column types are refactored
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
