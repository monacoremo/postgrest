{-|
Module      : PostgREST.App
Description : PostgREST main application

This module is in charge of mapping HTTP requests to PostgreSQL queries.
Some of its functionality includes:

- Mapping HTTP request methods to proper SQL statements. For example, a GET request is translated to executing a SELECT query in a read-only TRANSACTION.
- Producing HTTP Headers according to RFCs.
- Content Negotiation
-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE ScopedTypeVariables #-}

module PostgREST.App (postgrest) where

import qualified Data.ByteString.Char8      as BS
import qualified Data.HashMap.Strict        as HashMap
import qualified Data.List                  as List (union)
import qualified Data.Set                   as Set
import qualified Hasql.Pool                 as Hasql
import qualified Hasql.Transaction          as Hasql
import qualified Hasql.Transaction.Sessions as Hasql

import Data.IORef             (IORef, readIORef)
import Data.Time.Clock        (UTCTime)
import Network.HTTP.Types.URI (renderSimpleQuery)

import qualified Network.HTTP.Types.Header as HTTP
import qualified Network.HTTP.Types.Status as HTTP
import qualified Network.Wai as Wai

import qualified PostgREST.ApiRequest as ApiRequest
import qualified PostgREST.Auth as Auth
import qualified PostgREST.Config as Config
import PostgREST.DbRequestBuilder (mutateRequest, readRequest,
                                   returningCols)
import PostgREST.DbStructure
import PostgREST.Error            (PgError (..), SimpleError (..),
                                   errorResponseFor, singularityError)
import PostgREST.Middleware
import PostgREST.OpenAPI
import PostgREST.QueryBuilder     (limitedQuery, mutateRequestToQuery,
                                   readRequestToCountQuery,
                                   readRequestToQuery,
                                   requestToCallProcQuery)
import PostgREST.RangeQuery       (allRange, contentRangeH,
                                   rangeStatusHeader)
import PostgREST.Statements       (callProcStatement,
                                   createExplainStatement,
                                   createReadStatement,
                                   createWriteStatement)
import PostgREST.Types
import Protolude                  hiding (Proxy, intercalate, toS)
import Protolude.Conv             (toS)

postgrest ::
  LogLevel
  -> IORef Config.AppConfig
  -> IORef (Maybe DbStructure)
  -> Hasql.Pool
  -> IO UTCTime
  -> IO ()
  -> Wai.Application
postgrest logLev refConf refDbStructure pool getTime connWorker =
  pgrstMiddleware logLev $ \ req respond -> do
    time <- getTime
    body <- Wai.strictRequestBody req
    maybeDbStructure <- readIORef refDbStructure
    conf <- readIORef refConf
    case maybeDbStructure of
      Nothing -> respond . errorResponseFor $ ConnectionLostError
      Just dbStructure -> do
        response <- do
          let
            apiReq =
              ApiRequest.userApiRequest
                (Config.configDbSchemas conf)
                (Config.configDbRootSpec conf)
                dbStructure
                req
                body
          case apiReq of
            Left err -> return . errorResponseFor $ err
            Right apiRequest -> do
              -- The jwt must be checked before touching the db.
              attempt <-
                Auth.attemptJwtClaims
                  (Config.configJWKS conf)
                  (Config.configJwtAudience conf)
                  (toS $ ApiRequest.iJWT apiRequest)
                  time
                  (rightToMaybe $ Config.configJwtRoleClaimKey conf)
              case Auth.jwtClaims attempt of
                Left errJwt -> return . errorResponseFor $ errJwt
                Right claims -> do
                  let
                    authed = Auth.containsRole claims
                    shouldCommit   = Config.configDbTxAllowOverride conf && ApiRequest.iPreferTransaction apiRequest == Just Commit
                    shouldRollback = Config.configDbTxAllowOverride conf && ApiRequest.iPreferTransaction apiRequest == Just Rollback
                    preferenceApplied
                      | shouldCommit    = addHeadersIfNotIncluded [(HTTP.hPreferenceApplied, BS.pack (show Commit))]
                      | shouldRollback  = addHeadersIfNotIncluded [(HTTP.hPreferenceApplied, BS.pack (show Rollback))]
                      | otherwise       = identity
                    handleReq = do
                      when (shouldRollback || (Config.configDbTxRollbackAll conf && not shouldCommit)) Hasql.condemn
                      Wai.mapResponseHeaders preferenceApplied <$> runPgLocals conf claims (app dbStructure conf) apiRequest
                  dbResp <- Hasql.use pool $ Hasql.transaction Hasql.ReadCommitted (txMode apiRequest) handleReq
                  return $ either (errorResponseFor . PgError authed) identity dbResp
        -- Launch the connWorker when the connection is down. The postgrest function can respond successfully(with a stale schema cache) before the connWorker is done.
        when (Wai.responseStatus response == HTTP.status503) connWorker
        respond response

txMode :: ApiRequest.ApiRequest -> Hasql.Mode
txMode apiRequest =
  case (ApiRequest.iAction apiRequest, ApiRequest.iTarget apiRequest) of
    (ApiRequest.ActionRead _        , _) -> Hasql.Read
    (ApiRequest.ActionInfo          , _) -> Hasql.Read
    (ApiRequest.ActionInspect _     , _) -> Hasql.Read
    (ApiRequest.ActionInvoke ApiRequest.InvGet , _) -> Hasql.Read
    (ApiRequest.ActionInvoke ApiRequest.InvHead, _) -> Hasql.Read
    (ApiRequest.ActionInvoke ApiRequest.InvPost, ApiRequest.TargetProc ProcDescription{pdVolatility=Stable} _)    -> Hasql.Read
    (ApiRequest.ActionInvoke ApiRequest.InvPost, ApiRequest.TargetProc ProcDescription{pdVolatility=Immutable} _) -> Hasql.Read
    _ -> Hasql.Write

app :: DbStructure -> Config.AppConfig -> ApiRequest.ApiRequest -> Hasql.Transaction Wai.Response
app dbStructure conf apiRequest =
  let rawContentTypes = (decodeContentType <$> Config.configRawMediaTypes conf) `List.union` [ CTOctetStream, CTTextPlain ] in
  case responseContentTypeOrError (ApiRequest.iAccepts apiRequest) rawContentTypes (ApiRequest.iAction apiRequest) (ApiRequest.iTarget apiRequest) of
    Left errorResponse -> return errorResponse
    Right contentType ->
      case (ApiRequest.iAction apiRequest, ApiRequest.iTarget apiRequest) of

        (ApiRequest.ActionRead headersOnly, ApiRequest.TargetIdent (QualifiedIdentifier tSchema tName)) ->
          case readSqlParts tSchema tName of
            Left errorResponse -> return errorResponse
            Right (q, cq, bField, _) -> do
              let cQuery = if estimatedCount
                             then limitedQuery cq ((+ 1) <$> maxRows) -- LIMIT maxRows + 1 so we can determine below that maxRows was surpassed
                             else cq
                  stm = createReadStatement q cQuery (contentType == CTSingularJSON) shouldCount
                        (contentType == CTTextCSV) bField pgVer prepared
                  explStm = createExplainStatement cq prepared
              row <- Hasql.statement mempty stm
              let (tableTotal, queryTotal, _ , body, gucHeaders, gucStatus) = row
                  gucs =  (,) <$> gucHeaders <*> gucStatus
              case gucs of
                Left err -> return $ errorResponseFor err
                Right (ghdrs, gstatus) -> do
                  total <- if | plannedCount   -> Hasql.statement () explStm
                              | estimatedCount -> if tableTotal > (fromIntegral <$> maxRows)
                                                    then do estTotal <- Hasql.statement () explStm
                                                            pure $ if estTotal > tableTotal then estTotal else tableTotal
                                                    else pure tableTotal
                              | otherwise      -> pure tableTotal
                  let (rangeStatus, contentRange) = rangeStatusHeader topLevelRange queryTotal total
                      status = fromMaybe rangeStatus gstatus
                      headers = addHeadersIfNotIncluded (catMaybes [
                                  Just $ toHeader contentType, Just contentRange,
                                  Just $ contentLocationH tName (ApiRequest.iCanonicalQS apiRequest), profileH])
                                (unwrapGucHeader <$> ghdrs)
                      rBody = if headersOnly then mempty else toS body
                  return $
                    if contentType == CTSingularJSON && queryTotal /= 1
                      then errorResponseFor . singularityError $ queryTotal
                      else Wai.responseLBS status headers rBody

        (ApiRequest.ActionCreate, ApiRequest.TargetIdent (QualifiedIdentifier tSchema tName)) ->
          case mutateSqlParts tSchema tName of
            Left errorResponse -> return errorResponse
            Right (sq, mq) -> do
              let pkCols = tablePKCols dbStructure tSchema tName
                  stm = createWriteStatement sq mq
                    (contentType == CTSingularJSON) True
                    (contentType == CTTextCSV) (ApiRequest.iPreferRepresentation apiRequest) pkCols pgVer prepared
              row <- Hasql.statement mempty stm
              let (_, queryTotal, fields, body, gucHeaders, gucStatus) = row
                  gucs =  (,) <$> gucHeaders <*> gucStatus
              case gucs of
                Left err -> return $ errorResponseFor err
                Right (ghdrs, gstatus) -> do
                  let
                    (ctHeaders, rBody) = if ApiRequest.iPreferRepresentation apiRequest == Full
                                          then ([Just $ toHeader contentType, profileH], toS body)
                                          else ([], mempty)
                    status = fromMaybe HTTP.status201 gstatus
                    headers = addHeadersIfNotIncluded (catMaybes ([
                          if null fields
                            then Nothing
                            else Just $ locationH tName fields
                        , Just $ contentRangeH 1 0 $ if shouldCount then Just queryTotal else Nothing
                        , if null pkCols && isNothing (ApiRequest.iOnConflict apiRequest)
                            then Nothing
                            else (\x -> ("Preference-Applied", BS.pack (show x))) <$> ApiRequest.iPreferResolution apiRequest
                        ] ++ ctHeaders)) (unwrapGucHeader <$> ghdrs)
                  if contentType == CTSingularJSON && queryTotal /= 1
                    then do
                      Hasql.condemn
                      return . errorResponseFor . singularityError $ queryTotal
                    else
                      return $ Wai.responseLBS status headers rBody

        (ApiRequest.ActionUpdate, ApiRequest.TargetIdent (QualifiedIdentifier tSchema tName)) ->
          case mutateSqlParts tSchema tName of
            Left errorResponse -> return errorResponse
            Right (sq, mq) -> do
              row <- Hasql.statement mempty $
                     createWriteStatement sq mq
                       (contentType == CTSingularJSON) False (contentType == CTTextCSV)
                       (ApiRequest.iPreferRepresentation apiRequest) [] pgVer prepared
              let (_, queryTotal, _, body, gucHeaders, gucStatus) = row
                  gucs =  (,) <$> gucHeaders <*> gucStatus
              case gucs of
                Left err -> return $ errorResponseFor err
                Right (ghdrs, gstatus) -> do
                  let
                    updateIsNoOp = Set.null (ApiRequest.iColumns apiRequest)
                    defStatus | queryTotal == 0 && not updateIsNoOp      = HTTP.status404
                              | ApiRequest.iPreferRepresentation apiRequest == Full = HTTP.status200
                              | otherwise                                = HTTP.status204
                    status = fromMaybe defStatus gstatus
                    contentRangeHeader = contentRangeH 0 (queryTotal - 1) $ if shouldCount then Just queryTotal else Nothing
                    (ctHeaders, rBody) = if ApiRequest.iPreferRepresentation apiRequest == Full
                                          then ([Just $ toHeader contentType, profileH], toS body)
                                          else ([], mempty)
                    headers = addHeadersIfNotIncluded (catMaybes ctHeaders ++ [contentRangeHeader]) (unwrapGucHeader <$> ghdrs)
                  if contentType == CTSingularJSON && queryTotal /= 1
                    then do
                      Hasql.condemn
                      return . errorResponseFor . singularityError $ queryTotal
                    else
                      return $ Wai.responseLBS status headers rBody

        (ApiRequest.ActionSingleUpsert, ApiRequest.TargetIdent (QualifiedIdentifier tSchema tName)) ->
          case mutateSqlParts tSchema tName of
            Left errorResponse -> return errorResponse
            Right (sq, mq) ->
              if topLevelRange /= allRange
                then return . errorResponseFor $ PutRangeNotAllowedError
              else do
                row <- Hasql.statement mempty $
                       createWriteStatement sq mq (contentType == CTSingularJSON) False
                                            (contentType == CTTextCSV) (ApiRequest.iPreferRepresentation apiRequest) [] pgVer prepared
                let (_, queryTotal, _, body, gucHeaders, gucStatus) = row
                    gucs =  (,) <$> gucHeaders <*> gucStatus
                case gucs of
                  Left err -> return $ errorResponseFor err
                  Right (ghdrs, gstatus) -> do
                    let headers = addHeadersIfNotIncluded (catMaybes [Just $ toHeader contentType, profileH]) (unwrapGucHeader <$> ghdrs)
                        (defStatus, rBody) = if ApiRequest.iPreferRepresentation apiRequest == Full then (HTTP.status200, toS body) else (HTTP.status204, mempty)
                        status = fromMaybe defStatus gstatus
                    -- Makes sure the querystring pk matches the payload pk
                    -- e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted, PUT /items?id=eq.14 { "id" : 2, .. } is rejected
                    -- If this condition is not satisfied then nothing is inserted, check the WHERE for INSERT in QueryBuilder.hs to see how it's done
                    if queryTotal /= 1
                      then do
                        Hasql.condemn
                        return . errorResponseFor $ PutMatchingPkError
                      else
                        return $ Wai.responseLBS status headers rBody

        (ApiRequest.ActionDelete, ApiRequest.TargetIdent (QualifiedIdentifier tSchema tName)) ->
          case mutateSqlParts tSchema tName of
            Left errorResponse -> return errorResponse
            Right (sq, mq) -> do
              let stm = createWriteStatement sq mq
                    (contentType == CTSingularJSON) False
                    (contentType == CTTextCSV)
                    (ApiRequest.iPreferRepresentation apiRequest) [] pgVer prepared
              row <- Hasql.statement mempty stm
              let (_, queryTotal, _, body, gucHeaders, gucStatus) = row
                  gucs =  (,) <$> gucHeaders <*> gucStatus
              case gucs of
                Left err -> return $ errorResponseFor err
                Right (ghdrs, gstatus) -> do
                  let
                    defStatus =
                      if ApiRequest.iPreferRepresentation apiRequest == Full then
                        HTTP.status200
                      else
                        HTTP.status204
                    status = fromMaybe defStatus gstatus
                    contentRangeHeader = contentRangeH 1 0 $ if shouldCount then Just queryTotal else Nothing
                    (ctHeaders, rBody) =
                      if ApiRequest.iPreferRepresentation apiRequest == Full then
                        ([Just $ toHeader contentType, profileH], toS body)
                      else
                        ([], mempty)
                    headers = addHeadersIfNotIncluded (catMaybes ctHeaders ++ [contentRangeHeader]) (unwrapGucHeader <$> ghdrs)
                  if contentType == CTSingularJSON
                     && queryTotal /= 1
                    then do
                      Hasql.condemn
                      return . errorResponseFor . singularityError $ queryTotal
                    else
                      return $ Wai.responseLBS status headers rBody

        (ApiRequest.ActionInfo, ApiRequest.TargetIdent (QualifiedIdentifier tSchema tTable)) ->
          let mTable = find (\t -> tableName t == tTable && tableSchema t == tSchema) (dbTables dbStructure) in
          case mTable of
            Nothing -> return notFound
            Just table ->
              let allowH = (HTTP.hAllow, if tableInsertable table then "GET,POST,PATCH,DELETE" else "GET")
                  allOrigins = ("Access-Control-Allow-Origin", "*") :: HTTP.Header in
              return $ Wai.responseLBS HTTP.status200 [allOrigins, allowH] mempty

        (ApiRequest.ActionInvoke invMethod, ApiRequest.TargetProc proc@ProcDescription{pdSchema, pdName} _) ->
          let tName = fromMaybe pdName $ procTableName proc in
          case readSqlParts pdSchema tName of
            Left errorResponse -> return errorResponse
            Right (q, cq, bField, returning) -> do
              let
                preferParams = ApiRequest.iPreferParameters apiRequest
                pq = requestToCallProcQuery (QualifiedIdentifier pdSchema pdName) (specifiedProcArgs (ApiRequest.iColumns apiRequest) proc)
                       (ApiRequest.iPayload apiRequest) returnsScalar preferParams returning
                stm = callProcStatement returnsScalar returnsSingle pq q cq shouldCount (contentType == CTSingularJSON)
                        (contentType == CTTextCSV) (preferParams == Just MultipleObjects) bField pgVer prepared
              row <- Hasql.statement mempty stm
              let (tableTotal, queryTotal, body, gucHeaders, gucStatus) = row
                  gucs =  (,) <$> gucHeaders <*> gucStatus
              case gucs of
                Left err -> return $ errorResponseFor err
                Right (ghdrs, gstatus) -> do
                  let (rangeStatus, contentRange) = rangeStatusHeader topLevelRange queryTotal tableTotal
                      status = fromMaybe rangeStatus gstatus
                      headers = addHeadersIfNotIncluded
                        (catMaybes [Just $ toHeader contentType, Just contentRange, profileH])
                        (unwrapGucHeader <$> ghdrs)
                      rBody = if invMethod == ApiRequest.InvHead then mempty else toS body
                  if contentType == CTSingularJSON && queryTotal /= 1
                    then do
                      Hasql.condemn
                      return . errorResponseFor . singularityError $ queryTotal
                    else
                      return $ Wai.responseLBS status headers rBody

        (ApiRequest.ActionInspect headersOnly, ApiRequest.TargetDefaultSpec tSchema) -> do
          let host = Config.configServerHost conf
              port = toInteger $ Config.configServerPort conf
              proxy = pickProxy $ toS <$> Config.configOpenApiServerProxyUri conf
              uri Nothing = ("http", host, port, "/")
              uri (Just Proxy { proxyScheme = s, proxyHost = h, proxyPort = p, proxyPath = b }) = (s, h, p, b)
              uri' = uri proxy
              toTableInfo :: [Table] -> [(Table, [Column], [Text])]
              toTableInfo = map (\t -> let (s, tn) = (tableSchema t, tableName t) in (t, tableCols dbStructure s tn, tablePKCols dbStructure s tn))
              encodeApi ti sd procs = encodeOpenAPI (concat $ HashMap.elems procs) (toTableInfo ti) uri' sd $ dbPrimaryKeys dbStructure

          body <- encodeApi <$>
            Hasql.statement tSchema accessibleTables <*>
            Hasql.statement tSchema schemaDescription <*>
            Hasql.statement tSchema accessibleProcs
          return $
            Wai.responseLBS
              HTTP.status200
              (catMaybes [Just $ toHeader CTOpenAPI, profileH])
              (if headersOnly then mempty else toS body)

        _ -> return notFound

      where
        notFound = Wai.responseLBS HTTP.status404 [] ""
        maxRows = Config.configDbMaxRows conf
        prepared = Config.configDbPreparedStatements conf
        exactCount = ApiRequest.iPreferCount apiRequest == Just ExactCount
        estimatedCount = ApiRequest.iPreferCount apiRequest == Just EstimatedCount
        plannedCount = ApiRequest.iPreferCount apiRequest == Just PlannedCount
        shouldCount = exactCount || estimatedCount
        topLevelRange = ApiRequest.iTopLevelRange apiRequest
        returnsScalar =
          case ApiRequest.iTarget apiRequest of
            ApiRequest.TargetProc proc _ ->
              procReturnsScalar proc
            _ ->
              False
        returnsSingle =
          case ApiRequest.iTarget apiRequest of
            ApiRequest.TargetProc proc _ ->
              procReturnsSingle proc
            _ ->
              False
        pgVer = pgVersion dbStructure
        profileH = contentProfileH <$> ApiRequest.iProfile apiRequest

        readSqlParts s t =
          let
            readReq = readRequest s t maxRows (dbRelations dbStructure) apiRequest
            returnings :: ReadRequest -> Either Wai.Response [FieldName]
            returnings rr = Right (returningCols rr [])
          in
          (,,,) <$>
          (readRequestToQuery <$> readReq) <*>
          (readRequestToCountQuery <$> readReq) <*>
          (binaryField contentType rawContentTypes returnsScalar =<< readReq) <*>
          (returnings =<< readReq)

        mutateSqlParts s t =
          let
            readReq = readRequest s t maxRows (dbRelations dbStructure) apiRequest
            mutReq = mutateRequest s t apiRequest (tablePKCols dbStructure s t) =<< readReq
          in
          (,) <$>
          (readRequestToQuery <$> readReq) <*>
          (mutateRequestToQuery <$> mutReq)

responseContentTypeOrError ::
  [ContentType]
  -> [ContentType]
  -> ApiRequest.Action
  -> ApiRequest.Target
  -> Either Wai.Response ContentType
responseContentTypeOrError accepts rawContentTypes action target =
  serves contentTypesForRequest accepts
  where
    contentTypesForRequest =
      case action of
        ApiRequest.ActionRead _ ->
          [CTApplicationJSON, CTSingularJSON, CTTextCSV] ++ rawContentTypes
        ApiRequest.ActionCreate ->
          [CTApplicationJSON, CTSingularJSON, CTTextCSV]
        ApiRequest.ActionUpdate ->
          [CTApplicationJSON, CTSingularJSON, CTTextCSV]
        ApiRequest.ActionDelete ->
          [CTApplicationJSON, CTSingularJSON, CTTextCSV]
        ApiRequest.ActionInvoke _ ->
          [CTApplicationJSON, CTSingularJSON, CTTextCSV]
            ++ rawContentTypes
            ++ [CTOpenAPI | ApiRequest.tpIsRootSpec target]
        ApiRequest.ActionInspect _ ->
          [CTOpenAPI, CTApplicationJSON]
        ApiRequest.ActionInfo ->
          [CTTextCSV]
        ApiRequest.ActionSingleUpsert ->
          [CTApplicationJSON, CTSingularJSON, CTTextCSV]
    serves sProduces cAccepts =
      case ApiRequest.mutuallyAgreeable sProduces cAccepts of
        Nothing -> Left . errorResponseFor . ContentTypeError . map toMime $ cAccepts
        Just ct -> Right ct

{-
  | If raw(binary) output is requested, check that ContentType is one of the admitted rawContentTypes and that
  | `?select=...` contains only one field other than `*`
-}
binaryField ::
  ContentType
  -> [ContentType]
  -> Bool
  -> ReadRequest
  -> Either Wai.Response (Maybe FieldName)
binaryField ct rawContentTypes isScalarProc readReq
  | isScalarProc =
      if ct `elem` rawContentTypes
        then Right $ Just "pgrst_scalar"
        else Right Nothing
  | ct `elem` rawContentTypes =
      let fieldName = headMay fldNames in
      if length fldNames == 1 && fieldName /= Just "*"
        then Right fieldName
        else Left . errorResponseFor $ BinaryFieldError ct
  | otherwise = Right Nothing
  where
    fldNames = fstFieldNames readReq

locationH :: TableName -> [BS.ByteString] -> HTTP.Header
locationH tName fields =
  let
    locationFields = renderSimpleQuery True $ splitKeyValue <$> fields
  in
    (HTTP.hLocation, "/" <> toS tName <> locationFields)
  where
    splitKeyValue :: BS.ByteString -> (BS.ByteString, BS.ByteString)
    splitKeyValue kv =
      let (k, v) = BS.break (== '=') kv
      in (k, BS.tail v)

contentLocationH :: TableName -> ByteString -> HTTP.Header
contentLocationH tName qString =
  ("Content-Location", "/" <> toS tName <> if BS.null qString then mempty else "?" <> toS qString)

contentProfileH :: Schema -> HTTP.Header
contentProfileH schema =
   ("Content-Profile", toS schema)
