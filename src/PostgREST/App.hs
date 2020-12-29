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
module PostgREST.App (postgrest) where

import Data.IORef (IORef, readIORef)
import Data.Time.Clock (UTCTime)
import Data.Either.Combinators (mapLeft)

import qualified Data.Aeson as Aeson
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
import PostgREST.Types (DbStructure, ContentType)

import Protolude hiding (toS)
import Protolude.Conv (toS)


type JWTClaims =
  HashMap.HashMap Text Aeson.Value


postgrest
  :: Types.LogLevel
  -> IORef AppConfig
  -> IORef (Maybe DbStructure)
  -> Hasql.Pool
  -> IO UTCTime
  -> IO ()
  -> Wai.Application
postgrest logLev refConf refDbStructure pool getTime connWorker =
  Middleware.pgrstMiddleware logLev $ \req respond -> do
    time <- getTime
    body <- Wai.strictRequestBody req
    maybeDbStructure <- readIORef refDbStructure
    conf <- readIORef refConf

    case maybeDbStructure of
      Nothing ->
        respond . Error.errorResponseFor $ Error.ConnectionLostError

      Just dbStructure ->
        do
          response <-
            postgrestResponse dbStructure conf pool time req body

          let
            resp = either identity identity response

          -- Launch the connWorker when the connection is down.
          -- The postgrest function can respond successfully (with a stale schema
          -- cache) before the connWorker is done.
          when (Wai.responseStatus resp == HTTP.status503) connWorker

          respond resp


postgrestResponse
  :: DbStructure
  -> AppConfig
  -> Hasql.Pool
  -> UTCTime
  -> Wai.Request
  -> LazyByteString.ByteString
  -> IO (Either Wai.Response Wai.Response)
postgrestResponse dbStructure conf pool time req body =
  let
    apiReq =
      ApiRequest.userApiRequest
        (Config.configDbSchemas conf)
        (Config.configDbRootSpec conf)
        dbStructure
        req
        body
  in
  case apiReq of
    Left err ->
      return . Left $ Error.errorResponseFor err

    Right apiRequest ->
      do
        -- The jwt must be checked before touching the db.
        clms <- mapLeft Error.errorResponseFor <$> jwtClaims conf apiRequest time

        case clms of
          Left err ->
            return . Left $ err

          Right claims ->
            case getContentType conf apiRequest of
              Left err ->
                return . Left $ err

              Right contentType ->
                abcde conf pool dbStructure apiRequest claims contentType


getContentType :: AppConfig -> ApiRequest -> Either Wai.Response ContentType
getContentType conf apiRequest =
  mapLeft Error.errorResponseFor $ responseContentType conf apiRequest


abcde
  :: AppConfig
  -> Hasql.Pool
  -> DbStructure
  -> ApiRequest
  -> JWTClaims
  -> ContentType
  -> IO (Either Wai.Response Wai.Response)
abcde conf pool dbStructure apiRequest claims contentType =
  do
    dbResp <-
      Hasql.use pool $
        Hasql.transaction Hasql.ReadCommitted (txMode apiRequest) $
          optionalRollback conf apiRequest $
            Middleware.runPgLocals
              conf
              claims
              (handleRequest dbStructure conf contentType)
              apiRequest

    case dbResp of
      Left err ->
        return . Left . Error.errorResponseFor $
          Error.PgError (Auth.containsRole claims) err

      Right resp ->
        return $ Right resp


jwtClaims
  :: AppConfig
  -> ApiRequest
  -> UTCTime
  -> IO (Either Error.SimpleError JWTClaims)
jwtClaims conf apiRequest time =
  Auth.jwtClaims <$>
    Auth.attemptJwtClaims
      (Config.configJWKS conf)
      (Config.configJwtAudience conf)
      (toS $ ApiRequest.iJWT apiRequest)
      time
      (rightToMaybe $ Config.configJwtRoleClaimKey conf)


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


handleRequest
  :: DbStructure
  -> AppConfig
  -> ContentType
  -> ApiRequest
  -> Hasql.Transaction Wai.Response
handleRequest dbStructure conf contentType apiRequest =
  case (ApiRequest.iAction apiRequest, ApiRequest.iTarget apiRequest) of
    (ApiRequest.ActionRead headersOnly, ApiRequest.TargetIdent identifier) ->
      either identity identity <$>
        handleRead conf dbStructure apiRequest headersOnly contentType identifier

    (ApiRequest.ActionCreate, ApiRequest.TargetIdent identifier) ->
      handleCreate conf dbStructure apiRequest contentType identifier

    (ApiRequest.ActionUpdate, ApiRequest.TargetIdent identifier) ->
      handleUpdate conf dbStructure apiRequest contentType identifier

    (ApiRequest.ActionSingleUpsert, ApiRequest.TargetIdent identifier) ->
      handleSingleUpsert conf dbStructure apiRequest contentType identifier

    (ApiRequest.ActionDelete, ApiRequest.TargetIdent identifier) ->
      handleDelete conf dbStructure contentType apiRequest identifier

    (ApiRequest.ActionInfo, ApiRequest.TargetIdent identifier) ->
      return $ handleInfo dbStructure identifier

    (ApiRequest.ActionInvoke invMethod, ApiRequest.TargetProc proc _) ->
      either identity identity <$>
        handleInvoke conf dbStructure invMethod contentType apiRequest proc

    (ApiRequest.ActionInspect headersOnly, ApiRequest.TargetDefaultSpec tSchema) ->
      handleOpenApi conf dbStructure apiRequest headersOnly tSchema

    _ ->
      return notFound


handleRead
  :: AppConfig
  -> DbStructure
  -> ApiRequest
  -> Bool
  -> ContentType
  -> Types.QualifiedIdentifier
  -> Hasql.Transaction (Either Wai.Response Wai.Response)
handleRead conf dbStructure apiRequest headersOnly contentType identifier =
  case readRequest conf dbStructure identifier apiRequest of
    Left errorResponse ->
      return $ Left errorResponse

    Right req ->
      let
        cq =
          QueryBuilder.readRequestToCountQuery req

        cQuery =
          if estimatedCount apiRequest then
            -- LIMIT maxRows + 1 so we can determine below that maxRows
            -- was surpassed
            QueryBuilder.limitedQuery cq ((+ 1) <$> Config.configDbMaxRows conf)
          else
            cq

        field =
          binaryField
            contentType
            (rawContentTypes conf)
            (returnsScalar apiRequest)
            req
      in
      case field of
        Left err ->
          return $ Left err

        Right bField ->
          let
            stm =
              Statements.createReadStatement
                (QueryBuilder.readRequestToQuery req)
                cQuery
                (contentType == Types.CTSingularJSON)
                (shouldCount apiRequest)
                (contentType == Types.CTTextCSV)
                bField
                (Types.pgVersion dbStructure)
                (Config.configDbPreparedStatements conf)
          in
          do
            (tableTotal, queryTotal, _ , body, gucHeaders, gucStatus) <-
              Hasql.statement mempty $ stm

            case (,) <$> gucHeaders <*> gucStatus of
              Left err ->
                return . Left $ Error.errorResponseFor err

              Right (ghdrs, gstatus) -> do
                total <- readTotal conf tableTotal apiRequest cq
                let
                  (rangeStatus, contentRange) =
                    RangeQuery.rangeStatusHeader
                      (ApiRequest.iTopLevelRange apiRequest)
                      queryTotal
                      total

                  headers =
                    Types.addHeadersIfNotIncluded
                      (catMaybes
                        [ Just $ Types.toHeader contentType, Just contentRange
                        , Just $
                            contentLocationH
                              (Types.qiName identifier)
                              (ApiRequest.iCanonicalQS apiRequest)
                        , profileH apiRequest
                        ]
                      )
                      (Types.unwrapGucHeader <$> ghdrs)

                failNotSingular contentType queryTotal $
                  Wai.responseLBS
                    (fromMaybe rangeStatus gstatus)
                    headers
                    (if headersOnly then mempty else toS body)


readTotal
  :: AppConfig
  -> Maybe Int64
  -> ApiRequest
  -> Hasql.Snippet
  -> Hasql.Transaction (Maybe Int64)
readTotal conf tableTotal apiRequest cq =
  let
    explStm =
      Statements.createExplainStatement cq (Config.configDbPreparedStatements conf)
  in
  case ApiRequest.iPreferCount apiRequest of
    Just Types.PlannedCount ->
      Hasql.statement () explStm

    Just Types.EstimatedCount ->
      if tableTotal > (fromIntegral <$> Config.configDbMaxRows conf) then
        do
          estTotal <- Hasql.statement () explStm
          return $
            if estTotal > tableTotal then
              estTotal
            else
              tableTotal
      else
        return tableTotal

    _ ->
      return tableTotal


handleCreate
  :: AppConfig
  -> DbStructure
  -> ApiRequest
  -> ContentType
  -> Types.QualifiedIdentifier
  -> Hasql.Transaction Wai.Response
handleCreate conf dbStructure apiRequest contentType identifier =
  case mutateSqlParts conf dbStructure apiRequest identifier of
    Left errorResponse -> return errorResponse
    Right (sq, mq) ->
      let
        pkCols =
          Types.tablePKCols
            dbStructure
            (Types.qiSchema identifier)
            (Types.qiName identifier)

        stm =
          Statements.createWriteStatement
            sq
            mq
            (contentType == Types.CTSingularJSON)
            True
            (contentType == Types.CTTextCSV)
            (ApiRequest.iPreferRepresentation apiRequest)
            pkCols
            (Types.pgVersion dbStructure)
            (Config.configDbPreparedStatements conf)
      in
      do
        (_, queryTotal, fields, body, gucHeaders, gucStatus) <-
          Hasql.statement mempty stm

        let
          gucs =
            (,) <$> gucHeaders <*> gucStatus

        case gucs of
          Left err -> return $ Error.errorResponseFor err
          Right (ghdrs, gstatus) -> do
            let
              (ctHeaders, rBody) =
                if ApiRequest.iPreferRepresentation apiRequest == Types.Full then
                  ([Just $ Types.toHeader contentType, profileH apiRequest], toS body)
                else
                  ([], mempty)

              status =
                fromMaybe HTTP.status201 gstatus

              headers =
                Types.addHeadersIfNotIncluded
                  (catMaybes (
                    [ if null fields then
                        Nothing
                      else
                        Just $ locationH (Types.qiName identifier) fields
                    , Just $
                        RangeQuery.contentRangeH
                          1
                          0
                          (if shouldCount apiRequest then Just queryTotal else Nothing)
                    , if null pkCols && isNothing (ApiRequest.iOnConflict apiRequest) then
                        Nothing
                      else
                        (\x -> ("Preference-Applied", Char8ByteString.pack (show x))) <$>
                          ApiRequest.iPreferResolution apiRequest
                    ] ++ ctHeaders)
                  )
                  (Types.unwrapGucHeader <$> ghdrs)
            if contentType == Types.CTSingularJSON && queryTotal /= 1 then
              do
                Hasql.condemn
                return . Error.errorResponseFor . Error.singularityError $ queryTotal
            else
              return $ Wai.responseLBS status headers rBody


handleUpdate
  :: AppConfig
  -> DbStructure
  -> ApiRequest
  -> ContentType
  -> Types.QualifiedIdentifier
  -> Hasql.Transaction Wai.Response
handleUpdate conf dbStructure apiRequest contentType identifier =
  case mutateSqlParts conf dbStructure apiRequest identifier of
    Left errorResponse ->
      return errorResponse

    Right (sq, mq) ->
      let
        statement =
          Statements.createWriteStatement
            sq
            mq
            (contentType == Types.CTSingularJSON)
            False
            (contentType == Types.CTTextCSV)
            (ApiRequest.iPreferRepresentation apiRequest)
            []
            (Types.pgVersion dbStructure)
            (Config.configDbPreparedStatements conf)
      in
      do
        (_, queryTotal, _, body, gucHeaders, gucStatus) <-
          Hasql.statement mempty statement

        let
          gucs =  (,) <$> gucHeaders <*> gucStatus

        case gucs of
          Left err ->
            return $ Error.errorResponseFor err
          Right (ghdrs, gstatus) -> do
            let
              updateIsNoOp =
                Set.null (ApiRequest.iColumns apiRequest)

              defStatus
                | queryTotal == 0 && not updateIsNoOp =
                    HTTP.status404
                | ApiRequest.iPreferRepresentation apiRequest == Types.Full =
                    HTTP.status200
                | otherwise =
                    HTTP.status204

              status =
                fromMaybe defStatus gstatus

              contentRangeHeader =
                RangeQuery.contentRangeH
                  0
                  (queryTotal - 1)
                  (if shouldCount apiRequest then Just queryTotal else Nothing)

              (ctHeaders, rBody) =
                if ApiRequest.iPreferRepresentation apiRequest == Types.Full then
                  ([Just $ Types.toHeader contentType, profileH apiRequest], toS body)
                else
                  ([], mempty)

              headers =
                Types.addHeadersIfNotIncluded
                  (catMaybes ctHeaders ++ [contentRangeHeader])
                  (Types.unwrapGucHeader <$> ghdrs)

            if contentType == Types.CTSingularJSON && queryTotal /= 1 then
              do
                Hasql.condemn
                return . Error.errorResponseFor . Error.singularityError $ queryTotal
            else
              return $ Wai.responseLBS status headers rBody


handleSingleUpsert
  :: AppConfig
  -> DbStructure
  -> ApiRequest
  -> ContentType
  -> Types.QualifiedIdentifier
  -> Hasql.Transaction Wai.Response
handleSingleUpsert conf dbStructure apiRequest contentType identifier =
  case mutateSqlParts conf dbStructure apiRequest identifier of
    Left errorResponse ->
      return errorResponse

    Right (sq, mq) ->
      if ApiRequest.iTopLevelRange apiRequest /= RangeQuery.allRange then
        return . Error.errorResponseFor $ Error.PutRangeNotAllowedError
      else
        let
          statement =
              Statements.createWriteStatement
                sq
                mq
                (contentType == Types.CTSingularJSON)
                False
                (contentType == Types.CTTextCSV)
                (ApiRequest.iPreferRepresentation apiRequest)
                []
                (Types.pgVersion dbStructure)
                (Config.configDbPreparedStatements conf)
        in
        do
          (_, queryTotal, _, body, gucHeaders, gucStatus) <-
            Hasql.statement mempty statement

          let
            gucs =  (,) <$> gucHeaders <*> gucStatus

          case gucs of
            Left err -> return $ Error.errorResponseFor err
            Right (ghdrs, gstatus) -> do
              let
                headers =
                  Types.addHeadersIfNotIncluded
                    (catMaybes [Just $ Types.toHeader contentType, profileH apiRequest])
                    (Types.unwrapGucHeader <$> ghdrs)

                (defStatus, rBody) =
                  if ApiRequest.iPreferRepresentation apiRequest == Types.Full then
                    (HTTP.status200, toS body)
                  else
                    (HTTP.status204, mempty)

                status = fromMaybe defStatus gstatus

              -- Makes sure the querystring pk matches the payload pk
              -- e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
              -- PUT /items?id=eq.14 { "id" : 2, .. } is rejected
              -- If this condition is not satisfied then nothing is inserted,
              -- check the WHERE for INSERT in QueryBuilder.hs to see how it's done
              if queryTotal /= 1 then
                do
                  Hasql.condemn
                  return . Error.errorResponseFor $ Error.PutMatchingPkError
              else
                return $ Wai.responseLBS status headers rBody


handleDelete
  :: AppConfig
  -> DbStructure
  -> ContentType
  -> ApiRequest
  -> Types.QualifiedIdentifier
  -> Hasql.Transaction Wai.Response
handleDelete conf dbStructure contentType apiRequest identifier =
  case mutateSqlParts conf dbStructure apiRequest identifier of
    Left errorResponse -> return errorResponse
    Right (sq, mq) -> do
      let
        stm =
          Statements.createWriteStatement
            sq
            mq
            (contentType == Types.CTSingularJSON) False
            (contentType == Types.CTTextCSV)
            (ApiRequest.iPreferRepresentation apiRequest)
            []
            (Types.pgVersion dbStructure)
            (Config.configDbPreparedStatements conf)

      (_, queryTotal, _, body, gucHeaders, gucStatus) <-
        Hasql.statement mempty stm

      let
        gucs =  (,) <$> gucHeaders <*> gucStatus

      case gucs of
        Left err ->
          return $ Error.errorResponseFor err

        Right (ghdrs, gstatus) ->
          let
            defStatus =
              if ApiRequest.iPreferRepresentation apiRequest == Types.Full then
                HTTP.status200
              else
                HTTP.status204

            status =
              fromMaybe defStatus gstatus

            contentRangeHeader =
              RangeQuery.contentRangeH 1 0 $
                if shouldCount apiRequest then Just queryTotal else Nothing

            (ctHeaders, rBody) =
              if ApiRequest.iPreferRepresentation apiRequest == Types.Full then
                ([Just $ Types.toHeader contentType, profileH apiRequest], toS body)
              else
                ([], mempty)

            headers =
              Types.addHeadersIfNotIncluded
                (catMaybes ctHeaders ++ [contentRangeHeader])
                (Types.unwrapGucHeader <$> ghdrs)
          in
          if contentType == Types.CTSingularJSON && queryTotal /= 1 then
            do
              Hasql.condemn
              return . Error.errorResponseFor . Error.singularityError $ queryTotal
          else
            return $ Wai.responseLBS status headers rBody


handleInfo :: DbStructure -> Types.QualifiedIdentifier -> Wai.Response
handleInfo dbStructure identifier =
  case findTable dbStructure identifier of
    Nothing ->
      notFound

    Just table ->
      let
        allowH =
          ( HTTP.hAllow
          , if Types.tableInsertable table then
              "GET,POST,PATCH,DELETE"
            else
              "GET"
          )

        allOrigins =
          ("Access-Control-Allow-Origin", "*") :: HTTP.Header
      in
      Wai.responseLBS HTTP.status200 [allOrigins, allowH] mempty


findTable :: DbStructure -> Types.QualifiedIdentifier -> Maybe Types.Table
findTable dbStructure identifier =
  find
    (\t ->
      Types.tableName t == Types.qiName identifier
      && Types.tableSchema t == Types.qiSchema identifier
    )
    (Types.dbTables dbStructure)


handleInvoke
  :: AppConfig
  -> DbStructure
  -> ApiRequest.InvokeMethod
  -> ContentType
  -> ApiRequest
  -> Types.ProcDescription
  -> Hasql.Transaction (Either Wai.Response Wai.Response)
handleInvoke conf dbStructure invMethod contentType apiRequest proc =
  let
    identifier =
      Types.QualifiedIdentifier
        (Types.pdSchema proc)
        (fromMaybe (Types.pdName proc) $ Types.procTableName proc)
  in
  case readRequest conf dbStructure identifier apiRequest of
    Left errorResponse ->
      return $ Left errorResponse

    Right req ->
      let
        field =
          binaryField
            contentType
            (rawContentTypes conf)
            (returnsScalar apiRequest)
            req
      in
      case field of
        Left err ->
          return $ Left err

        Right bField ->
          do
            (tableTotal, queryTotal, body, gucHeaders, gucStatus) <-
              handleInvokeTransaction conf dbStructure apiRequest req proc contentType bField

            case (,) <$> gucHeaders <*> gucStatus of
              Left err ->
                return . Left $ Error.errorResponseFor err

              Right (ghdrs, gstatus) ->
                let
                  (rangeStatus, contentRange) =
                    RangeQuery.rangeStatusHeader
                      (ApiRequest.iTopLevelRange apiRequest)
                      queryTotal
                      tableTotal

                  headers =
                    Types.addHeadersIfNotIncluded
                      (catMaybes
                        [ Just $ Types.toHeader contentType
                        , Just contentRange
                        , profileH apiRequest
                        ]
                      )
                      (Types.unwrapGucHeader <$> ghdrs)
                in
                failNotSingular contentType queryTotal $
                  Wai.responseLBS
                    (fromMaybe rangeStatus gstatus)
                    headers
                    (if invMethod == ApiRequest.InvHead then mempty else toS body)


handleInvokeTransaction
  :: AppConfig
  -> DbStructure
  -> ApiRequest
  -> Types.ReadRequest
  -> Types.ProcDescription
  -> ContentType
  -> Maybe Types.FieldName
  -> Hasql.Transaction Statements.ProcResults
handleInvokeTransaction conf dbStructure apiRequest req proc contentType bField =
  let
    pq =
      QueryBuilder.requestToCallProcQuery
        (Types.QualifiedIdentifier (Types.pdSchema proc) (Types.pdName proc))
        (Types.specifiedProcArgs (ApiRequest.iColumns apiRequest) proc)
        (ApiRequest.iPayload apiRequest)
        (returnsScalar apiRequest)
        (ApiRequest.iPreferParameters apiRequest)
        (DbRequestBuilder.returningCols req [])
  in
    Hasql.statement mempty $
      Statements.callProcStatement
        (returnsScalar apiRequest)
        (returnsSingle apiRequest)
        pq
        (QueryBuilder.readRequestToQuery req)
        (QueryBuilder.readRequestToCountQuery req)
        (shouldCount apiRequest)
        (contentType == Types.CTSingularJSON)
        (contentType == Types.CTTextCSV)
        (ApiRequest.iPreferParameters apiRequest == Just Types.MultipleObjects)
        bField
        (Types.pgVersion dbStructure)
        (Config.configDbPreparedStatements conf)


-- | Fail a response if a single JSON object was requested and not exactly one
--   was found.
failNotSingular
  :: ContentType
  -> Int64
  -> Wai.Response
  -> Hasql.Transaction (Either Wai.Response Wai.Response)
failNotSingular contentType queryTotal response =
  if contentType == Types.CTSingularJSON && queryTotal /= 1 then
    do
      Hasql.condemn
      return . Left . Error.errorResponseFor . Error.singularityError $ queryTotal
  else
    return $ Right response


handleOpenApi
  :: AppConfig
  -> DbStructure
  -> ApiRequest
  -> Bool
  -> Types.Schema
  -> Hasql.Transaction Wai.Response
handleOpenApi conf dbStructure apiRequest headersOnly tSchema =
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
      encodeApi
        <$> Hasql.statement tSchema DbStructure.accessibleTables
        <*> Hasql.statement tSchema DbStructure.schemaDescription
        <*> Hasql.statement tSchema DbStructure.accessibleProcs

    return $
      Wai.responseLBS
        HTTP.status200
        (catMaybes [Just $ Types.toHeader Types.CTOpenAPI, profileH apiRequest])
        (if headersOnly then mempty else toS body)


-- Should be obsolete when Table and Column types are refactored
openApiTableInfo :: DbStructure -> Types.Table -> (Types.Table, [Types.Column], [Text])
openApiTableInfo dbStructure table =
  let
    schema =
      Types.tableSchema table

    name =
      Types.tableName table
  in
    ( table
    , Types.tableCols dbStructure schema name
    , Types.tablePKCols dbStructure schema name
    )


notFound :: Wai.Response
notFound =
  Wai.responseLBS HTTP.status404 [] mempty


estimatedCount :: ApiRequest -> Bool
estimatedCount apiRequest =
  ApiRequest.iPreferCount apiRequest == Just Types.EstimatedCount


shouldCount :: ApiRequest -> Bool
shouldCount apiRequest =
  let
    exactCount =
      ApiRequest.iPreferCount apiRequest == Just Types.ExactCount
  in
  exactCount || estimatedCount apiRequest


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


mutateSqlParts
  :: AppConfig
  -> DbStructure
  -> ApiRequest
  -> Types.QualifiedIdentifier
  -> Either Wai.Response (Hasql.Snippet, Hasql.Snippet)
mutateSqlParts conf dbStructure apiRequest identifier =
  (,) <$>
    (readQuery conf dbStructure identifier apiRequest) <*>
    (mutateQuery conf dbStructure identifier apiRequest)


readQuery
  :: AppConfig
  -> DbStructure
  -> Types.QualifiedIdentifier
  -> ApiRequest
  -> Either Wai.Response Hasql.Snippet
readQuery conf dbStructure identifier apiRequest =
  QueryBuilder.readRequestToQuery <$>
    readRequest conf dbStructure identifier apiRequest


readRequest
  :: AppConfig
  -> DbStructure
  -> Types.QualifiedIdentifier
  -> ApiRequest
  -> Either Wai.Response Types.ReadRequest
readRequest conf dbStructure identifier apiRequest =
  DbRequestBuilder.readRequest
    (Types.qiSchema identifier)
    (Types.qiName identifier)
    (Config.configDbMaxRows conf)
    (Types.dbRelations dbStructure)
    apiRequest


mutateQuery
  :: AppConfig
  -> DbStructure
  -> Types.QualifiedIdentifier
  -> ApiRequest
  -> Either Wai.Response Hasql.Snippet
mutateQuery conf dbStructure identifier apiRequest =
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
  (Types.decodeContentType <$> Config.configRawMediaTypes conf)
  `List.union`
  [ Types.CTOctetStream, Types.CTTextPlain ]


responseContentType
  :: AppConfig
  -> ApiRequest
  -> Either Error.SimpleError ContentType
responseContentType conf apiRequest =
  let
    produces =
      requestContentTypes
        (rawContentTypes conf)
        (ApiRequest.iAction apiRequest)
        (ApiRequest.iTarget apiRequest)

    accepts =
      ApiRequest.iAccepts apiRequest
  in
  case ApiRequest.mutuallyAgreeable produces accepts of
    Just contentType ->
      Right contentType

    Nothing ->
      Left . Error.ContentTypeError . map Types.toMime $ accepts


requestContentTypes
  :: [ContentType]
  -> ApiRequest.Action
  -> ApiRequest.Target
  -> [ContentType]
requestContentTypes contentTypes action target =
  case action of
    ApiRequest.ActionRead _ ->
      defaultContentTypes ++ contentTypes
    ApiRequest.ActionCreate ->
      defaultContentTypes
    ApiRequest.ActionUpdate ->
      defaultContentTypes
    ApiRequest.ActionDelete ->
      defaultContentTypes
    ApiRequest.ActionInvoke _ ->
      defaultContentTypes
        ++ contentTypes
        ++ [Types.CTOpenAPI | ApiRequest.tpIsRootSpec target]
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
binaryField ::
  ContentType
  -> [ContentType]
  -> Bool
  -> Types.ReadRequest
  -> Either Wai.Response (Maybe Types.FieldName)
binaryField ct contentTypes isScalarProc readReq
  | isScalarProc && ct `elem` contentTypes =
      Right $ Just "pgrst_scalar"
  | ct `elem` contentTypes =
      let
        fldNames =
          Types.fstFieldNames readReq

        fieldName =
          headMay fldNames
      in
      if length fldNames == 1 && fieldName /= Just "*" then
        Right fieldName
      else
        Left . Error.errorResponseFor $ Error.BinaryFieldError ct
  | otherwise =
      Right Nothing



-- HEADERS


profileH :: ApiRequest -> Maybe HTTP.Header
profileH apiRequest =
  contentProfileH <$> ApiRequest.iProfile apiRequest


locationH :: Types.TableName -> [Char8ByteString.ByteString] -> HTTP.Header
locationH tName fields =
  ( HTTP.hLocation
  , "/" <> toS tName <> (HTTP.renderSimpleQuery True $ splitKeyValue <$> fields)
  )


splitKeyValue
  :: Char8ByteString.ByteString
  -> (Char8ByteString.ByteString, Char8ByteString.ByteString)
splitKeyValue kv =
  let
    (k, v) =
      Char8ByteString.break (== '=') kv
  in
    (k, Char8ByteString.tail v)


contentLocationH :: Types.TableName -> ByteString -> HTTP.Header
contentLocationH tName qString =
  ( "Content-Location"
  , "/" <> toS tName <> if Char8ByteString.null qString then mempty else "?" <> toS qString
  )


contentProfileH :: Types.Schema -> HTTP.Header
contentProfileH schema =
  ( "Content-Profile"
  , toS schema
  )



-- MIDDLEWARE - to be moved to Middleware.hs


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



-- CONFIG - to be moved to Config.hs


openApiUri :: AppConfig -> (Text, Text, Integer, Text)
openApiUri conf =
  let
    maybeProxy =
      OpenAPI.pickProxy $ toS <$> Config.configOpenApiServerProxyUri conf
  in
    case maybeProxy of
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
