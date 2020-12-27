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
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE ScopedTypeVariables #-}

module PostgREST.App (postgrest) where

import Data.IORef (IORef, readIORef)
import Data.Time.Clock (UTCTime)

import qualified Data.ByteString.Char8 as Char8ByteString
import qualified Data.ByteString.Lazy as LazyByteString
import qualified Data.Ranged.Ranges as Ranges
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
import qualified PostgREST.Auth as Auth
import qualified PostgREST.Config as Config
import qualified PostgREST.DbRequestBuilder as DbRequestBuilder
import qualified PostgREST.DbStructure as DbStructure
import qualified PostgREST.Error as Error
import qualified PostgREST.Middleware as Middleware
import qualified PostgREST.OpenAPI as OpenAPI
import qualified PostgREST.QueryBuilder as QueryBuilder
import qualified PostgREST.RangeQuery as RangeQuery
import qualified PostgREST.Statements as Statements
import qualified PostgREST.Types as Types

import Protolude hiding (toS)
import Protolude.Conv (toS)

postgrest
  :: Types.LogLevel
  -> IORef Config.AppConfig
  -> IORef (Maybe Types.DbStructure)
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
      Just dbStructure -> do
        response <-
          postgrestResponse time body dbStructure conf pool req

        -- Launch the connWorker when the connection is down.
        -- The postgrest function can respond successfully (with a stale schema
        -- cache) before the connWorker is done.
        when (Wai.responseStatus response == HTTP.status503) connWorker
        respond response

postgrestResponse
  :: UTCTime
  -> LazyByteString.ByteString
  -> Types.DbStructure
  -> Config.AppConfig
  -> Hasql.Pool
  -> Wai.Request
  -> IO Wai.Response
postgrestResponse time body dbStructure conf pool req =
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
      return . Error.errorResponseFor $ err

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
        Left errJwt ->
          return . Error.errorResponseFor $ errJwt
        Right claims ->
          let
            authed =
              Auth.containsRole claims

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

            handleReq =
              do
                when (shouldRollback || (Config.configDbTxRollbackAll conf && not shouldCommit))
                  Hasql.condemn

                Wai.mapResponseHeaders preferenceApplied <$>
                  Middleware.runPgLocals
                    conf
                    claims
                    (app dbStructure conf)
                    apiRequest
          in
          do
            dbResp <-
              Hasql.use pool $
                Hasql.transaction
                  Hasql.ReadCommitted
                  (txMode apiRequest)
                  handleReq

            return $
              either
                (Error.errorResponseFor . Error.PgError authed)
                identity
                dbResp


txMode :: ApiRequest.ApiRequest -> Hasql.Mode
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
    _ -> Hasql.Write

app
  :: Types.DbStructure
  -> Config.AppConfig
  -> ApiRequest.ApiRequest
  -> Hasql.Transaction Wai.Response
app dbStructure conf apiRequest =
  let
    rawContentTypes =
      (Types.decodeContentType <$> Config.configRawMediaTypes conf)
      `List.union`
      [ Types.CTOctetStream, Types.CTTextPlain ]

    contentTypeOrError =
      responseContentTypeOrError
        (ApiRequest.iAccepts apiRequest)
        rawContentTypes
        (ApiRequest.iAction apiRequest)
        (ApiRequest.iTarget apiRequest)
  in
  case contentTypeOrError of
    Left errorResponse ->
      return errorResponse

    Right contentType ->
      handleRequest dbStructure conf apiRequest rawContentTypes contentType


handleRequest
  :: Types.DbStructure
  -> Config.AppConfig
  -> ApiRequest.ApiRequest
  -> [Types.ContentType]
  -> Types.ContentType
  -> Hasql.Transaction Wai.Response
handleRequest dbStructure conf apiRequest rawContentTypes contentType =
  case (ApiRequest.iAction apiRequest, ApiRequest.iTarget apiRequest) of
    (ApiRequest.ActionRead headersOnly, ApiRequest.TargetIdent identifier) ->
      handleRead conf dbStructure apiRequest headersOnly rawContentTypes contentType identifier

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
      handleInvoke conf dbStructure invMethod rawContentTypes contentType apiRequest proc

    (ApiRequest.ActionInspect headersOnly, ApiRequest.TargetDefaultSpec tSchema) ->
      handleOpenApi conf dbStructure apiRequest headersOnly tSchema

    _ -> return notFound


handleRead
  :: Config.AppConfig
  -> Types.DbStructure
  -> ApiRequest.ApiRequest
  -> Bool
  -> [Types.ContentType]
  -> Types.ContentType
  -> Types.QualifiedIdentifier
  -> Hasql.Transaction Wai.Response
handleRead conf dbStructure apiRequest headersOnly rawContentTypes contentType identifier =
  case readSqlParts conf dbStructure apiRequest rawContentTypes contentType identifier of
    Left errorResponse -> return errorResponse
    Right (q, cq, bField, _) ->
      let
        cQuery =
          if estimatedCount apiRequest then
            -- LIMIT maxRows + 1 so we can determine below that maxRows
            -- was surpassed
            QueryBuilder.limitedQuery cq ((+ 1) <$> Config.configDbMaxRows conf)
          else
            cq

        stm =
          Statements.createReadStatement
            q
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
          Hasql.statement mempty stm
        let
          gucs =
            (,) <$> gucHeaders <*> gucStatus

        case gucs of
          Left err ->
            return $ Error.errorResponseFor err
          Right (ghdrs, gstatus) -> do
            total <- readTotal conf tableTotal apiRequest cq
            let
              (rangeStatus, contentRange) =
                RangeQuery.rangeStatusHeader (topLevelRange apiRequest) queryTotal total

              status =
                fromMaybe rangeStatus gstatus

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

              rBody =
                if headersOnly then mempty else toS body
            return $
              if contentType == Types.CTSingularJSON && queryTotal /= 1 then
                Error.errorResponseFor . Error.singularityError $ queryTotal
              else
                Wai.responseLBS status headers rBody


readTotal
  :: Config.AppConfig
  -> Maybe Int64
  -> ApiRequest.ApiRequest
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
  :: Config.AppConfig
  -> Types.DbStructure
  -> ApiRequest.ApiRequest
  -> Types.ContentType
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
  :: Config.AppConfig
  -> Types.DbStructure
  -> ApiRequest.ApiRequest
  -> Types.ContentType
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
  :: Config.AppConfig
  -> Types.DbStructure
  -> ApiRequest.ApiRequest
  -> Types.ContentType
  -> Types.QualifiedIdentifier
  -> Hasql.Transaction Wai.Response
handleSingleUpsert conf dbStructure apiRequest contentType identifier =
  case mutateSqlParts conf dbStructure apiRequest identifier of
    Left errorResponse ->
      return errorResponse

    Right (sq, mq) ->
      if topLevelRange apiRequest /= RangeQuery.allRange then
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
  :: Config.AppConfig
  -> Types.DbStructure
  -> Types.ContentType
  -> ApiRequest.ApiRequest
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


handleInfo :: Types.DbStructure -> Types.QualifiedIdentifier -> Wai.Response
handleInfo dbStructure identifier =
  let
    mTable =
      find
        (\t ->
          Types.tableName t == Types.qiName identifier
          && Types.tableSchema t == Types.qiSchema identifier
        )
        (Types.dbTables dbStructure)
  in
  case mTable of
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


handleInvoke
  :: Config.AppConfig
  -> Types.DbStructure
  -> ApiRequest.InvokeMethod
  -> [Types.ContentType]
  -> Types.ContentType
  -> ApiRequest.ApiRequest
  -> Types.ProcDescription
  -> Hasql.Transaction Wai.Response
handleInvoke conf dbStructure invMethod rawContentTypes contentType apiRequest proc =
  let
    pdName =
      Types.pdName proc

    pdSchema =
      Types.pdSchema proc

    tName =
      fromMaybe pdName $ Types.procTableName proc

    identifier =
      Types.QualifiedIdentifier pdSchema tName
  in
  case readSqlParts conf dbStructure apiRequest rawContentTypes contentType identifier of
    Left errorResponse ->
      return errorResponse

    Right (q, cq, bField, returning) ->
      let
        preferParams =
          ApiRequest.iPreferParameters apiRequest

        pq =
          QueryBuilder.requestToCallProcQuery
            (Types.QualifiedIdentifier pdSchema pdName)
            (Types.specifiedProcArgs (ApiRequest.iColumns apiRequest) proc)
            (ApiRequest.iPayload apiRequest)
            (returnsScalar apiRequest)
            preferParams
            returning

        stm =
          Statements.callProcStatement
            (returnsScalar apiRequest)
            (returnsSingle apiRequest)
            pq
            q
            cq
            (shouldCount apiRequest)
            (contentType == Types.CTSingularJSON)
            (contentType == Types.CTTextCSV)
            (preferParams == Just Types.MultipleObjects)
            bField
            (Types.pgVersion dbStructure)
            (Config.configDbPreparedStatements conf)
      in
      do
        (tableTotal, queryTotal, body, gucHeaders, gucStatus) <-
          Hasql.statement mempty stm

        let
          gucs =
            (,) <$> gucHeaders <*> gucStatus

        case gucs of
          Left err ->
            return $ Error.errorResponseFor err

          Right (ghdrs, gstatus) ->
            let
              (rangeStatus, contentRange) =
                RangeQuery.rangeStatusHeader
                  (topLevelRange apiRequest)
                  queryTotal
                  tableTotal

              status =
                fromMaybe rangeStatus gstatus

              headers =
                Types.addHeadersIfNotIncluded
                  (catMaybes
                    [ Just $ Types.toHeader contentType
                    , Just contentRange, profileH apiRequest
                    ]
                  )
                  (Types.unwrapGucHeader <$> ghdrs)

              rBody =
                if invMethod == ApiRequest.InvHead then mempty else toS body
            in
            if contentType == Types.CTSingularJSON && queryTotal /= 1
              then do
                Hasql.condemn
                return . Error.errorResponseFor . Error.singularityError $ queryTotal
              else
                return $ Wai.responseLBS status headers rBody


handleOpenApi
  :: Config.AppConfig
  -> Types.DbStructure
  -> ApiRequest.ApiRequest
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
      encodeApi <$>
        Hasql.statement tSchema DbStructure.accessibleTables <*>
        Hasql.statement tSchema DbStructure.schemaDescription <*>
        Hasql.statement tSchema DbStructure.accessibleProcs

    return $
      Wai.responseLBS
        HTTP.status200
        (catMaybes [Just $ Types.toHeader Types.CTOpenAPI, profileH apiRequest])
        (if headersOnly then mempty else toS body)


openApiUri :: Config.AppConfig -> (Text, Text, Integer, Text)
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


openApiTableInfo :: Types.DbStructure -> Types.Table -> (Types.Table, [Types.Column], [Text])
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
  Wai.responseLBS HTTP.status404 [] ""


exactCount :: ApiRequest.ApiRequest -> Bool
exactCount apiRequest =
  ApiRequest.iPreferCount apiRequest == Just Types.ExactCount


estimatedCount :: ApiRequest.ApiRequest -> Bool
estimatedCount apiRequest =
  ApiRequest.iPreferCount apiRequest == Just Types.EstimatedCount


shouldCount :: ApiRequest.ApiRequest -> Bool
shouldCount apiRequest =
  exactCount apiRequest || estimatedCount apiRequest


topLevelRange :: ApiRequest.ApiRequest -> Ranges.Range Integer
topLevelRange apiRequest =
  ApiRequest.iTopLevelRange apiRequest


returnsScalar :: ApiRequest.ApiRequest -> Bool
returnsScalar apiRequest =
  case ApiRequest.iTarget apiRequest of
    ApiRequest.TargetProc proc _ ->
      Types.procReturnsScalar proc
    _ ->
      False


returnsSingle :: ApiRequest.ApiRequest -> Bool
returnsSingle apiRequest =
  case ApiRequest.iTarget apiRequest of
    ApiRequest.TargetProc proc _ ->
      Types.procReturnsSingle proc
    _ ->
      False


profileH :: ApiRequest.ApiRequest -> Maybe HTTP.Header
profileH apiRequest =
  contentProfileH <$> ApiRequest.iProfile apiRequest


readSqlParts
  :: Config.AppConfig
  -> Types.DbStructure
  -> ApiRequest.ApiRequest
  -> [Types.ContentType]
  -> Types.ContentType
  -> Types.QualifiedIdentifier
  -> Either
       Wai.Response
       (Hasql.Snippet, Hasql.Snippet, Maybe Types.FieldName, [Types.FieldName])
readSqlParts conf dbStructure apiRequest rawContentTypes contentType identifier=
  let
    readReq =
      DbRequestBuilder.readRequest
        (Types.qiSchema identifier)
        (Types.qiName identifier)
        (Config.configDbMaxRows conf)
        (Types.dbRelations dbStructure)
        apiRequest

    returnings :: Types.ReadRequest -> Either Wai.Response [Types.FieldName]
    returnings rr =
      Right (DbRequestBuilder.returningCols rr [])
  in
  (,,,) <$>
    (QueryBuilder.readRequestToQuery <$> readReq) <*>
    (QueryBuilder.readRequestToCountQuery <$> readReq) <*>
    (binaryField contentType rawContentTypes (returnsScalar apiRequest) =<< readReq) <*>
    (returnings =<< readReq)


mutateSqlParts
  :: Config.AppConfig
  -> Types.DbStructure
  -> ApiRequest.ApiRequest
  -> Types.QualifiedIdentifier
  -> Either Wai.Response (Hasql.Snippet, Hasql.Snippet)
mutateSqlParts conf dbStructure apiRequest identifier =
  let
    readReq =
      DbRequestBuilder.readRequest
        (Types.qiSchema identifier)
        (Types.qiName identifier)
        (Config.configDbMaxRows conf)
        (Types.dbRelations dbStructure)
        apiRequest

    mutReq =
      DbRequestBuilder.mutateRequest
        (Types.qiSchema identifier)
        (Types.qiName identifier)
        apiRequest
        (Types.tablePKCols dbStructure (Types.qiSchema identifier) (Types.qiName identifier))
        =<< readReq
  in
  (,) <$>
    (QueryBuilder.readRequestToQuery <$> readReq) <*>
    (QueryBuilder.mutateRequestToQuery <$> mutReq)


responseContentTypeOrError ::
  [Types.ContentType]
  -> [Types.ContentType]
  -> ApiRequest.Action
  -> ApiRequest.Target
  -> Either Wai.Response Types.ContentType
responseContentTypeOrError accepts rawContentTypes action target =
  let
    produces =
      requestContentTypes rawContentTypes action target
  in
  case ApiRequest.mutuallyAgreeable produces accepts of
    Just ct ->
      Right ct

    Nothing ->
      Left . Error.errorResponseFor . Error.ContentTypeError . map Types.toMime $ accepts


requestContentTypes
  :: [Types.ContentType]
  -> ApiRequest.Action
  -> ApiRequest.Target
  -> [Types.ContentType]
requestContentTypes rawContentTypes action target =
  case action of
    ApiRequest.ActionRead _ ->
      defaultContentTypes ++ rawContentTypes
    ApiRequest.ActionCreate ->
      defaultContentTypes
    ApiRequest.ActionUpdate ->
      defaultContentTypes
    ApiRequest.ActionDelete ->
      defaultContentTypes
    ApiRequest.ActionInvoke _ ->
      defaultContentTypes
        ++ rawContentTypes
        ++ [Types.CTOpenAPI | ApiRequest.tpIsRootSpec target]
    ApiRequest.ActionInspect _ ->
      [Types.CTOpenAPI, Types.CTApplicationJSON]
    ApiRequest.ActionInfo ->
      [Types.CTTextCSV]
    ApiRequest.ActionSingleUpsert ->
      defaultContentTypes


defaultContentTypes :: [Types.ContentType]
defaultContentTypes =
  [Types.CTApplicationJSON, Types.CTSingularJSON, Types.CTTextCSV]


{-
  | If raw(binary) output is requested, check that ContentType is one of the admitted rawContentTypes and that
  | `?select=...` contains only one field other than `*`
-}
binaryField ::
  Types.ContentType
  -> [Types.ContentType]
  -> Bool
  -> Types.ReadRequest
  -> Either Wai.Response (Maybe Types.FieldName)
binaryField ct rawContentTypes isScalarProc readReq
  | isScalarProc && ct `elem` rawContentTypes =
      Right $ Just "pgrst_scalar"
  | ct `elem` rawContentTypes =
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
  | otherwise = Right Nothing


locationH :: Types.TableName -> [Char8ByteString.ByteString] -> HTTP.Header
locationH tName fields =
  let
    locationFields = HTTP.renderSimpleQuery True $ splitKeyValue <$> fields
  in
    (HTTP.hLocation, "/" <> toS tName <> locationFields)


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
