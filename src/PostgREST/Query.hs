{-# LANGUAGE RecordWildCards #-}
module PostgREST.Query where

import Control.Monad.Except (liftEither)

import qualified Hasql.Statement as SQL

import qualified PostgREST.ApiRequest             as ApiRequest
import qualified PostgREST.Error                  as Error
import qualified PostgREST.Query.Builder          as QueryBuilder
import qualified PostgREST.Query.DbRequestBuilder as ReqBuilder
import qualified PostgREST.Query.Statements       as Statements

import PostgREST.ApiRequest              (ApiRequest (..),
                                          Target (..))
import PostgREST.ApiRequest.Preferences  (PreferCount (..),
                                          PreferParameters (..),
                                          shouldCount)
import PostgREST.Config                  (AppConfig (..),
                                          rawContentTypes)
import PostgREST.ContentType             (ContentType (..))
import PostgREST.DbStructure             (DbStructure (..),
                                          tablePKCols)
import PostgREST.DbStructure.Identifiers (FieldName,
                                          QualifiedIdentifier (..))
import PostgREST.DbStructure.Proc        (ProcDescription (..))
import PostgREST.Error                   (Error)
import PostgREST.Query.Types             (ReadRequest, fstFieldNames)

import qualified PostgREST.DbStructure.Proc as Proc

import Protolude hiding (Handler)


type Handler = ExceptT Error


readStatement :: Monad m => AppConfig -> DbStructure -> ApiRequest -> ContentType -> QualifiedIdentifier -> Handler m (SQL.Statement () Statements.ResultsWithCount)
readStatement config@AppConfig{..} dbStructure apiRequest@ApiRequest{..} contentType identifier = do
  req <- readRequest config dbStructure apiRequest identifier
  bField <- binaryField config apiRequest contentType req
  let countQuery = QueryBuilder.readRequestToCountQuery req
  return $ Statements.createReadStatement
    (QueryBuilder.readRequestToQuery req)
    (if iPreferCount == Just EstimatedCount then
       -- LIMIT maxRows + 1 so we can determine below that maxRows was surpassed
       QueryBuilder.limitedQuery countQuery ((+ 1) <$> configDbMaxRows)
     else
       countQuery
    )
    (contentType == CTSingularJSON)
    (shouldCount iPreferCount)
    (contentType == CTTextCSV)
    bField
    (pgVersion dbStructure)
    configDbPreparedStatements

readRequest :: Monad m => AppConfig -> DbStructure -> ApiRequest -> QualifiedIdentifier -> Handler m ReadRequest
readRequest AppConfig{..} dbStructure apiRequest QualifiedIdentifier{..} =
  liftEither $
    ReqBuilder.readRequest qiSchema qiName configDbMaxRows
      (dbRelations dbStructure)
      apiRequest

explainStatement :: Monad m => AppConfig -> DbStructure -> ApiRequest -> QualifiedIdentifier -> Handler m (SQL.Statement () (Maybe Int64))
explainStatement ctxConfig ctxDbStructure ctxApiRequest identifier = do
  req <- readRequest ctxConfig ctxDbStructure ctxApiRequest identifier
  return $
    Statements.createExplainStatement
      (QueryBuilder.readRequestToCountQuery req)
      (configDbPreparedStatements ctxConfig)

callProcStatement :: Monad m => AppConfig -> DbStructure -> ApiRequest -> ContentType -> ProcDescription -> Handler m (SQL.Statement () Statements.ProcResults)
callProcStatement ctxConfig ctxDbStructure ctxApiRequest ctxContentType proc = do
  let
    ApiRequest{..} = ctxApiRequest

    identifier =
      QualifiedIdentifier
        (pdSchema proc)
        (fromMaybe (pdName proc) $ Proc.procTableName proc)

    returnsSingle (ApiRequest.TargetProc target _) = Proc.procReturnsSingle target
    returnsSingle _                                = False

  req <- readRequest ctxConfig ctxDbStructure ctxApiRequest identifier
  bField <- binaryField ctxConfig ctxApiRequest ctxContentType req

  return $
    Statements.callProcStatement
        (returnsScalar iTarget)
        (returnsSingle iTarget)
        (QueryBuilder.requestToCallProcQuery
          (QualifiedIdentifier (pdSchema proc) (pdName proc))
          (Proc.specifiedProcArgs iColumns proc)
          iPayload
          (returnsScalar iTarget)
          iPreferParameters
          (ReqBuilder.returningCols req [])
        )
        (QueryBuilder.readRequestToQuery req)
        (QueryBuilder.readRequestToCountQuery req)
        (shouldCount iPreferCount)
        (ctxContentType == CTSingularJSON)
        (ctxContentType == CTTextCSV)
        (iPreferParameters == Just MultipleObjects)
        bField
        (pgVersion ctxDbStructure)
        (configDbPreparedStatements ctxConfig)

-- |
-- If raw(binary) output is requested, check that ContentType is one of the admitted
-- rawContentTypes and that`?select=...` contains only one field other than `*`
binaryField :: Monad m => AppConfig -> ApiRequest -> ContentType -> ReadRequest -> Handler m (Maybe FieldName)
binaryField ctxConfig ctxApiRequest ctxContentType readReq
  | returnsScalar (iTarget ctxApiRequest) && ctxContentType `elem` rawContentTypes ctxConfig =
      return $ Just "pgrst_scalar"
  | ctxContentType `elem` rawContentTypes ctxConfig =
      let
        fldNames = fstFieldNames readReq
        fieldName = headMay fldNames
      in
      if length fldNames == 1 && fieldName /= Just "*" then
        return fieldName
      else
        throwError $ Error.BinaryFieldError ctxContentType
  | otherwise =
      return Nothing

returnsScalar :: ApiRequest.Target -> Bool
returnsScalar (TargetProc proc _) = Proc.procReturnsScalar proc
returnsScalar _                   = False

writeStatement :: Monad m => AppConfig -> DbStructure -> ApiRequest -> ContentType -> QualifiedIdentifier -> Bool -> [Text] -> Handler m (SQL.Statement () Statements.ResultsWithCount)
writeStatement ctxConfig ctxDbStructure ctxApiRequest ctxContentType identifier@QualifiedIdentifier{..} isInsert pkCols = do
  readReq <- readRequest ctxConfig ctxDbStructure ctxApiRequest identifier

  mutateReq <-
    liftEither $
      ReqBuilder.mutateRequest qiSchema qiName ctxApiRequest
        (tablePKCols ctxDbStructure qiSchema qiName)
        readReq

  return $ Statements.createWriteStatement
    (QueryBuilder.readRequestToQuery readReq)
    (QueryBuilder.mutateRequestToQuery mutateReq)
    (ctxContentType == CTSingularJSON)
    isInsert
    (ctxContentType == CTTextCSV)
    (iPreferRepresentation ctxApiRequest)
    pkCols
    (pgVersion ctxDbStructure)
    (configDbPreparedStatements ctxConfig)
