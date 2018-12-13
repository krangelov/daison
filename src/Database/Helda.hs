{-# LANGUAGE ExistentialQuantification, TypeFamilies #-}
module Database.Helda
            ( Database, openDB, closeDB
            , Table, table
            , Index, index, withIndex
            , runSelda, AccessMode(..)
            , createTable, tryCreateTable
            , dropTable, tryDropTable
            , alterTable, renameTable
            , select, from, fromAt, fromIndexAt
            , insert, insertQuery, store, update
            ) where

import Foreign
import Foreign.C
import Data.Data
import Data.IORef
import Data.ByteString.Lazy(ByteString,toStrict,fromStrict)
import Data.ByteString.Unsafe(unsafeUseAsCStringLen,unsafePackCStringLen,unsafePackMallocCStringLen)
import qualified Data.Map as Map
import Control.Exception(Exception,throwIO,bracket,bracket_,onException)
import Control.Applicative
import Control.Monad
import Control.Monad.IO.Class

import Database.Helda.FFI
import Database.Helda.Serialize

-----------------------------------------------------------------
-- Access the database
-----------------------------------------------------------------

type Cookie = Word32
type Schema = Map.Map String (Int64,Int)
data Database = Database (Ptr Btree) (IORef (Cookie, Schema))

openDB :: String -> IO Database
openDB fpath = 
  withCString fpath $ \c_fpath ->
  alloca $ \ppBtree -> do
    checkSqlite3Error $ sqlite3BtreeOpen nullPtr c_fpath ppBtree 0 openFlags
    pBtree <- peek ppBtree
    schemaRef <- newIORef (-1,Map.empty)
    bracket_ (checkSqlite3Error $ sqlite3BtreeBeginTrans pBtree 0)
             (checkSqlite3Error $ sqlite3BtreeCommit pBtree)
             (fetchSchema pBtree schemaRef)
    return (Database pBtree schemaRef)

fetchSchema pBtree schemaRef = do
  (cookie,schema) <- readIORef schemaRef
  sqlite3BtreeLockTable pBtree 1 0
  cookie' <- alloca $ \pCookie -> do
               sqlite3BtreeGetMeta pBtree 1 pCookie
               peek pCookie
  if cookie == cookie'
    then return schema
    else do tables <- bracket (alloca $ \ppCursor -> do
                                 checkSqlite3Error $ sqlite3BtreeCursor pBtree 1 0 0 0 ppCursor
                                 peek ppCursor)
                              (\pCursor -> checkSqlite3Error $ sqlite3BtreeCloseCursor pCursor)
                              (\pCursor -> alloca $ \pRes -> do
                                             checkSqlite3Error $ sqlite3BtreeFirst pCursor pRes
                                             fetchEntries pCursor pRes)
            let schema = Map.fromList tables
            writeIORef schemaRef (cookie', schema)
            return schema
  where
    fetchEntries pCursor pRes = do
      res <- peek pRes
      if res == 0
        then do e <- fetchEntry pCursor
                checkSqlite3Error $ sqlite3BtreeNext pCursor pRes
                es <- fetchEntries pCursor pRes
                return (e:es)
        else return []

    fetchEntry pCursor = do
      key         <- alloca $ \pKey -> do
                       checkSqlite3Error $ sqlite3BtreeKeySize pCursor pKey
                       peek pKey
      (name,tnum) <- alloca $ \pSize -> do
                       checkSqlite3Error $ sqlite3BtreeDataSize pCursor pSize
                       peek pSize
                       ptr <- sqlite3BtreeDataFetch pCursor pSize
                       amt <- peek pSize
                       bs <- unsafePackCStringLen (castPtr ptr,fromIntegral amt)
                       return $! (deserialize (fromStrict bs))
      return (name,(key,tnum))

updateSchema pBtree schemaRef schema = do
  (cookie,_) <- readIORef schemaRef
  let cookie' = cookie+1
  checkSqlite3Error $ sqlite3BtreeUpdateMeta pBtree 1 cookie'
  writeIORef schemaRef (cookie',schema)

closeDB :: Database -> IO ()
closeDB (Database pBtree _) = do
  checkSqlite3Error $ sqlite3BtreeClose pBtree


-----------------------------------------------------------------
-- The monad
-----------------------------------------------------------------

newtype Selda a = Selda {doTransaction :: Database -> IO a}

data AccessMode = ReadWriteMode | ReadOnlyMode

toCMode ReadWriteMode = 1
toCMode ReadOnlyMode  = 0

runSelda :: Database -> AccessMode -> Selda a -> IO a
runSelda db@(Database pBtree schemaRef) m t =
  (do checkSqlite3Error $ sqlite3BtreeBeginTrans pBtree (toCMode m)
      r <- doTransaction t db
      checkSqlite3Error $ sqlite3BtreeCommit pBtree
      return r)
  `onException`
  (checkSqlite3Error $ sqlite3BtreeRollback pBtree sqlite_ABORT_ROLLBACK 0)

instance Functor Selda where
  fmap f (Selda m) = Selda (\db -> fmap f (m db))

instance Applicative Selda where
  pure x  = Selda (\db -> pure x)
  f <*> g = Selda (\db -> doTransaction f db <*> doTransaction g db)

instance Monad Selda where
  return x = Selda (\db -> return x)
  f >>= g  = Selda (\db -> doTransaction f db >>= \x -> doTransaction (g x) db)


-----------------------------------------------------------------
-- Access the tables
-----------------------------------------------------------------

type Key a     = Int64
data Table a   = Table String [(String,a -> Maybe ByteString)]

table :: String -> Table a
table name = Table name []

createTable :: Table a -> Selda ()
createTable (Table name indices) = Selda $ \db ->
  createTableHelper db name indices True

tryCreateTable :: Table a -> Selda ()
tryCreateTable (Table name indices) = Selda $ \db ->
  createTableHelper db name indices False

createTableHelper (Database pBtree schemaRef) name indices doFail = do
  schema <- fetchSchema pBtree schemaRef
  bracket (alloca $ \ppCursor -> do
             checkSqlite3Error $ sqlite3BtreeCursor pBtree 1 1 0 0 ppCursor
             peek ppCursor)
          (\pCursor -> checkSqlite3Error $ sqlite3BtreeCloseCursor pCursor)
          (\pCursor -> do res <- alloca $ \pRes -> do
                                   sqlite3BtreeLast pCursor pRes
                                   peek pRes
                          key <- if res /= 0
                                   then return 1
                                   else alloca $ \pKey -> do
                                          checkSqlite3Error $ sqlite3BtreeKeySize pCursor pKey
                                          fmap (+1) (peek pKey)
                          key_schema <- mkTable name btreeINTKEY pCursor key schema
                          (key,schema) <- foldM (\(key,schema) (name,_) -> mkTable name 0 pCursor key schema) key_schema indices
                          updateSchema pBtree schemaRef schema)
  where
    mkTable name flags pCursor key schema = do
      case Map.lookup name schema of
        Just _  -> if doFail
                     then throwAlreadyExists name
                     else return (key,schema)
        Nothing -> do tnum <- alloca $ \pTNum -> do
                                checkSqlite3Error $ sqlite3BtreeCreateTable pBtree pTNum flags
                                fmap fromIntegral (peek pTNum)
                      unsafeUseAsCStringLen (toStrict (serialize (name,tnum))) $ \(ptr,size) -> do
                        sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0
                      return (key+1,Map.insert name (key,tnum) schema)

dropTable :: Table a -> Selda ()
dropTable (Table name indices) = Selda $ \db ->
  dropTableHelper db name indices False

tryDropTable :: Table a -> Selda ()
tryDropTable (Table name indices) = Selda $ \db ->
  dropTableHelper db name indices True

dropTableHelper (Database pBtree schemaRef) name indices doFail = do
  schema <- fetchSchema pBtree schemaRef
  schema_ids   <- rmTable name (schema,[])
  (schema,ids) <- foldM (\schema_ids (name,_) -> rmTable name schema_ids) schema_ids indices
  bracket (alloca $ \ppCursor -> do
             checkSqlite3Error $ sqlite3BtreeCursor pBtree 1 1 0 0 ppCursor
             peek ppCursor)
          (\pCursor -> checkSqlite3Error $ sqlite3BtreeCloseCursor pCursor)
          (\pCursor -> mapM_ (rmSchema pCursor) ids)
  updateSchema pBtree schemaRef schema
  where
    rmTable name (schema,ids) =
      case Map.lookup name schema of
        Nothing         -> if doFail
                             then throwDoesn'tExist name
                             else return (schema,ids)
        Just (key,tnum) -> do alloca $ \piMoved -> do
                                checkSqlite3Error $ sqlite3BtreeDropTable pBtree (fromIntegral tnum) piMoved
                              return (Map.delete name schema,key:ids)

    rmSchema pCursor key = do
      alloca $ \pRes ->
        checkSqlite3Error $ sqlite3BtreeMovetoUnpacked pCursor nullPtr key 0 pRes
      checkSqlite3Error $ sqlite3BtreeDelete pCursor 0

alterTable :: (Data a, Data b) => Table a -> Table b -> (a -> b) -> Selda ()
alterTable tbl@(Table name _) (Table name' _) f = Selda $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  case Map.lookup name schema of
    Nothing         -> throwDoesn'tExist name
    Just (key,tnum) -> do when (name /= name' && Map.member name' schema) $
                            throwAlreadyExists name'
                          tnum' <- alloca $ \pTNum -> do
                                     checkSqlite3Error $ sqlite3BtreeCreateTable pBtree pTNum btreeINTKEY
                                     fmap fromIntegral (peek pTNum)
                          sqlite3BtreeLockTable pBtree (fromIntegral tnum) 0
                          bracket (alloca $ \ppCursor -> do
                                     checkSqlite3Error $ sqlite3BtreeCursor pBtree (fromIntegral tnum) 0 0 0 ppCursor
                                     peek ppCursor)
                                  (\pCursor -> checkSqlite3Error $ sqlite3BtreeCloseCursor pCursor)
                                  (\pSrcCursor -> bracket (alloca $ \ppCursor -> do
                                                             checkSqlite3Error $ sqlite3BtreeCursor pBtree (fromIntegral tnum') 1 0 0 ppCursor
                                                             peek ppCursor)
                                                          (\pCursor -> checkSqlite3Error $ sqlite3BtreeCloseCursor pCursor)
                                                          (\pDstCursor -> step sqlite3BtreeFirst pSrcCursor pDstCursor))
                          bracket (alloca $ \ppCursor -> do
                                     checkSqlite3Error $ sqlite3BtreeCursor pBtree 1 1 0 0 ppCursor
                                     peek ppCursor)
                                  (\pCursor -> checkSqlite3Error $ sqlite3BtreeCloseCursor pCursor)
                                  (\pCursor -> unsafeUseAsCStringLen (toStrict (serialize (name',tnum'))) $ \(ptr,size) -> do
                                                 sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0)
                          alloca $ \piMoved -> do
                            checkSqlite3Error $ sqlite3BtreeDropTable pBtree (fromIntegral tnum) piMoved
                          updateSchema pBtree schemaRef (Map.insert name' (key,tnum') (Map.delete name schema))
                          return ()
  where
    step moveCursor pSrcCursor pDstCursor = do
      res <- (alloca $ \pRes -> do
                checkSqlite3Error $ moveCursor pSrcCursor pRes
                peek pRes)
      if res /= 0
        then return ()
        else do key  <- alloca $ \pKey -> do
                          checkSqlite3Error $ sqlite3BtreeKeySize pSrcCursor pKey
                          peek pKey
                size <- alloca $ \pSize -> do
                          checkSqlite3Error $ sqlite3BtreeDataSize pSrcCursor pSize
                          peek pSize
                allocaBytes (fromIntegral size) $ \ptr -> do
                  checkSqlite3Error $ sqlite3BtreeData pSrcCursor 0 size ptr
                  bs <- unsafePackCStringLen (castPtr ptr,fromIntegral size)
                  let bs' = (toStrict . serialize . f . deserialize . fromStrict) bs
                  unsafeUseAsCStringLen bs' $ \(ptr',size') -> do
                    checkSqlite3Error $ sqlite3BtreeInsert pDstCursor nullPtr key (castPtr ptr') (fromIntegral size') 0 0 0
                step sqlite3BtreeNext pSrcCursor pDstCursor

renameTable :: Table a -> String -> Selda ()
renameTable (Table name _) name' = Selda $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  case Map.lookup name schema of
    Nothing         -> throwDoesn'tExist name
    Just (key,tnum) -> do when (name /= name' && Map.member name' schema) $
                            throwAlreadyExists name'
                          bracket (alloca $ \ppCursor -> do
                                     checkSqlite3Error $ sqlite3BtreeCursor pBtree 1 1 0 0 ppCursor
                                     peek ppCursor)
                                  (\pCursor -> checkSqlite3Error $ sqlite3BtreeCloseCursor pCursor)
                                  (\pCursor -> unsafeUseAsCStringLen (toStrict (serialize (name',tnum))) $ \(ptr,size) -> do
                                                 sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0)
                          updateSchema pBtree schemaRef (Map.insert name' (key,tnum) (Map.delete name schema))
                          return ()


openTable pBtree schema (Table name _) m =
  case Map.lookup name schema of
    Just (_,tnum) -> do sqlite3BtreeLockTable pBtree (fromIntegral tnum) (fromIntegral cmode)
                        alloca $ \ppCursor -> do
                          checkSqlite3Error $ sqlite3BtreeCursor pBtree (fromIntegral tnum) cmode 0 0 ppCursor
                          peek ppCursor
    Nothing       -> throwDoesn'tExist name
  where
    cmode = toCMode m

closeTable pCursor = checkSqlite3Error $ sqlite3BtreeCloseCursor pCursor


withTable pBtree schema (Table name _) m io =
  case Map.lookup name schema of
    Just (_,tnum) -> do sqlite3BtreeLockTable pBtree (fromIntegral tnum) (fromIntegral cmode)
                        bracket (alloca $ \ppCursor -> do
                                   checkSqlite3Error $ sqlite3BtreeCursor pBtree (fromIntegral tnum) cmode 0 0 ppCursor
                                   peek ppCursor)
                                (\pCursor -> checkSqlite3Error $ sqlite3BtreeCloseCursor pCursor)
                                io
    Nothing       -> throwDoesn'tExist name
  where
    cmode = toCMode m


-----------------------------------------------------------------
-- Access the indices
-----------------------------------------------------------------

data    Index a b = Index String (a -> Maybe b)

index :: String -> (a -> Maybe b) -> Index a b
index = Index

withIndex :: Data b => Table a -> Index a b -> Table a
withIndex (Table tname indices) (Index iname fn) = Table tname ((iname,fmap serialize . fn) : indices)


-----------------------------------------------------------------
-- Queries
-----------------------------------------------------------------

newtype Query a = Query {doQuery :: Ptr Btree -> Schema -> IO (QSeq a)}

data QSeq a
  = Output a (IO (QSeq a))
  | Done

nilQSeq = return Done

appendQSeq map Done            rest' = rest'
appendQSeq map (Output x rest) rest' = return (Output (map x) cont)
  where
    cont = do r <- rest
              appendQSeq map r rest'

instance Functor Query where
  fmap f q = Query (\pBtree schema -> fmap mapSeq (doQuery q pBtree schema))
             where mapSeq (Output a m) = Output (f a) (fmap mapSeq m)
                   mapSeq Done         = Done

instance Applicative Query where
  pure x  = Query (\pBtree schema -> pure (Output x nilQSeq))
  f <*> g = Query (\pBtree schema -> doQuery f pBtree schema  >>= loop pBtree schema )
    where
      loop pBtree schema Done          = return Done
      loop pBtree schema (Output x f') = do s <- doQuery g pBtree schema 
                                            appendQSeq x s (f' >>= loop pBtree schema )

instance Alternative Query where
  empty   = Query (\pBtree schema -> return Done)
  f <|> g = Query (\pBtree schema -> do r <- doQuery f pBtree schema
                                        appendQSeq id r (doQuery g pBtree schema))

instance Monad Query where
  return x  = Query (\pBtree schema  -> return (Output x nilQSeq))
  f >>= g   = Query (\pBtree schema  -> doQuery f pBtree schema  >>= loop pBtree schema)
    where
      loop pBtree schema Done          = return Done
      loop pBtree schema (Output x f') = do s <- doQuery (g x) pBtree schema
                                            appendQSeq id s (f' >>= loop pBtree schema)

instance MonadPlus Query where
  mzero     = Query (\pBtree schema -> return Done)
  mplus f g = Query (\pBtree schema -> do r <- doQuery f pBtree schema
                                          appendQSeq id r (doQuery g pBtree schema))

select :: Query a -> Selda [a]
select q = Selda $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  seq <- doQuery q pBtree schema
  loop seq
  where
    loop Done         = return []
    loop (Output x r) = do xs <- r >>= loop
                           return (x:xs)


-----------------------------------------------------------------
-- Select
-----------------------------------------------------------------

class From s where
  type K s
  type V s

  from   :: s -> Query (K s,V s)
  fromAt :: s -> K s -> Query (V s)

instance Data a => From (Table a) where
  type K (Table a) = (Key a)
  type V (Table a) = a

  from tbl = Query $ \pBtree schema -> do
    pCursor <- openTable pBtree schema tbl ReadOnlyMode
    step sqlite3BtreeFirst pCursor
    where
      step moveCursor pCursor = do
        res <- (alloca $ \pRes -> do
                  checkSqlite3Error $ moveCursor pCursor pRes
                  peek pRes)
               `onException`
               (sqlite3BtreeCloseCursor pCursor)
        if res /= 0
          then do sqlite3BtreeCloseCursor pCursor
                  return Done
          else (do key  <- alloca $ \pKey -> do
                             checkSqlite3Error $ sqlite3BtreeKeySize pCursor pKey
                             peek pKey
                   size <- alloca $ \pSize -> do
                             checkSqlite3Error $ sqlite3BtreeDataSize pCursor pSize
                             peek pSize
                   ptr  <- mallocBytes (fromIntegral size)
                   checkSqlite3Error $ sqlite3BtreeData pCursor 0 size ptr
                   bs <- unsafePackMallocCStringLen (castPtr ptr,fromIntegral size)
                   return (Output (key, deserialize (fromStrict bs)) (step sqlite3BtreeNext pCursor)))
               `onException`
               (sqlite3BtreeCloseCursor pCursor)

  fromAt tbl key = Query $ \pBtree schema ->
    withTable pBtree schema tbl ReadOnlyMode $ \pCursor -> do
      res <- alloca $ \pRes -> do
               checkSqlite3Error $ sqlite3BtreeMovetoUnpacked pCursor nullPtr key 0 pRes
               peek pRes
      if res /= 0
        then return Done
        else do alloca $ \pSize -> do
                  checkSqlite3Error $ sqlite3BtreeDataSize pCursor pSize
                  size <- peek pSize
                  ptr <- mallocBytes (fromIntegral size)
                  checkSqlite3Error $ sqlite3BtreeData pCursor 0 size ptr
                  bs <- unsafePackMallocCStringLen (castPtr ptr,fromIntegral size)
                  return (Output (deserialize (fromStrict bs)) nilQSeq)

instance Data b => From (Index a b) where
  type K (Index a b) = b
  type V (Index a b) = Key a

fromIndexAt :: (Data a,Data b) => Table a -> Index a b -> b -> Query (Key a,a)
fromIndexAt tbl idx x = do key <- fromAt idx x
                           val <- fromAt tbl key
                           return (key,val)


-----------------------------------------------------------------
-- Insert
-----------------------------------------------------------------

insert :: Data a => Table a -> a -> Selda (Key a)
insert tbl val = Selda $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  withTable pBtree schema tbl ReadWriteMode $ \pCursor -> do
    res <- alloca $ \pRes -> do
             sqlite3BtreeLast pCursor pRes
             peek pRes
    key <- if res /= 0
             then return 1
             else alloca $ \pKey -> do
                    checkSqlite3Error $ sqlite3BtreeKeySize pCursor pKey
                    fmap (+1) (peek pKey)
    unsafeUseAsCStringLen (toStrict (serialize val)) $ \(ptr,size) -> do
      checkSqlite3Error $ sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0
    return key

insertQuery :: Data a => Table a -> Query a -> Selda (Key a,Key a)
insertQuery tbl q = Selda $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  withTable pBtree schema tbl ReadWriteMode $ \pCursor -> do
    res <- alloca $ \pRes -> do
             sqlite3BtreeLast pCursor pRes
             peek pRes
    key <- if res /= 0
             then return 1
             else alloca $ \pKey -> do
                    checkSqlite3Error $ sqlite3BtreeKeySize pCursor pKey
                    fmap (+1) (peek pKey)
    seq <- doQuery q pBtree schema
    key' <- loop pCursor key seq
    return (key,key')
  where
    loop pCursor key Done           = return key
    loop pCursor key (Output val r) = do unsafeUseAsCStringLen (toStrict (serialize val)) $ \(ptr,size) -> do
                                           checkSqlite3Error $ sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0
                                         r >>= loop pCursor (key+1)


-----------------------------------------------------------------
-- Update
-----------------------------------------------------------------

store :: Data a => Table a -> Key a -> a -> Selda ()
store tbl key val = Selda $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  withTable pBtree schema tbl ReadWriteMode $ \pCursor ->
    unsafeUseAsCStringLen (toStrict (serialize val)) $ \(ptr,size) -> do
      checkSqlite3Error $ sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0

update :: Data a => Table a -> (Key a -> b -> a) -> Query (Key a,b) -> Selda ()
update tbl f q = Selda $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  withTable pBtree schema tbl ReadWriteMode $ \pCursor -> do
     seq <- doQuery q pBtree schema
     loop pCursor seq
  where
    loop pCursor Done               = return ()
    loop pCursor (Output (key,x) r) = do unsafeUseAsCStringLen (toStrict (serialize (f key x))) $ \(ptr,size) -> do
                                           checkSqlite3Error $ sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0
                                         r >>= loop pCursor


-----------------------------------------------------------------
-- Exceptions
-----------------------------------------------------------------

newtype DatabaseError = DatabaseError String
     deriving (Show, Typeable)

instance Exception DatabaseError

checkSqlite3Error m = do
  rc <- m
  if rc /= sqlite_OK
    then do msg <- sqlite3BtreeErrName rc >>= peekCString
            throwIO (DatabaseError msg)
    else return ()

throwAlreadyExists name =
  throwIO (DatabaseError ("Table "++name++" already exists"))

throwDoesn'tExist name =
  throwIO (DatabaseError ("Table "++name++" does not exist"))
