{-# LANGUAGE ExistentialQuantification, TypeFamilies, MultiParamTypeClasses #-}
module Database.Daison
            ( Database, openDB, closeDB
            , Key
            , Table, table, tableName
            , Index, index, listIndex, maybeIndex, withIndex
            , indexedTable, applyIndex
            , runDaison, AccessMode(..), Daison
            , createTable, tryCreateTable
            , dropTable, tryDropTable
            , alterTable, renameTable
            , withForeignKey, withCascadeDelete, withCascadeUpdate
            , Query
            , select, anyOf, listAll
            , foldlQ, foldl1Q, foldrQ
            , groupQ
            , At, at
            , Restriction, everything, asc, desc, (^>=), (^>), (^<=), (^<)
            , From(K,V), from, FromIndex(VI,VL), fromIndex, fromList

            , store
            , insert, insert_
            , update, update_
            , delete, delete_
            ) where

import Foreign
import Foreign.C
import Data.Data
import Data.IORef
import Data.ByteString(ByteString,append,null,empty)
import Data.ByteString.Unsafe(unsafeUseAsCStringLen,unsafePackCStringLen,unsafePackMallocCStringLen)
import Data.Maybe(maybeToList, fromMaybe)
import qualified Data.Map as Map
import qualified Data.List as List
import Control.Exception(Exception,throwIO,bracket,bracket_,onException)
import Control.Applicative
import Control.Monad
import Control.Monad.IO.Class

import Database.Daison.FFI
import Database.Daison.Serialize

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
                       return $! (deserialize bs)
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

newtype Daison a = Daison {doTransaction :: Database -> IO a}

data AccessMode = ReadWriteMode | ReadOnlyMode

toCMode ReadWriteMode = 1
toCMode ReadOnlyMode  = 0

runDaison :: Database -> AccessMode -> Daison a -> IO a
runDaison db@(Database pBtree schemaRef) m t =
  (do checkSqlite3Error $ sqlite3BtreeBeginTrans pBtree (toCMode m)
      r <- doTransaction t db
      checkSqlite3Error $ sqlite3BtreeCommit pBtree
      return r)
  `onException`
  (checkSqlite3Error $ sqlite3BtreeRollback pBtree sqlite_ABORT_ROLLBACK 0)

instance Functor Daison where
  fmap f (Daison m) = Daison (\db -> fmap f (m db))

instance Applicative Daison where
  pure x  = Daison (\db -> pure x)
  f <*> g = Daison (\db -> doTransaction f db <*> doTransaction g db)

instance Monad Daison where
  return x = Daison (\db -> return x)
  f >>= g  = Daison (\db -> doTransaction f db >>= \x -> doTransaction (g x) db)

instance MonadIO Daison where
  liftIO f = Daison (\db -> f)

-----------------------------------------------------------------
-- Access the tables
-----------------------------------------------------------------

type Key a     = Int64
data Table a   = Table String [(String,a -> [ByteString])] [Ptr Btree -> Schema -> Key a -> IO ()]

table :: String -> Table a
table name = Table name [] []

tableName :: Table a -> String
tableName (Table name _ _) = name

createTable :: Table a -> Daison ()
createTable (Table name indices _) = Daison $ \db ->
  createTableHelper db name indices True

tryCreateTable :: Table a -> Daison ()
tryCreateTable (Table name indices _) = Daison $ \db ->
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
                          (key,schema) <- foldM (\(key,schema) (name,_) -> mkTable name btreeBLOBKEY pCursor key schema) key_schema indices
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
                      unsafeUseAsCStringLen (serialize (name,tnum)) $ \(ptr,size) -> do
                        sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0
                      return (key+1,Map.insert name (key,tnum) schema)

dropTable :: Table a -> Daison ()
dropTable (Table name indices _) = Daison $ \db ->
  dropTableHelper db name indices True

tryDropTable :: Table a -> Daison ()
tryDropTable (Table name indices _) = Daison $ \db ->
  dropTableHelper db name indices False

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
        checkSqlite3Error $ sqlite3BtreeMoveTo pCursor nullPtr key 0 pRes
      checkSqlite3Error $ sqlite3BtreeDelete pCursor 0

alterTable :: (Data a, Data b) => Table a -> Table b -> (a -> b) -> Daison ()
alterTable tbl@(Table name _ _) (Table name' _ _) f = Daison $ \(Database pBtree schemaRef) -> do
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
                                  (\pCursor -> unsafeUseAsCStringLen (serialize (name',tnum')) $ \(ptr,size) -> do
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
                  let bs' = (serialize . f . deserialize) bs
                  unsafeUseAsCStringLen bs' $ \(ptr',size') -> do
                    checkSqlite3Error $ sqlite3BtreeInsert pDstCursor nullPtr key (castPtr ptr') (fromIntegral size') 0 0 0
                step sqlite3BtreeNext pSrcCursor pDstCursor

renameTable :: Table a -> String -> Daison ()
renameTable (Table name _ _) name' = Daison $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  case Map.lookup name schema of
    Nothing         -> throwDoesn'tExist name
    Just (key,tnum) -> do when (name /= name' && Map.member name' schema) $
                            throwAlreadyExists name'
                          bracket (alloca $ \ppCursor -> do
                                     checkSqlite3Error $ sqlite3BtreeCursor pBtree 1 1 0 0 ppCursor
                                     peek ppCursor)
                                  (\pCursor -> checkSqlite3Error $ sqlite3BtreeCloseCursor pCursor)
                                  (\pCursor -> unsafeUseAsCStringLen (serialize (name',tnum)) $ \(ptr,size) -> do
                                                 sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0)
                          updateSchema pBtree schemaRef (Map.insert name' (key,tnum) (Map.delete name schema))
                          return ()


openBtreeCursor pBtree schema name m n x =
  case Map.lookup name schema of
    Just (_,tnum) -> do sqlite3BtreeLockTable pBtree (fromIntegral tnum) (fromIntegral cmode)
                        alloca $ \ppCursor -> do
                          checkSqlite3Error $ sqlite3BtreeCursor pBtree (fromIntegral tnum) cmode n x ppCursor
                          peek ppCursor
    Nothing       -> throwDoesn'tExist name
  where
    cmode = toCMode m

closeBtreeCursor pCursor = checkSqlite3Error $ sqlite3BtreeCloseCursor pCursor


withTableCursor pBtree schema (Table name _ _) m io =
  bracket (openBtreeCursor pBtree schema name m 0 0)
          (closeBtreeCursor)
          io

withIndexCursors pBtree schema (Table _ indices _) m io =
  forM_ indices $ \(name,fn) ->
    bracket (openBtreeCursor pBtree schema name m 1 1)
            (closeBtreeCursor)
            (io fn)

withIndexCursor pBtree schema (Index tbl name fn) m io =
  bracket (openBtreeCursor pBtree schema name m 1 1)
          (closeBtreeCursor)
          (io (map serialize . fn))

runTriggers pBtree schema (Table _ _ ts) key = forM_ ts (\t -> t pBtree schema key)

-----------------------------------------------------------------
-- Access the indices
-----------------------------------------------------------------

data    Index a b = Index (Table a) String (a -> [b])

index :: Table a -> String -> (a -> b) -> Index a b
index tbl iname f = listIndex tbl iname ((:[]) . f)

listIndex :: Table a -> String -> (a -> [b]) -> Index a b
listIndex ~tbl@(Table tname indices _) iname = Index tbl (tname++"_"++iname)

maybeIndex :: Table a -> String -> (a -> Maybe b) -> Index a b
maybeIndex tbl iname f = listIndex tbl iname (maybeToList . f)

withIndex :: Data b => Table a -> Index a b -> Table a
withIndex (Table tname indices fkeys) (Index _ iname fn) = Table tname ((iname,map serialize . fn) : indices) fkeys

indexedTable :: Index a b -> Table a
indexedTable (Index tbl _ _) = tbl

applyIndex :: Index a b -> a -> [b]
applyIndex (Index _ _ fn) = fn

-----------------------------------------------------------------
-- Foreign Keys
-----------------------------------------------------------------

withForeignKey :: Table a -> Index b (Key a) -> Table a
withForeignKey t@(Table tname indices fkeys) index = Table tname indices (check:fkeys)
  where
    check pBtree schema key = do
      bs <- fromAt' return index (At key) pBtree schema
      unless (Data.ByteString.null bs) $
        throwIO (DatabaseError ("The foreign key from "++tableName (indexedTable index)++" to "++tname++" is violated"))

withCascadeDelete :: Data b => Table a -> Index b (Key a) -> Table a
withCascadeDelete (Table tname indices fkeys) index = Table tname indices (cascade:fkeys)
  where
    cascade pBtree schema key =
      withTableCursor pBtree schema tbl ReadWriteMode $ \pCursor ->
        fromAt' (deleteForeign pCursor) index (At key) pBtree schema
      where
        tbl = indexedTable index

        deleteForeign pCursor bs =
          case deserializeKey bs of
            Nothing       -> do return ()
            Just (key,bs) -> do res <- alloca $ \pRes -> do
                                         checkSqlite3Error $ sqlite3BtreeMoveTo pCursor nullPtr key 0 pRes
                                         peek pRes
                                when (res == 0) $ do
                                  runTriggers pBtree schema tbl key
                                  deleteIndices pBtree schema tbl key pCursor
                                  checkSqlite3Error $ sqlite3BtreeDelete pCursor 0
                                deleteForeign pCursor bs

withCascadeUpdate :: Data b => Table a -> (Index b (Key a), b -> b) -> Table a
withCascadeUpdate (Table tname indices fkeys) (index,f) = Table tname indices (cascade:fkeys)
  where
    cascade pBtree schema key =
      withTableCursor pBtree schema tbl ReadWriteMode $ \pCursor ->
        fromAt' (updateForeign pCursor) index (At key) pBtree schema
      where
        tbl = indexedTable index

        updateForeign pCursor bs =
          case deserializeKey bs of
            Nothing       -> do return ()
            Just (key,bs) -> do res <- alloca $ \pRes -> do
                                         checkSqlite3Error $ sqlite3BtreeMoveTo pCursor nullPtr key 0 pRes
                                         peek pRes
                                when (res == 0) $ do
                                  val <- deleteIndices pBtree schema tbl key pCursor
                                  let val' = f val
                                  unsafeUseAsCStringLen (serialize val') $ \(ptr,size) ->
                                    checkSqlite3Error $ sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0
                                  insertIndices pBtree schema tbl key val'
                                updateForeign pCursor bs

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

select :: Query a -> Daison [a]
select q = Daison $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  seq <- doQuery q pBtree schema
  loop seq
  where
    loop Done         = return []
    loop (Output x r) = do xs <- r >>= loop
                           return (x:xs)

listAll :: Query a -> Query [a]
listAll q = Query $ \pBtree schema -> do
  seq <- doQuery q pBtree schema
  rs <- loop seq
  return (Output rs nilQSeq)
  where
    loop Done         = return []
    loop (Output x r) = do xs <- r >>= loop
                           return (x:xs)

anyOf :: [a] -> Query a
anyOf xs = Query (\pBtree schema -> loop xs)
  where loop []     = return Done
        loop (x:xs) = return (Output x (loop xs))

foldlQ :: (b -> a -> b) -> b -> Query a -> Query b
foldlQ f x q = Query $ \pBtree schema -> do
    seq <- doQuery q pBtree schema
    x <- loop x seq
    return (Output x nilQSeq)
    where
      loop x Done         = return x
      loop x (Output y r) = r >>= loop (f x y)

foldl1Q :: (a -> a -> a) -> Query a -> Query a
foldl1Q f q = Query $ \pBtree schema -> do
    seq <- doQuery q pBtree schema
    case seq of
      Done       -> return Done
      Output x r -> do x <- r >>= loop x
                       return (Output x nilQSeq)
    where
      loop x Done         = return x
      loop x (Output y r) = r >>= loop (f x y)

foldrQ :: (a -> b -> b) -> b -> Query a -> Query b
foldrQ f x q = Query $ \pBtree schema -> do
    seq <- doQuery q pBtree schema
    x <- loop seq
    return (Output x nilQSeq)
    where
      loop Done         = return x
      loop (Output x r) = do y <- r >>= loop
                             return (f x y)

groupQ :: Ord b => (a -> b) -> Query a -> Query [a]
groupQ f q = Query $ \pBtree schema -> do
    seq <- doQuery q pBtree schema
    loop Map.empty seq
    where
      loop m Done         = Map.foldr yield nilQSeq m
      loop m (Output x r) = r >>= loop (add x m)

      add   x = Map.alter (Just . (x:) . fromMaybe []) (f x)
      yield x = return . Output (reverse x)

-----------------------------------------------------------------
-- Select
-----------------------------------------------------------------

data Restriction a
  = Restriction (IntervalBoundry a) (IntervalBoundry a) Order

data Order
  = Asc | Desc

data IntervalBoundry a
  = Including a
  | Excluding a
  | Unbounded

everything = asc

asc  = Restriction Unbounded Unbounded Asc
desc = Restriction Unbounded Unbounded Desc

infixl 4 ^>=, ^>, ^<=, ^<

(^>=) :: Restriction a -> a -> Restriction a
(Restriction s e Asc ) ^>= x = Restriction (Including x) e Asc
(Restriction s e Desc) ^>= x = Restriction s (Including x) Desc

(^>)  :: Restriction a -> a -> Restriction a
(Restriction s e Asc ) ^> x = Restriction (Excluding x) e Asc
(Restriction s e Desc) ^> x = Restriction s (Excluding x) Desc

(^<=) :: Restriction a -> a -> Restriction a
(Restriction s e Asc ) ^<= x = Restriction s (Including x) Asc
(Restriction s e Desc) ^<= x = Restriction (Including x) e Desc

(^<)  :: Restriction a -> a -> Restriction a
(Restriction s e Asc ) ^< x = Restriction s (Excluding x) Asc
(Restriction s e Desc) ^< x = Restriction (Excluding x) e Desc



newtype At a = At a

at = At



class From s (r :: * -> *) where
  type K s
  type V s r

  from :: s -> r (K s) -> Query (V s r)


instance Data a => From (Table a) At where
  type K (Table a)    = Key a
  type V (Table a) At = a

  from tbl (At key) = Query $ \pBtree schema ->
    withTableCursor pBtree schema tbl ReadOnlyMode $ \pCursor -> do
      res <- alloca $ \pRes -> do
               checkSqlite3Error $ sqlite3BtreeMoveTo pCursor nullPtr key 0 pRes
               peek pRes
      if res /= 0
        then return Done
        else do alloca $ \pSize -> do
                  checkSqlite3Error $ sqlite3BtreeDataSize pCursor pSize
                  size <- peek pSize
                  ptr <- mallocBytes (fromIntegral size)
                  bs <- unsafePackMallocCStringLen (castPtr ptr,fromIntegral size)
                  checkSqlite3Error $ sqlite3BtreeData pCursor 0 size ptr
                  return (Output (deserialize bs) nilQSeq)

instance Data a => From (Table a) Restriction where
  type K (Table a)             = Key a
  type V (Table a) Restriction = (Key a,a)

  from (Table name _ _) (Restriction s e order) = Query $ \pBtree schema -> do
    pCursor <- openBtreeCursor pBtree schema name ReadOnlyMode 0 0
    step (start s) pCursor
    where
      start (Including key) pCursor pRes = do
        rc <- sqlite3BtreeMoveTo pCursor nullPtr key 0 pRes
        case order of
          Asc  -> do when (rc == sqlite_OK) $ do res <- peek pRes
                                                 when (res > 0) $ poke pRes 0
                     return rc
          Desc -> do if rc == sqlite_OK
                       then do res <- peek pRes
                               if res > 0 
                                 then sqlite3BtreePrevious pCursor pRes
                                 else return rc
                       else return rc
      start (Excluding key) pCursor pRes = do
        rc <- sqlite3BtreeMoveTo pCursor nullPtr key 0 pRes
        if rc == sqlite_OK
          then do res <- peek pRes
                  case order of
                    Asc  | res == 0 -> sqlite3BtreeNext pCursor pRes
                    Desc | res >= 0 -> sqlite3BtreePrevious pCursor pRes
                    _               -> return rc
          else return rc
      start Unbounded pCursor pRes =
        case order of
          Asc  -> sqlite3BtreeFirst pCursor pRes
          Desc -> sqlite3BtreeLast  pCursor pRes

      moveNext pCursor pRes = do
        rc <- case order of
                Asc  -> sqlite3BtreeNext     pCursor pRes
                Desc -> sqlite3BtreePrevious pCursor pRes
        if rc /= sqlite_OK
          then return rc
          else alloca $ \pKey -> do
                 rc <- sqlite3BtreeKeySize pCursor pKey
                 if rc /= sqlite_OK
                   then return rc
                   else do key' <- peek pKey
                           case (order,e) of
                             (Asc, Including key) | key' >  key -> poke pRes 1
                             (Asc, Excluding key) | key' >= key -> poke pRes 1
                             (Desc,Including key) | key' <  key -> poke pRes (-1)
                             (Desc,Excluding key) | key' <= key -> poke pRes (-1)
                             _                                  -> return ()
                           return rc

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
                   return (Output (key, deserialize bs) (step moveNext pCursor)))
               `onException`
               (sqlite3BtreeCloseCursor pCursor)


instance Data b => From (Index a b) At where
  type K (Index a b)    = b
  type V (Index a b) At = Key a
  
  from idx x = Query (fromAt' deserializeKeys idx x)
    where
      deserializeKeys bs =
        case deserializeKey bs of
          Nothing       -> return Done
          Just (key,bs) -> return (Output key (deserializeKeys bs))

instance Data b => From (Index a b) Restriction where
  type K (Index a b)             = b
  type V (Index a b) Restriction = (b,Key a)

  from idx x = Query (fromInterval' deserialize idx x)
    where
      deserialize bs next =
        let (val,bs') = deserializeIndex bs
        in deserializeKeys val bs'
        where
          deserializeKeys val bs =
            case deserializeKey bs of
              Nothing       -> next
              Just (key,bs) -> return (Output (val,key) (deserializeKeys val bs))

class FromIndex (r :: * -> *) where
  type VI r a b
  type VL r a b

  fromIndex :: (Data a,Data b) => Index a b -> r b -> Query (VI r a b)
  fromList  :: (Data a,Data b) => Index a b -> r b -> Query (VL r a b)

instance FromIndex At where
  type VI At a b = (Key a,a)
  type VL At a b = [Key a]

  fromIndex idx r = do key <- from idx r
                       val <- from (indexedTable idx) (at key)
                       return (key,val)

  fromList idx x = Query (fromAt' deserialize idx x)
    where
      deserialize bs = return (Output (deserializeKeys bs) (return Done))

      deserializeKeys bs =
        case deserializeKey bs of
          Nothing       -> []
          Just (key,bs) -> key : deserializeKeys bs

instance FromIndex Restriction where
  type VI Restriction a b = (Key a,a,b)
  type VL Restriction a b = (b,[Key a])

  fromIndex idx r = do (ival,key) <- from idx r
                       dval       <- from (indexedTable idx) (at key)
                       return (key,dval,ival)

  fromList idx x = Query (fromInterval' deserialize idx x)
    where
      deserialize bs next = do
        let (val,bs') = deserializeIndex bs
        return (Output (val,deserializeKeys bs') next)
        where
          deserializeKeys bs =
            case deserializeKey bs of
              Nothing       -> []
              Just (key,bs) -> key : deserializeKeys bs

fromAt' deserialize idx (At val) = \pBtree schema ->
    withIndexCursor pBtree schema idx ReadOnlyMode $ \fn pCursor ->
    unsafeUseAsCStringLen (serialize val) $ \(indexPtr,indexSize) -> do
      res <- alloca $ \pRes -> do
               checkSqlite3Error $ sqlite3BtreeMoveTo pCursor (castPtr indexPtr) (fromIntegral indexSize) 0 pRes
               peek pRes
      if res /= 0
        then deserialize Data.ByteString.empty
        else do alloca $ \pSize -> do
                  checkSqlite3Error $ sqlite3BtreeKeySize pCursor pSize
                  payloadSize <- peek pSize
                  let size = fromIntegral payloadSize-indexSize
                  ptr <- mallocBytes size
                  bs <- unsafePackMallocCStringLen (castPtr ptr,fromIntegral size)
                  checkSqlite3Error $ sqlite3BtreeKey pCursor (fromIntegral indexSize) (fromIntegral size) ptr
                  deserialize bs

fromInterval' deserialize (Index tbl name fn) (Restriction s e order) = \pBtree schema -> do
  pCursor <- openBtreeCursor pBtree schema name ReadOnlyMode 1 1
  step (start s) pCursor
  where
    ebs = case e of
            Including val -> serialize val
            Excluding val -> serialize val
            Unbounded     -> error "No end value"

    start (Including val) pCursor pRes =
      unsafeUseAsCStringLen (serialize val) $ \(indexPtr,indexSize) -> do
        rc <- sqlite3BtreeMoveTo pCursor (castPtr indexPtr) (fromIntegral indexSize) 0 pRes
        case order of
          Asc  -> do when (rc == sqlite_OK) $ do res <- peek pRes
                                                 when (res > 0) $ poke pRes 0
                     return rc
          Desc -> do if rc == sqlite_OK
                       then do res <- peek pRes
                               if res > 0 
                                 then sqlite3BtreePrevious pCursor pRes
                                 else return rc
                       else return rc
    start (Excluding val) pCursor pRes =
      unsafeUseAsCStringLen (serialize val) $ \(indexPtr,indexSize) -> do
        rc <- sqlite3BtreeMoveTo pCursor (castPtr indexPtr) (fromIntegral indexSize) 0 pRes
        if rc == sqlite_OK
          then do res <- peek pRes
                  case order of
                    Asc  | res == 0 -> sqlite3BtreeNext     pCursor pRes
                    Desc | res >= 0 -> sqlite3BtreePrevious pCursor pRes
                    _               -> return rc
          else return rc
    start Unbounded       pCursor pRes =
      case order of
        Asc  -> sqlite3BtreeFirst pCursor pRes
        Desc -> sqlite3BtreeLast  pCursor pRes

    next =
      case order of
        Asc  -> sqlite3BtreeNext
        Desc -> sqlite3BtreePrevious

    checkEnd (Including _) pCursor nCell pCell f =
      unsafeUseAsCStringLen ebs $ \(pKey,_) -> do
      alloca $ \pRC -> do
        res <- sqlite3BtreeRecordCompare nCell pCell (castPtr pKey) pRC
        checkSqlite3Error $ peek pRC
        case order of
          Asc  | res <= 0 -> f pCursor
          Desc | res >= 0 -> f pCursor
          _               -> do sqlite3BtreeCloseCursor pCursor
                                return Done
    checkEnd (Excluding _) pCursor nCell pCell f =
      unsafeUseAsCStringLen ebs $ \(pKey,_) -> do
      alloca $ \pRC -> do
        res <- sqlite3BtreeRecordCompare nCell pCell (castPtr pKey) pRC
        checkSqlite3Error $ peek pRC
        case order of
          Asc  | res <  0 -> f pCursor
          Desc | res >  0 -> f pCursor
          _               -> do sqlite3BtreeCloseCursor pCursor
                                return Done
    checkEnd Unbounded     pCursor nCell pCell f = f pCursor

    step moveCursor pCursor = do
      res <- (alloca $ \pRes -> do
                checkSqlite3Error $ moveCursor pCursor pRes
                peek pRes)
             `onException`
             (sqlite3BtreeCloseCursor pCursor)
      if res /= 0
        then do sqlite3BtreeCloseCursor pCursor
                return Done
        else (alloca $ \pSize -> do
                checkSqlite3Error $ sqlite3BtreeKeySize pCursor pSize
                payloadSize <- peek pSize
                ptr <- mallocBytes (fromIntegral payloadSize)
                bs <- unsafePackMallocCStringLen (castPtr ptr,fromIntegral payloadSize)
                checkSqlite3Error $ sqlite3BtreeKey pCursor 0 (fromIntegral payloadSize) ptr
                checkEnd e pCursor payloadSize ptr $ \pCursor -> do
                  deserialize bs (step next pCursor))
             `onException`
             (sqlite3BtreeCloseCursor pCursor)


-----------------------------------------------------------------
-- Insert
-----------------------------------------------------------------

insert :: Data a => Table a -> Query a -> Daison (Key a,Key a)
insert tbl q = Daison $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  withTableCursor pBtree schema tbl ReadWriteMode $ \pCursor -> do
    res <- alloca $ \pRes -> do
             sqlite3BtreeLast pCursor pRes
             peek pRes
    key <- if res /= 0
             then return 1
             else alloca $ \pKey -> do
                    checkSqlite3Error $ sqlite3BtreeKeySize pCursor pKey
                    fmap (+1) (peek pKey)
    seq <- doQuery q pBtree schema
    key' <- loop pBtree schema pCursor key seq
    return (key,key')
  where
    loop pBtree schema pCursor key Done           = return key
    loop pBtree schema pCursor key (Output val r) = do unsafeUseAsCStringLen (serialize val) $ \(ptr,size) -> do
                                                         checkSqlite3Error $ sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0
                                                       insertIndices pBtree schema tbl key val
                                                       r >>= loop pBtree schema pCursor (key+1)

insert_ :: Data a => Table a -> a -> Daison (Key a)
insert_ tbl = store tbl Nothing

insertIndices pBtree schema tbl key val =
  unsafeUseAsCStringLen (serializeKey key) $ \(keyPtr,keySize) -> do
    withIndexCursors pBtree schema tbl ReadWriteMode $ \fn pCursor ->
      forM_ (fn val) $ \bs ->
      unsafeUseAsCStringLen bs $ \(indexPtr,indexSize) -> do
        res <- alloca $ \pRes -> do
                 checkSqlite3Error $ sqlite3BtreeMoveTo pCursor (castPtr indexPtr) (fromIntegral indexSize) 0 pRes
                 peek pRes
        if res == 0
          then do payloadSize <- alloca $ \pPayloadSize -> do
                                   checkSqlite3Error $ sqlite3BtreeKeySize pCursor pPayloadSize
                                   peek pPayloadSize
                  allocaBytes (fromIntegral payloadSize+keySize) $ \buf -> do
                    checkSqlite3Error $ sqlite3BtreeKey pCursor 0 (fromIntegral payloadSize) buf
                    copyBytes (buf `plusPtr` fromIntegral payloadSize) keyPtr keySize
                    checkSqlite3Error $ sqlite3BtreeInsert pCursor buf (fromIntegral (fromIntegral payloadSize+keySize)) nullPtr 0 0 0 0 
          else allocaBytes (indexSize+keySize) $ \buf -> do
                 copyBytes buf indexPtr indexSize
                 copyBytes (buf `plusPtr` indexSize) keyPtr keySize
                 checkSqlite3Error $ sqlite3BtreeInsert pCursor (castPtr buf) (fromIntegral (indexSize+keySize)) nullPtr 0 0 0 0
  

-----------------------------------------------------------------
-- Store
-----------------------------------------------------------------

store :: Data a => Table a -> Maybe (Key a) -> a -> Daison (Key a)
store tbl mb_key val = Daison $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  key <- withTableCursor pBtree schema tbl ReadWriteMode $ \pCursor -> do
           key <- alloca $ \pRes -> do
                    case mb_key of
                      Nothing  -> do sqlite3BtreeLast pCursor pRes
                                     res <- peek pRes
                                     if res /= 0
                                       then return 1
                                       else alloca $ \pKey -> do
                                              checkSqlite3Error $ sqlite3BtreeKeySize pCursor pKey
                                              fmap (+1) (peek pKey)
                      Just key -> do checkSqlite3Error $ sqlite3BtreeMoveTo pCursor nullPtr key 0 pRes
                                     res <- peek pRes
                                     when (res == 0) (deleteIndices pBtree schema tbl key pCursor >> return ())
                                     return key
           unsafeUseAsCStringLen (serialize val) $ \(ptr,size) -> do
             checkSqlite3Error $ sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0
           return key
  insertIndices pBtree schema tbl key val
  return key

-----------------------------------------------------------------
-- Update
-----------------------------------------------------------------

update :: Data a => Table a -> Query (Key a,a) -> Daison [(Key a,a)]
update tbl q = Daison $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  withTableCursor pBtree schema tbl ReadWriteMode $ \pCursor -> do
     seq <- doQuery q pBtree schema
     loop pBtree schema pCursor seq
  where
    loop pBtree schema pCursor Done                 = return []
    loop pBtree schema pCursor (Output p@(key,x) r) = do res <- alloca $ \pRes -> do
                                                                  checkSqlite3Error $ sqlite3BtreeMoveTo pCursor nullPtr key 0 pRes
                                                                  peek pRes
                                                         when (res == 0) (deleteIndices pBtree schema tbl key pCursor >> return ())
                                                         unsafeUseAsCStringLen (serialize x) $ \(ptr,size) -> do
                                                           checkSqlite3Error $ sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0
                                                         insertIndices pBtree schema tbl key x
                                                         ps <- r >>= loop pBtree schema pCursor
                                                         return (p:ps)

update_ :: Data a => Table a -> Query (Key a,a) -> Daison ()
update_ tbl q = Daison $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  withTableCursor pBtree schema tbl ReadWriteMode $ \pCursor -> do
     seq <- doQuery q pBtree schema
     loop pBtree schema pCursor seq
  where
    loop pBtree schema pCursor Done               = return ()
    loop pBtree schema pCursor (Output (key,x) r) = do res <- alloca $ \pRes -> do
                                                                checkSqlite3Error $ sqlite3BtreeMoveTo pCursor nullPtr key 0 pRes
                                                                peek pRes
                                                       when (res == 0) (deleteIndices pBtree schema tbl key pCursor >> return ())
                                                       unsafeUseAsCStringLen (serialize x) $ \(ptr,size) -> do
                                                         checkSqlite3Error $ sqlite3BtreeInsert pCursor nullPtr key (castPtr ptr) (fromIntegral size) 0 0 0
                                                       insertIndices pBtree schema tbl key x
                                                       r >>= loop pBtree schema pCursor

-----------------------------------------------------------------
-- Delete
-----------------------------------------------------------------

delete :: Data a => Table a -> Query (Key a) -> Daison [Key a]
delete tbl q = Daison $ \db@(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  withTableCursor pBtree schema tbl ReadWriteMode $ \pCursor -> do
    seq <- doQuery q pBtree schema
    loop pBtree schema pCursor seq
  where
    loop pBtree schema pCursor Done           = return []
    loop pBtree schema pCursor (Output key r) = do 
      res <- alloca $ \pRes -> do
               checkSqlite3Error $ sqlite3BtreeMoveTo pCursor nullPtr key 0 pRes
               peek pRes
      if res == 0
        then do runTriggers pBtree schema tbl key
                deleteIndices pBtree schema tbl key pCursor
                checkSqlite3Error $ sqlite3BtreeDelete pCursor 0
                keys <- r >>= loop pBtree schema pCursor
                return (key:keys)
        else do r >>= loop pBtree schema pCursor

delete_ :: Data a => Table a -> Query (Key a) -> Daison ()
delete_ tbl q = Daison $ \(Database pBtree schemaRef) -> do
  schema <- fetchSchema pBtree schemaRef
  withTableCursor pBtree schema tbl ReadWriteMode $ \pCursor -> do
     seq <- doQuery q pBtree schema
     loop pBtree schema pCursor seq
  where
    loop pBtree schema pCursor Done           = return ()
    loop pBtree schema pCursor (Output key r) = do 
      res <- alloca $ \pRes -> do
               checkSqlite3Error $ sqlite3BtreeMoveTo pCursor nullPtr key 0 pRes
               peek pRes
      when (res == 0) $ do
        runTriggers pBtree schema tbl key
        deleteIndices pBtree schema tbl key pCursor
        checkSqlite3Error $ sqlite3BtreeDelete pCursor 0
      r >>= loop pBtree schema pCursor

deleteIndices pBtree schema tbl key pCursor = do
  size <- alloca $ \pSize -> do
    checkSqlite3Error $ sqlite3BtreeDataSize pCursor pSize
    peek pSize
  ptr <- mallocBytes (fromIntegral size)
  bs <- unsafePackMallocCStringLen (castPtr ptr,fromIntegral size)
  checkSqlite3Error $ sqlite3BtreeData pCursor 0 size ptr
  let val = deserialize bs
  withIndexCursors pBtree schema tbl ReadWriteMode $ \fn pCursor ->
    forM_ (fn val) $ \bsKey ->
      unsafeUseAsCStringLen bsKey $ \(indexPtr,indexSize) -> do
        res <- alloca $ \pRes -> do
                 checkSqlite3Error $ sqlite3BtreeMoveTo pCursor (castPtr indexPtr) (fromIntegral indexSize) 0 pRes
                 peek pRes
        if res /= 0
          then return ()
          else alloca $ \pSize -> do
                  checkSqlite3Error $ sqlite3BtreeKeySize pCursor pSize
                  payloadSize <- peek pSize
                  let size = fromIntegral payloadSize-indexSize
                  ptr <- mallocBytes size
                  bs <- unsafePackMallocCStringLen (castPtr ptr,fromIntegral size)
                  checkSqlite3Error $ sqlite3BtreeKey pCursor (fromIntegral indexSize) (fromIntegral size) ptr
                  case List.delete key (deserializeKeys bs) of
                    [] -> checkSqlite3Error $ sqlite3BtreeDelete pCursor 0
                    ks -> unsafeUseAsCStringLen (bsKey `append` serializeKeys ks) $ \(indexPtr,indexSize) -> do
                            checkSqlite3Error $ sqlite3BtreeInsert pCursor (castPtr indexPtr) (fromIntegral indexSize) nullPtr 0 0 0 0
  return val
  where
    deserializeKeys bs =
      case deserializeKey bs of
        Nothing       -> []
        Just (key,bs) -> key : deserializeKeys bs


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

