{-# LANGUAGE BinaryLiterals #-}
module Database.Helda.Serialize(serialize,deserialize) where

import Data.Bits
import Data.Binary
import Data.Binary.Put
import Data.Binary.Get
import Data.Generics
import Data.ByteString.Lazy(ByteString)

data Tag = Tag {-# UNPACK #-} !Word8 {-# UNPACK #-} !Int String

tag_list     = Tag     0b00 2 "a list"
tag_str      = Tag     0b10 2 "a string"
tag_int      = Tag     0b01 2 "an int"
tag_con      = Tag    0b011 3 "a constructor"
tag_end      = Tag  0b00111 5 "an args-end"
tag_float    = Tag  0b01111 5 "a float"
tag_double   = Tag  0b10111 5 "a double"
tag_rational = Tag  0b11111 5 "a rational"
tag_char     = Tag 0b100111 6 "a char"

serialize :: Data a => a -> ByteString
serialize = runPut . serializeM

serializeM :: Data a => a -> Put
serializeM =
  serializeDefault `extQ`
--  serializeList `extQ`
  serializeString `extQ`
  serializeFloat `extQ`
  serializeDouble
  where
    serializeDefault t =
      case constrRep (toConstr t) of
        AlgConstr i   -> putVInt (fromIntegral i) tag_con >>
                         foldr (>>) (return ()) (gmapQ serializeM t) >>
                         putTag tag_end
        IntConstr i   -> putVInt i tag_int
        FloatConstr r -> putTag tag_rational >> put r
        CharConstr  c -> putTag tag_char  >> put c

    serializeList :: Data a => [a] -> Put
    serializeList xs = do
      putVInt (fromIntegral (length xs)) tag_list
      mapM_ serializeM xs

    serializeString :: String -> Put
    serializeString s = do
      putVInt (fromIntegral (length s)) tag_str
      mapM_ put s
      
    serializeFloat :: Float -> Put
    serializeFloat f = putTag tag_float >> put f
      
    serializeDouble :: Double -> Put
    serializeDouble f = putTag tag_double >> put f

putTag :: Tag -> Put
putTag (Tag tag tbits _) = putWord8 tag

putVInt :: Integer -> Tag -> Put
putVInt n (Tag tag tbits _) =
  let rbits = 7-tbits
      n0    = fromIntegral (n .&. (1 `shiftL` rbits - 1)) `shiftL` 1
      n'    = n `shiftR` rbits
  in if n' == 0
       then do putWord8 ((n0 `shiftL` tbits) .|. tag)
       else do putWord8 (((n0 .|. 1) `shiftL` tbits) .|. tag)
               putRest n'
  where
    -- specialized version without tag bits
    putRest n =
      let n0 = fromIntegral (n .&. (1 `shiftL` 7 - 1)) `shiftL` 1
          n' = n `shiftR` 7
      in if n' == 0
           then putWord8 n0
           else do putWord8 (n0 .|. 1)
                   putRest n'

deserialize :: Data a => ByteString -> a
deserialize = runGet deserializeM

deserializeM :: Data a => Get a
deserializeM =
  deserializeDefault `extR`
  deserializeString `extR`
  deserializeChar `extR`
  deserializeInt `extR`
  deserializeFloat `extR`
  deserializeDouble
  where
    deserializeDefault = do
      con  <- getConstr                   -- get the constructor
      x    <- fromConstrM deserializeM con -- Read the children
      getTag tag_end
      return x

    deserializeString :: Get String
    deserializeString = do
      len <- getVInt tag_str
      getString len
      where
        getString 0 = return []
        getString n = do
          c  <- get
          cs <- getString (n-1)
          return (c:cs)

    deserializeChar :: Get Char
    deserializeChar = getTag tag_char >> get

    deserializeInt :: Get Int
    deserializeInt = fmap fromIntegral $ getVInt tag_int

    deserializeFloat :: Get Float
    deserializeFloat = getTag tag_float >> get

    deserializeDouble :: Get Double
    deserializeDouble = getTag tag_double >> get
    
    deserializeRational :: Get Rational
    deserializeRational = getTag tag_rational >> get

    myDataType = dataTypeOf (getArg deserializeDefault)
      where
        getArg :: Get a'' -> a''
        getArg = undefined

    getConstr :: Get Constr
    getConstr = do
      i <- getVInt tag_con
      if i <= fromIntegral (maxConstrIndex myDataType)
        then return (indexConstr myDataType (fromIntegral i))
        else fail ("the data type has no constructor with index "++show i)


getTag :: Tag -> Get ()
getTag (Tag tag bits name) = do
  w <- getWord8
  if w .&. (1 `shiftL` bits - 1) == tag
    then return ()
    else fail ("failed to find "++name)

getVInt :: Tag -> Get Integer
getVInt (Tag tag bits name) = do
  w <- getWord8
  if w .&. (1 `shiftL` bits - 1) == tag
    then let n = fromIntegral (w `shiftR` (bits+1))
         in if w .&. (1 `shiftL` bits) == 0
              then return n
              else getRest (7-bits) n
    else fail ("failed to find "++name)
  where
    getRest bits n = do
      w <- getWord8
      let n' = n .|. (fromIntegral (w .&. 0xFE) `shiftL` (bits-1))
      if w .&. 1 == 0
        then return n'
        else getRest (bits+7) n'
