{-# LANGUAGE MonadComprehensions #-}
import Database.Helda
import System.Environment

people_name :: Index (String,Int) String
people_name = index "people_name" (Just . fst)

people :: Table (String,Int)
people = table "people"
           `withIndex` people_name

main = do
  db <- openDB "test.db"
  x <- runSelda db ReadWriteMode $ do
         --dropTable people
         tryCreateTable people
         insert people ("Aga",15)
         insert people ("Henry",22)
         --update people2 (\_ (name,age,x) -> (name,age,10))
           --             (from people2)
         select [x | x <- from people]
  print x
  closeDB db
