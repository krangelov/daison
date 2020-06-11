# The Daison Tutorial

## Open a database

Open and closing a database is simply:
```haskell
main = do db <- openDB "student_database.db"
          closeDB db
```
If a file with that name does not exist then a new database with that name will be created. The new database will not have any tables by default. You have to ask for the creation of each table separately. For example:
```haskell
data Student
  = Student 
     { name   :: String
     , code   :: Int
     , grades :: [Int]
     }

avg_grade :: Student -> Double
avg_grade s = fromIntegral (sum (grades s)) / fromIntegral (length (grades s))

students :: Table Student
students = table "students"
           `withIndex` students_name
           `withIndex` students_avg_grade

students_name :: Index Student String
students_name = index students "name" name

students_avg_grade :: Index Student String
students_avg_grade = index students "avg_grade" avg_grade

main = do db <- openDB "student_database.db"
          runDaison db ReadWriteMode $ do
            createTable students
          closeDB db
```

Using `createTable` creates the table in the database, but fails if a table with that name already exists. If you want to create a table only if it is not created yet then use `tryCreateTable` instead. Creating a table automatically creates all associated indices as well.

After a table is created you can rename it:
```haskell
runDaison db ReadWriteMode $ do
  bar <- renameTable foo "bar"
  ...
```
Once this is done, using the old table name `foo` will result in an error. In order to access the data you should use `bar` instead.

Changing the type of the table is possible with `alterTable`:
```haskell
runDaison db ReadWriteMode $ do
  alterTable foo bar modify
where
  modify = ...
```
Here you need the definitions of two tables `foo :: Table a` and `bar :: Table b`, and a function `modify :: a -> b`.

Finally if you want to remove a table just use `dropTable` or `tryDropTable`.


## Defining Tables

Before you define a table in Daison you first need to declare the necessary Haskell data types, for example:
```haskell
data Student
  = Student 
     { name   :: String
     , code   :: Int
     , grades :: [Int]
     }
```
after that you define the table itself as a Haskell function:
```haskell
students :: Table Student
students = table "students"
```

If the only thing that you need to store for a student is his/her name, code and grades then a plain tuple would work just as well:
```haskell
students :: Table (String,Int,[Int])
students = table "students"
```
However, in the long run using custom data types is more convenient than using tuples.

The whole point of a database is that we should be able to quickly find the information that we need. For that purpose we create search indices over the tables. In Daison a record can be indexed over the result of an arbitrary function applied to that record. The index itself is defined as yet another Haskell function:
```haskell
students_name :: Index Student String
students_name = index students "name" name
```
The example above defines an index by name over the table of students. Note, however, that `name` is nothing else but a record selector, i.e. a function from `Student` to `String`. In principle we could use any function, for instance indexing over the average grade would look like this:
```haskell
avg_grade :: Student -> Double
avg_grade s = fromIntegral (sum (grades s)) / fromIntegral (length (grades s))

students_avg_grade :: Index Student Double
students_avg_grade = index students "avg_grade" avg_grade
```

The above definitions defined the indices, but they would not be populated unless if you explicitly ask for that in the table definition:
```haskell
students :: Table Student
students = table "students"
           `withIndex` students_name
           `withIndex` students_avg_grade
```

It is possible to index a single record with more than one value. For instance, if we want to get all students who ever got a grade 5 we should use the index:
```haskell
students_grade :: Index Student Int
students_grade = listIndex students "grade" grades
```
Here function `listIndex` expects in the third argument a function of type `Student -> [Int]` and indexes every record under any of the values in the result list. Similarly you can use `maybeIndex` if you want to put only some records in the index:
```haskell
maybeHead :: [a] -> Maybe a
maybeHead []    = Nothing
maybeHead (x:_) = Just x

students_first_grade :: Index Student Int
students_first_grade = maybeIndex students "first_grade" (maybeHead . grades)
```

## Transactions

In order to anything with a Daison database, you first need to start a transaction. The SQLite backend supports multiple readers/single writer access. Correspondingly there are read-only and read-write transactions. This is reflected with the `AccessMode` data type:
```haskell
data AccessMode = ReadWriteMode | ReadOnlyMode
```

The transaction itself is started by using:
```haskell
runDaison :: Database -> AccessMode -> Daison a -> IO a
```
This function takes a database and an access mode and performs an operation in the `Daison` monad. The operation can create/drop tables, query the data or modify the data. If the operation is performed without any exceptions, then the transaction will be automatically committed. In case of exceptions the transaction will be rolled back.


## Access the Data

### Insert

TODO...

### Select

```haskell
select :: Query a -> Daison [a]
```
TODO...

### Update

TODO...

### Store

TODO...

### Delete

TODO...
