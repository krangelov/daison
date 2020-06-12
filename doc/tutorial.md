# The Daison Tutorial

Daison (DAta lISt comprehensiON) is a database where the data management language is Haskell instead of SQL. The main idea is that instead of SELECT statements, you use Haskell's List Comprehensions as generalized to monads by using the Monad Comprehension extension. The other benefit is that the database can store any serializable data type defined Haskell, this avoids the need to convert between Haskell types and SQL types for every query. An added benefit is that Daison naturally supports algebraic data types which is problematic in relational databases.

The backend storage is SQLite from which I have stripped all SQL related features. The result is a simple key-value storage,  on top of which there is a Haskell API which replaces the SQL language. This gives us the efficiency and the reliability of an established database, but without the intermediatry of the SQL interpreter.

The following tutorial will introduce how to perform different operations in Daison, sometimes with comparison with SQL.

## Opening and closing a database

Open and closing a database is simply:
```haskell
main = do db <- openDB "student_database.db"
          closeDB db
```
If a file with that name does not exist then a new database with that name will be created. The new database will not have any tables by default. Like in SQL you have to create the tables by executing certain operations.

## Transactions

In order to do anything with a Daison database, you first need to start a transaction. This applies both to read-only as well as read-write operations. The backend allows multiple reader - single writer access. This means that even if you only read from the database, the engine must know when you are done in order to allow changes. What kind of access you need is described with `AccessMode` data type:
```haskell
data AccessMode = ReadWriteMode | ReadOnlyMode
```

The transaction itself is started by using:
```haskell
runDaison :: Database -> AccessMode -> Daison a -> IO a
```
This function takes a database and an access mode and performs an operation in the `Daison` monad. The operation can create/drop tables, query the data or modify the data. If the operation is performed without any exceptions, then the transaction will be automatically committed. In case of exceptions the transaction will be rolled back.

## Defining and creating tables

Although this is a NoSQL database it is still based on the concept of tables. A table however doesn't have any columns, it is just a list rows, where each row stores one Haskell value. The value, of course, could be of record type, so in that sense the Daison tables could have columns as well.

Before we create a table, we need to define the data type for the rows:
```haskell
data Student
  = Student 
     { name   :: String
     , code   :: Int
     , grades :: [Int]
     }
     deriving Data
```
Note that you need to derive an instance of `Data`, since Daison uses it for generic serialization/deserialization.

Once you have all necessary data types, you can declare the associated tables and indices:
```haskell
students :: Table Student
students = table "students"
           `withIndex` students_name
           `withIndex` students_grade
           `withIndex` students_avg_grade

students_name :: Index Student String
students_name = index students "name" name

students_grade :: Index Student Int
students_grade = listIndex students "grade" grades

students_avg_grade :: Index Student String
students_avg_grade = index students "avg_grade" avg_grade

avg_grade :: Student -> Double
avg_grade s = fromIntegral (sum (grades s)) / fromIntegral (length (grades s))
```
As you can see the tables/indices are just Haskell functions defined by using primitives from the Daison DSL. Here `table`:
```haskell
table :: String -> Table a
```
takes a table name and returns a value of type Table. By default every table has an `Int64` primary key and arbitrary value as a content. If you need indices, then they must be added with the `withIndex` primitive:
```haskell
withIndex :: Data b => Table a -> Index a b -> Table a
```
The index itself is defined in one of three possible ways:

- The simplest way is by using the `index` primitive:
```haskell
index :: Table a -> String -> (a -> b) -> Index a b
```
It takes a table, an index name and an arbitrary function which from a row value computes the value by which the row must be indexed. In the example above there two indexes of this kind - `students_name` and `students_avg_grade`. When the row value is of record type, it is natural that some of the indices will be over a particular field. This is the case with `students_name` but it does not have to be the case. Any function will work just as well. For example `students_avg_grade` indexes over the average grade which is not stored but is computed every time when a row is inserted or updated. In the SQL terminology this is called "computed index", which is supported by some databases but not all. In that case you need to define an appropriate function:

- You can also index a row by more than one value by using the primitive:
```haskell
listIndex :: Table a -> String -> (a -> [b]) -> Index a b
```
In the above example we have the index `students_grade` which lets you to search for students who got a particular grade. This kind of indices does not have correspondence in SQL since in (most) relational databases you cannot store lists.

- A special case is when you want to index some rows but not all. This is possible with the primitive:
```haskell
maybeIndex :: Table a -> String -> (a -> Maybe b) -> Index a b
```
when the indexing function returns `Nothing` then the current row will be skipped from the index. This is equivalent to an index over a nullable column in SQL.

Finally, when a tables is defined, you must also create it. The definitions above are just Haskell functions and they do not do anything with the database. You should call `createTable` explicitly:
```haskell
runDaison db ReadWriteMode $ do
  createTable students
```
This creates both the table and the indices which are associated with that table. Note that since this operation changes the database, it must be executed within a read-write transaction.

If a table with that name already exists, `createTable` will fail. If you want to create a table only if it is not created yet then use `tryCreateTable` instead.

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
Here you need the definitions of two tables `foo :: Table a` and `bar :: Table b`, and a function `modify :: a -> b` which transforms the old values into the new ones.

Finally if you want to remove a table just use `dropTable` or `tryDropTable`.


## Access the Data

### Insert

The simplest way to insert data in a table is the primitive:
```haskell
insert_ :: Data a => Table a -> a -> Daison (Key a)
```
It just takes a table and a value and inserts the new value in the table. The results is the value of the primary key. Here the type `Key a` is just a type synonym for a 64-bit integer:
```haskell
type Key a = Int64
```

A more advanced way is to use the equivalent for `INSERT-SELECT` in SQL:
```haskell
insert :: Data a => Table a -> Query a -> Daison (Key a, Key a)
```
Instead of a single value, this primitive takes a query which can extract data from other tables in order to prepare the values to be inserted in the target table. The query itself is a monadic function which is often conveniently expressed as a list comprehension. The result from `insert` is a pair of the initial and final primary keys, for the newly inserted rows. Here is an example:
```haskell
runDaison db ReadWriteMode $ do
  (start,end) <- insert foo [f x | x <- from bar]
  ...
```
where we take all values from table `bar`, we apply a transformation function `f`, and finally we insert the results in table `foo`.

More details about queries follow.

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
