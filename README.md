SQLx Batch Executor
===================
Small package to do batch updates and inserts using `sqlx`.  
Create a batcher, add values and then let it batch insert or update.  

The package will take into account the fact that there's a limit to how many sql placeholders you're allowed to have in 1 prepared statement.

Dependancies
------------
Dependancies are managed with glide, enjoy ;-)

Usage
-----

#### Execer
The batcher expects an object which it can use to execute queries with,
the interface for it matches that of `sqlx` but could also take other things that implent it:

```go
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}
```

#### Insert
In the query leave a `%s` where the placeholders go.

```go
b, err := NewBatchInserter(dbTx,
    "INSERT INTO mytable "+
        "(id, name, other) "+
        "VALUES %s", 3) // it's import to specify how many fields there will be here; 3
assert.NoError(err)

b.AddN(nil, "roobs", "dev")
b.AddN(nil, "bb", "boss")

err = b.BatchExec()
assert.NoError(err)
```

will execute:
```sql
INSERT INTO mytable (id, name, other) VALUES (NULL, "roobs", "dev"), (NULL, "bb", "boss");
```


#### Update
In the query leave a `%s` where the placeholders go.

```go
b, err := NewBatchUpdater(dbTx,
    "UPDATE mytable SET other = 'nub'"+
        "WHERE name IN(%s)", 1) // it's import to specify how many fields there will be here; however update only supports 1
assert.NoError(err)

b.AddN("roobs")
b.AddN("bb")

err = b.BatchExec()
assert.NoError(err)
```

will execute:
```sql
UPDATE mytable SET other = "nub" WHERE name IN ("roobs", "bb");
```

#### Base Args
When you have query arguments that don't need to be repeated you can add those with `AddBaseArg`.

```go
b, err := NewBatchUpdater(dbTx,
    "UPDATE mytable SET other = 'nub' "+
        "WHERE id > ? AND name IN(%s) AND other = ?", 1)
assert.NoError(err)

b.AddN("roobs")
b.AddN("bb")

b.AddBaseArg(0, BASE_ARG_BEFORE)
b.AddBaseArg("dev", BASE_ARG_AFTER)

err = b.BatchExec()
assert.NoError(err)
```

will execute:
```sql
UPDATE mytable SET other = "nub" WHERE id > 0 AND name IN ("roobs", "bb") AND other = "dev";
```


#### Any Query
If whatever you want doesn't really work with `NewBatchInserter` or `NewBatchUpdater`;  
you can use `NewBatchExecer` and specify the values part of the query that is repeated.

```go
b, err := NewBatchExecer(dbTx,
    "INSERT INTO mytable "+
        "(id, name, other) "+
        "VALUES %s",
    2, "(NULL, ?, ?)")
assert.NoError(err)

b.AddN("roobs", "dev")
b.AddN("bb", "boss")

err = b.BatchExec()
assert.NoError(err)
```

will execute:
```sql
INSERT INTO mytable (id, name, other) VALUES (NULL, "roobs", "dev"), (NULL, "bb", "boss");
```

Testing
-------
To run the tests you need to create a test DB for the tests to use, 
so that tests can run in parallel you need N test DBs equal to your amount of cores.  
You can easily create these with ```./tools/create-dbs.sh __test_btcwalletd_sqlxbatch `cat /proc/cpuinfo | grep processor | wc -l````

Running tests simply works with `go test`.
