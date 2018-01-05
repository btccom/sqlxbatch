package sqlxbatch

import (
	_ "github.com/go-sql-driver/mysql"

	"flag"
	"fmt"
	"io/ioutil"
	"runtime"
	"strings"
	"sync"
	"time"

	"os"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

type myTableRow struct {
	Id    int
	Name  string
	Other string
}

var availableDBCount int
var testDBName string

type TestDB struct {
	dbName string
	inUse  bool
}

var dbs []*TestDB

var claimNextDBLock = &sync.Mutex{}

func init() {
	flag.StringVar(&testDBName, "sqlxbatch.testdb.name", "__test_btcwalletd_sqlxbatch_%d", "the name of the test DB, should contain '%d'")
	flag.IntVar(&availableDBCount, "sqlxbatch.testdb.available-db-count", runtime.GOMAXPROCS(0), "available parallel DBs, should be >= `-parallel`")
	flag.Parse()

	if !strings.Contains(testDBName, "%d") {
		panic(errors.Errorf("sqlxbatch.testdb.name (%s) must contain '%%d'", testDBName))
	}

	dbs = make([]*TestDB, availableDBCount)
	for i := 0; i < availableDBCount; i++ {
		dbs[i] = &TestDB{dbName: fmt.Sprintf(testDBName, i), inUse: false}
	}
}

func makeTimestamp() int {
	return int(time.Now().UnixNano() / int64(time.Millisecond))
}

func claimNextDB() *TestDB {
	claimNextDBLock.Lock()
	defer claimNextDBLock.Unlock()

	for _, testDB := range dbs {
		if !testDB.inUse {
			testDB.inUse = true
			return testDB
		}
	}

	panic("Failed to claim a DB")

	return nil
}

func getenv(key string, deflt string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return deflt
	}

	return value
}

func initDB() (*sqlx.DB, func()) {
	testDB := claimNextDB()

	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}

	mysqlHost := getenv("DATABASE_HOST", "127.0.0.1")
	mysqlPort := getenv("DATABASE_PORT", "3306")
	mysqlUser := getenv("DATABASE_USER", "root")
	mysqlPass := getenv("DATABASE_PASSWORD", "root")

	db := sqlx.MustConnect("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?strict=true&multiStatements=true", mysqlUser, mysqlPass, mysqlHost, mysqlPort, testDB.dbName))

	// load schema
	schema, err := ioutil.ReadFile("./sqlxbatch_test_schema.sql")
	if err != nil {
		panic(err)
	}
	db.MustExec(string(schema))

	// create close function
	closeFn := func() {
		db.Close()
		testDB.inUse = false
	}

	return db, closeFn
}
