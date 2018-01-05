package sqlxbatch

import (
	_ "github.com/go-sql-driver/mysql"

	"testing"

	"github.com/jmoiron/sqlx"
	assert "github.com/stretchr/testify/require"
)

func TestBatchInsertSingle(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	db, closeFn := initDB()
	defer closeFn()

	dbTx, err := db.Beginx()
	assert.NoError(err)
	defer dbTx.Rollback()

	b, err := NewBatchInserter(dbTx,
		"INSERT INTO mytable "+
			"(id, name, other) "+
			"VALUES %s",
		3)
	assert.NoError(err)

	b.AddN(nil, "roobs", "dev")

	err = b.BatchExec()
	assert.NoError(err)

	rows := make([]myTableRow, 0)
	err = dbTx.Select(&rows, "SELECT * FROM mytable")
	assert.NoError(err)

	assert.Equal(1, len(rows))
	assert.Equal(1, rows[0].Id)
	assert.Equal("roobs", rows[0].Name)
	assert.Equal("dev", rows[0].Other)
}

func TestBatchInsertMultiple(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	db, closeFn := initDB()
	defer closeFn()

	dbTx, err := db.Beginx()
	assert.NoError(err)
	defer dbTx.Rollback()

	b, err := NewBatchInserter(dbTx,
		"INSERT INTO mytable "+
			"(id, name, other) "+
			"VALUES %s",
		3)
	assert.NoError(err)

	b.AddN(nil, "roobs", "dev")
	b.AddN(nil, "bb", "boss")

	err = b.BatchExec()
	assert.NoError(err)

	rows := make([]myTableRow, 0)
	err = dbTx.Select(&rows, "SELECT * FROM mytable")
	assert.NoError(err)

	assert.Equal(2, len(rows))
	assert.Equal(1, rows[0].Id)
	assert.Equal("roobs", rows[0].Name)
	assert.Equal("dev", rows[0].Other)
	assert.Equal(2, rows[1].Id)
	assert.Equal("bb", rows[1].Name)
	assert.Equal("boss", rows[1].Other)
}

func TestBatchInsertMutilpleCustomTpl(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	db, closeFn := initDB()
	defer closeFn()

	dbTx, err := db.Beginx()
	assert.NoError(err)
	defer dbTx.Rollback()

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

	rows := make([]myTableRow, 0)
	err = dbTx.Select(&rows, "SELECT * FROM mytable")
	assert.NoError(err)

	assert.Equal(2, len(rows))
	assert.Equal(1, rows[0].Id)
	assert.Equal("roobs", rows[0].Name)
	assert.Equal("dev", rows[0].Other)
	assert.Equal(2, rows[1].Id)
	assert.Equal("bb", rows[1].Name)
	assert.Equal("boss", rows[1].Other)
}

func testBatchUpdatePrep(t *testing.T, assert *assert.Assertions, dbTx *sqlx.Tx) {
	b, err := NewBatchInserter(dbTx,
		"INSERT INTO mytable "+
			"(id, name, other) "+
			"VALUES %s", 3)
	assert.NoError(err)

	b.AddN(nil, "roobs", "dev")
	b.AddN(nil, "bb", "boss")

	err = b.BatchExec()
	assert.NoError(err)
}

func TestBatchUpdate(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	db, closeFn := initDB()
	defer closeFn()

	dbTx, err := db.Beginx()
	assert.NoError(err)
	defer dbTx.Rollback()

	testBatchUpdatePrep(t, assert, dbTx)

	b, err := NewBatchUpdater(dbTx,
		"UPDATE mytable SET other = 'nub'"+
			"WHERE name IN(%s)", 1)
	assert.NoError(err)

	b.AddN("roobs")
	b.AddN("bb")

	err = b.BatchExec()
	assert.NoError(err)

	rows := make([]myTableRow, 0)
	err = dbTx.Select(&rows, "SELECT * FROM mytable")
	assert.NoError(err)

	assert.Equal(2, len(rows))
	assert.Equal(1, rows[0].Id)
	assert.Equal("roobs", rows[0].Name)
	assert.Equal("nub", rows[0].Other)
	assert.Equal(2, rows[1].Id)
	assert.Equal("bb", rows[1].Name)
	assert.Equal("nub", rows[1].Other)
}

func TestBatchUpdateWithBaseArg(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	db, closeFn := initDB()
	defer closeFn()

	dbTx, err := db.Beginx()
	assert.NoError(err)
	defer dbTx.Rollback()

	testBatchUpdatePrep(t, assert, dbTx)

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

	rows := make([]myTableRow, 0)
	err = dbTx.Select(&rows, "SELECT * FROM mytable")
	assert.NoError(err)

	assert.Equal(2, len(rows))
	assert.Equal(1, rows[0].Id)
	assert.Equal("roobs", rows[0].Name)
	assert.Equal("nub", rows[0].Other)
	assert.Equal(2, rows[1].Id)
	assert.Equal("bb", rows[1].Name)
	assert.Equal("boss", rows[1].Other)
}

func TestBaseArgMax(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	db, closeFn := initDB()
	defer closeFn()

	dbTx, err := db.Beginx()
	assert.NoError(err)
	defer dbTx.Rollback()

	b, err := NewBatchUpdater(dbTx,
		"UPDATE mytable SET other = 'nub' "+
			"WHERE id > ? AND name IN(%s) AND other = ?", 1)
	assert.NoError(err)

	err = b.AddBaseArg(0, BASE_ARG_AFTER)
	assert.NoError(err)
	err = b.AddBaseArg(0, BASE_ARG_AFTER)
	assert.NoError(err)
	err = b.AddBaseArg(0, BASE_ARG_AFTER)
	assert.NoError(err)
	err = b.AddBaseArg(0, BASE_ARG_AFTER)
	assert.NoError(err)
	err = b.AddBaseArg(0, BASE_ARG_AFTER)
	assert.NoError(err)
	err = b.AddBaseArg(0, BASE_ARG_AFTER)
	assert.NoError(err)
	err = b.AddBaseArg(0, BASE_ARG_AFTER)
	assert.NoError(err)
	err = b.AddBaseArg(0, BASE_ARG_AFTER)
	assert.NoError(err)
	err = b.AddBaseArg(0, BASE_ARG_AFTER)
	assert.NoError(err)
	err = b.AddBaseArg(0, BASE_ARG_AFTER)
	assert.NoError(err)

	// 1th base arg should err
	err = b.AddBaseArg(0, BASE_ARG_AFTER)
	assert.Error(err)
}

func TestBatchInsertEmpty(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	db, closeFn := initDB()
	defer closeFn()

	dbTx, err := db.Beginx()
	assert.NoError(err)
	defer dbTx.Rollback()

	b, err := NewBatchInserter(dbTx,
		"INSERT INTO mytable "+
			"(id, name, other) "+
			"VALUES %s",
		3)
	assert.NoError(err)

	err = b.BatchExec()
	assert.NoError(err)
}

func TestBatchInsertMultipleBatches(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	db, closeFn := initDB()
	defer closeFn()

	dbTx, err := db.Beginx()
	assert.NoError(err)
	defer dbTx.Rollback()

	b, err := NewBatchInserter(dbTx,
		"INSERT INTO mytable "+
			"(id, name, other) "+
			"VALUES %s",
		3)
	assert.NoError(err)

	// lower max sql place holders to trigger chunking
	b.maxSqlPlaceHolders = 3

	b.AddN(nil, "roobs", "dev")
	b.AddN(nil, "bb", "boss")

	err = b.BatchExec()
	assert.NoError(err)

	rows := make([]myTableRow, 0)
	err = dbTx.Select(&rows, "SELECT * FROM mytable")
	assert.NoError(err)

	assert.Equal(2, len(rows))
	assert.Equal(1, rows[0].Id)
	assert.Equal("roobs", rows[0].Name)
	assert.Equal("dev", rows[0].Other)
	assert.Equal(2, rows[1].Id)
	assert.Equal("bb", rows[1].Name)
	assert.Equal("boss", rows[1].Other)
}

func TestBatchInsertBadQry(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	db, closeFn := initDB()
	defer closeFn()

	dbTx, err := db.Beginx()
	assert.NoError(err)
	defer dbTx.Rollback()

	b, err := NewBatchInserter(dbTx,
		"THIS IS A A BAD QUERY",
		3)
	assert.NoError(err)

	b.AddN(nil, "roobs", "dev")

	err = b.BatchExec()
	assert.Error(err)
}

func TestBatchUpdateUnsupportedCols(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	db, closeFn := initDB()
	defer closeFn()

	dbTx, err := db.Beginx()
	assert.NoError(err)
	defer dbTx.Rollback()

	_, err = NewBatchUpdater(dbTx,
		"UPDATE mytable SET other = 'nub'"+
			"WHERE name IN(%s)", 2)
	assert.Error(err)
}
