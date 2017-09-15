package sqlxbatch

import (
	"database/sql"
	"fmt"
	"math"
	"strings"

	"sync"

	"github.com/juju/loggo"
	"github.com/pkg/errors"
)

type BASE_ARG_POSITION int8

const (
	MAX_SQL_PLACEHOLDERS int               = 65535
	BASE_ARG_BEFORE      BASE_ARG_POSITION = -1
	BASE_ARG_AFTER       BASE_ARG_POSITION = 11
)

var logger = loggo.GetLogger("sqlxbatch")

type batch struct {
	rows [][]interface{}
}

type baseArg struct {
	val      interface{}
	position BASE_ARG_POSITION
}

type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type BatchExecer struct {
	dbTx           Execer
	baseQuery      string
	cols           int
	tplValueString string
	baseArgs       []*baseArg

	lock         sync.Mutex
	batches      []*batch
	currentBatch *batch

	expectedBaseArgs   int
	maxSqlPlaceHolders int
}

func NewBatchUpdater(dbTx Execer, updateQuery string, cols int) (*BatchExecer, error) {
	if cols > 1 {
		return nil, errors.Errorf("Unsupported number of columns")
	}

	return NewBatchExecer(dbTx, updateQuery, cols, "("+strings.TrimSuffix(strings.Repeat("?, ", cols), ", ")+")")
}

func NewBatchInserter(dbTx Execer, insertQuery string, cols int) (*BatchExecer, error) {
	return NewBatchExecer(dbTx, insertQuery, cols, "("+strings.TrimSuffix(strings.Repeat("?, ", cols), ", ")+")")
}

func NewBatchExecer(dbTx Execer, baseQuery string, cols int, tplValueString string) (*BatchExecer, error) {
	batches := make([]*batch, 0)
	currentBatch := batch{
		rows: make([][]interface{}, 0),
	}

	batches = append(batches, &currentBatch)

	n := BatchExecer{
		dbTx:           dbTx,
		baseQuery:      baseQuery,
		cols:           cols,
		tplValueString: tplValueString,
		batches:        batches,
		currentBatch:   &currentBatch,

		expectedBaseArgs:   10,
		maxSqlPlaceHolders: MAX_SQL_PLACEHOLDERS,
	}

	return &n, nil
}

func (b *BatchExecer) insertsPerChuck() int {
	return int(math.Floor(float64(b.maxSqlPlaceHolders-b.expectedBaseArgs) / float64(b.cols)))
}

func (b *BatchExecer) Count() int {
	sum := 0

	for _, batch := range b.batches {
		sum += len(batch.rows)
	}

	return sum
}

func (b *BatchExecer) AddBaseArg(val interface{}, position BASE_ARG_POSITION) error {
	arg := baseArg{
		val:      val,
		position: position,
	}

	if len(b.baseArgs)+1 > b.expectedBaseArgs {
		return errors.Errorf("More base args than expected")
	}

	b.baseArgs = append(b.baseArgs, &arg)

	return nil
}

func (b *BatchExecer) AddN(vals ...interface{}) {
	b.Add(vals)
}

func (b *BatchExecer) Add(vals []interface{}) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.currentBatch.rows = append(b.currentBatch.rows, vals)

	if len(b.currentBatch.rows) >= b.insertsPerChuck() {
		newBatch := batch{
			rows: make([][]interface{}, 0),
		}

		b.batches = append(b.batches, &newBatch)
		b.currentBatch = &newBatch
	}
}

func (b *BatchExecer) BatchExec() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	for idx, batch := range b.batches {
		if len(batch.rows) > 0 {
			logger.Debugf("%d / %d ...", idx, len(b.batches))

			valueStrings := make([]string, 0, len(batch.rows))
			valueArgs := make([]interface{}, 0, (len(batch.rows)*b.cols)+len(b.baseArgs))

			// apply base args before
			for _, arg := range b.baseArgs {
				if arg.position == BASE_ARG_BEFORE {
					valueArgs = append(valueArgs, arg.val)
				}
			}

			// apply rows
			for _, row := range batch.rows {
				valueStrings = append(valueStrings, b.tplValueString)
				valueArgs = append(valueArgs, row...)
			}

			// apply base args after
			for _, arg := range b.baseArgs {
				if arg.position == BASE_ARG_AFTER {
					valueArgs = append(valueArgs, arg.val)
				}
			}

			stmt := fmt.Sprintf(b.baseQuery, strings.Join(valueStrings, ","))
			_, err := b.dbTx.Exec(stmt, valueArgs...)
			if err != nil {
				return errors.Wrap(err, "")
			}
		}
	}

	return nil
}
