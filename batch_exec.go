package sqlxbatch

import (
	"database/sql"
	"fmt"
	"math"
	"strings"

	"sync"

	"github.com/jmoiron/sqlx"
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

type workBatch struct {
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
	execer         Execer
	baseQuery      string
	cols           int
	tplValueString string
	baseArgs       []*baseArg
	nWorkers       int

	lock         sync.Mutex
	batches      []*workBatch
	currentBatch *workBatch

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
	n := &BatchExecer{
		execer:         dbTx,
		baseQuery:      baseQuery,
		cols:           cols,
		tplValueString: tplValueString,

		expectedBaseArgs:   10,
		maxSqlPlaceHolders: MAX_SQL_PLACEHOLDERS,
	}

	n.reset()

	return n, nil
}

func (b *BatchExecer) reset() {
	batches := make([]*workBatch, 0)
	currentBatch := &workBatch{
		rows: make([][]interface{}, 0),
	}

	batches = append(batches, currentBatch)

	b.batches = batches
	b.currentBatch = currentBatch
}

func (b *BatchExecer) insertsPerChuck() int {
	return int(math.Floor(float64(b.maxSqlPlaceHolders-b.expectedBaseArgs) / float64(b.cols)))
}

func (b *BatchExecer) UseNWorkers(nWorkers int) {
	b.nWorkers = nWorkers
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
		newBatch := workBatch{
			rows: make([][]interface{}, 0),
		}

		b.batches = append(b.batches, &newBatch)
		b.currentBatch = &newBatch
	}
}
func (b *BatchExecer) execBatch(batch *workBatch) error {
	if len(batch.rows) > 0 {
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
		_, err := b.execer.Exec(stmt, valueArgs...)
		if err != nil {
			return errors.Wrap(err, "")
		}
	}

	return nil
}

func (b *BatchExecer) BatchExec() error {
	b.lock.Lock()
	defer func() { b.reset() }()
	defer b.lock.Unlock()

	nWorkers := b.nWorkers
	if nWorkers < 1 {
		nWorkers = 1
	}

	// if we're trying to use multiple workers then we need to make sure this is safe
	//  since sql transactions aren't thread safe to use
	if nWorkers > 1 {
		switch v := b.execer.(type) {
		case *sqlx.Tx:
			return errors.Errorf("Can't use multiple workers on a sqlx.Tx")
		case *sqlx.DB:
			// ok
		default:
			return errors.Errorf("Unknown Execer type %T", v)
		}
	}

	var workChan chan *workBatch
	workChan = make(chan *workBatch, nWorkers)
	resChan := make(chan error, nWorkers)
	doneChan := make(chan bool, nWorkers)

	for i := 0; i < nWorkers; i++ {
		go func() {
			for true {
				select {
				case batch, more := <-workChan:
					// nil means we're done
					if !more {
						doneChan <- true
						return
					} else {
						resChan <- b.execBatch(batch)
					}
				}
			}
		}()
	}

	// push batches into chan
	go func() {
		for idx, batch := range b.batches {
			logger.Debugf("%d / %d ...", idx, len(b.batches))

			if len(batch.rows) > 0 {
				workChan <- batch
			}
		}

		close(workChan)
	}()

	// wait for results
	// keep track of how many workers are still alive, we won't shutdown until they're all done
	iWorkersAlive := nWorkers

	for true {
		select {
		// check of result and return if err
		case err := <-resChan:
			if err != nil {
				return err
			}

		// check for workers who are done
		case <-doneChan:
			iWorkersAlive--

			// if this was the last worker alive then we can exit
			if iWorkersAlive == 0 {
				close(resChan)
				close(doneChan)
				return nil
			}
		}
	}

	return nil
}
