package sagma

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

type pgUserError struct {
	err error
}

// Returns the error wrapped in a pgUserError if it is a user error
func pgMaybeUserError(err error) error {
	if pqerr, ok := err.(*pq.Error); ok {
		errcls := pqerr.Code.Class()
		if errcls == "22" || errcls == "23" {
			return newPgUserError(err)
		}
	}
	return err
}

func newPgUserError(err error) error {
	return &pgUserError{err: err}
}

func (e *pgUserError) IsUserError() bool {
	return true
}

func (e *pgUserError) Error() string {
	return e.err.Error()
}

type pgQueries struct {
	pgQueryINSERT         string
	pgQueryINSERTctx      string
	pgQueryFailUPDATE     string
	pgQueryUPDATE         string
	pgQuerySaveCtx        string
	pgQueryGetStateStatus string
	pgQueryAllByID        string
	pgQueryGetStatus      string
	pgQueryRunnables      string
	pgQueryArchive        string
}

const (
	_pgQueryINSERT         = `INSERT INTO %s (id, state, status, created, modified) VALUES ($1, $2, $3, NOW(), NOW()) ON CONFLICT (id, state) DO UPDATE SET status = EXCLUDED.status;`
	_pgQueryINSERTctx      = `INSERT INTO %s (id, state, status, created, modified, fileid, context) VALUES ($1, $2, $3, NOW(), NOW(), $4, $5::jsonb) ON CONFLICT (id, state) DO UPDATE SET status = EXCLUDED.status, context = EXCLUDED.context;`
	_pgQueryFailUPDATE     = `UPDATE %s SET failed = $1, modified = NOW() WHERE id = $2 AND state = $3;`
	_pgQueryUPDATE         = `UPDATE %s SET status = $1, modified = NOW() WHERE id = $2 AND state = $3 AND status = $4;`
	_pgQuerySaveCtx        = `UPDATE %s SET context = $1::jsonb WHERE id = $2 AND state = $3;`
	_pgQueryGetStateStatus = `SELECT COALESCE(fileid, ''), context FROM %s WHERE id = $1 AND state = $2 AND status = $3 LIMIT 1;`
	_pgQueryAllByID        = `SELECT COALESCE(state, ''), COALESCE(status, ''), created, modified, COALESCE(error, ''), context FROM %s WHERE id = $1;`
	_pgQueryGetStatus      = `SELECT COALESCE(status, '') FROM %s WHERE id = $1 AND state = $2 LIMIT 1;`
	_pgQueryRunnables      = `SELECT id, state FROM %s WHERE status = $1 AND modified < NOW() - INTERVAL '%d seconds' LIMIT 100;`
	_pgQueryArchive        = `UPDATE %s SET status = $1, modified = NOW() WHERE id = $2 AND status = $3;`
)

func makePgQueries(table string, runnablesAfter time.Duration) pgQueries {
	return pgQueries{
		pgQueryINSERT:         fmt.Sprintf(_pgQueryINSERT, table),
		pgQueryINSERTctx:      fmt.Sprintf(_pgQueryINSERTctx, table),
		pgQueryFailUPDATE:     fmt.Sprintf(_pgQueryFailUPDATE, table),
		pgQueryUPDATE:         fmt.Sprintf(_pgQueryUPDATE, table),
		pgQuerySaveCtx:        fmt.Sprintf(_pgQuerySaveCtx, table),
		pgQueryGetStateStatus: fmt.Sprintf(_pgQueryGetStateStatus, table),
		pgQueryAllByID:        fmt.Sprintf(_pgQueryAllByID, table),
		pgQueryGetStatus:      fmt.Sprintf(_pgQueryGetStatus, table),
		pgQueryArchive:        fmt.Sprintf(_pgQueryArchive, table),
		pgQueryRunnables:      fmt.Sprintf(_pgQueryRunnables, table, int(runnablesAfter.Seconds())),
	}
}

func (q *pgQueries) archive(tx *sql.Tx, id MsgID) (sql.Result, error) {
	return tx.Exec(q.pgQueryArchive, stateStatusArchived, id, stateStatusDone)
}

func (q *pgQueries) insertNew(tx *sql.Tx, id MsgID, state State, status StateStatus) (sql.Result, error) {
	return tx.Exec(q.pgQueryINSERT, string(id), string(state), string(status))
}

func (q *pgQueries) insertNewContext(tx *sql.Tx, id MsgID, state State, status StateStatus, blobID BlobID, ctx string) (sql.Result, error) {
	return tx.Exec(q.pgQueryINSERTctx, string(id), string(state), string(status), string(blobID), ctx)
}

func (q *pgQueries) updateFailure(tx *sql.Tx, id MsgID, state State, reason error) (sql.Result, error) {
	return tx.Exec(q.pgQueryFailUPDATE, reason.Error(), string(id), string(state))
}

func (q *pgQueries) updateStatus(tx *sql.Tx, nextStatus StateStatus, id MsgID, st State, currStatus StateStatus) (sql.Result, error) {
	return tx.Exec(q.pgQueryUPDATE, string(nextStatus), string(id), string(st), string(currStatus))
}

func (q *pgQueries) updateContext(tx *sql.Tx, id MsgID, st State, context string) (sql.Result, error) {
	return tx.Exec(q.pgQuerySaveCtx, context, id, st)
}

func (q *pgQueries) allByID(tx *sql.Tx, id MsgID) (*sql.Rows, error) {
	return tx.Query(q.pgQueryAllByID, string(id))
}

func (q *pgQueries) allByStatus(tx *sql.Tx, status StateStatus) (*sql.Rows, error) {
	return tx.Query(q.pgQueryRunnables, string(status))
}

func (q *pgQueries) getByState(tx *sql.Tx, id MsgID, state State) (*sql.Rows, error) {
	// TODO: should be QueryRow
	return tx.Query(q.pgQueryGetStatus, string(id), string(state))
}

func (q *pgQueries) contextAndBlobIDAtStateStatus(tx *sql.Tx, id MsgID, state State, status StateStatus) *sql.Row {
	return tx.QueryRow(q.pgQueryGetStateStatus, string(id), string(state), string(status))
}

type PSQLConnString string

type PSQLStore struct {
	log     *Loggers
	db      *sql.DB
	table   string
	queries pgQueries
	states  []State
}

func NewPSQLStore(log *Loggers, dsn PSQLConnString, table string, timeouts *Timeouts, states []State) (*PSQLStore, error) {
	if table == "" {
		table = "sagma_messages"
	}
	queries := makePgQueries(table, timeouts.RunnableLeftBehind)
	db, err := sql.Open("postgres", string(dsn))
	if err != nil {
		return nil, fmt.Errorf("cannot open database connection pool: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("cannot ping database: %w", err)
	}
	// TODO: create table if not exists
	return &PSQLStore{
		log:     log,
		db:      db,
		table:   table,
		queries: queries,
		states:  states,
	}, nil
}

func (s *PSQLStore) Store(transaction Transaction, id MsgID, blobID BlobID, st State, status StateStatus, ctx Context) error {
	t := transaction.(*txSQL)
	ctxJSON, err := json.Marshal(ctx)
	if err != nil {
		return fmt.Errorf("cannot marshal JSON context: %w", err)
	}
	if _, err := s.queries.insertNewContext(t.tx, id, st, status, blobID, string(ctxJSON)); err != nil {
		return fmt.Errorf("cannot insert message %s: %w", id, pgMaybeUserError(err))
	}
	return nil
}

func (s *PSQLStore) StoreContext(transaction Transaction, id MsgID, st State, ctx Context) error {
	t := transaction.(*txSQL)
	ctxJSON, err := json.Marshal(ctx)
	if err != nil {
		return fmt.Errorf("cannot marshall JSON from handler: %w", err)
	}
	res, err := s.queries.updateContext(t.tx, id, st, string(ctxJSON))
	if err != nil {
		return fmt.Errorf("cannot run update query for context: %w", err)
	}
	nrows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cannot get rows affected: %w", err)
	}
	if nrows == 0 {
		return newNotFoundError(fmt.Errorf("context for message %s at status %s was not updated", id, st))
	}
	return nil
}

func (s *PSQLStore) Fail(transaction Transaction, id MsgID, st State, reason error) error {
	t := transaction.(*txSQL)
	if _, err := s.queries.updateFailure(t.tx, id, st, reason); err != nil {
		return fmt.Errorf("cannot set error in database for message %s in state %s: %w", id, st, err)
	}
	return nil
}

func (s *PSQLStore) FetchStates(transaction Transaction, id MsgID, visitor MessageVisitor) error {
	t := transaction.(*txSQL)
	rows, err := s.queries.allByID(t.tx, id) // TODO: bind to query the returned fields for Scan
	if err != nil {
		return fmt.Errorf("cannot get all states for message %s: %w", id, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			state             string
			status            string
			failure           string
			ctxRaw            []byte
			created, modified time.Time
		)
		if err := rows.Scan(&state, &status, &created, &modified, &failure, &ctxRaw); err != nil {
			return fmt.Errorf("cannot scan row for message statuses: %w", err)
		}
		ctx := NewContext()
		if err := json.Unmarshal(ctxRaw, &ctx); err != nil {
			return fmt.Errorf("cannot unmarshal context JSON: %w", err)
		}
		if failure != "" {
			if err := visitor.Failed(id, State(state), errors.New(failure), ctx); err != nil {
				return err
			}
			continue
		}
		if err := visitor.Visit(id, State(state), StateStatus(status), ctx); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("cannot scan rows for message statuses: %w", err)
	}
	return nil
}

func (s *PSQLStore) Fetch(transaction Transaction, id MsgID, state State, status StateStatus) (BlobID, Context, error) {
	var blobID BlobID
	t := transaction.(*txSQL)
	row := s.queries.contextAndBlobIDAtStateStatus(t.tx, id, state, status)
	var (
		blobRaw string
		ctxRaw  []byte
	)
	if err := row.Scan(&blobRaw, &ctxRaw); err != nil {
		if err == sql.ErrNoRows {
			return blobID, nil, fmt.Errorf("cannot get message %s at state %s in status %s: no such entry in DB", id, state, status)
		}
		return blobID, nil, fmt.Errorf("cannot scan row for message status: %w", err)
	}
	blobID = BlobID(blobRaw)
	ctx := NewContext()
	if err := json.Unmarshal(ctxRaw, &ctx); err != nil {
		return blobID, nil, fmt.Errorf("cannot unmarshal context from database: %w", err)
	}
	return blobID, ctx, nil
}

func (s *PSQLStore) StoreStateStatus(transaction Transaction, id MsgID, st State, currStatus, nextStatus StateStatus) error {
	t := transaction.(*txSQL)
	if currStatus == stateStatusWaiting {
		if _, err := s.queries.insertNew(t.tx, id, st, nextStatus); err != nil {
			return fmt.Errorf("cannot insert placeholder for message %s at state %s in status %v: %w", id, st, currStatus, pgMaybeUserError(err))
		}
		return nil
	}
	res, err := s.queries.updateStatus(t.tx, nextStatus, id, st, currStatus)
	if err != nil {
		return fmt.Errorf("cannot update message %s at state %s from status %s: %w", id, st, currStatus, pgMaybeUserError(err))
	}
	nrows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cannot get rows affected: %w", err)
	}
	if nrows == 0 {
		return newNotFoundError(fmt.Errorf("message %s at status %s was not updated", id, currStatus))
	}
	return nil
}

func (s *PSQLStore) Archive(transaction Transaction, id MsgID) error {
	t := transaction.(*txSQL)
	res, err := s.queries.archive(t.tx, id)
	if err != nil {
		return fmt.Errorf("cannot archive message %s: %w", id, err)
	}
	nrows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cannot get rows affected: %w", err)
	}
	if nrows < int64(len(s.states)) {
		return fmt.Errorf("archiving of message %s changes %d states, but should have changed %d", id, nrows, len(s.states))
	}
	return nil
}

func (s *PSQLStore) FetchStateStatus(transaction Transaction, id MsgID, state State) (StateStatus, error) {
	t := transaction.(*txSQL)
	rows, err := s.queries.getByState(t.tx, id, state)
	if err != nil {
		return stateStatusWaiting, fmt.Errorf("cannot get current status of message %s at state %s: %w", id, state, err)
	}
	defer rows.Close()
	var status StateStatus
	for rows.Next() {
		if err := rows.Scan(&status); err != nil {
			return stateStatusWaiting, fmt.Errorf("cannot scan result row: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return stateStatusWaiting, fmt.Errorf("cannot scan result rows: %w", err)
	}
	if !status.IsValid() {
		return stateStatusWaiting, fmt.Errorf("invalid status %s retrieved from database", status)
	}
	return status, nil
}

func (s *PSQLStore) PollRunnables(ids chan<- StateID) error {
	var foundEntries bool
	for {
		tx, err := s.db.Begin()
		if err != nil {
			return fmt.Errorf("cannot create runnables transaction: %w", err)
		}
		defer tx.Commit() // Ignore errors, we only read
		rows, err := s.queries.allByStatus(tx, stateStatusReady)
		if err != nil {
			return fmt.Errorf("cannot query for runnables: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var (
				id    MsgID
				state State
			)
			if err := rows.Scan(&id, &state); err != nil {
				return fmt.Errorf("cannot scan for runnable row: %w", err)
			}
			foundEntries = true
			ids <- StateID{id: id, state: state}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("cannot scan for runnables rows: %w", err)
		}
		// if nothing was found, stop polling; entries get left behind only when
		// the process terminates. A new process will pick up the leftovers we leave on shutdown.
		if !foundEntries {
			break
		}
	}
	return nil
}

type txSQL struct {
	tx *sql.Tx
}

func (s *PSQLStore) Transaction(id MsgID) (Transaction, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("cannot create transaction: %w", err)
	}
	return &txSQL{tx: tx}, nil
}

func (t *txSQL) Discard(err error) error {
	if err := t.tx.Rollback(); err != nil {
		return fmt.Errorf("cannot rollback transaction: %w", err)
	}
	return err
}

func (t *txSQL) Commit() error {
	if err := t.tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction: %w", err)
	}
	return nil
}
