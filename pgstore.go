package sagma

import (
	"crypto/sha1"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

type psqlTx struct {
	txs map[MsgID]*sql.Tx
	mux sync.Mutex
}

type blobBasename string

type blobFolder string

func (b blobFolder) basename(id MsgID) blobBasename {
	return blobBasename(fmt.Sprintf("%x", sha1.Sum([]byte(id))))
}

func (b blobFolder) file(streamer StoreStreamer, state State, name blobBasename) string {
	return filepath.Join(string(b), string(state), string(name[0:2]), streamer.Filename(string(name)))
}

type PSQLConnString string

type PSQLStore struct {
	log      *Loggers
	db       *sql.DB
	table    string
	folder   blobFolder
	streamer StoreStreamer
}

// TODO: generate with table variable
const (
	pgQueryINSERT         = `INSERT INTO messages (id, state, status, created, modified, fileid) VALUES ($1, $2, $3, NOW(), NOW(), $4);`
	pgQueryFailUPDATE     = `UPDATE messages SET failed = $1 WHERE id = $3 AND state = $4;`
	pgQueryUPDATE         = `UPDATE messages SET status = $1 WHERE id = $2 AND state = $3 AND status = $4;`
	pgQueryGetStateStatus = `SELECT 1 FROM messages WHERE id = $1 AND state = $2 AND status = $3 LIMIT 1;`
	pgQueryAllByID        = `SELECT state, status, created, modified, error FROM messages WHERE id = $1;`
	pgQueryGetStatus      = `SELECT status FROM messages WHERE id = $1 AND state = $2 LIMIT 1;`
	pgQueryRunnables      = `SELECT id, state FROM messages WHERE status = $1 LIMIT 100;`
)

func NewPSQLStore(log *Loggers, dsn PSQLConnString, folder string, streamer StoreStreamer, table string) (*PSQLStore, error) {
	if streamer == nil {
		streamer = NopStreamer{}
	}
	// TODO: hardcoded table name for now
	/*
		if table == "" {
			table = "sagma_messages"
		}
	*/
	db, err := sql.Open("postgres", string(dsn))
	if err != nil {
		return nil, fmt.Errorf("cannot open database connection pool: %v", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("cannot ping database: %v", err)
	}
	// TODO: create table if not exists
	return &PSQLStore{
		log:      log,
		db:       db,
		table:    table,
		folder:   blobFolder(folder),
		streamer: streamer,
	}, nil
}

func (s *PSQLStore) Store(transaction Transaction, id MsgID, body io.Reader, st State, status StateStatus) error {
	basename := s.folder.basename(id)
	filename := s.folder.file(s.streamer, st, basename)
	if err := os.MkdirAll(filepath.Dir(string(filename)), 0744); err != nil {
		return fmt.Errorf("cannot make message folder in blob store: %v", err)
	}
	if err := writeFile(s.streamer, filename, body, 0644); err != nil {
		return fmt.Errorf("cannot write file to blob store: %v", err)
	}
	t := transaction.(*txSQL)
	t.onDiscard(func() {
		if err := os.Remove(filename); err != nil {
			s.log.err.Printf("message %s at state %s in status %s: cannot remove file %s on transaction rollback: %v", id, st, status, filename, err)
		}
	})
	if _, err := t.tx.Exec(pgQueryINSERT, string(id), string(st), string(status), string(basename)); err != nil {
		return fmt.Errorf("cannot insert message %s: %v", id, err)
	}
	return nil
}

func (s *PSQLStore) Fail(transaction Transaction, id MsgID, st State, reason error) error {
	t := transaction.(*txSQL)
	if _, err := t.tx.Exec(pgQueryFailUPDATE, reason.Error(), string(id), string(st)); err != nil {
		return fmt.Errorf("cannot set error in database for message %s in state %s: %v", id, st, err)
	}
	return nil
}

func (s *PSQLStore) FetchStates(transaction Transaction, id MsgID, visitor MessageVisitor) error {
	t := transaction.(*txSQL)
	rows, err := t.tx.Query(pgQueryAllByID, string(id))
	if err != nil {
		return fmt.Errorf("cannot get all states for message %s: %v", id, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			state             string
			status            string
			created, modified time.Time
			failure           string
		)
		if err := rows.Scan(&state, &status, &created, &modified, &failure); err != nil {
			return fmt.Errorf("cannot scan row for message statuses: %v", err)
		}
		if failure != "" {
			if err := visitor.Failed(id, State(state), errors.New(failure)); err != nil {
				return err
			}
			continue
		}
		if err := visitor.Visit(id, State(state), StateStatus(status)); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("cannot scan rows for message statuses: %v", err)
	}
	return nil
}

func (s *PSQLStore) Fetch(transaction Transaction, id MsgID, state State, status StateStatus) (io.ReadCloser, error) {
	t := transaction.(*txSQL)
	row := t.tx.QueryRow(pgQueryGetStateStatus, string(id), string(state), string(status))
	var dummy int
	if err := row.Scan(&dummy); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("cannot get message %s at state %s in status %s: no such entry in DB", id, state, status)
		}
		return nil, fmt.Errorf("cannot scan row for message status: %v", err)
	}
	basename := s.folder.basename(id)
	filename := s.folder.file(s.streamer, state, basename)
	fh, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open blob store file: %v", err)
	}
	r, err := s.streamer.Reader(fh)
	if err != nil {
		return nil, fmt.Errorf("cannot wrap reader with streamer: %v", err)
	}
	return r, nil
}

func (s *PSQLStore) StoreStateStatus(transaction Transaction, id MsgID, st State, currStatus, nextStatus StateStatus) error {
	t := transaction.(*txSQL)
	if currStatus == stateStatusWaiting {
		basename := s.folder.basename(id)
		if _, err := t.tx.Exec(pgQueryINSERT, string(id), string(st), string(nextStatus), string(basename)); err != nil {
			return fmt.Errorf("cannot insert placeholder for message %s at state %s in status %v: %v", id, st, currStatus, err)
		}
		return nil
	}
	res, err := t.tx.Exec(pgQueryUPDATE, string(nextStatus), string(id), string(st), string(currStatus))
	if err != nil {
		return fmt.Errorf("cannot update message %s at state %s from status %s: %v", id, st, currStatus, err)
	}
	nrows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cannot get rows affected: %v", err)
	}
	if nrows == 0 {
		return fmt.Errorf("message %s at status %s was not updated", id, currStatus)
	}
	return nil
}

func (s *PSQLStore) Dispose(transaction Transaction, id MsgID) error {
	// TODO: for each state and status, if msg exists, move to archived folder
	return nil
}

func (s *PSQLStore) FetchStateStatus(transaction Transaction, id MsgID, state State) (StateStatus, error) {
	t := transaction.(*txSQL)
	rows, err := t.tx.Query(pgQueryGetStatus, string(id), string(state))
	if err != nil {
		return stateStatusWaiting, fmt.Errorf("cannot get current status of message %s at state %s: %v", id, state, err)
	}
	defer rows.Close()
	var status StateStatus
	for rows.Next() {
		if err := rows.Scan(&status); err != nil {
			return stateStatusWaiting, fmt.Errorf("cannot scan result row: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		return stateStatusWaiting, fmt.Errorf("cannot scan result rows: %v", err)
	}
	if !status.IsValid() {
		return stateStatusWaiting, fmt.Errorf("invalid status %s retrieved from database", status)
	}
	return status, nil
}

func (s *PSQLStore) PollRunnables(ids chan<- StateID) error {
	rows, err := s.db.Query(pgQueryRunnables, string(stateStatusReady))
	if err != nil {
		return fmt.Errorf("cannot query for runnables: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id    MsgID
			state State
		)
		if err := rows.Scan(&id, &state); err != nil {
			return fmt.Errorf("cannot scan for runnable row: %v", err)
		}
		ids <- StateID{id: id, state: state}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("cannot scan for runnables rows: %v", err)
	}
	return nil
}

type txSQL struct {
	tx       *sql.Tx
	rollback []func()
}

func (t *txSQL) onDiscard(fn func()) {
	t.rollback = append(t.rollback, fn)
}

func (s *PSQLStore) Transaction(id MsgID) (Transaction, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("cannot create transaction: %v", err)
	}
	return &txSQL{tx: tx}, nil
}

func (t *txSQL) Discard(err error) error {
	for _, fn := range t.rollback {
		fn()
	}
	if err := t.tx.Rollback(); err != nil {
		return fmt.Errorf("cannot rollback transaction: %v", err)
	}
	return err
}

func (t *txSQL) Commit() error {
	if err := t.tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction: %v", err)
	}
	return nil
}
