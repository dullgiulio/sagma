package sagma

import (
	"crypto/sha1"
	"fmt"
	"io"
	"path/filepath"
)

type Shardstore struct {
	stores map[byte]*Filestore
}

func NewShardstore(log *Loggers, prefix string, states []State, streamer StoreStreamer) (*Shardstore, error) {
	stores := make(map[byte]*Filestore)
	for _, c := range []byte("0123456789abcdef") {
		fs, err := NewFilestore(log, filepath.Join(prefix, string(c)), states, streamer)
		if err != nil {
			return nil, fmt.Errorf("cannot initialize sharded filestore: %w", err)
		}
		stores[c] = fs
	}
	return &Shardstore{
		stores: stores,
	}, nil
}

func (s *Shardstore) Store(tx Transaction, id MsgID, body io.Reader, st State, status StateStatus, ctx Context) error {
	hash, store := s.get(id)
	return store.Store(tx, hash, body, st, status, ctx)
}

func (s *Shardstore) StoreContext(tx Transaction, id MsgID, st State, ctx Context) error {
	hash, store := s.get(id)
	return store.StoreContext(tx, hash, st, ctx)
}

func (s *Shardstore) Fail(tx Transaction, id MsgID, state State, reason error) error {
	hash, store := s.get(id)
	return store.Fail(tx, hash, state, reason)
}

func (s *Shardstore) FetchStates(tx Transaction, id MsgID, visitor MessageVisitor) error {
	hash, store := s.get(id)
	return store.FetchStates(tx, hash, visitor)
}

func (s *Shardstore) Fetch(tx Transaction, id MsgID, state State, status StateStatus) (io.ReadCloser, Context, error) {
	hash, store := s.get(id)
	return store.Fetch(tx, hash, state, status)
}

func (s *Shardstore) StoreStateStatus(tx Transaction, id MsgID, st State, currStatus, nextStatus StateStatus) error {
	hash, store := s.get(id)
	return store.StoreStateStatus(tx, hash, st, currStatus, nextStatus)
}

func (s *Shardstore) Archive(tx Transaction, id MsgID) error {
	hash, store := s.get(id)
	return store.Archive(tx, hash)
}

func (s *Shardstore) FetchStateStatus(tx Transaction, id MsgID, state State) (StateStatus, error) {
	hash, store := s.get(id)
	return store.FetchStateStatus(tx, hash, state)
}

func (s *Shardstore) PollRunnables(ids chan<- StateID) error {
	errs := make(chan error, len(s.stores))
	for _, store := range s.stores {
		go func(store Store) {
			errs <- store.PollRunnables(ids)
		}(store)
	}
	var err error
	for e := range errs {
		if err == nil {
			err = e
		}
	}
	return err
}

func (s *Shardstore) Transaction(id MsgID) (Transaction, error) {
	hash, store := s.get(id)
	return store.Transaction(hash)
}

func (s *Shardstore) get(id MsgID) (MsgID, Store) {
	hash := fmt.Sprintf("%x", sha1.Sum([]byte(id)))
	return MsgID(hash), s.stores[hash[0]]
}
