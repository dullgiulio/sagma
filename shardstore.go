package main

import (
	"crypto/sha1"
	"fmt"
	"io"
	"path/filepath"
)

type shardstore struct {
	stores map[byte]*filestore
}

func newShardstore(log *Loggers, prefix string, states []state, streamer StoreStreamer) (*shardstore, error) {
	stores := make(map[byte]*filestore)
	for _, c := range []byte("0123456789abcdef") {
		fs, err := newFilestore(log, filepath.Join(prefix, string(c)), states, streamer)
		if err != nil {
			return nil, fmt.Errorf("cannot initialize sharded filestore: %v", err)
		}
		stores[c] = fs
	}
	return &shardstore{
		stores: stores,
	}, nil
}

func (s *shardstore) Store(id msgID, body io.Reader, st state, status stateStatus) error {
	hash, store := s.get(id)
	return store.Store(hash, body, st, status)
}

func (s *shardstore) Fetch(id msgID, state state, status stateStatus) (io.ReadCloser, error) {
	hash, store := s.get(id)
	return store.Fetch(hash, state, status)
}

func (s *shardstore) StoreStateStatus(id msgID, st state, currStatus, nextStatus stateStatus) error {
	hash, store := s.get(id)
	return store.StoreStateStatus(hash, st, currStatus, nextStatus)
}

func (s *shardstore) Dispose(id msgID) error {
	hash, store := s.get(id)
	return store.Dispose(hash)
}

func (s *shardstore) FetchStateStatus(id msgID, state state) (stateStatus, error) {
	hash, store := s.get(id)
	return store.FetchStateStatus(hash, state)
}

func (s *shardstore) PollRunnables(ids chan<- stateID) error {
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

func (s *shardstore) Transaction(id msgID) Transaction {
	hash, store := s.get(id)
	return store.Transaction(hash)
}

func (s *shardstore) get(id msgID) (msgID, Store) {
	hash := fmt.Sprintf("%x", sha1.Sum([]byte(id)))
	return msgID(hash), s.stores[hash[0]]
}
