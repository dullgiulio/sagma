package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
)

var _ Store = newMemstore()

type memstore struct {
	mux            sync.Mutex
	statusStateMsg map[stateStatus]map[state]map[msgID][]byte
}

func newMemstore() *memstore {
	return &memstore{
		statusStateMsg: make(map[stateStatus]map[state]map[msgID][]byte),
	}
}

func (m *memstore) Store(id msgID, body io.Reader, st state, status stateStatus) error {
	stateMsg, ok := m.statusStateMsg[status]
	if !ok {
		stateMsg = make(map[state]map[msgID][]byte)
		m.statusStateMsg[status] = stateMsg
	}
	msgs, ok := stateMsg[st]
	if !ok {
		msgs = make(map[msgID][]byte)
		stateMsg[st] = msgs
	}
	if _, ok = msgs[id]; ok {
		return fmt.Errorf("message %s already in store at state %s in status %s", id, st, status)
	}
	buf, err := ioutil.ReadAll(body)
	if err != nil {
		return fmt.Errorf("could not copy body: %v", err)
	}
	msgs[id] = buf
	return nil
}

func (m *memstore) Fail(id msgID, state state, reason error) error {
	// TODO
	return nil
}

func (m *memstore) Fetch(id msgID, state state, status stateStatus) (io.ReadCloser, error) {
	buf := func() []byte {
		stateMsg, ok := m.statusStateMsg[status]
		if !ok {
			return nil
		}
		msgs, ok := stateMsg[state]
		if !ok {
			return nil
		}
		return msgs[id]
	}()
	if buf == nil {
		return nil, fmt.Errorf("message %s not found at state %s in status %s", id, state, status)
	}
	return ioutil.NopCloser(bytes.NewReader(buf)), nil
}

func (m *memstore) StoreStateStatus(id msgID, st state, currStatus, nextStatus stateStatus) error {
	// move from one map to the other
	done := func() bool {
		stateMsg, ok := m.statusStateMsg[currStatus]
		if !ok {
			return false
		}
		msgs, ok := stateMsg[st]
		if !ok {
			return false
		}
		msg, ok := msgs[id]
		if !ok {
			return false
		}
		delete(msgs, id)
		stateMsg, ok = m.statusStateMsg[nextStatus]
		if !ok {
			stateMsg = make(map[state]map[msgID][]byte)
			m.statusStateMsg[nextStatus] = stateMsg
		}
		msgs, ok = stateMsg[st]
		if !ok {
			msgs = make(map[msgID][]byte)
			stateMsg[st] = msgs
		}
		msgs[id] = msg
		return true
	}()
	if !done {
		return fmt.Errorf("cannot change status to %s for message %s at state %s in status %s", nextStatus, id, st, currStatus)
	}
	return nil
}

func (m *memstore) Dispose(id msgID) error {
	for _, stateMsg := range m.statusStateMsg {
		for _, msgs := range stateMsg {
			delete(msgs, id)
		}
	}
	return nil
}

func (m *memstore) FetchStateStatus(id msgID, state state) (stateStatus, error) {
	for status, stateMsg := range m.statusStateMsg {
		msgs, ok := stateMsg[state]
		if !ok {
			continue
		}
		if _, ok = msgs[id]; ok {
			return status, nil
		}
	}
	return stateStatusWaiting, nil
}

func (m *memstore) PollRunnables(chan<- stateID) error {
	return nil
}

func (m *memstore) Transaction(id msgID) Transaction {
	m.mux.Lock()
	return m
}

func (m *memstore) Commit() error {
	m.mux.Unlock()
	return nil
}

func (m *memstore) Discard(err error) error {
	m.mux.Unlock()
	return err
}
