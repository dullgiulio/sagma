package main

import (
	"fmt"
	"sync"
)

type memstore struct {
	mux            sync.Mutex
	statusStateMsg map[stateStatus]map[state]map[msgID]*message
}

func newMemstore() *memstore {
	return &memstore{
		statusStateMsg: make(map[stateStatus]map[state]map[msgID]*message),
	}
}

func (m *memstore) Store(msg *message, st state, status stateStatus) error {
	stateMsg, ok := m.statusStateMsg[status]
	if !ok {
		stateMsg = make(map[state]map[msgID]*message)
		m.statusStateMsg[status] = stateMsg
	}
	msgs, ok := stateMsg[st]
	if !ok {
		msgs = make(map[msgID]*message)
		stateMsg[st] = msgs
	}
	if _, ok = msgs[msg.id]; ok {
		return fmt.Errorf("message %s already in store at state %s in status %s", msg.id, st, status)
	}
	msgs[msg.id] = msg
	return nil
}

func (m *memstore) Fetch(id msgID, state state, status stateStatus) (*message, error) {
	msg := func() *message {
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
	if msg == nil {
		return nil, fmt.Errorf("message %s not found at state %s in status %s", msg.id, state, status)
	}
	return msg, nil
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
			stateMsg = make(map[state]map[msgID]*message)
			m.statusStateMsg[nextStatus] = stateMsg
		}
		msgs, ok = stateMsg[st]
		if !ok {
			msgs = make(map[msgID]*message)
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

func (m *memstore) FetchRunnable() (msgID, state, error) {
	for state, stateMsg := range m.statusStateMsg[stateStatusReady] {
		for id := range stateMsg {
			return id, state, nil
		}
	}
	return "", "", nil
}

func (m *memstore) Transaction() Transaction {
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
