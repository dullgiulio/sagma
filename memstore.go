package main

import (
	"fmt"
	"sync"
)

type memstore struct {
	mux sync.Mutex

	messagesByID map[msgID]map[state]*message
	statusByID   map[msgID]map[state]*messageStatus

	runnable chan *msgIDState
}

type msgIDState struct {
	id    msgID
	state state
}

func newMemstore() *memstore {
	return &memstore{
		messagesByID: make(map[msgID]map[state]*message),
		statusByID:   make(map[msgID]map[state]*messageStatus),
		runnable:     make(chan *msgIDState, 100),
	}
}

func (m *memstore) Store(msg *message) error {
	func() {
		states, ok := m.messagesByID[msg.id]
		if !ok {
			states = make(map[state]*message)
		}
		states[msg.state] = msg
		m.messagesByID[msg.id] = states
	}()
	func() {
		states, ok := m.statusByID[msg.id]
		if !ok {
			states = make(map[state]*messageStatus)
		}
		states[msg.state] = &messageStatus{
			state:    msg.state,
			received: true,
		}
		m.statusByID[msg.id] = states
	}()
	return nil
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

// idempotent, if already marked we should not care
func (m *memstore) MarkRunnable(id msgID, state state) error {
	marked := m.statusByID[id][state].runnable // TODO: check existence
	if marked {
		return nil
	}
	m.runnable <- &msgIDState{id: id, state: state}
	m.statusByID[id][state].runnable = true
	return nil
}

func (m *memstore) FetchRunnable() (msgID, state, error) {
	ms := <-m.runnable
	return ms.id, ms.state, nil
}

func (m *memstore) LockHandling(id msgID, state state) error {
	states, ok := m.statusByID[id]
	if !ok {
		return fmt.Errorf("message %s not in status store", id)
	}
	if !states[state].received {
		return fmt.Errorf("message %s not marked as received", id)
	}
	if states[state].handleStarted {
		return fmt.Errorf("message %s already marked as handler started", id)
	}
	states[state].handleStarted = true
	return nil
}

func (m *memstore) UnlockHandling(id msgID, state state) error {
	states, ok := m.statusByID[id]
	if !ok {
		return fmt.Errorf("message %s not in status store", id)
	}
	status := states[state]
	if !status.received {
		return fmt.Errorf("message %s not marked as received", id)
	}
	if !status.handleStarted {
		return fmt.Errorf("message %s not marked as handler started", id)
	}
	if status.handleFinished {
		return fmt.Errorf("message %s already marked as handler finished", id)
	}
	states[state].handleFinished = true
	return nil
}

func (m *memstore) FetchAtState(id msgID, state state) (*message, error) {
	states, ok := m.messagesByID[id]
	if !ok {
		return nil, fmt.Errorf("message %s not in store", id)
	}
	msg, ok := states[state]
	if !ok {
		return nil, fmt.Errorf("state %s not completed for message %d", state, id)
	}
	return msg, nil
}

func (m *memstore) FetchStates(id msgID, saga *saga) (map[state]messageStatus, error) {
	statuses, ok := m.statusByID[id]
	if !ok {
		return nil, fmt.Errorf("unknown message %s", id)
	}
	// copy statuses map to avoid data races
	msgStatuses := make(map[state]messageStatus)
	for state, status := range statuses {
		msgStatuses[state] = *status
	}
	return msgStatuses, nil
}
