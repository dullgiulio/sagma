package sagma

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
)

var _ Store = NewMemstore()

type Memstore struct {
	mux            sync.Mutex
	statusStateMsg map[StateStatus]map[State]map[MsgID][]byte
}

func NewMemstore() *Memstore {
	return &Memstore{
		statusStateMsg: make(map[StateStatus]map[State]map[MsgID][]byte),
	}
}

// TODO: support context
func (m *Memstore) Store(tx Transaction, id MsgID, body io.Reader, st State, status StateStatus, ctx Context) error {
	stateMsg, ok := m.statusStateMsg[status]
	if !ok {
		stateMsg = make(map[State]map[MsgID][]byte)
		m.statusStateMsg[status] = stateMsg
	}
	msgs, ok := stateMsg[st]
	if !ok {
		msgs = make(map[MsgID][]byte)
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

func (m *Memstore) StoreContext(tx Transaction, id MsgID, state State, ctx Context) error {
	// TODO
	return nil
}

func (m *Memstore) Fail(tx Transaction, id MsgID, state State, reason error) error {
	// TODO
	return nil
}

func (m *Memstore) FetchStates(tx Transaction, id MsgID, visitor MessageVisitor) error {
	ctx := Context(make(map[string]interface{}))
	for status, stateMsg := range m.statusStateMsg {
		for state, msgs := range stateMsg {
			if _, ok := msgs[id]; ok {
				visitor.Visit(id, state, status, ctx)
			}
		}
	}
	return nil
}

func (m *Memstore) Fetch(tx Transaction, id MsgID, state State, status StateStatus) (io.ReadCloser, Context, error) {
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
		return nil, nil, NotFoundError(fmt.Errorf("message %s not found at state %s in status %s", id, state, status))
	}
	ctx := Context(make(map[string]interface{})) // TODO: fetch
	return ioutil.NopCloser(bytes.NewReader(buf)), ctx, nil
}

func (m *Memstore) StoreStateStatus(tx Transaction, id MsgID, st State, currStatus, nextStatus StateStatus) error {
	removeCurrent := func() []byte {
		stateMsg, ok := m.statusStateMsg[currStatus]
		if !ok {
			return nil
		}
		msgs, ok := stateMsg[st]
		if !ok {
			return nil
		}
		msg, ok := msgs[id]
		if !ok {
			return nil
		}
		delete(msgs, id)
		return msg
	}
	// move from one map to the other
	msg := removeCurrent()
	stateMsg, ok := m.statusStateMsg[nextStatus]
	if !ok {
		stateMsg = make(map[State]map[MsgID][]byte)
		m.statusStateMsg[nextStatus] = stateMsg
	}
	msgs, ok := stateMsg[st]
	if !ok {
		msgs = make(map[MsgID][]byte)
		stateMsg[st] = msgs
	}
	msgs[id] = msg
	return nil
}

func (m *Memstore) Dispose(tx Transaction, id MsgID) error {
	for _, stateMsg := range m.statusStateMsg {
		for _, msgs := range stateMsg {
			delete(msgs, id)
		}
	}
	return nil
}

func (m *Memstore) FetchStateStatus(tx Transaction, id MsgID, state State) (StateStatus, error) {
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

func (m *Memstore) PollRunnables(chan<- StateID) error {
	return nil
}

func (m *Memstore) Transaction(id MsgID) (Transaction, error) {
	m.mux.Lock()
	return m, nil
}

func (m *Memstore) Commit() error {
	m.mux.Unlock()
	return nil
}

func (m *Memstore) Discard(err error) error {
	m.mux.Unlock()
	return err
}
