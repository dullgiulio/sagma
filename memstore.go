package sagma

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
)

var _ BlobStore = NewBlobMemstore()

type BlobMemstore struct {
	mux    sync.Mutex
	values map[BlobID][]byte
}

func NewBlobMemstore() *BlobMemstore {
	return &BlobMemstore{
		values: make(map[BlobID][]byte),
	}
}

func (b *BlobMemstore) Put(id MsgID, state State, body io.Reader) (BlobID, error) {
	blobID := blobStoreID(id, state)
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return blobID, fmt.Errorf("cannot read full body into memory: %w", err)
	}

	b.mux.Lock()
	b.values[blobID] = data
	b.mux.Unlock()

	return blobID, nil
}

func (b *BlobMemstore) Get(id MsgID, state State) (io.ReadCloser, error) {
	blobID := blobStoreID(id, state)

	b.mux.Lock()
	defer b.mux.Unlock()

	bs, ok := b.values[blobID]
	if !ok {
		return nil, fmt.Errorf("blob of key %s does not exist", blobID)
	}
	return ioutil.NopCloser(bytes.NewReader(bs)), nil
}

func (b *BlobMemstore) Delete(blobID BlobID) error {
	b.mux.Lock()
	delete(b.values, blobID)
	b.mux.Unlock()

	return nil
}

var _ Store = NewMemstore()

type Memstore struct {
	mux            sync.Mutex
	statusStateMsg map[StateStatus]map[State]map[MsgID]BlobID
}

func NewMemstore() *Memstore {
	return &Memstore{
		statusStateMsg: make(map[StateStatus]map[State]map[MsgID]BlobID),
	}
}

// TODO: support context
func (m *Memstore) Store(tx Transaction, id MsgID, blobID BlobID, st State, status StateStatus, ctx Context) error {
	stateMsg, ok := m.statusStateMsg[status]
	if !ok {
		stateMsg = make(map[State]map[MsgID]BlobID)
		m.statusStateMsg[status] = stateMsg
	}
	msgs, ok := stateMsg[st]
	if !ok {
		msgs = make(map[MsgID]BlobID)
		stateMsg[st] = msgs
	}
	if _, ok = msgs[id]; ok {
		return fmt.Errorf("message %s already in store at state %s in status %s", id, st, status)
	}
	msgs[id] = blobID
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

func (m *Memstore) Fetch(tx Transaction, id MsgID, state State, status StateStatus) (BlobID, Context, error) {
	blobID, ok := func() (BlobID, bool) {
		stateMsg, ok := m.statusStateMsg[status]
		if !ok {
			return "", false
		}
		msgs, ok := stateMsg[state]
		if !ok {
			return "", false
		}
		return msgs[id], true
	}()
	if !ok {
		return blobID, nil, NotFoundError(fmt.Errorf("message %s not found at state %s in status %s", id, state, status))
	}
	ctx := Context(make(map[string]interface{})) // TODO: fetch
	return blobID, ctx, nil
}

func (m *Memstore) StoreStateStatus(tx Transaction, id MsgID, st State, currStatus, nextStatus StateStatus) error {
	// move from one map to the other
	msg, ok := func() (BlobID, bool) {
		stateMsg, ok := m.statusStateMsg[currStatus]
		if !ok {
			return "", false
		}
		msgs, ok := stateMsg[st]
		if !ok {
			return "", false
		}
		msg, ok := msgs[id]
		if !ok {
			return "", false
		}
		delete(msgs, id)
		return msg, true
	}()
	if !ok {
		return fmt.Errorf("cannot move message marker from state %s", currStatus)
	}
	stateMsg, ok := m.statusStateMsg[nextStatus]
	if !ok {
		stateMsg = make(map[State]map[MsgID]BlobID)
		m.statusStateMsg[nextStatus] = stateMsg
	}
	msgs, ok := stateMsg[st]
	if !ok {
		msgs = make(map[MsgID]BlobID)
		stateMsg[st] = msgs
	}
	msgs[id] = msg
	return nil
}

func (m *Memstore) Archive(tx Transaction, id MsgID) error {
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
