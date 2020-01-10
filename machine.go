package sagma

import (
	"fmt"
	"io"
	"sync"
)

type Machine struct {
	log       *Loggers
	saga      *Saga
	blobs     BlobStore
	store     Store
	runnables chan StateID
	shutdown  chan struct{}
	finished  chan struct{}
}

func NewMachine(saga *Saga, store Store, blobs BlobStore, log *Loggers, runbuf int) *Machine {
	m := &Machine{
		saga:      saga,
		store:     store,
		blobs:     blobs,
		log:       log,
		runnables: make(chan StateID, runbuf),
		shutdown:  make(chan struct{}),
		finished:  make(chan struct{}),
	}
	return m
}

func (m *Machine) runRunnable(id MsgID, state State) error {
	nextRunnables, err := m.transitionRunnable(id, state)
	if err != nil {
		return fmt.Errorf("cannot transition message %s at state %s: %w", id, state, err)
	}
	// doesn't matter if this gets interrupted mid-way by shutdown: unprocessed messages will be picked up at restart
	go func() {
		for _, next := range nextRunnables {
			if next.id == "" {
				continue
			}
			m.runnables <- next
		}
	}()
	return nil
}

func (m *Machine) runMachine(wg *sync.WaitGroup) {
	for {
		select {
		case runnable := <-m.runnables:
			if err := m.runRunnable(runnable.id, runnable.state); err != nil {
				m.log.err.Printf("runnable: %v\n", err)
			}
		case <-m.shutdown:
			wg.Done()
			return
		}
	}
}

func (m *Machine) Run(n int) {
	go func() {
		if err := m.store.PollRunnables(m.runnables); err != nil {
			m.log.err.Printf("store scanning failed: %w", err)
		}
	}()
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go m.runMachine(&wg)
	}
	wg.Wait()
	close(m.finished)
}

func (m *Machine) Shutdown() {
	close(m.shutdown)
	<-m.finished
}

func (m *Machine) archive(id MsgID) error {
	transaction, err := m.store.Transaction(id)
	if err != nil {
		return fmt.Errorf("cannot open archive transaction: %w", err)
	}
	if err := m.store.Archive(transaction, id); err != nil {
		return transaction.Discard(fmt.Errorf("store disposal failed: %w", err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction for end handling: %w", err)
	}
	return nil
}

func (m *Machine) commitFailure(id MsgID, currState State, reason error) error {
	transaction, err := m.store.Transaction(id)
	if err != nil {
		return fmt.Errorf("cannot open commit failure transaction: %w", err)
	}
	if err := m.store.Fail(transaction, id, currState, reason); err != nil {
		return transaction.Discard(fmt.Errorf("cannot store failure state: %w", err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit failure transaction: %w", err)
	}
	return nil
}

func (m *Machine) retry(n int, fn func() error) error {
	var err error
	for retries := 0; retries < n; retries++ {
		err = fn()
		if err == nil {
			break
		}
		m.log.err.Printf("retying after error: %v\n", err)
	}
	if err != nil {
		return fmt.Errorf("failed after %d retries: %w", n, err)
	}
	return nil
}

func (m *Machine) transitionRunnable(id MsgID, state State) ([]StateID, error) {
	var nextRunnables []StateID

	body, err := m.blobs.Get(id, state)
	if err != nil {
		return nil, fmt.Errorf("cannot get message blob from store for handler: %w", err)
	}

	transaction, err := m.store.Transaction(id)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction: %w", err)
	}
	// book runnable for exclusive start
	if err := m.store.StoreStateStatus(transaction, id, state, stateStatusReady, stateStatusRunning); err != nil {
		return nil, transaction.Discard(fmt.Errorf("cannot mark handler started: %w", err))
	}
	_, ctx, err := m.store.Fetch(transaction, id, state, stateStatusRunning)
	if err != nil {
		return nil, transaction.Discard(fmt.Errorf("cannot fetch message for handler: %w", err))
	}
	if err := transaction.Commit(); err != nil {
		return nil, fmt.Errorf("cannot commit transaction for start handling: %w", err)
	}

	handler, ok := m.saga.handlers[state]
	if !ok {
		return nil, fmt.Errorf("state %s does not have a handler, execution finished", state)
	}

	// sets current state and message ID for handler to update context data
	saveCtx := func(ctx Context) error {
		return m.SaveContext(id, state, ctx)
	}

	nextStates, handlerErr := handler(id, ctx, body, saveCtx)
	if handlerErr != nil {
		commErr := m.retry(10, func() error {
			return m.commitFailure(id, state, handlerErr)
		})
		if commErr != nil {
			m.log.err.Printf("cannot commit failed handler result: %w", commErr)
		}
		return nil, fmt.Errorf("handler returned error: %w", handlerErr)
	}

	// errors while committing should be retried, we know the handler ran
	err = m.retry(10, func() error {
		transaction, err := m.store.Transaction(id)
		if err != nil {
			return fmt.Errorf("cannot open handler finished transaction: %w", err)
		}
		if err := m.store.StoreStateStatus(transaction, id, state, stateStatusRunning, stateStatusDone); err != nil {
			return transaction.Discard(fmt.Errorf("cannot mark handler finished: %w", err))
		}
		for _, nextState := range nextStates.states {
			if nextState.IsEnd() {
				continue
			}

			nextStateStatus, err := m.computeNextStateStatus(transaction, id, nextState)
			if err != nil {
				return transaction.Discard(fmt.Errorf("cannot mark next runnable: %w", err))
			}
			// if succeeded, mark next state as ready to run if it has a message already
			if nextStateStatus == stateStatusReady {
				nextRunnable := StateID{
					id:    id,
					state: nextState,
				}
				nextRunnables = append(nextRunnables, nextRunnable)
			}
		}
		if err = transaction.Commit(); err != nil {
			return fmt.Errorf("cannot commit transaction for end handling: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("commit retry: %w", err)
	}
	if nextStates == SagaEnd {
		if err := m.archive(id); err != nil {
			return nil, fmt.Errorf("cannot archive of completed message saga for message %s: %w", id, err)
		}
	}
	return nextRunnables, nil
}

func (m *Machine) computeNextStateStatus(transaction Transaction, id MsgID, nextState State) (StateStatus, error) {
	var nextStateStatus StateStatus
	currStateStatus, err := m.store.FetchStateStatus(transaction, id, nextState)
	if err != nil {
		return nextStateStatus, fmt.Errorf("cannot fetch state status: %w", err)
	}
	switch currStateStatus {
	case stateStatusWaiting:
		nextStateStatus = stateStatusReadyWaiting
	case stateStatusRecvWaiting:
		nextStateStatus = stateStatusReady
	default:
		return nextStateStatus, fmt.Errorf("message %s is in unexpected state %s status %s", id, nextState, currStateStatus)
	}

	if err = m.store.StoreStateStatus(transaction, id, nextState, currStateStatus, nextStateStatus); err != nil {
		return nextStateStatus, fmt.Errorf("could not set state %s status %s: %w", nextState, nextStateStatus, err)
	}
	return nextStateStatus, nil
}

func (m *Machine) FetchStates(id MsgID, visitor MessageVisitor) error {
	transaction, err := m.store.Transaction(id)
	if err != nil {
		return fmt.Errorf("cannot open fetch states transaction: %w", err)
	}
	if err := m.store.FetchStates(transaction, id, visitor); err != nil {
		return transaction.Discard(fmt.Errorf("cannot fetch states of %s: %w", id, err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot close read-only transaction: %w", err)
	}
	return nil
}

func (m *Machine) Fetch(id MsgID, state State) (io.ReadCloser, Context, error) {
	transaction, err := m.store.Transaction(id)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot open fetch transaction: %w", err)
	}
	_, ctx, err := m.store.Fetch(transaction, id, state, stateStatusDone)
	if err != nil {
		return nil, nil, transaction.Discard(fmt.Errorf("cannot fetch message: %w", err))
	}
	if err := transaction.Commit(); err != nil {
		return nil, nil, fmt.Errorf("cannot close read-only transaction: %w", err)
	}
	rc, err := m.blobs.Get(id, state)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot get message blob from store: %w", err)
	}
	return rc, ctx, nil
}

func (m *Machine) SaveContext(id MsgID, state State, ctx Context) error {
	transaction, err := m.store.Transaction(id)
	if err != nil {
		return fmt.Errorf("cannot open save context transaction: %w", err)
	}
	if err := m.store.StoreContext(transaction, id, state, ctx); err != nil {
		return transaction.Discard(fmt.Errorf("cannot save context for message %s at state %s: %w", id, state, err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit save context transaction: %w", err)
	}
	return nil
}

func (m *Machine) Receive(id MsgID, state State, ctx Context, body io.Reader) error {
	blobID, err := m.blobs.Put(id, state, body)
	if err != nil {
		return fmt.Errorf("cannot put data into blob store: %w", err)
	}

	transaction, err := m.store.Transaction(id)
	if err != nil {
		return fmt.Errorf("cannot open receive transaction: %w", err)
	}
	removeBlob := func() {
		if err := m.blobs.Delete(blobID); err != nil {
			m.log.err.Printf("cannot remove blob while discarding transaction to store for message %s at state %s: %w", id, state, err)
		}
	}
	discard := func(err error) error {
		removeBlob()
		return transaction.Discard(err)
	}

	currStatus, err := m.store.FetchStateStatus(transaction, id, state)
	if err != nil {
		return discard(fmt.Errorf("cannot fetch status of state %s for %s: %w", state, id, err))
	}

	var nextStatus StateStatus
	switch currStatus {
	case stateStatusWaiting:
		nextStatus = stateStatusRecvWaiting
		if state == m.saga.initial {
			nextStatus = stateStatusReady
		}
	case stateStatusReadyWaiting:
		nextStatus = stateStatusReady
	default:
		return discard(fmt.Errorf("trying to store message %s at state %s in invalid status %s", id, state, currStatus))
	}

	if err := m.store.Store(transaction, id, blobID, state, nextStatus, ctx); err != nil {
		return discard(fmt.Errorf("cannot store message at state %s: %w", state, err))
	}

	if err := transaction.Commit(); err != nil {
		removeBlob()
		return fmt.Errorf("cannot commit marking of next runnable: %w", err)
	}

	if nextStatus == stateStatusReady {
		m.runnables <- StateID{id: id, state: state}
	}

	return nil
}
