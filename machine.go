package sagma

import (
	"fmt"
	"io"
	"sync"
)

type Machine struct {
	log       *Loggers
	saga      *Saga
	store     Store
	runnables chan StateID
	shutdown  chan struct{}
	finished  chan struct{}
}

func NewMachine(saga *Saga, store Store, log *Loggers, runbuf int) *Machine {
	m := &Machine{
		saga:      saga,
		store:     store,
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
		return fmt.Errorf("cannot transition message %s at state %s: %v", id, state, err)
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
			m.log.err.Printf("store scanning failed: %v", err)
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

func (m *Machine) dispose(id MsgID) error {
	transaction, err := m.store.Transaction(id)
	if err != nil {
		return fmt.Errorf("cannot open dispose transaction: %v", err)
	}
	if err := m.store.Dispose(transaction, id); err != nil {
		return transaction.Discard(fmt.Errorf("store disposal failed: %v", err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction for end handling: %v", err)
	}
	return nil
}

func (m *Machine) commitFailure(id MsgID, currState State, reason error) error {
	transaction, err := m.store.Transaction(id)
	if err != nil {
		return fmt.Errorf("cannot open commit failure transaction: %v", err)
	}
	if err := m.store.Fail(transaction, id, currState, reason); err != nil {
		return transaction.Discard(fmt.Errorf("cannot store failure state: %v", err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit failure transaction: %v", err)
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
		return fmt.Errorf("failed after %d retries: %v", n, err)
	}
	return nil
}

func (m *Machine) transitionRunnable(id MsgID, state State) ([]StateID, error) {
	var nextRunnables []StateID

	transaction, err := m.store.Transaction(id)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction: %v", err)
	}
	// book runnable for exclusive start
	if err := m.store.StoreStateStatus(transaction, id, state, stateStatusReady, stateStatusRunning); err != nil {
		return nil, transaction.Discard(fmt.Errorf("cannot mark handler started: %v", err))
	}
	body, err := m.store.Fetch(transaction, id, state, stateStatusRunning)
	if err != nil {
		return nil, transaction.Discard(fmt.Errorf("cannot fetch message for handler: %v", err))
	}
	// TODO: catch closer error
	defer body.Close()
	if err := transaction.Commit(); err != nil {
		return nil, fmt.Errorf("cannot commit transaction for start handling: %v", err)
	}

	handler, ok := m.saga.handlers[state]
	if !ok {
		return nil, fmt.Errorf("state %s does not have a handler, execution finished", state)
	}

	nextStates, handlerErr := handler(id, body)
	if handlerErr != nil {
		commErr := m.retry(10, func() error {
			return m.commitFailure(id, state, handlerErr)
		})
		if commErr != nil {
			m.log.err.Printf("cannot commit failed handler result: %v", commErr)
		}
		return nil, fmt.Errorf("handler returned error: %v", handlerErr)
	}

	// errors while committing should be retried, we know the handler ran
	err = m.retry(10, func() error {
		transaction, err := m.store.Transaction(id)
		if err != nil {
			return fmt.Errorf("cannot open handler finished transaction: %v", err)
		}
		if err := m.store.StoreStateStatus(transaction, id, state, stateStatusRunning, stateStatusDone); err != nil {
			return transaction.Discard(fmt.Errorf("cannot mark handler finished: %v", err))
		}
		for _, nextState := range nextStates {
			if nextState.IsEnd() {
				continue
			}

			nextStateStatus, err := m.computeNextStateStatus(transaction, id, nextState)
			if err != nil {
				return transaction.Discard(fmt.Errorf("cannot mark next runnable: %v", err))
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
			return fmt.Errorf("cannot commit transaction for end handling: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("commit retry: %v", err)
	}
	// TODO: move this; dispose should be called when all states returned END as next
	/*
		if err := m.dispose(id); err != nil {
			return nil, fmt.Errorf("cannot dispose of completed message saga for message %s: %v", id, err)
		}
	*/
	return nextRunnables, nil
}

func (m *Machine) computeNextStateStatus(transaction Transaction, id MsgID, nextState State) (StateStatus, error) {
	var nextStateStatus StateStatus
	currStateStatus, err := m.store.FetchStateStatus(transaction, id, nextState)
	if err != nil {
		return nextStateStatus, fmt.Errorf("cannot fetch state status: %v", err)
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
		return nextStateStatus, fmt.Errorf("could not set state %s status %s: %v", nextState, nextStateStatus, err)
	}
	return nextStateStatus, nil
}

func (m *Machine) FetchStates(id MsgID, visitor MessageVisitor) error {
	transaction, err := m.store.Transaction(id)
	if err != nil {
		return fmt.Errorf("cannot open fetch states transaction: %v", err)
	}
	if err := m.store.FetchStates(transaction, id, visitor); err != nil {
		return transaction.Discard(fmt.Errorf("cannot fetch states of %s: %v", id, err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot close read-only transaction: %v", err)
	}
	return nil
}

func (m *Machine) Fetch(id MsgID, state State) (io.ReadCloser, error) {
	transaction, err := m.store.Transaction(id)
	if err != nil {
		return nil, fmt.Errorf("cannot open fetch transaction: %v", err)
	}
	body, err := m.store.Fetch(transaction, id, state, stateStatusDone)
	if err != nil {
		return nil, transaction.Discard(fmt.Errorf("cannot fetch message: %v", err))
	}
	if err := transaction.Commit(); err != nil {
		return nil, fmt.Errorf("cannot close read-only transaction: %v", err)
	}
	return body, nil
}

func (m *Machine) Receive(id MsgID, body io.ReadCloser, state State) error {
	err := m.receive(id, body, state)
	if err1 := body.Close(); err == nil {
		err = err1
	}
	return err
}

func (m *Machine) receive(id MsgID, body io.ReadCloser, state State) error {
	transaction, err := m.store.Transaction(id)
	if err != nil {
		return fmt.Errorf("cannot open receive transaction: %v", err)
	}
	currStatus, err := m.store.FetchStateStatus(transaction, id, state)
	if err != nil {
		return transaction.Discard(fmt.Errorf("cannot fetch status of state %s for %s: %v", state, id, err))
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
		return transaction.Discard(fmt.Errorf("trying to store message %s at state %s in invalid status %s", id, state, currStatus))
	}

	if err := m.store.Store(transaction, id, body, state, nextStatus); err != nil {
		return transaction.Discard(fmt.Errorf("cannot store message at state %s: %v", state, err))
	}

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit marking of next runnable: %v", err)
	}

	if nextStatus == stateStatusReady {
		m.runnables <- StateID{id: id, state: state}
	}

	return nil
}
