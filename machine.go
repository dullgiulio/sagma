package main

import (
	"fmt"
	"io"
)

type machine struct {
	log       *Loggers
	saga      *saga
	store     Store
	runnables chan stateID
	shutdown  chan struct{}
	finished  chan struct{}
}

func newMachine(saga *saga, store Store, log *Loggers, runbuf int) *machine {
	m := &machine{
		saga:      saga,
		store:     store,
		log:       log,
		runnables: make(chan stateID, runbuf),
		shutdown:  make(chan struct{}),
		finished:  make(chan struct{}),
	}
	return m
}

func (m *machine) runRunnable(id msgID, state state) error {
	// run all ready states for this ID directly if possible
	for {
		next, err := m.transitionRunnable(id, state)
		if err != nil {
			return fmt.Errorf("cannot trasition message %s at state %s: %v", id, state, err)
		}
		if next.id == "" {
			return nil
		}
		state = next.state
	}
}

func (m *machine) Run() {
	go func() {
		if err := m.store.PollRunnables(m.runnables); err != nil {
			m.log.err.Printf("store scanning failed: %v", err)
		}
	}()
	for {
		select {
		case runnable := <-m.runnables:
			if err := m.runRunnable(runnable.id, runnable.state); err != nil {
				m.log.err.Printf("runnable: %v\n", err)
			}
		case <-m.shutdown:
			close(m.finished)
			return
		}
	}
}

func (m *machine) Shutdown() {
	close(m.shutdown)
	<-m.finished
}

func (m *machine) dispose(id msgID) error {
	transaction := m.store.Transaction(id)
	if err := m.store.Dispose(id); err != nil {
		return transaction.Discard(fmt.Errorf("store disposal failed: %v", err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction for end handling: %v", err)
	}
	return nil
}

func (m *machine) transitionRunnable(id msgID, state state) (stateID, error) {
	var nextRunnable stateID

	transaction := m.store.Transaction(id)
	// book runnable for exclusive start
	if err := m.store.StoreStateStatus(id, state, stateStatusReady, stateStatusRunning); err != nil {
		return nextRunnable, transaction.Discard(fmt.Errorf("cannot mark handler started: %v", err))
	}
	body, err := m.store.Fetch(id, state, stateStatusRunning)
	if err != nil {
		return nextRunnable, transaction.Discard(fmt.Errorf("cannot fetch message for handler: %v", err))
	}
	// TODO: catch closer error
	defer body.Close()
	if err := transaction.Commit(); err != nil {
		return nextRunnable, fmt.Errorf("cannot commit transaction for start handling: %v", err)
	}

	handler, ok := m.saga.handlers[state]
	if !ok {
		return nextRunnable, fmt.Errorf("state %s does not have a handler, execution finished", state)
	}

	nextState, err := handler(id, body)
	if err != nil {
		return nextRunnable, fmt.Errorf("handler returned error: %v", err)
	}

	commit := func() error {
		transaction = m.store.Transaction(id)
		if err := m.store.StoreStateStatus(id, state, stateStatusRunning, stateStatusDone); err != nil {
			return transaction.Discard(fmt.Errorf("cannot mark handler finished: %v", err))
		}
		var (
			nextStateStatus stateStatus
			err             error
		)
		if nextState != SagaEnd {
			if nextStateStatus, err = m.computeNextStateStatus(id, nextState); err != nil {
				return transaction.Discard(fmt.Errorf("cannot mark next runnable: %v", err))
			}
		}
		if err = transaction.Commit(); err != nil {
			return fmt.Errorf("cannot commit transaction for end handling: %v", err)
		}

		if nextStateStatus == stateStatusReady {
			nextRunnable.id = id
			nextRunnable.state = nextState
		}

		return nil
	}

	// errors while committing should be retried, we know the handler ran
	for retries := 0; retries < 10; retries++ {
		err = commit()
		if err == nil {
			break
		}
		m.log.err.Printf("retying after error: %v\n", err)
	}
	if err != nil {
		return nextRunnable, fmt.Errorf("commit retry: %v", err)
	}

	if nextState == SagaEnd {
		if err := m.dispose(id); err != nil {
			return nextRunnable, fmt.Errorf("cannot dispose of completed message saga for message %s: %v", id, err)
		}
	}

	return nextRunnable, nil
}

func (m *machine) computeNextStateStatus(id msgID, nextState state) (stateStatus, error) {
	var nextStateStatus stateStatus
	currStateStatus, err := m.store.FetchStateStatus(id, nextState)
	if err != nil {
		return nextStateStatus, fmt.Errorf("cannot fetch state status: %v", nextState, err)
	}
	switch currStateStatus {
	case stateStatusWaiting:
		nextStateStatus = stateStatusReadyWaiting
	case stateStatusRecvWaiting:
		nextStateStatus = stateStatusReady
	default:
		return nextStateStatus, fmt.Errorf("message %s is in unexpected state %s status %s", id, nextState, currStateStatus)
	}

	if err = m.store.StoreStateStatus(id, nextState, currStateStatus, nextStateStatus); err != nil {
		return nextStateStatus, fmt.Errorf("could not set state %s status %s: %v", nextState, nextStateStatus, err)
	}
	return nextStateStatus, nil
}

func (m *machine) Fetch(id msgID, state state) (io.ReadCloser, error) {
	body, err := m.store.Fetch(id, state, stateStatusDone)
	if err != nil {
		return nil, fmt.Errorf("cannot fetch message: %v", err)
	}
	return body, nil
}

func (m *machine) Receive(id msgID, body io.ReadCloser, state state) error {
	err := m.receive(id, body, state)
	if err1 := body.Close(); err == nil {
		err = err1
	}
	return err
}

func (m *machine) receive(id msgID, body io.ReadCloser, state state) error {
	transaction := m.store.Transaction(id)

	currStatus, err := m.store.FetchStateStatus(id, state)
	if err != nil {
		return transaction.Discard(fmt.Errorf("cannot fetch status of state %s for %s: %v", state, id, err))
	}

	var nextStatus stateStatus
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

	if err := m.store.Store(id, body, state, nextStatus); err != nil {
		return transaction.Discard(fmt.Errorf("cannot store message at state %s: %v", state, err))
	}

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit marking of next runnable: %v", err)
	}

	if nextStatus == stateStatusReady {
		m.runnables <- stateID{id: id, state: state}
	}

	return nil
}
