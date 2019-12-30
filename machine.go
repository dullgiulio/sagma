package main

import (
	"fmt"
	"io"
	"sync"
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
	nextRunnables, err := m.transitionRunnable(id, state)
	if err != nil {
		return fmt.Errorf("cannot trasition message %s at state %s: %v", id, state, err)
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

func (m *machine) runMachine(wg *sync.WaitGroup) {
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

func (m *machine) Run(n int) {
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

func (m *machine) commitFailure(id msgID, currState state, reason error) error {
	transaction := m.store.Transaction(id)
	if err := m.store.Fail(id, currState, reason); err != nil {
		return transaction.Discard(fmt.Errorf("cannot store failure state: %v", err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit failure transaction: %v", err)
	}
	return nil
}

func (m *machine) retry(n int, fn func() error) error {
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

func (m *machine) transitionRunnable(id msgID, state state) ([]stateID, error) {
	var nextRunnables []stateID

	transaction := m.store.Transaction(id)
	// book runnable for exclusive start
	if err := m.store.StoreStateStatus(id, state, stateStatusReady, stateStatusRunning); err != nil {
		return nil, transaction.Discard(fmt.Errorf("cannot mark handler started: %v", err))
	}
	body, err := m.store.Fetch(id, state, stateStatusRunning)
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
		transaction := m.store.Transaction(id)
		if err := m.store.StoreStateStatus(id, state, stateStatusRunning, stateStatusDone); err != nil {
			return transaction.Discard(fmt.Errorf("cannot mark handler finished: %v", err))
		}
		for _, nextState := range nextStates {
			if nextState.IsEnd() {
				continue
			}

			nextStateStatus, err := m.computeNextStateStatus(id, nextState)
			if err != nil {
				return transaction.Discard(fmt.Errorf("cannot mark next runnable: %v", err))
			}
			// if succeeded, mark next state as ready to run if it has a message already
			if nextStateStatus == stateStatusReady {
				nextRunnable := stateID{
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

func (m *machine) computeNextStateStatus(id msgID, nextState state) (stateStatus, error) {
	var nextStateStatus stateStatus
	currStateStatus, err := m.store.FetchStateStatus(id, nextState)
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
