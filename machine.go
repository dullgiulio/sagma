package main

import (
	"fmt"
)

type machine struct {
	saga      *saga
	store     Store
	runnables chan stateID
}

func newMachine(saga *saga, store Store) *machine {
	m := &machine{
		saga:      saga,
		store:     store,
		runnables: make(chan stateID), // TODO: can buffer
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
	storeDone := make(chan struct{})
	go func() {
		if err := m.store.PollRunnables(m.runnables); err != nil {
			fmt.Printf("ERROR: store scanning failed: %v", err)
		}
		close(storeDone)
	}()
	for runnable := range m.runnables {
		if err := m.runRunnable(runnable.id, runnable.state); err != nil {
			fmt.Printf("ERROR: runnable: %v\n", err)
		}
		fmt.Printf("INFO: runnable finished, polling\n")
	}
	<-storeDone
}

func (m *machine) dispose(id msgID) error {
	transaction := m.store.Transaction(id)
	if err := m.store.Dispose(id); err != nil {
		return transaction.Discard(fmt.Errorf("store disposal failed: %v", err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction for end handling: %v", err)
	}

	fmt.Printf("INFO: disposing of %s\n", id)

	return nil
}

func (m *machine) transitionRunnable(id msgID, state state) (stateID, error) {
	var nextRunnable stateID

	transaction := m.store.Transaction(id)
	// book runnable for exclusive start
	if err := m.store.StoreStateStatus(id, state, stateStatusReady, stateStatusRunning); err != nil {
		return nextRunnable, transaction.Discard(fmt.Errorf("cannot mark handler started: %v", err))
	}
	msg, err := m.store.Fetch(id, state, stateStatusRunning)
	if err != nil {
		return nextRunnable, transaction.Discard(fmt.Errorf("cannot fetch message for handler: %v", err))
	}
	if err := transaction.Commit(); err != nil {
		return nextRunnable, fmt.Errorf("cannot commit transaction for start handling: %v", err)
	}

	fmt.Printf("INFO: handler started for %s at state %s\n", id, state)

	handler, ok := m.saga.handlers[state]
	if !ok {
		return nextRunnable, fmt.Errorf("state %s does not have a handler, execution finished", state)
	}

	nextState, err := handler(msg)
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
		if nextState != sagaEnd {
			if nextStateStatus, err = m.computeNextStateStatus(msg.id, nextState); err != nil {
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
		fmt.Printf("ERROR: retying after error: %v\n", err)
	}
	if err != nil {
		return nextRunnable, fmt.Errorf("commit retry: %v", err)
	}

	fmt.Printf("INFO: handler finished for %s at state %s\n", id, state)

	if nextState == sagaEnd {
		if err := m.dispose(msg.id); err != nil {
			return nextRunnable, fmt.Errorf("cannot dispose of completed message saga for message %s: %v", msg.id, err)
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
		fmt.Printf("INFO: state %s is runnable without message\n", nextState)
		nextStateStatus = stateStatusReadyWaiting
	case stateStatusRecvWaiting:
		fmt.Printf("INFO: state %s is runnable\n", nextState)
		nextStateStatus = stateStatusReady
	default:
		return nextStateStatus, fmt.Errorf("message %s is in unexpected state %s status %s", id, nextState, currStateStatus)
	}

	fmt.Printf("INFO: storing status %s for %s after handler\n", nextStateStatus, id)

	if err = m.store.StoreStateStatus(id, nextState, currStateStatus, nextStateStatus); err != nil {
		return nextStateStatus, fmt.Errorf("could not set state %s status %s: %v", nextState, nextStateStatus, err)
	}
	return nextStateStatus, nil
}

func (m *machine) Receive(msg *message, state state) error {
	transaction := m.store.Transaction(msg.id)

	fmt.Printf("INFO: receive got transaction for %s\n", msg.id)

	currStatus, err := m.store.FetchStateStatus(msg.id, state)
	if err != nil {
		return transaction.Discard(fmt.Errorf("cannot fetch status of state %s for %s: %v", state, msg.id, err))
	}

	fmt.Printf("INFO: current status of received message is %s\n", currStatus)

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
		return transaction.Discard(fmt.Errorf("trying to store message %s at state %s in invalid status %s", msg.id, state, currStatus))
	}

	if err := m.store.Store(msg, state, nextStatus); err != nil {
		return transaction.Discard(fmt.Errorf("cannot store message at state %s: %v", state, err))
	}

	fmt.Printf("INFO: stored message for state %s, next status is %s\n", state, nextStatus)

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit marking of next runnable: %v", err)
	}

	if nextStatus == stateStatusReady {
		fmt.Printf("INFO: pushing next runnable\n")
		m.runnables <- stateID{id: msg.id, state: state}
		fmt.Printf("INFO: pushed next runnable\n")
	}

	return nil
}
