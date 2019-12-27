package main

import (
	"fmt"
	"time"
)

type machine struct {
	saga  *saga
	store Store
}

func newMachine(saga *saga, store Store) *machine {
	m := &machine{
		saga:  saga,
		store: store,
	}
	return m
}

func (m *machine) runRunnable() error {
	id, state, err := m.store.FetchRunnable()
	if err != nil {
		return fmt.Errorf("could not fetch runnable message: %v", err)
	}
	if id == "" {
		return nil // no work available
	}
	if err := m.transitionRunnable(id, state); err != nil {
		return fmt.Errorf("cannot trasition message %s at state %s: %v", id, state, err)
	}
	return nil
}

func (m *machine) RunRunnables(sleep time.Duration) {
	for {
		if err := m.runRunnable(); err != nil {
			fmt.Printf("ERROR: runnable: %v\n", err)
		}
		time.Sleep(sleep)
	}
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

func (m *machine) transitionRunnable(id msgID, state state) error {
	transaction := m.store.Transaction(id)
	// book runnable for exclusive start
	if err := m.store.StoreStateStatus(id, state, stateStatusReady, stateStatusRunning); err != nil {
		return transaction.Discard(fmt.Errorf("cannot mark handler started: %v", err))
	}
	msg, err := m.store.Fetch(id, state, stateStatusRunning)
	if err != nil {
		return transaction.Discard(fmt.Errorf("cannot fetch message for handler: %v", err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction for start handling: %v", err)
	}

	fmt.Printf("INFO: handler started for %s at state %s\n", id, state)

	handler, ok := m.saga.handlers[state]
	if !ok {
		return fmt.Errorf("state %s does not have a handler, execution finished", state)
	}

	nextState, err := handler(msg)
	if err != nil {
		return fmt.Errorf("handler returned error: %v", err)
	}

	// TODO: errors in this block should be retried, we know the handler ran
	transaction = m.store.Transaction(id)
	if err := m.store.StoreStateStatus(id, state, stateStatusRunning, stateStatusDone); err != nil {
		return transaction.Discard(fmt.Errorf("cannot mark handler finished: %v", err))
	}
	if nextState != sagaEnd {
		if err := m.markNextRunnable(msg.id, nextState); err != nil {
			return transaction.Discard(fmt.Errorf("cannot mark next runnable: %v", err))
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction for end handling: %v", err)
	}

	fmt.Printf("INFO: handler finished for %s at state %s\n", id, state)

	if nextState == sagaEnd {
		if err := m.dispose(msg.id); err != nil {
			return fmt.Errorf("cannot dispose of completed message saga for message %s: %v", msg.id, err)
		}
	}

	return nil
}

func (m *machine) markNextRunnable(id msgID, nextState state) error {
	currStateStatus, err := m.store.FetchStateStatus(id, nextState)
	if err != nil {
		return fmt.Errorf("cannot fetch state status: %v", nextState, err)
	}
	var nextStateStatus stateStatus
	switch currStateStatus {
	case stateStatusWaiting:
		fmt.Printf("INFO: state %s is runnable without message\n", nextState)
		nextStateStatus = stateStatusReadyWaiting
	case stateStatusRecvWaiting:
		fmt.Printf("INFO: state %s is runnable\n", nextState)
		nextStateStatus = stateStatusReady
	default:
		return fmt.Errorf("message %s is in unexpected state %s status %s", id, nextState, currStateStatus)
	}
	if err := m.store.StoreStateStatus(id, nextState, currStateStatus, nextStateStatus); err != nil {
		return fmt.Errorf("could not set state %s status %s: %v", nextState, nextStateStatus, err)
	}
	return nil
}

func (m *machine) Receive(msg *message, state state) error {
	transaction := m.store.Transaction(msg.id)

	currStatus, err := m.store.FetchStateStatus(msg.id, state)
	if err != nil {
		return transaction.Discard(fmt.Errorf("cannot fetch status of state %s for %s: %v", state, msg.id, err))
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
		return transaction.Discard(fmt.Errorf("trying to store message %s at state %s in invalid status %s", msg.id, state, currStatus))
	}

	if err := m.store.Store(msg, state, nextStatus); err != nil {
		return transaction.Discard(fmt.Errorf("cannot store message at state %s: %v", state, err))
	}

	fmt.Printf("INFO: stored message for state %s\n", state)

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit marking of next runnable: %v", err)
	}
	return nil
}
