package main

import (
	"fmt"
	"time"
)

type machine struct {
	saga  *saga
	store store
}

func newMachine(saga *saga, store store) *machine {
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

func (m *machine) transitionRunnable(id msgID, state state) error {
	transaction := m.store.Transaction()
	// book runnable for exclusive start
	if err := m.store.LockHandling(id, state); err != nil {
		return transaction.Discard(fmt.Errorf("cannot mark handler started: %v", err))
	}
	msg, err := m.store.FetchAtState(id, state)
	if err != nil {
		return transaction.Discard(fmt.Errorf("cannot fetch message for handler: %v", err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction for start handling: %v", err)
	}

	fmt.Printf("INFO: handler started for %s at state %s\n", id, state)
	handler, ok := m.saga.handlers[state]
	if ok {
		if err := handler(msg); err != nil {
			return fmt.Errorf("handler returned error: %v", err)
		}
	}

	// TODO: errors in this block should be retried, we know the handler ran
	transaction = m.store.Transaction()
	if err := m.store.UnlockHandling(id, state); err != nil {
		return transaction.Discard(fmt.Errorf("cannot mark handler finished: %v", err))
	}
	if err := m.markNextRunnable(msg.id); err != nil {
		return transaction.Discard(fmt.Errorf("cannot mark next runnable: %v", err))
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction for end handling: %v", err)
	}

	fmt.Printf("INFO: handler finished for %s at state %s\n", id, state)

	return nil
}

func (m *machine) fetchSortedStates(id msgID) ([]messageStatus, error) {
	// check completion level
	statuses, err := m.store.FetchStates(id, m.saga)
	if err != nil {
		return nil, fmt.Errorf("cannot fetch states for message %s: %v\n", id, err)
	}

	// sort states
	msgStatuses := make([]messageStatus, len(m.saga.states))
	for i, state := range m.saga.states {
		status, ok := statuses[state]
		if !ok {
			msgStatuses[i] = messageStatus{}
			continue
		}
		msgStatuses[i] = status
	}

	return msgStatuses, nil
}

func (m *machine) markNextRunnable(id msgID) error {
	statuses, err := m.fetchSortedStates(id)
	if err != nil {
		return err
	}

	var lastReceivedState state
	for _, status := range statuses {
		if !status.received {
			break
		}
		if !status.handleStarted {
			if err := m.store.MarkRunnable(id, status.state); err != nil {
				return fmt.Errorf("cannot store runnable mark: %v", err)
			}
			lastReceivedState = status.state
		}
	}

	if lastReceivedState == "" {
		fmt.Printf("INFO: no runnable state yet\n")
	} else {
		fmt.Printf("INFO: state %s is marked runnable next\n", lastReceivedState)
	}

	return nil
}

func (m *machine) Receive(msg *message) error {
	transaction := m.store.Transaction()

	if err := m.store.Store(msg); err != nil {
		return transaction.Discard(fmt.Errorf("cannot store message: %v", err))
	}

	fmt.Printf("INFO: stored message for state %s\n", msg.state)

	if err := m.markNextRunnable(msg.id); err != nil {
		return transaction.Discard(fmt.Errorf("cannot mark next message as runnable: %v", err))
	}

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("cannot commit marking of next runnable: %v", err)
	}
	return nil
}
