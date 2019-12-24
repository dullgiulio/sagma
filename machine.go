package main

import (
	"fmt"
	"time"
)

type machine struct {
	msgs  chan *message
	saga  *saga
	store *memstore
}

func newMachine(saga *saga, store *memstore) *machine {
	m := &machine{
		msgs:  make(chan *message),
		saga:  saga,
		store: store,
	}
	return m
}

func (m *machine) receive(msg *message) {
	m.msgs <- msg
}

func (m *machine) stop() {
	close(m.msgs)
}

func (m *machine) runRunnables(sleep time.Duration) {
	for {
		id, state := m.store.fetchRunnable()
		if err := m.transitionRunnable(id, state); err != nil {
			fmt.Printf("ERROR: message %s at state %s: %v\n", id, state, err)
		}
		time.Sleep(sleep)
	}
}

func (m *machine) transitionRunnable(id msgID, state state) error {
	// book runnable for exclusive start
	m.store.openTransaction()
	if err := m.store.setHandlingStarted(id, state); err != nil {
		m.store.discardTransaction()
		return fmt.Errorf("cannot mark handler started: %v", err)
	}
	msg, err := m.store.fetchAtState(id, state)
	if err != nil {
		m.store.discardTransaction()
		return fmt.Errorf("cannot fetch message for handler: %v", err)
	}
	if err := m.store.commitTransaction(); err != nil {
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
	m.store.openTransaction()
	if err := m.store.setHandlingEnded(id, state); err != nil {
		m.store.discardTransaction()
		return fmt.Errorf("cannot mark handler finished: %v", err)
	}
	if err := m.markNextRunnable(msg.id); err != nil {
		m.store.discardTransaction()
		return fmt.Errorf("cannot mark next runnable: %v", err)
	}
	if err := m.store.commitTransaction(); err != nil {
		return fmt.Errorf("cannot commit transaction for end handling: %v", err)
	}

	fmt.Printf("INFO: handler finished for %s at state %s\n", id, state)
	return nil
}

func (m *machine) markNextRunnable(id msgID) error {
	// check completion level
	statuses, err := m.store.fetchStates(id, m.saga)
	if err != nil {
		return fmt.Errorf("ERROR: cannot fetch states for message %s: %v\n", id, err)
	}

	var lastReceivedState state
	for _, status := range statuses {
		if !status.received {
			break
		}
		if !status.handleStarted {
			if err := m.store.markRunnable(id, status.state); err != nil {
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

func (m *machine) run() {
	for msg := range m.msgs {
		m.store.openTransaction()

		if err := m.store.store(msg); err != nil {
			fmt.Printf("ERROR: cannot store message: %v\n", err)
			m.store.discardTransaction()
			continue
		}
		fmt.Printf("INFO: stored message for state %s\n", msg.state)

		if err := m.markNextRunnable(msg.id); err != nil {
			m.store.discardTransaction()
			fmt.Printf("ERROR: cannot mark next runnable: %v\n", err)
		}

		if err := m.store.commitTransaction(); err != nil {
			fmt.Printf("ERROR: cannot commit marking of next runnable: %v\n", err)
		}
	}
}
