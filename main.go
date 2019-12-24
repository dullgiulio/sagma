package main

import (
	"fmt"
	"sync"
)

type msgID string

type message struct {
	id    msgID
	state state
	body  string
}

type handler func(msg *message) error

type state string

type saga struct {
	states   []state
	handlers map[state]handler
}

func newSaga() *saga {
	return &saga{
		states:   make([]state, 0),
		handlers: make(map[state]handler),
	}
}

func (s *saga) step(state state, handler handler) {
	s.states = append(s.states, state)
	s.handlers[state] = handler
}

type messageStatus struct {
	state          state
	received       bool
	handleStarted  bool
	handleFinished bool
}

type memstore struct {
	messagesByID map[msgID]map[state]*message
	statusByID   map[msgID]map[state]*messageStatus
}

func newMemstore() *memstore {
	return &memstore{
		messagesByID: make(map[msgID]map[state]*message),
		statusByID:   make(map[msgID]map[state]*messageStatus),
	}
}

func (m *memstore) store(msg *message) error {
	func() {
		states, ok := m.messagesByID[msg.id]
		if !ok {
			states = make(map[state]*message)
		}
		states[msg.state] = msg
		m.messagesByID[msg.id] = states
	}()
	func() {
		states, ok := m.statusByID[msg.id]
		if !ok {
			states = make(map[state]*messageStatus)
		}
		states[msg.state] = &messageStatus{
			state:    msg.state,
			received: true,
		}
		m.statusByID[msg.id] = states
	}()
	return nil
}

func (m *memstore) setHandlingStarted(id msgID, state state) error {
	states, ok := m.statusByID[id]
	if !ok {
		return fmt.Errorf("message %s not in status store", id)
	}
	if !states[state].received {
		return fmt.Errorf("message %s not marked as received", id)
	}
	if states[state].handleStarted {
		return fmt.Errorf("message %s already marked as handler started", id)
	}
	states[state].handleStarted = true
	return nil
}

func (m *memstore) setHandlingEnded(id msgID, state state) error {
	states, ok := m.statusByID[id]
	if !ok {
		return fmt.Errorf("message %s not in status store", id)
	}
	status := states[state]
	if !status.received {
		return fmt.Errorf("message %s not marked as received", id)
	}
	if !status.handleStarted {
		return fmt.Errorf("message %s not marked as handler started", id)
	}
	if status.handleFinished {
		return fmt.Errorf("message %s already marked as handler finished", id)
	}
	states[state].handleFinished = true
	return nil
}

func (m *memstore) fetchAtState(id msgID, state state) (*message, error) {
	states, ok := m.messagesByID[id]
	if !ok {
		return nil, fmt.Errorf("message %s not in store", id)
	}
	msg, ok := states[state]
	if !ok {
		return nil, fmt.Errorf("state %s not completed for message %d", state, id)
	}
	return msg, nil
}

func (m *memstore) fetchStates(id msgID, saga *saga) ([]messageStatus, error) {
	statuses, ok := m.statusByID[id]
	if !ok {
		return nil, fmt.Errorf("unknown message %s", id)
	}
	msgStatuses := make([]messageStatus, len(saga.states))
	for i, state := range saga.states {
		status, ok := statuses[state]
		if !ok {
			msgStatuses[i] = messageStatus{}
			continue
		}
		msgStatuses[i] = *status
	}
	return msgStatuses, nil
}

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

func (m *machine) run() {
	for msg := range m.msgs {
		if err := m.store.store(msg); err != nil {
			fmt.Printf("ERROR: cannot store message: %v\n", err)
			continue
		}
		fmt.Printf("INFO: stored message for state %s\n", msg.state)

		// check completion level
		statuses, err := m.store.fetchStates(msg.id, m.saga)
		if err != nil {
			fmt.Printf("ERROR: cannot fetch states for message %s: %v\n", msg.id, err)
			// TODO: what to do here?
			continue
		}

		var lastReceivedState state
		for _, status := range statuses {
			if !status.received {
				break
			}
			if !status.handleStarted {
				err := func(id msgID, state state) error {
					if err := m.store.setHandlingStarted(id, state); err != nil {
						return fmt.Errorf("cannot mark handler started: %v", err)
					}
					msg, err := m.store.fetchAtState(id, state)
					if err != nil {
						return fmt.Errorf("cannot fetch message for handler: %v", err)
					}
					// TODO: start in background but in order!
					fmt.Printf("INFO: handler started for %s at state %s\n", id, state)
					handler, ok := m.saga.handlers[state]
					if ok {
						if err := handler(msg); err != nil {
							return fmt.Errorf("handler returned error: %v", err)
						}
					}
					if err := m.store.setHandlingEnded(id, state); err != nil {
						return fmt.Errorf("cannot mark handler finished: %v", err)
					}
					fmt.Printf("INFO: handler finished for %s at state %s\n", id, state)
					return nil
				}(msg.id, status.state)
				if err != nil {
					fmt.Printf("ERROR: message %s at state %s: %v\n", msg.id, status.state, err)
				}
			}
			lastReceivedState = status.state
		}

		fmt.Printf("INFO: completed state is %s\n", lastReceivedState)
	}
}

func main() {
	saga := newSaga()
	stateFirst := state("firstState")
	saga.step(stateFirst, func(*message) error {
		fmt.Printf("handling first state completed\n")
		return nil
	})
	stateSecond := state("secondState")
	saga.step(stateSecond, func(*message) error {
		fmt.Printf("handling second state completed\n")
		return nil
	})
	stateThird := state("thirdState")
	saga.step(stateThird, func(*message) error {
		fmt.Printf("handling third state completed\n")
		return nil
	})
	machine := newMachine(saga, newMemstore())
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		machine.receive(&message{
			id:    "test",
			state: stateSecond,
			body:  "",
		})
		wg.Done()
	}()
	go func() {
		machine.receive(&message{
			id:    "test",
			state: stateFirst,
			body:  "",
		})
		wg.Done()
	}()
	go func() {
		machine.receive(&message{
			id:    "test",
			state: stateThird,
			body:  "",
		})
		wg.Done()
	}()
	go func() {
		wg.Wait()
		machine.stop()
	}()
	machine.run()
}
