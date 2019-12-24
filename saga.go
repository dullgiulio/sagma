package main

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
	runnable       bool
	handleStarted  bool
	handleFinished bool
}
