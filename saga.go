package sagma

import (
	"io"
)

type MsgID string

type message struct {
	id   MsgID
	body io.ReadCloser
}

type State string

func (s State) IsEnd() bool {
	return s == ""
}

type StateID struct {
	id    MsgID
	state State
}

type Handler func(id MsgID, body io.Reader) (nextStates SagaStates, err error)

type SagaStates []State

var SagaEnd SagaStates

func SagaNext(states ...State) SagaStates {
	return SagaStates(append(make([]State, 0, len(states)), states...))
}

type Saga struct {
	initial  State
	handlers map[State]Handler
}

func NewSaga() *Saga {
	return &Saga{
		handlers: make(map[State]Handler),
	}
}

func (s *Saga) Begin(state State, handler Handler) {
	s.initial = state
	s.Step(state, handler)
}

func (s *Saga) Step(state State, handler Handler) {
	s.handlers[state] = handler
}

type StateStatus string

const (
	stateStatusWaiting      StateStatus = ""              // no message yet
	stateStatusRecvWaiting  StateStatus = "recv-waiting"  // message but not runnable
	stateStatusReadyWaiting StateStatus = "ready-waiting" // runnable but no message
	stateStatusReady        StateStatus = "ready"         // both runnable and with message
	stateStatusRunning      StateStatus = "running"       // handler running
	stateStatusDone         StateStatus = "done"          // handler completed successfully
	stateStatusError        StateStatus = "error"         // handler completed but failed
)

func (s StateStatus) IsValid() bool {
	switch s {
	case stateStatusWaiting, stateStatusRecvWaiting, stateStatusReadyWaiting,
		stateStatusReady, stateStatusRunning, stateStatusDone, stateStatusError:
		return true
	default:
		return false
	}
}

var stateStatuses []StateStatus = []StateStatus{
	stateStatusRecvWaiting,
	stateStatusReadyWaiting,
	stateStatusReady,
	stateStatusRunning,
	stateStatusDone,
}
