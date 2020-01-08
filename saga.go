package sagma

import (
	"io"
	"time"
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

type Handler func(id MsgID, ctx Context, body io.Reader, saveCtx ContextSaverFn) (nextStates *SagaStates, err error)

type SagaStates struct {
	states   []State
	finished bool
}

var SagaEnd *SagaStates = &SagaStates{states: nil, finished: true}

func SagaNext(states ...State) *SagaStates {
	return &SagaStates{
		states:   append(make([]State, 0, len(states)), states...),
		finished: false,
	}
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

type Timeouts struct {
	RunnableLeftBehind time.Duration // after how long to pickup runnables that were not excuted yet
}

func NewTimeouts() *Timeouts {
	return &Timeouts{
		RunnableLeftBehind: 60 * time.Second,
	}
}

const (
	stateStatusWaiting      StateStatus = ""              // no message yet
	stateStatusRecvWaiting  StateStatus = "recv-waiting"  // message but not runnable
	stateStatusReadyWaiting StateStatus = "ready-waiting" // runnable but no message
	stateStatusReady        StateStatus = "ready"         // both runnable and with message
	stateStatusRunning      StateStatus = "running"       // handler running
	stateStatusDone         StateStatus = "done"          // handler completed successfully
	stateStatusError        StateStatus = "error"         // handler completed but failed
	stateStatusArchived     StateStatus = "archived"      // all handlers have been completed
)

func (s StateStatus) IsValid() bool {
	switch s {
	case stateStatusWaiting, stateStatusRecvWaiting, stateStatusReadyWaiting,
		stateStatusReady, stateStatusRunning, stateStatusDone, stateStatusError, stateStatusArchived:
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
