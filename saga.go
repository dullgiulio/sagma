package main

type msgID string

type message struct {
	id   msgID
	body []byte // TODO: turn into ReadCloser
}

type state string

type handler func(msg *message) (nextState state, err error)

const sagaEnd = state("__end")

type saga struct {
	initial  state
	handlers map[state]handler
}

func newSaga() *saga {
	return &saga{
		handlers: make(map[state]handler),
	}
}

func (s *saga) begin(state state, handler handler) {
	s.initial = state
	s.step(state, handler)
}

func (s *saga) step(state state, handler handler) {
	s.handlers[state] = handler
}

type stateStatus string

const (
	stateStatusWaiting      stateStatus = ""              // no message yet
	stateStatusRecvWaiting  stateStatus = "recv-waiting"  // message but not runnable
	stateStatusReadyWaiting stateStatus = "ready-waiting" // runnable but no message
	stateStatusReady        stateStatus = "ready"         // both runnable and with message
	stateStatusRunning      stateStatus = "running"       // handler running
	stateStatusDone         stateStatus = "done"          // handler completed
)

var stateStatuses []stateStatus = []stateStatus{
	stateStatusRecvWaiting,
	stateStatusReadyWaiting,
	stateStatusReady,
	stateStatusRunning,
	stateStatusDone,
}
