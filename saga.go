package main

import (
	"bytes"
	"io"
	"io/ioutil"
)

func stringReadCloser(s string) io.ReadCloser {
	return ioutil.NopCloser(bytes.NewReader([]byte(s)))
}

type msgID string

type message struct {
	id   msgID
	body io.ReadCloser
}

type state string

func (s state) IsEnd() bool {
	return s == ""
}

type stateID struct {
	id    msgID
	state state
}

type handler func(id msgID, body io.Reader) (nextStates sagaStates, err error)

type sagaStates []state

var SagaEnd sagaStates

func sagaNext(states ...state) sagaStates {
	return sagaStates(append(make([]state, 0, len(states)), states...))
}

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
	stateStatusDone         stateStatus = "done"          // handler completed successfully
	stateStatusError        stateStatus = "error"         // handler completed but failed
)

var stateStatuses []stateStatus = []stateStatus{
	stateStatusRecvWaiting,
	stateStatusReadyWaiting,
	stateStatusReady,
	stateStatusRunning,
	stateStatusDone,
}
