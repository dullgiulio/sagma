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

type stateID struct {
	id    msgID
	state state
}

type handler func(id msgID, body io.Reader) (nextState state, err error)

const SagaEnd = state("__end")

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
