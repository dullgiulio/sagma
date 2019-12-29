package main

import (
	"io"
)

type Transaction interface {
	// Transaction handling
	Discard(error) error
	Commit() error
}

type Store interface {
	// Returns an opened transaction
	Transaction(id msgID) Transaction

	// Store a message within a transaction; must reject duplicate messages for a state
	Store(id msgID, body io.Reader, state state, status stateStatus) error
	// Fetch message at a specific state
	Fetch(id msgID, state state, status stateStatus) (io.ReadCloser, error)

	// Dispose or archive of all messages for this ID at all states
	Dispose(id msgID) error

	// Failure handler: should save failure state; returns handling errors
	Fail(id msgID, state state, reason error) error

	// Saves transition status for a message
	StoreStateStatus(id msgID, state state, currStatus, nextStatus stateStatus) error
	// Get transition state for a message
	FetchStateStatus(id msgID, state state) (stateStatus, error)

	// Emits a message-state that could be ran
	PollRunnables(chan<- stateID) error
}
