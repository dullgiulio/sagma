package sagma

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
	Transaction(id MsgID) Transaction

	// Store a message within a transaction; must reject duplicate messages for a state
	Store(id MsgID, body io.Reader, state State, status StateStatus) error
	// Fetch message at a specific state
	// The machine will surround this call with a transaction, thus the body reader
	// returned must be available after the transaction is closed.
	Fetch(id MsgID, state State, status StateStatus) (io.ReadCloser, error)

	// Dispose or archive of all messages for this ID at all states
	Dispose(id MsgID) error

	// Failure handler: should save failure state; returns handling errors
	Fail(id MsgID, state State, reason error) error

	// Saves transition status for a message
	StoreStateStatus(id MsgID, state State, currStatus, nextStatus StateStatus) error
	// Get transition state for a message
	FetchStateStatus(id MsgID, state State) (StateStatus, error)

	// Get all current statuses for each state within one transaction
	// TODO: error states etc
	FetchStates(id MsgID, visitor MessageVisitor) error

	// Emits a message-state that could be ran
	PollRunnables(chan<- StateID) error
}

type MessageVisitor interface {
	// Called when a message has succeeded or is in progress at a certain state
	Visit(id MsgID, state State, status StateStatus) error
	// Called when a message has failed at a certain state
	Failed(id MsgID, state State, failed error) error
}

type NotFoundError error
