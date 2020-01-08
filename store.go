package sagma

import (
	"io"
)

// Transaction handling
type Transaction interface {
	Discard(error) error
	Commit() error
}

type Context map[string]interface{}

func NewContext() Context {
	return Context(make(map[string]interface{}))
}

// Function to be used by handlers to save context
type ContextSaverFn func(Context) error

type Store interface {
	// Returns an opened transaction
	Transaction(id MsgID) (Transaction, error)

	// Store a message within a transaction; must reject duplicate messages for a state
	Store(tx Transaction, id MsgID, body io.Reader, state State, status StateStatus, ctx Context) error
	// Fetch message at a specific state
	// The machine will surround this call with a transaction, thus the body reader
	// returned must be available after the transaction is closed.
	Fetch(tx Transaction, id MsgID, state State, status StateStatus) (io.ReadCloser, Context, error)

	// Persist context up to this point in a handler
	StoreContext(tx Transaction, id MsgID, state State, ctx Context) error

	// Dispose or archive of all messages for this ID at all states
	Dispose(tx Transaction, id MsgID) error

	// Failure handler: should save failure state; returns handling errors
	Fail(tx Transaction, id MsgID, state State, reason error) error

	// Saves transition status for a message
	StoreStateStatus(tx Transaction, id MsgID, state State, currStatus, nextStatus StateStatus) error
	// Get transition state for a message
	FetchStateStatus(tx Transaction, id MsgID, state State) (StateStatus, error)

	// Get all current statuses for each state within one transaction
	FetchStates(tx Transaction, id MsgID, visitor MessageVisitor) error

	// Emits a message-state that could be ran
	PollRunnables(chan<- StateID) error
}

type MessageVisitor interface {
	// Called when a message has succeeded or is in progress at a certain state
	Visit(id MsgID, state State, status StateStatus, ctx Context) error
	// Called when a message has failed at a certain state
	Failed(id MsgID, state State, failed error, ctx Context) error
}

type NotFoundError error
