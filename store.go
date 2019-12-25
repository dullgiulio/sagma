package main

type Transaction interface {
	// Transaction handling
	Discard(error) error
	Commit() error
}

type Store interface {
	// Returns an opened transaction
	Transaction() Transaction

	// Store a message within a transaction; must reject duplicate messages for a state
	Store(msg *message, state state, status stateStatus) error
	// Fetch message at a specific state
	Fetch(id msgID, state state, status stateStatus) (*message, error)

	// Dispose or archive of all messages for this ID at all states
	Dispose(id msgID) error

	// Saves transition status for a message
	StoreStateStatus(id msgID, state state, currStatus, nextStatus stateStatus) error
	// Get transition state for a message
	FetchStateStatus(id msgID, state state) (stateStatus, error)

	// Returns a message-state that could be ran
	FetchRunnable() (msgID, state, error)
}
