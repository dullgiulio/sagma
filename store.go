package main

type Transaction interface {
	// Transaction handling
	Discard(error) error
	Commit() error
}

type store interface {
	// Store a message within a transaction; must reject duplicate messages for a state
	Store(msg *message) error

	// Returns an opened transaction
	Transaction() Transaction

	// Idempotent, if already marked we should not care
	MarkRunnable(id msgID, state state) error

	// Returns a message-state that could be ran
	FetchRunnable() (msgID, state, error)

	// Locks that we handle this message; should fail if message already locked
	LockHandling(id msgID, state state) error
	// Unlock message; if message not locked, nothing to do
	UnlockHandling(id msgID, state state) error

	// Fetch message at a specific state
	FetchAtState(id msgID, state state) (*message, error)

	// Fetch all states (past and future) of a message for the saga in saga order
	// TODO: ordering could be done by the machine
	FetchStates(id msgID, saga *saga) ([]messageStatus, error)
}
