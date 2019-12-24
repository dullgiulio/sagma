package main

type store interface {
	// Store a message within a transaction; must reject duplicate messages for a state
	Store(msg *message) error

	// Transaction handling
	OpenTransaction()
	DiscardTransaction()
	CommitTransaction() error

	// Idempotent, if already marked we should not care
	MarkRunnable(id msgID, state state) error

	// Returns a message-state that could be ran
	FetchRunnable() (msgID, state)

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
