package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

var statusList = []stateStatus{
	stateStatusRecvWaiting,
	stateStatusReadyWaiting,
	stateStatusReady,
	stateStatusRunning,
	stateStatusDone,
}

type stateID struct {
	id    msgID
	state state
}

type msgFolder string

func (f msgFolder) contentsFile(filename string) string {
	return filepath.Join(string(f), filename)
}

type fileprefix string

func (f fileprefix) messageFolder(id msgID, state state, status stateStatus) msgFolder {
	msgid := string(id) // TODO: hash and make it like a0/a0484ff4859abc77
	return msgFolder(filepath.Join(string(f), string(status), string(state), msgid))
}

func (f fileprefix) stateFolder(state state, status stateStatus) stateFolder {
	return stateFolder(filepath.Join(string(f), string(status), string(state)))
}

func (f fileprefix) createAllFolders(statuses []stateStatus, states []state) error {
	for _, status := range statuses {
		for _, state := range states {
			if err := os.MkdirAll(string(f.stateFolder(state, status)), 0744); err != nil {
				return fmt.Errorf("cannot create folder for state %s at status %s: %v", state, status, err)
			}
		}
	}
	return nil
}

type stateFolder string

func (f stateFolder) emitMessages(state state, wakeup <-chan struct{}, ids chan<- stateID, batchSize int) error {
	var (
		err error
		n   int
	)
	emptyStateID := stateID{}
	batch := make([]msgID, batchSize)
	for range wakeup {
		fmt.Printf("DEBUG: received wakeup for %s\n", state)

		n, err = f.scan(batch)
		if err != nil {
			return fmt.Errorf("cannot scan folder %s: %v", f, err)
		}

		if n == 0 {
			ids <- emptyStateID
			continue
		}

		fmt.Printf("INFO: scanning finished, %d elements found in %s\n", n, state)

		for _, id := range batch[:n] {
			ids <- stateID{state: state, id: id}
		}
	}
	return nil
}

func (f stateFolder) scan(ids []msgID) (int, error) {
	// XXX: this will have to change if using two levels for ID in path
	dh, err := os.Open(string(f))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("cannot read runnables folder: %v", err)
	}
	defer dh.Close()

	fmt.Printf("INFO: scanning %s\n", f)

	names, err := dh.Readdirnames(len(ids))
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("cannot read batch of runnable folder names: %v", err)
	}

	fmt.Printf("INFO: scan returned %d elements\n", len(names))

	for i, name := range names {
		ids[i] = msgID(name)
	}
	return len(names), nil
}

type filestore struct {
	lockmap         *msgLockMap
	prefix          fileprefix
	contentFilename string
	ids             chan stateID
	wakeup          chan struct{}
}

func newFilestore(prefix string, states []state) (*filestore, error) {
	fprefix := fileprefix(prefix)
	if err := fprefix.createAllFolders(stateStatuses, states); err != nil {
		return nil, fmt.Errorf("cannot initialize file store folders: %v", err)
	}

	ids := make(chan stateID)
	wakeup := make(chan struct{})

	for _, st := range states {
		folder := fprefix.stateFolder(st, stateStatusReady)
		go func(st state, folder stateFolder) {
			if err := folder.emitMessages(st, wakeup, ids, 100); err != nil {
				fmt.Printf("ERROR: cannot scan runnable for state %s: %v\n", st, err)
			}
			// TODO: sleep and retry instead of giving up
		}(st, folder)
	}

	return &filestore{
		ids:             ids,
		prefix:          fprefix,
		lockmap:         newMsgLockMap(),
		wakeup:          wakeup,
		contentFilename: "contents", // TODO: contents.gz is compression
	}, nil
}

func (f *filestore) Store(msg *message, st state, status stateStatus) error {
	folder := f.prefix.messageFolder(msg.id, st, status)
	if err := os.MkdirAll(string(folder), 0744); err != nil {
		return fmt.Errorf("cannot make message folder: %v", err)
	}
	// TODO: compression
	if err := ioutil.WriteFile(folder.contentsFile(f.contentFilename), msg.body, 0644); err != nil {
		return fmt.Errorf("cannot write contents file: %v", err)
	}
	return nil
}

func (f *filestore) Fetch(id msgID, state state, status stateStatus) (*message, error) {
	folder := f.prefix.messageFolder(id, state, status)
	file := folder.contentsFile(f.contentFilename)
	// TODO: decompression
	body, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("cannot read message contents: %v", err)
	}
	return &message{id: id, body: body}, nil
}

func (f *filestore) StoreStateStatus(id msgID, st state, currStatus, nextStatus stateStatus) error {
	from := f.prefix.messageFolder(id, st, currStatus)
	to := f.prefix.messageFolder(id, st, nextStatus)
	if err := os.Rename(string(from), string(to)); err != nil {
		return fmt.Errorf("cannot rename folder for status change: %v", err)
	}
	return nil
}

func (f *filestore) Dispose(id msgID) error {
	// TODO: for each state and status, if msg exists, move to archived folder
	return nil
}

func (f *filestore) FetchStateStatus(id msgID, state state) (stateStatus, error) {
	for _, status := range statusList {
		folder := f.prefix.messageFolder(id, state, status)
		file := folder.contentsFile(f.contentFilename)
		_, err := os.Stat(string(file))
		if err == nil {
			return status, nil
		}
		if os.IsNotExist(err) {
			continue
		}
		return stateStatusWaiting, err
	}
	return stateStatusWaiting, nil
}

func (f *filestore) FetchRunnable() (msgID, state, error) {
	fmt.Printf("DEBUG: called FetchRunnable\n")

	var s stateID

	select {
	case s = <-f.ids:
		fmt.Printf("DEBUG: got some state id\n")
	default:
		fmt.Printf("DEBUG: sending wakeup\n")
		f.wakeup <- struct{}{}
		fmt.Printf("DEBUG: sent wakeup\n")
		s = <-f.ids
	}

	fmt.Printf("DEBUG: returned runnable %s %s\n", s.id, s.state)
	return s.id, s.state, nil
}

func (f *filestore) Transaction(id msgID) Transaction {
	return newFileTX(id, f.lockmap)
}

type cleanupFn func() error

type filetx struct {
	lock    *msgLock
	lockmap *msgLockMap
}

func newFileTX(id msgID, lockmap *msgLockMap) *filetx {
	lock := lockmap.Lock(id)
	return &filetx{
		lock:    lock,
		lockmap: lockmap,
	}
}

func (f *filetx) Discard(err error) error {
	f.lockmap.Unlock(f.lock)
	return err
}

func (f *filetx) Commit() error {
	f.lockmap.Unlock(f.lock)
	return nil
}
