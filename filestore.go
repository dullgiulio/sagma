package main

import (
	"compress/zlib"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

// TODO: implement atomic file write
func writeFile(filename string, r io.Reader, perm os.FileMode, compress bool) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	w := io.WriteCloser(f)
	if compress {
		w = zlib.NewWriter(w)
	}
	_, err = io.Copy(w, r)
	if compress {
		if err1 := w.Close(); err == nil {
			err = err1
		}
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

var statusList = []stateStatus{
	stateStatusRecvWaiting,
	stateStatusReadyWaiting,
	stateStatusReady,
	stateStatusRunning,
	stateStatusDone,
}

type msgFolder string

func (f msgFolder) contentsFile(filename string) string {
	return filepath.Join(string(f), filename)
}

type fileprefix string

func (f fileprefix) messageFolder(id msgID, state state, status stateStatus) msgFolder {
	msgid := string(id)
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

func (f stateFolder) scan(state state, ids chan<- stateID, batchSize int) error {
	dh, err := os.Open(string(f))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("cannot read runnables folder: %v", err)
	}
	defer dh.Close()

	for {
		names, err := dh.Readdirnames(batchSize)
		if err != nil && err != io.EOF {
			return fmt.Errorf("cannot read batch of runnable folder names: %v", err)
		}

		// TODO: move to statistics system
		// fmt.Printf("INFO: scan of %s returned %d elements\n", f, len(names))

		for _, name := range names {
			ids <- stateID{state: state, id: msgID(name)}
		}
		if err == io.EOF {
			break
		}
	}
	return nil
}

type filestore struct {
	log             *Loggers
	lockmap         *msgLockMap
	prefix          fileprefix
	compress        bool
	contentFilename string
	wakeup          chan struct{}
	states          []state
}

func newFilestore(log *Loggers, prefix string, states []state, compress bool) (*filestore, error) {
	fprefix := fileprefix(prefix)
	if err := fprefix.createAllFolders(stateStatuses, states); err != nil {
		return nil, fmt.Errorf("cannot initialize file store folders: %v", err)
	}

	contentFilename := "contents"
	if compress {
		contentFilename = "contents.z"
	}

	return &filestore{
		log:             log,
		prefix:          fprefix,
		states:          states,
		lockmap:         newMsgLockMap(),
		compress:        compress,
		contentFilename: contentFilename,
	}, nil
}

func (f *filestore) Store(id msgID, body io.Reader, st state, status stateStatus) error {
	folder := f.prefix.messageFolder(id, st, status)
	if err := os.MkdirAll(string(folder), 0744); err != nil {
		return fmt.Errorf("cannot make message folder: %v", err)
	}
	if err := writeFile(folder.contentsFile(f.contentFilename), body, 0644, f.compress); err != nil {
		return fmt.Errorf("cannot write contents file: %v", err)
	}
	return nil
}

func (f *filestore) Fetch(id msgID, state state, status stateStatus) (io.ReadCloser, error) {
	folder := f.prefix.messageFolder(id, state, status)
	file := folder.contentsFile(f.contentFilename)
	fh, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("cannot read message contents: %v", err)
	}
	r := io.ReadCloser(fh)
	if f.compress {
		r, err = zlib.NewReader(fh)
		if err != nil {
			return nil, fmt.Errorf("cannot start zlib decompressor on contents file: %v", err)
		}
	}
	return r, nil
}

func (f *filestore) StoreStateStatus(id msgID, st state, currStatus, nextStatus stateStatus) error {
	if currStatus == stateStatusWaiting {
		folder := f.prefix.messageFolder(id, st, nextStatus)
		if err := os.MkdirAll(string(folder), 0744); err != nil {
			return fmt.Errorf("cannot make message folder for status change: %v", err)
		}
		if err := ioutil.WriteFile(folder.contentsFile(f.contentFilename), []byte(""), 0644); err != nil {
			return fmt.Errorf("cannot write contents file for status change: %v", err)
		}
		return nil
	}
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

func (f *filestore) PollRunnables(ids chan<- stateID) error {
	var wg sync.WaitGroup
	wg.Add(len(f.states))
	for _, st := range f.states {
		folder := f.prefix.stateFolder(st, stateStatusReady)
		go func(st state, folder stateFolder) {
			if err := folder.scan(st, ids, 100); err != nil {
				f.log.err.Printf("cannot scan runnable for state %s: %v", st, err)
			}
			wg.Done()
		}(st, folder)
	}
	wg.Wait()
	return nil
}

func (f *filestore) Transaction(id msgID) Transaction {
	return newFileTX(id, f.lockmap)
}

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
