package sagma

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

func writeFile(streamer StoreStreamer, filename string, r io.Reader, perm os.FileMode) error {
	tmpname := filename + ".tmp"
	f, err := os.OpenFile(tmpname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	w, err := streamer.Writer(f)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, r)
	if err1 := w.Close(); err == nil {
		err = err1
	}
	if err == nil {
		if err1 := os.Rename(tmpname, filename); err1 != nil {
			os.Remove(tmpname) // cleanup is best effort only
			err = err1
		}
	} else {
		os.Remove(tmpname)
	}
	return err
}

var statusList = []StateStatus{
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

func (f fileprefix) messageFolder(id MsgID, state State, status StateStatus) msgFolder {
	msgid := string(id)
	return msgFolder(filepath.Join(string(f), string(status), string(state), msgid))
}

func (f fileprefix) stateFolder(state State, status StateStatus) stateFolder {
	return stateFolder(filepath.Join(string(f), string(status), string(state)))
}

func (f fileprefix) createAllFolders(statuses []StateStatus, states []State) error {
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

func (f stateFolder) scan(state State, ids chan<- StateID, batchSize int) error {
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
			ids <- StateID{state: state, id: MsgID(name)}
		}
		if err == io.EOF {
			break
		}
	}
	return nil
}

type Filestore struct {
	log             *Loggers
	lockmap         *msgLockMap
	prefix          fileprefix
	streamer        StoreStreamer
	contentFilename string
	wakeup          chan struct{}
	states          []State
}

func NewFilestore(log *Loggers, prefix string, states []State, streamer StoreStreamer) (*Filestore, error) {
	if streamer == nil {
		streamer = NopStreamer{}
	}
	fprefix := fileprefix(prefix)
	if err := fprefix.createAllFolders(stateStatuses, states); err != nil {
		return nil, fmt.Errorf("cannot initialize file store folders: %v", err)
	}

	contentFilename := streamer.Filename("contents")
	return &Filestore{
		log:             log,
		prefix:          fprefix,
		states:          states,
		lockmap:         newMsgLockMap(),
		streamer:        streamer,
		contentFilename: contentFilename,
	}, nil
}

func (f *Filestore) cleanWaitingStates(id MsgID, state State) error {
	folder := f.prefix.messageFolder(id, state, stateStatusRecvWaiting)
	file := folder.contentsFile(f.contentFilename)
	if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
		return err
	}
	folder = f.prefix.messageFolder(id, state, stateStatusReadyWaiting)
	file = folder.contentsFile(f.contentFilename)
	if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// TODO: context is unsupported
func (f *Filestore) Store(tx Transaction, id MsgID, body io.Reader, st State, status StateStatus, ctx Context) error {
	folder := f.prefix.messageFolder(id, st, status)
	if err := os.MkdirAll(string(folder), 0744); err != nil {
		return fmt.Errorf("cannot make message folder: %v", err)
	}
	if err := writeFile(f.streamer, folder.contentsFile(f.contentFilename), body, 0644); err != nil {
		return fmt.Errorf("cannot write contents file: %v", err)
	}
	if status == stateStatusReady {
		if err := f.cleanWaitingStates(id, st); err != nil {
			return fmt.Errorf("cannot clean readiness marker: %v", err)
		}
	}
	return nil
}

func (f *Filestore) StoreContext(tx Transaction, id MsgID, st State, ctx Context) error {
	// TODO
	return nil
}

func (f *Filestore) Fail(tx Transaction, id MsgID, st State, reason error) error {
	folder := f.prefix.messageFolder(id, st, stateStatusError)
	file := folder.contentsFile("error")
	if err := os.MkdirAll(string(folder), 0744); err != nil {
		return fmt.Errorf("cannot make message folder for failure result: %v", err)
	}
	if err := ioutil.WriteFile(file, []byte(reason.Error()), 0644); err != nil {
		return fmt.Errorf("cannot write contents file failure result: %v", err)
	}
	// TODO: write context file
	return nil
}

func (f *Filestore) FetchStates(tx Transaction, id MsgID, visitor MessageVisitor) error {
	stat := func(fname string) (bool, error) {
		_, err := os.Stat(fname)
		if err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	ctx := Context(make(map[string]interface{})) // TODO: fetch context from file
	for _, status := range statusList {
		for _, state := range f.states {
			folder := f.prefix.messageFolder(id, state, status)
			exists, err := stat(folder.contentsFile(f.contentFilename))
			if err != nil {
				return fmt.Errorf("cannot stat message file to get statuses: %v", err)
			}
			if exists {
				if err := visitor.Visit(id, state, status, ctx); err != nil {
					return err
				}
			}
			contents, err := ioutil.ReadFile(folder.contentsFile("error"))
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return fmt.Errorf("cannot read error file: %v", err)
			}
			if err := visitor.Failed(id, state, errors.New(string(contents)), ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *Filestore) Fetch(tx Transaction, id MsgID, state State, status StateStatus) (io.ReadCloser, Context, error) {
	folder := f.prefix.messageFolder(id, state, status)
	file := folder.contentsFile(f.contentFilename)
	fh, err := os.Open(file)
	if err != nil {
		return nil, nil, NotFoundError(fmt.Errorf("cannot read message contents: %v", err))
	}
	r, err := f.streamer.Reader(fh)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot wrap reader with streamer: %v", err)
	}
	ctx := Context(make(map[string]interface{})) // TODO: decode from context file
	return r, ctx, nil
}

func (f *Filestore) StoreStateStatus(tx Transaction, id MsgID, st State, currStatus, nextStatus StateStatus) error {
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

func (f *Filestore) Dispose(tx Transaction, id MsgID) error {
	// TODO: for each state and status, if msg exists, move to archived folder
	return nil
}

func (f *Filestore) FetchStateStatus(tx Transaction, id MsgID, state State) (StateStatus, error) {
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

func (f *Filestore) PollRunnables(ids chan<- StateID) error {
	var wg sync.WaitGroup
	wg.Add(len(f.states))
	for _, st := range f.states {
		folder := f.prefix.stateFolder(st, stateStatusReady)
		go func(st State, folder stateFolder) {
			if err := folder.scan(st, ids, 100); err != nil {
				f.log.err.Printf("cannot scan runnable for state %s: %v", st, err)
			}
			wg.Done()
		}(st, folder)
	}
	wg.Wait()
	return nil
}

func (f *Filestore) Transaction(id MsgID) (Transaction, error) {
	return newFileTX(id, f.lockmap), nil
}

type filetx struct {
	lock    *msgLock
	lockmap *msgLockMap
}

func newFileTX(id MsgID, lockmap *msgLockMap) *filetx {
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
