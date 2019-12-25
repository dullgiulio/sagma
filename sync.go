package main

import "sync"

type msgLock struct {
	id   msgID
	wait chan<- struct{}
}

type msgLockMap struct {
	mux sync.Mutex
	ids map[msgID]chan struct{}
}

func newMsgLockMap() *msgLockMap {
	return &msgLockMap{
		ids: make(map[msgID]chan struct{}),
	}
}

func (m *msgLockMap) Lock(id msgID) *msgLock {
	for {
		m.mux.Lock()
		wait, ok := m.ids[id]
		if !ok {
			wait = make(chan struct{})
			m.ids[id] = wait
			m.mux.Unlock()
			return &msgLock{id: id, wait: wait}
		}
		m.mux.Unlock()
		<-wait
	}
}

func (m *msgLockMap) Unlock(lock *msgLock) {
	close(lock.wait)
	// cleanup this entry if it is still ours
	// if it has been replaced by some other lock, the other Unlock will clean itself
	m.mux.Lock()
	wait, ok := m.ids[lock.id]
	if ok && wait == lock.wait {
		delete(m.ids, lock.id)
	}
	m.mux.Unlock()
}
