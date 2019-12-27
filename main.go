package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// TODO: store impl that shards multiple filestores depending on hash of key
// TODO: cleanup dead handlers for retry (reset started handler if not finished before deadline) for N times
// TODO: delay before dead letter (if not runnable before deadline, notify and remove)
// TODO: remove completed at deadline

func send(wg *sync.WaitGroup, machine *machine, state state, msg *message) {
	if err := machine.Receive(msg, state); err != nil {
		fmt.Printf("ERROR: cannot send message: %v\n", err)
	}
	wg.Done()
}

func main() {
	loggers := NewLoggers()
	saga := newSaga()

	stateFirst := state("firstState")
	stateSecond := state("secondState")
	stateThird := state("thirdState")

	//store := newMemstore()
	store, err := newFilestore(loggers, "tmp", []state{stateFirst, stateSecond, stateThird})
	if err != nil {
		log.Fatalf("cannot initialize filestore: %v", err)
	}

	saga.begin(stateFirst, func(*message) (state, error) {
		fmt.Printf("*** 1 handling first state completed\n")
		return stateSecond, nil
	})
	saga.step(stateSecond, func(*message) (state, error) {
		fmt.Printf("*** 2 handling second state completed\n")
		return stateThird, nil
	})
	saga.step(stateThird, func(*message) (state, error) {
		fmt.Printf("*** 3 handling third state completed\n")
		return SagaEnd, nil
	})

	machine := newMachine(saga, store, loggers)
	go machine.Run()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		send(&wg, machine, stateSecond, &message{
			id:   "test",
			body: []byte("second message"),
		})
	}()
	go func() {
		send(&wg, machine, stateFirst, &message{
			id:   "test",
			body: []byte("first message"),
		})
	}()
	go func() {
		send(&wg, machine, stateThird, &message{
			id:   "test",
			body: []byte("third message"),
		})
	}()
	go func() {
		wg.Wait()
	}()
	time.Sleep(10 * time.Second)
}
