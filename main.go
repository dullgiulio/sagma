package main

import (
	"fmt"
	"io"
	"log"
	"sync"
)

// TODO: store impl that shards multiple filestores depending on hash of key
// TODO: cleanup dead handlers for retry (reset started handler if not finished before deadline) for N times
// TODO: delay before dead letter (if not runnable before deadline, notify and remove)
// TODO: remove completed at deadline

func send(wg *sync.WaitGroup, machine *machine, state state, id msgID, body io.ReadCloser) {
	if err := machine.Receive(id, body, state); err != nil {
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

	saga.begin(stateFirst, func(id msgID, body io.Reader) (state, error) {
		fmt.Printf("*** 1 handling first state completed\n")
		return stateSecond, nil
	})
	saga.step(stateSecond, func(id msgID, body io.Reader) (state, error) {
		fmt.Printf("*** 2 handling second state completed\n")
		return stateThird, nil
	})
	saga.step(stateThird, func(id msgID, body io.Reader) (state, error) {
		fmt.Printf("*** 3 handling third state completed\n")
		// TODO: fetch done first and second message and concatenate all bodies to stdout
		return SagaEnd, nil
	})

	machine := newMachine(saga, store, loggers)
	go machine.Run()

	var wg sync.WaitGroup
	wg.Add(3)
	go send(&wg, machine, stateSecond, "test", stringReadCloser("second message\n"))
	go send(&wg, machine, stateFirst, "test", stringReadCloser("first message\n"))
	go send(&wg, machine, stateThird, "test", stringReadCloser("third message\n"))

	wg.Wait()

	machine.Shutdown()
}
