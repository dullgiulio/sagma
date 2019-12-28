package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

// TODO: external tool: cleanup dead handlers for retry (reset started handler if not finished before deadline) for N times
//						put files in running state back in ready state after some delay
// TODO: external tool: delay before dead letter (if not runnable before deadline, notify and remove)
//						report files stuck in waiting statuses, move and report
// TODO: external tool: remove completed at deadline
//						do something with files marked as done

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
	store, err := newShardstore(loggers, "tmp", []state{stateFirst, stateSecond, stateThird}, true)
	if err != nil {
		log.Fatalf("cannot initialize filestore: %v", err)
	}

	machine := newMachine(saga, store, loggers, 10)
	saga.begin(stateFirst, func(id msgID, body io.Reader) (state, error) {
		fmt.Printf("*** 1 handling first state completed\n")
		return stateSecond, nil
	})
	saga.step(stateSecond, func(id msgID, body io.Reader) (state, error) {
		fmt.Printf("*** 2 handling second state completed\n")
		return stateThird, nil
	})
	saga.step(stateThird, func(id msgID, body3 io.Reader) (state, error) {
		fmt.Printf("*** 3 handling third state completed\n")
		body1, err := machine.Fetch(id, stateFirst)
		if err != nil {
			return SagaEnd, fmt.Errorf("cannot fetch first message: %v", err)
		}
		defer body1.Close()
		body2, err := machine.Fetch(id, stateFirst)
		if err != nil {
			return SagaEnd, fmt.Errorf("cannot fetch first message: %v", err)
		}
		defer body2.Close()
		mr := io.MultiReader(body1, body2, body3)
		if _, err := io.Copy(os.Stdout, mr); err != nil {
			return SagaEnd, fmt.Errorf("cannot dump messages to output: %v", err)
		}
		return SagaEnd, nil
	})
	go machine.Run()

	var wg sync.WaitGroup
	wg.Add(3)
	go send(&wg, machine, stateSecond, "test", stringReadCloser("second message\n"))
	go send(&wg, machine, stateFirst, "test", stringReadCloser("first message\n"))
	go send(&wg, machine, stateThird, "test", stringReadCloser("third message\n"))

	wg.Wait()

	machine.Shutdown()
}
