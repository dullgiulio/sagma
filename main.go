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

	stateFirst := state("first-state")
	stateSecond := state("second-state")
	stateSecondHalf := state("second-state-half")
	stateThird := state("third-state")

	//store := newMemstore()
	store, err := newShardstore(loggers, "tmp", []state{stateFirst, stateSecond, stateSecondHalf, stateThird}, gzipStreamer{})
	if err != nil {
		log.Fatalf("cannot initialize filestore: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(6) // three steps, three sends

	machine := newMachine(saga, store, loggers, 10)
	saga.begin(stateFirst, func(id msgID, body io.Reader) (sagaStates, error) {
		defer wg.Done()
		fmt.Printf("*** 1 handling first state completed\n")
		return sagaNext(stateSecond, stateSecondHalf), nil
	})
	// TODO: optionally send message to complete this step
	saga.step(stateSecondHalf, func(id msgID, body io.Reader) (sagaStates, error) {
		fmt.Printf("*** 2-half handling second state and half completed\n")
		return SagaEnd, nil
	})
	saga.step(stateSecond, func(id msgID, body io.Reader) (sagaStates, error) {
		defer wg.Done()
		fmt.Printf("*** 2 handling second state completed\n")
		return sagaNext(stateThird), nil
	})
	saga.step(stateThird, func(id msgID, body3 io.Reader) (sagaStates, error) {
		defer wg.Done()
		fmt.Printf("*** 3 handling third state completed\n")
		body1, err := machine.Fetch(id, stateFirst)
		if err != nil {
			return SagaEnd, fmt.Errorf("cannot fetch first message: %v", err)
		}
		defer body1.Close()
		body2, err := machine.Fetch(id, stateSecond)
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
	go machine.Run(2)

	go send(&wg, machine, stateSecond, "test", stringReadCloser("2 second message\n"))
	go send(&wg, machine, stateFirst, "test", stringReadCloser("1 first message\n"))
	go send(&wg, machine, stateThird, "test", stringReadCloser("3 third message\n"))

	wg.Wait()

	machine.Shutdown()
}
