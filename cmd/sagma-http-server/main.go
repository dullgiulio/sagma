package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"github.com/dullgiulio/sagma"
)

// TODO: external tool: cleanup dead handlers for retry (reset started handler if not finished before deadline) for N times
//						put files in running state back in ready state after some delay
// TODO: external tool: delay before dead letter (if not runnable before deadline, notify and remove)
//						report files stuck in waiting statuses, move and report
// TODO: external tool: remove completed at deadline
//						do something with files marked as done

func send(wg *sync.WaitGroup, machine *sagma.Machine, state sagma.State, id sagma.MsgID, body io.ReadCloser) {
	if err := machine.Receive(id, body, state); err != nil {
		fmt.Printf("ERROR: cannot send message: %v\n", err)
	}
	wg.Done()
}

func stringReadCloser(s string) io.ReadCloser {
	return ioutil.NopCloser(bytes.NewReader([]byte(s)))
}

func main() {
	loggers := sagma.NewLoggers()
	saga := sagma.NewSaga()

	stateFirst := sagma.State("first-state")
	stateSecond := sagma.State("second-state")
	stateSecondHalf := sagma.State("second-state-half")
	stateThird := sagma.State("third-state")

	//store := sagma.NewMemstore()
	store, err := sagma.NewShardstore(loggers, "tmp", []sagma.State{stateFirst, stateSecond, stateSecondHalf, stateThird}, sagma.GzipStreamer{})
	if err != nil {
		log.Fatalf("cannot initialize filestore: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(6) // three steps, three sends

	machine := sagma.NewMachine(saga, store, loggers, 10)
	saga.Begin(stateFirst, func(id sagma.MsgID, body io.Reader) (sagma.SagaStates, error) {
		defer wg.Done()
		fmt.Printf("*** 1 handling first state completed\n")
		return sagma.SagaNext(stateSecond), nil
	})
	saga.Step(stateSecond, func(id sagma.MsgID, body io.Reader) (sagma.SagaStates, error) {
		defer wg.Done()
		fmt.Printf("*** 2 handling second state completed\n")
		return sagma.SagaNext(stateThird), nil
	})
	saga.Step(stateThird, func(id sagma.MsgID, body3 io.Reader) (sagma.SagaStates, error) {
		defer wg.Done()
		fmt.Printf("*** 3 handling third state completed\n")
		body1, err := machine.Fetch(id, stateFirst)
		if err != nil {
			return sagma.SagaEnd, fmt.Errorf("cannot fetch first message: %v", err)
		}
		defer body1.Close()
		body2, err := machine.Fetch(id, stateSecond)
		if err != nil {
			return sagma.SagaEnd, fmt.Errorf("cannot fetch first message: %v", err)
		}
		defer body2.Close()
		mr := io.MultiReader(body1, body2, body3)
		if _, err := io.Copy(os.Stdout, mr); err != nil {
			return sagma.SagaEnd, fmt.Errorf("cannot dump messages to output: %v", err)
		}
		return sagma.SagaEnd, nil
	})
	go machine.Run(2)

	go send(&wg, machine, stateSecond, "test", stringReadCloser("2 second message\n"))
	go send(&wg, machine, stateFirst, "test", stringReadCloser("1 first message\n"))
	go send(&wg, machine, stateThird, "test", stringReadCloser("3 third message\n"))

	wg.Wait()

	machine.Shutdown()
}
