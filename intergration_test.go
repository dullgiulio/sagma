package main

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
)

func TestOrderMessages(t *testing.T) {
	loggers := NewLoggers()
	saga := newSaga()

	stateFirst := state("first-state")
	stateSecond := state("second-state")
	//stateSecondHalf := state("second-state-half")
	stateThird := state("third-state")

	store := newMemstore()

	send := func(wg *sync.WaitGroup, machine *machine, state state, id msgID, body io.ReadCloser) {
		if err := machine.Receive(id, body, state); err != nil {
			t.Fatalf("cannot send message: %v\n", err)
		}
		wg.Done()
	}

	expectedStates := make(chan state, 3) // use like a safe stack
	expectedStates <- stateFirst
	expectedStates <- stateSecond
	expectedStates <- stateThird

	var wg sync.WaitGroup
	wg.Add(6) // three steps, three sends

	machine := newMachine(saga, store, loggers, 10)
	saga.begin(stateFirst, func(id msgID, body io.Reader) (sagaStates, error) {
		defer wg.Done()

		expectedState := <-expectedStates
		if expectedState != stateFirst {
			t.Fatalf("expected state is %s but got %s", expectedState, stateFirst)
		}

		return sagaNext(stateSecond), nil
		//return sagaNext(stateSecond, stateSecondHalf), nil
	})
	/*
		// TODO: optionally send message to complete this step
		saga.step(stateSecondHalf, func(id msgID, body io.Reader) (sagaStates, error) {
			fmt.Printf("*** 2-half handling second state and half completed\n")
			return SagaEnd, nil
		})
	*/
	saga.step(stateSecond, func(id msgID, body io.Reader) (sagaStates, error) {
		defer wg.Done()

		expectedState := <-expectedStates
		if expectedState != stateSecond {
			t.Fatalf("expected state is %s but got %s", expectedState, stateFirst)
		}

		return sagaNext(stateThird), nil
	})
	saga.step(stateThird, func(id msgID, body3 io.Reader) (sagaStates, error) {
		defer wg.Done()

		expectedState := <-expectedStates
		if expectedState != stateThird {
			t.Fatalf("expected state is %s but got %s", expectedState, stateFirst)
		}

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

		var buf bytes.Buffer
		mr := io.MultiReader(body1, body2, body3)
		if _, err := io.Copy(&buf, mr); err != nil {
			return SagaEnd, fmt.Errorf("cannot dump messages to output: %v", err)
		}
		if buf.String() != "1 first message\n2 second message\n3 third message\n" {
			t.Fatalf("did not return correct messages content, content is: %s", buf.String())
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
