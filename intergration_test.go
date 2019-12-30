package sagma

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"testing"
)

func stringReadCloser(s string) io.ReadCloser {
	return ioutil.NopCloser(bytes.NewReader([]byte(s)))
}

func TestOrderMessages(t *testing.T) {
	loggers := NewLoggers()
	saga := NewSaga()

	stateFirst := State("first-state")
	stateSecond := State("second-state")
	//stateSecondHalf := State("second-state-half")
	stateThird := State("third-state")

	store := NewMemstore()

	send := func(wg *sync.WaitGroup, machine *Machine, state State, id MsgID, body io.ReadCloser) {
		if err := machine.Receive(id, body, state); err != nil {
			t.Fatalf("cannot send message: %v\n", err)
		}
		wg.Done()
	}

	expectedStates := make(chan State, 3) // use like a safe stack
	expectedStates <- stateFirst
	expectedStates <- stateSecond
	expectedStates <- stateThird

	var wg sync.WaitGroup
	wg.Add(6) // three steps, three sends

	machine := NewMachine(saga, store, loggers, 10)
	saga.Begin(stateFirst, func(id MsgID, body io.Reader) (SagaStates, error) {
		defer wg.Done()

		expectedState := <-expectedStates
		if expectedState != stateFirst {
			t.Fatalf("expected state is %s but got %s", expectedState, stateFirst)
		}

		return SagaNext(stateSecond), nil
		//return sagaNext(stateSecond, stateSecondHalf), nil
	})
	/*
		// TODO: optionally send message to complete this step
		saga.step(stateSecondHalf, func(id msgID, body io.Reader) (sagaStates, error) {
			fmt.Printf("*** 2-half handling second state and half completed\n")
			return SagaEnd, nil
		})
	*/
	saga.Step(stateSecond, func(id MsgID, body io.Reader) (SagaStates, error) {
		defer wg.Done()

		expectedState := <-expectedStates
		if expectedState != stateSecond {
			t.Fatalf("expected state is %s but got %s", expectedState, stateFirst)
		}

		return SagaNext(stateThird), nil
	})
	saga.Step(stateThird, func(id MsgID, body3 io.Reader) (SagaStates, error) {
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
