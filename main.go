package main

import (
	"fmt"
	"sync"
	"time"
)

func send(wg *sync.WaitGroup, machine *machine, msg *message) {
	defer wg.Done()

	if err := machine.Receive(msg); err != nil {
		fmt.Printf("ERROR: cannot send message: %v\n", err)
	}
}

// TODO: cleanup state objects passed to and from store
// TODO: cleanup dead handlers for retry (reset started handler if not finished before deadline) for N times
// TODO: delay before dead letter (if not runnable before deadline, notify and remove)
// TODO: remove completed at deadline

func main() {
	saga := newSaga()
	stateFirst := state("firstState")
	stateSecond := state("secondState")
	stateThird := state("thirdState")
	saga.begin(stateFirst, func(*message) (state, error) {
		fmt.Printf("handling first state completed\n")
		return stateSecond, nil
	})
	saga.step(stateSecond, func(*message) (state, error) {
		fmt.Printf("handling second state completed\n")
		return stateThird, nil
	})
	saga.step(stateThird, func(*message) (state, error) {
		fmt.Printf("handling third state completed\n")
		return sagaEnd, nil
	})
	machine := newMachine(saga, newMemstore())
	go machine.RunRunnables(50 * time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		send(&wg, machine, &message{
			id:    "test",
			state: stateSecond,
			body:  "",
		})
	}()
	go func() {
		send(&wg, machine, &message{
			id:    "test",
			state: stateFirst,
			body:  "",
		})
	}()
	go func() {
		send(&wg, machine, &message{
			id:    "test",
			state: stateThird,
			body:  "",
		})
	}()
	go func() {
		wg.Wait()
	}()
	time.Sleep(1 * time.Second)
}
