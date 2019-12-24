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

func main() {
	saga := newSaga()
	stateFirst := state("firstState")
	saga.step(stateFirst, func(*message) error {
		fmt.Printf("handling first state completed\n")
		return nil
	})
	stateSecond := state("secondState")
	saga.step(stateSecond, func(*message) error {
		fmt.Printf("handling second state completed\n")
		return nil
	})
	stateThird := state("thirdState")
	saga.step(stateThird, func(*message) error {
		fmt.Printf("handling third state completed\n")
		return nil
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
