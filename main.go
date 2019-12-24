package main

import (
	"fmt"
	"sync"
	"time"
)

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
	go machine.runRunnables(50 * time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		machine.receive(&message{
			id:    "test",
			state: stateSecond,
			body:  "",
		})
		wg.Done()
	}()
	go func() {
		machine.receive(&message{
			id:    "test",
			state: stateFirst,
			body:  "",
		})
		wg.Done()
	}()
	go func() {
		machine.receive(&message{
			id:    "test",
			state: stateThird,
			body:  "",
		})
		wg.Done()
	}()
	go func() {
		wg.Wait()
		machine.stop()
	}()
	machine.run()
	time.Sleep(1 * time.Second)
}
