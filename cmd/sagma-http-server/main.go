package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/dullgiulio/sagma"
)

// TODO: external tool: cleanup dead handlers for retry (reset started handler if not finished before deadline) for N times
//						put files in running state back in ready state after some delay
// TODO: external tool: delay before dead letter (if not runnable before deadline, notify and remove)
//						report files stuck in waiting statuses, move and report
// TODO: external tool: remove completed at deadline
//						do something with files marked as done

var (
	elog *log.Logger
	dlog *log.Logger
	ilog *log.Logger
)

func initLogging(debug bool) {
	elog = log.New(os.Stderr, "error - ", log.LstdFlags)
	ilog = log.New(os.Stdout, "info - ", log.LstdFlags)
	dlog = log.New(ioutil.Discard, "", 0)
	if debug {
		dlog = log.New(os.Stdout, "debug - ", log.LstdFlags)
	}
}

func prefixEnv(prefix string, getenv func(string) string) func(*flag.Flag) {
	prefix = prefix + "_"
	return func(f *flag.Flag) {
		key := prefix + strings.Replace(strings.ToUpper(f.Name), "-", "_", -1)
		val := getenv(key)
		if val == "" {
			return
		}
		if err := f.Value.Set(val); err != nil {
			log.Fatalf("cannot set flag from environment variable %s: %v", key, err)
		}
	}
}

func handleSigterm(stop func()) <-chan struct{} {
	done := make(chan struct{})
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)
	go func() {
		var called bool
		for range c {
			if called {
				continue
			}
			called = true
			stop()
			close(done)
		}
	}()
	return done
}

type streamerType struct {
	val string
}

func (s *streamerType) String() string {
	return string(s.val)
}

func (s *streamerType) Set(v string) error {
	switch v {
	case "none":
		s.val = ""
	case "zlib", "gzip":
		s.val = v
	default:
		return fmt.Errorf("invalid compression type %s, known types are 'zlib', 'gzip', 'none'", v)
	}
	return nil
}

func (s *streamerType) make() sagma.StoreStreamer {
	switch s.val {
	case "zlib":
		return &sagma.ZlibStreamer{}
	case "gzip":
		return &sagma.GzipStreamer{}
	}
	return &sagma.NopStreamer{}
}

type storeType struct {
	val string
}

func (s *storeType) String() string {
	return string(s.val)
}

func (s *storeType) Set(v string) error {
	switch v {
	case "memory", "files", "shards":
		s.val = v
	case "":
		s.val = "memory"
	default:
		return fmt.Errorf("invalid store type %s; known store types are 'memory', 'files' and 'shards'", v)
	}
	return nil
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func send(machine *sagma.Machine, state sagma.State, id sagma.MsgID, body io.ReadCloser) {
	if err := machine.Receive(id, body, state); err != nil {
		elog.Printf("cannot send message: %v\n", err)
	}
}

func stringReadCloser(s string) io.ReadCloser {
	return ioutil.NopCloser(bytes.NewReader([]byte(s)))
}

func main() {
	storeType := &storeType{}
	streamerType := &streamerType{}
	flag.Var(storeType, "store", "Type of backing store")
	flag.Var(streamerType, "compression", "Type of compression to use for data at rest")
	workers := flag.Int("workers", 10, "Number of state machine workers to run")
	filesRoot := flag.String("files-root", "", "Root folder for files storage")
	listen := flag.String("listen", ":8080", "IP:PORT to listen to for HTTP")

	flag.VisitAll(prefixEnv("SAGMA_HTTP", os.Getenv))
	flag.Parse()

	loggers := sagma.NewLoggers(elog)
	saga := sagma.NewSaga()

	stateFirst := sagma.State("first-state")
	stateSecond := sagma.State("second-state")
	stateThird := sagma.State("third-state")

	states := []sagma.State{stateFirst, stateSecond, stateThird}

	var (
		store sagma.Store
		err   error
	)
	switch storeType.val {
	case "files":
		store, err = sagma.NewFilestore(loggers, *filesRoot, states, streamerType.make())
	case "shards":
		store, err = sagma.NewShardstore(loggers, *filesRoot, states, streamerType.make())
	default:
		store = sagma.NewMemstore()
	}
	if err != nil {
		elog.Fatalf("cannot initialize %s store: %v", storeType.val, err)
	}

	machine := sagma.NewMachine(saga, store, loggers, 10)
	saga.Begin(stateFirst, func(id sagma.MsgID, body io.Reader) (sagma.SagaStates, error) {
		dlog.Printf("*** 1 handling first state completed\n")
		return sagma.SagaNext(stateSecond), nil
	})
	saga.Step(stateSecond, func(id sagma.MsgID, body io.Reader) (sagma.SagaStates, error) {
		dlog.Printf("*** 2 handling second state completed\n")
		return sagma.SagaNext(stateThird), nil
	})
	saga.Step(stateThird, func(id sagma.MsgID, body3 io.Reader) (sagma.SagaStates, error) {
		dlog.Printf("*** 3 handling third state completed\n")
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
	go machine.Run(*workers)

	//go send(&wg, machine, stateSecond, "test", stringReadCloser("2 second message\n"))
	//go send(&wg, machine, stateFirst, "test", stringReadCloser("1 first message\n"))
	//go send(&wg, machine, stateThird, "test", stringReadCloser("3 third message\n"))

	server := &http.Server{Addr: *listen}
	exited := handleSigterm(func() {
		if err := server.Shutdown(context.Background()); err != nil {
			elog.Printf("cannot stop HTTP server: %v", err)
		}
		machine.Shutdown()
	})

	// TODO http.Handle("/metrics", metrics.handler())
	http.HandleFunc("/health", healthzHandler)
	// PUT /step/:stepName/messages/:messageID
	// GET /messages/:messageID/status -> returns full state:status map for message
	// GET /messages/:messageID -> contents of message

	ilog.Printf("listening on %s", *listen)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		elog.Fatalf("cannot start HTTP server: %v", err)
	}
	<-exited
}
