package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/dullgiulio/sagma"
)

// TODO Timeouts
// * cleanup dead handlers (states marked as running for longer than timeout) for retry (reset started handler if not finished before deadline) for N times
//	 put files in running state back in ready state after some delay
// * delay before dead letter (if not runnable before deadline, call a notify handler)
// * remove completed after a timeout has passed in "archived" mode

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
			log.Fatalf("cannot set flag from environment variable %s: %w", key, err)
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

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func httpErrorCode(err error) int {
	switch err.(type) {
	case sagma.NotFoundError:
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}

type messageStateStatuses map[sagma.State]interface{}

func (m messageStateStatuses) Visit(id sagma.MsgID, state sagma.State, status sagma.StateStatus, ctx sagma.Context) error {
	stmap := make(map[string]interface{})
	stmap["state"] = string(state)
	stmap["status"] = string(status)
	stmap["context"] = ctx
	m[state] = stmap
	return nil
}

func (m messageStateStatuses) Failed(id sagma.MsgID, state sagma.State, failed error, ctx sagma.Context) error {
	errmap := make(map[string]interface{})
	errmap["state"] = string(state)
	errmap["failed"] = true
	errmap["error"] = failed.Error()
	errmap["context"] = ctx
	m[state] = errmap
	return nil
}

func stateHandler(machine *sagma.Machine, states []sagma.State) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		id := sagma.MsgID(vars["messageID"])
		mstates := messageStateStatuses(make(map[sagma.State]interface{}))
		if err := machine.FetchStates(id, mstates); err != nil {
			elog.Printf("cannot fetch message %s states: %w", id, err)
			http.Error(w, err.Error(), httpErrorCode(err))
			return
		}
		statuses := make([]interface{}, len(states))
		i := 0
		for _, state := range states {
			status, ok := mstates[state]
			if ok {
				statuses[i] = status
			} else {
				unknown := make(map[string]interface{})
				unknown["state"] = string(state)
				unknown["pending"] = true
				statuses[i] = unknown
			}
			i++
		}
		enc := json.NewEncoder(w)
		if err := enc.Encode(statuses); err != nil {
			elog.Printf("cannot encode respose into JSON: %w", err)
			return
		}
		dlog.Printf("request for states of %s completed", id)
	}
}

func fetchHandler(machine *sagma.Machine, state sagma.State) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		id := sagma.MsgID(vars["messageID"])
		body, _, err := machine.Fetch(id, state)
		if err != nil {
			elog.Printf("cannot fetch message %s: %w", id, err)
			http.Error(w, err.Error(), httpErrorCode(err))
			return
		}
		defer body.Close()
		if _, err := io.Copy(w, body); err != nil {
			elog.Printf("cannot copy body to response: %w", err)
			return
		}
		dlog.Printf("request for %s completed", id)
	}
}

func sendHandler(machine *sagma.Machine, state sagma.State) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		id := sagma.MsgID(vars["messageID"])
		defer r.Body.Close()
		if err := machine.Receive(id, state, sagma.NewContext(), r.Body); err != nil {
			elog.Printf("cannot put message: %v\n", err)
			http.Error(w, err.Error(), httpErrorCode(err))
			return
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("Created"))
	}
}

func main() {
	streamerType := &streamerType{}
	flag.Var(streamerType, "compression", "Type of compression to use for data at rest")
	workers := flag.Int("workers", 10, "Number of state machine workers to run")
	filesRoot := flag.String("files-root", "", "Root folder for files storage")
	listen := flag.String("listen", ":8080", "IP:PORT to listen to for HTTP")
	debug := flag.Bool("debug", false, "Print more verbose logging")
	user := flag.String("db-user", "", "Database user for DB storage")
	pass := flag.String("db-pass", "", "Database password for DB storage")
	host := flag.String("db-host", "", "Database host for DB storage")
	dbname := flag.String("db-name", "", "Database name for DB storage")

	flag.VisitAll(prefixEnv("SAGMA_HTTP", os.Getenv))
	flag.Parse()

	initLogging(*debug)

	loggers := sagma.NewLoggers(elog)
	saga := sagma.NewSaga()

	stateFirst := sagma.State("first-state")
	stateSecond := sagma.State("second-state")
	stateThird := sagma.State("third-state")

	states := []sagma.State{stateFirst, stateSecond, stateThird}

	if *filesRoot == "" {
		elog.Fatalf("specify a files root for storing messages bodies")
	}
	if *user == "" || *host == "" || *dbname == "" {
		elog.Fatalf("specify user, host and db name for database store")
	}

	blobs := sagma.NewFileBlobStore(*filesRoot, streamerType.make())
	store, err := sagma.NewPSQLStore(
		loggers,
		sagma.PSQLConnString(fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", *user, *pass, *host, *dbname)),
		"messages",
		sagma.NewTimeouts(),
		states)
	if err != nil {
		elog.Fatalf("cannot initialize store: %v", err)
	}

	machine := sagma.NewMachine(saga, store, blobs, loggers, 10)
	saga.Begin(stateFirst, func(id sagma.MsgID, ctx sagma.Context, body io.Reader, saveCtx sagma.ContextSaverFn) (*sagma.SagaStates, error) {
		dlog.Printf("*** 1 handling first state completed for %s\n", id)

		ctx["executedAt"] = time.Now()
		if err := saveCtx(ctx); err != nil {
			return nil, fmt.Errorf("cannot save context: %w", err)
		}

		return sagma.SagaNext(stateSecond), nil
	})
	saga.Step(stateSecond, func(id sagma.MsgID, ctx sagma.Context, body io.Reader, saveCtx sagma.ContextSaverFn) (*sagma.SagaStates, error) {
		dlog.Printf("*** 2 handling second state completed for %s\n", id)

		ctx["executedAt"] = time.Now()
		if err := saveCtx(ctx); err != nil {
			return nil, fmt.Errorf("cannot save context: %w", err)
		}

		return sagma.SagaNext(stateThird), nil
	})
	saga.Step(stateThird, func(id sagma.MsgID, ctx sagma.Context, body3 io.Reader, saveCtx sagma.ContextSaverFn) (*sagma.SagaStates, error) {
		ctx["startedAt"] = time.Now()
		if err := saveCtx(ctx); err != nil {
			return sagma.SagaEnd, fmt.Errorf("cannot save context: %w", err)
		}

		body1, _, err := machine.Fetch(id, stateFirst)
		if err != nil {
			return nil, fmt.Errorf("cannot fetch first message: %w", err)
		}
		defer body1.Close()
		body2, _, err := machine.Fetch(id, stateSecond)
		if err != nil {
			return nil, fmt.Errorf("cannot fetch first message: %w", err)
		}
		defer body2.Close()
		mr := io.MultiReader(body1, body2, body3)
		if _, err := io.Copy(ioutil.Discard, mr); err != nil {
			return nil, fmt.Errorf("cannot dump messages to output: %w", err)
		}
		dlog.Printf("*** 3 handling third state completed %s\n", id)

		ctx["finishedAt"] = time.Now()
		if err := saveCtx(ctx); err != nil {
			return nil, fmt.Errorf("cannot save context: %w", err)
		}

		return sagma.SagaEnd, nil
	})
	go machine.Run(*workers)

	server := &http.Server{Addr: *listen}
	exited := handleSigterm(func() {
		if err := server.Shutdown(context.Background()); err != nil {
			elog.Printf("cannot stop HTTP server: %w", err)
		}
		machine.Shutdown()
	})

	router := mux.NewRouter()
	router.HandleFunc("/health", healthzHandler)
	router.HandleFunc("/messages/{messageID}/status", stateHandler(machine, states))
	for _, state := range states {
		path := "/step/" + string(state) + "/messages/{messageID}"
		dlog.Printf("registering %s", path)
		router.HandleFunc(path, sendHandler(machine, state)).Methods("PUT")
		router.HandleFunc(path, fetchHandler(machine, state)).Methods("GET")
	}

	// TODO http.Handle("/metrics", metrics.handler())
	// PUT /step/:stepName/messages/:messageID
	// GET /step/:stepName/messages/:messageID
	// GET /messages/:messageID/status -> returns full state:status map for message
	http.Handle("/", router)

	ilog.Printf("listening on %s", *listen)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		elog.Fatalf("cannot start HTTP server: %v", err)
	}
	<-exited
}
