package main

import (
	"log"
	"os"
)

type Loggers struct {
	err *log.Logger
}

func NewLoggers() *Loggers {
	return &Loggers{
		err: log.New(os.Stderr, "error", log.LstdFlags),
	}
}
