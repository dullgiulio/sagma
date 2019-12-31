package sagma

import (
	"log"
)

type Loggers struct {
	err *log.Logger
}

func NewLoggers(err *log.Logger) *Loggers {
	return &Loggers{
		err: err,
	}
}
