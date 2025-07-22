package async

import (
	"runtime/debug"

	"github.com/rs/zerolog/log"
)

// recoverPanic is a local helper to recover from panics and log the stack trace.
func recoverPanic() {
	if r := recover(); r != nil {
		log.Error().
			Interface("panic", r).
			Bytes("stack", debug.Stack()).
			Msg("goroutine panicked")
	}
}

func Go(fn func()) {
	go func() {
		defer recoverPanic()
		fn()
	}()
}
