package gomainevents

import (
	"fmt"
	"log"
)

// EventHandler is a function responsible for processing an event.
// A specific event handler should be registered for each event type
// although multiple can be registered for a single event.
type EventHandler func(Event) error

// ErrorHandler is responsible for passing errors back to the calling code
type ErrorHandler func(error)

// Provider is an interface for a source of events. The provider
// accumulates events and emits them via a channel for the Listener.
// The channel should be held open for as long as Listener is listening.
type Provider interface {
	// Return a channel that can be used to retrieve events
	Start() (<-chan Event, <-chan error)

	// Delete an event that we're done with
	Delete(Event)

	// Requeue an event for later
	Requeue(Event)

	// Stop the channel
	Stop()

	// Return the maximum retry count for the provider
	MaximumRetryCount() int
}

// Listener receives events and passes them to the registered event
// handlers. The events are provided by a Provider via a channel.
type Listener struct {
	provider Provider
	handlers map[string][]EventHandler
	done     chan bool
	debug    bool
	errorHandler *ErrorHandler
}

func NewListener(provider Provider) *Listener {
	return &Listener{
		provider: provider,
		handlers: make(map[string][]EventHandler),
		done:     make(chan bool, 1),
		debug:    true,
	}
}

func (l *Listener) RegisterHandler(name string, fn EventHandler) {
	l.handlers[name] = append(l.handlers[name], fn)
}

func (l *Listener) RegisterErrorHandler(fn ErrorHandler) {
	l.errorHandler = ErrorHandler
}

func (l *Listener) Listen() {
	// Initialize our provider
	events, errors := l.provider.Start()
	workers, max := 0, len(l.handlers)*4

	// Channel for notifying parent listener that a worker is done and needs
	// to be restarted.
	workerDone := make(chan bool, max)

	l.debugPrint("Domain events processed using %d handlers\n", max)

	// Start our workers
	for i := 0; i < max; i++ {
		go func() {
			defer func() { workers-- }()

			workers++
			l.worker(events, errors, workerDone)
			l.debugPrint("Worker closed\n")
		}()
	}

	// Start listening!
	for {
		select {
		case <-l.done:
			l.debugPrint("Halting...")
			l.provider.Stop()
			return
		case <-workerDone:
			if workers < max {
				l.debugPrint("Restarting worker...\n")
				go func() {
					defer func() { workers-- }()

					workers++
					l.worker(events, errors, workerDone)
				}()
			}
		}
	}

	l.debugPrint("finished\n")
}

func (l *Listener) worker(events <-chan Event, errors <-chan error, workerDone chan bool) {
	for {
		select {
		case event, ok := <-events:
			if !ok {
				l.debugPrint("Event provider closed.\n")
				return
			}

			l.debugPrint("Received event: %s %+v\n", event.Name(), event.Data())

			// Pass the event to a handler
			if err := l.handleEvent(event); err != nil {
				l.debugPrint("Error: %s\n", err)
				
				if (event.RetryCount() <= l.provider.MaximumRetryCount()) {
					// We should attempt to requeue the event for later
					l.provider.Requeue(event)
				} else {
					l.errorHandler(fmt.Errorf("Event exceeded maximum retry count: %s\n", event.EncodeEvent()))
				}

				workerDone <- true

				return
			}

			// If there were no errors, we're done with event. We can delete it.
			l.provider.Delete(event)
			l.debugPrint("Successfully processed.\n")
		}
	}
}

func (l *Listener) handleEvent(event Event) error {
	handlers, ok := l.handlers[event.Name()]
	if !ok {
		l.debugPrint("No handler registered for event.\n")
		return nil
	}

	for _, fn := range handlers {
		if err := fn(event); err != nil {
			return err
		}
	}

	return nil
}

func (l *Listener) debugPrint(format string, values ...interface{}) {
	if l.debug {
		log.Printf("[gomainevents] "+format, values...)
	}
}
