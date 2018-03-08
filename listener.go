package gomainevents

import "log"

// EventHandler is a function responsible for processing an event.
// A specific event handler should be registered for each event type
// although multiple can be registered for a single event.
type EventHandler func(Event) error

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
}

// Listener receives events and passes them to the registered event
// handlers. The events are provided by a Provider via a channel.
type Listener struct {
	provider Provider
	handlers map[string][]EventHandler
	done     chan bool
}

func NewListener(provider Provider) *Listener {
	return &Listener{
		provider: provider,
		handlers: make(map[string][]EventHandler),
		done:     make(chan bool, 1),
	}
}

func (l *Listener) RegisterHandler(name string, fn EventHandler) {
	l.handlers[name] = append(l.handlers[name], fn)
}

func (l *Listener) Listen() {
	// Initialize our provider
	events, errors := l.provider.Start()

	// Start listening!
	for {
		select {
		case <-l.done:
			log.Printf("Halting...")
			l.provider.Stop()
			return
		case err := <-errors:
			log.Printf("Error: %s\n", err)
			return
		case event, ok := <-events:
			if !ok {
				log.Printf("Event provider closed.\n")
				return
			}

			log.Printf("Received event: %s %+v\n", event.Name(), event.Data())

			// Pass the event to a handler
			if err := l.handleEvent(event); err != nil {
				log.Printf("Error: %s\n", err)

				// We should attempt to requeue the event for later
				l.provider.Requeue(event)
				l.provider.Stop()

				return
			}

			// If there were no errors, we're done with event. We can delete it.
			l.provider.Delete(event)
		}
	}
}

func (l *Listener) handleEvent(event Event) error {
	handlers, ok := l.handlers[event.Name()]
	if !ok {
		log.Printf("No handler registered for event.\n")
		return nil
	}

	for _, fn := range handlers {
		if err := fn(event); err != nil {
			return err
		}
	}

	return nil
}
