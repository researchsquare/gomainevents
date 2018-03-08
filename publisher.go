package gomainevents

// Publisher pushes events to a store
type Publisher interface {
	// Publish pushes an individual event to a store
	Publish(Event) error
}
