package gomainevents

// Event is the interface that concrete event types should implement
// in order to be used by publishers and listeners.
type Event interface {
	Name() string
	Data() map[string]interface{}
}
