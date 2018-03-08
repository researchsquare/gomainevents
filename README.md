# Gomain Events

This is the Go implementation for RS domain events. The interfaces for listening and publishing events are defined at the top level. In the individual folders, we implement the data sources/sinks for individual services.

- *Listener*: Long running service for retrieving domain events. Requires a specific *Provider*.
- *Provider*: Implementation for an event source.
- *Publisher*: Service that allows an application to push an event to an outside store. Presumably picked up by a *Provider*

## Usage

### Retrieving events

```go
provider := myFancyEventProvider()
listener := gomainevents.NewListener(provider)

listener.RegisterHandler("ResearchSquare\\App\\Domain\\Model\\ThingHappened", func(event gomainevents.Event) error {
        // Do whatever you need to with this event

        return nil
})

// Register any number of handlers for events. You can register multiple handlers for a single event

listener.Listen()
```

### Publishing events

```go
// Define your event
type ThingHappened struct {
    Any     string
    Kind    string
    Of      string
    Data    string
}

// Implement the Event interface 

func (t ThingHappened) Name() string {
        return "ResearchSquare\\App\\Domain\\Model\\ThingHappened"
}

func (t ThingHappened) Data() map[string]interface{} {
        return map[string]interface{} {
                "any": t.Any,
                "kind": t.Kind,
                "of": t.Of,
                "data": t.Data,
        }
}

// ...

publisher := service.NewPublisher(&Config{})
if err := publisher.Publish(&ThingHappened{}); err != nil {
        log.Fatal(err)
}
```
