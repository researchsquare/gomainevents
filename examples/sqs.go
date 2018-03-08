package main

import (
	"log"

	"github.com/researchsquare/gomainevents"
	"github.com/researchsquare/gomainevents/sqs"
)

func main() {
	provider, _ := sqs.NewProvider(&sqs.Config{
		QueueURL: "https://sqs.us-east-1.amazonaws.com/123/domain-events-app-development",
	})
	listener := gomainevents.NewListener(provider)

	listener.RegisterHandler("TestEvent", func(event gomainevents.Event) error {
		log.Printf("Hello from %s with %+v\n", event.Name(), event.Data())
		return nil
	})

	listener.Listen()
}
