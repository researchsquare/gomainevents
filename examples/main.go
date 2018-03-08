package main

import (
	"log"
	"time"

	"github.com/researchsquare/gomainevents"
	"github.com/researchsquare/gomainevents/sns"
	"github.com/researchsquare/gomainevents/sqs"
)

func main() {
	runSNSExample()
	runSQSExample()
}

// SNS
type ExampleDomainEvent struct{}

func (e ExampleDomainEvent) Name() string {
	return "ExampleDomainEvent"
}

func (e ExampleDomainEvent) Data() map[string]interface{} {
	return map[string]interface{}{
		"occurredOn": time.Now().Format(time.RFC3339),
	}
}

func runSNSExample() {
	publisher, _ := sns.NewPublisher(&sns.Config{
		TopicARN: "arn:aws:sns:us-east-1:798502617683:domain-events-rnewton-development",
	})

	publisher.Publish(&ExampleDomainEvent{})
}

// SQS
func runSQSExample() {
	provider, _ := sqs.NewProvider(&sqs.Config{
		QueueURL: "https://sqs.us-east-1.amazonaws.com/798502617683/domain-events-rnewton-megatron-development",
	})
	listener := gomainevents.NewListener(provider)

	listener.RegisterHandler("ExampleDomainEvent", func(event gomainevents.Event) error {
		log.Printf("Hello from %s with %+v\n", event.Name(), event.Data())
		return nil
	})

	listener.Listen()
}
