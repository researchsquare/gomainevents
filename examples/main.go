package main

import (
	"log"
	"net/http"
	"time"

	gorilla "github.com/gorilla/websocket"
	"github.com/researchsquare/gomainevents"
	"github.com/researchsquare/gomainevents/sns"
	"github.com/researchsquare/gomainevents/sqs"
	"github.com/researchsquare/gomainevents/websocket"
)

func main() {
	// runSNSExample()
	// runSQSExample()
	// runWebsocketExample()
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
		TopicARN: "arn:aws:sns:us-east-1:1234:domain-events-app-development",
	})

	publisher.Publish(&ExampleDomainEvent{})
}

// SQS
func runSQSExample() {
	provider, _ := sqs.NewProvider(&sqs.Config{
		QueueURL: "https://sqs.us-east-1.amazonaws.com/1234/domain-events-app-development",
	})
	listener := gomainevents.NewListener(provider)

	listener.RegisterHandler("ExampleDomainEvent", func(event gomainevents.Event) error {
		log.Printf("Hello from %s with %+v\n", event.Name(), event.Data())
		return nil
	})

	listener.Listen()
}

// Websockets
func runWebsocketExample() {
	addr := "localhost:8994"

	// Client
	time.AfterFunc(time.Second*5, func() {
		log.Println("connecting to", addr)
		conn, _, _ := gorilla.DefaultDialer.Dial("ws://"+addr, nil)

		go func() {
			for {
				_, message, _ := conn.ReadMessage()
				log.Printf("client: %s", message)
			}
		}()
	})

	// Server
	var upgrader = gorilla.Upgrader{}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upgrader.Upgrade(w, r, nil)

		publisher := websocket.NewPublisher(conn)
		publisher.Publish(&ExampleDomainEvent{})
	})

	log.Fatal(http.ListenAndServe(addr, nil))
}
