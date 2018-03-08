package websocket

import (
	gorilla "github.com/gorilla/websocket"
	"github.com/researchsquare/gomainevents"
)

type Publisher struct {
	conn *gorilla.Conn
}

func NewPublisher(conn *gorilla.Conn) *Publisher {
	return &Publisher{conn: conn}
}

type encodedEvent struct {
	Name string                 `json:"name"`
	Data map[string]interface{} `json:"data"`
}

func (p *Publisher) Publish(event gomainevents.Event) error {
	evt := &encodedEvent{
		Name: event.Name(),
		Data: event.Data(),
	}

	return p.conn.WriteJSON(evt)
}
