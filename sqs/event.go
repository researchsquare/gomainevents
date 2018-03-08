package sqs

import (
	"encoding/json"
	"math"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

// Event implements the standard domain event interface, but
// includes SQS-specific helpers.
type Event struct {
	name string
	data map[string]interface{}

	// We want a reference to the provider that retrieved the message
	// for this event from SQS. This allows the EventHandler to operate
	// on the message semi-directly, if necessary.
	provider *Provider

	// Messages have a unique identifier for each time they're
	// received. This is necessary for deleting and requeueing.
	receiptHandle string

	// FIFO queues require a deduplication ID if ContentBasedDeduplication
	// isn't being used.
	deduplicationID *string

	// Messages can be retried a set number of times before they
	// go to a deadletter queue.
	retryCount int
}

type encodedEvent struct {
	Name string                 `json:"name"`
	Data map[string]interface{} `json:"data"`
}

type encodedMessage struct {
	Message string
}

// DecodeEvent will take an SQS message and extract all the information
// for an event. Metadata (receipt handle and visibility timeout) is included
// for the purposes of re-queueing and deleting the message after the
// event handlers are done with it.
func DecodeEvent(provider *Provider, message *awssqs.Message) (*Event, error) {
	// Extract the metadata provided by SQS
	event := &Event{
		provider:        provider,
		receiptHandle:   *message.ReceiptHandle,
		deduplicationID: message.Attributes["DeduplicationID"],
	}

	// Determine if we have a retry count and default to 0 if this is the first time we've seen it.
	retryCountStr, ok := message.MessageAttributes["RetryCount"]
	if !ok {
		event.retryCount = 0
	} else {
		retryCount, err := strconv.Atoi(*retryCountStr.StringValue)
		if err != nil {
			return nil, err
		}

		event.retryCount = retryCount
	}

	// And now fill in the actual event!
	// We have to double-decode because the body is json and the message
	// inside the body is also json.
	body := []byte(aws.StringValue(message.Body))
	msg := &encodedMessage{}
	if err := json.Unmarshal(body, msg); err != nil {
		return nil, err
	}

	evt := &encodedEvent{}
	if err := json.Unmarshal([]byte(msg.Message), evt); err != nil {
		return nil, err
	}

	event.name = evt.Name
	event.data = evt.Data

	return event, nil
}

func (e *Event) EncodeEvent() string {
	evt := &encodedEvent{
		Name: e.Name(),
		Data: e.Data(),
	}
	bytes, _ := json.Marshal(evt)

	msg := &encodedMessage{
		Message: string(bytes),
	}

	bytes, _ = json.Marshal(msg)

	return string(bytes)
}

func (e Event) Name() string {
	return e.name
}

func (e Event) Data() map[string]interface{} {
	return e.data
}

// ReceiptHandle returns the unique identifier for the message that this event
// was created from.
func (e *Event) ReceiptHandle() string {
	return e.receiptHandle
}

// DeduplicationID returns the deduplication ID for FIFO queues, if set.
func (e *Event) DeduplicationID() *string {
	return e.deduplicationID
}

// DelaySeconds returns the number of seconds to delay before this
// message becomes available.
func (e *Event) DelaySeconds() int64 {
	return int64(math.Min(
		math.Pow(2, float64(e.retryCount+1)),
		15*60, // Max is 15 minutes
	))
}

// RetryCount returns the number of times this event has been delivered, but
// not processed.
func (e *Event) RetryCount() int {
	return e.retryCount
}

// UpdateVisibilityTimeout changes the timeout for this message only. It is up
// to the provider to check if the timeout is different from the default for the
// queue and to update it accordingly.
func (e *Event) UpdateVisibilityTimeout(newTimeout int64) error {
	return e.provider.updateVisibilityTimeout(e.receiptHandle, newTimeout)
}
