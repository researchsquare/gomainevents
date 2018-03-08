package sqs

import (
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/stretchr/testify/assert"
)

type mockSQS struct {
	sqsiface.SQSAPI
	ReceiveMessageOutput *awssqs.ReceiveMessageOutput
}

func (m mockSQS) ReceiveMessage(in *awssqs.ReceiveMessageInput) (*awssqs.ReceiveMessageOutput, error) {
	// Only need to return mocked response output
	return m.ReceiveMessageOutput, nil
}

func TestNewProvider(t *testing.T) {
	// Success case - provide client and queue
	mockClient := &mockSQS{}
	config := &Config{
		SQSClient: mockClient,
		QueueURL:  "queueueueueueue",
	}

	provider, err := NewProvider(config)
	assert.NotNil(t, provider)
	assert.Nil(t, err)

	// Success case - provide queue only, client is default
	config = &Config{
		QueueURL: "queueueueueueue",
	}

	provider, err = NewProvider(config)
	assert.NotNil(t, provider)
	assert.Nil(t, err)

	// Failure case - no queue provided
	provider, err = NewProvider(nil)
	assert.Nil(t, provider)
	assert.NotNil(t, err)
}

func TestStart(t *testing.T) {
	mockClient := &mockSQS{}
	config := &Config{
		SQSClient: mockClient,
		QueueURL:  "queueueueueueue",
	}

	provider, _ := NewProvider(config)

	mockClient.ReceiveMessageOutput = &awssqs.ReceiveMessageOutput{
		Messages: []*awssqs.Message{
			&awssqs.Message{
				ReceiptHandle: aws.String("Hello!"),
				Attributes: aws.StringMap(map[string]string{
					"DeduplicationID": "1234",
				}),
				MessageAttributes: map[string]*awssqs.MessageAttributeValue{
					"RetryCount": &awssqs.MessageAttributeValue{
						StringValue: aws.String("5"),
						DataType:    aws.String("Number"),
					},
				},
				Body: aws.String("{\"Message\":{\"name\":\"Domain\\\\Event\",\"data\":{\"occurredOn\":\"2018-03-08 11:11:11\"}}}"),
			},
			&awssqs.Message{
				ReceiptHandle: aws.String("Goodbye!"),
				Attributes: aws.StringMap(map[string]string{
					"DeduplicationID": "4321",
				}),
				MessageAttributes: map[string]*awssqs.MessageAttributeValue{
					"RetryCount": &awssqs.MessageAttributeValue{
						StringValue: aws.String("15"),
						DataType:    aws.String("Number"),
					},
				},
				Body: aws.String("{\"Message\":{\"name\":\"Domain\\\\Event\",\"data\":{\"occurredOn\":\"2018-03-08 12:12:12\"}}}"),
			},
		},
	}

	events, errors := provider.Start()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for {
			select {
			case err := <-errors:
				assert.Nil(t, err) // This should fail if we do get an error
			case event := <-events:
				assert.Equal(t, "Domain\\Event", event.Name())
				wg.Done()
			}
		}
	}()

	go func() {
		wg.Wait()
		provider.Stop()
	}()
}
