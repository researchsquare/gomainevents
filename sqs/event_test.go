package sqs

import (
	"math"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventDecode(t *testing.T) {
	provider := &Provider{}

	msg := &awssqs.Message{
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
		Body: aws.String("{\"Message\":\"{\\\"name\\\":\\\"Domain\\\\\\\\Event\\\",\\\"data\\\":{\\\"occurredOn\\\":\\\"2018-03-08 11:11:11\\\"}}\"}"),
	}

	event, err := DecodeEvent(provider, msg)

	require.Nil(t, err)
	assert.Equal(t, "Hello!", event.ReceiptHandle())
	assert.Equal(t, "1234", *event.DeduplicationID())
	assert.Equal(t, 5, event.RetryCount())
	assert.Equal(t, int64(math.Pow(2, 6)), event.DelaySeconds())
	assert.Equal(t, "Domain\\Event", event.Name())
	assert.Equal(t, "2018-03-08 11:11:11", event.Data()["occurredOn"].(string))
}

func TestEventEncode(t *testing.T) {
	event := &Event{
		name: "Domain\\Event",
		data: map[string]interface{}{
			"occurredOn": interface{}("2018-03-08 11:11:11"),
		},
	}

	assert.Equal(
		t,
		"{\"Message\":\"{\\\"name\\\":\\\"Domain\\\\\\\\Event\\\",\\\"data\\\":{\\\"occurredOn\\\":\\\"2018-03-08 11:11:11\\\"}}\"}",
		event.EncodeEvent(),
	)
}
