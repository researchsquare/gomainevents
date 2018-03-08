package sns

import (
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awssns "github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/researchsquare/gomainevents"
)

type Publisher struct {
	snsClient snsiface.SNSAPI
	topicARN  string
}

type Config struct {
	// Provide your own SNS client. Default will use the
	// default AWS session + shared credentials.
	SNSClient snsiface.SNSAPI

	// Specify the Queue URL. Required
	TopicARN string
}

func NewPublisher(config *Config) (*Publisher, error) {
	if nil == config {
		return nil, errors.New("Configuration is required")
	}

	// Default to a new client using shared credentials
	snsClient := config.SNSClient
	if nil == snsClient {
		sess := session.Must(session.NewSession())
		snsClient = awssns.New(sess, &aws.Config{Region: aws.String("us-east-1")})
	}

	if "" == config.TopicARN {
		return nil, errors.New("TopicARN is required")
	}

	return &Publisher{
		snsClient: snsClient,
		topicARN:  config.TopicARN,
	}, nil
}

func (p *Publisher) Publish(event gomainevents.Event) error {
	encoded, err := p.encodeEvent(event)
	if err != nil {
		return err
	}

	params := &awssns.PublishInput{
		TopicArn: aws.String(p.topicARN),
		Message:  aws.String(encoded),
	}

	_, err = p.snsClient.Publish(params)

	return err
}

type encodedEvent struct {
	Name string                 `json:"name"`
	Data map[string]interface{} `json:"data"`
}

func (p *Publisher) encodeEvent(event gomainevents.Event) (string, error) {
	evt := &encodedEvent{
		Name: event.Name(),
		Data: event.Data(),
	}
	bytes, err := json.Marshal(evt)
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}
