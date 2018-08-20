package sqs

import (
	"errors"
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/researchsquare/gomainevents"
)

type Provider struct {
	sqsClient sqsiface.SQSAPI
	queueURL  string
	events    chan gomainevents.Event
	errors    chan error
	done      chan bool
	debug     bool
}

type Config struct {
	// Provide your own SQS client. Default will use the
	// default AWS session + shared credentials.
	SQSClient sqsiface.SQSAPI

	// Specify the Queue URL. Required
	QueueURL string
}

func NewProvider(config *Config) (*Provider, error) {
	if nil == config {
		return nil, errors.New("Configuration is required")
	}

	// Default to a new client using shared credentials
	sqsClient := config.SQSClient
	if nil == sqsClient {
		sess := session.Must(session.NewSession())
		sqsClient = awssqs.New(sess, &aws.Config{Region: aws.String("us-east-1")})
	}

	if "" == config.QueueURL {
		return nil, errors.New("QueueURL is required")
	}

	return &Provider{
		sqsClient: sqsClient,
		queueURL:  config.QueueURL,

		// Buffered channel makes it so that the listener will block while the channel is empty.
		events: make(chan gomainevents.Event, 100),
		errors: make(chan error, 1),
		done:   make(chan bool, 1),
		debug:  true,
	}, nil
}

// Return a channel that can be used to retrieve events
func (p *Provider) Start() (<-chan gomainevents.Event, <-chan error) {
	params := &awssqs.ReceiveMessageInput{
		QueueUrl:              aws.String(p.queueURL),
		WaitTimeSeconds:       aws.Int64(20),
		MessageAttributeNames: aws.StringSlice([]string{"All"}),
	}

	p.debugPrint("Listening for events from %s\n", p.queueURL)

	// This goroutine is non-blocking due to the bufferred events channel
	go func() {
		for {
			select {
			case <-p.done:
				return
			case err := <-p.errors:
				log.Println(err)
			default:
				resp, err := p.sqsClient.ReceiveMessage(params)
				if err != nil {
					p.errors <- err
					return
				}

				for _, msg := range resp.Messages {
					event, err := DecodeEvent(p, msg)
					if err != nil {
						p.errors <- err
						return
					}

					p.events <- *event
				}
			}
		}
	}()

	return p.events, p.errors
}

// Delete an event that we're done with
func (p *Provider) Delete(event gomainevents.Event) {
	evt := event.(Event) // Cast to SQS flavor

	params := &awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(p.queueURL),
		ReceiptHandle: aws.String(evt.ReceiptHandle()),
	}

	if _, err := p.sqsClient.DeleteMessage(params); err != nil {
		p.errors <- err
	}
}

// Requeue an event for later
func (p *Provider) Requeue(event gomainevents.Event) {
	evt := event.(Event) // Cast to SQS flavor

	p.Delete(event)

	params := &awssqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageDeduplicationId: evt.DeduplicationID(),
		DelaySeconds:           aws.Int64(evt.DelaySeconds()),
		MessageAttributes: map[string]*awssqs.MessageAttributeValue{
			"RetryCount": &awssqs.MessageAttributeValue{
				StringValue: aws.String(strconv.Itoa(evt.RetryCount() + 1)),
				DataType:    aws.String("Number"),
			},
		},
		MessageBody: aws.String(evt.EncodeEvent()),
	}

	if _, err := p.sqsClient.SendMessage(params); err != nil {
		p.errors <- err
	}
}

// Stop the channel
func (p *Provider) Stop() {
	close(p.events)
	close(p.errors)
	p.done <- true
}

func (p *Provider) updateVisibilityTimeout(receiptHandle string, newTimeout int64) error {
	params := &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(p.queueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: aws.Int64(newTimeout),
	}

	_, err := p.sqsClient.ChangeMessageVisibility(params)

	return err
}

func (p *Provider) debugPrint(format string, values ...interface{}) {
	if p.debug {
		log.Printf("[gomainevents-sqs] "+format, values...)
	}
}
