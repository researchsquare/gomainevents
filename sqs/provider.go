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
	sqsClient         sqsiface.SQSAPI
	queueURL          string
	events            chan gomainevents.Event
	errors            chan error
	done              chan bool
	debug             bool
	maximumRetryCount int
}

type Config struct {
	// Provide your own SQS client. Default will use the
	// default AWS session + shared credentials.
	SQSClient sqsiface.SQSAPI

	// Specify the Queue URL. Required
	QueueURL string

	// This specifies the maximum number of times an event should be retried
	MaximumRetryCount int
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

	maximumRetryCount := 25
	if config.MaximumRetryCount > 0 {
		maximumRetryCount = config.MaximumRetryCount
	}

	return &Provider{
		sqsClient: sqsClient,
		queueURL:  config.QueueURL,

		// Buffered channel makes it so that the listener will block while the channel is empty.
		events: make(chan gomainevents.Event, 100),
		errors: make(chan error, 1),
		done:   make(chan bool, 1),
		debug:  true,
		maximumRetryCount: maximumRetryCount,
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
				p.debugPrint("Error: %s\n", err)
			default:
				resp, err := p.sqsClient.ReceiveMessage(params)
				if err != nil {
					p.errors <- err
					continue
				}

				for _, msg := range resp.Messages {
					event, err := DecodeEvent(p, msg)
					if err != nil {
						p.errors <- err
						continue
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

func (p *Provider) MaximumRetryCount() int {
	return p.maximumRetryCount
}

// Requeue an event for later
func (p *Provider) Requeue(event gomainevents.Event) {
	evt := event.(Event) // Cast to SQS flavor

	p.Delete(event)

	retryCount := &awssqs.MessageAttributeValue{}
	retryCount.SetStringValue(strconv.Itoa(evt.RetryCount() + 1))
	retryCount.SetDataType("Number")

	params := &awssqs.SendMessageInput{
		QueueUrl:          aws.String(p.queueURL),
		DelaySeconds:      aws.Int64(evt.DelaySeconds()),
		MessageAttributes: map[string]*awssqs.MessageAttributeValue{"RetryCount": retryCount},
		MessageBody:       aws.String(evt.EncodeEvent()),
	}

	if nil != evt.DeduplicationID() {
		params.MessageDeduplicationId = evt.DeduplicationID()
	}

	p.debugPrint("Requeuing event. Retries: %d, Delay: %d\n", evt.RetryCount()+1, evt.DelaySeconds())
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
