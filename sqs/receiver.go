package sqs

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

var logger = log.New(log.Writer(), "SQS: ", log.Ldate|log.Ltime|log.Lshortfile)

// MessageReceiver contract message receiver interface
type MessageReceiver interface {
	ReceiveMessage(ctx context.Context) chan *sqs.Message
}

// SQSMessageReceiver holds configuration for message receiver
type SQSMessageReceiver struct {
	sqsAPI              sqsiface.SQSAPI
	queueURL            string
	visibilityTimeout   int64
	waitTimeout         int64
	maxNumberOfMessages int64
}

// NewSQS creates new instance of SQSMessageReceiver
func NewSQS(sqsAPI sqsiface.SQSAPI, queueConfig *QueueConfig) *SQSMessageReceiver {
	return &SQSMessageReceiver{
		visibilityTimeout:   queueConfig.VisibilityTimeout,
		waitTimeout:         queueConfig.WaitTimeout,
		maxNumberOfMessages: queueConfig.MaxNumberOfMessages,
		queueURL:            queueConfig.QueueUrl,
		sqsAPI:              sqsAPI,
	}
}

// https://blog.golang.org/pipelines
// ReceiveMessage pulls messages with timeout
func (c *SQSMessageReceiver) ReceiveMessage(ctx context.Context) chan *sqs.Message {
	ch := make(chan *sqs.Message)
	logger.Printf("Started pulling messages from EndpointURL: %s", c.queueURL)
	logger.Printf("Started pulling messages only max number of messages: %v", c.maxNumberOfMessages)
	logger.Printf("Started pulling messages VisibilityTimeout of: %v", c.visibilityTimeout)
	logger.Printf("Started pulling messages WaitTimeout of: %v", c.waitTimeout)

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(ch)
				logger.Print("ReceiveMessage stopped!")
				return
			default:
				c.receiveMsg(ctx, ch)
			}
		}
	}()
	return ch
}

func (c *SQSMessageReceiver) receiveMsg(ctx context.Context, ch chan<- *sqs.Message) {
	msgResult, err := c.sqsAPI.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			aws.String(sqs.MessageSystemAttributeNameApproximateReceiveCount),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            &c.queueURL,
		MaxNumberOfMessages: &c.maxNumberOfMessages,
		WaitTimeSeconds:     &c.waitTimeout,
		VisibilityTimeout:   &c.visibilityTimeout,
	})

	if err != nil {
		logger.Panicf("Message receive failed: %v", err)
	}

	if len(msgResult.Messages) > 0 {
		for _, msg := range msgResult.Messages {
			ch <- msg
		}
	}
}
