package sqs

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type ExpBackoff struct {
	config QueueConfig
	sqs    sqsiface.SQSAPI
}

func (e *ExpBackoff) ChangeVisibility(ctx context.Context, message *sqs.Message) {
	approximateReceiveCount := getApproximateReceiveCount(message)
	logger.Printf("Message received at: %s", time.Now().Format(time.RFC3339))
	logger.Printf("Messaged received %v times", approximateReceiveCount)
	logger.Printf("Receipt handle %s", *message.ReceiptHandle)
	logger.Printf("Message id: %s", *message.MessageId)

	if approximateReceiveCount > 6 {
		e.deleteSQSMessage(ctx, message)
		return
	}

	messageVisibilityInput := sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &e.config.QueueUrl,
		ReceiptHandle:     message.ReceiptHandle,
		VisibilityTimeout: visibleAfterSeconds(approximateReceiveCount),
	}

	if _, err := e.sqs.ChangeMessageVisibilityWithContext(ctx, &messageVisibilityInput); err != nil {
		logger.Printf("Could not change MessageVisibility: %v", err.Error())
	}
}

// deletes processed message from SQS
func (e *ExpBackoff) deleteSQSMessage(ctx context.Context, message *sqs.Message) {
	logger.Printf("Deleting message from SQS started!")

	if _, err := e.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &e.config.QueueUrl,
		ReceiptHandle: message.ReceiptHandle,
	}); err != nil {
		logger.Printf("Deleting message from SQS failed: %v", err)
		return
	}

	logger.Printf("Deleting message completed")
}

func visibleAfterSeconds(count int) *int64 {
	var result int64

	switch count {
	case 1:
		result = 30
	case 2:
		result = 40
	case 3:
		result = 45
	case 4:
		result = 50
	case 5:
		result = 55
	case 6:
		result = 60
	default:
		result = 30
	}

	logger.Printf("Message will be visible after %v seconds!", result)
	return aws.Int64(result)
}

func getApproximateReceiveCount(message *sqs.Message) int {
	att := *message.Attributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]
	res, err := strconv.Atoi(att)

	if err != nil {
		logger.Print("Failed converting GetApproximateReceiveCount into Int")
		return -1
	}

	return res
}
