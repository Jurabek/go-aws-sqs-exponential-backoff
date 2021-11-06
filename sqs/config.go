package sqs

type QueueConfig struct {
	VisibilityTimeout   int64
	WaitTimeout         int64
	MaxNumberOfMessages int64
	QueueUrl            string
}
