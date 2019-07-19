package sqs

import (
	"fmt"
)

// RetryAttemptsExceededError represents a type of RequeuingEventFailedError
// where we've exceeded the maximum number of retries
type RetryAttemptsExceededError struct {
	EventName string
}

func (e *RetryAttemptsExceededError) Error() string {
	return fmt.Sprintf("Event exceeded maximum retry count: %s", e.EventName)
}
