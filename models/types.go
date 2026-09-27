package models

import (
	"errors"
	"fmt"
)

// TaskStatus represents the current status of an airport task
type TaskStatus string

const (
	StatusPending   TaskStatus = "PENDING"
	StatusRunning   TaskStatus = "RUNNING"
	StatusCompleted TaskStatus = "COMPLETED"
	StatusFailed    TaskStatus = "FAILED"
	StatusCancelled TaskStatus = "CANCELLED"
)

// Priority represents the execution priority level of an airport ground task
type Priority int

const (
	PriorityLow Priority = iota + 1
	PriorityMedium
	PriorityHigh
	PriorityCritical
)

// Action is the executable unit of work for a ground task
type Action func() error

// Standard Sentinel Errors
var (
	ErrInvalidTaskID    = errors.New("invalid task id")
	ErrEmptyTitle       = errors.New("task title cannot be empty")
	ErrEmptyFlightID    = errors.New("flight id cannot be empty")
	ErrNilAction        = errors.New("task action cannot be nil")
	ErrInvalidPriority  = errors.New("invalid task priority")
	ErrTaskNotPending   = errors.New("task is not in pending status")
	ErrTaskCannotCancel = errors.New("task cannot be cancelled in its current state")
)

// ValidationError is a structured custom error for task validation failures
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed on field '%s': %s", e.Field, e.Reason)
}

// TaskInfo is an exported immutable snapshot of a task's information
type TaskInfo struct {
	ID        string
	Title     string
	FlightID  string
	Priority  Priority
	Status    TaskStatus
	CreatedAt string
	Err       string
}
