package task

import (
	"fmt"
	"sync"
	"time"

	"GoRunway/models"
)

type Task struct {
	mu sync.RWMutex

	id       string
	title    string
	flightID string
	priority models.Priority
	status   models.TaskStatus
	action   models.Action

	createdAt   time.Time
	startedAt   time.Time
	completedAt time.Time

	err error
}

func NewTask(id, title, flightID string, priority models.Priority, action models.Action) (*Task, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: %w", models.ErrInvalidTaskID, &models.ValidationError{
			Field:  "id",
			Reason: "task id cannot be empty",
		})
	}
	if title == "" {
		return nil, fmt.Errorf("%w: %w", models.ErrEmptyTitle, &models.ValidationError{
			Field:  "title",
			Reason: "task title cannot be empty",
		})
	}
	if flightID == "" {
		return nil, fmt.Errorf("%w: %w", models.ErrEmptyFlightID, &models.ValidationError{
			Field:  "flightID",
			Reason: "flight id cannot be empty",
		})
	}
	if priority < models.PriorityLow || priority > models.PriorityCritical {
		return nil, fmt.Errorf("%w: %w", models.ErrInvalidPriority, &models.ValidationError{
			Field:  "priority",
			Reason: "priority must be between PriorityLow and PriorityCritical",
		})
	}
	if action == nil {
		return nil, fmt.Errorf("%w: %w", models.ErrNilAction, &models.ValidationError{
			Field:  "action",
			Reason: "task action cannot be nil",
		})
	}

	return &Task{
		id:        id,
		title:     title,
		flightID:  flightID,
		priority:  priority,
		status:    models.StatusPending,
		action:    action,
		createdAt: time.Now(),
	}, nil
}

func (t *Task) ID() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.id
}

func (t *Task) Title() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.title
}

func (t *Task) FlightID() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.flightID
}

func (t *Task) Priority() models.Priority {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.priority
}

func (t *Task) Status() models.TaskStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status
}

func (t *Task) Error() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.err
}

func (t *Task) Cancel() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.status != models.StatusPending {
		return fmt.Errorf("cannot cancel task in status %s: %w", t.status, models.ErrTaskCannotCancel)
	}

	t.status = models.StatusCancelled
	t.completedAt = time.Now()
	return nil
}

func (t *Task) Reset() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.status == models.StatusRunning {
		return fmt.Errorf("cannot reset running task")
	}

	t.status = models.StatusPending
	t.startedAt = time.Time{}
	t.completedAt = time.Time{}
	t.err = nil
	return nil
}

func (t *Task) Execute() error {
	t.mu.Lock()
	if t.status != models.StatusPending {
		t.mu.Unlock()
		return models.ErrTaskNotPending
	}
	t.status = models.StatusRunning
	t.startedAt = time.Now()
	action := t.action
	t.mu.Unlock()

	var runErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				runErr = fmt.Errorf("task panicked: %v", r)
			}
		}()
		runErr = action()
	}()

	t.mu.Lock()
	defer t.mu.Unlock()
	t.completedAt = time.Now()
	if runErr != nil {
		t.status = models.StatusFailed
		t.err = runErr
		return runErr
	}
	t.status = models.StatusCompleted
	return nil
}

func (t *Task) GetInfo() models.TaskInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	errStr := ""
	if t.err != nil {
		errStr = t.err.Error()
	}

	return models.TaskInfo{
		ID:        t.id,
		Title:     t.title,
		FlightID:  t.flightID,
		Priority:  t.priority,
		Status:    t.status,
		CreatedAt: t.createdAt.Format(time.RFC3339),
		Err:       errStr,
	}
}
