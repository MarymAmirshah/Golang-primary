package repository

import (
	"errors"
	"fmt"
	"sync"

	"GoRunway/models"
	"GoRunway/task"
)

var (
	ErrTaskNotFound    = errors.New("task not found")
	ErrDuplicateTaskID = errors.New("task with this ID already exists")
	ErrNilTask         = errors.New("cannot save nil task")
)

type TaskRepository struct {
	mu    sync.RWMutex
	tasks map[string]*task.Task
}

func NewTaskRepository() *TaskRepository {
	return &TaskRepository{
		tasks: make(map[string]*task.Task),
	}
}

func (r *TaskRepository) Save(t *task.Task) error {
	if t == nil {
		return ErrNilTask
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tasks[t.ID()]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateTaskID, t.ID())
	}

	r.tasks[t.ID()] = t
	return nil
}

func (r *TaskRepository) FindByID(id string) (*task.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, exists := r.tasks[id]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, id)
	}
	return t, nil
}

func (r *TaskRepository) FindAll() []*task.Task {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*task.Task, 0, len(r.tasks))
	for _, t := range r.tasks {
		result = append(result, t)
	}
	return result
}

func (r *TaskRepository) FindByFlightID(flightID string) []*task.Task {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*task.Task, 0)
	for _, t := range r.tasks {
		if t.FlightID() == flightID {
			result = append(result, t)
		}
	}
	return result
}

func (r *TaskRepository) FindByStatus(status models.TaskStatus) []*task.Task {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*task.Task, 0)
	for _, t := range r.tasks {
		if t.Status() == status {
			result = append(result, t)
		}
	}
	return result
}

func (r *TaskRepository) FindByPriority(priority models.Priority) []*task.Task {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*task.Task, 0)
	for _, t := range r.tasks {
		if t.Priority() == priority {
			result = append(result, t)
		}
	}
	return result
}

func (r *TaskRepository) Delete(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tasks[id]; !exists {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, id)
	}

	delete(r.tasks, id)
	return nil
}

func (r *TaskRepository) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.tasks)
}

func (r *TaskRepository) CountByStatus(status models.TaskStatus) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, t := range r.tasks {
		if t.Status() == status {
			count++
		}
	}
	return count
}
