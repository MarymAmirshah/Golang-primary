package engine

import (
	"errors"
	"sync"
	"time"

	"GoRunway/models"
	"GoRunway/pool"
	"GoRunway/repository"
)

var (
	ErrSchedulerStopped        = errors.New("scheduler is stopped")
	ErrInvalidDelay            = errors.New("delay must be positive")
	ErrInvalidInterval         = errors.New("interval must be positive")
	ErrNilRepository           = errors.New("repository cannot be nil")
	ErrNilWorkerPool           = errors.New("worker pool cannot be nil")
	ErrSchedulerAlreadyRunning = errors.New("scheduler is already running")
)

// FlightReport summarizes the execution statistics of all tasks for a given flight
type FlightReport struct {
	FlightID       string
	TotalTasks     int
	CompletedTasks int
	FailedTasks    int
	CancelledTasks int
	PendingTasks   int
	Errors         []string
}

type Scheduler struct {
	mu sync.RWMutex

	repo *repository.TaskRepository
	pool *pool.WorkerPool

	isRunning bool
	stopChan  chan struct{}
	wg        sync.WaitGroup
}

func NewScheduler(repo *repository.TaskRepository, wp *pool.WorkerPool) (*Scheduler, error) {
	if repo == nil {
		return nil, ErrNilRepository
	}
	if wp == nil {
		return nil, ErrNilWorkerPool
	}

	return &Scheduler{
		repo:     repo,
		pool:     wp,
		stopChan: make(chan struct{}),
	}, nil
}

func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isRunning {
		return ErrSchedulerAlreadyRunning
	}

	if !s.pool.IsRunning() {
		if err := s.pool.Start(); err != nil {
			return err
		}
	}

	s.isRunning = true
	return nil
}

func (s *Scheduler) ScheduleNow(taskID string) error {
	s.mu.RLock()
	running := s.isRunning
	s.mu.RUnlock()

	if !running {
		return ErrSchedulerStopped
	}

	t, err := s.repo.FindByID(taskID)
	if err != nil {
		return err
	}

	return s.pool.Submit(t)
}

func (s *Scheduler) ScheduleDelayed(taskID string, delay time.Duration) error {
	if delay <= 0 {
		return ErrInvalidDelay
	}

	s.mu.RLock()
	running := s.isRunning
	s.mu.RUnlock()

	if !running {
		return ErrSchedulerStopped
	}

	t, err := s.repo.FindByID(taskID)
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		timer := time.NewTimer(delay)
		select {
		case <-s.stopChan:
			timer.Stop()
			return
		case <-timer.C:
			s.pool.Submit(t)
		}
	}()

	return nil
}

func (s *Scheduler) SchedulePeriodic(taskID string, interval time.Duration, stopRecurring <-chan struct{}) error {
	if interval <= 0 {
		return ErrInvalidInterval
	}

	s.mu.RLock()
	running := s.isRunning
	s.mu.RUnlock()

	if !running {
		return ErrSchedulerStopped
	}

	t, err := s.repo.FindByID(taskID)
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-s.stopChan:
				return
			case <-stopRecurring:
				return
			case <-ticker.C:
				t.Reset()
				s.pool.Submit(t)
			}
		}
	}()

	return nil
}

func (s *Scheduler) GetFlightReport(flightID string) FlightReport {
	tasks := s.repo.FindByFlightID(flightID)

	report := FlightReport{
		FlightID:   flightID,
		TotalTasks: len(tasks),
		Errors:     make([]string, 0),
	}

	for _, t := range tasks {
		switch t.Status() {
		case models.StatusCompleted:
			report.CompletedTasks++
		case models.StatusFailed:
			report.FailedTasks++
			if err := t.Error(); err != nil {
				report.Errors = append(report.Errors, err.Error())
			}
		case models.StatusCancelled:
			report.CancelledTasks++
		case models.StatusPending, models.StatusRunning:
			report.PendingTasks++
		}
	}

	return report
}

func (s *Scheduler) Stop() error {
	s.mu.Lock()
	if !s.isRunning {
		s.mu.Unlock()
		return ErrSchedulerStopped
	}
	s.isRunning = false
	close(s.stopChan)
	s.mu.Unlock()

	s.wg.Wait()

	return s.pool.Stop()
}
