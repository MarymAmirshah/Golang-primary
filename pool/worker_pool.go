package pool

import (
	"errors"
	"fmt"
	"sync"

	"GoRunway/task"
)

var (
	ErrPoolStopped        = errors.New("worker pool is stopped")
	ErrPoolAlreadyStarted = errors.New("worker pool is already running")
	ErrQueueFull          = errors.New("task queue is full")
	ErrInvalidWorkerCount = errors.New("worker count must be greater than zero")
	ErrInvalidCapacity    = errors.New("queue capacity must be greater than zero")
	ErrNilTask            = errors.New("cannot submit nil task")
)

type WorkerPool struct {
	mu sync.RWMutex

	workerCount int
	taskQueue   chan *task.Task
	isRunning   bool
	wg          sync.WaitGroup
}

func NewWorkerPool(workerCount, queueCapacity int) (*WorkerPool, error) {
	if workerCount <= 0 {
		return nil, fmt.Errorf("%w: %d", ErrInvalidWorkerCount, workerCount)
	}
	if queueCapacity <= 0 {
		return nil, fmt.Errorf("%w: %d", ErrInvalidCapacity, queueCapacity)
	}

	return &WorkerPool{
		workerCount: workerCount,
		taskQueue:   make(chan *task.Task, queueCapacity),
	}, nil
}

func (p *WorkerPool) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isRunning {
		return ErrPoolAlreadyStarted
	}

	p.isRunning = true
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return nil
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for t := range p.taskQueue {
		t.Execute()
	}
}

func (p *WorkerPool) Submit(t *task.Task) error {
	if t == nil {
		return ErrNilTask
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.isRunning {
		return ErrPoolStopped
	}

	select {
	case p.taskQueue <- t:
		return nil
	default:
		return ErrQueueFull
	}
}

func (p *WorkerPool) Stop() error {
	p.mu.Lock()
	if !p.isRunning {
		p.mu.Unlock()
		return ErrPoolStopped
	}
	p.isRunning = false
	close(p.taskQueue)
	p.mu.Unlock()

	p.wg.Wait()
	return nil
}

func (p *WorkerPool) WorkerCount() int {
	return p.workerCount
}

func (p *WorkerPool) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isRunning
}
