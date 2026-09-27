package main_test

import (
	"sync/atomic"
	"testing"
	"time"

	"GoRunway/engine"
	"GoRunway/models"
	"GoRunway/pool"
	"GoRunway/repository"
	"GoRunway/task"
)

func TestSample_SchedulerDelayedExecution(t *testing.T) {
	repo := repository.NewTaskRepository()
	wp, _ := pool.NewWorkerPool(2, 10)
	sched, err := engine.NewScheduler(repo, wp)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	if err := sched.Start(); err != nil {
		t.Fatalf("unexpected error starting scheduler: %v", err)
	}
	defer sched.Stop()

	var executed atomic.Bool
	act := func() error {
		executed.Store(true)
		return nil
	}

	tsk, _ := task.NewTask("T-DELAY", "Delayed De-icing", "FL-555", models.PriorityHigh, act)
	_ = repo.Save(tsk)

	if err := sched.ScheduleDelayed("T-DELAY", 30*time.Millisecond); err != nil {
		t.Fatalf("failed to schedule delayed task: %v", err)
	}

	time.Sleep(70 * time.Millisecond)

	if !executed.Load() {
		t.Errorf("expected delayed task to execute after timeout")
	}

	report := sched.GetFlightReport("FL-555")
	if report.CompletedTasks != 1 {
		t.Errorf("expected 1 completed task in report, got %d", report.CompletedTasks)
	}
}
