package main

import (
	"fmt"
	"log"
	"time"

	"GoRunway/engine"
	"GoRunway/models"
	"GoRunway/pool"
	"GoRunway/repository"
	"GoRunway/task"
)

func main() {
	repo := repository.NewTaskRepository()

	wp, err := pool.NewWorkerPool(4, 20)
	if err != nil {
		log.Fatalf("failed to create worker pool: %v", err)
	}

	sched, err := engine.NewScheduler(repo, wp)
	if err != nil {
		log.Fatalf("failed to create scheduler: %v", err)
	}

	if err := sched.Start(); err != nil {
		log.Fatalf("failed to start scheduler: %v", err)
	}
	defer func() {
		if err := sched.Stop(); err != nil {
			log.Printf("error stopping scheduler: %v", err)
		}
	}()

	const flightID = "IR-777"

	// Immediate mission: emergency baggage unload.
	unload, _ := task.NewTask("T-UNLOAD", "Emergency Baggage Unload", flightID, models.PriorityCritical, func() error {
		fmt.Println("Unloading baggage...")
		return nil
	})
	_ = repo.Save(unload)
	if err := sched.ScheduleNow("T-UNLOAD"); err != nil {
		log.Printf("failed to schedule immediate task: %v", err)
	}

	// Delayed mission: de-icing must start exactly 150ms before departure.
	deicing, _ := task.NewTask("T-DEICE", "Wing De-icing", flightID, models.PriorityHigh, func() error {
		fmt.Println("De-icing wings...")
		return nil
	})
	_ = repo.Save(deicing)
	if err := sched.ScheduleDelayed("T-DEICE", 150*time.Millisecond); err != nil {
		log.Printf("failed to schedule delayed task: %v", err)
	}

	// Periodic mission: cabin pressure monitoring, every 50ms until stopped.
	monitor, _ := task.NewTask("T-MONITOR", "Cabin Pressure Monitoring", flightID, models.PriorityMedium, func() error {
		fmt.Println("Checking cabin pressure...")
		return nil
	})
	_ = repo.Save(monitor)

	stopMonitoring := make(chan struct{})
	if err := sched.SchedulePeriodic("T-MONITOR", 50*time.Millisecond, stopMonitoring); err != nil {
		log.Printf("failed to schedule periodic task: %v", err)
	}

	// Let the ground operations run for a while.
	time.Sleep(250 * time.Millisecond)
	close(stopMonitoring)

	// Give in-flight tasks a moment to settle before reporting.
	time.Sleep(50 * time.Millisecond)

	report := sched.GetFlightReport(flightID)
	fmt.Printf("Flight Turnaround Report: %+v\n", report)
}
