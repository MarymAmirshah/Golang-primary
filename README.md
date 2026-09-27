# GoRunway ✈️

A concurrent, in-memory **airport ground-operations engine** written in idiomatic Go — built as a capstone project to practice safe concurrency patterns (`sync.RWMutex`, worker pools, timers/tickers, graceful shutdown) on top of a realistic domain: coordinating ground-support missions (refueling, de-icing, baggage handling, pushback...) during an aircraft's turnaround time.

The project is structured as four cumulative stages, each building directly on the previous one's code:

| Stage | Package | What it adds |
|---|---|---|
| 1 | [`task`](./task) | The `Task` domain model — validation, structured errors, thread-safe state, panic-safe execution |
| 2 | [`repository`](./repository) | An in-memory, concurrency-safe `TaskRepository` (store, find, filter, count) |
| 3 | [`pool`](./pool) | A `WorkerPool` — bounded worker goroutines consuming from a buffered channel |
| 4 | [`engine`](./engine) | A `Scheduler` — immediate / delayed / periodic dispatch, flight reports, graceful shutdown |

## Architecture

```
                       ┌──────────────┐
   ScheduleNow ───────►│              │
   ScheduleDelayed ───►│  Scheduler   │──Submit──► ┌──────────────┐
   SchedulePeriodic ──►│  (engine)    │            │  WorkerPool  │──► Task.Execute()
                       └──────┬───────┘            │   (pool)     │      (N goroutines)
                              │                     └──────────────┘
                       FindByID / FindByFlightID
                              │
                       ┌──────▼───────┐
                       │ TaskRepository│
                       │ (repository)  │
                       └──────┬───────┘
                              │
                       ┌──────▼───────┐
                       │     Task      │
                       │    (task)     │
                       └──────────────┘
```

- **`Task`** owns its own `sync.RWMutex` and is safe to read/execute from any number of goroutines concurrently. `Execute()` recovers from panics inside the mission's `Action` and turns them into a `FAILED` status with a descriptive error instead of crashing a worker.
- **`TaskRepository`** is a `map[string]*Task` guarded by an `RWMutex`; all filter methods (`FindByFlightID`, `FindByStatus`, `FindByPriority`) return fresh slices, so callers can never race on the repository's internal map.
- **`WorkerPool`** is a fixed number of goroutines ranging over a buffered channel. `Submit` is **non-blocking** (`select`/`default`) — if the queue is full, it fails fast with `ErrQueueFull` instead of blocking the caller.
- **`Scheduler`** ties the repository and pool together and adds the time dimension: fire now, fire after a delay (`time.Timer`), or fire repeatedly on an interval (`time.Ticker`) until stopped — with every scheduling goroutine also listening for shutdown so `Stop()` cancels pending work instead of leaking goroutines.

## Getting started

Requires **Go 1.26+**.

```bash
go build ./...
go run .
```

Sample output:

```
Unloading baggage...
Checking cabin pressure...
De-icing wings...
Checking cabin pressure...
Flight Turnaround Report: {FlightID:IR-777 TotalTasks:3 CompletedTasks:3 FailedTasks:0 CancelledTasks:0 PendingTasks:0 Errors:[]}
```

### Running the tests

```bash
go test ./... -v -race
```

All packages are covered by unit tests exercising the happy path, validation failures, and concurrency (spawning dozens of goroutines against a shared repository/pool/scheduler under `-race`).

## Quick tour of the API

```go
repo := repository.NewTaskRepository()
wp, _ := pool.NewWorkerPool(4, 20)          // 4 workers, 20-slot queue
sched, _ := engine.NewScheduler(repo, wp)

_ = sched.Start()
defer sched.Stop()                          // graceful shutdown: drains in-flight work,
                                             // cancels pending timers/tickers

t, _ := task.NewTask("T-1", "Fueling", "IR-777", models.PriorityHigh, func() error {
    // ... do the actual ground-support work ...
    return nil
})
_ = repo.Save(t)

_ = sched.ScheduleDelayed("T-1", 100*time.Millisecond) // fire once, after a delay
// or: sched.ScheduleNow("T-1")                         // fire immediately
// or: sched.SchedulePeriodic("T-1", time.Second, stop) // fire repeatedly until `stop` closes

report := sched.GetFlightReport("IR-777")
fmt.Printf("%+v\n", report)
```

## Design notes

A few deliberate choices worth calling out for anyone reading the code:

- **Dual error wrapping.** `NewTask`'s validation errors need to satisfy both `errors.Is(err, models.ErrXxx)` (a sentinel check) *and* `errors.As(err, &validationErr)` (a structured-error check) on the very same returned error. This is done with Go 1.20+'s multi-`%w` support:
  ```go
  fmt.Errorf("%w: %w", models.ErrInvalidTaskID, &models.ValidationError{Field: "id", Reason: "..."})
  ```
  One error value, two independently-traceable causes — no extra fields needed on `ValidationError` itself.

- **Lock discipline.** `Task.Execute()` releases its write lock *while the mission's `Action` runs* and only re-acquires it to record the outcome — so a slow or misbehaving action never blocks concurrent `Status()`/`GetInfo()` reads, and can't deadlock against a re-entrant call into the same task.

- **Non-blocking submission.** `WorkerPool.Submit` never blocks the caller; a full queue is reported as `ErrQueueFull` immediately via `select { ... default: }`.

- **Cancellable scheduling.** Every `ScheduleDelayed`/`SchedulePeriodic` goroutine is registered on the scheduler's `sync.WaitGroup` and races its timer/ticker against `stopChan`, so `Scheduler.Stop()` deterministically waits for and cancels *all* outstanding scheduled work before stopping the underlying pool — no goroutine leaks, no orphaned timers.

## Project structure

```
GoRunway/
├── models/         # Shared types: TaskStatus, Priority, sentinel errors, ValidationError
├── task/           # Task domain model + lifecycle (NewTask, Execute, Cancel, Reset, GetInfo)
├── repository/      # TaskRepository — thread-safe in-memory store
├── pool/           # WorkerPool — bounded concurrent task execution
├── engine/         # Scheduler — immediate/delayed/periodic dispatch + flight reports
└── main.go         # End-to-end demo wiring everything together
```

## License

Personal / educational project — no license specified yet.
