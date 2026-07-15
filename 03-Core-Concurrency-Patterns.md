# Part 3: Core Concurrency Patterns — Worker Pools, Pipelines, Fan-In, Fan-Out, Producer-Consumer

> Go Concurrency Interview Guide — From Beginner to Staff Engineer

---

## Table of Contents

1. [Worker Pools](#1-worker-pools)
2. [Pipelines](#2-pipelines)
3. [Fan-In Pattern](#3-fan-in-pattern)
4. [Fan-Out Pattern](#4-fan-out-pattern)
5. [Producer-Consumer Pattern](#5-producer-consumer-pattern)

---

# 1. WORKER POOLS

## Concept Overview

A worker pool is a fixed set of goroutines that process tasks from a shared channel. Instead of launching a goroutine per task (unbounded), a pool limits concurrency.

**Architecture:**
```
[Task Producer] ---> [Task Channel (queue)] ---> [Worker 1]
                                             |-> [Worker 2]  ---> [Result Channel]
                                             |-> [Worker N]
```

**Why worker pools over goroutine-per-task:**
- **Resource control:** Limits concurrent DB connections, HTTP requests, file handles.
- **Backpressure:** When all workers are busy, producers block on a full task channel.
- **Predictable memory:** Fixed number of goroutines vs. potentially millions.
- **Better CPU cache behavior:** Fewer goroutines means less context switching.

**When goroutine-per-task is fine:**
- Tasks are CPU-bound and count ≈ GOMAXPROCS.
- Tasks are very short-lived.
- You use `errgroup.SetLimit(N)` for bounded concurrency without a full pool.

## Why Interviewers Ask It

- Most common concurrency pattern in production Go services.
- Tests understanding of bounded concurrency, resource management.
- Reveals ability to handle graceful shutdown, error handling, monitoring.
- Gateway to more complex patterns (pipelines, fan-out).

## Common Interview Questions

### Q1: Why would you use a worker pool instead of launching a goroutine per task?

**Answer:** A worker pool bounds the number of concurrently executing goroutines, which controls resource consumption such as memory, file descriptors, database connections, and outbound HTTP sockets. Launching a goroutine per task is fine when the number of tasks is small or known, but in production systems that receive millions of requests, unbounded goroutines can exhaust memory (each goroutine starts at ~2-8 KB stack) and overwhelm downstream services. Worker pools provide natural backpressure: when all workers are busy, new tasks queue up in the channel, and producers block when the channel buffer is full. This prevents cascading failures in systems with finite downstream capacity.

**Example:**
```go
// Bad: unbounded goroutines — can spawn millions
for _, task := range tasks {
    go process(task) // no limit on concurrency
}

// Good: worker pool — bounded concurrency
jobs := make(chan Task, 100)
for i := 0; i < 10; i++ {
    go func() {
        for job := range jobs {
            process(job)
        }
    }()
}
for _, task := range tasks {
    jobs <- task
}
close(jobs)
```

**Real-time Scenario:** An API gateway receiving 50K requests/second needs to call a downstream payment service that supports only 200 concurrent connections. A worker pool of 200 prevents connection exhaustion and 503 errors from the payment provider.

---

### Q2: How do you choose the number of workers?

**Answer:** The optimal worker count depends on whether the work is CPU-bound or I/O-bound. For CPU-bound tasks (e.g., image encoding, hashing), set workers to `runtime.GOMAXPROCS(0)` since more workers would just compete for CPU time. For I/O-bound tasks (e.g., HTTP calls, database queries), the worker count should match the downstream system's capacity — for example, the max database connection pool size. A common approach is to make it configurable and tune based on load testing, starting with a reasonable default and monitoring throughput and latency.

**Example:**
```go
func optimalWorkers(ioBound bool) int {
    if ioBound {
        // Match downstream capacity; e.g., DB pool = 50
        return 50
    }
    // CPU-bound: match available CPUs
    return runtime.GOMAXPROCS(0)
}
```

**Real-time Scenario:** A video transcoding service uses `GOMAXPROCS` workers for CPU-intensive FFmpeg operations, while a web scraper uses 100 workers to match the rate limit of the target API (100 concurrent requests allowed).

---

### Q3: How do you handle worker failures/panics?

**Answer:** Each worker goroutine should include a `defer recover()` block to catch panics, preventing a single bad task from crashing the entire pool. When a panic is recovered, the worker should log the error with full stack trace, report a metric, and continue processing the next task from the channel. For non-panic errors, the worker should wrap the error with context and send it through the results channel using a `Result` struct that carries both the value and error. In critical systems, you may also want to restart the worker automatically.

**Example:**
```go
func safeWorker(id int, jobs <-chan Job, results chan<- Result) {
    for job := range jobs {
        func() {
            defer func() {
                if r := recover(); r != nil {
                    results <- Result{
                        JobID: job.ID,
                        Err:   fmt.Errorf("worker %d panicked: %v\n%s", id, r, debug.Stack()),
                    }
                }
            }()
            output := process(job)
            results <- Result{JobID: job.ID, Output: output}
        }()
    }
}
```

**Real-time Scenario:** In an email sending service, a malformed template causes a panic in the rendering library. Without panic recovery, the worker dies and the pool gradually loses capacity until all workers are dead, causing a full outage.

---

### Q4: How do you gracefully shut down a worker pool?

**Answer:** Graceful shutdown follows a three-step protocol: (1) stop sending new tasks by closing the jobs channel, (2) let workers drain remaining tasks from the channel via `for range`, and (3) wait for all workers to finish using `sync.WaitGroup`. For context-based cancellation, cancel the context to signal workers to stop, but still allow in-flight work to complete by checking `ctx.Done()` only between tasks, not mid-task. The results channel should be closed only after all workers have exited to prevent sends on a closed channel.

**Example:**
```go
func gracefulPool(ctx context.Context, jobs <-chan Job) <-chan Result {
    results := make(chan Result)
    var wg sync.WaitGroup

    for i := 0; i < 5; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for {
                select {
                case job, ok := <-jobs:
                    if !ok {
                        return // channel closed, drain complete
                    }
                    results <- process(job) // finish in-flight work
                case <-ctx.Done():
                    return // shutdown signal received
                }
            }
        }()
    }

    go func() {
        wg.Wait()
        close(results) // safe: all workers exited
    }()
    return results
}
```

**Real-time Scenario:** A Kubernetes pod receives SIGTERM and has 30 seconds before SIGKILL. The service cancels the context, stops accepting new HTTP requests, and the worker pool drains remaining jobs before the process exits cleanly.

---

### Q5: How do you implement dynamic worker pool sizing?

**Answer:** Dynamic sizing monitors queue depth and adjusts the number of workers at runtime. A scaler goroutine periodically checks the job channel length: if the queue is growing, it spawns new workers (up to a max); if workers are idle for too long, they self-terminate (down to a min). Each worker uses an idle timeout — if no job arrives within the timeout and the current count exceeds the minimum, the worker exits. An `atomic.Int32` tracks the active worker count to make scaling decisions without locks.

**Example:**
```go
func (p *Pool) autoScale() {
    ticker := time.NewTicker(500 * time.Millisecond)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            depth := len(p.jobs)
            active := int(p.activeWorkers.Load())
            if depth > active*2 && active < p.maxWorkers {
                p.spawnWorker() // scale up
            }
            // Workers self-terminate after idle timeout (scale down)
        case <-p.ctx.Done():
            return
        }
    }
}
```

**Real-time Scenario:** An e-commerce platform scales its order-processing pool from 10 workers to 100 during a flash sale, then back down to 10 when traffic subsides, saving memory and CPU for other services on the same node.

---

### Q6: What is the difference between a worker pool and a semaphore?

**Answer:** A worker pool maintains a fixed set of long-lived goroutines that pull work from a shared channel, while a semaphore (typically a buffered channel of `struct{}` or `golang.org/x/sync/semaphore`) limits the number of concurrently running goroutines but creates a new goroutine per task. Worker pools amortize goroutine creation cost and are better for long-running services with continuous work. Semaphores are simpler for bounding concurrency over a batch of tasks where goroutine creation overhead is negligible. `errgroup.SetLimit(N)` provides a semaphore-style API that's often sufficient.

**Example:**
```go
// Semaphore approach: new goroutine per task, bounded by N
sem := make(chan struct{}, 10)
for _, task := range tasks {
    sem <- struct{}{} // acquire
    go func(t Task) {
        defer func() { <-sem }() // release
        process(t)
    }(task)
}

// Worker pool approach: fixed goroutines, tasks via channel
jobs := make(chan Task, 100)
for i := 0; i < 10; i++ {
    go func() {
        for t := range jobs { process(t) }
    }()
}
```

**Real-time Scenario:** A CLI tool that processes 1000 files once uses a semaphore for simplicity, while a web server handling continuous requests for hours uses a worker pool to avoid repeated goroutine creation and teardown overhead.

---

### Q7: How do you handle backpressure in a worker pool?

**Answer:** Backpressure in a worker pool is handled primarily through a bounded (buffered) job channel. When all workers are busy and the channel buffer is full, producers block on the send operation, naturally slowing down the rate of incoming work. This prevents unbounded memory growth from queued tasks. The buffer size should be tuned based on acceptable latency: a larger buffer absorbs bursts but increases memory and latency for items sitting in the queue. For HTTP servers, backpressure manifests as increased response times or 503 responses when the queue is full.

**Example:**
```go
func poolWithBackpressure(bufferSize int) chan<- Job {
    jobs := make(chan Job, bufferSize) // bounded buffer

    for i := 0; i < 5; i++ {
        go func() {
            for job := range jobs {
                process(job) // slow processing blocks producers
            }
        }()
    }

    return jobs // producer blocks when buffer + workers are full
}

// Producer with timeout to avoid blocking forever
select {
case jobs <- job:
    // accepted
case <-time.After(5 * time.Second):
    // reject: system overloaded, return 503
}
```

**Real-time Scenario:** A log ingestion pipeline uses a buffered channel of 10,000 entries. During a traffic spike, producers (HTTP handlers) block when the buffer is full, causing response times to increase. A load balancer detects the latency spike and routes traffic to other instances.

---

### Q8: How do you implement priority in a worker pool?

**Answer:** Go channels don't support priority natively, so priority is implemented using multiple channels (one per priority level) with a select statement that favors higher-priority channels. Workers check the high-priority channel first using a nested select: try the high channel in a non-blocking select, and only fall through to the low channel if no high-priority work is available. Alternatively, you can use a `container/heap`-based priority queue protected by a mutex, but the multi-channel approach is more idiomatic in Go.

**Example:**
```go
func priorityWorker(high, low <-chan Job, results chan<- Result) {
    for {
        // Always prefer high-priority work
        select {
        case job := <-high:
            results <- process(job)
        default:
            // No high-priority work, accept either
            select {
            case job := <-high:
                results <- process(job)
            case job := <-low:
                results <- process(job)
            }
        }
    }
}
```

**Real-time Scenario:** A payment processing system uses a high-priority channel for refunds (customer-facing, SLA-bound) and a low-priority channel for reconciliation reports (background, can wait). During peak hours, refunds are always processed first.

---

### Q9: How do you monitor worker pool health?

**Answer:** Key metrics to monitor are: (1) **queue depth** — `len(jobs)` indicates how backed up the pool is; (2) **worker utilization** — ratio of busy workers to total workers; (3) **processing latency** — time from task enqueue to completion; (4) **throughput** — tasks processed per second; and (5) **error rate** — percentage of failed tasks. These metrics should be exported to a monitoring system like Prometheus. Alert on sustained high queue depth (workers can't keep up) or low utilization (over-provisioned).

**Example:**
```go
type PoolMetrics struct {
    QueueDepth    func() int     // len(jobs)
    ActiveWorkers atomic.Int32
    Processed     atomic.Int64
    Errors        atomic.Int64
}

func monitoredWorker(id int, jobs <-chan Job, m *PoolMetrics) {
    for job := range jobs {
        m.ActiveWorkers.Add(1)
        start := time.Now()

        err := processJob(job)

        m.ActiveWorkers.Add(-1)
        m.Processed.Add(1)
        if err != nil {
            m.Errors.Add(1)
        }
        histogram.Observe(time.Since(start).Seconds()) // Prometheus
    }
}
```

**Real-time Scenario:** An SRE team sets a PagerDuty alert when the worker pool queue depth exceeds 5,000 for more than 2 minutes, indicating the pool is undersized and tasks are being delayed beyond SLA thresholds.

---

### Q10: How do you handle slow/stuck workers?

**Answer:** Slow or stuck workers are handled by wrapping each task execution with a timeout using `context.WithTimeout`. If a task exceeds the timeout, the context is cancelled, and the worker moves on to the next task (the stuck operation should respect context cancellation). For truly stuck operations (e.g., blocking syscalls that ignore context), you can use a watchdog goroutine that logs and escalates if a worker hasn't reported progress within a deadline. In extreme cases, abandon the stuck goroutine and spawn a replacement worker.

**Example:**
```go
func workerWithTimeout(jobs <-chan Job, results chan<- Result, timeout time.Duration) {
    for job := range jobs {
        ctx, cancel := context.WithTimeout(context.Background(), timeout)
        resultCh := make(chan Result, 1)

        go func() {
            resultCh <- processWithCtx(ctx, job)
        }()

        select {
        case r := <-resultCh:
            results <- r
        case <-ctx.Done():
            results <- Result{JobID: job.ID, Err: fmt.Errorf("timed out after %v", timeout)}
        }
        cancel()
    }
}
```

**Real-time Scenario:** A microservice calls a third-party geocoding API that occasionally hangs for 60+ seconds. A 5-second timeout per task ensures one slow API response doesn't block the worker indefinitely, keeping the pool healthy for other requests.

---

### Q11: What is the relationship between GOMAXPROCS and worker pool size?

**Answer:** `GOMAXPROCS` sets the maximum number of OS threads that can execute Go code simultaneously — it determines parallelism at the runtime level. Worker pool size determines how many goroutines are concurrently processing tasks — it determines concurrency at the application level. For CPU-bound work, setting worker count greater than `GOMAXPROCS` provides no benefit because the extra workers just contend for CPU time on the same threads. For I/O-bound work, worker count can (and should) exceed `GOMAXPROCS` because blocked goroutines are descheduled, allowing other goroutines to run on the same OS thread.

**Example:**
```go
func chooseWorkers(taskType string) int {
    switch taskType {
    case "cpu": // hashing, compression, math
        return runtime.GOMAXPROCS(0) // typically == number of CPU cores
    case "io": // HTTP calls, DB queries, file I/O
        return runtime.GOMAXPROCS(0) * 10 // many more, since they block on I/O
    default:
        return runtime.GOMAXPROCS(0) * 2 // mixed workload
    }
}
```

**Real-time Scenario:** A data pipeline on a 4-core machine uses 4 workers for CPU-bound JSON parsing stages but 40 workers for I/O-bound stages that write results to S3, because most of those 40 goroutines are waiting on network I/O at any given time.

---

### Q12: How do you implement work stealing between worker pools?

**Answer:** Work stealing allows idle workers from one pool to "steal" tasks from another pool's queue, improving utilization when workloads are uneven. In Go, this is implemented by having each pool expose its job channel, and idle workers attempt a non-blocking receive from other pools' channels when their own queue is empty. The `select` statement with `default` enables non-blocking channel reads. This pattern is inspired by the Go runtime scheduler itself, which uses work stealing between OS thread queues (P's local run queues).

**Example:**
```go
func stealingWorker(primary, steal <-chan Job, results chan<- Result) {
    for {
        select {
        case job, ok := <-primary:
            if !ok {
                return
            }
            results <- process(job)
        default:
            // Primary queue empty, try stealing
            select {
            case job, ok := <-primary:
                if !ok {
                    return
                }
                results <- process(job)
            case job := <-steal:
                results <- process(job) // stolen work
            }
        }
    }
}
```

**Real-time Scenario:** A multi-tenant task system has separate pools for each tenant. During off-peak hours for Tenant A, its idle workers steal tasks from Tenant B's overloaded queue, improving overall cluster utilization without requiring explicit rebalancing.

---

## Coding Problems

### Problem 1: Basic Worker Pool

**Difficulty:** Easy-Medium  
**Problem:** Implement a worker pool with N workers that processes jobs and sends results.

```go
type Job struct {
    ID    int
    Data  string
}

type Result struct {
    JobID int
    Output string
    Err    error
}

func workerPool(ctx context.Context, numWorkers int, jobs <-chan Job) <-chan Result {
    results := make(chan Result, numWorkers)
    var wg sync.WaitGroup

    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            for {
                select {
                case job, ok := <-jobs:
                    if !ok {
                        return
                    }
                    result := process(job)
                    select {
                    case results <- result:
                    case <-ctx.Done():
                        return
                    }
                case <-ctx.Done():
                    return
                }
            }
        }(i)
    }

    go func() {
        wg.Wait()
        close(results)
    }()

    return results
}
```

**Variations:** Add error handling, per-worker metrics, panic recovery.  
**Real-world Relevance:** HTTP request processing, background job execution, image processing.

---

### Problem 2: Worker Pool with Rate Limiting

**Difficulty:** Medium  
**Problem:** Implement a worker pool that processes at most K tasks per second.

```go
func rateLimitedPool(ctx context.Context, workers, ratePerSec int, jobs <-chan Job) <-chan Result {
    results := make(chan Result)
    limiter := rate.NewLimiter(rate.Limit(ratePerSec), ratePerSec)

    var wg sync.WaitGroup
    for i := 0; i < workers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for job := range jobs {
                if err := limiter.Wait(ctx); err != nil {
                    return
                }
                select {
                case results <- process(job):
                case <-ctx.Done():
                    return
                }
            }
        }()
    }

    go func() {
        wg.Wait()
        close(results)
    }()
    return results
}
```

**Real-world Relevance:** API rate limits, database query throttling, external service calls.

---

### Problem 3: Auto-Scaling Worker Pool

**Difficulty:** Hard  
**Problem:** Implement a worker pool that scales workers based on queue depth.

```go
type AutoScalingPool struct {
    minWorkers  int
    maxWorkers  int
    jobs        chan Job
    results     chan Result
    activeCount atomic.Int32
    ctx         context.Context
    cancel      context.CancelFunc
    wg          sync.WaitGroup
}

func NewAutoScalingPool(min, max int) *AutoScalingPool {
    ctx, cancel := context.WithCancel(context.Background())
    p := &AutoScalingPool{
        minWorkers: min,
        maxWorkers: max,
        jobs:       make(chan Job, max*10),
        results:    make(chan Result, max*10),
        ctx:        ctx,
        cancel:     cancel,
    }
    // Start minimum workers
    for i := 0; i < min; i++ {
        p.addWorker()
    }
    // Start scaler
    go p.scaler()
    return p
}

func (p *AutoScalingPool) scaler() {
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            queueDepth := len(p.jobs)
            workers := int(p.activeCount.Load())
            if queueDepth > workers*2 && workers < p.maxWorkers {
                p.addWorker()
            }
            // Scale down logic: idle workers exit after timeout
        case <-p.ctx.Done():
            return
        }
    }
}

func (p *AutoScalingPool) addWorker() {
    p.wg.Add(1)
    p.activeCount.Add(1)
    go func() {
        defer p.wg.Done()
        defer p.activeCount.Add(-1)
        idleTimeout := time.NewTimer(30 * time.Second)
        defer idleTimeout.Stop()
        for {
            select {
            case job, ok := <-p.jobs:
                if !ok {
                    return
                }
                idleTimeout.Reset(30 * time.Second)
                p.results <- process(job)
            case <-idleTimeout.C:
                if int(p.activeCount.Load()) > p.minWorkers {
                    return // scale down
                }
                idleTimeout.Reset(30 * time.Second)
            case <-p.ctx.Done():
                return
            }
        }
    }()
}
```

**Real-world Relevance:** Kubernetes HPA, cloud function auto-scaling, adaptive HTTP servers.

---

### Problem 4: Worker Pool with Task Dependencies (DAG)

**Difficulty:** Hard (FAANG-level)  
**Problem:** Process tasks with dependencies. A task runs only after all its dependencies complete.

```go
type Task struct {
    ID       string
    Deps     []string
    Execute  func() error
}

func executeDAG(ctx context.Context, tasks []Task, workers int) error {
    // Build dependency graph
    taskMap := make(map[string]*Task)
    depCount := make(map[string]*atomic.Int32) // remaining dependency count
    dependents := make(map[string][]string)    // task -> tasks that depend on it

    for i := range tasks {
        t := &tasks[i]
        taskMap[t.ID] = t
        dc := &atomic.Int32{}
        dc.Store(int32(len(t.Deps)))
        depCount[t.ID] = dc
        for _, dep := range t.Deps {
            dependents[dep] = append(dependents[dep], t.ID)
        }
    }

    ready := make(chan string, len(tasks))
    errs := make(chan error, 1)

    // Seed with tasks that have no dependencies
    for id, dc := range depCount {
        if dc.Load() == 0 {
            ready <- id
        }
    }

    var completed atomic.Int32
    var wg sync.WaitGroup

    for i := 0; i < workers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for {
                select {
                case id := <-ready:
                    if err := taskMap[id].Execute(); err != nil {
                        select {
                        case errs <- err:
                        default:
                        }
                        return
                    }
                    completed.Add(1)
                    // Notify dependents
                    for _, dep := range dependents[id] {
                        if depCount[dep].Add(-1) == 0 {
                            ready <- dep
                        }
                    }
                case <-ctx.Done():
                    return
                }
            }
        }()
    }

    // Wait for all tasks or error
    go func() {
        for completed.Load() < int32(len(tasks)) {
            runtime.Gosched()
        }
        close(ready)
    }()
    wg.Wait()

    select {
    case err := <-errs:
        return err
    default:
        return nil
    }
}
```

**Real-world Relevance:** Build systems (Bazel, Make), CI/CD pipelines, workflow engines (Temporal, Airflow).

---

## Common Mistakes

| Mistake | Consequence | Fix |
|---------|-------------|-----|
| Not closing the jobs channel | Workers never exit | Producer closes jobs channel when done |
| Panic in worker crashes pool | Entire pool dies | `defer recover()` in each worker |
| No backpressure | OOM from unbounded queue | Use bounded task channel |
| Ignoring context cancellation | Workers run after shutdown | Check `ctx.Done()` in workers |
| Hardcoded worker count | Suboptimal for varying workloads | Make configurable or auto-scale |

---

# 2. PIPELINES

## Concept Overview

A pipeline is a series of stages connected by channels. Each stage is a group of goroutines that:
1. Receives values from an inbound channel.
2. Processes/transforms them.
3. Sends values to an outbound channel.

**Stage signature:**
```go
func stage(ctx context.Context, in <-chan T) <-chan U
```

**Pipeline composition:**
```go
out := stage3(ctx, stage2(ctx, stage1(ctx, source)))
```

**Key principles:**
- Each stage owns its outbound channel (creates and closes it).
- Stages are composable and independently testable.
- Cancellation propagates via context.
- Backpressure flows naturally upstream via channel blocking.

## Common Interview Questions

### Q1: What is a pipeline pattern in Go?

**Answer:** A pipeline is a series of stages connected by channels, where each stage is a goroutine (or group of goroutines) that receives values from an inbound channel, performs a transformation, and sends results to an outbound channel. Each stage owns its output channel — it creates it and closes it when done. Pipelines enable composable, concurrent data processing where all stages run simultaneously.

**Example:**
```go
func double(ctx context.Context, in <-chan int) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        for n := range in {
            select {
            case out <- n * 2:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}
```

**Real-time Scenario:** A log processing system uses a pipeline: read raw logs -> parse JSON -> enrich with geo-IP data -> write to Elasticsearch, processing millions of lines per minute.

---

### Q2: How do you handle errors in a pipeline?

**Answer:** Use a `Result[T]` wrapper type that carries either a value or an error. Each stage checks for upstream errors and propagates them without processing. For fatal errors that should halt the entire pipeline, cancel the shared context.

**Example:**
```go
type Result[T any] struct {
    Value T
    Err   error
}

func transform(ctx context.Context, in <-chan Result[int]) <-chan Result[string] {
    out := make(chan Result[string])
    go func() {
        defer close(out)
        for r := range in {
            if r.Err != nil {
                out <- Result[string]{Err: r.Err}
                continue
            }
            out <- Result[string]{Value: fmt.Sprintf("processed: %d", r.Value)}
        }
    }()
    return out
}
```

**Real-time Scenario:** An ETL pipeline processes 10 million records — malformed rows are tagged and forwarded to a dead-letter stage while valid records proceed to the database.

---

### Q3: How do you cancel a running pipeline?

**Answer:** Pass a shared `context.Context` to every stage. When you call `cancel()`, every stage's `select` picks up `<-ctx.Done()` and exits. Deferred `close(out)` ensures downstream stages terminate when they see the channel close, cascading shutdown through the entire pipeline.

**Example:**
```go
func stage(ctx context.Context, in <-chan int) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        for v := range in {
            select {
            case out <- process(v):
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}
```

**Real-time Scenario:** A user cancels a large data export via API, and the server cancels the context, shutting down all pipeline stages immediately without leaking goroutines.

---

### Q4: What is the benefit of lazy evaluation in pipelines?

**Answer:** Lazy evaluation means stages process items on-demand rather than materializing the entire dataset. Because channels block until a receiver is ready, each stage only produces the next value when downstream requests it. This enables processing of infinite streams with constant memory usage.

**Example:**
```go
func naturalNumbers(ctx context.Context) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        for i := 1; ; i++ {
            select {
            case out <- i:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}
```

**Real-time Scenario:** A search engine generates candidate results lazily — if enough high-quality matches fill page 1, downstream stages never process the remaining millions of candidates.

---

### Q5: How do you handle backpressure between stages?

**Answer:** Backpressure flows naturally upstream through unbuffered or bounded-buffer channels. When a slow downstream stage can't consume fast enough, its input channel fills up, causing the upstream stage to block on send. Monitoring `len(ch)` vs `cap(ch)` reveals bottleneck stages.

**Example:**
```go
func monitoredStage(ctx context.Context, name string, in <-chan int) <-chan int {
    out := make(chan int, 50)
    go func() {
        defer close(out)
        for v := range in {
            if len(out) > cap(out)*80/100 {
                log.Printf("WARN: %s buffer 80%% full", name)
            }
            select {
            case out <- v:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}
```

**Real-time Scenario:** A video processing pipeline with a fast decoder and slow ML model — backpressure from the ML stage throttles the decoder, preventing OOM.

---

### Q6: How do you add batching to a pipeline stage?

**Answer:** A batching stage accumulates items until either the batch reaches a target size or a flush timer expires. The dual trigger ensures low-volume periods don't leave items sitting indefinitely while high-volume periods flush at optimal batch sizes.

**Example:**
```go
func batch[T any](ctx context.Context, in <-chan T, size int, timeout time.Duration) <-chan []T {
    out := make(chan []T)
    go func() {
        defer close(out)
        buf := make([]T, 0, size)
        timer := time.NewTimer(timeout)
        defer timer.Stop()
        flush := func() {
            if len(buf) > 0 {
                out <- buf
                buf = make([]T, 0, size)
            }
            timer.Reset(timeout)
        }
        for {
            select {
            case v, ok := <-in:
                if !ok { flush(); return }
                buf = append(buf, v)
                if len(buf) >= size { flush() }
            case <-timer.C:
                flush()
            case <-ctx.Done():
                flush(); return
            }
        }
    }()
    return out
}
```

**Real-time Scenario:** A metrics pipeline batches data points into groups of 500 before sending to InfluxDB, reducing network round-trips from 10,000/sec to 20/sec.

---

### Q7: How do you test pipeline stages in isolation?

**Answer:** Since each stage has the signature `func(ctx, <-chan T) <-chan U`, test independently by creating a channel, sending known inputs, closing it, passing to the stage function, and collecting outputs via `for range`. Each stage is a pure channel transformer that can be tested without the rest of the pipeline.

**Example:**
```go
func TestSquareStage(t *testing.T) {
    ctx := context.Background()
    in := make(chan int, 3)
    in <- 2; in <- 3; in <- 4
    close(in)

    out := square(ctx, in)
    var results []int
    for v := range out {
        results = append(results, v)
    }

    expected := []int{4, 9, 16}
    if !reflect.DeepEqual(results, expected) {
        t.Errorf("got %v, want %v", results, expected)
    }
}
```

**Real-time Scenario:** A data team tests their CSV parsing stage with 50 edge cases in unit tests, catching bugs without spinning up the full pipeline or connecting to external systems.

---

### Q8: How do you measure throughput of each stage?

**Answer:** Wrap each stage with instrumentation that counts items processed using `atomic.Int64` counters, and periodically compute the rate (items/sec). Compare rates across stages to identify the bottleneck — the slowest stage determines overall pipeline throughput.

**Example:**
```go
func instrumentedStage[T any](ctx context.Context, name string, in <-chan T, process func(T) T) <-chan T {
    out := make(chan T)
    var count atomic.Int64
    go func() {
        ticker := time.NewTicker(1 * time.Second)
        defer ticker.Stop()
        var last int64
        for {
            select {
            case <-ticker.C:
                current := count.Load()
                log.Printf("[%s] %d items/sec", name, current-last)
                last = current
            case <-ctx.Done():
                return
            }
        }
    }()
    go func() {
        defer close(out)
        for v := range in {
            result := process(v)
            count.Add(1)
            out <- result
        }
    }()
    return out
}
```

**Real-time Scenario:** Per-stage metrics reveal the "enrich" stage processes only 500 items/sec vs 10,000 for others — the team fans out that stage to fix the bottleneck.

---

### Q9: What is the relationship between pipelines and generators?

**Answer:** A generator is the first stage of a pipeline — it produces values from a source (database, file, network) and sends them into a channel. Generators create data; subsequent stages transform it. In Go, generators return `<-chan T` and spawn an internal goroutine that feeds the channel.

**Example:**
```go
func generateFromDB(ctx context.Context, db *sql.DB, query string) <-chan Row {
    out := make(chan Row)
    go func() {
        defer close(out)
        rows, _ := db.QueryContext(ctx, query)
        defer rows.Close()
        for rows.Next() {
            var r Row
            rows.Scan(&r.ID, &r.Name)
            select {
            case out <- r:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}
```

**Real-time Scenario:** A report system uses generators to lazily stream rows from PostgreSQL, feeding them into a pipeline that computes aggregates and writes a PDF.

---

### Q10: How do you handle slow stages?

**Answer:** Fan out the slow stage by running multiple concurrent instances reading from the same input channel, then merge their outputs via fan-in. The pipeline's throughput becomes the slowest stage's throughput multiplied by its fan-out factor. Add buffering between stages to absorb bursts.

**Example:**
```go
func fanOutStage[T, U any](ctx context.Context, in <-chan T, process func(T) U, n int) <-chan U {
    workers := make([]<-chan U, n)
    for i := 0; i < n; i++ {
        workers[i] = singleStage(ctx, in, process)
    }
    return merge(ctx, workers...)
}
```

**Real-time Scenario:** An image pipeline fans out the OCR stage (200ms/image) to 40 workers, matching the resize stage's throughput and eliminating the bottleneck.

---

## Coding Problems

### Problem 1: Number Processing Pipeline

**Difficulty:** Easy-Medium  
**Problem:** Build: generate numbers → square them → filter evens → collect.

```go
func generate(ctx context.Context, nums ...int) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        for _, n := range nums {
            select {
            case out <- n:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}

func square(ctx context.Context, in <-chan int) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        for n := range in {
            select {
            case out <- n * n:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}

func filterEven(ctx context.Context, in <-chan int) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        for n := range in {
            if n%2 == 0 {
                select {
                case out <- n:
                case <-ctx.Done():
                    return
                }
            }
        }
    }()
    return out
}

// Usage: pipeline composition
func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Compose pipeline
    nums := generate(ctx, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    squared := square(ctx, nums)
    evens := filterEven(ctx, squared)

    for v := range evens {
        fmt.Println(v)
    }
}
```

**Key insight:** Each stage is independently testable. You can test `square()` by feeding it a channel directly.

---

### Problem 2: File Processing Pipeline with Error Handling

**Difficulty:** Medium  
**Problem:** Build: read file paths → read contents → count words → aggregate. Handle per-file errors.

```go
type Result[T any] struct {
    Value T
    Err   error
}

func readFiles(ctx context.Context, paths <-chan string) <-chan Result[[]byte] {
    out := make(chan Result[[]byte])
    go func() {
        defer close(out)
        for path := range paths {
            data, err := os.ReadFile(path)
            select {
            case out <- Result[[]byte]{Value: data, Err: err}:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}

func countWords(ctx context.Context, in <-chan Result[[]byte]) <-chan Result[int] {
    out := make(chan Result[int])
    go func() {
        defer close(out)
        for r := range in {
            if r.Err != nil {
                select {
                case out <- Result[int]{Err: r.Err}:
                case <-ctx.Done():
                    return
                }
                continue
            }
            count := len(strings.Fields(string(r.Value)))
            select {
            case out <- Result[int]{Value: count}:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}
```

**Pattern:** Use a `Result[T]` type carrying either value or error. Each stage propagates errors downstream without stopping the pipeline.

---

### Problem 3: Pipeline with Backpressure and Monitoring

**Difficulty:** Hard  
**Problem:** Build a multi-stage pipeline where each stage has configurable concurrency and reports throughput.

```go
type StageMetrics struct {
    Name       string
    Processed  atomic.Int64
    BufferUsed func() int
    BufferCap  func() int
}

func pipelineStage[In, Out any](
    ctx context.Context,
    name string,
    concurrency int,
    bufferSize int,
    in <-chan In,
    process func(In) Out,
) (<-chan Out, *StageMetrics) {
    out := make(chan Out, bufferSize)
    metrics := &StageMetrics{
        Name:       name,
        BufferUsed: func() int { return len(out) },
        BufferCap:  func() int { return cap(out) },
    }

    var wg sync.WaitGroup
    for i := 0; i < concurrency; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for {
                select {
                case item, ok := <-in:
                    if !ok {
                        return
                    }
                    result := process(item)
                    metrics.Processed.Add(1)
                    select {
                    case out <- result:
                    case <-ctx.Done():
                        return
                    }
                case <-ctx.Done():
                    return
                }
            }
        }()
    }

    go func() {
        wg.Wait()
        close(out)
    }()

    return out, metrics
}
```

**Real-world Relevance:** Kafka/RabbitMQ consumer pipelines, ETL systems, data ingestion.

---

### Problem 4: Streaming Pipeline with Time-Based Windowing

**Difficulty:** Hard (FAANG-level)  
**Problem:** Implement a streaming pipeline with tumbling time windows.

```go
type Event struct {
    Timestamp time.Time
    Value     float64
}

type WindowResult struct {
    WindowStart time.Time
    WindowEnd   time.Time
    Sum         float64
    Count       int
}

func tumblingWindow(ctx context.Context, in <-chan Event, windowSize time.Duration) <-chan WindowResult {
    out := make(chan WindowResult)
    go func() {
        defer close(out)
        ticker := time.NewTicker(windowSize)
        defer ticker.Stop()

        var sum float64
        var count int
        windowStart := time.Now().Truncate(windowSize)

        for {
            select {
            case event, ok := <-in:
                if !ok {
                    // Emit final window
                    if count > 0 {
                        out <- WindowResult{
                            WindowStart: windowStart,
                            WindowEnd:   windowStart.Add(windowSize),
                            Sum:         sum,
                            Count:       count,
                        }
                    }
                    return
                }
                sum += event.Value
                count++

            case t := <-ticker.C:
                if count > 0 {
                    out <- WindowResult{
                        WindowStart: windowStart,
                        WindowEnd:   t,
                        Sum:         sum,
                        Count:       count,
                    }
                }
                sum, count = 0, 0
                windowStart = t

            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}
```

**Variations:** Sliding windows, session windows, late event handling, watermarks.  
**Real-world Relevance:** Flink/Spark Streaming patterns, real-time analytics, monitoring aggregation.

---

# 3. FAN-IN PATTERN

## Concept Overview

Fan-in merges multiple input channels into a single output channel. It is the inverse of fan-out.

```
[Chan 1] --\
[Chan 2] ---+--> [Merged Output Channel]
[Chan N] --/
```

**Implementation approaches:**
1. **Goroutine-per-channel:** Launch a goroutine for each input channel, all writing to the same output. Use WaitGroup to close output when all inputs are done.
2. **reflect.Select:** Dynamic case set for runtime-determined channels. More complex but avoids goroutine overhead for many channels.

## Common Interview Questions

### Q1: What is the fan-in pattern?

**Answer:** Fan-in merges multiple input channels into a single output channel. It's the inverse of fan-out and essential when consolidating results from multiple producers into a single stream. A goroutine is launched per input channel, all writing to the shared output. The merged output is closed only after all inputs are closed and drained using a WaitGroup.

**Example:**
```go
func fanIn[T any](ctx context.Context, channels ...<-chan T) <-chan T {
    out := make(chan T)
    var wg sync.WaitGroup
    for _, ch := range channels {
        wg.Add(1)
        go func(c <-chan T) {
            defer wg.Done()
            for v := range c {
                select {
                case out <- v:
                case <-ctx.Done():
                    return
                }
            }
        }(ch)
    }
    go func() { wg.Wait(); close(out) }()
    return out
}
```

**Real-time Scenario:** A monitoring dashboard aggregates health checks from 50 microservices via fan-in into a single stream for display.

---

### Q2: How do you merge N channels into one?

**Answer:** Launch one goroutine per input channel, all writing to the same output channel. A `sync.WaitGroup` tracks goroutines; a separate goroutine waits on it and closes the output when all inputs are drained. This is the standard merge implementation.

**Example:**
```go
func merge[T any](ctx context.Context, chs ...<-chan T) <-chan T {
    merged := make(chan T)
    var wg sync.WaitGroup
    for _, ch := range chs {
        wg.Add(1)
        go func(c <-chan T) {
            defer wg.Done()
            for val := range c {
                select {
                case merged <- val:
                case <-ctx.Done():
                    return
                }
            }
        }(ch)
    }
    go func() { wg.Wait(); close(merged) }()
    return merged
}
```

**Real-time Scenario:** A web crawler has 20 goroutines crawling different domains — merge combines discovered URLs into a single channel for deduplication.

---

### Q3: How do you know when all input channels are closed?

**Answer:** Use `sync.WaitGroup` with one entry per input goroutine. Each goroutine calls `wg.Done()` when its input channel is closed (i.e., `for range` exits). A coordinator goroutine calls `wg.Wait()` and then closes the output channel.

**Example:**
```go
var wg sync.WaitGroup
for _, ch := range inputChannels {
    wg.Add(1)
    go func(c <-chan int) {
        defer wg.Done()
        for v := range c {
            out <- v
        }
    }(ch)
}
go func() {
    wg.Wait()
    close(out)
}()
```

**Real-time Scenario:** A batch system fans out to 8 workers; the aggregator waits for all to complete before computing the final summary report.

---

### Q4: Does fan-in preserve ordering?

**Answer:** No, fan-in is inherently non-deterministic. Values arrive in whatever order the Go scheduler runs the forwarding goroutines. If ordering matters, attach sequence numbers before fan-out and use a reorder buffer downstream after fan-in.

**Example:**
```go
// ch1: 1, 2, 3  |  ch2: A, B, C
// merged output could be: A,1,B,2,C,3 or 1,A,2,B,3,C — unpredictable
merged := merge(ctx, ch1, ch2)
```

**Real-time Scenario:** A search engine queries 5 shards in parallel and fans in results — a downstream sort stage is needed before returning ranked results to the user.

---

### Q5: How would you maintain order if needed?

**Answer:** Attach sequence numbers before fan-out, then use a reorder buffer after fan-in that holds items and emits them in sequence order. The buffer maps sequence numbers to values and flushes consecutive items starting from the expected next sequence.

**Example:**
```go
type Sequenced[T any] struct { Seq int; Value T }

func reorder[T any](in <-chan Sequenced[T]) <-chan T {
    out := make(chan T)
    go func() {
        defer close(out)
        buf := make(map[int]T)
        next := 0
        for item := range in {
            buf[item.Seq] = item.Value
            for {
                if v, ok := buf[next]; ok {
                    out <- v
                    delete(buf, next)
                    next++
                } else {
                    break
                }
            }
        }
    }()
    return out
}
```

**Real-time Scenario:** A PDF generator processes pages in parallel but assembles them in order using sequence numbers so the final document is correctly ordered.

---

### Q6: How do you handle errors in fan-in?

**Answer:** Use a `Result[T]` type wrapping value or error. Fan-in merges Result items identically; the consumer inspects each result and handles errors. For fail-fast behavior, use `errgroup.WithContext` so the first error cancels all other workers.

**Example:**
```go
type Result[T any] struct { Value T; Err error }

merged := merge(ctx, resultCh1, resultCh2, resultCh3)
for r := range merged {
    if r.Err != nil {
        log.Printf("error from upstream: %v", r.Err)
        continue
    }
    processValue(r.Value)
}
```

**Real-time Scenario:** A price comparison engine queries 10 vendors; if 2 fail, it still shows prices from the 8 successful ones rather than failing entirely.

---

### Q7: What is `reflect.Select` and when would you use it?

**Answer:** `reflect.Select` is a reflection-based select for dynamic (runtime-determined) channel sets. It accepts a slice of `reflect.SelectCase` structs. Use it when the number of channels is unknown at compile time. However, prefer goroutine-per-channel for typical cases due to reflection overhead.

**Example:**
```go
func dynamicMerge(channels []<-chan int) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        cases := make([]reflect.SelectCase, len(channels))
        for i, ch := range channels {
            cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
        }
        for len(cases) > 0 {
            chosen, val, ok := reflect.Select(cases)
            if !ok {
                cases = append(cases[:chosen], cases[chosen+1:]...)
                continue
            }
            out <- int(val.Int())
        }
    }()
    return out
}
```

**Real-time Scenario:** A plugin system with runtime-registered notification channels uses `reflect.Select` to listen on all channels without spawning a goroutine per plugin.

---

### Q8: Performance: goroutine-per-channel vs reflect.Select?

**Answer:** Goroutine-per-channel is faster for typical use cases (up to hundreds of channels) using efficient native select. `reflect.Select` is O(N) per selection with reflection overhead but uses less memory (single goroutine) for thousands of channels. Goroutine-per-channel is the standard choice unless you have 1000+ dynamic channels.

**Example:**
```go
// goroutine-per-channel: O(1) per receive, N goroutines (~2-8KB each)
// reflect.Select: O(N) per receive, 1 goroutine
// Benchmark results typically show:
//   100 channels: goroutine-per-channel ~3x faster
//   10,000 channels: reflect.Select uses ~10x less memory
// Rule of thumb: < 1000 channels → goroutine-per-channel
```

**Real-time Scenario:** A trading platform monitoring 500 stock tickers uses goroutine-per-channel for sub-microsecond merge latency.

---

## Coding Problems

### Problem 1: Basic Channel Merge

**Difficulty:** Easy  
**Problem:** `func merge(channels ...<-chan int) <-chan int`

```go
func merge[T any](ctx context.Context, channels ...<-chan T) <-chan T {
    out := make(chan T)
    var wg sync.WaitGroup

    for _, ch := range channels {
        wg.Add(1)
        go func(c <-chan T) {
            defer wg.Done()
            for {
                select {
                case v, ok := <-c:
                    if !ok {
                        return
                    }
                    select {
                    case out <- v:
                    case <-ctx.Done():
                        return
                    }
                case <-ctx.Done():
                    return
                }
            }
        }(ch)
    }

    go func() {
        wg.Wait()
        close(out)
    }()
    return out
}
```

**Real-world Relevance:** Log aggregation from multiple sources, event stream merging, multi-service responses.

---

### Problem 2: Ordered Fan-In

**Difficulty:** Medium-Hard  
**Problem:** Merge N channels maintaining a total order. Each channel produces items with a sequence number.

```go
type Sequenced[T any] struct {
    SeqNum int
    Value  T
}

func orderedMerge[T any](ctx context.Context, channels ...<-chan Sequenced[T]) <-chan T {
    out := make(chan T)
    go func() {
        defer close(out)
        nextSeq := 0
        buffer := make(map[int]T)

        merged := merge(ctx, channels...)

        for item := range merged {
            buffer[item.SeqNum] = item.Value
            // Emit in order
            for {
                if val, ok := buffer[nextSeq]; ok {
                    delete(buffer, nextSeq)
                    select {
                    case out <- val:
                    case <-ctx.Done():
                        return
                    }
                    nextSeq++
                } else {
                    break
                }
            }
        }
    }()
    return out
}
```

**Real-world Relevance:** Reassembling ordered results from parallel processing, TCP stream reassembly.

---

### Problem 3: Fan-In with Error Propagation and Short-Circuit

**Difficulty:** Medium  
**Problem:** Merge results from N channels. First error cancels all sources.

```go
func fanInWithErrors[T any](
    ctx context.Context,
    fns ...func(ctx context.Context) (T, error),
) ([]T, error) {
    g, ctx := errgroup.WithContext(ctx)
    results := make([]T, len(fns))

    for i, fn := range fns {
        i, fn := i, fn
        g.Go(func() error {
            val, err := fn(ctx)
            if err != nil {
                return err // cancels context, stops others
            }
            results[i] = val
            return nil
        })
    }

    if err := g.Wait(); err != nil {
        return nil, err
    }
    return results, nil
}
```

**Real-world Relevance:** Parallel API calls where first failure should abort all.

---

# 4. FAN-OUT PATTERN

## Concept Overview

Fan-out distributes work from a single source to multiple workers for concurrent processing.

```
                    /--> [Worker 1] --\
[Single Input] ----+---> [Worker 2] ---+--> [Collected Results]
                    \--> [Worker N] --/
```

**Fan-out strategies:**
- **Shared channel:** All workers read from the same channel (automatic load balancing).
- **Round-robin:** Explicitly assign tasks to workers in order.
- **Hash-based:** Route by key (ensures same key goes to same worker for ordering).
- **Broadcast:** Send each item to ALL workers (different from load-balancing fan-out).

## Common Interview Questions

### Q1: What is the fan-out pattern?

**Answer:** Fan-out distributes work from a single source channel to multiple concurrent workers for parallel processing. The Go channel runtime ensures each item is received by exactly one worker, providing automatic load balancing. Fan-out is typically paired with fan-in to merge results back into a single stream.

**Example:**
```go
func fanOut[T, U any](ctx context.Context, in <-chan T, process func(T) U, n int) []<-chan U {
    workers := make([]<-chan U, n)
    for i := 0; i < n; i++ {
        out := make(chan U)
        go func() {
            defer close(out)
            for v := range in {
                select { case out <- process(v): case <-ctx.Done(): return }
            }
        }()
        workers[i] = out
    }
    return workers
}
```

**Real-time Scenario:** A CI/CD system fans out 1,000 test files to 16 workers, reducing test time from 30 minutes to ~2 minutes.

---

### Q2: How does fan-out differ from a worker pool?

**Answer:** Fan-out describes the *pattern* of distributing work within a pipeline, while worker pool describes a *reusable implementation*. A worker pool is a specific way to implement fan-out with lifecycle management, monitoring, and graceful shutdown. Fan-out is conceptual; worker pool is concrete.

**Example:**
```go
// Fan-out: pattern within a pipeline
stage2 := fanOutFanIn(ctx, stage1Output, transform, 10)

// Worker pool: standalone reusable component
pool := NewWorkerPool(10)
pool.Start()
pool.Submit(job)
pool.Shutdown()
```

**Real-time Scenario:** A microservice uses a persistent worker pool for HTTP handlers (long-lived), while a migration script uses fan-out for one-time parallel processing.

---

### Q3: How do you control the degree of fan-out?

**Answer:** Set the worker count `n` based on workload type. For CPU-bound work, use `runtime.GOMAXPROCS(0)`. For I/O-bound, match downstream capacity (e.g., database connection pool size). Use `errgroup.SetLimit(n)` for simple bounded fan-out. Make it configurable via environment variables.

**Example:**
```go
func controlledFanOut(ctx context.Context, items []Task, maxWorkers int) error {
    g, ctx := errgroup.WithContext(ctx)
    g.SetLimit(maxWorkers)
    for _, item := range items {
        g.Go(func() error { return process(ctx, item) })
    }
    return g.Wait()
}
```

**Real-time Scenario:** A cloud storage migration tool uses 5 workers in staging but 50 in production to match different API rate limits.

---

### Q4: How do you distribute work evenly?

**Answer:** Use a shared input channel — all workers read from the same channel, and Go's runtime gives the next item to whichever worker is free first, providing natural load balancing. Alternatives include round-robin for even distribution by count and hash-based routing for key affinity.

**Example:**
```go
// Shared channel (recommended): automatic load balancing
jobs := make(chan Task, 100)
for i := 0; i < N; i++ { go worker(jobs) }

// Hash-based: same key always goes to same worker
workerChannels[hash(task.UserID) % N] <- task
```

**Real-time Scenario:** An order system routes by customer ID using hash-based distribution, ensuring per-customer ordering while maintaining parallelism.

---

### Q5: How do you handle stragglers in fan-out?

**Answer:** Use timeouts per task to abandon slow work, speculative execution (launch duplicate on another worker), or hedged requests (send to multiple backends, take the first response). Hedged requests are especially effective for reducing tail latency.

**Example:**
```go
func hedgedRequest(ctx context.Context, req Request) (Response, error) {
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()
    results := make(chan Response, 2)
    for _, backend := range []string{"primary", "secondary"} {
        go func(b string) {
            if resp, err := callBackend(ctx, b, req); err == nil {
                results <- resp
            }
        }(backend)
    }
    select {
    case resp := <-results: return resp, nil
    case <-ctx.Done(): return Response{}, ctx.Err()
    }
}
```

**Real-time Scenario:** Google Search fans out to hundreds of index servers; hedged requests to replicas keep p99 latency low when one server is slow.

---

### Q6: What are strategies for fan-out distribution?

**Answer:** (1) **Shared channel** — automatic load balancing, simplest. (2) **Round-robin** — even by count but ignores processing time. (3) **Hash-based** — key affinity for ordering guarantees. (4) **Broadcast** — send to ALL workers for replication or redundancy.

**Example:**
```go
// Shared channel
shared := make(chan Task)
for i := 0; i < N; i++ { go worker(shared) }

// Round-robin
for i, task := range tasks { workerChannels[i%N] <- task }

// Hash-based
workerChannels[hash(task.UserID) % N] <- task

// Broadcast
for _, ch := range workerChannels { ch <- task }
```

**Real-time Scenario:** A chat app uses hash-based distribution by room ID to ensure in-room message ordering while processing different rooms in parallel.

---

## Coding Problems

### Problem 1: Parallel Map with Fan-Out

**Difficulty:** Easy-Medium  
**Problem:** `func parallelMap[T, U any](items []T, fn func(T) U, workers int) []U`

```go
func parallelMap[T, U any](items []T, fn func(T) U, workers int) []U {
    results := make([]U, len(items))
    jobs := make(chan int, len(items)) // send indices

    var wg sync.WaitGroup
    for w := 0; w < workers; w++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for i := range jobs {
                results[i] = fn(items[i])
            }
        }()
    }

    for i := range items {
        jobs <- i
    }
    close(jobs)
    wg.Wait()
    return results
}
```

**Key insight:** Using indices avoids needing a mutex for results — each goroutine writes to a unique index.

---

### Problem 2: Bounded Fan-Out with Backpressure

**Difficulty:** Medium  
**Problem:** Fan-out with at most N concurrent operations, backpressure when all busy.

```go
func boundedFanOut[T, U any](
    ctx context.Context,
    items []T,
    process func(context.Context, T) (U, error),
    maxConcurrency int,
) ([]U, error) {
    g, ctx := errgroup.WithContext(ctx)
    g.SetLimit(maxConcurrency) // built-in bounded concurrency

    results := make([]U, len(items))
    for i, item := range items {
        i, item := i, item
        g.Go(func() error {
            res, err := process(ctx, item)
            if err != nil {
                return err
            }
            results[i] = res
            return nil
        })
    }

    if err := g.Wait(); err != nil {
        return nil, err
    }
    return results, nil
}
```

**Real-world Relevance:** API client concurrency control, batch processing with rate limits.

---

### Problem 3: Fan-Out/Fan-In with Order Preservation

**Difficulty:** Hard  
**Problem:** Fan out to N workers, fan results back in, maintaining output order matching input order.

```go
type indexed[T any] struct {
    index int
    value T
}

func orderedFanOutFanIn[T, U any](
    ctx context.Context,
    in <-chan T,
    process func(T) U,
    workers int,
) <-chan U {
    jobs := make(chan indexed[T])
    results := make(chan indexed[U], workers)
    out := make(chan U)

    // Fan-out: distribute indexed jobs to workers
    var workerWg sync.WaitGroup
    for i := 0; i < workers; i++ {
        workerWg.Add(1)
        go func() {
            defer workerWg.Done()
            for job := range jobs {
                result := process(job.value)
                select {
                case results <- indexed[U]{index: job.index, value: result}:
                case <-ctx.Done():
                    return
                }
            }
        }()
    }

    // Indexer: add indices to input
    go func() {
        idx := 0
        for v := range in {
            select {
            case jobs <- indexed[T]{index: idx, value: v}:
                idx++
            case <-ctx.Done():
                break
            }
        }
        close(jobs)
        workerWg.Wait()
        close(results)
    }()

    // Reorder: collect results and emit in order
    go func() {
        defer close(out)
        nextIdx := 0
        buffer := make(map[int]U)
        for r := range results {
            buffer[r.index] = r.value
            for {
                if v, ok := buffer[nextIdx]; ok {
                    delete(buffer, nextIdx)
                    select {
                    case out <- v:
                    case <-ctx.Done():
                        return
                    }
                    nextIdx++
                } else {
                    break
                }
            }
        }
    }()

    return out
}
```

**Real-world Relevance:** Parallel HTTP request processing preserving request order, ordered data transformation.

---

# 5. PRODUCER-CONSUMER PATTERN

## Concept Overview

Producers generate data and place it on a shared queue (channel). Consumers take data from the queue and process it. This decouples production from consumption.

**Configurations:**
- SPSC: Single Producer, Single Consumer
- SPMC: Single Producer, Multiple Consumers
- MPSC: Multiple Producers, Single Consumer
- MPMC: Multiple Producers, Multiple Consumers

**Graceful shutdown protocol:**
1. Producers finish producing and close the channel.
2. Consumers drain remaining items via `for range`.
3. When channel is closed and drained, consumers exit.

**Rule:** Only the **producer** should close the channel. If multiple producers, use a `sync.WaitGroup` and a coordinating goroutine.

## Common Interview Questions

### Q1: How do you implement producer-consumer in Go?

**Answer:** Use a buffered channel as the shared queue. Producers send, consumers receive. The channel provides thread-safety and backpressure. The producer closes the channel when done; consumers use `for range` to drain.

**Example:**
```go
func main() {
    queue := make(chan int, 10)
    go func() {
        for i := 0; i < 100; i++ { queue <- i }
        close(queue)
    }()
    for item := range queue { fmt.Println("consumed:", item) }
}
```

**Real-time Scenario:** A web server produces request payloads onto a channel; background consumers process them asynchronously.

---

### Q2: How do you handle multiple producers?

**Answer:** Coordinate channel closure using `sync.WaitGroup`. Each producer calls `wg.Done()` when finished. A coordinator goroutine waits on the WaitGroup then closes the channel exactly once.

**Example:**
```go
func multiProducer(ctx context.Context, n int) <-chan int {
    queue := make(chan int, 100)
    var wg sync.WaitGroup
    for i := 0; i < n; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for j := 0; j < 50; j++ {
                select { case queue <- id*1000 + j: case <-ctx.Done(): return }
            }
        }(i)
    }
    go func() { wg.Wait(); close(queue) }()
    return queue
}
```

**Real-time Scenario:** Five Kafka partition readers write to a shared channel; WaitGroup coordinates channel closure after all readers finish.

---

### Q3: How do you ensure graceful shutdown?

**Answer:** (1) Signal producers to stop via context cancellation, (2) producers finish and WaitGroup completes, (3) channel closes, (4) consumers drain via `for range`, (5) consumers' WaitGroup completes. This guarantees no data loss.

**Example:**
```go
func gracefulShutdown() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()
    queue := make(chan Task, 100)
    var prodWg, consWg sync.WaitGroup
    prodWg.Add(1)
    go func() { defer prodWg.Done(); for { select { case queue <- produceTask(): case <-ctx.Done(): return } } }()
    go func() { prodWg.Wait(); close(queue) }()
    consWg.Add(1)
    go func() { defer consWg.Done(); for task := range queue { processTask(task) } }()
    consWg.Wait()
}
```

**Real-time Scenario:** A Kubernetes pod stops accepting jobs on SIGTERM, drains remaining 47 jobs within the 30-second grace period, and exits with zero data loss.

---

### Q4: What is the poison pill pattern?

**Answer:** The poison pill uses a sentinel value to signal consumers to shut down. In Go, this is less idiomatic because closing the channel achieves the same effect more elegantly — `for range` exits naturally when the channel is closed.

**Example:**
```go
// Poison pill (less idiomatic)
const PoisonPill = -1
for i := 0; i < numConsumers; i++ { queue <- PoisonPill }

// Idiomatic Go: just close the channel
close(queue)
```

**Real-time Scenario:** A Java port initially used poison pills; after review, the team replaced it with channel closing, eliminating bugs from miscounted pills.

---

### Q5: How do you handle slow consumers?

**Answer:** (1) Add more consumers to increase throughput, (2) increase buffer size for burst absorption, (3) drop oldest/newest items for real-time data where staleness matters, (4) accept backpressure and let producers slow down. The right choice depends on whether data loss is acceptable.

**Example:**
```go
func produce(queue chan<- Event, event Event) {
    select {
    case queue <- event:
    default:
        <-queue          // drop oldest
        queue <- event
        droppedCounter.Add(1)
    }
}
```

**Real-time Scenario:** A video analytics pipeline drops old frames when ML inference falls behind — processing stale frames is useless for live detection.

---

### Q6: How do you handle slow producers?

**Answer:** Slow producers are generally fine — consumers just block waiting. Use `select` with a ticker for housekeeping during idle periods. Consider reducing consumer count to save resources when production rate is consistently low.

**Example:**
```go
func consumer(queue <-chan Task) {
    housekeeping := time.NewTicker(5 * time.Second)
    defer housekeeping.Stop()
    for {
        select {
        case task, ok := <-queue:
            if !ok { return }
            process(task)
        case <-housekeeping.C:
            compactCache()
            reportMetrics()
        }
    }
}
```

**Real-time Scenario:** A notification service is busy during business hours but idle at night — consumers use idle time for cache compaction and health reporting.

---

### Q7: How do you ensure exactly-once processing?

**Answer:** Combine at-least-once delivery (retries) with idempotent processing (deduplication). Track processed message IDs in a persistent store and skip duplicates. This is the only practical way to achieve exactly-once semantics in distributed systems.

**Example:**
```go
func exactlyOnceConsumer(queue <-chan Message, db *sql.DB) {
    for msg := range queue {
        var exists bool
        db.QueryRow("SELECT EXISTS(SELECT 1 FROM processed WHERE id=$1)", msg.ID).Scan(&exists)
        if exists { msg.Ack(); continue }
        tx, _ := db.Begin()
        processInTx(tx, msg)
        tx.Exec("INSERT INTO processed (id) VALUES ($1)", msg.ID)
        tx.Commit()
        msg.Ack()
    }
}
```

**Real-time Scenario:** A payment system prevents double-charging by checking the payment intent ID in the processed table before executing a charge.

---

### Q8: What is the difference between producer-consumer and worker pool?

**Answer:** Producer-consumer is a broader *design pattern* describing the full relationship between producers, a shared buffer, and consumers with lifecycle management. Worker pool is a *specific implementation* of the consumer side — a reusable pool of goroutines processing tasks from a queue.

**Example:**
```go
// Producer-consumer: full pattern
queue := make(chan Task, 100)
go producer(queue)
go consumer(queue)

// Worker pool: specific consumer implementation
pool := NewWorkerPool(10, queue)
pool.Start()
pool.Shutdown()
```

**Real-time Scenario:** A message broker integration has distinct producer code (Kafka reader) and consumer code (worker pool), with producer-consumer governing the overall architecture.

---

### Q9: How do you add monitoring?

**Answer:** Track queue depth (`len(ch)`), produce/consume rates via atomic counters, end-to-end latency by embedding timestamps in messages, and error rate. Export metrics via Prometheus and visualize with Grafana dashboards.

**Example:**
```go
type MonitoredQueue[T any] struct {
    ch       chan T
    produced atomic.Int64
    consumed atomic.Int64
}

func (q *MonitoredQueue[T]) Metrics() {
    queueDepthGauge.Set(float64(len(q.ch)))
    producedCounter.Add(float64(q.produced.Swap(0)))
}
```

**Real-time Scenario:** Operations monitors a Grafana dashboard showing queue depth; when it spikes to 5,000 on Black Friday, auto-scaling adds consumer pods.

---

### Q10: How do you handle producer errors?

**Answer:** Use a `Result[T]` type carrying value or error on the same channel, or use a separate error channel. Cancel the shared context for fatal errors. Never silently swallow producer errors.

**Example:**
```go
type Result[T any] struct { Value T; Err error }

func producer(ctx context.Context, out chan<- Result[Data]) {
    defer close(out)
    for {
        data, err := fetchFromSource()
        if err != nil {
            out <- Result[Data]{Err: err}
            continue
        }
        out <- Result[Data]{Value: data}
    }
}
```

**Real-time Scenario:** A database CDC producer sends corrupt binlog errors through the Result type so consumers can log and continue processing valid events.

---

## Coding Problems

### Problem 1: Multi-Producer Multi-Consumer Queue

**Difficulty:** Medium  
**Problem:** M producers, N consumers, shared bounded buffer. Track metrics.

```go
func producerConsumer(ctx context.Context, numProducers, numConsumers, bufferSize int) {
    queue := make(chan int, bufferSize)
    var produced, consumed atomic.Int64

    // Start producers
    var prodWg sync.WaitGroup
    for i := 0; i < numProducers; i++ {
        prodWg.Add(1)
        go func(id int) {
            defer prodWg.Done()
            for j := 0; j < 100; j++ {
                item := id*1000 + j
                select {
                case queue <- item:
                    produced.Add(1)
                case <-ctx.Done():
                    return
                }
            }
        }(i)
    }

    // Close queue when all producers finish
    go func() {
        prodWg.Wait()
        close(queue)
    }()

    // Start consumers
    var consWg sync.WaitGroup
    for i := 0; i < numConsumers; i++ {
        consWg.Add(1)
        go func(id int) {
            defer consWg.Done()
            for item := range queue {
                process(item)
                consumed.Add(1)
            }
        }(i)
    }

    consWg.Wait()
    fmt.Printf("Produced: %d, Consumed: %d\n", produced.Load(), consumed.Load())
}
```

---

### Problem 2: Batch Consumer

**Difficulty:** Medium-Hard  
**Problem:** Consumer batches items and processes in groups of N or every T duration (whichever comes first).

```go
func batchConsumer[T any](
    ctx context.Context,
    in <-chan T,
    batchSize int,
    flushInterval time.Duration,
    processBatch func([]T),
) {
    batch := make([]T, 0, batchSize)
    timer := time.NewTimer(flushInterval)
    defer timer.Stop()

    flush := func() {
        if len(batch) > 0 {
            processBatch(batch)
            batch = make([]T, 0, batchSize)
        }
        timer.Reset(flushInterval)
    }

    for {
        select {
        case item, ok := <-in:
            if !ok {
                flush() // final flush
                return
            }
            batch = append(batch, item)
            if len(batch) >= batchSize {
                flush()
            }

        case <-timer.C:
            flush()

        case <-ctx.Done():
            flush() // flush on cancellation
            return
        }
    }
}
```

**Real-world Relevance:** Log batching (Fluentd), database bulk inserts, metrics aggregation (StatsD), Kafka produce batching.

---

### Problem 3: Delivery Guarantees — At-Most-Once, At-Least-Once, Exactly-Once

**Difficulty:** Hard  
**Problem:** Implement all three delivery semantics.

**At-Most-Once (fire and forget):**
```go
func atMostOnce(in <-chan Message, process func(Message)) {
    for msg := range in {
        process(msg) // if process fails, message is lost
    }
}
```

**At-Least-Once (ack after processing):**
```go
type AckableMessage struct {
    Msg Message
    Ack func()   // call after successful processing
    Nack func()  // call on failure — requeue
}

func atLeastOnce(in <-chan AckableMessage, process func(Message) error) {
    for amsg := range in {
        if err := process(amsg.Msg); err != nil {
            amsg.Nack() // requeue — will be processed again
        } else {
            amsg.Ack()  // processed successfully
        }
    }
}
```

**Exactly-Once (deduplication + at-least-once):**
```go
func exactlyOnce(in <-chan AckableMessage, process func(Message) error) {
    processed := make(map[string]bool) // dedup by message ID
    var mu sync.Mutex

    for amsg := range in {
        mu.Lock()
        if processed[amsg.Msg.ID] {
            mu.Unlock()
            amsg.Ack() // already processed, skip
            continue
        }
        mu.Unlock()

        if err := process(amsg.Msg); err != nil {
            amsg.Nack()
        } else {
            mu.Lock()
            processed[amsg.Msg.ID] = true
            mu.Unlock()
            amsg.Ack()
        }
    }
}
```

**Key insight:** True exactly-once is impossible in distributed systems. "Exactly-once" really means "effectively-once" through deduplication + idempotent processing + at-least-once delivery.

**Real-world Relevance:** Kafka exactly-once semantics, SQS message deduplication, payment processing.

---

## Common Mistakes

| Mistake | Consequence | Fix |
|---------|-------------|-----|
| Consumer closes channel | Panic if producer sends | Only producer closes |
| Multiple producers close channel | Panic (double close) | Use sync.Once or WaitGroup coordinator |
| No backpressure | OOM from unbounded production | Use bounded buffer channel |
| Forgetting to drain on shutdown | Data loss | Flush remaining items before exit |
| Mixing batch and single-item processing | Complexity, bugs | Choose one model per consumer |
| Not monitoring queue depth | Silent performance degradation | Expose `len(ch)` as a metric |
