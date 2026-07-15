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

1. Why would you use a worker pool instead of launching a goroutine per task?
2. How do you choose the number of workers? (CPU-bound: GOMAXPROCS; I/O-bound: depends on downstream capacity)
3. How do you handle worker failures/panics?
4. How do you gracefully shut down a worker pool?
5. How do you implement dynamic worker pool sizing?
6. What is the difference between a worker pool and a semaphore?
7. How do you handle backpressure in a worker pool?
8. How do you implement priority in a worker pool?
9. How do you monitor worker pool health? (queue depth, worker utilization, processing latency)
10. How do you handle slow/stuck workers?
11. What is the relationship between GOMAXPROCS and worker pool size?
12. How do you implement work stealing between worker pools?

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

1. What is a pipeline pattern in Go?
2. How do you handle errors in a pipeline?
3. How do you cancel a running pipeline?
4. What is the benefit of lazy evaluation in pipelines?
5. How do you handle backpressure between stages?
6. How do you add batching to a pipeline stage?
7. How do you test pipeline stages in isolation?
8. How do you measure throughput of each stage?
9. What is the relationship between pipelines and generators?
10. How do you handle slow stages? (Fan-out that stage, add buffering)

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

1. What is the fan-in pattern?
2. How do you merge N channels into one?
3. How do you know when all input channels are closed?
4. Does fan-in preserve ordering? (No — it's non-deterministic)
5. How would you maintain order if needed?
6. How do you handle errors in fan-in?
7. What is `reflect.Select` and when would you use it?
8. Performance: goroutine-per-channel vs reflect.Select?

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

1. What is the fan-out pattern?
2. How does fan-out differ from a worker pool? (Conceptually similar; fan-out emphasizes the pattern, worker pool the implementation)
3. How do you control the degree of fan-out?
4. How do you distribute work evenly?
5. How do you handle stragglers in fan-out?
6. What are strategies for fan-out distribution?

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

1. How do you implement producer-consumer in Go? (Channel as buffer)
2. How do you handle multiple producers? (WaitGroup to coordinate close)
3. How do you ensure graceful shutdown?
4. What is the poison pill pattern? (Send a special value to signal shutdown — less idiomatic in Go)
5. How do you handle slow consumers? (Add more consumers, buffer, drop, backpressure)
6. How do you handle slow producers? (Usually fine — consumers just wait)
7. How do you ensure exactly-once processing? (Deduplication, idempotency)
8. What is the difference between producer-consumer and worker pool? (Producer-consumer focuses on the pattern; worker pool is an implementation)
9. How do you add monitoring? (Queue depth, produce/consume rates, latency)
10. How do you handle producer errors? (Send errors through a separate channel or Result type)

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
