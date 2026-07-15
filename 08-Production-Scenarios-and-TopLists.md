# Part 8: Production Scenarios, Top Lists & Interview Preparation Roadmap

> Go Concurrency Interview Guide — From Beginner to Staff Engineer

---

## Table of Contents

1. [Production Scenario Code Skeletons](#production-scenario-code-skeletons)
2. [Top 100 Most Asked Go Concurrency Interview Questions](#top-100-most-asked-go-concurrency-interview-questions)
3. [Top 50 Coding Problems](#top-50-coding-problems)
4. [Top 25 Concurrency Design Pattern Problems](#top-25-concurrency-design-pattern-problems)
5. [Top 20 Debugging Scenarios](#top-20-debugging-scenarios)
6. [Top 10 Staff/Principal Engineer Questions](#top-10-staffprincipal-engineer-concurrency-design-questions)
7. [Interview Preparation Roadmap](#interview-preparation-roadmap)

---

# PRODUCTION SCENARIO CODE SKELETONS

## Scenario 1: Kafka Consumer Group

```go
type KafkaConsumer struct {
    brokers    []string
    topic      string
    group      string
    workers    int
    handler    func(context.Context, Message) error
}

func (kc *KafkaConsumer) Run(ctx context.Context) error {
    consumer := createConsumer(kc.brokers, kc.group, kc.topic)
    defer consumer.Close()

    // Per-partition goroutine for ordered processing
    partitionWorkers := make(map[int]chan Message)
    var wg sync.WaitGroup

    for {
        select {
        case msg, ok := <-consumer.Messages():
            if !ok { return nil }

            // Get or create partition worker
            if _, exists := partitionWorkers[msg.Partition]; !exists {
                ch := make(chan Message, 100)
                partitionWorkers[msg.Partition] = ch
                wg.Add(1)
                go func(partition int, messages <-chan Message) {
                    defer wg.Done()
                    for m := range messages {
                        if err := kc.handler(ctx, m); err != nil {
                            log.Printf("partition %d error: %v", partition, err)
                            // Dead letter queue or retry
                            continue
                        }
                        consumer.CommitOffset(m) // commit after processing
                    }
                }(msg.Partition, ch)
            }

            partitionWorkers[msg.Partition] <- msg

        case <-ctx.Done():
            // Graceful shutdown: close all partition channels, wait for drain
            for _, ch := range partitionWorkers {
                close(ch)
            }
            wg.Wait()
            return ctx.Err()
        }
    }
}
```

**Key design:** Goroutine-per-partition maintains message ordering within a partition while processing partitions concurrently.

---

## Scenario 2: Rate-Limited API Client

```go
type APIClient struct {
    baseURL   string
    client    *http.Client
    limiter   *rate.Limiter
    sem       chan struct{} // concurrency limiter
    breaker   *CircuitBreaker
}

func NewAPIClient(baseURL string, rps float64, maxConcurrent int) *APIClient {
    return &APIClient{
        baseURL: baseURL,
        client:  &http.Client{Timeout: 30 * time.Second},
        limiter: rate.NewLimiter(rate.Limit(rps), int(rps)),
        sem:     make(chan struct{}, maxConcurrent),
        breaker: NewCircuitBreaker(5, 30*time.Second), // 5 failures, 30s cooldown
    }
}

func (c *APIClient) Do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
    // Circuit breaker check
    if !c.breaker.Allow() {
        return nil, errors.New("circuit breaker open")
    }

    // Rate limit
    if err := c.limiter.Wait(ctx); err != nil {
        return nil, fmt.Errorf("rate limit: %w", err)
    }

    // Concurrency limit
    select {
    case c.sem <- struct{}{}:
        defer func() { <-c.sem }()
    case <-ctx.Done():
        return nil, ctx.Err()
    }

    // Make request with retry
    var lastErr error
    for attempt := 0; attempt < 3; attempt++ {
        if attempt > 0 {
            backoff := time.Duration(math.Pow(2, float64(attempt))) * 100 * time.Millisecond
            jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
            select {
            case <-time.After(backoff + jitter):
            case <-ctx.Done():
                return nil, ctx.Err()
            }
        }

        req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
        if err != nil {
            return nil, err
        }

        resp, err := c.client.Do(req)
        if err != nil {
            lastErr = err
            c.breaker.RecordFailure()
            continue
        }

        if resp.StatusCode == 429 {
            resp.Body.Close()
            retryAfter, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
            if retryAfter > 0 {
                time.Sleep(time.Duration(retryAfter) * time.Second)
            }
            continue
        }

        if resp.StatusCode >= 500 {
            resp.Body.Close()
            lastErr = fmt.Errorf("server error: %d", resp.StatusCode)
            c.breaker.RecordFailure()
            continue
        }

        c.breaker.RecordSuccess()
        return resp, nil
    }

    return nil, fmt.Errorf("all retries failed: %w", lastErr)
}
```

---

## Scenario 3: Concurrent Web Crawler (Complete)

```go
type Crawler struct {
    visited      sync.Map
    frontier     chan CrawlTask
    results      chan CrawlResult
    domainLimits map[string]*rate.Limiter
    dlMu         sync.RWMutex
    sem          chan struct{} // global concurrency limit
    client       *http.Client
}

type CrawlTask struct {
    URL   string
    Depth int
}

func (c *Crawler) Run(ctx context.Context, seeds []string, maxDepth, workers int) <-chan CrawlResult {
    c.frontier = make(chan CrawlTask, 10000)
    c.results = make(chan CrawlResult, 1000)
    c.sem = make(chan struct{}, workers)

    // Seed the frontier
    go func() {
        for _, url := range seeds {
            c.frontier <- CrawlTask{URL: url, Depth: 0}
        }
    }()

    // Workers
    var active sync.WaitGroup
    go func() {
        defer close(c.results)
        for {
            select {
            case task, ok := <-c.frontier:
                if !ok { return }
                if task.Depth > maxDepth { continue }
                if _, loaded := c.visited.LoadOrStore(task.URL, true); loaded {
                    continue
                }

                active.Add(1)
                c.sem <- struct{}{} // acquire concurrency slot
                go func(t CrawlTask) {
                    defer active.Done()
                    defer func() { <-c.sem }()

                    c.getDomainLimiter(t.URL).Wait(ctx)
                    result, links := c.fetch(ctx, t.URL)
                    c.results <- result

                    for _, link := range links {
                        select {
                        case c.frontier <- CrawlTask{URL: link, Depth: t.Depth + 1}:
                        default: // frontier full
                        }
                    }
                }(task)

            case <-ctx.Done():
                active.Wait()
                return
            }
        }
    }()

    return c.results
}
```

---

# TOP 100 MOST ASKED GO CONCURRENCY INTERVIEW QUESTIONS

## Foundation (1-25) — Most Frequently Asked

| # | Question | Difficulty | Category |
|---|----------|-----------|----------|
| 1 | What is the difference between concurrency and parallelism? | Easy | Concepts |
| 2 | What are goroutines and how do they differ from OS threads? | Easy | Goroutines |
| 3 | What is a channel? Buffered vs unbuffered? | Easy | Channels |
| 4 | What happens when you send on a closed channel? | Easy | Channels |
| 5 | What is sync.WaitGroup and how do you use it? | Easy | WaitGroups |
| 6 | How do you detect race conditions? | Easy | Debugging |
| 7 | What is the select statement? | Easy | Select |
| 8 | What is context.Context and why is it important? | Medium | Context |
| 9 | When do you use a mutex vs a channel? | Medium | Design |
| 10 | How does the Go scheduler work (G-M-P)? | Medium | Goroutines |
| 11 | What is a race condition vs a data race? | Medium | Debugging |
| 12 | What is sync.Mutex and how do you use it? | Easy | Mutex |
| 13 | What is sync.RWMutex and when would you use it? | Medium | RWMutex |
| 14 | How do you prevent goroutine leaks? | Medium | Debugging |
| 15 | What happens if you receive from a closed channel? | Easy | Channels |
| 16 | What is GOMAXPROCS? | Easy | Goroutines |
| 17 | How do you implement a worker pool? | Medium | Patterns |
| 18 | What is the done channel pattern? | Medium | Patterns |
| 19 | What is sync.Once? | Easy | Sync |
| 20 | How do you implement a timeout with channels? | Medium | Patterns |
| 21 | What happens if main goroutine exits? | Easy | Goroutines |
| 22 | What is errgroup? | Medium | Error Handling |
| 23 | What is the pipeline pattern? | Medium | Patterns |
| 24 | How do you handle panics in goroutines? | Medium | Error Handling |
| 25 | Who should close a channel — sender or receiver? | Easy | Channels |

## Intermediate (26-50)

| # | Question | Difficulty | Category |
|---|----------|-----------|----------|
| 26 | What is the fan-in/fan-out pattern? | Medium | Patterns |
| 27 | How does select choose between multiple ready cases? | Medium | Select |
| 28 | What is a nil channel and how does it behave in select? | Medium | Channels |
| 29 | How do you implement graceful shutdown? | Medium | Production |
| 30 | What is singleflight and when do you use it? | Medium | Patterns |
| 31 | How do you implement rate limiting in Go? | Medium | Patterns |
| 32 | What is the producer-consumer pattern? | Medium | Patterns |
| 33 | How do you implement a semaphore in Go? | Medium | Patterns |
| 34 | What is sync.Map and when should you use it? | Medium | Data Structures |
| 35 | What is atomic.Value? | Medium | Atomic |
| 36 | How do you profile goroutines in production? | Medium | Debugging |
| 37 | What is backpressure? | Medium | Patterns |
| 38 | How does context cancellation propagate? | Medium | Context |
| 39 | What is the closure variable capture bug in loops? | Medium | Goroutines |
| 40 | How do you implement a concurrent cache? | Medium | Data Structures |
| 41 | What is signal.NotifyContext? | Medium | Production |
| 42 | What is the memory leak with time.After in loops? | Medium | Debugging |
| 43 | How do you choose buffer size for channels? | Medium | Channels |
| 44 | What is context.WithValue and its dangers? | Medium | Context |
| 45 | How do you implement the future/promise pattern? | Medium | Patterns |
| 46 | What happens if WaitGroup counter goes negative? | Easy | WaitGroups |
| 47 | How do you implement non-blocking channel operations? | Medium | Select |
| 48 | What is the difference between context.WithDeadline and WithTimeout? | Easy | Context |
| 49 | What is TryLock and when was it added? | Medium | Mutex |
| 50 | How do you implement a pub-sub system? | Medium | Patterns |

## Advanced (51-75)

| # | Question | Difficulty | Category |
|---|----------|-----------|----------|
| 51 | What is the Go memory model? | Hard | Internals |
| 52 | How does mutex starvation mode work? | Hard | Mutex |
| 53 | What is CAS and how does it work? | Hard | Atomic |
| 54 | How do you implement lock-free data structures? | Hard | Data Structures |
| 55 | What is the ABA problem? | Hard | Atomic |
| 56 | How do you reduce lock contention in production? | Hard | Performance |
| 57 | What is false sharing? | Hard | Performance |
| 58 | How does sync.Pool work? When should you use it? | Hard | Performance |
| 59 | How do you implement a distributed lock? | Hard | Distributed |
| 60 | What is the Saga pattern? | Hard | Distributed |
| 61 | How do you handle exactly-once delivery? | Hard | Distributed |
| 62 | How do you implement leader election? | Hard | Distributed |
| 63 | What is a fencing token? | Hard | Distributed |
| 64 | How do you implement a connection pool? | Hard | Resource Mgmt |
| 65 | How do you implement adaptive rate limiting? | Hard | Patterns |
| 66 | What is work stealing in the Go scheduler? | Hard | Internals |
| 67 | How do you implement a priority channel? | Hard | Patterns |
| 68 | What is the hedged request pattern? | Hard | Patterns |
| 69 | How do you implement load shedding? | Hard | Production |
| 70 | What is a weighted semaphore? | Hard | Patterns |
| 71 | How do you implement windowed aggregation? | Hard | Patterns |
| 72 | What is complex event processing? | Hard | Patterns |
| 73 | How do you design a concurrent LRU cache? | Hard | Data Structures |
| 74 | How do you propagate deadline budgets? | Hard | Production |
| 75 | What is context.WithoutCancel and when do you use it? | Hard | Context |

## Expert / Staff Level (76-100)

| # | Question | Difficulty | Category |
|---|----------|-----------|----------|
| 76 | Design a system handling 1M RPS with Go | Staff | System Design |
| 77 | Implement a channel from scratch using mutex+cond | Staff | Internals |
| 78 | Design a distributed rate limiter for multi-region | Staff | Distributed |
| 79 | How do you implement a lock-free MPSC queue? | Staff | Data Structures |
| 80 | Design a saga orchestrator | Staff | Distributed |
| 81 | How do you implement zero-allocation hot paths? | Staff | Performance |
| 82 | Design a real-time stream processing engine | Staff | System Design |
| 83 | Implement a work-stealing scheduler | Staff | Internals |
| 84 | Design a concurrent cache with thundering herd protection | Staff | System Design |
| 85 | How do channels work internally (hchan)? | Staff | Internals |
| 86 | Design a connection pool with health management | Staff | System Design |
| 87 | Implement copy-on-write data structures | Staff | Data Structures |
| 88 | Design a backpressure-aware data pipeline | Staff | System Design |
| 89 | Explain goroutine preemption changes across Go versions | Staff | Internals |
| 90 | Design a multi-tenant rate limiter with fair queuing | Staff | System Design |
| 91 | Implement a barrier synchronization primitive | Staff | Primitives |
| 92 | Design a distributed worker with task dependencies | Staff | System Design |
| 93 | How would you debug a production goroutine leak? | Staff | Debugging |
| 94 | Design a zero-downtime deployment system | Staff | Production |
| 95 | Implement an event sourcing system with concurrent readers | Staff | Patterns |
| 96 | How does the GC interact with goroutine scheduling? | Staff | Internals |
| 97 | Design a Kafka consumer group implementation | Staff | System Design |
| 98 | Implement sharded counters for 10M ops/sec | Staff | Performance |
| 99 | Design a full application lifecycle manager | Staff | Production |
| 100 | Explain the trade-offs between all Go synchronization primitives | Staff | Design |

---

# TOP 50 CODING PROBLEMS

| # | Problem | Difficulty | Category |
|---|---------|-----------|----------|
| 1 | Goroutine-safe counter (mutex vs atomic) | Easy | Mutex/Atomic |
| 2 | Fix closure variable capture bug in goroutine loop | Easy | Goroutines |
| 3 | Ping-pong between two goroutines | Easy | Channels |
| 4 | Parallel URL fetcher with WaitGroup | Easy | WaitGroups |
| 5 | Identify deadlock in channel code | Easy | Channels |
| 6 | Implement `merge(channels ...<-chan int) <-chan int` | Easy-Medium | Fan-In |
| 7 | Fibonacci generator with channels | Easy-Medium | Channels |
| 8 | Bounded queue with TryEnqueue/TryDequeue | Medium | Channels |
| 9 | Basic worker pool with N workers | Medium | Worker Pool |
| 10 | Number processing pipeline (generate→square→filter→sum) | Medium | Pipeline |
| 11 | Token bucket rate limiter from scratch | Medium | Rate Limiting |
| 12 | HTTP handler with 3-second timeout | Medium | Context |
| 13 | Channel-based mutex (no sync package) | Medium | Channels |
| 14 | Thread-safe map with Get/Set/Delete | Medium | Mutex |
| 15 | WaitGroup bug hunt (find and fix all bugs) | Medium | WaitGroups |
| 16 | Heartbeat monitor with timeout | Medium | Select |
| 17 | Singleton DB connection with sync.Once | Medium | Once |
| 18 | In-process event bus (subscribe/publish) | Medium | Pub-Sub |
| 19 | Batch consumer (flush on size or timeout) | Medium | Producer-Consumer |
| 20 | Context-aware long computation with periodic cancellation check | Medium | Context |
| 21 | Parallel map with bounded concurrency | Medium | Fan-Out |
| 22 | Future[T] with Get, GetWithTimeout | Medium | Future/Promise |
| 23 | Semaphore-based download limiter | Medium | Semaphore |
| 24 | Config store with RWMutex | Medium | RWMutex |
| 25 | Atomic config swap (read-copy-update) | Medium | Atomic |
| 26 | Merge N sorted channels | Medium-Hard | Channels |
| 27 | Unbounded channel implementation | Medium-Hard | Channels |
| 28 | Bank account transfer without deadlock | Medium-Hard | Mutex |
| 29 | RWMutex upgrade deadlock fix | Medium-Hard | RWMutex |
| 30 | Ordered fan-in (maintain sequence numbers) | Medium-Hard | Fan-In |
| 31 | Priority channel (high/low without starvation) | Medium-Hard | Select |
| 32 | Worker pool with rate limiting | Medium-Hard | Worker Pool |
| 33 | Multi-producer multi-consumer queue | Medium-Hard | Producer-Consumer |
| 34 | Per-key rate limiter with cleanup | Medium-Hard | Rate Limiting |
| 35 | Bounded buffer with sync.Cond | Medium-Hard | Cond |
| 36 | Resettable Once / Once with retry on error | Medium-Hard | Once |
| 37 | Fan-out/fan-in with order preservation | Hard | Fan-Out |
| 38 | Auto-scaling worker pool | Hard | Worker Pool |
| 39 | Pipeline with backpressure and metrics | Hard | Pipeline |
| 40 | Thread-safe LRU cache | Hard | Data Structures |
| 41 | Concurrent web crawler | Hard | System Design |
| 42 | Dining philosophers | Hard | Mutex |
| 43 | Lock-free stack with CAS | Hard | Atomic |
| 44 | HTTP server graceful shutdown | Hard | Production |
| 45 | Multi-service aggregator with cascading timeouts | Hard | Context |
| 46 | Hedged requests (first response wins) | Hard | Patterns |
| 47 | Sliding window rate limiter | Hard | Rate Limiting |
| 48 | Worker pool with task dependencies (DAG) | Hard | Worker Pool |
| 49 | High-throughput sharded counter (10M ops/sec) | Hard | Performance |
| 50 | Full application lifecycle manager | Hard | Production |

---

# TOP 25 CONCURRENCY DESIGN PATTERN PROBLEMS

| # | Pattern Problem | Difficulty |
|---|----------------|-----------|
| 1 | Worker pool with dynamic scaling and monitoring | Hard |
| 2 | Multi-stage pipeline with error handling and cancellation | Hard |
| 3 | Fan-out/fan-in with ordered results | Hard |
| 4 | Producer-consumer with exactly-once delivery | Hard |
| 5 | Pub-sub with slow subscriber strategies (drop/block/buffer) | Hard |
| 6 | Token bucket rate limiter with burst and per-key support | Medium-Hard |
| 7 | Backpressure-aware pipeline with monitoring | Hard |
| 8 | Weighted semaphore for multi-resource management | Medium-Hard |
| 9 | Future/Promise with WhenAll/WhenAny composition | Medium-Hard |
| 10 | Event dispatcher with panic recovery | Medium |
| 11 | Context-aware cache with stale-while-revalidate | Hard |
| 12 | Cancellable pipeline with graceful drain | Hard |
| 13 | Timeout budget propagation across services | Hard |
| 14 | Error propagation with first-error-cancellation | Medium-Hard |
| 15 | Graceful shutdown with reverse-order component teardown | Hard |
| 16 | Concurrent LRU cache with TTL and singleflight | Hard |
| 17 | Connection pool with health checks and idle timeout | Hard |
| 18 | Distributed lock with lease renewal | Hard |
| 19 | Leader election with heartbeats | Hard |
| 20 | Saga orchestrator with compensation | Hard |
| 21 | Stream processing with time-based windowing | Hard |
| 22 | Complex event processing (pattern detection) | Hard |
| 23 | Load shedder with priority-based rejection | Hard |
| 24 | Adaptive rate limiter with AIMD | Hard |
| 25 | Lock-free MPSC queue for high-throughput | Staff |

---

# TOP 20 DEBUGGING SCENARIOS

| # | Scenario | Root Cause | Key Skill |
|---|----------|-----------|-----------|
| 1 | Goroutine leak from blocked channel send | No receiver for sent values | Lifecycle management |
| 2 | Deadlock from circular mutex acquisition | Lock ordering inconsistency | Lock ordering |
| 3 | Race condition on concurrent map writes | Missing synchronization | Race detector |
| 4 | Memory leak from `time.After` in for-select loop | New timer every iteration | Timer reuse |
| 5 | Context cancellation not propagated to downstream | Using `context.Background()` | Context propagation |
| 6 | Self-deadlock from non-reentrant mutex | Method A calls method B, both lock same mutex | API design |
| 7 | WaitGroup misuse — `Add()` inside goroutine | Race between `Add()` and `Wait()` | WaitGroup rules |
| 8 | Slice append race condition | Multiple goroutines appending | Mutex or index-based writes |
| 9 | Sending on closed channel panic | Wrong closer (receiver closes instead of sender) | Close ownership |
| 10 | RWMutex upgrade deadlock | RLock then Lock from same goroutine | Double-check pattern |
| 11 | Goroutine leak from unclosed HTTP response body | Connection not returned to pool | `defer resp.Body.Close()` |
| 12 | Starvation from always-ready select case | High-priority channel always has data | Nested select pattern |
| 13 | Closure captures loop variable (pre-Go 1.22) | Variable shared, not value | Pass as parameter |
| 14 | Livelock from retry without backoff | Immediate retry under contention | Exponential backoff |
| 15 | Memory ordering issue — flag without atomic | Compiler/CPU reordering | Use atomic or channel |
| 16 | Lock contention in hot HTTP handler | Global mutex for shared state | Sharding, RWMutex, atomic |
| 17 | Double close panic | Multiple goroutines close same channel | `sync.Once` for close |
| 18 | Ticker not stopped — goroutine leak | `time.NewTicker` without `defer Stop()` | Always stop tickers |
| 19 | Context cancel func not called — resource leak | `ctx, _ := context.WithCancel(...)` | Always `defer cancel()` |
| 20 | Partial deadlock undetected by Go runtime | Some goroutines still running (HTTP server) | pprof goroutine dump |

---

# TOP 10 STAFF/PRINCIPAL ENGINEER CONCURRENCY DESIGN QUESTIONS

## 1. Design a Distributed Rate Limiter for Multi-Region (1M RPS)

**Discussion topics:** Token bucket per region vs global, Redis vs local+sync, consistency model (eventual OK for rate limiting), sliding window vs token bucket trade-offs, handling Redis failures (local fallback), latency impact.

**Red flags:** Not considering network latency in distributed design, exact consistency requirement (overkill), not handling Redis failure.

**Great answer includes:** Local rate limiting with periodic sync to central store, configurable per-tenant limits, graceful degradation, monitoring and alerting.

## 2. Design a Lock-Free Concurrent Data Structure for a Hot Path

**Discussion topics:** CAS loops, ABA problem, memory reclamation, false sharing, cache line alignment, benchmarking methodology.

**Great answer includes:** Profile-first approach, explains when lock-free is/isn't worth it, discusses GC implications in Go.

## 3. Design a Saga Orchestrator for Distributed Transactions

**Discussion topics:** Compensation actions, partial failure, idempotency, state persistence, concurrent independent steps, monitoring/observability.

**Great answer includes:** State machine design, persistent saga log, timeout handling, dead letter for failed compensations.

## 4. Design a Real-Time Stream Processing Engine

**Discussion topics:** Windowing (tumbling/sliding/session), watermarks, late events, exactly-once, checkpointing, backpressure, horizontal scaling.

**Great answer includes:** Partition-based parallelism, watermark-based window triggering, checkpoint-based fault tolerance.

## 5. Design a Connection Pool with Auto-Scaling and Health Management

**Discussion topics:** Little's Law for sizing, health checks (active vs passive), idle timeout, max lifetime, LIFO vs FIFO, graceful drain.

**Great answer includes:** Formulas for pool sizing, background health checker, connection warming, integration with service discovery.

## 6. Design a Concurrent Cache with Thundering Herd Protection

**Discussion topics:** singleflight, stale-while-revalidate, cache stampede, TTL jitter, hot key handling, memory bounds.

**Great answer includes:** singleflight for dedup, probabilistic early expiry, size-aware eviction, metrics.

## 7. Design a Work-Stealing Scheduler for Heterogeneous Tasks

**Discussion topics:** Per-worker queues, steal from tail (cache-friendly), task priority, fairness, affinity.

**Great answer includes:** Double-ended queue per worker, steal algorithm, locality awareness, adaptive stealing.

## 8. Design a Backpressure-Aware Pipeline for High-Throughput Data Ingestion

**Discussion topics:** Bounded buffers, batch processing, per-stage metrics, adaptive concurrency, circuit breakers per stage.

**Great answer includes:** End-to-end backpressure propagation, monitoring dashboards, auto-scaling stages, zero-allocation hot paths.

## 9. Design a Leader Election System with Graceful Failover

**Discussion topics:** Lease-based, fencing tokens, split brain prevention, failover latency, leadership transfer.

**Great answer includes:** TTL-based leases, fencing tokens for stale leader protection, graceful resignation, observer pattern for followers.

## 10. Design a Multi-Tenant Rate Limiter with Fair Queuing

**Discussion topics:** Weighted fair queuing, per-tenant isolation, burst handling, global vs per-tenant limits, priority.

**Great answer includes:** Token bucket per tenant, fair queue for requests, admission control, tenant isolation guarantees.

---

# INTERVIEW PREPARATION ROADMAP

## Phase 1: Foundation (Week 1-2)

| Day | Topic | Practice |
|-----|-------|---------|
| 1-2 | Goroutines, Go scheduler (G-M-P) | Q1-5, Problem 1-2 |
| 3-4 | Channels (buffered, unbuffered, directional) | Q6-10, Problem 3-5 |
| 5-6 | Select statement, done pattern | Q11-15, Problem 6-8 |
| 7-8 | WaitGroup, errgroup | Q16-20, Problem 9-10 |
| 9-10 | Mutex, RWMutex | Q21-25, Problem 11-14 |
| 11-12 | Atomic operations | Problem 15-16 |
| 13-14 | Review + practice easy problems | Coding problems 1-10 |

**Milestone:** Can explain G-M-P model, implement basic concurrent patterns, use race detector.

## Phase 2: Core Patterns (Week 3-4)

| Day | Topic | Practice |
|-----|-------|---------|
| 15-16 | Worker pool pattern | Problem 17-20 |
| 17-18 | Pipeline pattern | Problem 21-22 |
| 19-20 | Fan-in, fan-out | Problem 23-25 |
| 21-22 | Producer-consumer | Problem 26-28 |
| 23-24 | Context package deep dive | Problem 29-31 |
| 25-26 | Error propagation in concurrent code | Problem 32-33 |
| 27-28 | Review + practice medium problems | Coding problems 11-25 |

**Milestone:** Can implement all core patterns from scratch with proper error handling and cancellation.

## Phase 3: Advanced Patterns (Week 5-6)

| Day | Topic | Practice |
|-----|-------|---------|
| 29-30 | Rate limiting (token bucket, sliding window) | Problem 34-36 |
| 31-32 | Pub-sub, event processing | Problem 37-38 |
| 33-34 | Semaphore, future/promise | Problem 39-40 |
| 35-36 | Concurrent data structures (LRU, sharded map) | Problem 41-42 |
| 37-38 | Resource pool management | Problem 43 |
| 39-40 | Graceful shutdown | Problem 44 |
| 41-42 | Review + practice hard problems | Coding problems 26-40 |

**Milestone:** Can design concurrent systems with proper patterns, handle edge cases.

## Phase 4: Production & System Design (Week 7-8)

| Day | Topic | Practice |
|-----|-------|---------|
| 43-44 | Debugging deadlocks, race conditions | Debug scenarios 1-8 |
| 45-46 | Goroutine leaks, starvation, livelocks | Debug scenarios 9-16 |
| 47-48 | System design: web crawler, rate limiter | System design problems |
| 49-50 | System design: message queue, cache | System design problems |
| 51-52 | Distributed concurrency concepts | Q59-65 |
| 53-54 | High-throughput system design | Q76, coding problems 49-50 |
| 55-56 | Review + FAANG-level problems | Coding problems 41-50 |

**Milestone:** Can debug production concurrency issues. Can design concurrent systems end-to-end.

## Phase 5: Staff Level (Week 9-10) — Optional

| Day | Topic | Practice |
|-----|-------|---------|
| 57-58 | Go runtime internals (scheduler, memory model) | Q85-89, 96 |
| 59-60 | Lock-free programming | Q54-55, 79, 98 |
| 61-62 | Distributed consensus and coordination | Q59-63 |
| 63-64 | Performance optimization and profiling | Q56-57, 81 |
| 65-66 | Staff-level design questions | Top 10 Staff questions |
| 67-68 | Mock interviews | Practice explaining trade-offs |
| 69-70 | Final review and weak areas | Full review |

**Milestone:** Can lead technical discussions on concurrency, design large-scale concurrent systems, mentor others.

---

## Resources

### Books
- **"Concurrency in Go"** by Katherine Cox-Buday — Best introduction
- **"The Go Programming Language"** by Donovan & Kernighan — Chapter 8 & 9
- **"Go in Practice"** by Butcher & Farina — Production patterns

### Official Resources
- [Go Blog: Pipelines and Cancellation](https://go.dev/blog/pipelines)
- [Go Blog: Context](https://go.dev/blog/context)
- [Go Blog: Share Memory by Communicating](https://go.dev/blog/codelab-share)
- [Go Memory Model](https://go.dev/ref/mem)
- [Go Scheduler Design](https://docs.google.com/document/d/1TTj4T2JO42uD5ID9e89oa0sLKhJYD0Y_kqxDv3I3XMw)

### Source Code to Read
- `runtime/chan.go` — Channel implementation
- `runtime/proc.go` — Goroutine scheduler
- `sync/mutex.go` — Mutex with starvation mode
- `sync/once.go` — Once implementation (simple but instructive)
- `golang.org/x/sync/errgroup` — errgroup source
- `golang.org/x/sync/semaphore` — Weighted semaphore
- `golang.org/x/sync/singleflight` — Singleflight

### Open-Source Go Projects to Study
- **etcd** — Distributed consensus, leader election
- **CockroachDB** — Distributed SQL, concurrent query execution
- **Kubernetes** — Controller patterns, informers, work queues
- **Prometheus** — Metrics collection, concurrent scraping
- **Caddy** — HTTP server, graceful restarts
- **NATS** — High-performance message broker

### GopherCon Talks
- "Concurrency Is Not Parallelism" by Rob Pike
- "Advanced Go Concurrency Patterns" by Sameer Ajmani
- "Understanding Channels" by Kavya Joshi
- "The Scheduler Saga" by Kavya Joshi

---

## Study Tips

1. **Implement every pattern from scratch** — don't just read about them.
2. **Always use `go test -race`** — make it a CI gate.
3. **Profile with pprof** — understand goroutine, mutex, and block profiles.
4. **Read production Go code** — etcd, CockroachDB, Kubernetes.
5. **Practice explaining trade-offs** — interviewers care about WHY, not just WHAT.
6. **Build a project combining multiple patterns** — e.g., a concurrent web crawler with rate limiting, caching, and graceful shutdown.
7. **Do mock interviews** — practice whiteboarding concurrent solutions.
8. **Understand the Go memory model** — it's short and critical.
9. **Know when NOT to use concurrency** — simpler is better when concurrency isn't needed.
10. **Master `context.Context`** — it's used in every production Go program.
