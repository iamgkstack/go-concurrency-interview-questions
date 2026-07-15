# Part 7: Real-World System Design & Concurrency Debugging

> Go Concurrency Interview Guide — From Beginner to Staff Engineer

---

## Table of Contents

**Part A: System Design Concurrency Problems**
1. [Concurrent Web Crawler](#1-concurrent-web-crawler)
2. [Distributed Task Queue](#2-distributed-task-queue)
3. [Real-Time Chat Server](#3-real-time-chat-server)
4. [Rate-Limited API Gateway](#4-rate-limited-api-gateway)
5. [Log Aggregation System](#5-log-aggregation-system)
6. [Concurrent Cache with TTL](#6-concurrent-cache-with-ttl)
7. [Stream Processing Engine](#7-stream-processing-engine)
8. [Connection Pool Manager](#8-connection-pool-manager)
9. [Background Job Scheduler](#9-background-job-scheduler)
10. [Metrics Collection System](#10-metrics-collection-system)
11. [Pub-Sub Message Broker](#11-pub-sub-message-broker)
12. [Concurrent File Processor](#12-concurrent-file-processor)

**Part B: Debugging & Troubleshooting**
13. [Deadlocks](#13-deadlocks)
14. [Race Conditions](#14-race-conditions)
15. [Goroutine Leaks](#15-goroutine-leaks)
16. [Starvation](#16-starvation)
17. [Livelocks](#17-livelocks)
18. [Memory Ordering Issues](#18-memory-ordering-issues)
19. [Lock Contention](#19-lock-contention)
20. [Channel Misuse](#20-channel-misuse)
21. [Context Cancellation Bugs](#21-context-cancellation-bugs)

---

# PART A: REAL-WORLD SYSTEM DESIGN

## 1. Concurrent Web Crawler

**Difficulty:** Medium-Hard  
**Concepts:** Worker pool, rate limiting, concurrent set, BFS

**Architecture:**
```
[Seed URLs] → [URL Frontier (chan)] → [Fetcher Workers (N)] → [Parser] → [URL Extractor] → back to Frontier
                                         ↓
                                    [Results Store]
```

**Key Design:**
```go
type Crawler struct {
    visited   sync.Map           // concurrent URL dedup
    frontier  chan string         // URL queue
    results   chan Page           // crawled pages
    limiter   map[string]*rate.Limiter // per-domain rate limiting
    limiterMu sync.RWMutex
    workers   int
    maxDepth  int
}

func (c *Crawler) Run(ctx context.Context, seeds []string) {
    for _, s := range seeds {
        c.frontier <- s
    }
    var wg sync.WaitGroup
    for i := 0; i < c.workers; i++ {
        wg.Add(1)
        go c.worker(ctx, &wg)
    }
    wg.Wait()
}

func (c *Crawler) worker(ctx context.Context, wg *sync.WaitGroup) {
    defer wg.Done()
    for {
        select {
        case url, ok := <-c.frontier:
            if !ok { return }
            if _, loaded := c.visited.LoadOrStore(url, true); loaded {
                continue // already visited
            }
            domain := extractDomain(url)
            c.getDomainLimiter(domain).Wait(ctx) // per-domain rate limit
            page, links := c.fetch(ctx, url)
            c.results <- page
            for _, link := range links {
                select {
                case c.frontier <- link:
                default: // frontier full — backpressure
                }
            }
        case <-ctx.Done():
            return
        }
    }
}
```

**Companies:** Google, Bing, CommonCrawl, Scrapy-based systems.

---

## 2. Distributed Task Queue

**Difficulty:** Hard  
**Concepts:** Producer-consumer, worker pool, retry, dead letter queue

**Key Components:**
- Task submission API (producers).
- Persistent queue (Redis lists, Kafka, PostgreSQL).
- Worker pool with heartbeats.
- Retry with exponential backoff + max attempts.
- Dead letter queue for permanently failed tasks.
- Graceful shutdown with in-flight task completion.

**Companies:** Sidekiq, Celery, Temporal, Uber Cadence.

---

## 3. Real-Time Chat Server

**Difficulty:** Hard  
**Concepts:** Goroutine-per-connection, pub-sub, fan-out

```go
type ChatServer struct {
    rooms   sync.Map // roomID -> *Room
}

type Room struct {
    mu      sync.RWMutex
    clients map[string]*Client
}

type Client struct {
    conn   *websocket.Conn
    sendCh chan Message
    roomID string
}

// One goroutine per client for reading, one for writing
func (c *Client) readLoop(room *Room) {
    defer c.conn.Close()
    for {
        msg, err := c.conn.ReadMessage()
        if err != nil { return }
        room.broadcast(msg, c) // fan-out to all clients
    }
}

func (c *Client) writeLoop() {
    for msg := range c.sendCh {
        c.conn.WriteJSON(msg) // non-blocking per client
    }
}

func (r *Room) broadcast(msg Message, sender *Client) {
    r.mu.RLock()
    defer r.mu.RUnlock()
    for _, client := range r.clients {
        if client != sender {
            select {
            case client.sendCh <- msg:
            default: // slow client, drop message
            }
        }
    }
}
```

**Companies:** Slack, Discord, WhatsApp.

---

## 4. Rate-Limited API Gateway

**Difficulty:** Hard  
**Concepts:** Per-user rate limiting, circuit breaker, request queuing

**Companies:** Kong, Envoy, AWS API Gateway.

---

## 5. Log Aggregation System

**Difficulty:** Hard (FAANG-level)  
**Concepts:** Pipeline, fan-in, buffering, backpressure

```
[Service 1] -\
[Service 2] --+--> [Fan-In Collector] → [Batch Buffer] → [Fan-Out Writers]
[Service N] -/                                              ├→ Elasticsearch
                                                            ├→ S3
                                                            └→ Kafka
```

**Companies:** Datadog Agent, Fluentd, Vector.

---

## 6. Concurrent Cache with TTL

**Difficulty:** Medium-Hard  
**Concepts:** RWMutex, singleflight, background cleanup

```go
type TTLCache struct {
    mu      sync.RWMutex
    items   map[string]*cacheItem
    sf      singleflight.Group
    stopCh  chan struct{}
}

type cacheItem struct {
    value     interface{}
    expiresAt time.Time
}

func (c *TTLCache) Get(key string) (interface{}, bool) {
    c.mu.RLock()
    item, ok := c.items[key]
    c.mu.RUnlock()
    if !ok || time.Now().After(item.expiresAt) {
        return nil, false
    }
    return item.value, true
}

// GetOrFetch with thundering herd protection
func (c *TTLCache) GetOrFetch(key string, ttl time.Duration, fetch func() (interface{}, error)) (interface{}, error) {
    if val, ok := c.Get(key); ok {
        return val, nil
    }
    // singleflight: only one goroutine fetches, others wait
    val, err, _ := c.sf.Do(key, func() (interface{}, error) {
        v, err := fetch()
        if err == nil {
            c.Set(key, v, ttl)
        }
        return v, err
    })
    return val, err
}

// Background cleanup goroutine
func (c *TTLCache) startCleanup(interval time.Duration) {
    go func() {
        ticker := time.NewTicker(interval)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                c.mu.Lock()
                now := time.Now()
                for k, v := range c.items {
                    if now.After(v.expiresAt) {
                        delete(c.items, k)
                    }
                }
                c.mu.Unlock()
            case <-c.stopCh:
                return
            }
        }
    }()
}
```

**Key pattern: `singleflight.Group`** — Prevents thundering herd. If 100 goroutines request the same expired key simultaneously, only one fetches; the other 99 wait and share the result.

---

## 7-12: Additional System Design Problems (Summary)

| # | Problem | Key Patterns | Difficulty |
|---|---------|-------------|------------|
| 7 | Stream Processing Engine | Pipeline, windowing, checkpointing | Staff-level |
| 8 | Connection Pool Manager | Semaphore, health checks, idle cleanup | Medium-Hard |
| 9 | Background Job Scheduler | Priority queue, timer heap, worker pool, cron | Hard |
| 10 | Metrics Collection System | Sharded counters, sync.Pool, batch flush | Hard |
| 11 | Pub-Sub Message Broker | Partitioning, consumer groups, acknowledgment | Staff-level |
| 12 | Concurrent File Processor | Worker pool, fan-out/fan-in, error aggregation | Medium |

---

# PART B: DEBUGGING & TROUBLESHOOTING

## 13. DEADLOCKS

### Types of Deadlocks in Go

**Type 1: Single-Goroutine Channel Deadlock**
```go
// DEADLOCK: unbuffered channel, single goroutine
ch := make(chan int)
ch <- 1        // blocks forever — no receiver
fmt.Println(<-ch)
```
**Fix:** Use buffered channel or separate goroutine.

**Type 2: Circular Mutex Deadlock**
```go
// Goroutine A: Lock mu1, then mu2
// Goroutine B: Lock mu2, then mu1 — DEADLOCK
```
**Fix:** Always acquire locks in consistent order (by resource ID).

**Type 3: Self-Deadlock (Non-Reentrant Mutex)**
```go
func (s *Service) A() {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.B() // calls B which also locks s.mu — DEADLOCK
}
func (s *Service) B() {
    s.mu.Lock()
    defer s.mu.Unlock()
}
```
**Fix:** Extract unlocked helper methods: `A` calls `b()` (unlocked), `B` locks then calls `b()`.

**Type 4: RWMutex Upgrade Deadlock**
```go
rw.RLock()
// ...
rw.Lock() // DEADLOCK: can't upgrade read to write lock
```
**Fix:** Release RLock first, then acquire Lock, then re-check condition.

**Type 5: Channel Direction Deadlock**
```go
ch1 := make(chan int)
ch2 := make(chan int)
go func() { ch2 <- <-ch1 }() // waits for ch1
go func() { ch1 <- <-ch2 }() // waits for ch2 — circular wait
```

**Type 6: WaitGroup Misuse**
```go
var wg sync.WaitGroup
wg.Add(1) // forgot to launch goroutine that calls Done()
wg.Wait() // blocks forever (not detected by Go runtime as "deadlock" if other goroutines exist)
```

### Go's Deadlock Detector
- Runtime detects when **ALL** goroutines are blocked → "fatal error: all goroutines are asleep - deadlock!"
- **Limitation:** If any goroutine is running (e.g., HTTP server, timer), partial deadlocks are NOT detected.
- Use `go tool pprof` goroutine profile to find blocked goroutines in production.

### Interview Questions
1. How does Go detect deadlocks? (Runtime checks if all goroutines are blocked)
2. Can Go detect partial deadlocks? (No)
3. What are the four Coffman conditions for deadlock? (Mutual exclusion, hold and wait, no preemption, circular wait)
4. How do you prevent deadlocks? (Lock ordering, timeout-based lock acquisition, lock-free algorithms)
5. How do you debug a deadlock in production? (goroutine dump via pprof, `kill -ABRT` for stack trace)

---

## 14. RACE CONDITIONS

### Data Race vs Race Condition

| | Data Race | Race Condition |
|--|-----------|---------------|
| Definition | Two goroutines access same memory, at least one writes, no synchronization | Program correctness depends on goroutine execution order |
| Detection | `go test -race` / `go run -race` | Logic errors, harder to detect |
| Example | Concurrent `map[k] = v` | Check-then-act without lock |
| Fix | Add synchronization | Add synchronization, redesign |

**Scenario 1: Concurrent Map Write (fatal, not just a race)**
```go
m := make(map[string]int)
go func() { m["a"] = 1 }()
go func() { m["b"] = 2 }()
// FATAL ERROR: concurrent map writes (Go runtime detects this)
```

**Scenario 2: Slice Append Race**
```go
var results []int
var wg sync.WaitGroup
for i := 0; i < 10; i++ {
    wg.Add(1)
    go func(v int) {
        defer wg.Done()
        results = append(results, v) // RACE: append is not atomic
    }(i)
}
```
**Fix:** Mutex, or pre-allocated slice with indexed writes, or channel.

**Scenario 3: Check-Then-Act (TOCTOU)**
```go
// RACE: another goroutine may modify between check and act
if _, ok := cache[key]; !ok {
    cache[key] = expensiveCompute(key)
}
```
**Fix:** Lock the entire check-then-act, or use `sync.Map.LoadOrStore`, or `singleflight`.

**Scenario 4: Loop Variable Capture (pre-Go 1.22)**
```go
for i := 0; i < 5; i++ {
    go func() {
        fmt.Println(i) // RACE: captures variable, not value
    }()
}
```

### Race Detector
- Enable: `go test -race`, `go run -race`, `go build -race`
- Based on ThreadSanitizer (TSan).
- **Dynamic analysis:** Only finds races on executed code paths. Not exhaustive.
- **Overhead:** 2-20x CPU, 5-10x memory. Not for production.
- **Best practice:** Run tests with `-race` in CI. Every time. No exceptions.

---

## 15. GOROUTINE LEAKS

### Common Causes

**Cause 1: Blocked Channel Send (no receiver)**
```go
func leak() {
    ch := make(chan int)
    go func() {
        ch <- 42 // blocked forever if nobody reads ch
    }()
    // ch goes out of scope, goroutine leaks
}
```

**Cause 2: Missing Context Cancellation**
```go
func leak(url string) {
    go func() {
        for {
            resp, _ := http.Get(url) // never stops polling
            process(resp)
            time.Sleep(time.Second)
        }
    }()
}
```
**Fix:** Pass context, check `ctx.Done()` in loop.

**Cause 3: HTTP Response Body Not Closed**
```go
resp, err := http.Get(url)
if err != nil { return err }
// Missing: defer resp.Body.Close()
// Connection is never returned to pool, goroutine managing it leaks
```

**Cause 4: Ticker Not Stopped**
```go
ticker := time.NewTicker(time.Second)
// Missing: defer ticker.Stop()
// Ticker goroutine leaks
```

### Detection
- **Runtime:** `runtime.NumGoroutine()` — monitor over time, alert on growth.
- **pprof:** `GET /debug/pprof/goroutine` — shows all goroutine stack traces.
- **Testing:** Use `go.uber.org/goleak` in tests:
```go
func TestMain(m *testing.M) {
    goleak.VerifyTestMain(m)
}
```

### Interview Questions
1. How do you detect goroutine leaks? (NumGoroutine, pprof, goleak)
2. Most common causes? (Blocked channels, missing context check, unclosed resources)
3. How do you prevent them? (Context cancellation, resource cleanup, goleak in tests)
4. How do you monitor in production? (Prometheus gauge for goroutine count)

---

## 16. STARVATION

### Definition
A goroutine is ready to run but never gets scheduled because other goroutines monopolize resources.

**Scenario 1: Writer Starvation with RWMutex**
In older Go versions, continuous readers could prevent a writer from ever acquiring the lock. Go 1.9+ prevents this by blocking new readers when a writer is waiting.

**Scenario 2: Select Channel Starvation**
```go
for {
    select {
    case msg := <-highPriority: // always has data
        process(msg)
    case msg := <-lowPriority:  // never selected if highPriority always ready
        process(msg)
    }
}
```
**Fix:** Nested selects (drain high priority first, then select on both).

**Scenario 3: Worker Monopolization**
One worker processes very long tasks, others sit idle. All new tasks queue behind the slow one.
**Fix:** Task timeout, work stealing, task splitting.

---

## 17. LIVELOCKS

### Definition
Goroutines are active (not blocked) but make no progress. They keep reacting to each other without advancing.

**Classic Example: Hallway Problem**
Two goroutines try to acquire two resources but keep yielding to each other.
```go
// Both goroutines: try lock A, if fail try lock B, if fail release everything and retry
// They keep trying and failing in lockstep — no progress
```

**Scenario: Retry Without Backoff**
```go
for {
    err := tryOperation()
    if err == nil { break }
    // Immediately retry without backoff — consumes CPU, contends with others
}
```
**Fix:** Add randomized exponential backoff:
```go
backoff := time.Millisecond * 100
for attempts := 0; attempts < maxRetries; attempts++ {
    if err := tryOperation(); err == nil { break }
    jitter := time.Duration(rand.Int63n(int64(backoff)))
    time.Sleep(backoff + jitter)
    backoff *= 2
}
```

---

## 18. MEMORY ORDERING ISSUES

### Go Memory Model
The Go memory model defines when a read of a variable in one goroutine is **guaranteed** to observe a write in another goroutine.

**Key rule:** Without synchronization, there is NO guarantee that a write in one goroutine is visible in another.

**Broken pattern:**
```go
var data int
var ready bool

go func() {
    data = 42
    ready = true // NO guarantee this is visible to other goroutines
}()

for !ready {} // might loop forever, or see ready=true with data=0
fmt.Println(data)
```

**Why it breaks:**
1. Compiler may reorder `data = 42` and `ready = true`.
2. CPU may reorder writes.
3. Other goroutine may see stale cached value.

**Fix:** Use synchronization (channel, mutex, atomic):
```go
var data int
var ready atomic.Bool

go func() {
    data = 42
    ready.Store(true) // atomic store provides happens-before
}()

for !ready.Load() {}
fmt.Println(data) // guaranteed to see 42
```

---

## 19. LOCK CONTENTION

### Diagnosis
```go
import _ "net/http/pprof"

func main() {
    runtime.SetMutexProfileFraction(1) // enable mutex profiling
    runtime.SetBlockProfileRate(1)     // enable block profiling
    go http.ListenAndServe(":6060", nil)
    // ...
}
```
Then: `go tool pprof http://localhost:6060/debug/pprof/mutex`

### Common Contention Patterns

**Pattern 1: Global Logger Lock**
```go
var globalLogger struct {
    mu  sync.Mutex
    out io.Writer
}
// Every goroutine contends on this single mutex
```
**Fix:** Buffered async logger, per-goroutine buffer, or lock-free logger (zap/zerolog).

**Pattern 2: Hot Cache Mutex**
**Fix:** RWMutex → sharded map → copy-on-write → lock-free (progressive optimization).

**Pattern 3: Metrics Counter**
**Fix:** Sharded atomic counters (see Part 6).

---

## 20. CHANNEL MISUSE

**Scenario 1: Sending on Closed Channel**
```go
ch := make(chan int, 1)
close(ch)
ch <- 1 // PANIC: send on closed channel
```

**Scenario 2: Double Close**
```go
close(ch)
close(ch) // PANIC: close of closed channel
```
**Fix:** `sync.Once` for closing.

**Scenario 3: time.After Memory Leak**
```go
for {
    select {
    case <-ch:
    case <-time.After(5 * time.Second): // NEW timer every iteration
    }
}
```
**Fix:** `time.NewTimer` + `Reset()`.

**Scenario 4: Using int Channel for Signaling**
```go
done := make(chan int) // wastes memory
done <- 1
```
**Fix:** `chan struct{}` for zero-memory signaling.

**Scenario 5: Unbuffered Channel Latency**
```go
// Producer blocks until consumer is ready — adds latency
ch := make(chan Result)
go func() { ch <- expensiveCompute() }()
// If consumer isn't ready yet, producer goroutine sits idle after computing
```
**Fix:** `make(chan Result, 1)` for decoupling.

---

## 21. CONTEXT CANCELLATION BUGS

**Bug 1: Not Propagating Context**
```go
func handler(ctx context.Context) {
    result := fetchFromDB(context.Background()) // BUG: ignores request timeout
}
```

**Bug 2: Ignoring Context in Loop**
```go
func processItems(ctx context.Context, items []Item) {
    for _, item := range items {
        process(item) // never checks ctx.Done() — continues after cancellation
    }
}
```

**Bug 3: Using Background Context for Request Work**
```go
func handler(w http.ResponseWriter, r *http.Request) {
    // BUG: should use r.Context(), not Background()
    go doWork(context.Background(), r.Body)
}
```

**Bug 4: Leaking Cancel Function**
```go
ctx, _ = context.WithCancel(parent) // BUG: cancel func not called, resources leak
```
**Fix:** Always `ctx, cancel := ...; defer cancel()`.

**Bug 5: Context Value Key Collisions**
```go
ctx = context.WithValue(ctx, "user", user1)   // string key
ctx = context.WithValue(ctx, "user", user2)   // overwrites!
```
**Fix:** Use unexported type as key:
```go
type ctxKey struct{}
ctx = context.WithValue(ctx, ctxKey{}, user)
```

---

## Debugging Tools Summary

| Tool | What It Detects | Usage |
|------|----------------|-------|
| `go test -race` | Data races | CI must-have |
| `go tool pprof` (goroutine) | Goroutine leaks, blocked goroutines | Production debugging |
| `go tool pprof` (mutex) | Lock contention | Performance tuning |
| `go tool pprof` (block) | Channel/mutex blocking | Latency debugging |
| `runtime/trace` | Goroutine scheduling, GC pauses | Deep performance analysis |
| `goleak` | Goroutine leaks in tests | Test-time detection |
| `runtime.NumGoroutine()` | Goroutine count monitoring | Production alerting |
| `GODEBUG=schedtrace=1000` | Scheduler behavior | Scheduler debugging |
| `dlv` (Delve) | Breakpoints, goroutine inspection | Development debugging |
