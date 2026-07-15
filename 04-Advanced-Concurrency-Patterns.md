# Part 4: Advanced Concurrency Patterns — Pub-Sub, Rate Limiting, Backpressure, Semaphore, Future/Promise, Event Processing

> Go Concurrency Interview Guide — From Beginner to Staff Engineer

---

## Table of Contents

1. [Publish-Subscribe Pattern](#1-publish-subscribe-pattern)
2. [Rate Limiting](#2-rate-limiting)
3. [Backpressure](#3-backpressure)
4. [Semaphore Pattern](#4-semaphore-pattern)
5. [Future/Promise Pattern](#5-futurepromise-pattern)
6. [Event Processing](#6-event-processing)

---

# 1. PUBLISH-SUBSCRIBE PATTERN

## Concept Overview

Pub-sub decouples message producers (publishers) from consumers (subscribers). Publishers broadcast messages to a topic; all subscribers on that topic receive a copy.

**Key differences from Fan-Out:**
- Fan-Out: one message goes to ONE worker (load balancing).
- Pub-Sub: one message goes to ALL subscribers (broadcast).

**Design decisions:**
- **Slow subscriber handling:** Drop messages? Buffer? Block publisher? Disconnect subscriber?
- **Delivery guarantees:** At-most-once (fire and forget) vs at-least-once (with ack).
- **Ordering:** Per-topic ordering? Global ordering?
- **Subscriber lifecycle:** Dynamic subscribe/unsubscribe.

## Common Interview Questions

1. How does pub-sub differ from fan-out? (Broadcast vs load-balance)
2. How do you handle slow subscribers? (Drop, buffer, backpressure, disconnect)
3. How do you implement topic-based routing?
4. How do you handle subscriber disconnection?
5. What is the difference between at-most-once and at-least-once in pub-sub?
6. How do you scale a pub-sub system?
7. How do you implement wildcard subscriptions?
8. How do you prevent a slow subscriber from affecting others?
9. How do you implement message replay?
10. What is a consumer group and how does it differ from pub-sub?

## Coding Problems

### Problem 1: In-Process Event Bus

**Difficulty:** Medium  
**Problem:** Implement a thread-safe event bus with Subscribe, Unsubscribe, Publish.

```go
type EventBus struct {
    mu     sync.RWMutex
    subs   map[string]map[int]chan interface{}
    nextID int
}

func NewEventBus() *EventBus {
    return &EventBus{subs: make(map[string]map[int]chan interface{})}
}

func (eb *EventBus) Subscribe(topic string, bufSize int) (int, <-chan interface{}) {
    eb.mu.Lock()
    defer eb.mu.Unlock()
    if eb.subs[topic] == nil {
        eb.subs[topic] = make(map[int]chan interface{})
    }
    id := eb.nextID
    eb.nextID++
    ch := make(chan interface{}, bufSize)
    eb.subs[topic][id] = ch
    return id, ch
}

func (eb *EventBus) Unsubscribe(topic string, id int) {
    eb.mu.Lock()
    defer eb.mu.Unlock()
    if subs, ok := eb.subs[topic]; ok {
        if ch, ok := subs[id]; ok {
            close(ch)
            delete(subs, id)
        }
    }
}

func (eb *EventBus) Publish(topic string, msg interface{}) {
    eb.mu.RLock()
    defer eb.mu.RUnlock()
    for _, ch := range eb.subs[topic] {
        // Non-blocking send — drop if subscriber buffer is full
        select {
        case ch <- msg:
        default:
            // subscriber is slow — message dropped
        }
    }
}
```

**Variations:** Wildcard topics (`events.*`), message filtering, replay buffer.  
**Real-world Relevance:** Event-driven architectures, notification systems, microservice communication.

---

### Problem 2: Pub-Sub with Slow Subscriber Strategies

**Difficulty:** Hard  
**Problem:** Support three strategies: DropOldest, DropNewest, Block.

```go
type OverflowStrategy int
const (
    DropNewest OverflowStrategy = iota
    DropOldest
    Block
)

type Subscriber struct {
    ch       chan interface{}
    strategy OverflowStrategy
}

func (s *Subscriber) Send(msg interface{}) {
    switch s.strategy {
    case Block:
        s.ch <- msg // blocks if full
    case DropNewest:
        select {
        case s.ch <- msg:
        default: // drop this message
        }
    case DropOldest:
        select {
        case s.ch <- msg:
        default:
            <-s.ch     // discard oldest
            s.ch <- msg // send new
        }
    }
}
```

**Real-world Relevance:** Real-time data feeds (stock tickers), WebSocket broadcast, log streaming.

---

### Problem 3: Partitioned Pub-Sub (Kafka-style)

**Difficulty:** Hard (Staff-level)  
**Problem:** Design pub-sub with partitions and consumer groups.

```go
type Partition struct {
    mu       sync.Mutex
    messages []Message
    offset   int64
}

type Topic struct {
    partitions []*Partition
    numParts   int
}

type ConsumerGroup struct {
    mu          sync.Mutex
    assignments map[string][]int // consumerID -> partition indices
    committed   map[int]int64    // partition -> committed offset
}

// Publish to a partition based on key hash
func (t *Topic) Publish(key string, msg Message) {
    partIdx := hash(key) % t.numParts
    p := t.partitions[partIdx]
    p.mu.Lock()
    defer p.mu.Unlock()
    msg.Offset = p.offset
    p.messages = append(p.messages, msg)
    p.offset++
}

// Consumer reads from assigned partitions
func (cg *ConsumerGroup) Consume(consumerID string, t *Topic) <-chan Message {
    out := make(chan Message)
    go func() {
        defer close(out)
        cg.mu.Lock()
        partitions := cg.assignments[consumerID]
        cg.mu.Unlock()
        for _, pIdx := range partitions {
            p := t.partitions[pIdx]
            offset := cg.committed[pIdx]
            // Read from offset forward
            p.mu.Lock()
            for i := offset; i < int64(len(p.messages)); i++ {
                out <- p.messages[i]
            }
            p.mu.Unlock()
        }
    }()
    return out
}
```

**Real-world Relevance:** Kafka consumer group implementation, NATS JetStream, Pulsar.

---

# 2. RATE LIMITING

## Concept Overview

Rate limiting controls the frequency of operations — essential for protecting services and respecting API quotas.

**Algorithms:**

| Algorithm | How It Works | Burst Handling | Memory |
|-----------|-------------|----------------|--------|
| **Token Bucket** | Tokens added at fixed rate; request takes a token | Yes (bucket can fill up) | O(1) |
| **Leaky Bucket** | Requests enter bucket, leak at fixed rate | No (smooths bursts) | O(N) |
| **Fixed Window** | Count requests per time window | Boundary burst (2x at window edge) | O(1) |
| **Sliding Window** | Count requests in sliding time range | Accurate but more complex | O(N) |

**Go standard library:**
- `golang.org/x/time/rate` — Token bucket implementation with `Allow()`, `Wait()`, `Reserve()`.
- `time.Ticker` — Simple but no burst support.

## Common Interview Questions

1. What is the difference between token bucket and leaky bucket?
2. How do you implement rate limiting with `time.Ticker`?
3. How do you handle burst traffic? (Token bucket with burst size)
4. What is the sliding window algorithm?
5. How do you implement distributed rate limiting? (Redis + Lua scripts)
6. How does `golang.org/x/time/rate` work?
7. How do you rate limit per-user vs globally?
8. What is adaptive rate limiting? (Adjust rate based on downstream health)
9. What is the difference between rate limiting and throttling?
10. How do you implement rate limiting with Redis? (INCR + EXPIRE, or sorted sets for sliding window)
11. What is AIMD (Additive Increase Multiplicative Decrease)?
12. How do you handle rate limit errors gracefully? (429 + Retry-After header)

## Coding Problems

### Problem 1: Token Bucket Rate Limiter from Scratch

**Difficulty:** Medium  
**Problem:** Implement a token bucket with configurable rate and burst.

```go
type TokenBucket struct {
    mu         sync.Mutex
    tokens     float64
    maxTokens  float64
    refillRate float64 // tokens per second
    lastRefill time.Time
}

func NewTokenBucket(rate float64, burst int) *TokenBucket {
    return &TokenBucket{
        tokens:     float64(burst),
        maxTokens:  float64(burst),
        refillRate: rate,
        lastRefill: time.Now(),
    }
}

func (tb *TokenBucket) refill() {
    now := time.Now()
    elapsed := now.Sub(tb.lastRefill).Seconds()
    tb.tokens = min(tb.maxTokens, tb.tokens+elapsed*tb.refillRate)
    tb.lastRefill = now
}

// Allow: non-blocking — returns true if allowed
func (tb *TokenBucket) Allow() bool {
    tb.mu.Lock()
    defer tb.mu.Unlock()
    tb.refill()
    if tb.tokens >= 1 {
        tb.tokens--
        return true
    }
    return false
}

// Wait: blocking — waits until a token is available or context cancelled
func (tb *TokenBucket) Wait(ctx context.Context) error {
    for {
        tb.mu.Lock()
        tb.refill()
        if tb.tokens >= 1 {
            tb.tokens--
            tb.mu.Unlock()
            return nil
        }
        // Calculate wait time for next token
        waitTime := time.Duration((1 - tb.tokens) / tb.refillRate * float64(time.Second))
        tb.mu.Unlock()

        select {
        case <-time.After(waitTime):
            // retry
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}

func min(a, b float64) float64 {
    if a < b { return a }
    return b
}
```

**Real-world Relevance:** API rate limiting, resource access control.

---

### Problem 2: Per-Key Rate Limiter with Cleanup

**Difficulty:** Medium-Hard  
**Problem:** Rate limit per API key with automatic cleanup of inactive keys.

```go
type PerKeyLimiter struct {
    mu       sync.Mutex
    limiters map[string]*entry
    rate     rate.Limit
    burst    int
}

type entry struct {
    limiter  *rate.Limiter
    lastSeen time.Time
}

func NewPerKeyLimiter(r rate.Limit, burst int) *PerKeyLimiter {
    pkl := &PerKeyLimiter{
        limiters: make(map[string]*entry),
        rate:     r,
        burst:    burst,
    }
    go pkl.cleanup()
    return pkl
}

func (pkl *PerKeyLimiter) GetLimiter(key string) *rate.Limiter {
    pkl.mu.Lock()
    defer pkl.mu.Unlock()
    if e, ok := pkl.limiters[key]; ok {
        e.lastSeen = time.Now()
        return e.limiter
    }
    limiter := rate.NewLimiter(pkl.rate, pkl.burst)
    pkl.limiters[key] = &entry{limiter: limiter, lastSeen: time.Now()}
    return limiter
}

func (pkl *PerKeyLimiter) cleanup() {
    ticker := time.NewTicker(time.Minute)
    defer ticker.Stop()
    for range ticker.C {
        pkl.mu.Lock()
        for key, e := range pkl.limiters {
            if time.Since(e.lastSeen) > 10*time.Minute {
                delete(pkl.limiters, key)
            }
        }
        pkl.mu.Unlock()
    }
}
```

**Variations:** Configurable per-key rates (free vs paid tier), distributed with Redis.  
**Real-world Relevance:** API gateway rate limiting (Kong, Envoy).

---

### Problem 3: Sliding Window Rate Limiter

**Difficulty:** Hard  
**Problem:** Accurate sliding window counter.

```go
type SlidingWindowLimiter struct {
    mu         sync.Mutex
    timestamps []time.Time
    limit      int
    window     time.Duration
}

func NewSlidingWindow(limit int, window time.Duration) *SlidingWindowLimiter {
    return &SlidingWindowLimiter{limit: limit, window: window}
}

func (s *SlidingWindowLimiter) Allow() bool {
    s.mu.Lock()
    defer s.mu.Unlock()
    now := time.Now()
    cutoff := now.Add(-s.window)

    // Remove expired timestamps
    idx := sort.Search(len(s.timestamps), func(i int) bool {
        return s.timestamps[i].After(cutoff)
    })
    s.timestamps = s.timestamps[idx:]

    if len(s.timestamps) >= s.limit {
        return false
    }
    s.timestamps = append(s.timestamps, now)
    return true
}
```

**Trade-off:** O(N) memory per window, but most accurate. For memory efficiency, use weighted fixed windows.

---

### Problem 4: Adaptive Rate Limiter (AIMD)

**Difficulty:** Hard (FAANG-level)  
**Problem:** Rate limiter that adjusts based on downstream responses.

```go
type AdaptiveLimiter struct {
    mu          sync.Mutex
    currentRate float64
    minRate     float64
    maxRate     float64
    limiter     *rate.Limiter
    burst       int
}

func NewAdaptiveLimiter(initialRate, minRate, maxRate float64, burst int) *AdaptiveLimiter {
    al := &AdaptiveLimiter{
        currentRate: initialRate,
        minRate:     minRate,
        maxRate:     maxRate,
        burst:       burst,
        limiter:     rate.NewLimiter(rate.Limit(initialRate), burst),
    }
    return al
}

// Call after a successful response
func (al *AdaptiveLimiter) RecordSuccess() {
    al.mu.Lock()
    defer al.mu.Unlock()
    // Additive increase
    al.currentRate = math.Min(al.currentRate+1, al.maxRate)
    al.limiter.SetLimit(rate.Limit(al.currentRate))
}

// Call after a rate-limit error (429) or server error (503)
func (al *AdaptiveLimiter) RecordFailure() {
    al.mu.Lock()
    defer al.mu.Unlock()
    // Multiplicative decrease
    al.currentRate = math.Max(al.currentRate*0.5, al.minRate)
    al.limiter.SetLimit(rate.Limit(al.currentRate))
}

func (al *AdaptiveLimiter) Wait(ctx context.Context) error {
    return al.limiter.Wait(ctx)
}
```

**Real-world Relevance:** Netflix Concurrency Limits library, TCP congestion control, adaptive client throttling.

---

# 3. BACKPRESSURE

## Concept Overview

Backpressure is a flow-control mechanism where downstream components signal upstream to slow down when overwhelmed.

**Go channels naturally provide backpressure:**
- Bounded channel: when full, sender blocks → producer slows down.
- This propagates up the pipeline: if stage 3 is slow, stage 2 fills its output buffer, then stage 1 fills its buffer, etc.

**Strategies when backpressure is needed:**

| Strategy | Behavior | Use When |
|----------|----------|----------|
| Block | Sender blocks when buffer full | Data cannot be lost |
| Drop newest | Discard new items when full | Latest data is less important |
| Drop oldest | Discard old items when full | Latest data is more important |
| Load shed | Reject with error (503) | Client can retry |
| Sample | Process every Nth item | High volume, approximate is OK |

## Common Interview Questions

1. What is backpressure and why is it important?
2. How do Go channels naturally provide backpressure?
3. What are the strategies for handling backpressure?
4. How does buffer size relate to backpressure?
5. What is the difference between backpressure and rate limiting? (Backpressure = reactive feedback; rate limiting = proactive throttling)
6. How do you implement backpressure in a pipeline?
7. How do you monitor backpressure? (`len(ch)/cap(ch)` as fullness percentage)
8. What happens when backpressure is not handled? (OOM, cascading failure, unbounded latency)

## Coding Problems

### Problem 1: Backpressure-Aware Pipeline

**Difficulty:** Medium  
```go
type PipelineMonitor struct {
    stages []StageInfo
}

type StageInfo struct {
    Name     string
    Channel  interface{} // channel to monitor
    Len      func() int
    Cap      func() int
}

func (pm *PipelineMonitor) Report() {
    for _, s := range pm.stages {
        used := s.Len()
        total := s.Cap()
        pct := float64(used) / float64(total) * 100
        status := "OK"
        if pct > 80 {
            status = "BACKPRESSURE"
        }
        fmt.Printf("[%s] %d/%d (%.1f%%) %s\n", s.Name, used, total, pct, status)
    }
}
```

---

### Problem 2: Load Shedder

**Difficulty:** Hard  
**Problem:** Implement an HTTP handler that sheds load under pressure.

```go
type LoadShedder struct {
    queue    chan func()
    workers  int
    maxQueue int
}

func NewLoadShedder(workers, maxQueue int) *LoadShedder {
    ls := &LoadShedder{
        queue:    make(chan func(), maxQueue),
        workers:  workers,
        maxQueue: maxQueue,
    }
    for i := 0; i < workers; i++ {
        go func() {
            for fn := range ls.queue {
                fn()
            }
        }()
    }
    return ls
}

func (ls *LoadShedder) Handle(w http.ResponseWriter, r *http.Request) {
    done := make(chan struct{})
    task := func() {
        defer close(done)
        // process request
        w.Write([]byte("OK"))
    }

    select {
    case ls.queue <- task:
        <-done // wait for processing
    default:
        // Queue full — shed load
        w.WriteHeader(http.StatusServiceUnavailable)
        w.Write([]byte("server overloaded, try again later"))
    }
}
```

**Real-world Relevance:** HTTP server overload protection, API gateway load shedding, Envoy circuit breaking.

---

# 4. SEMAPHORE PATTERN

## Concept Overview

A semaphore controls access to a resource by maintaining a count of available permits.

**Binary semaphore** (1 permit) = mutex.  
**Counting semaphore** (N permits) = at most N concurrent accesses.

**Go implementations:**
1. **Buffered channel** — simple, idiomatic.
2. **`golang.org/x/sync/semaphore`** — weighted, context-aware, production-grade.

## Common Interview Questions

1. What is a semaphore and how do you implement one in Go?
2. Difference between binary semaphore and mutex?
3. How do you implement a weighted semaphore?
4. When would you use a semaphore vs worker pool?
5. How do you implement a semaphore with timeout?
6. What is `golang.org/x/sync/semaphore`?
7. How do you handle semaphore acquisition failures?
8. Relationship between semaphores and resource pools?

## Coding Problems

### Problem 1: Channel-Based Semaphore

**Difficulty:** Easy-Medium  
```go
type Semaphore struct {
    ch chan struct{}
}

func NewSemaphore(n int) *Semaphore {
    return &Semaphore{ch: make(chan struct{}, n)}
}

func (s *Semaphore) Acquire(ctx context.Context) error {
    select {
    case s.ch <- struct{}{}:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

func (s *Semaphore) Release() {
    <-s.ch
}

func (s *Semaphore) TryAcquire() bool {
    select {
    case s.ch <- struct{}{}:
        return true
    default:
        return false
    }
}

// Usage: limit concurrent downloads
func downloadAll(ctx context.Context, urls []string, maxConcurrent int) {
    sem := NewSemaphore(maxConcurrent)
    var wg sync.WaitGroup

    for _, url := range urls {
        wg.Add(1)
        go func(u string) {
            defer wg.Done()
            if err := sem.Acquire(ctx); err != nil {
                return
            }
            defer sem.Release()
            download(u)
        }(url)
    }
    wg.Wait()
}
```

---

### Problem 2: Weighted Semaphore

**Difficulty:** Medium  
**Problem:** Different operations consume different numbers of permits.

```go
type WeightedSemaphore struct {
    mu       sync.Mutex
    cond     *sync.Cond
    permits  int64
    maxPerms int64
}

func NewWeightedSemaphore(maxPermits int64) *WeightedSemaphore {
    ws := &WeightedSemaphore{permits: maxPermits, maxPerms: maxPermits}
    ws.cond = sync.NewCond(&ws.mu)
    return ws
}

func (ws *WeightedSemaphore) Acquire(ctx context.Context, weight int64) error {
    ws.mu.Lock()
    defer ws.mu.Unlock()
    for ws.permits < weight {
        // Check context before waiting
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }
        ws.cond.Wait()
    }
    ws.permits -= weight
    return nil
}

func (ws *WeightedSemaphore) Release(weight int64) {
    ws.mu.Lock()
    defer ws.mu.Unlock()
    ws.permits += weight
    ws.cond.Broadcast()
}
```

**Use case:** Read operations cost 1 permit, write operations cost 5 permits, from a pool of 10.

**Real-world Relevance:** Database connection pools with query cost, memory-bounded operations, I/O scheduling.

---

### Problem 3: Resource Governor

**Difficulty:** Hard  
**Problem:** Multi-resource semaphore with priority and preemption.

```go
type ResourceRequest struct {
    CPU      int
    Memory   int
    IO       int
    Priority int
}

type ResourceGovernor struct {
    mu        sync.Mutex
    cond      *sync.Cond
    available ResourceRequest // currently available
    capacity  ResourceRequest
}

func (rg *ResourceGovernor) Acquire(ctx context.Context, req ResourceRequest) error {
    rg.mu.Lock()
    defer rg.mu.Unlock()
    for !rg.canFulfill(req) {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }
        rg.cond.Wait()
    }
    rg.available.CPU -= req.CPU
    rg.available.Memory -= req.Memory
    rg.available.IO -= req.IO
    return nil
}

func (rg *ResourceGovernor) Release(req ResourceRequest) {
    rg.mu.Lock()
    defer rg.mu.Unlock()
    rg.available.CPU += req.CPU
    rg.available.Memory += req.Memory
    rg.available.IO += req.IO
    rg.cond.Broadcast()
}

func (rg *ResourceGovernor) canFulfill(req ResourceRequest) bool {
    return rg.available.CPU >= req.CPU &&
        rg.available.Memory >= req.Memory &&
        rg.available.IO >= req.IO
}
```

**Real-world Relevance:** Container resource management, query admission control (CockroachDB), cloud resource scheduling.

---

# 5. FUTURE/PROMISE PATTERN

## Concept Overview

A Future represents a value that will be available asynchronously. Go doesn't have built-in futures, but they're easily implemented with channels.

**Go idiomatic alternatives:**
- Channels (`<-chan Result`)
- `errgroup.Group`
- `sync.OnceValue` (Go 1.21+)

**Why futures matter in interviews:** Tests understanding of async composition, which is key in microservice architectures.

## Common Interview Questions

1. How do you implement the future/promise pattern in Go?
2. What is the idiomatic Go alternative to futures? (Channels + goroutines)
3. How do you compose multiple futures?
4. How do you implement async/await semantics in Go?
5. Difference between a future and a channel? (Future resolves once; channel is a stream)
6. How do you handle future cancellation?
7. How do you handle future errors?
8. How do you implement a future that can be awaited multiple times?

## Coding Problems

### Problem 1: Generic Future Implementation

**Difficulty:** Medium  
```go
type Future[T any] struct {
    ch   chan struct{}
    val  T
    err  error
    once sync.Once
}

func NewFuture[T any](fn func() (T, error)) *Future[T] {
    f := &Future[T]{ch: make(chan struct{})}
    go func() {
        f.val, f.err = fn()
        close(f.ch) // signal completion
    }()
    return f
}

func (f *Future[T]) Get() (T, error) {
    <-f.ch // block until done
    return f.val, f.err
}

func (f *Future[T]) GetWithTimeout(d time.Duration) (T, error) {
    select {
    case <-f.ch:
        return f.val, f.err
    case <-time.After(d):
        var zero T
        return zero, errors.New("timeout")
    }
}

func (f *Future[T]) Done() bool {
    select {
    case <-f.ch:
        return true
    default:
        return false
    }
}
```

**Key insight:** Closing a channel broadcasts to all waiters. This is why `close(f.ch)` works for multiple concurrent `Get()` calls — once closed, all `<-f.ch` return immediately.

---

### Problem 2: WhenAll / WhenAny

**Difficulty:** Medium-Hard  
```go
func WhenAll[T any](futures ...*Future[T]) *Future[[]T] {
    return NewFuture(func() ([]T, error) {
        results := make([]T, len(futures))
        var firstErr error
        var mu sync.Mutex
        var wg sync.WaitGroup

        for i, f := range futures {
            wg.Add(1)
            go func(idx int, fut *Future[T]) {
                defer wg.Done()
                val, err := fut.Get()
                mu.Lock()
                defer mu.Unlock()
                if err != nil && firstErr == nil {
                    firstErr = err
                }
                results[idx] = val
            }(i, f)
        }
        wg.Wait()
        return results, firstErr
    })
}

func WhenAny[T any](futures ...*Future[T]) *Future[T] {
    return NewFuture(func() (T, error) {
        ch := make(chan struct {
            val T
            err error
        }, 1)

        for _, f := range futures {
            go func(fut *Future[T]) {
                val, err := fut.Get()
                select {
                case ch <- struct{ val T; err error }{val, err}:
                default:
                }
            }(f)
        }

        result := <-ch
        return result.val, result.err
    })
}
```

**Real-world Relevance:** Parallel API aggregation, hedged requests, microservice orchestration.

---

### Problem 3: Async Pipeline with Futures

**Difficulty:** Hard  
**Problem:** Chain async operations: `fetchUser → fetchOrders → calculateTotal → formatReport`

```go
func Then[T, U any](f *Future[T], fn func(T) (U, error)) *Future[U] {
    return NewFuture(func() (U, error) {
        val, err := f.Get()
        if err != nil {
            var zero U
            return zero, err
        }
        return fn(val)
    })
}

// Usage
func buildReport(userID string) *Future[string] {
    userFuture := NewFuture(func() (User, error) {
        return fetchUser(userID)
    })

    ordersFuture := Then(userFuture, func(user User) ([]Order, error) {
        return fetchOrders(user.ID)
    })

    totalFuture := Then(ordersFuture, func(orders []Order) (float64, error) {
        return calculateTotal(orders), nil
    })

    reportFuture := Then(totalFuture, func(total float64) (string, error) {
        return formatReport(total), nil
    })

    return reportFuture
}
```

**Real-world Relevance:** API orchestration, BFF patterns, microservice composition.

---

# 6. EVENT PROCESSING

## Concept Overview

Event-driven architecture processes discrete events (state changes, messages, signals) asynchronously. In Go, this typically involves goroutines + channels as the event bus.

**Patterns:**
- **Event Loop:** Single goroutine processing events sequentially from a channel.
- **Event Dispatcher:** Routes events to registered handlers by type.
- **Event Sourcing:** Store all events; rebuild state by replaying.
- **CQRS:** Separate read and write models, synchronized via events.
- **Complex Event Processing (CEP):** Detect patterns across event streams.

## Common Interview Questions

1. How do you implement an event loop in Go?
2. What is event sourcing and how do you implement it concurrently?
3. How do you handle event ordering across goroutines?
4. What is the reactor pattern and how does it map to Go?
5. How do you implement idempotent event handling?
6. How do you handle event replay?
7. How do you handle out-of-order events?
8. How do you implement CQRS with goroutines?

## Coding Problems

### Problem 1: Type-Safe Event Dispatcher

**Difficulty:** Medium  
```go
type Event interface {
    EventType() string
}

type Dispatcher struct {
    mu       sync.RWMutex
    handlers map[string][]func(Event)
}

func NewDispatcher() *Dispatcher {
    return &Dispatcher{handlers: make(map[string][]func(Event))}
}

func (d *Dispatcher) On(eventType string, handler func(Event)) {
    d.mu.Lock()
    defer d.mu.Unlock()
    d.handlers[eventType] = append(d.handlers[eventType], handler)
}

func (d *Dispatcher) Dispatch(event Event) {
    d.mu.RLock()
    handlers := make([]func(Event), len(d.handlers[event.EventType()]))
    copy(handlers, d.handlers[event.EventType()])
    d.mu.RUnlock()

    for _, h := range handlers {
        go func(handler func(Event)) {
            defer func() {
                if r := recover(); r != nil {
                    log.Printf("handler panic: %v", r)
                }
            }()
            handler(event)
        }(h)
    }
}
```

---

### Problem 2: Event Store with Concurrent Readers

**Difficulty:** Hard  
```go
type EventStore struct {
    mu     sync.RWMutex
    events []Event
    subs   []chan Event
}

func (es *EventStore) Append(event Event) {
    es.mu.Lock()
    defer es.mu.Unlock()
    es.events = append(es.events, event)
    // Notify subscribers
    for _, sub := range es.subs {
        select {
        case sub <- event:
        default: // don't block on slow subscribers
        }
    }
}

func (es *EventStore) Subscribe(fromOffset int) <-chan Event {
    ch := make(chan Event, 100)
    es.mu.Lock()
    defer es.mu.Unlock()
    // Send historical events
    go func() {
        es.mu.RLock()
        for i := fromOffset; i < len(es.events); i++ {
            ch <- es.events[i]
        }
        es.mu.RUnlock()
    }()
    es.subs = append(es.subs, ch)
    return ch
}

func (es *EventStore) ReadFrom(offset int) []Event {
    es.mu.RLock()
    defer es.mu.RUnlock()
    if offset >= len(es.events) {
        return nil
    }
    result := make([]Event, len(es.events)-offset)
    copy(result, es.events[offset:])
    return result
}
```

---

### Problem 3: Complex Event Processing — Pattern Detection

**Difficulty:** Hard (FAANG-level)  
**Problem:** Detect "3 or more error events within 1 minute" across a stream.

```go
type PatternMatcher struct {
    window    time.Duration
    threshold int
    events    []time.Time
    mu        sync.Mutex
    alertCh   chan Alert
}

func NewPatternMatcher(window time.Duration, threshold int) *PatternMatcher {
    return &PatternMatcher{
        window:    window,
        threshold: threshold,
        alertCh:   make(chan Alert, 10),
    }
}

func (pm *PatternMatcher) Process(event Event) {
    if event.EventType() != "error" {
        return
    }

    pm.mu.Lock()
    defer pm.mu.Unlock()

    now := time.Now()
    pm.events = append(pm.events, now)

    // Remove events outside window
    cutoff := now.Add(-pm.window)
    i := 0
    for i < len(pm.events) && pm.events[i].Before(cutoff) {
        i++
    }
    pm.events = pm.events[i:]

    // Check threshold
    if len(pm.events) >= pm.threshold {
        select {
        case pm.alertCh <- Alert{
            Message: fmt.Sprintf("%d errors in %v", len(pm.events), pm.window),
            Time:    now,
        }:
        default:
        }
        pm.events = nil // reset after alert
    }
}
```

**Variations:** Pattern "A followed by B within 5 seconds," session windowing, temporal joins.  
**Real-world Relevance:** Fraud detection, monitoring alerting, IoT event processing, security event correlation.

---

## Summary: Pattern Selection Guide

| Need | Pattern | Key Primitive |
|------|---------|---------------|
| Broadcast to many | Pub-Sub | Channel per subscriber |
| Control request rate | Rate Limiting | Token bucket / `x/time/rate` |
| Slow down producers | Backpressure | Bounded channels |
| Limit concurrent access | Semaphore | Buffered channel / `x/sync/semaphore` |
| Async result | Future/Promise | Channel + sync.Once |
| React to events | Event Processing | Dispatcher + goroutines |
| Transform data in stages | Pipeline | Channel chain |
| Distribute work | Fan-Out / Worker Pool | Shared job channel |
| Merge results | Fan-In | Goroutine per source + WaitGroup |
