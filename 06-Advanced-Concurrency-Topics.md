# Part 6: Advanced Topics — Concurrent Data Structures, Resource Pools, Distributed Concurrency, High-Throughput Systems

> Go Concurrency Interview Guide — From Beginner to Staff Engineer

---

## Table of Contents

1. [Concurrent Data Structures](#1-concurrent-data-structures)
2. [Resource Pool Management](#2-resource-pool-management)
3. [Distributed Concurrency Concepts](#3-distributed-concurrency-concepts)
4. [High-Throughput Systems](#4-high-throughput-systems)

---

# 1. CONCURRENT DATA STRUCTURES

## Concept Overview

Building thread-safe data structures requires choosing the right synchronization strategy:

| Strategy | Overhead | Best For | Example |
|----------|----------|----------|---------|
| Mutex | Low-medium | Simple structures, write-heavy | Map + Mutex |
| RWMutex | Medium | Read-heavy structures | Config store |
| Lock striping | Low per shard | High concurrency maps | Sharded map |
| Copy-on-write | Write cost | Read-dominant, rare writes | Config, routing tables |
| Lock-free (CAS) | Very low reads | Hot paths, simple structures | Counters, stacks |
| Immutable | Zero sync | Functional style | Value objects |

**Key Go concurrency-safe structures:**
- `sync.Map` — Optimized for two patterns: (1) key-stable maps with many reads, (2) disjoint goroutine writes.
- `sync.Pool` — Object reuse to reduce GC pressure. Items can be collected at any time.

**sync.Map internals:**
- Two maps: `read` (atomic, lock-free) and `dirty` (mutex-protected).
- Reads check `read` map first (fast path, no lock).
- Misses in `read` check `dirty` (slow path, with lock).
- After enough misses, `dirty` is promoted to `read`.
- **When sync.Map is better than map+mutex:** Key set is stable, many reads; or each goroutine writes a disjoint set of keys.
- **When map+mutex is better:** Lots of key churn, need iteration, need custom operations.

## Common Interview Questions

1. When should you use sync.Map vs map + mutex?
2. How does sync.Map work internally?
3. What is copy-on-write and when is it useful?
4. How do you make a concurrent-safe slice?
5. What is sync.Pool and when should you use it?
6. Dangers of sync.Pool? (Items collected by GC at any time — don't store state)
7. How do you implement a lock-free queue?
8. What is false sharing and how does it affect concurrent data structures?
9. How does CPU cache affect concurrent data structure design?
10. What is the ABA problem?
11. How do you reduce lock contention with sharding?
12. What is a concurrent skip list?

## Coding Problems

### Problem 1: Thread-Safe LRU Cache

**Difficulty:** Medium-Hard  
```go
type LRUCache struct {
    mu       sync.Mutex
    capacity int
    items    map[string]*list.Element
    order    *list.List
}

type entry struct {
    key   string
    value interface{}
}

func NewLRUCache(capacity int) *LRUCache {
    return &LRUCache{
        capacity: capacity,
        items:    make(map[string]*list.Element),
        order:    list.New(),
    }
}

func (c *LRUCache) Get(key string) (interface{}, bool) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if elem, ok := c.items[key]; ok {
        c.order.MoveToFront(elem)
        return elem.Value.(*entry).value, true
    }
    return nil, false
}

func (c *LRUCache) Put(key string, value interface{}) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if elem, ok := c.items[key]; ok {
        c.order.MoveToFront(elem)
        elem.Value.(*entry).value = value
        return
    }
    if c.order.Len() >= c.capacity {
        oldest := c.order.Back()
        if oldest != nil {
            c.order.Remove(oldest)
            delete(c.items, oldest.Value.(*entry).key)
        }
    }
    elem := c.order.PushFront(&entry{key: key, value: value})
    c.items[key] = elem
}
```

**Variations:** Add TTL, sharded LRU (split into N caches by key hash), size-based eviction.  
**Real-world Relevance:** Every caching layer — database query cache, DNS cache, HTTP response cache.

---

### Problem 2: Copy-on-Write Map

**Difficulty:** Medium-Hard  
```go
type COWMap[K comparable, V any] struct {
    data atomic.Pointer[map[K]V]
    mu   sync.Mutex // serialize writes
}

func NewCOWMap[K comparable, V any]() *COWMap[K, V] {
    m := make(map[K]V)
    c := &COWMap[K, V]{}
    c.data.Store(&m)
    return c
}

// Lock-free read
func (c *COWMap[K, V]) Get(key K) (V, bool) {
    m := *c.data.Load()
    v, ok := m[key]
    return v, ok
}

// Serialized write: copy, modify, swap
func (c *COWMap[K, V]) Set(key K, value V) {
    c.mu.Lock()
    defer c.mu.Unlock()
    old := *c.data.Load()
    newMap := make(map[K]V, len(old)+1)
    for k, v := range old {
        newMap[k] = v
    }
    newMap[key] = value
    c.data.Store(&newMap)
}
```

**When to use:** Configuration stores, routing tables, feature flags — anything with very rare writes and many concurrent reads.

---

### Problem 3: Concurrent Ring Buffer

**Difficulty:** Medium  
```go
type RingBuffer[T any] struct {
    mu   sync.Mutex
    buf  []T
    head int
    tail int
    full bool
    size int
}

func NewRingBuffer[T any](size int) *RingBuffer[T] {
    return &RingBuffer[T]{buf: make([]T, size), size: size}
}

func (rb *RingBuffer[T]) Write(item T) bool {
    rb.mu.Lock()
    defer rb.mu.Unlock()
    if rb.full {
        return false
    }
    rb.buf[rb.tail] = item
    rb.tail = (rb.tail + 1) % rb.size
    rb.full = rb.tail == rb.head
    return true
}

func (rb *RingBuffer[T]) Read() (T, bool) {
    rb.mu.Lock()
    defer rb.mu.Unlock()
    if rb.head == rb.tail && !rb.full {
        var zero T
        return zero, false
    }
    item := rb.buf[rb.head]
    rb.head = (rb.head + 1) % rb.size
    rb.full = false
    return item, true
}
```

**Real-world Relevance:** Network buffers, logging, metrics collection.

---

### Problem 4: High-Performance Concurrent Hash Map

**Difficulty:** Hard (Staff-level)  
```go
const numShards = 64

// Pad to avoid false sharing (CPU cache line = 64 bytes typically)
type paddedShard[V any] struct {
    sync.RWMutex
    m map[string]V
    _ [64 - 24]byte // padding to fill cache line
}

type ConcurrentMap[V any] struct {
    shards [numShards]paddedShard[V]
}

func NewConcurrentMap[V any]() *ConcurrentMap[V] {
    cm := &ConcurrentMap[V]{}
    for i := range cm.shards {
        cm.shards[i].m = make(map[string]V)
    }
    return cm
}

func (cm *ConcurrentMap[V]) shard(key string) *paddedShard[V] {
    h := fnv.New32a()
    h.Write([]byte(key))
    return &cm.shards[h.Sum32()%numShards]
}

func (cm *ConcurrentMap[V]) Get(key string) (V, bool) {
    s := cm.shard(key)
    s.RLock()
    v, ok := s.m[key]
    s.RUnlock()
    return v, ok
}

func (cm *ConcurrentMap[V]) Set(key string, val V) {
    s := cm.shard(key)
    s.Lock()
    s.m[key] = val
    s.Unlock()
}

func (cm *ConcurrentMap[V]) Delete(key string) {
    s := cm.shard(key)
    s.Lock()
    delete(s.m, key)
    s.Unlock()
}

// Iteration requires locking all shards
func (cm *ConcurrentMap[V]) ForEach(fn func(key string, val V) bool) {
    for i := range cm.shards {
        s := &cm.shards[i]
        s.RLock()
        for k, v := range s.m {
            if !fn(k, v) {
                s.RUnlock()
                return
            }
        }
        s.RUnlock()
    }
}
```

**Key details:**
- **Cache line padding** prevents false sharing between shards on different CPU cores.
- **64 shards** reduces contention to ~1/64th.
- **Per-shard RWMutex** allows concurrent reads within a shard.

---

# 2. RESOURCE POOL MANAGEMENT

## Concept Overview

Resource pools maintain a set of reusable resources (connections, objects, handles) to avoid the cost of creation/destruction.

**Pool types:**
- **Fixed:** Exactly N resources, never more or fewer.
- **Elastic:** Min to Max resources, scales with demand.
- **sync.Pool:** Object recycling for GC reduction. NOT a connection pool (GC can reclaim items).

**Key concerns:**
- Health checking (validate before use).
- Idle timeout (close resources sitting unused too long).
- Max lifetime (prevent using very old connections).
- Queue for acquisition (FIFO, LIFO, priority).
- Graceful drain for shutdown.

## Common Interview Questions

1. How do you implement a connection pool?
2. Fixed-size vs elastic pool?
3. How do you handle pool exhaustion?
4. How do you implement idle resource cleanup?
5. How do you handle unhealthy resources?
6. sync.Pool vs custom pool?
7. How do you implement connection pooling for databases?
8. What is pool draining?
9. How do you monitor pool health?
10. Optimal pool size formula? (Little's Law: `pool_size = throughput * latency`)

## Coding Problems

### Problem 1: Generic Resource Pool

**Difficulty:** Medium-Hard  
```go
type Pool[T any] struct {
    idle     chan T
    factory  func() (T, error)
    validate func(T) bool
    destroy  func(T)
    mu       sync.Mutex
    size     int
    maxSize  int
}

func NewPool[T any](maxSize int, factory func() (T, error), validate func(T) bool, destroy func(T)) *Pool[T] {
    return &Pool[T]{
        idle:     make(chan T, maxSize),
        factory:  factory,
        validate: validate,
        destroy:  destroy,
        maxSize:  maxSize,
    }
}

func (p *Pool[T]) Get(ctx context.Context) (T, error) {
    // Try idle resource first
    select {
    case r := <-p.idle:
        if p.validate(r) {
            return r, nil
        }
        p.destroy(r)
        p.mu.Lock()
        p.size--
        p.mu.Unlock()
        return p.Get(ctx) // try again
    default:
    }

    // Try creating new
    p.mu.Lock()
    if p.size < p.maxSize {
        p.size++
        p.mu.Unlock()
        r, err := p.factory()
        if err != nil {
            p.mu.Lock()
            p.size--
            p.mu.Unlock()
            var zero T
            return zero, err
        }
        return r, nil
    }
    p.mu.Unlock()

    // Wait for one to be returned
    select {
    case r := <-p.idle:
        if p.validate(r) {
            return r, nil
        }
        p.destroy(r)
        p.mu.Lock()
        p.size--
        p.mu.Unlock()
        return p.Get(ctx)
    case <-ctx.Done():
        var zero T
        return zero, ctx.Err()
    }
}

func (p *Pool[T]) Put(r T) {
    if !p.validate(r) {
        p.destroy(r)
        p.mu.Lock()
        p.size--
        p.mu.Unlock()
        return
    }
    select {
    case p.idle <- r:
    default:
        p.destroy(r) // pool full
        p.mu.Lock()
        p.size--
        p.mu.Unlock()
    }
}

func (p *Pool[T]) Drain() {
    close(p.idle)
    for r := range p.idle {
        p.destroy(r)
    }
}
```

---

### Problem 2: Connection Pool with Idle Timeout

**Difficulty:** Hard  
```go
type timedResource[T any] struct {
    resource T
    idleSince time.Time
}

type TimedPool[T any] struct {
    pool        *Pool[T]
    idleTimeout time.Duration
    maxLifetime time.Duration
    stop        chan struct{}
}

func (tp *TimedPool[T]) startCleaner() {
    go func() {
        ticker := time.NewTicker(tp.idleTimeout / 2)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                tp.evictIdle()
            case <-tp.stop:
                return
            }
        }
    }()
}

func (tp *TimedPool[T]) evictIdle() {
    // Drain and re-add non-expired items
    for {
        select {
        case r := <-tp.pool.idle:
            // Check if resource has been idle too long
            // (In a real impl, wrap resource with timestamp)
            tp.pool.idle <- r // put back for now
            return
        default:
            return
        }
    }
}
```

**Real-world Relevance:** `database/sql` connection pool, Redis client pools, gRPC connection pools.

---

# 3. DISTRIBUTED CONCURRENCY CONCEPTS

## Concept Overview

Distributed concurrency extends local concurrency primitives across multiple machines. This introduces new challenges: network partitions, clock skew, partial failures.

**Key concepts:**
| Concept | Local Equivalent | Distributed Challenge |
|---------|-----------------|----------------------|
| Mutex | `sync.Mutex` | Network delay, leader failure |
| Semaphore | `chan struct{}` | Coordination service required |
| Compare-and-swap | `atomic.CompareAndSwap` | Requires consensus |
| Leader election | N/A | Split brain, fencing |
| Transactions | Mutex around operations | 2PC, Saga, distributed locks |

## Common Interview Questions

1. How do you implement distributed locking? (Redis SET NX EX, etcd leases, ZooKeeper)
2. What is the Redlock algorithm? (Lock across N/2+1 Redis nodes)
3. How do you implement leader election? (etcd lease, Raft consensus)
4. Difference between distributed and local locks?
5. How do you handle lock holder failure? (TTL, lease expiry)
6. What is a fencing token? (Monotonic counter to prevent stale lock holders from modifying data)
7. How do you implement a distributed rate limiter? (Redis INCR + EXPIRE, or sliding window with sorted sets)
8. What is the Saga pattern? (Series of local transactions with compensating actions)
9. How do you handle "exactly-once"? (Idempotency keys + deduplication)
10. Optimistic vs pessimistic concurrency control?
11. How do you implement a distributed work queue?
12. What is the CAP theorem and how does it affect concurrent design?

## Coding Problems

### Problem 1: Distributed Lock with Redis

**Difficulty:** Hard  
```go
type DistributedLock struct {
    client  *redis.Client
    key     string
    value   string // unique per lock holder (UUID)
    ttl     time.Duration
    stopCh  chan struct{}
}

func NewDistributedLock(client *redis.Client, key string, ttl time.Duration) *DistributedLock {
    return &DistributedLock{
        client: client,
        key:    key,
        value:  uuid.New().String(),
        ttl:    ttl,
        stopCh: make(chan struct{}),
    }
}

func (dl *DistributedLock) Lock(ctx context.Context) error {
    for {
        ok, err := dl.client.SetNX(ctx, dl.key, dl.value, dl.ttl).Result()
        if err != nil {
            return err
        }
        if ok {
            go dl.renewLoop() // keep lock alive
            return nil
        }
        select {
        case <-time.After(100 * time.Millisecond): // retry
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}

func (dl *DistributedLock) renewLoop() {
    ticker := time.NewTicker(dl.ttl / 3)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            dl.client.Expire(context.Background(), dl.key, dl.ttl)
        case <-dl.stopCh:
            return
        }
    }
}

// Unlock uses Lua script to ensure we only delete our own lock
func (dl *DistributedLock) Unlock(ctx context.Context) error {
    close(dl.stopCh)
    script := `
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
    `
    _, err := dl.client.Eval(ctx, script, []string{dl.key}, dl.value).Result()
    return err
}
```

**Why Lua script for unlock:** Without it, a race exists: check value → delete is not atomic. Another client could acquire the lock between check and delete.

---

### Problem 2: Leader Election with Heartbeats

**Difficulty:** Hard  
```go
type LeaderElection struct {
    mu        sync.Mutex
    isLeader  bool
    leaderID  string
    myID      string
    store     KVStore // etcd or Redis
    leaseKey  string
    leaseTTL  time.Duration
    onElected func()
    onLost    func()
}

func (le *LeaderElection) Run(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            le.resign(context.Background())
            return
        default:
        }

        // Try to become leader
        acquired, err := le.store.SetNX(ctx, le.leaseKey, le.myID, le.leaseTTL)
        if err != nil {
            time.Sleep(time.Second)
            continue
        }

        if acquired {
            le.mu.Lock()
            le.isLeader = true
            le.mu.Unlock()
            le.onElected()
            le.runAsLeader(ctx)
            le.mu.Lock()
            le.isLeader = false
            le.mu.Unlock()
            le.onLost()
        } else {
            time.Sleep(le.leaseTTL / 2) // wait and retry
        }
    }
}

func (le *LeaderElection) runAsLeader(ctx context.Context) {
    ticker := time.NewTicker(le.leaseTTL / 3)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            // Renew lease
            ok, err := le.store.CompareAndExtend(ctx, le.leaseKey, le.myID, le.leaseTTL)
            if err != nil || !ok {
                return // lost leadership
            }
        case <-ctx.Done():
            return
        }
    }
}
```

**Real-world Relevance:** Kubernetes controller manager, scheduler HA, Consul leader election.

---

### Problem 3: Saga Orchestrator

**Difficulty:** Hard (Staff-level)  
```go
type SagaStep struct {
    Name       string
    Execute    func(ctx context.Context) error
    Compensate func(ctx context.Context) error
}

type SagaOrchestrator struct {
    steps []SagaStep
}

func (s *SagaOrchestrator) Run(ctx context.Context) error {
    var completed []SagaStep

    for _, step := range s.steps {
        log.Printf("executing step: %s", step.Name)
        if err := step.Execute(ctx); err != nil {
            log.Printf("step %s failed: %v — starting compensation", step.Name, err)
            // Compensate in reverse order
            s.compensate(ctx, completed)
            return fmt.Errorf("saga failed at step %s: %w", step.Name, err)
        }
        completed = append(completed, step)
    }

    return nil
}

func (s *SagaOrchestrator) compensate(ctx context.Context, completed []SagaStep) {
    // Use background context — compensation must complete
    compCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    for i := len(completed) - 1; i >= 0; i-- {
        step := completed[i]
        log.Printf("compensating step: %s", step.Name)
        if err := step.Compensate(compCtx); err != nil {
            log.Printf("CRITICAL: compensation for %s failed: %v", step.Name, err)
            // In production: alert, persist for manual intervention
        }
    }
}

// Usage
func processOrder(ctx context.Context, order Order) error {
    saga := &SagaOrchestrator{
        steps: []SagaStep{
            {
                Name:       "reserve_inventory",
                Execute:    func(ctx context.Context) error { return reserveInventory(ctx, order) },
                Compensate: func(ctx context.Context) error { return releaseInventory(ctx, order) },
            },
            {
                Name:       "charge_payment",
                Execute:    func(ctx context.Context) error { return chargePayment(ctx, order) },
                Compensate: func(ctx context.Context) error { return refundPayment(ctx, order) },
            },
            {
                Name:       "ship_order",
                Execute:    func(ctx context.Context) error { return shipOrder(ctx, order) },
                Compensate: func(ctx context.Context) error { return cancelShipment(ctx, order) },
            },
        },
    }
    return saga.Run(ctx)
}
```

**Real-world Relevance:** E-commerce order processing, payment workflows, multi-service transactions.

---

# 4. HIGH-THROUGHPUT SYSTEMS

## Concept Overview

Designing Go systems for millions of operations per second requires understanding:

1. **Lock contention:** The #1 killer of throughput. Solutions: sharding, lock-free, batch.
2. **Memory allocation:** GC pressure. Solutions: sync.Pool, arena, pre-allocation.
3. **Channel overhead:** ~50-200ns per operation. Solutions: batch, direct writes for hot paths.
4. **Cache lines:** False sharing wastes CPU cycles. Solution: padding between shared variables.
5. **Goroutine overhead:** Scheduling cost. Solution: worker pools, not goroutine-per-request at extreme scale.

**Profiling tools:**
- `go tool pprof` — CPU, memory, goroutine, mutex, block profiles.
- `go test -bench` — Microbenchmarks.
- `go test -race` — Race detector (2-20x overhead, not for production).
- `runtime/trace` — Detailed execution trace with goroutine scheduling.
- `GODEBUG=schedtrace=1000` — Print scheduler state every 1000ms.

## Common Interview Questions

1. How do you design a Go service for 1M requests/second?
2. What is lock contention and how do you reduce it?
3. How do you use pprof to debug concurrency issues?
4. What is false sharing and how do you avoid it?
5. How does sync.Pool help?
6. Impact of GC on high-throughput Go services?
7. How do you implement batching for throughput?
8. How do you benchmark concurrent Go code accurately?
9. Role of GOMAXPROCS in high-throughput systems?
10. How do you reduce memory allocations in hot paths?
11. Cost of channel operations at high throughput?
12. What is zero-copy and when applicable?

## Coding Problems

### Problem 1: Sharded Counter (10M increments/sec)

**Difficulty:** Medium-Hard  
```go
const counterShards = 128

type ShardedCounter struct {
    shards [counterShards]struct {
        val atomic.Int64
        _   [56]byte // pad to cache line (64 bytes - 8 bytes for int64)
    }
}

func (sc *ShardedCounter) Inc() {
    // Use goroutine ID or random shard to distribute load
    shard := runtime_procPin() % counterShards
    runtime_procUnpin()
    sc.shards[shard].val.Add(1)
}

// Simpler version without procPin:
func (sc *ShardedCounter) IncSimple() {
    shard := fastrand() % counterShards
    sc.shards[shard].val.Add(1)
}

func (sc *ShardedCounter) Load() int64 {
    var total int64
    for i := range sc.shards {
        total += sc.shards[i].val.Load()
    }
    return total
}
```

**Why sharding helps:** With a single `atomic.Int64`, all cores compete for the same cache line. With sharding, each core typically updates a different cache line.

---

### Problem 2: Zero-Allocation Logger

**Difficulty:** Hard  
```go
type LogEntry struct {
    Level   int
    Message string
    Fields  [8]Field // fixed-size to avoid allocation
    NumFields int
    buf     [256]byte // pre-allocated buffer for formatting
}

type Field struct {
    Key   string
    Value interface{}
}

var logEntryPool = sync.Pool{
    New: func() interface{} {
        return &LogEntry{}
    },
}

type AsyncLogger struct {
    entries chan *LogEntry
    writer  io.Writer
}

func NewAsyncLogger(writer io.Writer, bufSize int) *AsyncLogger {
    l := &AsyncLogger{
        entries: make(chan *LogEntry, bufSize),
        writer:  writer,
    }
    go l.drain()
    return l
}

func (l *AsyncLogger) Log(level int, msg string, fields ...Field) {
    entry := logEntryPool.Get().(*LogEntry)
    entry.Level = level
    entry.Message = msg
    entry.NumFields = copy(entry.Fields[:], fields)

    select {
    case l.entries <- entry:
    default:
        // Drop log entry under pressure
        logEntryPool.Put(entry)
    }
}

func (l *AsyncLogger) drain() {
    batch := make([]*LogEntry, 0, 64)
    timer := time.NewTimer(100 * time.Millisecond)
    defer timer.Stop()

    for {
        select {
        case e, ok := <-l.entries:
            if !ok {
                l.flushBatch(batch)
                return
            }
            batch = append(batch, e)
            if len(batch) >= 64 {
                l.flushBatch(batch)
                batch = batch[:0]
                timer.Reset(100 * time.Millisecond)
            }
        case <-timer.C:
            l.flushBatch(batch)
            batch = batch[:0]
            timer.Reset(100 * time.Millisecond)
        }
    }
}

func (l *AsyncLogger) flushBatch(batch []*LogEntry) {
    for _, e := range batch {
        // Format and write...
        fmt.Fprintf(l.writer, "[%d] %s\n", e.Level, e.Message)
        logEntryPool.Put(e) // return to pool
    }
}
```

**Real-world Relevance:** zerolog, zap, high-performance logging at scale.

---

### Problem 3: High-Throughput Pipeline (1M events/sec)

**Difficulty:** Hard (FAANG-level)  
```go
type HighThroughputPipeline struct {
    inputCh    chan []Event        // batched input
    stages     []func([]Event) []Event
    batchSize  int
    workers    int
    metrics    PipelineMetrics
}

type PipelineMetrics struct {
    ingested   atomic.Int64
    processed  atomic.Int64
    dropped    atomic.Int64
    batchLatNs atomic.Int64
}

func (p *HighThroughputPipeline) Run(ctx context.Context) {
    var channels []chan []Event
    channels = append(channels, p.inputCh)

    for _, stageFn := range p.stages {
        outCh := make(chan []Event, p.workers*2)
        fn := stageFn

        // Fan-out workers for this stage
        var wg sync.WaitGroup
        for w := 0; w < p.workers; w++ {
            wg.Add(1)
            go func(in chan []Event) {
                defer wg.Done()
                for batch := range in {
                    start := time.Now()
                    result := fn(batch)
                    p.metrics.batchLatNs.Store(int64(time.Since(start)))
                    p.metrics.processed.Add(int64(len(result)))

                    select {
                    case outCh <- result:
                    case <-ctx.Done():
                        return
                    }
                }
            }(channels[len(channels)-1])
        }

        go func() {
            wg.Wait()
            close(outCh)
        }()

        channels = append(channels, outCh)
    }
}

// Batch ingestion from source
func (p *HighThroughputPipeline) Ingest(events []Event) bool {
    batch := make([]Event, len(events))
    copy(batch, events)
    select {
    case p.inputCh <- batch:
        p.metrics.ingested.Add(int64(len(batch)))
        return true
    default:
        p.metrics.dropped.Add(int64(len(batch)))
        return false
    }
}
```

**Key design decisions:**
- **Batch processing** amortizes channel overhead across many events.
- **sync.Pool** for batch slices to reduce allocation.
- **Multiple workers** per stage for horizontal scaling.
- **Bounded channels** for backpressure.
- **Atomic metrics** for lock-free monitoring.

---

### Problem 4: Lock-Free MPSC Queue

**Difficulty:** Hard (Staff-level)  
```go
type MPSCNode[T any] struct {
    value T
    next  atomic.Pointer[MPSCNode[T]]
}

type MPSCQueue[T any] struct {
    head atomic.Pointer[MPSCNode[T]]
    tail *MPSCNode[T] // only consumer accesses tail
}

func NewMPSCQueue[T any]() *MPSCQueue[T] {
    stub := &MPSCNode[T]{}
    q := &MPSCQueue[T]{tail: stub}
    q.head.Store(stub)
    return q
}

// Push — lock-free, multiple producers
func (q *MPSCQueue[T]) Push(val T) {
    node := &MPSCNode[T]{value: val}
    // Atomically swap head to our new node
    prev := q.head.Swap(node)
    // Link previous head to new node
    prev.next.Store(node)
}

// Pop — single consumer only
func (q *MPSCQueue[T]) Pop() (T, bool) {
    tail := q.tail
    next := tail.next.Load()
    if next == nil {
        var zero T
        return zero, false
    }
    q.tail = next
    return next.value, true
}
```

**Real-world Relevance:** Runtime scheduler run queues, high-performance event loops, inter-goroutine communication hot paths.

---

## Common Mistakes in High-Throughput Systems

| Mistake | Impact | Fix |
|---------|--------|-----|
| Single atomic counter under high contention | Cache line bouncing, throughput collapse | Sharded counters |
| Allocating in hot path | GC pauses, latency spikes | sync.Pool, pre-allocation |
| Unbounded goroutines | OOM, scheduler overhead | Worker pools |
| Fine-grained locking on hot data | Contention | Lock striping, lock-free |
| Not profiling before optimizing | Wrong bottleneck | Always profile first (pprof) |
| Using channels in hot path | 50-200ns overhead per op | Direct function calls, batch |
| Ignoring false sharing | ~10x slowdown | Cache line padding |
