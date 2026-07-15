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

### Q1: When should you use sync.Map vs map + mutex?

**Answer:** `sync.Map` is optimized for two specific patterns: (1) keys are written once but read many times (append-only), (2) multiple goroutines read/write disjoint sets of keys. For general-purpose concurrent maps, `map + sync.RWMutex` is usually faster. `sync.Map` avoids lock contention by using read-only snapshots, but its overhead is higher for write-heavy workloads. Always benchmark your specific use case.

**Example:**
```go
// sync.Map: good for stable, read-heavy keys
var cache sync.Map
cache.Store("config", configValue)    // written rarely
val, _ := cache.Load("config")       // read frequently — no lock

// map + RWMutex: better for general use
type SafeMap struct {
    mu sync.RWMutex
    m  map[string]string
}
func (s *SafeMap) Get(key string) string {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.m[key]
}
```

**Real-time Scenario:** A DNS cache (keys rarely change, read 100K times/sec) benefits from `sync.Map`. A session store (keys constantly created/deleted) performs better with `map + RWMutex`.

---

### Q2: How does sync.Map work internally?

**Answer:** `sync.Map` uses a two-layer structure: a **read-only map** (accessed with atomic load, no lock) and a **dirty map** (protected by mutex). Reads check the read-only map first (fast path). Misses fall through to the dirty map (slow path). When miss count exceeds dirty map size, the dirty map is promoted to the read-only map atomically. Writes go to the dirty map. This design makes reads nearly lock-free.

**Example:**
```go
// Conceptual internal structure:
type Map struct {
    mu      sync.Mutex
    read    atomic.Pointer[readOnly] // read without lock
    dirty   map[interface{}]*entry   // write with lock
    misses  int                      // promotes dirty→read when high
}

// Read fast path: atomic load, no lock
// Read slow path: lock + check dirty
// Write: lock + update dirty + possibly read
// Promotion: dirty becomes read, dirty cleared, misses reset
```

**Real-time Scenario:** Understanding the internal structure explains why `sync.Map` is fast for reads (atomic pointer load) but slow for new keys (mutex + dirty map allocation). It guided a team to use `sync.Map` for their 99% read / 1% write configuration cache.

---

### Q3: What is copy-on-write and when is it useful?

**Answer:** Copy-on-write (COW) allows concurrent readers to access data without locks. When a writer needs to modify data, it copies the entire data structure, modifies the copy, then atomically swaps the pointer. Readers see either the old or new version — never a partial update. Useful when reads vastly outnumber writes and the data structure is small enough to copy.

**Example:**
```go
type COWConfig struct {
    data atomic.Pointer[map[string]string]
}

func (c *COWConfig) Get(key string) string {
    m := c.data.Load()
    return (*m)[key] // lock-free read
}

func (c *COWConfig) Set(key, value string) {
    for {
        old := c.data.Load()
        newMap := make(map[string]string, len(*old)+1)
        for k, v := range *old { newMap[k] = v }
        newMap[key] = value
        if c.data.CompareAndSwap(old, &newMap) {
            return
        }
    }
}
```

**Real-time Scenario:** A feature flag system uses COW — flags are read 1M times/sec by request handlers (lock-free) and updated once per minute by the admin panel (copies the entire flag map and swaps).

---

### Q4: How do you make a concurrent-safe slice?

**Answer:** Slices can't be made safely concurrent by just adding a mutex — the underlying array can be shared between slices. Options: (1) mutex-protected slice (simplest), (2) copy-on-write for append-only use cases, (3) channel of operations (command pattern), (4) fixed-size with atomic access per index. For most cases, a mutex-protected slice is sufficient and simplest.

**Example:**
```go
type SafeSlice[T any] struct {
    mu    sync.RWMutex
    items []T
}

func (s *SafeSlice[T]) Append(item T) {
    s.mu.Lock()
    s.items = append(s.items, item)
    s.mu.Unlock()
}

func (s *SafeSlice[T]) Get(i int) T {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.items[i]
}

func (s *SafeSlice[T]) Snapshot() []T {
    s.mu.RLock()
    defer s.mu.RUnlock()
    cp := make([]T, len(s.items))
    copy(cp, s.items)
    return cp // safe to use without lock
}
```

**Real-time Scenario:** A metrics collector appends data points from multiple goroutines. The `Snapshot()` method is called every 10 seconds to flush metrics — it returns a copy so the flush can proceed without holding the lock.

---

### Q5: What is sync.Pool and when should you use it?

**Answer:** `sync.Pool` is a per-P (logical processor) cache of temporary objects. It reduces GC pressure by reusing allocations. Use it for: request-scoped buffers, encoder/decoder instances, temporary structs that are created and discarded frequently. Don't use it for: persistent state, things that need deterministic lifecycle, objects that must exist across GC cycles.

**Example:**
```go
var encoderPool = sync.Pool{
    New: func() any { return json.NewEncoder(nil) },
}

func marshal(v interface{}) ([]byte, error) {
    buf := bufPool.Get().(*bytes.Buffer)
    buf.Reset()
    defer bufPool.Put(buf)

    enc := encoderPool.Get().(*json.Encoder)
    enc.Reset(buf)
    defer encoderPool.Put(enc)

    err := enc.Encode(v)
    return buf.Bytes(), err
}
```

**Real-time Scenario:** An HTTP server handling 500K JSON responses/sec used sync.Pool for `bytes.Buffer` and `json.Encoder`. GC pauses dropped from 50ms to 2ms because 99% of allocations were recycled.

---

### Q6: Dangers of sync.Pool?

**Answer:** (1) **GC reclaims objects**: items in the pool may be collected at any GC cycle — never rely on them being there. (2) **No size control**: the pool grows unboundedly during traffic spikes. (3) **Must reset objects**: Put back dirty objects → data leaks between requests. (4) **Not for state**: objects have no guaranteed lifetime. (5) **Type assertions**: `Get()` returns `any`, requires assertion. (6) **Per-P sharding**: may allocate more objects than expected under high GOMAXPROCS.

**Example:**
```go
// DANGER 1: data leak — forgot to reset
buf := bufPool.Get().(*bytes.Buffer)
buf.WriteString(sensitiveData)
bufPool.Put(buf) // BUG: next Get() sees sensitive data!
// FIX: buf.Reset() before Put or after Get

// DANGER 2: relying on pool contents
var myPool = sync.Pool{New: func() any { return &Expensive{} }}
obj := myPool.Get().(*Expensive)
// DON'T assume obj has any specific state — GC may have reclaimed it
```

**Real-time Scenario:** A security audit found that response buffers put back into sync.Pool without resetting contained previous users' personal data. Another request would get that buffer and potentially leak data.

---

### Q7: How do you implement a lock-free queue?

**Answer:** A lock-free queue uses CAS (Compare-And-Swap) operations instead of mutexes. The Michael-Scott queue is the classic algorithm: a linked list with head and tail pointers updated atomically. In Go, use `atomic.Pointer[T]` for the pointers. Lock-free queues avoid blocking but are complex to implement correctly. For most Go programs, a channel or mutex-protected queue is preferred.

**Example:**
```go
type Node[T any] struct {
    value T
    next  atomic.Pointer[Node[T]]
}

type LockFreeQueue[T any] struct {
    head atomic.Pointer[Node[T]]
    tail atomic.Pointer[Node[T]]
}

func (q *LockFreeQueue[T]) Enqueue(value T) {
    node := &Node[T]{value: value}
    for {
        tail := q.tail.Load()
        next := tail.next.Load()
        if next == nil {
            if tail.next.CompareAndSwap(nil, node) {
                q.tail.CompareAndSwap(tail, node)
                return
            }
        } else {
            q.tail.CompareAndSwap(tail, next) // help advance tail
        }
    }
}
```

**Real-time Scenario:** The Go runtime itself uses lock-free queues internally for goroutine scheduling. Application code rarely needs lock-free data structures — channels provide similar functionality with much simpler code.

---

### Q8: What is false sharing and how does it affect concurrent data structures?

**Answer:** False sharing occurs when two goroutines on different CPU cores modify different variables that happen to be on the same CPU cache line (64 bytes). Each modification invalidates the other core's cache, causing expensive cache-line bouncing (100x slower). Fix: pad struct fields to cache-line boundaries so each field is on its own cache line.

**Example:**
```go
// FALSE SHARING: two goroutines contend on cache line
type Counters struct {
    goroutine1Counter int64 // these are on the same
    goroutine2Counter int64 // 64-byte cache line!
}

// FIXED: each on its own cache line
type Counters struct {
    goroutine1Counter int64
    _                 [56]byte // padding to 64 bytes
    goroutine2Counter int64
    _                 [56]byte
}
```

**Real-time Scenario:** A per-CPU counter array showed 8x worse performance than expected. Adding cache-line padding made each counter independent, restoring expected linear scaling across 8 cores.

---

### Q9: How does CPU cache affect concurrent data structure design?

**Answer:** Data accessed together should be on the same cache line (spatial locality). Data modified by different goroutines should be on different cache lines (avoid false sharing). Keep hot fields (frequently accessed) at the start of structs. Align atomically accessed fields to prevent split cache-line access. The mutex itself and the data it protects should ideally be on the same cache line.

**Example:**
```go
// GOOD: mutex and protected data on same cache line
type Optimized struct {
    mu    sync.Mutex // 8 bytes
    count int64      // 8 bytes — same cache line as mu
    // total: 16 bytes — fits in one 64-byte cache line
}

// BAD: hot and cold data mixed
type Mixed struct {
    hotCounter int64      // accessed every request
    coldConfig [4096]byte // accessed once at startup
    hotFlag    int32      // accessed every request
    // hotCounter and hotFlag are 4KB apart — different cache lines
}
```

**Real-time Scenario:** Reordering struct fields to keep hot fields together (mutex + counter + flag) on the same cache line improved throughput by 30% in a benchmark — the CPU fetched everything needed in one cache-line load.

---

### Q10: What is the ABA problem?

**Answer:** The ABA problem occurs in lock-free algorithms using CAS: a value changes from A to B and back to A. CAS sees the value is still A and succeeds, but the state may have changed semantically. In Go, this is less common due to garbage collection (pointers to deallocated objects remain valid), but it can occur with value types. Fix: use generation counters or tagged pointers.

**Example:**
```go
// ABA scenario in a lock-free stack:
// T1: reads top = Node_A (A→B→C)
// T2: pops A, pops B, pushes A back (A→C, different structure)
// T1: CAS(top, A, B) succeeds — but B is no longer valid!

// Fix: tagged pointer with generation counter
type TaggedPtr struct {
    ptr *Node
    gen uint64 // increments on every modification
}
// CAS checks both ptr AND gen — ABA is detected because gen changed
```

**Real-time Scenario:** The ABA problem is mostly theoretical in Go due to GC preventing pointer reuse, but it's a crucial concept for understanding lock-free algorithm design and for interview discussions.

---

### Q11: How do you reduce lock contention with sharding?

**Answer:** Instead of one lock for all data, split the data into N shards, each with its own lock. Route operations to a shard using a hash of the key. Contention is reduced by factor of N because goroutines accessing different shards never compete. Choose N based on expected concurrency level (typically 16-256 shards). Power-of-2 shard counts enable fast modulo with bitwise AND.

**Example:**
```go
const numShards = 256

type ShardedMap struct {
    shards [numShards]struct {
        mu   sync.RWMutex
        data map[string]string
    }
}

func (m *ShardedMap) getShard(key string) *struct {
    mu   sync.RWMutex
    data map[string]string
} {
    h := fnv.New32a()
    h.Write([]byte(key))
    return &m.shards[h.Sum32()%numShards]
}

func (m *ShardedMap) Get(key string) string {
    shard := m.getShard(key)
    shard.mu.RLock()
    defer shard.mu.RUnlock()
    return shard.data[key]
}
```

**Real-time Scenario:** A session store with 1 lock handled 10K ops/sec. Sharding to 256 locks handled 2M ops/sec — each shard processes ~8K ops/sec independently, and goroutines almost never contend.

---

### Q12: What is a concurrent skip list?

**Answer:** A concurrent skip list is a probabilistic sorted data structure that supports O(log n) search, insert, and delete with fine-grained locking or lock-free algorithms. Unlike balanced BSTs (which need global rebalancing under lock), skip lists only lock the nodes being modified. Each node has random-height "express lanes" for fast traversal. Used in databases (LevelDB, RocksDB memtable) for concurrent sorted access.

**Example:**
```go
type SkipNode struct {
    key   int
    value interface{}
    next  []*atomic.Pointer[SkipNode] // one per level
    mu    sync.Mutex                   // per-node lock
}

// Insert only locks the predecessor nodes at each level
// Other goroutines can traverse and modify different parts simultaneously
// No global lock or rebalancing needed
```

**Real-time Scenario:** A concurrent priority queue uses a skip list — 100 goroutines insert tasks with different priorities, and workers extract the highest-priority task. Fine-grained locking ensures inserters and extractors rarely contend.

---

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

### Q1: How do you implement a connection pool?

**Answer:** A connection pool manages reusable connections: a buffered channel holds idle connections, `Get()` retrieves one (or creates new if below max), `Put()` returns it. Key features: max pool size, idle timeout, health checks, and factory function for creating new connections. Go's `database/sql` has a built-in pool — study its design.

**Example:**
```go
type Pool struct {
    idle    chan *Conn
    factory func() (*Conn, error)
    maxSize int
    mu      sync.Mutex
    active  int
}

func (p *Pool) Get(ctx context.Context) (*Conn, error) {
    select {
    case conn := <-p.idle:
        if conn.IsHealthy() { return conn, nil }
        conn.Close() // discard unhealthy
    default:
    }
    p.mu.Lock()
    if p.active >= p.maxSize {
        p.mu.Unlock()
        select {
        case conn := <-p.idle:
            return conn, nil
        case <-ctx.Done():
            return nil, ctx.Err()
        }
    }
    p.active++
    p.mu.Unlock()
    return p.factory()
}

func (p *Pool) Put(conn *Conn) {
    select {
    case p.idle <- conn:
    default:
        conn.Close() // pool full, discard
        p.mu.Lock(); p.active--; p.mu.Unlock()
    }
}
```

**Real-time Scenario:** A microservice with 50 database connections in a pool handles 10K requests/sec. Each request borrows a connection for ~5ms, so only ~50 connections are needed (Little's Law: 10K * 0.005 = 50).

---

### Q2: Fixed-size vs elastic pool?

**Answer:** **Fixed-size**: pre-allocates N connections at startup. Simple, predictable memory usage, but wastes resources during low traffic and can't handle spikes. **Elastic**: grows up to max size on demand, shrinks during idle periods. More efficient but complex — needs min/max bounds, idle timeout, and shrink policy. Most production pools are elastic with a fixed maximum.

**Example:**
```go
// Fixed-size pool:
pool := make(chan *Conn, 50) // always 50 connections
for i := 0; i < 50; i++ {
    pool <- createConn() // pre-allocate all at startup
}

// Elastic pool:
type ElasticPool struct {
    idle    chan *Conn
    minSize int
    maxSize int
    active  int
}
// Creates connections on demand, destroys idle ones exceeding minSize
```

**Real-time Scenario:** A batch processing system uses a fixed pool (always needs all connections). A web API uses an elastic pool — 5 connections at night (low traffic), scaling to 50 during peak hours.

---

### Q3: How do you handle pool exhaustion?

**Answer:** When all pool resources are in use: (1) **Block with timeout** — wait for a resource, fail if timeout expires. (2) **Return error immediately** — fail fast, let caller retry or use fallback. (3) **Queue requests** — bounded queue with priority. (4) **Overcommit** — create temporary resources beyond the max (risky). Always return appropriate errors (HTTP 503) so callers know to back off.

**Example:**
```go
func (p *Pool) Get(ctx context.Context) (*Conn, error) {
    select {
    case conn := <-p.idle:
        return conn, nil
    case <-ctx.Done():
        return nil, fmt.Errorf("pool exhausted: %w", ctx.Err())
    }
}

// Caller with timeout:
ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
defer cancel()
conn, err := pool.Get(ctx)
if err != nil {
    return nil, fmt.Errorf("service unavailable: %w", err) // 503
}
```

**Real-time Scenario:** A database pool with 50 connections handles 10K req/sec normally. During a traffic spike to 50K req/sec, pool exhaustion is detected in 2 seconds and returns 503 — the load balancer routes to other instances.

---

### Q4: How do you implement idle resource cleanup?

**Answer:** Run a background goroutine that periodically scans idle resources. If a resource has been idle longer than the timeout, close and remove it. Track last-used time on each resource. Go's `database/sql` uses `SetConnMaxIdleTime` for this. The cleanup interval should be shorter than the idle timeout (e.g., cleanup every 30s, idle timeout 5 minutes).

**Example:**
```go
type PooledConn struct {
    conn     *Conn
    lastUsed time.Time
}

func (p *Pool) startCleanup(idleTimeout time.Duration) {
    ticker := time.NewTicker(30 * time.Second)
    go func() {
        for range ticker.C {
            now := time.Now()
            for {
                select {
                case pc := <-p.idle:
                    if now.Sub(pc.lastUsed) > idleTimeout {
                        pc.conn.Close() // idle too long
                        continue
                    }
                    p.idle <- pc // still valid, put back
                default:
                    goto done
                }
            }
            done:
        }
    }()
}
```

**Real-time Scenario:** After a traffic spike, a pool with 50 connections only needs 5. The cleanup goroutine closes 45 idle connections over the next 5 minutes, freeing database resources for other services.

---

### Q5: How do you handle unhealthy resources?

**Answer:** Validate resources before returning them to the caller: (1) **on-borrow validation** — test the resource when it's retrieved (e.g., ping the database). (2) **on-return validation** — test before putting back in pool. (3) **background validation** — periodically test idle resources. Discard unhealthy resources and create fresh ones. Track health check results for monitoring.

**Example:**
```go
func (p *Pool) Get(ctx context.Context) (*Conn, error) {
    for {
        select {
        case conn := <-p.idle:
            if err := conn.Ping(ctx); err != nil {
                conn.Close()         // discard unhealthy
                p.decrementActive()
                continue             // try next
            }
            return conn, nil
        default:
            return p.createNew(ctx)
        }
    }
}

func (p *Pool) Put(conn *Conn) {
    if conn.IsBroken() {
        conn.Close()         // don't put broken connections back
        p.decrementActive()
        return
    }
    conn.lastUsed = time.Now()
    p.idle <- conn
}
```

**Real-time Scenario:** After a database failover, all pooled connections are stale. On-borrow health checks detect the broken connections, discard them, and create new connections to the new primary — automatic recovery without restart.

---

### Q6: sync.Pool vs custom pool?

**Answer:** **sync.Pool**: no size limit, objects can be GC'd at any time, no lifecycle management, per-P sharding for performance. Best for temporary allocations (buffers, encoders). **Custom pool**: fixed/elastic size, objects persist, lifecycle management (create, validate, destroy), blocking `Get()` with timeout. Best for expensive resources (database connections, gRPC clients).

**Example:**
```go
// sync.Pool: cheap, temporary objects
var bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
buf := bufPool.Get().(*bytes.Buffer)
defer bufPool.Put(buf)
// GC may reclaim buf at any time — no guarantees

// Custom pool: expensive, persistent resources
dbPool := NewPool(Options{
    Factory:  func() (*sql.Conn, error) { return db.Conn(ctx) },
    MaxSize:  50,
    MinIdle:  5,
    IdleTimeout: 5 * time.Minute,
})
conn, _ := dbPool.Get(ctx) // blocks if pool exhausted
defer dbPool.Put(conn)     // connection persists until explicitly closed
```

**Real-time Scenario:** A team used `sync.Pool` for database connections — the GC periodically reclaimed them, causing connection storms. Switching to a custom pool with min-idle kept 5 connections warm at all times.

---

### Q7: How do you implement connection pooling for databases?

**Answer:** Go's `database/sql` package has a built-in connection pool. Configure: `SetMaxOpenConns` (max concurrent connections), `SetMaxIdleConns` (max idle connections to keep), `SetConnMaxLifetime` (max time a connection can be reused), `SetConnMaxIdleTime` (max idle time before close). Use `QueryContext`/`ExecContext` to participate in pool management.

**Example:**
```go
db, _ := sql.Open("postgres", dsn)

// Pool configuration:
db.SetMaxOpenConns(25)          // max 25 connections total
db.SetMaxIdleConns(10)          // keep 10 idle connections
db.SetConnMaxLifetime(30 * time.Minute) // recreate after 30 min
db.SetConnMaxIdleTime(5 * time.Minute)  // close if idle > 5 min

// Monitor pool:
stats := db.Stats()
log.Printf("Open: %d, InUse: %d, Idle: %d, WaitCount: %d",
    stats.OpenConnections, stats.InUse, stats.Idle, stats.WaitCount)
```

**Real-time Scenario:** A service set `MaxOpenConns=100` but the database only allows 50 connections per client. The excess connections were rejected, causing errors. Setting `MaxOpenConns=25` (half of limit) left room for other instances.

---

### Q8: What is pool draining?

**Answer:** Pool draining gracefully empties a pool during shutdown: (1) stop accepting new borrow requests, (2) wait for all in-use resources to be returned, (3) close all returned resources, (4) close remaining idle resources. This ensures no connections are leaked and all in-flight operations complete. Use a `sync.WaitGroup` to track in-use resources.

**Example:**
```go
func (p *Pool) Drain(ctx context.Context) error {
    p.draining.Store(true) // reject new Get() calls

    // Wait for all in-use connections to be returned
    done := make(chan struct{})
    go func() {
        p.wg.Wait() // tracks active connections
        close(done)
    }()

    select {
    case <-done:
        // all returned — close idle connections
        close(p.idle)
        for conn := range p.idle {
            conn.Close()
        }
        return nil
    case <-ctx.Done():
        return fmt.Errorf("drain timed out: %d still in use", p.active)
    }
}
```

**Real-time Scenario:** During a rolling deployment, the old pod drains its connection pool: in-flight queries complete, connections are returned and closed, then the pod exits. The new pod creates its own fresh pool.

---

### Q9: How do you monitor pool health?

**Answer:** Track metrics: (1) **active connections** — currently in use, (2) **idle connections** — available but unused, (3) **wait count** — times a Get() had to wait, (4) **wait duration** — how long Gets waited, (5) **create/destroy rate**, (6) **health check failures**. Alert on: wait count > 0 sustained (pool too small), idle > active sustained (pool too large).

**Example:**
```go
type PoolMetrics struct {
    Active       int64 // currently borrowed
    Idle         int64 // available in pool
    WaitCount    int64 // total times Get() blocked
    WaitDuration int64 // total time spent waiting (ns)
    CreateCount  int64 // total connections created
    CloseCount   int64 // total connections closed
}

func (p *Pool) Metrics() PoolMetrics {
    return PoolMetrics{
        Active:    atomic.LoadInt64(&p.active),
        Idle:      int64(len(p.idle)),
        WaitCount: atomic.LoadInt64(&p.waitCount),
    }
}
// Export to Prometheus/Grafana for dashboards and alerting
```

**Real-time Scenario:** A Grafana alert fires when `WaitCount > 0` for 5 minutes — the team increases `MaxOpenConns` from 25 to 50, eliminating connection wait time and reducing P99 latency by 200ms.

---

### Q10: Optimal pool size formula?

**Answer:** **Little's Law**: `pool_size = throughput * avg_latency`. If you process 1,000 requests/sec and each uses a connection for 10ms, you need 1000 * 0.01 = 10 connections. Add 20-50% buffer for variance. For databases, also consider: the database's own max connections limit, other clients sharing the database, and connection creation overhead.

**Example:**
```go
// Calculation:
// Throughput: 5,000 req/sec
// Avg DB latency: 5ms = 0.005s
// Connections needed: 5000 * 0.005 = 25
// With 50% buffer: 25 * 1.5 = 38
// Round up: 40

db.SetMaxOpenConns(40)

// Verify empirically:
// If WaitCount stays at 0 under peak load → pool is large enough
// If Idle stays above 50% → pool is too large
```

**Real-time Scenario:** A team set the pool size to 200 "to be safe." Each connection consumed 10MB of memory (2GB total) and the database couldn't handle 200 connections. Using Little's Law, they calculated 30 connections were sufficient, saving 1.7GB of memory.

---

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

### Q1: How do you implement distributed locking?

**Answer:** Use an external coordination service: (1) **Redis**: `SET key value NX EX ttl` — set-if-not-exists with TTL. (2) **etcd**: lease-based locks with automatic expiry. (3) **ZooKeeper**: ephemeral sequential nodes. All approaches need a TTL to handle lock holder crashes. The lock holder must periodically renew the TTL (heartbeat) and release the lock when done.

**Example:**
```go
func (dl *DistributedLock) Lock(ctx context.Context) error {
    dl.value = uuid.New().String() // unique per holder
    for {
        ok, err := dl.client.SetNX(ctx, dl.key, dl.value, dl.ttl).Result()
        if err != nil { return err }
        if ok { return nil } // acquired

        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(100 * time.Millisecond):
            // retry
        }
    }
}

func (dl *DistributedLock) Unlock(ctx context.Context) error {
    // Only delete if we own the lock (check value)
    script := `if redis.call("GET",KEYS[1])==ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`
    return dl.client.Eval(ctx, script, []string{dl.key}, dl.value).Err()
}
```

**Real-time Scenario:** A cron scheduler running on 5 instances uses a Redis distributed lock to ensure only one instance runs the daily report — preventing duplicate reports without manual coordination.

---

### Q2: What is the Redlock algorithm?

**Answer:** Redlock acquires locks on N/2+1 independent Redis nodes to handle individual node failures. Algorithm: (1) record start time, (2) try to acquire the lock on all N nodes sequentially, (3) the lock is acquired if majority (N/2+1) succeed AND total elapsed time is less than the lock TTL. If failed, release all acquired locks. Controversial — Martin Kleppmann argues it's unsafe under clock drift and network delays.

**Example:**
```go
func (rl *Redlock) Lock(ctx context.Context) error {
    start := time.Now()
    acquired := 0
    for _, node := range rl.nodes {
        if node.SetNX(ctx, rl.key, rl.value, rl.ttl).Val() {
            acquired++
        }
    }
    elapsed := time.Since(start)
    if acquired >= len(rl.nodes)/2+1 && elapsed < rl.ttl {
        return nil // lock acquired on majority
    }
    rl.unlockAll(ctx) // failed — release partial locks
    return ErrLockFailed
}
```

**Real-time Scenario:** A payment deduplication system uses Redlock across 5 Redis instances. Even if 2 Redis nodes fail, the lock still works with the remaining 3 — providing fault-tolerant mutual exclusion.

---

### Q3: How do you implement leader election?

**Answer:** Leader election ensures exactly one instance is the "leader" at any time. Approaches: (1) **etcd**: create a lease, use `Campaign()` — the instance that acquires the lease becomes leader. (2) **Consul**: session-based lock on a key. (3) **Custom**: periodic Redis lock with renewal. The leader must continuously renew its lease; if it fails, another instance becomes leader.

**Example:**
```go
func runLeaderElection(ctx context.Context, client *clientv3.Client) {
    s, _ := concurrency.NewSession(client, concurrency.WithTTL(10))
    defer s.Close()
    e := concurrency.NewElection(s, "/leader")

    if err := e.Campaign(ctx, "instance-1"); err != nil {
        log.Fatal(err)
    }
    log.Println("I am the leader!")
    // Do leader work...

    // Resign when done:
    e.Resign(ctx)
}
```

**Real-time Scenario:** A scheduler cluster with 3 instances uses etcd leader election. Only the leader schedules jobs. If the leader crashes, its lease expires in 10 seconds, and another instance automatically becomes leader.

---

### Q4: Difference between distributed and local locks?

**Answer:** **Local locks** (`sync.Mutex`): in-process, nanosecond latency, 100% reliable, no network. **Distributed locks**: cross-process/machine, millisecond latency, can fail (network partition, node crash), need TTL for safety, require consensus. Distributed locks are fundamentally less reliable — you must design for lock loss (idempotent operations, fencing tokens).

**Example:**
```go
// Local lock: fast, simple, reliable
var mu sync.Mutex // ~25ns
mu.Lock()
counter++
mu.Unlock()

// Distributed lock: slow, complex, can fail
lock := NewRedisLock(client, "counter-lock", 30*time.Second) // ~5ms
if err := lock.Lock(ctx); err != nil {
    return err // network error, timeout, or lock contention
}
defer lock.Unlock(ctx)
// Must handle: lock expired while we were working
```

**Real-time Scenario:** A developer used a distributed lock for protecting in-memory state — adding 5ms latency per operation for no benefit. Local mutex was sufficient since all goroutines are in the same process.

---

### Q5: How do you handle lock holder failure?

**Answer:** Use a TTL (Time-To-Live) on the lock. If the holder crashes, the lock automatically expires after the TTL. The holder must renew the TTL periodically (heartbeat). If renewal fails (network partition), the holder must assume it lost the lock and stop working. Use fencing tokens to prevent a partitioned holder from making stale updates after the lock is re-acquired by another instance.

**Example:**
```go
func (dl *DistributedLock) StartHeartbeat(ctx context.Context) {
    ticker := time.NewTicker(dl.ttl / 3) // renew at 1/3 TTL
    go func() {
        for {
            select {
            case <-ticker.C:
                ok := dl.client.Expire(ctx, dl.key, dl.ttl).Val()
                if !ok {
                    log.Warn("lost lock — stopping work")
                    dl.cancel() // signal to stop processing
                    return
                }
            case <-ctx.Done():
                return
            }
        }
    }()
}
```

**Real-time Scenario:** A batch processor holds a distributed lock with 30s TTL and renews every 10s. If the process crashes, the lock expires after 30s, and another instance takes over. If the network partitions, the heartbeat fails, and the process stops itself.

---

### Q6: What is a fencing token?

**Answer:** A fencing token is a monotonically increasing number issued with each lock acquisition. The token is included in all writes. The storage system rejects writes with a token lower than the last seen token. This prevents a "zombie" lock holder (one that lost the lock due to GC pause or partition) from overwriting data written by the new lock holder.

**Example:**
```go
// Lock service issues fencing tokens:
type Lock struct {
    Token int64 // monotonically increasing
}

// Storage checks fencing token:
func (s *Storage) Write(token int64, key string, value []byte) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    if token < s.lastToken[key] {
        return errors.New("stale lock: fencing token too old")
    }
    s.lastToken[key] = token
    s.data[key] = value
    return nil
}
```

**Real-time Scenario:** Process A acquires lock (token=5), then has a 30-second GC pause. Process B acquires lock (token=6) and writes data. Process A wakes up and tries to write with token=5 — the storage rejects it because token 6 > 5.

---

### Q7: How do you implement a distributed rate limiter?

**Answer:** Use Redis as the shared state store. Approaches: (1) **Fixed window**: `INCR key` + `EXPIRE key window` — simple but allows 2x burst at boundaries. (2) **Sliding window**: Redis sorted sets with timestamp scores — accurate but more expensive. (3) **Token bucket**: periodic token addition with atomic operations. Use Lua scripts for atomicity.

**Example:**
```go
const rateLimitScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local current = redis.call("INCR", key)
if current == 1 then
    redis.call("EXPIRE", key, window)
end
if current > limit then
    return 0
end
return 1
`

func (rl *DistributedRateLimiter) Allow(ctx context.Context, key string) bool {
    result, _ := rl.client.Eval(ctx, rateLimitScript,
        []string{key}, rl.limit, rl.windowSecs).Int()
    return result == 1
}
```

**Real-time Scenario:** A multi-region API with 20 instances enforces "1000 requests per minute per API key" using Redis. All instances share the same counter, ensuring accurate limits regardless of which region handles the request.

---

### Q8: What is the Saga pattern?

**Answer:** A Saga is a sequence of local transactions where each step has a compensating action (undo). If step N fails, compensating actions for steps N-1, N-2, ..., 1 are executed. Unlike distributed transactions (2PC), Sagas don't hold locks across services — they achieve eventual consistency. Two orchestration models: choreography (each service triggers the next) and orchestration (a central coordinator).

**Example:**
```go
type Saga struct {
    steps []SagaStep
}

type SagaStep struct {
    Action     func(ctx context.Context) error
    Compensate func(ctx context.Context) error
}

func (s *Saga) Execute(ctx context.Context) error {
    var completed []int
    for i, step := range s.steps {
        if err := step.Action(ctx); err != nil {
            // Compensate in reverse order
            for j := len(completed) - 1; j >= 0; j-- {
                s.steps[completed[j]].Compensate(ctx)
            }
            return err
        }
        completed = append(completed, i)
    }
    return nil
}
```

**Real-time Scenario:** An e-commerce order: (1) reserve inventory, (2) charge payment, (3) schedule shipping. If payment fails, the saga compensates by releasing the inventory reservation — no distributed lock needed.

---

### Q9: How do you handle "exactly-once"?

**Answer:** True exactly-once is impossible in distributed systems. Instead, implement "at-least-once delivery + idempotent processing" = "effectively exactly-once." Use idempotency keys: before processing, check if the key exists in a deduplication store. If yes, return the cached result. If no, process, store the result with the key, then return.

**Example:**
```go
func (s *Service) ProcessPayment(ctx context.Context, idempotencyKey string, req PaymentRequest) (PaymentResult, error) {
    // Check dedup store
    if result, found := s.dedupStore.Get(ctx, idempotencyKey); found {
        return result, nil // already processed
    }

    // Process
    result, err := s.processPaymentInternal(ctx, req)
    if err != nil { return PaymentResult{}, err }

    // Store result for dedup (with TTL)
    s.dedupStore.Set(ctx, idempotencyKey, result, 24*time.Hour)
    return result, nil
}
```

**Real-time Scenario:** A payment API receives duplicate webhook notifications (at-least-once delivery). The idempotency key (transaction ID) ensures each payment is processed exactly once — retries return the cached result.

---

### Q10: Optimistic vs pessimistic concurrency control?

**Answer:** **Pessimistic**: lock first, then operate (mutexes, distributed locks). Prevents conflicts by blocking. **Optimistic**: operate without locking, check for conflicts at commit time (version numbers, CAS, ETags). Retry on conflict. Optimistic is better when conflicts are rare (most reads succeed). Pessimistic is better when conflicts are common (avoids wasted work).

**Example:**
```go
// Pessimistic: lock before reading
mu.Lock()
value := data[key]
data[key] = newValue
mu.Unlock()

// Optimistic: read version, update with version check
for {
    value, version := db.Get(key)
    newValue := transform(value)
    ok := db.CompareAndSwap(key, newValue, version) // CAS
    if ok { break }
    // conflict: another writer updated — retry
}
```

**Real-time Scenario:** A wiki uses optimistic concurrency — conflicts (two users editing the same page simultaneously) are rare. On conflict, the second user sees a merge dialog. A banking system uses pessimistic locking — conflicts (transfers from the same account) are common.

---

### Q11: How do you implement a distributed work queue?

**Answer:** Use a message broker (Redis, RabbitMQ, Kafka) as the queue. Producers push work items; consumers pop items and process them. Key features: (1) visibility timeout (item re-appears if not acknowledged), (2) at-least-once delivery, (3) dead-letter queue for failed items, (4) consumer groups for load balancing.

**Example:**
```go
// Redis-based work queue:
func (q *Queue) Push(ctx context.Context, item Item) error {
    data, _ := json.Marshal(item)
    return q.client.LPush(ctx, q.key, data).Err()
}

func (q *Queue) Pop(ctx context.Context) (Item, error) {
    // BRPOPLPUSH: pop from queue, push to processing queue
    data, err := q.client.BRPopLPush(ctx, q.key, q.processingKey, 30*time.Second).Bytes()
    if err != nil { return Item{}, err }
    var item Item
    json.Unmarshal(data, &item)
    return item, nil
}

func (q *Queue) Ack(ctx context.Context, item Item) error {
    data, _ := json.Marshal(item)
    return q.client.LRem(ctx, q.processingKey, 1, data).Err()
}
```

**Real-time Scenario:** An email service uses a Redis work queue. 10 worker instances pop emails, send them, and acknowledge. If a worker crashes, unacknowledged emails re-appear after the visibility timeout and are picked up by another worker.

---

### Q12: What is the CAP theorem and how does it affect concurrent design?

**Answer:** The CAP theorem states that a distributed system can provide at most 2 of 3: **Consistency** (all nodes see the same data), **Availability** (every request gets a response), **Partition tolerance** (system works despite network splits). Since partitions are inevitable, you choose CP (consistent but may be unavailable during partition) or AP (available but may return stale data). This affects lock design, caching strategy, and data replication.

**Example:**
```go
// CP system: distributed lock with strong consistency
// etcd/ZooKeeper — if partitioned, refuses to serve (unavailable)
lock := etcd.NewLock("/locks/resource")
lock.Lock() // may fail during partition — CP

// AP system: cached data with eventual consistency
// Redis replica — if partitioned, serves stale data (available)
value := redis.Get("config") // may return stale data — AP
```

**Real-time Scenario:** A payment system chooses CP (consistency over availability) — it's better to reject a payment during a partition than to process it twice. A product catalog chooses AP (availability over consistency) — showing a slightly stale price is better than showing an error page.

---

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

### Q1: How do you design a Go service for 1M requests/second?

**Answer:** Key techniques: (1) minimize allocations (sync.Pool, pre-allocated buffers), (2) shard locks to reduce contention, (3) use atomics for counters/flags, (4) batch I/O operations, (5) tune GOMAXPROCS and GC (GOGC), (6) use connection pooling, (7) profile with pprof continuously. Architecture: stateless handlers, sharded in-memory caches, async processing with bounded channels, and horizontal scaling behind a load balancer.

**Example:**
```go
// High-throughput HTTP handler:
var bufPool = sync.Pool{New: func() any { return make([]byte, 4096) }}

func handler(w http.ResponseWriter, r *http.Request) {
    buf := bufPool.Get().([]byte)
    defer bufPool.Put(buf)

    n := processInto(buf, r) // zero allocation processing
    w.Write(buf[:n])
}

// Sharded counter for metrics:
var reqCount ShardedCounter // no lock contention at 1M/sec
```

**Real-time Scenario:** A CDN edge server handling 1M requests/sec uses sync.Pool for response buffers (saves 4GB/sec of allocations), sharded counters for metrics, and GOGC=400 to reduce GC frequency from 100/sec to 20/sec.

---

### Q2: What is lock contention and how do you reduce it?

**Answer:** Lock contention occurs when multiple goroutines compete for the same lock, serializing work and wasting CPU time. Reduction strategies: (1) **shard** — split one lock into N locks by key hash, (2) **minimize critical section** — do only essential work under lock, (3) **use atomics** for single-value operations, (4) **use RWMutex** for read-heavy workloads, (5) **lock-free data structures** for extreme cases.

**Example:**
```go
// HIGH contention: single lock
type Cache struct {
    mu   sync.Mutex
    data map[string]string
}

// LOW contention: sharded locks
type ShardedCache struct {
    shards [256]struct {
        mu   sync.Mutex
        data map[string]string
    }
}

func (c *ShardedCache) getShard(key string) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32() % 256)
}
```

**Real-time Scenario:** A single-lock cache at 100K operations/sec showed 80% of CPU time in mutex contention (pprof). Sharding to 256 locks reduced contention to <1% and increased throughput to 2M operations/sec.

---

### Q3: How do you use pprof to debug concurrency issues?

**Answer:** Go's pprof provides several concurrent debugging profiles: (1) **goroutine** — shows all goroutines and their stacks (detects goroutine leaks), (2) **mutex** — shows time spent waiting for locks (detects contention), (3) **block** — shows time blocked on synchronization primitives, (4) **cpu** — shows CPU-intensive code. Enable `net/http/pprof` for live profiling. Automate profiling in CI/CD.

**Example:**
```go
import _ "net/http/pprof"
func main() {
    runtime.SetMutexProfileFraction(5)
    runtime.SetBlockProfileRate(1)
    go http.ListenAndServe(":6060", nil)
}

// Debug commands:
// go tool pprof http://localhost:6060/debug/pprof/goroutine  — goroutine leaks
// go tool pprof http://localhost:6060/debug/pprof/mutex      — lock contention
// go tool pprof http://localhost:6060/debug/pprof/block       — blocking
// go tool pprof http://localhost:6060/debug/pprof/profile     — CPU (30s sample)
```

**Real-time Scenario:** A production service had increasing latency. Goroutine pprof showed 50,000 goroutines stuck on a channel send — a downstream consumer was paused. Mutex pprof showed the database pool lock was held for 100ms per acquisition.

---

### Q4: What is false sharing and how do you avoid it?

**Answer:** False sharing occurs when two goroutines on different CPU cores modify variables that share the same CPU cache line (64 bytes). Each write invalidates the other core's cache, causing expensive cache-line bouncing. Fix: pad structs to ensure each field is on its own cache line using `[64]byte` padding or `_ [cacheline - unsafe.Sizeof(field)]byte`.

**Example:**
```go
// FALSE SHARING: counters share cache lines
type BadCounters struct {
    counter1 int64 // same cache line
    counter2 int64 // as counter1 — false sharing!
}

// FIXED: each counter on its own cache line
type GoodCounters struct {
    counter1 int64
    _        [56]byte // pad to 64 bytes (cache line size)
    counter2 int64
    _        [56]byte
}
```

**Real-time Scenario:** A metrics system with 8 per-CPU counters in a struct showed 10x slower performance than expected. Cache-line padding eliminated false sharing and restored expected throughput — each core's counter was on its own cache line.

---

### Q5: How does sync.Pool help?

**Answer:** `sync.Pool` provides a per-P (processor) pool of temporary objects that can be reused. It reduces GC pressure by recycling allocations: `Get()` retrieves an object (or creates one via `New`), and `Put()` returns it for reuse. Objects may be garbage collected at any GC cycle. Perfect for request-scoped buffers, temporary structs, and encoder/decoder instances.

**Example:**
```go
var bufPool = sync.Pool{
    New: func() any { return new(bytes.Buffer) },
}

func handler(w http.ResponseWriter, r *http.Request) {
    buf := bufPool.Get().(*bytes.Buffer)
    buf.Reset() // always reset before use
    defer bufPool.Put(buf)

    json.NewEncoder(buf).Encode(response)
    w.Write(buf.Bytes())
}
// Without pool: 1M requests = 1M allocations = heavy GC
// With pool: 1M requests = ~GOMAXPROCS allocations
```

**Real-time Scenario:** A JSON API handler allocating 4KB buffers per request caused 400ms GC pauses at 100K req/sec. Using sync.Pool reduced allocations by 99%, bringing GC pauses down to 4ms.

---

### Q6: Impact of GC on high-throughput Go services?

**Answer:** GC pauses cause latency spikes: all goroutines are briefly paused during the mark phase. High allocation rates increase GC frequency. Mitigation: (1) **reduce allocations** (sync.Pool, pre-allocate, reuse buffers), (2) **increase GOGC** (default 100, set higher to reduce GC frequency at cost of more memory), (3) **use `GOMEMLIMIT`** (Go 1.19+, controls GC based on memory target), (4) **profile allocations** with `go tool pprof -alloc_space`.

**Example:**
```go
// Tune GC:
// GOGC=200: GC triggers at 2x live heap (fewer, larger GCs)
// GOMEMLIMIT=4GiB: GC targets 4GB memory usage

// Reduce allocations:
var result [1024]byte // stack-allocated, no GC
// vs
result := make([]byte, 1024) // heap-allocated, GC tracks it

// Profile allocations:
// go tool pprof -alloc_objects http://localhost:6060/debug/pprof/heap
```

**Real-time Scenario:** A trading system set GOGC=2000 and GOMEMLIMIT=8GiB — GC runs only when approaching 8GB memory, reducing GC pauses from 50/sec to 2/sec and cutting P99 latency from 50ms to 2ms.

---

### Q7: How do you implement batching for throughput?

**Answer:** Collect individual items and process them in batches. Use a timer + buffer: collect items until the buffer is full OR a timer fires (whichever comes first). This amortizes per-operation overhead (syscalls, network round-trips, disk seeks) across many items. Common for database inserts, network sends, and log flushes.

**Example:**
```go
type Batcher struct {
    items []Item
    mu    sync.Mutex
    flush chan struct{}
}

func (b *Batcher) Add(item Item) {
    b.mu.Lock()
    b.items = append(b.items, item)
    if len(b.items) >= 100 {
        b.mu.Unlock()
        b.flush <- struct{}{} // flush when batch is full
        return
    }
    b.mu.Unlock()
}

func (b *Batcher) Run() {
    ticker := time.NewTicker(100 * time.Millisecond)
    for {
        select {
        case <-ticker.C:
            b.processBatch() // flush on timer
        case <-b.flush:
            b.processBatch() // flush when full
        }
    }
}
```

**Real-time Scenario:** Individual database INSERTs: 1,000/sec (1ms per INSERT). Batched INSERTs (100 per batch): 100,000/sec (10ms per batch of 100). Batching gave 100x throughput improvement.

---

### Q8: How do you benchmark concurrent Go code accurately?

**Answer:** Use `testing.B.RunParallel` to benchmark under concurrency. Set `GOMAXPROCS` explicitly. Run with `-count=10` for statistical significance. Use `-benchmem` for allocation stats. Avoid timer contamination from setup. Use `b.ResetTimer()` after setup. Compare contended vs uncontended results. Use `benchstat` to compare before/after.

**Example:**
```go
func BenchmarkMutexContended(b *testing.B) {
    var mu sync.Mutex
    var counter int
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            mu.Lock()
            counter++
            mu.Unlock()
        }
    })
}

// Run:
// go test -bench=. -benchmem -count=10 -cpu=1,2,4,8 | tee results.txt
// benchstat results.txt
```

**Real-time Scenario:** A developer claimed their lock-free queue was 10x faster than a channel. Accurate benchmarking with `RunParallel` and `benchstat` showed it was only 1.5x faster under realistic contention — not worth the complexity.

---

### Q9: Role of GOMAXPROCS in high-throughput systems?

**Answer:** `GOMAXPROCS` sets the number of OS threads that can execute goroutines simultaneously. Default: number of CPU cores. For CPU-bound work, GOMAXPROCS = cores is optimal. For I/O-bound work, increasing it slightly (1.5-2x cores) can help. For systems sharing a machine, reduce it to leave cores for other processes. Setting it too high increases scheduler overhead.

**Example:**
```go
import "runtime"

func init() {
    // Default: use all cores
    runtime.GOMAXPROCS(runtime.NumCPU())

    // I/O-bound service: slightly more than cores
    runtime.GOMAXPROCS(runtime.NumCPU() * 2)

    // Container with CPU limit of 2 cores:
    // Go automatically detects cgroup limits (Go 1.19+)
    // GOMAXPROCS is set to 2, not the host's 64 cores
}
```

**Real-time Scenario:** A Go service in a Kubernetes pod with 4 CPU cores was setting GOMAXPROCS=64 (host machine cores). Using `automaxprocs` library to auto-detect cgroup limits reduced context switching by 90% and improved throughput by 30%.

---

### Q10: How do you reduce memory allocations in hot paths?

**Answer:** (1) **sync.Pool** for reusable buffers, (2) **stack allocation** — keep objects small and don't let them escape to heap, (3) **pre-allocate slices** with `make([]T, 0, expectedCap)`, (4) **avoid string concatenation** — use `strings.Builder`, (5) **avoid interface boxing** — use concrete types, (6) **check escape analysis** with `go build -gcflags='-m'` to find heap escapes.

**Example:**
```go
// BAD: allocates on every call
func bad() string {
    return fmt.Sprintf("user:%d", id) // allocates
}

// GOOD: pre-allocated buffer
var buf [64]byte // stack-allocated
func good() string {
    n := copy(buf[:], "user:")
    n += strconv.AppendInt(buf[:n], id, 10) // no allocation
    return string(buf[:n])
}

// Check escape analysis:
// go build -gcflags='-m' ./...
// ./main.go:10: moved to heap: result
```

**Real-time Scenario:** A hot-path logger was allocating 2KB per log message. Pre-allocating log buffers via sync.Pool reduced allocations from 500K/sec to 1K/sec and cut GC pauses by 80%.

---

### Q11: Cost of channel operations at high throughput?

**Answer:** Unbuffered channel send+receive: ~100-300ns (involves goroutine scheduling). Buffered channel send: ~50-100ns (just a memory copy if buffer not full). Under high contention, channels become bottlenecks because sends and receives require lock acquisition on the channel's internal mutex. For ultra-high throughput (>10M ops/sec), consider lock-free alternatives or batching through channels.

**Example:**
```go
// Benchmark results (typical):
// Unbuffered channel:   ~200ns/op
// Buffered channel(1):  ~100ns/op
// Buffered channel(100): ~60ns/op
// Mutex lock+unlock:     ~25ns/op
// Atomic add:            ~5ns/op

// For 1M ops/sec: channels are fine (~100ns * 1M = 100ms CPU/sec)
// For 100M ops/sec: channels are too slow — use atomics or batching
```

**Real-time Scenario:** A log pipeline processing 10M events/sec was bottlenecked by channel operations. Batching 100 events per channel send reduced channel operations from 10M/sec to 100K/sec, restoring throughput.

---

### Q12: What is zero-copy and when applicable?

**Answer:** Zero-copy avoids copying data between buffers by sharing the same memory. In Go, pass slices (which are references to underlying arrays) instead of copying data. Use `io.Reader`/`io.Writer` interfaces for streaming without buffering entire payloads. `sendfile` syscall sends file data directly from kernel to network socket without copying to userspace.

**Example:**
```go
// COPY: data copied multiple times
data := make([]byte, 1MB)
file.Read(data)    // kernel → userspace copy
conn.Write(data)   // userspace → kernel copy

// ZERO-COPY: io.Copy uses sendfile when possible
func serveFile(conn net.Conn, f *os.File) {
    io.Copy(conn, f) // may use sendfile — zero userspace copies
}

// Slice sharing (zero-copy within process):
buf := make([]byte, 1024)
header := buf[:4]   // shares underlying array — no copy
payload := buf[4:]  // shares underlying array — no copy
```

**Real-time Scenario:** A static file server using `io.Copy` from file to HTTP response triggers `sendfile`, serving files at 10 Gbps with <5% CPU usage vs 40% CPU with explicit read+write buffers.

---

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
