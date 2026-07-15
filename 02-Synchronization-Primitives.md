# Part 2: Synchronization Primitives — Mutex, RWMutex, Atomic, Cond, Once

> Go Concurrency Interview Guide — From Beginner to Staff Engineer

---

## Table of Contents

1. [Mutex (sync.Mutex)](#1-mutex-syncmutex)
2. [RWMutex (sync.RWMutex)](#2-rwmutex-syncrwmutex)
3. [Atomic Operations (sync/atomic)](#3-atomic-operations-syncatomic)
4. [Condition Variables (sync.Cond)](#4-condition-variables-synccond)
5. [Once (sync.Once)](#5-once-synconce)

---

# 1. MUTEX (sync.Mutex)

## Concept Overview

`sync.Mutex` provides mutual exclusion — only one goroutine can hold the lock at a time. It is the most fundamental synchronization primitive for protecting shared state.

**Key Properties:**
- **Not reentrant:** Locking a locked mutex from the same goroutine causes deadlock. Go intentionally does not support reentrant mutexes (forces cleaner design).
- **Fairness:** Since Go 1.9, mutexes have two modes:
  - **Normal mode:** Waiters are queued FIFO, but a newly arriving goroutine can steal the lock if it arrives while the lock is being released (better throughput).
  - **Starvation mode:** Activated if a waiter has been waiting >1ms. Lock is handed directly to the longest-waiting goroutine (fairness guarantee).
- **TryLock:** Added in Go 1.18 — non-blocking lock attempt. Returns `true` if locked, `false` otherwise. Use sparingly.
- **Zero value is unlocked:** `var mu sync.Mutex` is ready to use.
- **Must not be copied after first use:** `go vet` detects this.
- **Memory ordering:** Lock/Unlock creates happens-before relationships.

**Internal Structure:**
```
state (int32):
  - bit 0: locked
  - bit 1: woken (a goroutine has been woken and is trying to acquire)
  - bit 2: starvation mode
  - bits 3+: waiter count
sema (uint32): semaphore for blocking/waking goroutines
```

## Why Interviewers Ask It

- Most commonly used synchronization primitive in production Go code.
- Tests understanding of critical sections, lock granularity, contention.
- Reveals awareness of deadlock prevention (lock ordering).
- Differentiates candidates who understand performance implications.
- Common source of production bugs (deadlocks, contention).

## Common Interview Questions

1. What happens if you lock a mutex twice from the same goroutine? (Deadlock — Go mutexes are not reentrant)
2. Why doesn't Go have reentrant mutexes? (Encourages cleaner design — if you need reentrant, your abstraction is wrong)
3. What is mutex starvation and how does Go handle it? (Starvation mode after 1ms wait)
4. How do you detect lock contention? (`go tool pprof` mutex profile)
5. What is the performance cost of a mutex? (~20-50ns uncontended on modern CPUs)
6. Should you embed a mutex in a struct or keep it separate? (Embed when the mutex protects the struct's fields)
7. What is the defer pattern for mutexes and why is it important? (Guarantees unlock even on panic)
8. How does `go vet` detect mutex copying? (Checks for value copies of sync types)
9. What is TryLock and when was it added? (Go 1.18 — use for non-critical optimistic locking)
10. How do you profile mutex contention in production? (Enable mutex profiling in pprof)
11. What is lock ordering and how does it prevent deadlocks? (Always acquire locks in consistent order)
12. Can you share a mutex between processes? (No — in-process only)
13. What is the relationship between mutexes and memory ordering? (Lock/Unlock creates happens-before edge)
14. When should you use a mutex vs a channel? (Mutex for shared state, channel for communication)
15. What is the "minimize critical section" principle? (Hold the lock for the shortest time possible)

## Coding Problems

### Problem 1: Thread-Safe Map

**Difficulty:** Easy-Medium  
**Problem:** Implement a thread-safe map with Get, Set, Delete, and Len operations.

**Concepts Tested:** Mutex usage, critical sections, API design  
**Expected Approach:**
```go
type SafeMap[K comparable, V any] struct {
    mu sync.Mutex
    m  map[K]V
}

func NewSafeMap[K comparable, V any]() *SafeMap[K, V] {
    return &SafeMap[K, V]{m: make(map[K]V)}
}

func (s *SafeMap[K, V]) Get(key K) (V, bool) {
    s.mu.Lock()
    defer s.mu.Unlock()
    v, ok := s.m[key]
    return v, ok
}

func (s *SafeMap[K, V]) Set(key K, value V) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.m[key] = value
}

func (s *SafeMap[K, V]) Delete(key K) {
    s.mu.Lock()
    defer s.mu.Unlock()
    delete(s.m, key)
}
```

**Variations:**
- When would `sync.Map` be better? (Many goroutines reading, few writing; keys are stable)
- When would `RWMutex` be better? (Read-heavy workloads)
- How would you shard this for high concurrency? (Hash key to N sub-maps)

**Real-world Relevance:** Configuration stores, caches, session stores.

---

### Problem 2: Bank Account Transfer — Deadlock Prevention

**Difficulty:** Medium-Hard  
**Problem:** Implement a banking system with accounts. Support concurrent transfers between accounts without deadlocks.

**Concepts Tested:** Lock ordering, deadlock prevention, multiple mutexes  

**Naive approach (DEADLOCKS):**
```go
func transfer(from, to *Account, amount int) {
    from.mu.Lock()   // Goroutine A: locks account 1
    defer from.mu.Unlock()
    to.mu.Lock()     // Goroutine A: tries to lock account 2
    defer to.mu.Unlock()
    // Goroutine B simultaneously does transfer(to, from, ...) — DEADLOCK
    from.balance -= amount
    to.balance += amount
}
```

**Fixed — Lock ordering by account ID:**
```go
func transfer(from, to *Account, amount int) error {
    // Always lock lower ID first to prevent deadlock
    first, second := from, to
    if from.id > to.id {
        first, second = to, from
    }

    first.mu.Lock()
    defer first.mu.Unlock()
    second.mu.Lock()
    defer second.mu.Unlock()

    if from.balance < amount {
        return errors.New("insufficient funds")
    }
    from.balance -= amount
    to.balance += amount
    return nil
}
```

**Variations:**
- What if accounts are in different services? (Distributed locks, saga pattern)
- What if you have 3-way transfers? (Same principle: sort all accounts by ID, lock in order)
- How do you handle timeouts on lock acquisition? (Use channels or TryLock with retry)

**Real-world Relevance:** Financial systems, distributed transactions, resource allocation.

---

### Problem 3: Dining Philosophers

**Difficulty:** Hard  
**Problem:** Solve the Dining Philosophers problem in Go. 5 philosophers sit around a table with 5 forks. Each needs 2 forks to eat. Prevent deadlock and starvation.

**Concepts Tested:** Deadlock prevention, resource allocation, classical CS problem  

**Solution — Resource Ordering:**
```go
type Fork struct {
    mu sync.Mutex
    id int
}

func philosopher(id int, left, right *Fork, wg *sync.WaitGroup) {
    defer wg.Done()
    // Always pick up lower-numbered fork first
    first, second := left, right
    if left.id > right.id {
        first, second = right, left
    }

    for i := 0; i < 3; i++ { // eat 3 times
        // Think
        time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
        // Eat
        first.mu.Lock()
        second.mu.Lock()
        fmt.Printf("Philosopher %d eating (round %d)\n", id, i)
        time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
        second.mu.Unlock()
        first.mu.Unlock()
    }
}
```

**Alternative — Channel-based:**
```go
// Use a semaphore channel allowing only 4 philosophers to attempt eating simultaneously
sem := make(chan struct{}, 4)
```

**Variations:** Solve with channels only. Add fairness constraints. Implement with a waiter/arbiter goroutine.  
**Real-world Relevance:** Resource contention in database systems, connection pool allocation.

---

### Problem 4: Read-Heavy Cache Contention Diagnosis

**Difficulty:** Hard (FAANG-level)  
**Problem:** Your cache uses `sync.Mutex` and handles 99% reads, 1% writes. Under high concurrency you see p99 latency spikes. Diagnose and fix.

**Concepts Tested:** Lock contention, profiling, RWMutex, optimization  

**Diagnosis Steps:**
1. Enable mutex profiling: `runtime.SetMutexProfileFraction(1)`
2. Check pprof: `go tool pprof http://localhost:6060/debug/pprof/mutex`
3. Identify: readers blocking each other even though they don't modify data.

**Solutions (progressive optimization):**
```go
// Level 1: Switch to RWMutex
type Cache struct {
    mu sync.RWMutex
    m  map[string]interface{}
}
func (c *Cache) Get(key string) (interface{}, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    v, ok := c.m[key]
    return v, ok
}

// Level 2: Copy-on-write (lock-free reads)
type Cache struct {
    data atomic.Pointer[map[string]interface{}]
    mu   sync.Mutex // only for writes
}
func (c *Cache) Get(key string) (interface{}, bool) {
    m := *c.data.Load()
    v, ok := m[key]
    return v, ok // completely lock-free!
}
func (c *Cache) Set(key string, value interface{}) {
    c.mu.Lock()
    defer c.mu.Unlock()
    old := *c.data.Load()
    newMap := make(map[string]interface{}, len(old)+1)
    for k, v := range old {
        newMap[k] = v
    }
    newMap[key] = value
    c.data.Store(&newMap)
}

// Level 3: Sharded map (reduces contention per shard)
type ShardedCache struct {
    shards [256]struct {
        mu sync.RWMutex
        m  map[string]interface{}
    }
}
func (c *ShardedCache) getShard(key string) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32()) % len(c.shards)
}
```

**Real-world Relevance:** Every production cache system at scale.

---

### Problem 5: Implement a Mutex from Scratch

**Difficulty:** Hard (Staff-level)  
**Problem:** Implement a basic mutex using only atomic operations.

**Expected Approach (Spin Lock):**
```go
type SpinMutex struct {
    locked atomic.Int32
}

func (m *SpinMutex) Lock() {
    for !m.locked.CompareAndSwap(0, 1) {
        runtime.Gosched() // yield to other goroutines
    }
}

func (m *SpinMutex) Unlock() {
    m.locked.Store(0)
}
```

**Why real mutexes are more complex:**
- Spin locks waste CPU — real mutexes park goroutines after a spin phase.
- Go's mutex has a two-phase approach: spin briefly, then park on semaphore.
- Starvation prevention requires tracking waiters and handoff.

---

## Common Mistakes

| Mistake | Consequence | Fix |
|---------|-------------|-----|
| Locking twice (same goroutine) | Deadlock | Don't nest locks; refactor |
| Forgetting to unlock | Deadlock | Always `defer mu.Unlock()` |
| Copying a mutex | Both copies work independently — data race | Never copy; pass by pointer |
| Lock ordering inconsistency | Deadlock under concurrency | Define and enforce lock order |
| Holding lock during I/O | Terrible performance (contention) | Minimize critical section |
| Locking in hot loop | High contention | Use atomic ops or sharding |
| Not using defer | Panic skips unlock | Always defer |

---

# 2. RWMUTEX (sync.RWMutex)

## Concept Overview

`sync.RWMutex` allows **multiple concurrent readers OR a single writer**. It is an optimization over `sync.Mutex` for read-heavy workloads.

**Methods:**
- `RLock()` / `RUnlock()` — Acquire/release read lock. Multiple goroutines can hold read locks simultaneously.
- `Lock()` / `Unlock()` — Acquire/release write lock. Exclusive — blocks all readers and other writers.

**Key Properties:**
- **Writer starvation prevention:** When a writer is waiting, new readers are blocked. This ensures writers eventually get access.
- **Cannot upgrade read lock to write lock:** Calling `Lock()` while holding `RLock()` deadlocks.
- **Overhead:** RWMutex has more overhead than Mutex (~2x) due to reader counting. Only beneficial when reads significantly outnumber writes.
- **Rule of thumb:** RWMutex helps when reads are >10x more frequent than writes.

## Common Interview Questions

1. When should you use RWMutex vs Mutex? (Read-heavy: >10:1 read-write ratio)
2. Can you upgrade a read lock to a write lock? (NO — will deadlock)
3. What happens with 1000 readers and 1 writer trying to acquire? (Readers drain, writer blocks new readers, writer acquires)
4. How does RWMutex prevent writer starvation? (Pending writer blocks new readers)
5. What is the overhead of RWMutex compared to Mutex? (~2x for uncontended)
6. Is RWMutex always faster than Mutex for read-heavy workloads? (No — uncontended mutex is faster)
7. What is a lock downgrade and can you do it in Go? (Write->Read downgrade not natively supported)
8. How many readers can hold an RWMutex simultaneously? (Unlimited)
9. What happens if you call `RUnlock()` without `RLock()`? (Panic or undefined behavior)
10. Can you use `RLock()` recursively? (Technically yes, but dangerous — can prevent writers from ever acquiring)

## Coding Problems

### Problem 1: Concurrent Config Store

**Difficulty:** Medium  
**Problem:** Implement a configuration store with concurrent reads and exclusive writes. Support atomic config reloads and change notifications.

```go
type ConfigStore struct {
    mu        sync.RWMutex
    config    map[string]string
    listeners []func(key, value string)
}

func (cs *ConfigStore) Get(key string) (string, bool) {
    cs.mu.RLock()
    defer cs.mu.RUnlock()
    v, ok := cs.config[key]
    return v, ok
}

func (cs *ConfigStore) Set(key, value string) {
    cs.mu.Lock()
    defer cs.mu.Unlock()
    cs.config[key] = value
    for _, fn := range cs.listeners {
        fn(key, value) // notify inside lock to guarantee ordering
    }
}

func (cs *ConfigStore) GetAll() map[string]string {
    cs.mu.RLock()
    defer cs.mu.RUnlock()
    copy := make(map[string]string, len(cs.config))
    for k, v := range cs.config {
        copy[k] = v
    }
    return copy // return copy, not reference
}
```

**Real-world Relevance:** Feature flag systems, config management (Viper-like).

---

### Problem 2: Read-Write Lock Upgrade Bug

**Difficulty:** Medium-Hard  
**Problem:** Find the deadlock and fix it:

```go
func updateIfNeeded(rw *sync.RWMutex, data map[string]int, key string) {
    rw.RLock()
    if _, ok := data[key]; !ok {
        rw.Lock() // BUG: deadlock! Already holding RLock
        data[key] = computeValue(key)
        rw.Unlock()
    }
    rw.RUnlock()
}
```

**Fix — Double-checked locking pattern:**
```go
func updateIfNeeded(rw *sync.RWMutex, data map[string]int, key string) {
    // Fast path: read lock
    rw.RLock()
    _, ok := data[key]
    rw.RUnlock()

    if !ok {
        // Slow path: write lock + re-check
        rw.Lock()
        defer rw.Unlock()
        if _, ok := data[key]; !ok { // re-check after acquiring write lock
            data[key] = computeValue(key)
        }
    }
}
```

**Why re-check is needed:** Between releasing `RLock` and acquiring `Lock`, another goroutine may have already computed the value.

---

### Problem 3: Sharded RWMutex Map

**Difficulty:** Hard  
**Problem:** Implement a sharded map with N shards, each with its own RWMutex.

```go
const numShards = 32

type ShardedMap[V any] struct {
    shards [numShards]shard[V]
}

type shard[V any] struct {
    mu sync.RWMutex
    m  map[string]V
}

func NewShardedMap[V any]() *ShardedMap[V] {
    sm := &ShardedMap[V]{}
    for i := range sm.shards {
        sm.shards[i].m = make(map[string]V)
    }
    return sm
}

func (sm *ShardedMap[V]) shardIndex(key string) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32()) % numShards
}

func (sm *ShardedMap[V]) Get(key string) (V, bool) {
    s := &sm.shards[sm.shardIndex(key)]
    s.mu.RLock()
    defer s.mu.RUnlock()
    v, ok := s.m[key]
    return v, ok
}

func (sm *ShardedMap[V]) Set(key string, value V) {
    s := &sm.shards[sm.shardIndex(key)]
    s.mu.Lock()
    defer s.mu.Unlock()
    s.m[key] = value
}
```

**Why this is superior:** Contention is distributed across N shards. Reads/writes to different shards are fully concurrent.

**Real-world Relevance:** How `sync.Map` works internally (two maps), Java `ConcurrentHashMap`, production caches.

---

# 3. ATOMIC OPERATIONS (sync/atomic)

## Concept Overview

The `sync/atomic` package provides low-level atomic operations on primitive types. These are the building blocks for lock-free data structures.

**Operations:**
- `Load` / `Store` — Atomic read/write.
- `Add` — Atomic increment/decrement.
- `Swap` — Atomic exchange (returns old value).
- `CompareAndSwap` (CAS) — Atomic conditional update. Sets new value only if current equals expected.

**Types (Go 1.19+):**
- `atomic.Bool`, `atomic.Int32`, `atomic.Int64`, `atomic.Uint32`, `atomic.Uint64`
- `atomic.Pointer[T]` — Type-safe atomic pointer operations.
- `atomic.Value` — Store/load any interface{} atomically.

**Memory Ordering:**
- All atomic operations in Go provide sequential consistency (strong ordering).
- An atomic write "happens before" a subsequent atomic read of the same variable.
- Weaker orderings (relaxed, acquire/release) are NOT exposed in Go.

**When to use Atomics vs Mutex:**

| Atomics | Mutex |
|---------|-------|
| Single variable updates | Multi-variable consistency |
| Simple counters/flags | Complex state transitions |
| Hot paths (better performance) | When you need to protect a block of code |
| Lock-free data structures | When atomics would be too complex |

## Common Interview Questions

1. What is CAS (Compare-And-Swap) and how does it work? (CPU instruction: if *addr == old then *addr = new, returns success/failure)
2. When should you use atomics vs mutexes? (Atomics for single values, mutex for multi-value consistency)
3. What is `atomic.Value` and when would you use it? (Store/load any interface atomically — config hot-reload)
4. What memory ordering guarantees do Go atomics provide? (Sequential consistency)
5. What is the ABA problem? (Value changes A→B→A, CAS succeeds incorrectly)
6. What are the typed atomics in Go 1.19+? (Bool, Int32, Int64, Uint32, Uint64, Pointer[T])
7. What is a spin lock and when would you use one? (CAS loop — use when lock hold time is very short)
8. How do you atomically update a struct? (Can't — use `atomic.Pointer[MyStruct]` to swap entire struct)
9. What is the performance difference between atomic and mutex? (Atomic: ~5-15ns, uncontended mutex: ~20-50ns)
10. What is a memory barrier/fence? (CPU instruction ensuring memory operations complete in order)
11. Can atomic operations alone guarantee correctness for complex state? (Rarely — usually need mutex for multi-step operations)
12. What is `atomic.Pointer[T]` and how does it differ from `atomic.Value`? (Type-safe, no boxing, better performance)
13. How do you implement a CAS loop? (Load-Compare-Swap in a for loop until success)

## Coding Problems

### Problem 1: Lock-Free Counter with Max

**Difficulty:** Easy-Medium  
**Problem:** Implement a counter using atomic operations that supports Increment, Decrement, Load, and a Max constraint (counter cannot exceed a maximum value).

```go
type BoundedCounter struct {
    val atomic.Int64
    max int64
}

func (c *BoundedCounter) Increment() bool {
    for {
        old := c.val.Load()
        if old >= c.max {
            return false
        }
        if c.val.CompareAndSwap(old, old+1) {
            return true
        }
    }
}

func (c *BoundedCounter) Decrement() bool {
    for {
        old := c.val.Load()
        if old <= 0 {
            return false
        }
        if c.val.CompareAndSwap(old, old-1) {
            return true
        }
    }
}
```

**Key pattern:** The CAS loop — load, compute, CAS, retry on failure. This is the fundamental pattern for all lock-free algorithms.

**Real-world Relevance:** Rate limiters, connection pool counters, semaphore implementations.

---

### Problem 2: Atomic Config Swap (Read-Copy-Update)

**Difficulty:** Medium  
**Problem:** Implement a configuration holder that can be atomically swapped while concurrent readers continue without locks.

```go
type Config struct {
    DBHost     string
    DBPort     int
    MaxConns   int
    DebugMode  bool
}

type ConfigHolder struct {
    config atomic.Pointer[Config]
}

func NewConfigHolder(initial *Config) *ConfigHolder {
    h := &ConfigHolder{}
    h.config.Store(initial)
    return h
}

// Lock-free read — safe for concurrent access
func (h *ConfigHolder) Get() *Config {
    return h.config.Load()
}

// Atomic swap — readers see either old or new, never partial
func (h *ConfigHolder) Update(newConfig *Config) {
    h.config.Store(newConfig)
}

// CAS-based conditional update
func (h *ConfigHolder) UpdateIf(expected, newConfig *Config) bool {
    return h.config.CompareAndSwap(expected, newConfig)
}
```

**Key insight:** Readers are completely lock-free. The config struct is immutable once published. Writers create a new struct and atomically swap the pointer.

**Real-world Relevance:** Hot config reloading, feature flag updates, routing table updates.

---

### Problem 3: Lock-Free Stack

**Difficulty:** Hard  
**Problem:** Implement a lock-free stack (push/pop) using CAS operations.

```go
type Node[T any] struct {
    Value T
    Next  *Node[T]
}

type LockFreeStack[T any] struct {
    head atomic.Pointer[Node[T]]
}

func (s *LockFreeStack[T]) Push(val T) {
    newNode := &Node[T]{Value: val}
    for {
        old := s.head.Load()
        newNode.Next = old
        if s.head.CompareAndSwap(old, newNode) {
            return
        }
    }
}

func (s *LockFreeStack[T]) Pop() (T, bool) {
    for {
        old := s.head.Load()
        if old == nil {
            var zero T
            return zero, false
        }
        if s.head.CompareAndSwap(old, old.Next) {
            return old.Value, true
        }
    }
}
```

**ABA Problem:** In garbage-collected languages like Go, ABA is less of a concern because the GC prevents memory reuse of the node while a reference exists. In C/C++, you'd need tagged pointers or hazard pointers.

**Real-world Relevance:** High-performance concurrent data structures, runtime scheduler internals.

---

### Problem 4: Implement sync.Once with CAS

**Difficulty:** Medium-Hard  
**Problem:** Implement `sync.Once` using only atomic operations.

**Naive attempt (INCORRECT):**
```go
type NaiveOnce struct {
    done atomic.Int32
}

func (o *NaiveOnce) Do(f func()) {
    if o.done.CompareAndSwap(0, 1) {
        f() // Problem: other callers return before f() completes!
    }
}
```

**Why it's wrong:** Concurrent callers return immediately before `f()` finishes. They might access uninitialized data.

**Correct approach (matching real sync.Once):**
```go
type Once struct {
    done atomic.Uint32
    mu   sync.Mutex
}

func (o *Once) Do(f func()) {
    if o.done.Load() == 0 { // fast path
        o.doSlow(f)
    }
}

func (o *Once) doSlow(f func()) {
    o.mu.Lock()
    defer o.mu.Unlock()
    if o.done.Load() == 0 { // double-check
        defer o.done.Store(1)
        f()
    }
}
```

**Lesson:** Even a simple "do once" requires both atomic (fast path) and mutex (slow path with blocking).

---

### Problem 5: Atomic Max (CAS Loop Pattern)

**Difficulty:** Medium  
**Problem:** Implement an atomic "update maximum" — atomically updates a shared int64 only if the new value is greater.

```go
func atomicMax(addr *atomic.Int64, val int64) {
    for {
        old := addr.Load()
        if val <= old {
            return // current max is already >= val
        }
        if addr.CompareAndSwap(old, val) {
            return
        }
        // CAS failed: another goroutine changed it; retry
    }
}
```

**Variations:** Implement atomic min, atomic "update if function returns true."  
**Real-world Relevance:** Tracking peak values, high watermarks, monitoring metrics.

---

## Common Mistakes

| Mistake | Consequence | Fix |
|---------|-------------|-----|
| Using atomics for multi-variable state | Inconsistent view of state | Use mutex for multi-variable consistency |
| Forgetting CAS can fail | Lost updates, incorrect values | Always loop on CAS failure |
| Mixing atomic and non-atomic access | Data race | All access must be atomic |
| Using `atomic.Value` for frequently updated values | Boxing overhead | Use typed atomics (Go 1.19+) |
| Assuming non-atomic reads are "good enough" | Compiler/CPU reordering, stale values | Always use atomic for shared data |

---

# 4. CONDITION VARIABLES (sync.Cond)

## Concept Overview

`sync.Cond` provides a way for goroutines to wait for or announce the occurrence of an event. It is the Go equivalent of POSIX condition variables.

**Methods:**
- `Wait()` — Atomically unlocks the associated mutex and suspends the goroutine. When woken, re-acquires the lock.
- `Signal()` — Wakes one waiting goroutine.
- `Broadcast()` — Wakes all waiting goroutines.

**Critical rules:**
1. Must hold the lock before calling `Wait()`.
2. **Always check the condition in a loop, not an if:**
```go
// WRONG:
cond.L.Lock()
if !condition {
    cond.Wait() // might be spurious wakeup!
}
// use data
cond.L.Unlock()

// RIGHT:
cond.L.Lock()
for !condition {
    cond.Wait()
}
// use data
cond.L.Unlock()
```
3. Spurious wakeups can occur — the condition might not be true when `Wait()` returns.

**Why sync.Cond is rarely used in Go:**
- Channels are usually more idiomatic and safer.
- `sync.Cond` is useful when you need to wake multiple waiters or when the condition is complex.
- Main use cases: bounded buffers, barriers, reader-writer coordination.

## Common Interview Questions

1. What is a condition variable and when would you use one? (Signaling between goroutines about state changes)
2. Why must you check the condition in a loop, not an if? (Spurious wakeups, multiple waiters)
3. What is the difference between Signal and Broadcast? (Signal: wake one; Broadcast: wake all)
4. When would you use sync.Cond vs a channel? (Cond: complex conditions, multiple waiters; Channel: simpler patterns)
5. What is a spurious wakeup? (Runtime may wake a goroutine without an explicit Signal)
6. Why is sync.Cond rarely used in Go? (Channels are more idiomatic and harder to misuse)
7. How does sync.Cond work internally? (Notification list + semaphore)
8. Can you use sync.Cond with RWMutex? (Yes — pass `rwMutex.RLocker()` or `&rwMutex`)

## Coding Problems

### Problem 1: Bounded Buffer with Cond

**Difficulty:** Medium  
**Problem:** Implement a bounded buffer (producer-consumer) using sync.Cond.

```go
type BoundedBuffer[T any] struct {
    mu       sync.Mutex
    notFull  *sync.Cond
    notEmpty *sync.Cond
    buf      []T
    capacity int
}

func NewBoundedBuffer[T any](capacity int) *BoundedBuffer[T] {
    bb := &BoundedBuffer[T]{
        buf:      make([]T, 0, capacity),
        capacity: capacity,
    }
    bb.notFull = sync.NewCond(&bb.mu)
    bb.notEmpty = sync.NewCond(&bb.mu)
    return bb
}

func (bb *BoundedBuffer[T]) Put(item T) {
    bb.mu.Lock()
    defer bb.mu.Unlock()
    for len(bb.buf) == bb.capacity {
        bb.notFull.Wait() // wait until not full
    }
    bb.buf = append(bb.buf, item)
    bb.notEmpty.Signal() // signal that buffer is not empty
}

func (bb *BoundedBuffer[T]) Get() T {
    bb.mu.Lock()
    defer bb.mu.Unlock()
    for len(bb.buf) == 0 {
        bb.notEmpty.Wait() // wait until not empty
    }
    item := bb.buf[0]
    bb.buf = bb.buf[1:]
    bb.notFull.Signal() // signal that buffer is not full
    return item
}
```

**Comparison with channels:** A buffered channel does this in one line: `ch := make(chan T, capacity)`. Use Cond only when you need more complex coordination.

---

### Problem 2: Barrier Synchronization

**Difficulty:** Medium-Hard  
**Problem:** Implement a barrier that blocks until N goroutines have all reached it, then releases all.

```go
type Barrier struct {
    mu      sync.Mutex
    cond    *sync.Cond
    count   int
    parties int
    gen     int // generation — for reusable barrier
}

func NewBarrier(parties int) *Barrier {
    b := &Barrier{parties: parties}
    b.cond = sync.NewCond(&b.mu)
    return b
}

func (b *Barrier) Wait() {
    b.mu.Lock()
    defer b.mu.Unlock()
    gen := b.gen
    b.count++
    if b.count == b.parties {
        b.count = 0
        b.gen++
        b.cond.Broadcast() // wake all waiting goroutines
    } else {
        for gen == b.gen { // wait until generation changes
            b.cond.Wait()
        }
    }
}
```

**Why check generation:** Prevents a goroutine from the next phase waking up early if `Wait()` is called again before all goroutines from the current phase have woken.

**Real-world Relevance:** Parallel computation phases (e.g., parallel simulation steps), iterative algorithms.

---

### Problem 3: Read-Write Lock from Scratch

**Difficulty:** Hard  
**Problem:** Implement a read-write lock using Mutex + Cond.

```go
type RWLock struct {
    mu      sync.Mutex
    cond    *sync.Cond
    readers int
    writer  bool
}

func NewRWLock() *RWLock {
    rw := &RWLock{}
    rw.cond = sync.NewCond(&rw.mu)
    return rw
}

func (rw *RWLock) RLock() {
    rw.mu.Lock()
    defer rw.mu.Unlock()
    for rw.writer {
        rw.cond.Wait()
    }
    rw.readers++
}

func (rw *RWLock) RUnlock() {
    rw.mu.Lock()
    defer rw.mu.Unlock()
    rw.readers--
    if rw.readers == 0 {
        rw.cond.Broadcast()
    }
}

func (rw *RWLock) Lock() {
    rw.mu.Lock()
    defer rw.mu.Unlock()
    for rw.writer || rw.readers > 0 {
        rw.cond.Wait()
    }
    rw.writer = true
}

func (rw *RWLock) Unlock() {
    rw.mu.Lock()
    defer rw.mu.Unlock()
    rw.writer = false
    rw.cond.Broadcast()
}
```

**Real-world Relevance:** Understanding RWMutex internals, systems programming interviews.

---

# 5. ONCE (sync.Once)

## Concept Overview

`sync.Once` ensures a function is executed exactly once, regardless of how many goroutines call it. It is the standard way to implement lazy initialization in Go.

**Key properties:**
- Thread-safe: Multiple goroutines can call `Do()` concurrently.
- Blocks concurrent callers: If goroutine A is executing `f`, goroutine B calling `Do` blocks until A finishes.
- Once done, subsequent calls are near-zero cost (just an atomic load).
- If `f` panics, `Once` considers it "done" — `f` will not be called again.

**New additions (Go 1.21+):**
```go
// sync.OnceFunc — returns a function that calls f exactly once
init := sync.OnceFunc(func() { /* expensive init */ })
init() // first call does work
init() // no-op

// sync.OnceValue — returns a function that calls f once and caches result
getDB := sync.OnceValue(func() *sql.DB {
    db, _ := sql.Open("postgres", dsn)
    return db
})
db := getDB() // first call opens connection
db := getDB() // returns cached connection

// sync.OnceValues — same but for (value, error) pair
getDB := sync.OnceValues(func() (*sql.DB, error) {
    return sql.Open("postgres", dsn)
})
db, err := getDB()
```

## Common Interview Questions

1. How does sync.Once guarantee exactly-once execution? (Atomic done flag + mutex for slow path)
2. What happens if the function passed to `Once.Do` panics? (Marked as done — panic propagates, function won't run again)
3. Can you reset a sync.Once? (No — create a new one)
4. What are `OnceFunc`, `OnceValue`, `OnceValues`? (Convenience wrappers in Go 1.21+)
5. How does sync.Once handle concurrent callers? (Second caller blocks until first finishes)
6. What is the performance of sync.Once after first call? (~1ns — just an atomic load)
7. How would you implement lazy initialization without sync.Once? (Atomic + mutex double-check)
8. Is sync.Once a replacement for `init()`? (Different — init is package-level, Once is instance-level and lazy)

## Coding Problems

### Problem 1: Singleton Database Connection

**Difficulty:** Easy  
```go
var (
    db     *sql.DB
    dbOnce sync.Once
)

func GetDB() *sql.DB {
    dbOnce.Do(func() {
        var err error
        db, err = sql.Open("postgres", os.Getenv("DATABASE_URL"))
        if err != nil {
            log.Fatal(err) // or handle more gracefully
        }
        db.SetMaxOpenConns(25)
        db.SetMaxIdleConns(5)
    })
    return db
}
```

**Variations:** What if `sql.Open` fails? Should it retry? (sync.Once won't retry — need custom solution)  

---

### Problem 2: Resettable Once

**Difficulty:** Medium  
**Problem:** Implement a "resettable once" for testing and config reload scenarios.

```go
type ResettableOnce struct {
    mu   sync.Mutex
    done bool
}

func (o *ResettableOnce) Do(f func()) {
    o.mu.Lock()
    defer o.mu.Unlock()
    if !o.done {
        f()
        o.done = true
    }
}

func (o *ResettableOnce) Reset() {
    o.mu.Lock()
    defer o.mu.Unlock()
    o.done = false
}
```

**Trade-off:** This is slower than sync.Once (no atomic fast path). Acceptable for non-hot-path use.

---

### Problem 3: Once with Retry on Error

**Difficulty:** Medium  
**Problem:** Implement a once that retries if initialization fails.

```go
type RetryOnce struct {
    mu   sync.Mutex
    done bool
    val  interface{}
    err  error
}

func (o *RetryOnce) Do(f func() (interface{}, error)) (interface{}, error) {
    o.mu.Lock()
    defer o.mu.Unlock()
    if o.done {
        return o.val, o.err
    }
    val, err := f()
    if err == nil {
        o.done = true
        o.val = val
    }
    return val, err
}
```

**With optimized fast path:**
```go
type RetryOnce struct {
    done atomic.Bool
    mu   sync.Mutex
    val  interface{}
}

func (o *RetryOnce) Do(f func() (interface{}, error)) (interface{}, error) {
    if o.done.Load() { // fast path: already initialized
        return o.val, nil
    }
    o.mu.Lock()
    defer o.mu.Unlock()
    if o.done.Load() { // double-check
        return o.val, nil
    }
    val, err := f()
    if err != nil {
        return nil, err // don't mark done — allow retry
    }
    o.val = val
    o.done.Store(true)
    return val, nil
}
```

**Real-world Relevance:** Database connection initialization with retries, service discovery resolution.

---

## Summary: Choosing the Right Synchronization Primitive

| Primitive | Use When | Overhead | Notes |
|-----------|----------|----------|-------|
| `sync.Mutex` | Protecting shared state | ~20-50ns uncontended | Most common; not reentrant |
| `sync.RWMutex` | Read-heavy access (>10:1 ratio) | ~40-80ns uncontended | Cannot upgrade RLock→Lock |
| `atomic.*` | Single variable, hot path | ~5-15ns | Only for single-value operations |
| `sync.Cond` | Complex wait conditions, barriers | Moderate | Rarely needed — channels usually better |
| `sync.Once` | Exactly-once initialization | ~1ns after init | Panics mark as done |
| `sync.Map` | Key-stable maps with many readers | Moderate | Two-map design with promotion |
| `sync.Pool` | Object reuse to reduce GC | Low | Items can be collected anytime |
| Channels | Communication, signaling | ~50-200ns | Higher overhead but safer API |
