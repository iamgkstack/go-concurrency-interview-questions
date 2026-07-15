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

### Q1: What happens if you lock a mutex twice from the same goroutine?

**Answer:** Deadlock. Go's `sync.Mutex` is not reentrant (non-recursive). If a goroutine holds a lock and tries to lock the same mutex again, it blocks forever waiting for itself to release the lock — which will never happen. This is a fundamental design choice in Go. The fix is to restructure code so locked helper functions receive already-locked state, or use separate internal methods that assume the lock is held.

**Example:**
```go
var mu sync.Mutex

func outer() {
    mu.Lock()
    defer mu.Unlock()
    inner() // DEADLOCK: inner tries to lock the same mutex
}

func inner() {
    mu.Lock() // blocks forever — outer already holds this lock
    defer mu.Unlock()
    // ...
}

// FIX: separate locked/unlocked versions
func inner() {       // public: acquires lock
    mu.Lock()
    defer mu.Unlock()
    innerLocked()
}
func innerLocked() { // private: assumes lock is held
    // ... work without locking
}
```

**Real-time Scenario:** A logging library deadlocks because `Log()` acquires a mutex and calls `formatMessage()`, which internally calls `Log()` again for debug output — a recursive lock attempt.

---

### Q2: Why doesn't Go have reentrant mutexes?

**Answer:** Go deliberately omits reentrant mutexes because they encourage poor design. If you need to re-acquire the same lock, it usually means your abstraction boundaries are wrong — a function that needs the lock is being called from code that already holds it. Reentrant locks hide this design flaw. The Go team's stance: refactor into locked/unlocked internal methods rather than masking the problem with reentrant locks.

**Example:**
```go
// BAD design that "needs" reentrant mutex:
func (s *Store) Save(item Item) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.validate(item) // calls s.Get() which also locks — deadlock!
}

// GOOD design — no reentrant lock needed:
func (s *Store) Save(item Item) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.validateLocked(item) // doesn't lock — assumes caller holds lock
}

func (s *Store) validateLocked(item Item) {
    existing := s.data[item.ID] // direct access, no lock needed
    // ...
}
```

**Real-time Scenario:** A team porting Java code to Go initially missed the non-reentrant behavior, causing intermittent deadlocks. Refactoring to `fooLocked()` internal methods eliminated all deadlocks and improved code clarity.

---

### Q3: What is mutex starvation and how does Go handle it?

**Answer:** Mutex starvation occurs when a goroutine waits to acquire a mutex but keeps losing to other goroutines. Go's `sync.Mutex` has two modes: **normal mode** (goroutines compete via CAS — fast but unfair) and **starvation mode** (activated when a waiter has been waiting >1ms — the mutex is handed directly to the longest-waiting goroutine). Starvation mode ensures fairness at the cost of throughput.

**Example:**
```go
// Normal mode: fast, unfair
// Goroutines spin and CAS to acquire — newly arriving goroutines
// can "steal" the lock from waiting goroutines

// Starvation mode (after 1ms wait):
// Mutex is handed directly to the head of the FIFO wait queue
// No spinning allowed — guarantees progress for starving goroutines

// The mutex switches back to normal mode when:
// 1. The waiter is the last in the queue, OR
// 2. The waiter waited less than 1ms
```

**Real-time Scenario:** In a high-throughput server, one slow goroutine kept getting starved by fast goroutines grabbing the lock. After 1ms, Go's starvation mode kicked in and gave the slow goroutine its turn, preventing indefinite starvation.

---

### Q4: How do you detect lock contention?

**Answer:** Use Go's built-in mutex profiling via `runtime.SetMutexProfileFraction(rate)` and then analyze with `go tool pprof`. This shows you which mutexes have the most contention (time spent waiting). You can also use `runtime/pprof` to write mutex profiles to files, or expose them via `net/http/pprof` for live production profiling.

**Example:**
```go
import "runtime"

func init() {
    runtime.SetMutexProfileFraction(5) // sample 1/5 of mutex contention events
}

// Then profile:
// go tool pprof http://localhost:6060/debug/pprof/mutex
// (pprof) top
// Shows: time spent waiting on each mutex location
// (pprof) list MyFunction
// Shows: line-by-line contention in MyFunction
```

**Real-time Scenario:** A production API had P99 latency spikes. Mutex profiling revealed that a single mutex in the connection pool was contended 80% of the time, leading to a redesign using sharded locks that reduced P99 by 10x.

---

### Q5: What is the performance cost of a mutex?

**Answer:** An uncontended mutex lock/unlock costs ~20-50ns on modern CPUs (essentially two CAS operations). Under contention, the cost grows significantly: goroutines must be parked (suspended) and woken, each costing ~1-5us. The real cost of contention isn't the lock operation itself but the serialization — goroutines that could run in parallel are forced to wait.

**Example:**
```go
// Uncontended: ~25ns
mu.Lock()
x++
mu.Unlock()

// Contended (8 goroutines): ~200-500ns per operation
// The lock itself is fast, but waiting for others to release is slow

// Benchmark tip:
func BenchmarkMutex(b *testing.B) {
    var mu sync.Mutex
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            mu.Lock()
            mu.Unlock()
        }
    })
}
```

**Real-time Scenario:** A metrics counter using a mutex at 1M ops/sec was fine with 1 goroutine (~25ns each) but collapsed to 100K ops/sec with 8 goroutines due to contention. Switching to `atomic.Int64` restored throughput.

---

### Q6: Should you embed a mutex in a struct or keep it separate?

**Answer:** Embed the mutex in the struct when the mutex protects that struct's fields — this makes the association clear and keeps the mutex close to the data it guards. Keep it separate only when the mutex protects multiple unrelated resources. By convention, name the mutex `mu` and place it as the first field or directly above the fields it protects.

**Example:**
```go
// GOOD: embedded, protects struct fields
type SafeCounter struct {
    mu    sync.Mutex // guards count
    count int
}

func (c *SafeCounter) Increment() {
    c.mu.Lock()
    c.count++
    c.mu.Unlock()
}

// AVOID: separate mutex, unclear what it protects
var globalMu sync.Mutex
var counter1 int
var counter2 int
// Which fields does globalMu protect? Unclear!
```

**Real-time Scenario:** During a code review, a bug was found where a developer added a new field to a struct but forgot to protect it with the existing mutex — embedding the mutex with a comment above the protected fields makes this relationship explicit.

---

### Q7: What is the defer pattern for mutexes and why is it important?

**Answer:** The pattern is `mu.Lock(); defer mu.Unlock()`. The `defer` guarantees the mutex is released even if the function panics, returns early from multiple paths, or has complex control flow. Without `defer`, a panic or forgotten return path can leave the mutex locked permanently, deadlocking the entire system. The overhead of `defer` (~35ns) is negligible compared to the safety it provides.

**Example:**
```go
// SAFE: defer guarantees unlock
func (s *Store) Get(key string) (string, error) {
    s.mu.Lock()
    defer s.mu.Unlock() // guaranteed even if panic occurs below

    val, ok := s.data[key]
    if !ok {
        return "", ErrNotFound // unlock happens via defer
    }
    return val, nil // unlock happens via defer
}

// UNSAFE: manual unlock can be missed
func (s *Store) Get(key string) (string, error) {
    s.mu.Lock()
    val, ok := s.data[key]
    if !ok {
        // BUG: forgot to unlock before return!
        return "", ErrNotFound
    }
    s.mu.Unlock()
    return val, nil
}
```

**Real-time Scenario:** A production system had a deadlock every 2-3 days. Root cause: a rare error path returned without unlocking the mutex. Adding `defer mu.Unlock()` eliminated the issue permanently.

---

### Q8: How does `go vet` detect mutex copying?

**Answer:** `go vet` checks for value copies of types containing `sync.Mutex` (and other sync types). Since a mutex has internal state (locked/unlocked, wait queue), copying it creates a second mutex with the same state but independent behavior — leading to data races. `go vet` flags assignments, function parameters, and range loops that copy sync types by value.

**Example:**
```go
type Cache struct {
    mu   sync.Mutex
    data map[string]int
}

func process(c Cache) { // go vet ERROR: copies lock value
    c.mu.Lock() // this is a DIFFERENT mutex than the original!
}

func process(c *Cache) { // CORRECT: pointer, no copy
    c.mu.Lock() // same mutex as original
}

// Also caught:
c1 := Cache{}
c2 := c1 // go vet ERROR: copies lock value
```

**Real-time Scenario:** A benchmark was passing a sync.Map by value into a goroutine, causing each goroutine to have its own independent map — no data was actually shared. `go vet` caught this immediately.

---

### Q9: What is TryLock and when was it added?

**Answer:** `TryLock()` was added in Go 1.18. It attempts to acquire the mutex without blocking — returns `true` if successful, `false` if the mutex is already held. Use it for optimistic patterns where you can skip or defer work if the lock isn't available. Do NOT use it as a general replacement for `Lock()` — it can lead to starvation and unfairness if misused.

**Example:**
```go
var mu sync.Mutex

func tryOptimisticUpdate() {
    if mu.TryLock() {
        defer mu.Unlock()
        // do the update — lock acquired
        performUpdate()
    } else {
        // lock is busy — skip or queue for later
        log.Println("skipping update, lock contended")
    }
}
```

**Real-time Scenario:** A metrics aggregation goroutine uses `TryLock` to flush stats every second. If the lock is contended (a heavy write is in progress), it skips this flush cycle and tries again next second — better than blocking the aggregation loop.

---

### Q10: How do you profile mutex contention in production?

**Answer:** Enable mutex profiling with `runtime.SetMutexProfileFraction(n)` (sample 1/n contention events), expose `net/http/pprof` endpoints, and analyze with `go tool pprof`. The profile shows total time goroutines spent waiting to acquire each mutex, helping identify hot locks. You can also use `runtime/pprof.Lookup("mutex").WriteTo(w, 0)` for programmatic access.

**Example:**
```go
import (
    "net/http"
    _ "net/http/pprof"
    "runtime"
)

func main() {
    runtime.SetMutexProfileFraction(5) // sample 20% of contention events
    go http.ListenAndServe(":6060", nil)

    // In production, analyze:
    // curl http://localhost:6060/debug/pprof/mutex > mutex.prof
    // go tool pprof mutex.prof
    // (pprof) top       — most contended mutexes
    // (pprof) web       — visualization
}
```

**Real-time Scenario:** A microservice's P99 latency spiked every 30 seconds. Mutex profiling showed a database connection pool mutex was held during DNS resolution (~50ms), blocking all other requests. Moving DNS resolution outside the lock fixed it.

---

### Q11: What is lock ordering and how does it prevent deadlocks?

**Answer:** Lock ordering is a convention where all goroutines acquire multiple locks in the same predefined order. Deadlocks occur when goroutine A holds lock 1 and waits for lock 2, while goroutine B holds lock 2 and waits for lock 1 (circular wait). By always acquiring lock 1 before lock 2, the circular wait is impossible. Document the ordering and enforce it through code review.

**Example:**
```go
var muA, muB sync.Mutex

// DEADLOCK possible:
// Goroutine 1: Lock(A) -> Lock(B)
// Goroutine 2: Lock(B) -> Lock(A)

// SAFE — consistent ordering (always A before B):
func transfer1() {
    muA.Lock(); defer muA.Unlock()
    muB.Lock(); defer muB.Unlock()
    // ... transfer
}

func transfer2() {
    muA.Lock(); defer muA.Unlock() // same order!
    muB.Lock(); defer muB.Unlock()
    // ... transfer
}
```

**Real-time Scenario:** A banking system acquires account locks in ascending account-ID order for transfers. Transferring between accounts 100->200 and 200->100 both lock account 100 first, preventing deadlock.

---

### Q12: Can you share a mutex between processes?

**Answer:** No. Go's `sync.Mutex` is in-process only — it exists in the goroutine's memory space and cannot be shared across OS processes. For cross-process synchronization, use OS-level primitives: file locks (`flock`/`fcntl`), named semaphores, or distributed locks (Redis, etcd, Consul). Cross-process coordination is fundamentally different from in-process coordination.

**Example:**
```go
// In-process: sync.Mutex works
var mu sync.Mutex

// Cross-process: use file lock
import "golang.org/x/sys/unix"

func acquireFileLock(path string) (*os.File, error) {
    f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
    if err != nil {
        return nil, err
    }
    err = unix.Flock(int(f.Fd()), unix.LOCK_EX) // blocks until acquired
    return f, err
}
```

**Real-time Scenario:** A log rotation tool uses file locks to coordinate between multiple instances of the same service — ensuring only one process rotates logs at a time, while each process uses `sync.Mutex` for internal goroutine coordination.

---

### Q13: What is the relationship between mutexes and memory ordering?

**Answer:** In Go's memory model, `mu.Unlock()` "happens before" any subsequent `mu.Lock()` on the same mutex. This means all memory writes before `Unlock()` are guaranteed visible to the goroutine that next acquires `Lock()`. Without this guarantee, CPU caches and compiler optimizations could cause goroutines to see stale data. Mutexes provide both mutual exclusion AND memory visibility.

**Example:**
```go
var mu sync.Mutex
var data string

// Goroutine 1:
mu.Lock()
data = "hello"   // write happens before Unlock
mu.Unlock()      // creates happens-before edge

// Goroutine 2:
mu.Lock()        // sees all writes before the previous Unlock
fmt.Println(data) // guaranteed to print "hello"
mu.Unlock()
```

**Real-time Scenario:** Without the memory ordering guarantee, a goroutine could lock a mutex and read stale data from its CPU cache. The happens-before relationship ensures that locked critical sections always see the latest state.

---

### Q14: When should you use a mutex vs a channel?

**Answer:** Use a mutex to protect shared state (cache, map, counter). Use a channel to communicate between goroutines (send data, signal completion, coordinate work). The Go proverb: "Don't communicate by sharing memory; share memory by communicating." If you're protecting data, use a mutex. If you're coordinating behavior, use a channel. If both work, channels are usually more idiomatic.

**Example:**
```go
// Mutex: protecting shared state
type Cache struct {
    mu   sync.Mutex
    data map[string]string
}
func (c *Cache) Set(k, v string) {
    c.mu.Lock()
    c.data[k] = v
    c.mu.Unlock()
}

// Channel: coordinating work
func coordinator(tasks <-chan Task, results chan<- Result) {
    for task := range tasks {
        results <- process(task)
    }
}
```

**Real-time Scenario:** A web server uses mutexes for its in-memory session store (shared state) and channels for its request pipeline (goroutine coordination) — each tool used where it fits best.

---

### Q15: What is the "minimize critical section" principle?

**Answer:** Hold the mutex for the shortest time possible. Only the code that actually reads or writes shared state should be inside the lock. Expensive computations, I/O, network calls, and other slow operations should happen outside the lock. This reduces contention and improves throughput because other goroutines spend less time waiting.

**Example:**
```go
// BAD: expensive work inside lock
mu.Lock()
data := fetchFromDB(key)    // 50ms network I/O while lock is held!
cache[key] = data
mu.Unlock()

// GOOD: minimize critical section
data := fetchFromDB(key)    // slow I/O outside lock
mu.Lock()
cache[key] = data            // only the map write inside lock (~10ns)
mu.Unlock()
```

**Real-time Scenario:** A service had 500ms P99 latency because a mutex was held during HTTP calls to a downstream service. Moving the HTTP call outside the lock reduced P99 to 5ms — the lock was held for nanoseconds instead of milliseconds.

---

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

### Q1: When should you use RWMutex vs Mutex?

**Answer:** Use `RWMutex` when reads vastly outnumber writes (>10:1 ratio) and reads are non-trivial (take enough time that parallelism helps). RWMutex allows unlimited concurrent readers but exclusive writers. For write-heavy or short critical sections, a regular `Mutex` is faster because `RWMutex` has higher overhead (~2x uncontended). Profile first — many developers reach for `RWMutex` prematurely when a simple `Mutex` would be faster.

**Example:**
```go
type ConfigStore struct {
    mu   sync.RWMutex
    data map[string]string
}

func (c *ConfigStore) Get(key string) string {
    c.mu.RLock()         // multiple readers concurrently
    defer c.mu.RUnlock()
    return c.data[key]
}

func (c *ConfigStore) Set(key, value string) {
    c.mu.Lock()          // exclusive writer
    defer c.mu.Unlock()
    c.data[key] = value
}
```

**Real-time Scenario:** A DNS cache with 100,000 reads/sec and 1 write/min is a perfect RWMutex case — 99.999% of operations run concurrently without blocking.

---

### Q2: Can you upgrade a read lock to a write lock?

**Answer:** No — attempting to acquire a write lock while holding a read lock will deadlock. The write lock waits for all readers to release, but the current goroutine is a reader that will never release because it's blocked waiting for the write lock. This is a fundamental limitation of RWMutex in Go (and most languages). You must release the read lock first, then acquire the write lock, accepting that the state may have changed.

**Example:**
```go
// DEADLOCK:
rw.RLock()
// realize we need to write...
rw.Lock()    // DEADLOCK: waits for all readers, but we ARE a reader
rw.Unlock()
rw.RUnlock()

// CORRECT: release read lock, then acquire write lock
rw.RLock()
val := data[key]
rw.RUnlock()       // release read lock
rw.Lock()          // acquire write lock
// re-check condition — state may have changed!
if _, ok := data[key]; !ok {
    data[key] = compute(val)
}
rw.Unlock()
```

**Real-time Scenario:** A cache using "check-then-write" must release the read lock before writing. Between releasing and re-acquiring, another goroutine may have updated the cache — always re-check under the write lock.

---

### Q3: What happens with 1000 readers and 1 writer trying to acquire?

**Answer:** When a writer calls `Lock()`, it first blocks new readers from acquiring read locks, then waits for existing readers to release. The 1,000 existing readers continue until they call `RUnlock()`. Once all readers release, the writer acquires the write lock. After the writer releases, the blocked readers proceed. This "writer preference" prevents writer starvation.

**Example:**
```go
// Timeline:
// T0: 1000 readers hold RLock
// T1: Writer calls Lock() — blocks new readers, waits for existing ones
// T2: Readers finish, call RUnlock() — counter goes from 1000 to 0
// T3: Writer acquires Lock, does write, calls Unlock
// T4: Blocked readers acquire RLock
```

**Real-time Scenario:** In a feature flag system, a configuration reload (writer) briefly blocks new readers but lets in-flight reads complete. The reload takes ~1ms while blocked, then thousands of readers resume instantly.

---

### Q4: How does RWMutex prevent writer starvation?

**Answer:** When a writer is waiting for the write lock, the `RWMutex` blocks new readers from acquiring read locks. Without this, a continuous stream of readers could prevent the writer from ever acquiring the lock (writer starvation). Go's implementation sets a "writer pending" flag when `Lock()` is called, causing subsequent `RLock()` calls to block until the writer finishes.

**Example:**
```go
// Without writer priority:
// Reader 1 holds RLock
// Writer waits for Lock
// Reader 2 acquires RLock (skips writer) — STARVATION
// Reader 3 acquires RLock (skips writer) — STARVATION
// Writer never gets the lock

// Go's RWMutex with writer priority:
// Reader 1 holds RLock
// Writer waits for Lock — sets "writer pending"
// Reader 2 calls RLock — BLOCKS (writer pending)
// Reader 1 releases — writer acquires Lock
// Writer releases — Reader 2 proceeds
```

**Real-time Scenario:** A database connection pool config reload (writer) is guaranteed to eventually acquire the lock even under continuous read pressure from 10,000 active connections.

---

### Q5: What is the overhead of RWMutex compared to Mutex?

**Answer:** `RWMutex` has roughly 2x the overhead of `Mutex` for uncontended operations because `RLock`/`RUnlock` must atomically update a reader counter. For a simple `Mutex`, lock/unlock is a single CAS (~20-25ns). For `RWMutex`, `RLock` involves an atomic add + check for pending writer (~30-50ns). The benefit only appears under contention with many concurrent readers — if reads are short, a simple `Mutex` may be faster overall.

**Example:**
```go
// Benchmark results (typical, uncontended):
// Mutex Lock+Unlock:    ~25ns
// RWMutex RLock+RUnlock: ~40ns
// RWMutex Lock+Unlock:   ~45ns

// Contended (8 goroutines, 95% reads):
// Mutex:    ~200ns (serialized)
// RWMutex:  ~50ns (readers parallel) — RWMutex wins!
```

**Real-time Scenario:** A team replaced Mutex with RWMutex in a hot-path configuration lookup, expecting faster reads. Benchmarks showed slower performance because the critical section was only 10ns — the RWMutex overhead dominated.

---

### Q6: Is RWMutex always faster than Mutex for read-heavy workloads?

**Answer:** No. For very short critical sections (< 100ns), the overhead of RWMutex's reader counter management exceeds the benefit of parallel reads. RWMutex wins when the read-side critical section is long enough that parallelism pays off AND the read-to-write ratio is high. For short reads with moderate contention, a simple `Mutex` is often faster. Always benchmark your specific workload.

**Example:**
```go
// Short critical section: Mutex wins
mu.Lock()
val := cache[key] // ~10ns
mu.Unlock()

// Long critical section: RWMutex wins
rw.RLock()
val := expensiveComputation(cache[key]) // ~1ms
rw.RUnlock()
```

**Real-time Scenario:** A configuration cache with 5ns reads saw no improvement from RWMutex. A DNS resolver with 100us lookups (parsing, validation) saw 8x improvement with RWMutex because 8 readers could work in parallel.

---

### Q7: What is a lock downgrade and can you do it in Go?

**Answer:** Lock downgrade means transitioning from a write lock to a read lock without releasing and re-acquiring (which would create a window where another writer could intervene). Go's `RWMutex` does not support native lock downgrade. You must `Unlock()` the write lock and then `RLock()`, accepting the brief gap. Some databases support downgrade natively for transactional consistency.

**Example:**
```go
// Lock downgrade (NOT natively supported in Go):
rw.Lock()
data[key] = computeValue()
// Want to downgrade to read lock here, but can't
rw.Unlock()  // brief gap — another writer could intervene
rw.RLock()   // re-acquire as reader
result := data[key] // may have changed!
rw.RUnlock()
```

**Real-time Scenario:** A caching layer that computes and stores a value needs to read it back immediately. Without downgrade support, it must either return the value directly under the write lock or accept a potential re-computation race.

---

### Q8: How many readers can hold an RWMutex simultaneously?

**Answer:** Unlimited readers can hold a `RWMutex` concurrently. Internally, `RLock` increments a `readerCount` (an `int32`), so the theoretical limit is `math.MaxInt32` (~2 billion). In practice, the number of concurrent readers is limited by the number of goroutines. Each concurrent reader adds the overhead of an atomic increment and decrement (the reader counter operations).

**Example:**
```go
var rw sync.RWMutex
var wg sync.WaitGroup
for i := 0; i < 100000; i++ {
    wg.Add(1)
    go func() {
        defer wg.Done()
        rw.RLock()         // all 100,000 can hold RLock simultaneously
        _ = sharedData     // read shared data
        rw.RUnlock()
    }()
}
wg.Wait()
```

**Real-time Scenario:** A shared in-memory feature flag store is read by 50,000 concurrent request handlers — all hold the read lock simultaneously with negligible contention.

---

### Q9: What happens if you call `RUnlock()` without `RLock()`?

**Answer:** Calling `RUnlock()` without a matching `RLock()` causes a panic: `sync: RUnlock of unlocked RWMutex`. Internally, `RUnlock` decrements the reader counter — if it goes below zero, the runtime panics. This is a programming error similar to double-unlocking a Mutex. Always use `defer rw.RUnlock()` immediately after `RLock()` to prevent mismatched lock/unlock.

**Example:**
```go
var rw sync.RWMutex
rw.RUnlock() // PANIC: sync: RUnlock of unlocked RWMutex

// CORRECT: always pair RLock with RUnlock
rw.RLock()
defer rw.RUnlock()
// read operations here
```

**Real-time Scenario:** A refactoring that removed `RLock()` but left `RUnlock()` in a deferred call caused a panic in production. Code review with `go vet` and consistent `defer` patterns prevent these bugs.

---

### Q10: Can you use `RLock()` recursively?

**Answer:** Technically yes — the same goroutine can call `RLock()` multiple times since it just increments the reader counter. However, this is dangerous: if a writer is waiting, the recursive `RLock()` will deadlock because the writer blocks new readers. Go's `RWMutex` doesn't track which goroutine holds the lock, so it can't grant recursive access past a waiting writer. Avoid recursive read locking.

**Example:**
```go
func readOuter(rw *sync.RWMutex) {
    rw.RLock()
    defer rw.RUnlock()
    readInner(rw) // recursive RLock — DANGEROUS
}

func readInner(rw *sync.RWMutex) {
    rw.RLock()         // if a writer is pending, DEADLOCK here
    defer rw.RUnlock() // writer blocks new readers, we're a new reader
}
```

**Real-time Scenario:** A nested function call where both the caller and callee take read locks works fine under no contention, but deadlocks in production when a writer (config reload) starts waiting — an intermittent bug that's extremely hard to reproduce.

---

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

### Q1: What is CAS (Compare-And-Swap) and how does it work?

**Answer:** CAS is a CPU-level atomic instruction: it compares the value at a memory address with an expected "old" value, and if they match, swaps in a "new" value — all in a single, uninterruptible operation. It returns whether the swap succeeded. In Go, it's `atomic.CompareAndSwapInt64(addr, old, new)`. CAS is the foundation of lock-free data structures because it allows state transitions without locks.

**Example:**
```go
var counter int64 = 0

func atomicIncrement() {
    for {
        old := atomic.LoadInt64(&counter)
        if atomic.CompareAndSwapInt64(&counter, old, old+1) {
            return // success
        }
        // CAS failed — another goroutine modified counter, retry
    }
}
```

**Real-time Scenario:** A lock-free counter in a metrics system uses CAS to increment without mutex overhead, handling millions of increments per second with minimal contention.

---

### Q2: When should you use atomics vs mutexes?

**Answer:** Use atomics for simple, single-value operations (counters, flags, pointer swaps) where the operation is a single read-modify-write. Use mutexes when you need to protect multi-value state or perform multi-step operations that must be atomic together. Atomics are faster (~5-15ns vs ~20-50ns) but can only guarantee correctness for individual values. If you need "increment counter AND update timestamp," use a mutex.

**Example:**
```go
// Atomic: single value — perfect
var requestCount atomic.Int64
requestCount.Add(1)

// Mutex: multi-value consistency — must use mutex
type Stats struct {
    mu    sync.Mutex
    count int
    last  time.Time
}
func (s *Stats) Record() {
    s.mu.Lock()
    s.count++            // these two must be
    s.last = time.Now()  // updated together
    s.mu.Unlock()
}
```

**Real-time Scenario:** A web server uses `atomic.Int64` for request counting (single value, high contention) but a mutex for updating the request log (count + timestamp + status must be consistent).

---

### Q3: What is `atomic.Value` and when would you use it?

**Answer:** `atomic.Value` provides atomic `Store` and `Load` operations for any type (stored as `interface{}`). It's ideal for read-heavy patterns where you need to atomically swap an entire struct (e.g., config reload). All `Load` calls see either the old or new complete value — never a partial update. Once a type is stored, all subsequent stores must use the same concrete type.

**Example:**
```go
var config atomic.Value

func init() {
    config.Store(&Config{Timeout: 5 * time.Second, MaxRetries: 3})
}

func GetConfig() *Config {
    return config.Load().(*Config)  // lock-free, ~1ns
}

func ReloadConfig() {
    newCfg := loadFromDisk()
    config.Store(newCfg)  // atomic swap — readers see old or new, never partial
}
```

**Real-time Scenario:** A production service reloads TLS certificates every hour using `atomic.Value`. The swap is instantaneous — active connections keep using the old cert while new connections use the new one, with zero downtime.

---

### Q4: What memory ordering guarantees do Go atomics provide?

**Answer:** Go atomics provide sequential consistency — the strongest memory ordering guarantee. All atomic operations appear to execute in a single, global total order, and all goroutines agree on this order. This is simpler than C/C++ which offers multiple memory orderings (relaxed, acquire/release, sequentially consistent). Go chose simplicity over maximum performance by not exposing weaker orderings.

**Example:**
```go
var x, y atomic.Int32

// Goroutine 1:
x.Store(1)
y.Store(1)

// Goroutine 2:
if y.Load() == 1 {
    // x.Load() is guaranteed to return 1
    // Sequential consistency ensures stores in G1 are visible in order
}
```

**Real-time Scenario:** In a lock-free state machine, sequential consistency ensures that state transitions observed by one goroutine are visible to all others in the same order, preventing impossible state combinations.

---

### Q5: What is the ABA problem?

**Answer:** The ABA problem occurs when a value changes from A to B and back to A. A CAS operation sees the value is still A and succeeds, but the state may have changed in a way that makes the swap invalid. This is primarily a concern in lock-free data structures with pointer reuse. In garbage-collected languages like Go, the ABA problem is rare because freed memory isn't reused immediately, but it can still occur with value types (not pointers).

**Example:**
```go
// Lock-free stack pop: ABA scenario
// Thread 1: reads top = A (A -> B -> C)
// Thread 2: pops A, pops B, pushes A back (A -> C)
// Thread 1: CAS(top, A, B) succeeds! But B is no longer in the stack
// Fix: use generation counters or tagged pointers
type TaggedPointer struct {
    ptr uintptr
    gen uint64 // increment on each modification
}
```

**Real-time Scenario:** The ABA problem rarely affects Go programs due to GC (pointers to old objects remain valid), but it matters when implementing lock-free data structures with value-type CAS on indices or counters.

---

### Q6: What are the typed atomics in Go 1.19+?

**Answer:** Go 1.19 introduced method-based typed atomics: `atomic.Bool`, `atomic.Int32`, `atomic.Int64`, `atomic.Uint32`, `atomic.Uint64`, and `atomic.Pointer[T]`. They replace the function-based API (e.g., `atomic.AddInt64(&x, 1)` becomes `x.Add(1)`). Benefits: type safety (can't accidentally pass wrong pointer), cleaner syntax, and `atomic.Pointer[T]` is generic (no `interface{}` boxing like `atomic.Value`).

**Example:**
```go
// Old API (pre-1.19):
var count int64
atomic.AddInt64(&count, 1)
val := atomic.LoadInt64(&count)

// New API (Go 1.19+):
var count atomic.Int64
count.Add(1)
val := count.Load()

// Typed pointer:
var cfg atomic.Pointer[Config]
cfg.Store(&Config{Timeout: 5 * time.Second})
c := cfg.Load() // *Config, no type assertion needed
```

**Real-time Scenario:** Migrating a metrics library from `atomic.AddInt64` to `atomic.Int64.Add` eliminated 3 bugs where developers accidentally passed the wrong pointer, and improved code readability across 200+ counter declarations.

---

### Q7: What is a spin lock and when would you use one?

**Answer:** A spin lock is a lock implemented with a CAS loop — the goroutine continuously tries to acquire the lock rather than sleeping. Use when: (1) the critical section is extremely short (< 100ns), (2) contention is low, and (3) you want to avoid the overhead of goroutine parking/waking. In Go, spin locks are rarely used because `sync.Mutex` already spins briefly before parking, and goroutine scheduling overhead is low.

**Example:**
```go
type SpinLock struct {
    locked atomic.Int32
}

func (s *SpinLock) Lock() {
    for !s.locked.CompareAndSwap(0, 1) {
        runtime.Gosched() // yield to avoid pure CPU spin
    }
}

func (s *SpinLock) Unlock() {
    s.locked.Store(0)
}
```

**Real-time Scenario:** Inside the Go runtime itself, spin locks are used for very short critical sections (like scheduler queue access) where the overhead of a full mutex sleep/wake cycle would be more expensive than spinning for a few nanoseconds.

---

### Q8: How do you atomically update a struct?

**Answer:** You can't atomically update individual fields of a struct. Instead, create the complete new struct, then atomically swap the pointer using `atomic.Pointer[T]` (Go 1.19+) or `atomic.Value`. Readers always see either the complete old struct or the complete new struct — never a mix. This is the copy-on-write pattern applied to configuration, settings, and routing tables.

**Example:**
```go
type Config struct {
    Timeout    time.Duration
    MaxRetries int
    Endpoints  []string
}

var cfg atomic.Pointer[Config]

func UpdateConfig(timeout time.Duration) {
    old := cfg.Load()
    newCfg := *old                  // copy
    newCfg.Timeout = timeout        // modify
    cfg.Store(&newCfg)              // atomic swap
}

func GetConfig() *Config {
    return cfg.Load() // lock-free read
}
```

**Real-time Scenario:** A service mesh proxy atomically swaps its entire routing table (hundreds of entries) on configuration updates, ensuring active requests always see a complete, consistent routing table.

---

### Q9: What is the performance difference between atomic and mutex?

**Answer:** Uncontended atomic operations take ~5-15ns (essentially the cost of a CPU cache-line operation). Uncontended mutex lock/unlock takes ~20-50ns (CAS + potential goroutine scheduling overhead). Under high contention, the gap widens because mutex involves goroutine parking/waking while atomics just retry the CAS. However, atomics only work for single-value operations — for multi-value consistency, the mutex overhead is the cost of correctness.

**Example:**
```go
// Benchmark results (typical):
// atomic.Int64.Add:     ~5ns (uncontended)
// sync.Mutex Lock+Unlock: ~25ns (uncontended)
// atomic.Int64.Add:     ~50ns (8 goroutines contending)
// sync.Mutex:           ~200ns (8 goroutines contending)

var counter atomic.Int64
counter.Add(1) // ~5ns

var mu sync.Mutex
mu.Lock()     // }
x++           // } ~25ns total
mu.Unlock()   // }
```

**Real-time Scenario:** A rate limiter checking 1M requests/sec uses `atomic.Int64` for the counter (~5ns per check) instead of a mutex (~25ns), saving 20ns per request = 20ms of CPU time per second.

---

### Q10: What is a memory barrier/fence?

**Answer:** A memory barrier (or fence) is a CPU instruction that ensures memory operations before the barrier complete before operations after it. CPUs and compilers reorder memory operations for performance, which can cause goroutines to see stale or inconsistent data. In Go, atomic operations implicitly include memory barriers, providing sequential consistency. You don't need to insert barriers manually — the Go memory model guarantees visibility through atomics, channels, and locks.

**Example:**
```go
// Go atomics include implicit memory barriers:
var ready atomic.Bool
var data int

// Writer:
data = 42           // happens before the store (barrier)
ready.Store(true)    // includes memory barrier

// Reader:
if ready.Load() {   // includes memory barrier
    fmt.Println(data) // guaranteed to see 42
}
```

**Real-time Scenario:** Without memory barriers, a CPU might reorder the `data = 42` write to happen after `ready.Store(true)`, causing readers to see stale data. Go's atomic operations prevent this silently.

---

### Q11: Can atomic operations alone guarantee correctness for complex state?

**Answer:** Rarely. Atomics guarantee correctness for individual operations on single values, but not for operations that span multiple values. If you need "check condition AND update state" as one atomic unit, you need either a mutex or a carefully designed CAS loop on a single atomic value that encodes all the state. For most real-world multi-step operations, a mutex is simpler and more correct.

**Example:**
```go
// WRONG: two atomics don't compose atomically
var balance atomic.Int64
var lastTx atomic.Int64
func Transfer(amount int64) {
    balance.Add(-amount)     // these two operations are NOT
    lastTx.Store(time.Now()) // atomic together — inconsistent snapshot possible
}

// CORRECT: use mutex for multi-value consistency
type Account struct {
    mu      sync.Mutex
    balance int64
    lastTx  time.Time
}
func (a *Account) Transfer(amount int64) {
    a.mu.Lock()
    a.balance -= amount
    a.lastTx = time.Now()
    a.mu.Unlock()
}
```

**Real-time Scenario:** A banking system that uses separate atomics for balance and transaction log can show a deducted balance with no matching transaction entry — a mutex ensures both updates are visible together.

---

### Q12: What is `atomic.Pointer[T]` and how does it differ from `atomic.Value`?

**Answer:** `atomic.Pointer[T]` (Go 1.19+) is a generic, type-safe atomic pointer. Unlike `atomic.Value` which stores `interface{}` (requiring boxing and type assertions), `atomic.Pointer[T]` stores `*T` directly with no boxing overhead. It's faster (no interface indirection), type-safe at compile time, and the API is clearer. Use `atomic.Pointer[T]` for new code; `atomic.Value` is still valid for non-pointer types or pre-1.19 compatibility.

**Example:**
```go
// atomic.Value: interface{} boxing, needs type assertion
var v atomic.Value
v.Store(&Config{})
cfg := v.Load().(*Config) // runtime type assertion

// atomic.Pointer[T]: type-safe, no boxing
var p atomic.Pointer[Config]
p.Store(&Config{})
cfg := p.Load() // returns *Config directly — compile-time safe
```

**Real-time Scenario:** A routing table using `atomic.Pointer[RouteMap]` instead of `atomic.Value` eliminated runtime panics from wrong type assertions during hot-reload and reduced load latency by ~2ns per lookup (no interface boxing).

---

### Q13: How do you implement a CAS loop?

**Answer:** A CAS loop repeatedly: (1) loads the current value, (2) computes the desired new value, (3) attempts CAS — if it succeeds, the operation is complete; if it fails (another goroutine modified the value), it retries from step 1. This pattern is the foundation of lock-free algorithms. Add `runtime.Gosched()` in the retry path to avoid pure CPU spinning under high contention.

**Example:**
```go
var maxSeen atomic.Int64

func UpdateMax(val int64) {
    for {
        current := maxSeen.Load()
        if val <= current {
            return // not a new max
        }
        if maxSeen.CompareAndSwap(current, val) {
            return // successfully updated
        }
        // CAS failed — another goroutine updated, retry
    }
}
```

**Real-time Scenario:** A monitoring system tracks the peak request latency using a CAS loop to atomically update the maximum value — thousands of goroutines can concurrently report latencies without any mutex.

---

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

### Q1: What is a condition variable and when would you use one?

**Answer:** A condition variable (`sync.Cond`) allows goroutines to wait until a specific condition becomes true, and to signal other goroutines when the condition changes. It's used when multiple goroutines need to coordinate based on shared state — e.g., "wait until the buffer is non-empty" or "wait until all workers are ready." Unlike channels (which transfer data), Cond is for signaling state changes on shared data protected by a mutex.

**Example:**
```go
type Queue struct {
    mu    sync.Mutex
    cond  *sync.Cond
    items []int
}

func NewQueue() *Queue {
    q := &Queue{}
    q.cond = sync.NewCond(&q.mu)
    return q
}

func (q *Queue) Put(item int) {
    q.mu.Lock()
    q.items = append(q.items, item)
    q.cond.Signal() // wake one waiting consumer
    q.mu.Unlock()
}

func (q *Queue) Get() int {
    q.mu.Lock()
    for len(q.items) == 0 {
        q.cond.Wait() // release lock, sleep, reacquire on wake
    }
    item := q.items[0]
    q.items = q.items[1:]
    q.mu.Unlock()
    return item
}
```

**Real-time Scenario:** A bounded thread pool uses `sync.Cond` to make workers wait when the task queue is empty and wake them when new tasks arrive, avoiding busy-spinning.

---

### Q2: Why must you check the condition in a loop, not an if?

**Answer:** You must use a `for` loop (not `if`) because of spurious wakeups and multiple waiters. A goroutine may be woken by `Signal` or `Broadcast` but the condition may no longer be true — another goroutine may have already consumed the item. Additionally, the runtime may wake goroutines spuriously. The loop re-checks the condition after each wakeup, ensuring the goroutine only proceeds when the condition is genuinely satisfied.

**Example:**
```go
// WRONG: using if — may proceed when condition is false
q.mu.Lock()
if len(q.items) == 0 {
    q.cond.Wait() // wakes up, but another goroutine may have taken the item
}
item := q.items[0] // CRASH: items might be empty!

// CORRECT: using for loop — always re-check
q.mu.Lock()
for len(q.items) == 0 {
    q.cond.Wait() // re-check condition on each wakeup
}
item := q.items[0] // safe: condition is guaranteed true
```

**Real-time Scenario:** In a producer-consumer with 10 consumers, `Broadcast` wakes all 10, but only 1 item was added — 9 consumers find the queue empty and must go back to sleep via the loop check.

---

### Q3: What is the difference between Signal and Broadcast?

**Answer:** `Signal()` wakes exactly one goroutine waiting on the Cond (if any). `Broadcast()` wakes all waiting goroutines. Use `Signal` when a single waiter can handle the state change (e.g., one item added to queue). Use `Broadcast` when the state change affects all waiters (e.g., shutdown, barrier, or when the new state could satisfy multiple waiters' conditions).

**Example:**
```go
// Signal: wake one consumer — one item was added
q.cond.Signal()

// Broadcast: wake all waiters — system is shutting down
q.cond.Broadcast()

// Broadcast: wake all — barrier completed
barrier.cond.Broadcast()
```

**Real-time Scenario:** A barrier synchronization uses `Broadcast` to wake all 100 waiting goroutines simultaneously when the last goroutine arrives, while a work queue uses `Signal` to wake one worker per new job.

---

### Q4: When would you use sync.Cond vs a channel?

**Answer:** Use `sync.Cond` when you need to wait on complex conditions involving shared state (e.g., "buffer has space AND not shutting down"), when multiple goroutines wait on the same condition, or when you need `Broadcast` semantics. Use channels for simpler patterns: signaling completion, passing data, or fan-in/fan-out. Channels are more idiomatic in Go and harder to misuse — prefer them unless Cond's specific features are needed.

**Example:**
```go
// Channel: simple completion signal
done := make(chan struct{})
go func() { work(); close(done) }()
<-done

// sync.Cond: complex condition with shared state
cond.L.Lock()
for !ready || shuttingDown {
    cond.Wait()
}
// proceed with complex state
cond.L.Unlock()
```

**Real-time Scenario:** A connection pool uses `sync.Cond` to wait for "connection available AND pool not draining" — two conditions that must both be true, which is awkward to express with channels alone.

---

### Q5: What is a spurious wakeup?

**Answer:** A spurious wakeup occurs when a goroutine waiting on a condition variable is woken without an explicit `Signal` or `Broadcast` call. While the Go runtime tries to minimize these, they can occur due to implementation details of the OS scheduler or runtime internals. This is why the `Wait()` call must always be inside a `for` loop that re-checks the condition — the pattern is the same across all languages with condition variables.

**Example:**
```go
cond.L.Lock()
for !conditionMet() {
    cond.Wait() // may wake spuriously — loop handles it
}
// condition is guaranteed true here
cond.L.Unlock()
```

**Real-time Scenario:** In a high-concurrency system with thousands of goroutines, spurious wakeups are rare but possible. The for-loop pattern costs nothing when it doesn't happen and saves you from subtle bugs when it does.

---

### Q6: Why is sync.Cond rarely used in Go?

**Answer:** Channels provide most of the functionality of condition variables with a simpler, more idiomatic API. Channels naturally handle signaling, data transfer, and `select`-based composition. `sync.Cond` is error-prone (forget the loop? deadlock. forget to hold the lock? race condition). The Go community strongly prefers channels for coordination. Cond is mainly useful for specific patterns like barriers, bounded buffers with complex conditions, or when porting algorithms from other languages.

**Example:**
```go
// sync.Cond approach (complex, error-prone):
cond.L.Lock()
for !ready { cond.Wait() }
cond.L.Unlock()

// Channel approach (simple, idiomatic):
<-readyCh
```

**Real-time Scenario:** In code reviews at Go-focused companies, using `sync.Cond` often triggers a comment: "Can this be done with channels?" — and the answer is almost always yes for typical use cases.

---

### Q7: How does sync.Cond work internally?

**Answer:** Internally, `sync.Cond` maintains a notification list (a linked list of waiting goroutines represented as `notifyList` with `sudog` entries). `Wait()` adds the goroutine to the list, releases the associated mutex, and parks the goroutine on a runtime semaphore. `Signal()` dequeues one goroutine from the list and wakes it via the semaphore. `Broadcast()` wakes all goroutines in the list. The goroutine then re-acquires the mutex before `Wait()` returns.

**Example:**
```go
// Conceptual internal flow of cond.Wait():
// 1. Add current goroutine to cond's notifyList
// 2. Release cond.L (the mutex)
// 3. Park goroutine (block on semaphore)
// 4. [woken by Signal/Broadcast]
// 5. Re-acquire cond.L
// 6. Return to caller (who re-checks condition in loop)
```

**Real-time Scenario:** Understanding the internal notifyList helps debug performance: `Broadcast` waking 1,000 goroutines that all try to re-acquire the same mutex causes a "thundering herd" — only one proceeds while 999 contend.

---

### Q8: Can you use sync.Cond with RWMutex?

**Answer:** Yes. You can create a `sync.Cond` with either `&rwMutex` (associates with the write lock) or `rwMutex.RLocker()` (associates with the read lock). However, using it with the read lock is unusual — `Wait()` releases and re-acquires the read lock, but `Signal`/`Broadcast` don't care which lock type is used. Typically you use `sync.NewCond(&rwMutex)` for write-lock semantics.

**Example:**
```go
var rw sync.RWMutex
cond := sync.NewCond(&rw) // uses write lock

// Writer: update state and signal
rw.Lock()
data = newData
cond.Broadcast() // wake all readers
rw.Unlock()

// Reader: wait for state change
rw.Lock()
for !isReady(data) {
    cond.Wait() // releases write lock, re-acquires on wake
}
process(data)
rw.Unlock()
```

**Real-time Scenario:** A configuration hot-reload system uses `sync.Cond` with `RWMutex` — readers wait for config changes and can all proceed simultaneously once the writer updates the config and calls `Broadcast`.

---

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

### Q1: How does sync.Once guarantee exactly-once execution?

**Answer:** `sync.Once` uses a two-phase approach: an atomic `done` flag for the fast path and a mutex for the slow path. On the first call, it acquires the mutex, runs the function, sets `done` to 1 atomically, then releases the mutex. Subsequent calls see `done == 1` via an atomic load and return immediately (~1ns). The mutex ensures that if multiple goroutines call `Do` simultaneously, only one runs the function while others block until it completes.

**Example:**
```go
var once sync.Once
var config *Config

func GetConfig() *Config {
    once.Do(func() {
        config = loadConfigFromDisk() // runs exactly once
    })
    return config // subsequent calls: just atomic load + return
}
```

**Real-time Scenario:** A database connection pool uses `sync.Once` to initialize the pool on first query, ensuring the pool is created exactly once even when 100 goroutines hit the first query simultaneously at server startup.

---

### Q2: What happens if the function passed to `Once.Do` panics?

**Answer:** If the function panics, `sync.Once` still marks it as "done" — the panic propagates to the calling goroutine, and the function will never be called again on subsequent `Do` calls. This can leave your program in a bad state where initialization failed but `Once` thinks it succeeded. If initialization can fail, use `OnceValue` (Go 1.21+) or wrap the init function to handle errors and use a different pattern that allows retry.

**Example:**
```go
var once sync.Once
once.Do(func() {
    panic("init failed!") // Once is now "done" despite the panic
})
// Subsequent calls will NOT retry:
once.Do(func() {
    fmt.Println("This never runs") // skipped — Once already "done"
})
```

**Real-time Scenario:** A service using `sync.Once` to load TLS certificates panics on first load due to a missing file. All subsequent requests fail because Once won't retry — the fix is to use error-returning initialization with retry logic outside Once.

---

### Q3: Can you reset a sync.Once?

**Answer:** No, there is no `Reset()` method on `sync.Once`. Once it's been triggered, it stays in the "done" state permanently. If you need to re-execute initialization (e.g., config reload), create a new `sync.Once` instance or use a different pattern like `atomic.Pointer` with a generation counter.

**Example:**
```go
type Reloadable struct {
    mu     sync.Mutex
    once   sync.Once
    config *Config
}

func (r *Reloadable) Reload() {
    r.mu.Lock()
    r.once = sync.Once{} // create new Once
    r.mu.Unlock()
}

func (r *Reloadable) Get() *Config {
    r.once.Do(func() { r.config = loadConfig() })
    return r.config
}
```

**Real-time Scenario:** A feature flag service needs to reload configuration periodically. Instead of resetting `sync.Once`, it uses `atomic.Pointer[Config]` to swap configs atomically on reload, which is both simpler and more correct.

---

### Q4: What are `OnceFunc`, `OnceValue`, `OnceValues`?

**Answer:** Added in Go 1.21, these are convenience wrappers around `sync.Once`. `OnceFunc(f)` returns a function that calls `f` exactly once. `OnceValue(f)` returns a function that calls `f` once and caches its return value. `OnceValues(f)` does the same for functions returning two values (typically value + error). They simplify the common "lazy init" pattern by eliminating the need for a separate `Once` variable and result variable.

**Example:**
```go
// Before Go 1.21:
var once sync.Once
var db *sql.DB
func GetDB() *sql.DB {
    once.Do(func() { db, _ = sql.Open("postgres", dsn) })
    return db
}

// Go 1.21+:
var getDB = sync.OnceValue(func() *sql.DB {
    db, _ := sql.Open("postgres", dsn)
    return db
})
// Usage: db := getDB()
```

**Real-time Scenario:** A microservice with 15 lazy-initialized singletons (DB pool, Redis client, gRPC connections) uses `OnceValue` for each, reducing boilerplate from 45 lines (3 per singleton) to 15 lines.

---

### Q5: How does sync.Once handle concurrent callers?

**Answer:** When multiple goroutines call `Do` simultaneously on the same `Once`, the first goroutine to acquire the internal mutex runs the function. All other goroutines block on the mutex until the first one finishes. After the function completes and `done` is set to 1, all blocked goroutines proceed without running the function. Subsequent calls see `done == 1` via a fast atomic load and return immediately without contention.

**Example:**
```go
var once sync.Once
var wg sync.WaitGroup
for i := 0; i < 100; i++ {
    wg.Add(1)
    go func(id int) {
        defer wg.Done()
        once.Do(func() {
            fmt.Println("Init by goroutine", id) // only one prints
            time.Sleep(100 * time.Millisecond)    // other 99 block here
        })
        fmt.Println("Goroutine", id, "proceeding") // all 100 print
    }(i)
}
wg.Wait()
```

**Real-time Scenario:** At server startup, 100 incoming requests simultaneously trigger lazy initialization of the connection pool. All 100 goroutines call `Once.Do` — one creates the pool while 99 others block briefly, then all proceed with the initialized pool.

---

### Q6: What is the performance of sync.Once after first call?

**Answer:** After the first call, `sync.Once.Do` costs approximately 1 nanosecond — it's just an atomic load of the `done` flag that returns immediately. The hot path is a single `atomic.LoadUint32(&o.done)` check, which is cheaper than a mutex lock. This makes it safe to call in hot paths without performance concerns. The slow path (first call) involves a mutex lock and is more expensive.

**Example:**
```go
var once sync.Once
once.Do(func() { /* init */ }) // slow path: ~100ns (mutex + function call)

// All subsequent calls:
once.Do(func() { /* never runs */ }) // fast path: ~1ns (atomic load)
// Safe to call in hot paths — negligible overhead
```

**Real-time Scenario:** A logging library uses `sync.Once` to initialize the logger on first log call. Since every subsequent log statement hits the fast path (~1ns), the Once check adds virtually zero overhead to the millions of log calls per second.

---

### Q7: How would you implement lazy initialization without sync.Once?

**Answer:** Use the double-checked locking pattern: first check an atomic flag without locking (fast path), then if not initialized, acquire a mutex and check again (slow path). This avoids the mutex on every call after initialization. However, `sync.Once` is preferred because it handles edge cases (panics, concurrent callers) correctly and is more readable.

**Example:**
```go
type LazyInit struct {
    done uint32
    mu   sync.Mutex
    val  *Config
}

func (l *LazyInit) Get() *Config {
    if atomic.LoadUint32(&l.done) == 1 { // fast path: no lock
        return l.val
    }
    l.mu.Lock()
    defer l.mu.Unlock()
    if l.done == 0 { // double-check under lock
        l.val = loadConfig()
        atomic.StoreUint32(&l.done, 1)
    }
    return l.val
}
```

**Real-time Scenario:** Understanding double-checked locking is valuable for interviews, but in production Go code, always prefer `sync.Once` or `sync.OnceValue` — they're battle-tested and handle all the edge cases you'd otherwise need to worry about.

---

### Q8: Is sync.Once a replacement for `init()`?

**Answer:** No, they serve different purposes. `init()` runs at package load time (before `main()`), is package-level, and is eager — it always runs. `sync.Once` is instance-level, can be placed anywhere, and is lazy — it runs only when `Do` is first called. Use `init()` for package-level setup that must happen before any package function is called. Use `sync.Once` for deferred initialization that should only happen on demand.

**Example:**
```go
// init(): eager, package-level, runs before main()
func init() {
    flag.Parse()
    log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// sync.Once: lazy, instance-level, runs on first use
var getDB = sync.OnceValue(func() *sql.DB {
    db, err := sql.Open("postgres", os.Getenv("DB_URL"))
    if err != nil { log.Fatal(err) }
    return db
})
```

**Real-time Scenario:** A CLI tool uses `init()` to parse flags and `sync.Once` for database connection — the DB is only connected when the user runs a subcommand that needs it, not for `--help` or `--version`.

---

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
