# Top 50 Go Concurrency Problems — Complete Working Solutions

> Every solution is a **self-contained, compilable `main.go`**. Each problem is designed to teach one or more concurrency concepts that interviewers test at top product companies.

---

## Concept Coverage Map

| Problem | Concepts Tested |
|---------|----------------|
| 1-3 | Goroutines fundamentals, closure bug, WaitGroup |
| 4-6 | Channels (unbuffered, buffered, directional), range over channel |
| 7-9 | Select statement, timeout, nil channel trick |
| 10 | Done channel pattern |
| 11-12 | Mutex, race condition fix |
| 13-14 | RWMutex, read-heavy optimization |
| 15-16 | Atomic operations, CAS loop |
| 17 | sync.Once, lazy initialization |
| 18 | sync.Cond, producer-consumer signaling |
| 19 | sync.Pool, object reuse |
| 20 | sync.Map vs map+mutex |
| 21-22 | Worker pool, bounded concurrency |
| 23-24 | Pipeline pattern, pipeline with error handling |
| 25-26 | Fan-out / Fan-in |
| 27-28 | Producer-consumer, batch consumer |
| 29-30 | Context cancellation, context with timeout |
| 31-32 | errgroup, error propagation |
| 33-34 | Semaphore, weighted semaphore |
| 35-36 | Rate limiter (token bucket), per-key rate limiter |
| 37-38 | Pub-Sub event bus, slow subscriber |
| 39-40 | Future/Promise, WhenAll/WhenAny |
| 41-42 | Graceful shutdown, lifecycle manager |
| 43 | Circuit breaker |
| 44 | Singleflight cache (thundering herd) |
| 45 | Concurrent LRU cache |
| 46 | Concurrent web crawler |
| 47 | Timeout budget propagation |
| 48 | Goroutine leak detector |
| 49 | Deadlock detector / Bank transfer |
| 50 | High-throughput sharded counter |

---

# FOUNDATION (Problems 1-10)

---

## Problem 1: Goroutine Basics — Parallel Printer

**Concepts:** Goroutine creation, sync.WaitGroup, goroutine ordering is non-deterministic

**Problem:** Launch N goroutines that each print their ID. Guarantee all finish before main exits. Observe that output order is non-deterministic.

```go
package main

import (
	"fmt"
	"sync"
)

func main() {
	const n = 10
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fmt.Printf("goroutine %d running\n", id)
		}(i) // pass i as argument — critical to avoid closure bug
	}

	wg.Wait()
	fmt.Println("all goroutines finished")
}
```

**Key Takeaway:** Always call `wg.Add(1)` BEFORE launching the goroutine. Always pass loop variables as function arguments (or use Go 1.22+). Always `defer wg.Done()`.

---

## Problem 2: The Closure Bug — Fix This Code

**Concepts:** Closure variable capture, loop variable semantics

**Problem:** This code should print 0 through 4 but prints "5 5 5 5 5". Fix it two ways.

```go
package main

import (
	"fmt"
	"sync"
)

// BROKEN VERSION — DO NOT USE
// for i := 0; i < 5; i++ {
//     go func() { fmt.Println(i) }()
// }

func main() {
	var wg sync.WaitGroup

	fmt.Println("=== Fix 1: Pass as parameter ===")
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fmt.Println(id)
		}(i) // i is copied at goroutine launch time
	}
	wg.Wait()

	fmt.Println("\n=== Fix 2: Shadow the variable ===")
	for i := 0; i < 5; i++ {
		i := i // creates a new variable scoped to this iteration
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Println(i)
		}()
	}
	wg.Wait()
}
```

**Key Takeaway:** In Go <1.22, the loop variable `i` is shared across iterations. The closure captures the variable, not its value. In Go 1.22+, `for i := range` creates a new variable per iteration.

---

## Problem 3: Goroutine Panic Recovery

**Concepts:** Panic in goroutines, defer/recover, error propagation

**Problem:** A panic in a goroutine crashes the entire process. Build a safe launcher that recovers panics and converts them to errors.

```go
package main

import (
	"fmt"
	"sync"
)

func safeGo(wg *sync.WaitGroup, errCh chan<- error, fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("panic recovered: %v", r)
			}
		}()
		fn()
	}()
}

func main() {
	var wg sync.WaitGroup
	errCh := make(chan error, 10)

	// Normal task
	safeGo(&wg, errCh, func() {
		fmt.Println("task 1: success")
	})

	// Panicking task — won't crash the program
	safeGo(&wg, errCh, func() {
		panic("something went horribly wrong")
	})

	// Another normal task
	safeGo(&wg, errCh, func() {
		fmt.Println("task 3: success")
	})

	wg.Wait()
	close(errCh)

	for err := range errCh {
		fmt.Println("error:", err)
	}
	fmt.Println("program survived all panics")
}
```

---

## Problem 4: Unbuffered Channel — Synchronous Handoff

**Concepts:** Unbuffered channels as synchronization points, goroutine coordination

**Problem:** Two goroutines play ping-pong, passing a counter back and forth. Stop when counter reaches 10.

```go
package main

import "fmt"

func main() {
	ball := make(chan int) // unbuffered: sender blocks until receiver is ready

	// Player 1
	go func() {
		for val := range ball {
			fmt.Println("Player 1 hits:", val)
			if val >= 10 {
				close(ball) // game over — will cause range to exit in player 2
				return
			}
			ball <- val + 1
		}
	}()

	// Main goroutine acts as Player 2
	ball <- 0 // serve
	for val := range ball {
		fmt.Println("Player 2 hits:", val)
		if val >= 10 {
			return
		}
		ball <- val + 1
	}
}
```

**Key Takeaway:** Unbuffered channels are synchronization points — sender and receiver must meet. `range` over a channel exits when the channel is closed.

---

## Problem 5: Buffered Channel — Job Queue

**Concepts:** Buffered channels, channel direction types, producer-consumer

**Problem:** Producer generates 20 jobs. Consumer processes them. Use a buffered channel as the queue.

```go
package main

import (
	"fmt"
	"time"
)

func producer(jobs chan<- int, count int) {
	for i := 1; i <= count; i++ {
		fmt.Printf("produced job %d\n", i)
		jobs <- i
	}
	close(jobs) // producer closes — the rule is: SENDER CLOSES
}

func consumer(jobs <-chan int, done chan<- bool) {
	for job := range jobs {
		fmt.Printf("  consumed job %d\n", job)
		time.Sleep(50 * time.Millisecond) // simulate work
	}
	done <- true
}

func main() {
	jobs := make(chan int, 5) // buffer of 5: producer can get ahead by 5 items
	done := make(chan bool)

	go producer(jobs, 20)
	go consumer(jobs, done)

	<-done
	fmt.Println("all jobs processed")
}
```

**Key Takeaway:** Directional channel types (`chan<-`, `<-chan`) enforce correct usage at compile time. Buffer size controls how far producer can get ahead of consumer.

---

## Problem 6: Channel Directions — Function Signatures

**Concepts:** Send-only/receive-only channels, generator pattern

**Problem:** Implement a number generator that returns a receive-only channel. Consumer reads until channel closes.

```go
package main

import "fmt"

// Generator returns a receive-only channel
func generate(nums ...int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for _, n := range nums {
			out <- n
		}
	}()
	return out
}

// Square reads from in, squares each value, sends to out
func square(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			out <- n * n
		}
	}()
	return out
}

func main() {
	// Pipeline: generate -> square -> print
	nums := generate(2, 3, 4, 5, 6)
	squared := square(nums)

	for result := range squared {
		fmt.Println(result) // 4, 9, 16, 25, 36
	}
}
```

---

## Problem 7: Select — First Response Wins

**Concepts:** Select statement, non-deterministic selection, context cancellation

**Problem:** Query 3 replicas concurrently, return the first response, cancel the rest.

```go
package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

func queryReplica(ctx context.Context, id int) (string, error) {
	delay := time.Duration(rand.Intn(500)) * time.Millisecond
	select {
	case <-time.After(delay):
		return fmt.Sprintf("result from replica %d (took %v)", id, delay), nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func queryFirst(ctx context.Context, replicas int) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // cancels losing goroutines

	type result struct {
		val string
		err error
	}
	ch := make(chan result, replicas)

	for i := 0; i < replicas; i++ {
		go func(id int) {
			val, err := queryReplica(ctx, id)
			ch <- result{val, err}
		}(i)
	}

	// Return first successful result
	for i := 0; i < replicas; i++ {
		r := <-ch
		if r.err == nil {
			return r.val, nil
		}
	}
	return "", fmt.Errorf("all replicas failed")
}

func main() {
	ctx := context.Background()
	result, err := queryFirst(ctx, 3)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(result)
}
```

---

## Problem 8: Select with Timeout

**Concepts:** Select, time.After, timeout patterns, timer reuse

**Problem:** Read from a channel with a 2-second timeout. Demonstrate both the leak-prone and correct approaches.

```go
package main

import (
	"fmt"
	"time"
)

func main() {
	ch := make(chan string, 1)

	// Simulate a slow sender
	go func() {
		time.Sleep(1 * time.Second)
		ch <- "data arrived"
	}()

	// CORRECT: Use time.NewTimer (reusable, no leak)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case msg := <-ch:
		fmt.Println("received:", msg)
		if !timer.Stop() {
			<-timer.C // drain if already fired
		}
	case <-timer.C:
		fmt.Println("timeout!")
	}

	// Demonstrate timeout case
	ch2 := make(chan string)
	go func() {
		time.Sleep(3 * time.Second) // slower than timeout
		ch2 <- "too late"
	}()

	timer2 := time.NewTimer(1 * time.Second)
	defer timer2.Stop()
	select {
	case msg := <-ch2:
		fmt.Println("received:", msg)
	case <-timer2.C:
		fmt.Println("timeout on ch2!")
	}
}
```

**Key Takeaway:** Never use `time.After` inside a for-select loop — it leaks a timer every iteration. Use `time.NewTimer` + `Reset()`.

---

## Problem 9: Select with Nil Channel — Dynamic Enable/Disable

**Concepts:** Nil channel behavior in select (never selected), dynamic case toggling

**Problem:** Read from two sources. When source A is exhausted, disable it without stopping the loop.

```go
package main

import "fmt"

func main() {
	a := make(chan int, 3)
	b := make(chan int, 5)

	// Fill channels
	for i := 1; i <= 3; i++ {
		a <- i * 10
	}
	close(a)
	for i := 1; i <= 5; i++ {
		b <- i
	}
	close(b)

	aCh, bCh := (<-chan int)(a), (<-chan int)(b)
	for aCh != nil || bCh != nil {
		select {
		case val, ok := <-aCh:
			if !ok {
				aCh = nil // set to nil: this case will NEVER be selected again
				fmt.Println("channel A closed")
				continue
			}
			fmt.Println("A:", val)
		case val, ok := <-bCh:
			if !ok {
				bCh = nil
				fmt.Println("channel B closed")
				continue
			}
			fmt.Println("B:", val)
		}
	}
	fmt.Println("both channels drained")
}
```

**Key Takeaway:** Setting a channel to `nil` in select permanently disables that case. This is essential for dynamic fan-in where channels close at different times.

---

## Problem 10: Done Channel Pattern

**Concepts:** Done channel for goroutine lifecycle management, signaling with `chan struct{}`

**Problem:** Launch a background worker that runs until told to stop. Use a done channel (not context — to understand the primitive).

```go
package main

import (
	"fmt"
	"time"
)

func worker(done <-chan struct{}, results chan<- int) {
	i := 0
	for {
		select {
		case <-done:
			fmt.Println("worker: received shutdown signal")
			close(results)
			return
		default:
			i++
			results <- i
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func main() {
	done := make(chan struct{})
	results := make(chan int, 10)

	go worker(done, results)

	// Let worker run for 1 second
	time.Sleep(1 * time.Second)
	close(done) // signal worker to stop (broadcast to all receivers)

	// Drain results
	for r := range results {
		fmt.Println("received:", r)
	}
	fmt.Println("worker stopped cleanly")
}
```

**Key Takeaway:** `chan struct{}` is zero-memory signaling. `close(done)` broadcasts to ALL goroutines listening — much better than sending one value per goroutine.

---

# SYNCHRONIZATION PRIMITIVES (Problems 11-20)

---

## Problem 11: Mutex — Thread-Safe Counter

**Concepts:** sync.Mutex, critical section, data race prevention

**Problem:** Increment a counter from 1000 goroutines. Without a mutex, you get a data race. Demonstrate both broken and fixed versions.

```go
package main

import (
	"fmt"
	"sync"
)

type SafeCounter struct {
	mu    sync.Mutex
	count int
}

func (c *SafeCounter) Increment() {
	c.mu.Lock()
	c.count++
	c.mu.Unlock()
}

func (c *SafeCounter) Value() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func main() {
	counter := &SafeCounter{}
	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counter.Increment()
		}()
	}

	wg.Wait()
	fmt.Printf("final count: %d (expected 1000)\n", counter.Value())
	// Without mutex: random value < 1000 due to lost updates
	// With mutex: always exactly 1000
}
```

---

## Problem 12: Mutex — Bank Transfer Without Deadlock

**Concepts:** Multiple mutexes, lock ordering to prevent deadlock

**Problem:** Transfer money between accounts concurrently. Naive lock ordering deadlocks. Fix with consistent ordering.

```go
package main

import (
	"fmt"
	"sync"
)

type Account struct {
	id      int
	mu      sync.Mutex
	balance int
}

func transfer(from, to *Account, amount int) error {
	// ALWAYS lock the lower ID first to prevent deadlock
	first, second := from, to
	if from.id > to.id {
		first, second = to, from
	}

	first.mu.Lock()
	defer first.mu.Unlock()
	second.mu.Lock()
	defer second.mu.Unlock()

	if from.balance < amount {
		return fmt.Errorf("insufficient funds: have %d, need %d", from.balance, amount)
	}
	from.balance -= amount
	to.balance += amount
	return nil
}

func main() {
	alice := &Account{id: 1, balance: 1000}
	bob := &Account{id: 2, balance: 1000}

	var wg sync.WaitGroup
	// Concurrent transfers in both directions
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			transfer(alice, bob, 10) // alice -> bob
		}()
		go func() {
			defer wg.Done()
			transfer(bob, alice, 10) // bob -> alice (would deadlock without ordering)
		}()
	}
	wg.Wait()

	total := alice.balance + bob.balance
	fmt.Printf("alice: %d, bob: %d, total: %d (expected 2000)\n",
		alice.balance, bob.balance, total)
}
```

---

## Problem 13: RWMutex — Concurrent Config Store

**Concepts:** sync.RWMutex, read-heavy optimization, multiple concurrent readers

**Problem:** Config store with many readers and rare writers. Benchmark shows RWMutex is better than Mutex here.

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

type ConfigStore struct {
	mu     sync.RWMutex
	config map[string]string
}

func NewConfigStore() *ConfigStore {
	return &ConfigStore{config: make(map[string]string)}
}

func (cs *ConfigStore) Get(key string) (string, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	val, ok := cs.config[key]
	return val, ok
}

func (cs *ConfigStore) Set(key, value string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.config[key] = value
}

func (cs *ConfigStore) GetSnapshot() map[string]string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	snapshot := make(map[string]string, len(cs.config))
	for k, v := range cs.config {
		snapshot[k] = v
	}
	return snapshot // return a copy, not a reference
}

func main() {
	store := NewConfigStore()
	store.Set("db_host", "localhost")
	store.Set("db_port", "5432")

	var wg sync.WaitGroup

	// 100 concurrent readers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				val, _ := store.Get("db_host")
				_ = val
			}
		}(i)
	}

	// 1 writer updating config periodically
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			store.Set("db_host", fmt.Sprintf("host-%d", i))
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()
	snap := store.GetSnapshot()
	fmt.Println("final config:", snap)
}
```

---

## Problem 14: RWMutex Upgrade Deadlock — Detect and Fix

**Concepts:** RWMutex upgrade impossibility, double-checked locking

**Problem:** Demonstrate the deadlock from trying to upgrade RLock to Lock, then fix it.

```go
package main

import (
	"fmt"
	"sync"
)

type LazyCache struct {
	mu    sync.RWMutex
	store map[string]int
}

func NewLazyCache() *LazyCache {
	return &LazyCache{store: make(map[string]int)}
}

// BROKEN: This DEADLOCKS
// func (c *LazyCache) GetOrCompute(key string) int {
//     c.mu.RLock()
//     if val, ok := c.store[key]; ok {
//         c.mu.RUnlock()
//         return val
//     }
//     c.mu.Lock() // DEADLOCK: still holding RLock!
//     ...
// }

// CORRECT: Double-checked locking pattern
func (c *LazyCache) GetOrCompute(key string, compute func() int) int {
	// Fast path: read lock
	c.mu.RLock()
	if val, ok := c.store[key]; ok {
		c.mu.RUnlock()
		return val
	}
	c.mu.RUnlock() // must release read lock FIRST

	// Slow path: write lock
	c.mu.Lock()
	defer c.mu.Unlock()

	// Re-check: another goroutine may have computed it
	if val, ok := c.store[key]; ok {
		return val
	}

	val := compute()
	c.store[key] = val
	return val
}

func main() {
	cache := NewLazyCache()
	var wg sync.WaitGroup

	// 100 goroutines all trying to compute the same expensive value
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val := cache.GetOrCompute("answer", func() int {
				fmt.Println("computing... (should print only once)")
				return 42
			})
			_ = val
		}()
	}

	wg.Wait()
	fmt.Println("result:", cache.store["answer"])
}
```

---

## Problem 15: Atomic Operations — Lock-Free Counter

**Concepts:** sync/atomic, atomic.Int64, compare-and-swap loop

**Problem:** Implement a counter using atomics (no mutex). Then implement atomic-max using CAS.

```go
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type AtomicCounter struct {
	val atomic.Int64
}

func (c *AtomicCounter) Increment() int64 { return c.val.Add(1) }
func (c *AtomicCounter) Decrement() int64 { return c.val.Add(-1) }
func (c *AtomicCounter) Load() int64      { return c.val.Load() }

// AtomicMax: update shared max value using CAS loop
func atomicMax(target *atomic.Int64, newVal int64) {
	for {
		old := target.Load()
		if newVal <= old {
			return // current max is already >= newVal
		}
		if target.CompareAndSwap(old, newVal) {
			return // successfully updated
		}
		// CAS failed: another goroutine changed it, retry
	}
}

func main() {
	// Part 1: Counter
	counter := &AtomicCounter{}
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counter.Increment()
		}()
	}
	wg.Wait()
	fmt.Printf("counter: %d (expected 1000)\n", counter.Load())

	// Part 2: Atomic max
	var maxVal atomic.Int64
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(v int64) {
			defer wg.Done()
			atomicMax(&maxVal, v)
		}(int64(i))
	}
	wg.Wait()
	fmt.Printf("max: %d (expected 99)\n", maxVal.Load())
}
```

---

## Problem 16: Atomic Pointer — Lock-Free Config Swap

**Concepts:** atomic.Pointer, read-copy-update pattern, immutable data

**Problem:** Hot-swap configuration without locks. Readers are completely lock-free.

```go
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	DBHost   string
	DBPort   int
	LogLevel string
}

type ConfigHolder struct {
	ptr atomic.Pointer[Config]
}

func NewConfigHolder(initial Config) *ConfigHolder {
	h := &ConfigHolder{}
	h.ptr.Store(&initial)
	return h
}

// Lock-free read
func (h *ConfigHolder) Get() Config {
	return *h.ptr.Load()
}

// Atomic swap (could add mutex if concurrent writers need serialization)
func (h *ConfigHolder) Update(newCfg Config) {
	h.ptr.Store(&newCfg)
}

func main() {
	holder := NewConfigHolder(Config{
		DBHost: "localhost", DBPort: 5432, LogLevel: "info",
	})

	var wg sync.WaitGroup

	// 50 concurrent readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				cfg := holder.Get() // lock-free!
				_ = cfg
			}
		}()
	}

	// 1 writer updating config
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		holder.Update(Config{DBHost: "prod-db.internal", DBPort: 5432, LogLevel: "warn"})
		fmt.Println("config updated!")
	}()

	wg.Wait()
	fmt.Printf("final config: %+v\n", holder.Get())
}
```

---

## Problem 17: sync.Once — Singleton with Retry

**Concepts:** sync.Once semantics, lazy initialization, handling init failures

**Problem:** Standard sync.Once marks as done even on failure. Implement one that retries on error.

```go
package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Standard sync.Once usage
var (
	instance *DB
	initOnce sync.Once
)

type DB struct{ connStr string }

func GetDB() *DB {
	initOnce.Do(func() {
		fmt.Println("initializing DB (only once)")
		instance = &DB{connStr: "postgres://localhost/mydb"}
	})
	return instance
}

// RetryableOnce: retries if init fails
type RetryableOnce struct {
	done atomic.Bool
	mu   sync.Mutex
}

func (o *RetryableOnce) Do(f func() error) error {
	if o.done.Load() {
		return nil // fast path
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.done.Load() {
		return nil // double-check
	}
	if err := f(); err != nil {
		return err // don't mark done — allow retry
	}
	o.done.Store(true)
	return nil
}

func main() {
	// Standard Once
	for i := 0; i < 5; i++ {
		db := GetDB()
		fmt.Printf("got DB: %v\n", db.connStr)
	}

	// RetryableOnce
	var ro RetryableOnce
	attempts := 0
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ro.Do(func() error {
				attempts++
				if attempts < 3 {
					fmt.Printf("  attempt %d: failing\n", attempts)
					return errors.New("not ready")
				}
				fmt.Printf("  attempt %d: success!\n", attempts)
				return nil
			})
		}()
	}
	wg.Wait()
}
```

---

## Problem 18: sync.Cond — Bounded Buffer

**Concepts:** Condition variables, Wait in a loop (spurious wakeups), Signal vs Broadcast

**Problem:** Classic producer-consumer with a bounded buffer using sync.Cond.

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

type BoundedBuffer struct {
	mu       sync.Mutex
	notFull  *sync.Cond
	notEmpty *sync.Cond
	buf      []int
	capacity int
}

func NewBoundedBuffer(cap int) *BoundedBuffer {
	bb := &BoundedBuffer{buf: make([]int, 0, cap), capacity: cap}
	bb.notFull = sync.NewCond(&bb.mu)
	bb.notEmpty = sync.NewCond(&bb.mu)
	return bb
}

func (bb *BoundedBuffer) Put(val int) {
	bb.mu.Lock()
	defer bb.mu.Unlock()
	for len(bb.buf) == bb.capacity { // MUST be 'for', not 'if' (spurious wakeups)
		bb.notFull.Wait()
	}
	bb.buf = append(bb.buf, val)
	bb.notEmpty.Signal() // wake one consumer
}

func (bb *BoundedBuffer) Get() int {
	bb.mu.Lock()
	defer bb.mu.Unlock()
	for len(bb.buf) == 0 {
		bb.notEmpty.Wait()
	}
	val := bb.buf[0]
	bb.buf = bb.buf[1:]
	bb.notFull.Signal() // wake one producer
	return val
}

func main() {
	buf := NewBoundedBuffer(3)
	var wg sync.WaitGroup

	// Producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 10; i++ {
			buf.Put(i)
			fmt.Printf("produced: %d\n", i)
		}
	}()

	// Consumer (slower than producer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			val := buf.Get()
			fmt.Printf("  consumed: %d\n", val)
			time.Sleep(100 * time.Millisecond)
		}
	}()

	wg.Wait()
}
```

---

## Problem 19: sync.Pool — Object Reuse

**Concepts:** sync.Pool for reducing GC pressure, pool lifecycle (GC can reclaim items)

**Problem:** Process log entries without allocating new structs for each one.

```go
package main

import (
	"bytes"
	"fmt"
	"sync"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		fmt.Println("  allocating new buffer")
		return new(bytes.Buffer)
	},
}

func processRequest(id int) string {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset() // CRITICAL: reset before use
	defer bufferPool.Put(buf)

	// Use the buffer
	fmt.Fprintf(buf, "processing request %d with data...", id)
	return buf.String()
}

func main() {
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			result := processRequest(id)
			_ = result
		}(i)
	}

	wg.Wait()
	// You'll see "allocating new buffer" printed far fewer than 20 times
	// because buffers are reused from the pool
	fmt.Println("done — buffers were reused via sync.Pool")
}
```

**Key Takeaway:** sync.Pool is NOT a connection pool. GC can reclaim items at any time. Use it for short-lived, frequently allocated objects (buffers, structs in hot paths).

---

## Problem 20: sync.Map vs map+Mutex

**Concepts:** sync.Map for specific use cases, LoadOrStore, Range

**Problem:** Demonstrate when sync.Map is better and when map+mutex is better.

```go
package main

import (
	"fmt"
	"sync"
)

func main() {
	// sync.Map: great when keys are stable and reads dominate
	var sm sync.Map
	var wg sync.WaitGroup

	// Many goroutines writing DISJOINT keys (sync.Map sweet spot)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			sm.Store(key, id*10)
		}(i)
	}
	wg.Wait()

	// LoadOrStore: atomic "get or create" — no double-check locking needed
	actual, loaded := sm.LoadOrStore("key-0", 999)
	fmt.Printf("key-0: val=%v, already_existed=%v\n", actual, loaded)

	// Range: iterate over all entries
	count := 0
	sm.Range(func(key, value interface{}) bool {
		count++
		return true // return false to stop iteration
	})
	fmt.Printf("total entries: %d\n", count)

	// map+Mutex: better when you need custom operations, iteration control, or bulk ops
	type MapWithMutex struct {
		mu sync.RWMutex
		m  map[string]int
	}
	mwm := &MapWithMutex{m: make(map[string]int)}
	mwm.mu.Lock()
	// Can do complex multi-key operations atomically
	mwm.m["a"] = 1
	mwm.m["b"] = 2
	total := mwm.m["a"] + mwm.m["b"] // consistent view of both keys
	mwm.mu.Unlock()
	fmt.Printf("consistent total: %d\n", total)
}
```

---

# CORE PATTERNS (Problems 21-30)

---

## Problem 21: Worker Pool — Fixed Size

**Concepts:** Worker pool, bounded concurrency, graceful shutdown

**Problem:** Process URL fetch jobs with exactly N concurrent workers.

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Job struct {
	ID  int
	URL string
}

type Result struct {
	JobID    int
	Response string
	Err      error
}

func worker(ctx context.Context, id int, jobs <-chan Job, results chan<- Result) {
	for {
		select {
		case job, ok := <-jobs:
			if !ok {
				return // channel closed, exit worker
			}
			// Simulate HTTP fetch
			time.Sleep(100 * time.Millisecond)
			results <- Result{
				JobID:    job.ID,
				Response: fmt.Sprintf("worker %d fetched %s", id, job.URL),
			}
		case <-ctx.Done():
			return
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numWorkers = 3
	jobs := make(chan Job, 10)
	results := make(chan Result, 10)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(ctx, id, jobs, results)
		}(i)
	}

	// Send jobs
	urls := []string{"google.com", "github.com", "go.dev", "aws.com", "azure.com",
		"stripe.com", "uber.com", "netflix.com", "meta.com", "apple.com"}
	go func() {
		for i, url := range urls {
			jobs <- Job{ID: i, URL: url}
		}
		close(jobs) // signal no more jobs
	}()

	// Close results after workers finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	for r := range results {
		fmt.Printf("  job %d: %s\n", r.JobID, r.Response)
	}
	fmt.Println("all jobs done")
}
```

---

## Problem 22: Bounded Concurrency with errgroup

**Concepts:** errgroup.Group, SetLimit, first-error cancellation

**Problem:** Process items with bounded concurrency. If any fails, cancel all and return the error.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

func processItem(ctx context.Context, id int) error {
	// Simulate work
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	if id == 7 {
		return fmt.Errorf("item %d failed", id)
	}
	fmt.Printf("  processed item %d\n", id)
	return nil
}

func main() {
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(3) // at most 3 concurrent goroutines

	for i := 0; i < 15; i++ {
		id := i
		g.Go(func() error {
			return processItem(ctx, id)
		})
	}

	if err := g.Wait(); err != nil {
		fmt.Println("pipeline failed:", err)
		// errgroup cancelled all other goroutines via context
	} else {
		fmt.Println("all items processed successfully")
	}
}
```

**Note:** This requires `go get golang.org/x/sync`. If you can't install it, see Problem 21 for the channel-based equivalent.

---

## Problem 23: Pipeline — Three-Stage Data Processing

**Concepts:** Pipeline pattern, channel chaining, stage ownership

**Problem:** generate -> transform -> filter -> collect. Each stage runs in its own goroutine.

```go
package main

import (
	"context"
	"fmt"
)

// Stage 1: Generate numbers
func generate(ctx context.Context, start, end int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for i := start; i <= end; i++ {
			select {
			case out <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// Stage 2: Square each number
func squareStage(ctx context.Context, in <-chan int) <-chan int {
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

// Stage 3: Filter — keep only values > threshold
func filterStage(ctx context.Context, in <-chan int, threshold int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			if n > threshold {
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

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Compose pipeline: generate(1..20) -> square -> filter(>100)
	nums := generate(ctx, 1, 20)
	squared := squareStage(ctx, nums)
	filtered := filterStage(ctx, squared, 100)

	// Collect
	for v := range filtered {
		fmt.Println(v) // 121, 144, 169, 196, 225, 256, 289, 324, 361, 400
	}
}
```

---

## Problem 24: Pipeline with Error Channel

**Concepts:** Error handling in pipelines, Result type pattern

**Problem:** Pipeline where each stage can produce errors. Errors flow alongside data.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
)

type Result[T any] struct {
	Value T
	Err   error
}

func parseStage(ctx context.Context, in <-chan string) <-chan Result[int] {
	out := make(chan Result[int])
	go func() {
		defer close(out)
		for s := range in {
			val, err := strconv.Atoi(s)
			select {
			case out <- Result[int]{Value: val, Err: err}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func doubleStage(ctx context.Context, in <-chan Result[int]) <-chan Result[int] {
	out := make(chan Result[int])
	go func() {
		defer close(out)
		for r := range in {
			if r.Err != nil {
				select {
				case out <- r: // propagate error downstream
				case <-ctx.Done():
					return
				}
				continue
			}
			if r.Value < 0 {
				r.Err = errors.New("negative value")
			} else {
				r.Value *= 2
			}
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func main() {
	ctx := context.Background()

	input := make(chan string, 6)
	for _, s := range []string{"10", "abc", "20", "-5", "30", "xyz"} {
		input <- s
	}
	close(input)

	parsed := parseStage(ctx, input)
	doubled := doubleStage(ctx, parsed)

	for r := range doubled {
		if r.Err != nil {
			fmt.Printf("  ERROR: %v\n", r.Err)
		} else {
			fmt.Printf("  OK: %d\n", r.Value)
		}
	}
}
```

---

## Problem 25: Fan-Out / Fan-In

**Concepts:** Fan-out (one → many), fan-in (many → one), WaitGroup for closing merged channel

**Problem:** Fan out work to multiple workers, fan results back into a single channel.

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func fanOut(ctx context.Context, in <-chan int, workers int) []<-chan int {
	channels := make([]<-chan int, workers)
	for i := 0; i < workers; i++ {
		channels[i] = processWorker(ctx, in)
	}
	return channels
}

func processWorker(ctx context.Context, in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			time.Sleep(50 * time.Millisecond) // simulate work
			select {
			case out <- n * n:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func fanIn(ctx context.Context, channels ...<-chan int) <-chan int {
	out := make(chan int)
	var wg sync.WaitGroup
	for _, ch := range channels {
		wg.Add(1)
		go func(c <-chan int) {
			defer wg.Done()
			for val := range c {
				select {
				case out <- val:
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

func main() {
	ctx := context.Background()

	// Source: 20 numbers
	source := make(chan int, 20)
	for i := 1; i <= 20; i++ {
		source <- i
	}
	close(source)

	// Fan out to 4 workers
	workerChannels := fanOut(ctx, source, 4)

	// Fan in all results
	merged := fanIn(ctx, workerChannels...)

	count := 0
	for result := range merged {
		fmt.Printf("%d ", result)
		count++
	}
	fmt.Printf("\ntotal results: %d\n", count)
}
```

---

## Problem 26: Fan-Out/Fan-In with Order Preservation

**Concepts:** Indexed results, reorder buffer, parallel processing with ordered output

**Problem:** Process items in parallel but emit results in the same order as input.

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func orderedParallelMap(ctx context.Context, items []string, workers int, fn func(string) string) []string {
	type indexedResult struct {
		index int
		value string
	}

	jobs := make(chan int, len(items))      // send indices
	results := make(chan indexedResult, len(items))

	// Fan-out: workers
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				val := fn(items[idx])
				results <- indexedResult{index: idx, value: val}
			}
		}()
	}

	// Send all job indices
	for i := range items {
		jobs <- i
	}
	close(jobs)

	// Close results when all workers done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and reorder
	ordered := make([]string, len(items))
	for r := range results {
		ordered[r.index] = r.value
	}
	return ordered
}

func main() {
	items := []string{"apple", "banana", "cherry", "date", "elderberry",
		"fig", "grape", "honeydew"}

	results := orderedParallelMap(context.Background(), items, 4, func(s string) string {
		time.Sleep(50 * time.Millisecond) // simulate varying work
		return fmt.Sprintf("%s(%d)", s, len(s))
	})

	for i, r := range results {
		fmt.Printf("[%d] %s\n", i, r) // guaranteed in order
	}
}
```

---

## Problem 27: Producer-Consumer — Multiple Producers, Multiple Consumers

**Concepts:** MPMC pattern, producer sync for closing, consumer WaitGroup

**Problem:** M producers, N consumers, proper shutdown coordination.

```go
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	const (
		numProducers = 3
		numConsumers = 5
		itemsPerProd = 10
	)

	queue := make(chan int, 10)
	var produced, consumed atomic.Int64

	// Producers
	var prodWg sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		prodWg.Add(1)
		go func(id int) {
			defer prodWg.Done()
			for i := 0; i < itemsPerProd; i++ {
				item := id*1000 + i
				queue <- item
				produced.Add(1)
			}
		}(p)
	}

	// Coordinating goroutine: close queue after all producers finish
	go func() {
		prodWg.Wait()
		close(queue) // safe: only closed AFTER all producers are done
	}()

	// Consumers
	var consWg sync.WaitGroup
	for c := 0; c < numConsumers; c++ {
		consWg.Add(1)
		go func(id int) {
			defer consWg.Done()
			for item := range queue {
				_ = item
				consumed.Add(1)
				time.Sleep(10 * time.Millisecond)
			}
		}(c)
	}

	consWg.Wait()
	fmt.Printf("produced: %d, consumed: %d\n", produced.Load(), consumed.Load())
	// Both should equal numProducers * itemsPerProd = 30
}
```

---

## Problem 28: Batch Consumer — Flush on Size or Timeout

**Concepts:** Batching, time-based flushing, select with timer

**Problem:** Consumer collects items into batches. Flush when batch is full OR after a timeout, whichever comes first.

```go
package main

import (
	"fmt"
	"time"
)

func batchConsumer(in <-chan int, batchSize int, flushInterval time.Duration) {
	batch := make([]int, 0, batchSize)
	timer := time.NewTimer(flushInterval)
	defer timer.Stop()

	flush := func(reason string) {
		if len(batch) > 0 {
			fmt.Printf("  flush (%s): %v\n", reason, batch)
			batch = batch[:0] // reset
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(flushInterval)
	}

	for {
		select {
		case item, ok := <-in:
			if !ok {
				flush("channel closed")
				return
			}
			batch = append(batch, item)
			if len(batch) >= batchSize {
				flush("batch full")
			}
		case <-timer.C:
			flush("timeout")
			timer.Reset(flushInterval)
		}
	}
}

func main() {
	ch := make(chan int, 20)

	go func() {
		// Send a burst
		for i := 1; i <= 8; i++ {
			ch <- i
		}
		// Pause — triggers timeout flush
		time.Sleep(600 * time.Millisecond)
		// Send a few more
		for i := 9; i <= 11; i++ {
			ch <- i
		}
		close(ch)
	}()

	batchConsumer(ch, 5, 500*time.Millisecond)
	// Expected: flush(batch full): [1 2 3 4 5], flush(timeout): [6 7 8], flush(channel closed): [9 10 11]
}
```

---

## Problem 29: Context Cancellation — Cooperative Shutdown

**Concepts:** context.WithCancel, checking ctx.Done(), cleanup after cancellation

**Problem:** Long-running worker that checks for cancellation and cleans up properly.

```go
package main

import (
	"context"
	"fmt"
	"time"
)

func longRunningTask(ctx context.Context) error {
	for i := 1; ; i++ {
		select {
		case <-ctx.Done():
			fmt.Printf("task: cancelled at iteration %d, cleaning up...\n", i)
			// Cleanup: flush buffers, close connections, etc.
			time.Sleep(100 * time.Millisecond) // simulate cleanup
			fmt.Println("task: cleanup complete")
			return ctx.Err()
		default:
			fmt.Printf("task: working on iteration %d\n", i)
			time.Sleep(200 * time.Millisecond) // simulate work
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- longRunningTask(ctx)
	}()

	// Let it run for 1 second, then cancel
	time.Sleep(1 * time.Second)
	fmt.Println("main: cancelling task...")
	cancel()

	err := <-done
	fmt.Println("main: task returned:", err)
}
```

---

## Problem 30: Context with Timeout — HTTP-Style Request Processing

**Concepts:** context.WithTimeout, deadline propagation, timeout budget

**Problem:** Simulate an HTTP handler that calls a slow service with a timeout.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func slowService(ctx context.Context) (string, error) {
	// Simulate slow work
	select {
	case <-time.After(2 * time.Second):
		return "service response", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func handleRequest(ctx context.Context) (string, error) {
	// Set 500ms timeout for downstream call
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel() // ALWAYS defer cancel

	result, err := slowService(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "", fmt.Errorf("upstream timeout: %w", err)
		}
		return "", err
	}
	return result, nil
}

func main() {
	// Simulate request with 3-second overall timeout
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := handleRequest(ctx)
	if err != nil {
		fmt.Println("request failed:", err) // will print: upstream timeout
	} else {
		fmt.Println("request succeeded:", result)
	}

	// Show remaining budget
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		fmt.Printf("remaining budget: %v\n", remaining)
	}
}
```

---

# ADVANCED PATTERNS (Problems 31-40)

---

## Problem 31: errgroup — Parallel API Calls with First-Error Cancel

**Concepts:** errgroup.WithContext, automatic cancellation, structured concurrency

**Problem:** Fetch data from 3 services in parallel. If any fails, cancel the rest immediately.

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Since errgroup requires external dependency, here's a from-scratch version
type ErrGroup struct {
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	err    error
}

func WithContext(ctx context.Context) (*ErrGroup, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &ErrGroup{ctx: ctx, cancel: cancel}, ctx
}

func (g *ErrGroup) Go(fn func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := fn(); err != nil {
			g.mu.Lock()
			if g.err == nil {
				g.err = err
				g.cancel() // cancel all other goroutines
			}
			g.mu.Unlock()
		}
	}()
}

func (g *ErrGroup) Wait() error {
	g.wg.Wait()
	g.cancel()
	return g.err
}

// Simulated services
func fetchUsers(ctx context.Context) (string, error) {
	time.Sleep(100 * time.Millisecond)
	return "users data", nil
}

func fetchOrders(ctx context.Context) (string, error) {
	time.Sleep(50 * time.Millisecond)
	return "", fmt.Errorf("orders service unavailable")
}

func fetchProducts(ctx context.Context) (string, error) {
	select {
	case <-time.After(200 * time.Millisecond):
		return "products data", nil
	case <-ctx.Done():
		fmt.Println("  products: cancelled (good!)")
		return "", ctx.Err()
	}
}

func main() {
	g, ctx := WithContext(context.Background())

	var users, orders, products string

	g.Go(func() error {
		var err error
		users, err = fetchUsers(ctx)
		return err
	})
	g.Go(func() error {
		var err error
		orders, err = fetchOrders(ctx) // this will fail
		return err
	})
	g.Go(func() error {
		var err error
		products, err = fetchProducts(ctx) // this will be cancelled
		return err
	})

	if err := g.Wait(); err != nil {
		fmt.Println("aggregation failed:", err)
	} else {
		fmt.Println(users, orders, products)
	}
}
```

---

## Problem 32: Error Collection — Collect All Errors

**Concepts:** Multi-error aggregation, errors.Join, concurrent error collection

**Problem:** Run N tasks concurrently, collect ALL errors (don't stop on first).

```go
package main

import (
	"errors"
	"fmt"
	"sync"
)

func collectAllErrors(tasks []func() error) error {
	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)

	for i, task := range tasks {
		wg.Add(1)
		go func(id int, fn func() error) {
			defer wg.Done()
			if err := fn(); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("task %d: %w", id, err))
				mu.Unlock()
			}
		}(i, task)
	}

	wg.Wait()
	return errors.Join(errs...) // Go 1.20+: joins multiple errors into one
}

func main() {
	tasks := []func() error{
		func() error { return nil },
		func() error { return fmt.Errorf("network timeout") },
		func() error { return nil },
		func() error { return fmt.Errorf("disk full") },
		func() error { return nil },
	}

	if err := collectAllErrors(tasks); err != nil {
		fmt.Println("errors occurred:")
		fmt.Println(err)
		// Can unwrap individual errors:
		for _, e := range []error{fmt.Errorf("network timeout"), fmt.Errorf("disk full")} {
			_ = e // errors.Is/As would work with proper wrapping
		}
	} else {
		fmt.Println("all tasks succeeded")
	}
}
```

---

## Problem 33: Semaphore — Bounded Concurrent Downloads

**Concepts:** Channel-based semaphore, bounded concurrency without worker pool

**Problem:** Download 20 files with at most 3 concurrent downloads.

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Semaphore struct {
	ch chan struct{}
}

func NewSemaphore(max int) *Semaphore {
	return &Semaphore{ch: make(chan struct{}, max)}
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

func download(url string) {
	time.Sleep(200 * time.Millisecond)
}

func main() {
	sem := NewSemaphore(3) // max 3 concurrent
	var wg sync.WaitGroup
	ctx := context.Background()

	urls := make([]string, 20)
	for i := range urls {
		urls[i] = fmt.Sprintf("https://example.com/file-%d", i)
	}

	start := time.Now()
	for _, url := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			sem.Acquire(ctx)
			defer sem.Release()
			download(u)
			fmt.Printf("  downloaded: %s\n", u)
		}(url)
	}
	wg.Wait()
	fmt.Printf("all done in %v (with 3 concurrent limit)\n", time.Since(start))
	// ~1.4s (20 files / 3 concurrent * 200ms each)
}
```

---

## Problem 34: Token Bucket Rate Limiter

**Concepts:** Rate limiting, token bucket algorithm, time-based refill

**Problem:** Implement a rate limiter from scratch — no external packages.

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type RateLimiter struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

func NewRateLimiter(ratePerSec float64, burst int) *RateLimiter {
	return &RateLimiter{
		tokens:     float64(burst),
		maxTokens:  float64(burst),
		refillRate: ratePerSec,
		lastRefill: time.Now(),
	}
}

func (rl *RateLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(rl.lastRefill).Seconds()
	rl.tokens += elapsed * rl.refillRate
	if rl.tokens > rl.maxTokens {
		rl.tokens = rl.maxTokens
	}
	rl.lastRefill = now
}

func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.refill()
	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}
	return false
}

func (rl *RateLimiter) Wait(ctx context.Context) error {
	for {
		if rl.Allow() {
			return nil
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func main() {
	limiter := NewRateLimiter(5, 5) // 5 requests/sec, burst of 5

	start := time.Now()
	for i := 0; i < 15; i++ {
		limiter.Wait(context.Background())
		fmt.Printf("request %d at %v\n", i+1, time.Since(start).Truncate(time.Millisecond))
	}
	// First 5 are instant (burst), then ~200ms apart (5/sec)
}
```

---

## Problem 35: Per-Key Rate Limiter

**Concepts:** Map of rate limiters, background cleanup, per-user throttling

**Problem:** Rate limit API requests per user ID.

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type PerKeyLimiter struct {
	mu       sync.Mutex
	limiters map[string]*RateLimiter
	rate     float64
	burst    int
}

func NewPerKeyLimiter(ratePerSec float64, burst int) *PerKeyLimiter {
	return &PerKeyLimiter{
		limiters: make(map[string]*RateLimiter),
		rate:     ratePerSec,
		burst:    burst,
	}
}

func (pkl *PerKeyLimiter) Allow(key string) bool {
	pkl.mu.Lock()
	limiter, ok := pkl.limiters[key]
	if !ok {
		limiter = NewRateLimiter(pkl.rate, pkl.burst)
		pkl.limiters[key] = limiter
	}
	pkl.mu.Unlock()
	return limiter.Allow()
}

func main() {
	limiter := NewPerKeyLimiter(2, 2) // 2 req/sec per user

	users := []string{"alice", "bob", "alice", "alice", "bob", "alice", "alice"}
	for _, user := range users {
		allowed := limiter.Allow(user)
		status := "ALLOWED"
		if !allowed {
			status = "REJECTED"
		}
		fmt.Printf("user=%s: %s\n", user, status)
	}
	// alice: first 2 allowed (burst), 3rd+ rejected
	// bob: first 2 allowed (burst)

	fmt.Println("\n--- after waiting 1 second ---")
	time.Sleep(1 * time.Second)
	fmt.Printf("alice: %v\n", limiter.Allow("alice")) // true: tokens refilled
}
```

---

## Problem 36: Pub-Sub Event Bus

**Concepts:** Publish-subscribe, topic-based routing, subscriber management, non-blocking send

**Problem:** In-process event bus with Subscribe, Unsubscribe, Publish.

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

type EventBus struct {
	mu     sync.RWMutex
	subs   map[string]map[int]chan interface{} // topic -> {id -> channel}
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
	eb.nextID++
	id := eb.nextID
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
		select {
		case ch <- msg:
		default: // non-blocking: drop if subscriber is slow
		}
	}
}

func main() {
	bus := NewEventBus()

	// Subscriber 1 for "orders"
	id1, ch1 := bus.Subscribe("orders", 10)
	go func() {
		for msg := range ch1 {
			fmt.Printf("  sub1 got: %v\n", msg)
		}
		fmt.Println("  sub1 channel closed")
	}()

	// Subscriber 2 for "orders"
	_, ch2 := bus.Subscribe("orders", 10)
	go func() {
		for msg := range ch2 {
			fmt.Printf("  sub2 got: %v\n", msg)
		}
	}()

	// Publish
	bus.Publish("orders", "order-123 created")
	bus.Publish("orders", "order-456 created")

	time.Sleep(100 * time.Millisecond)

	// Unsubscribe sub1
	bus.Unsubscribe("orders", id1)
	bus.Publish("orders", "order-789 created") // only sub2 gets this

	time.Sleep(100 * time.Millisecond)
}
```

---

## Problem 37: Pub-Sub with Slow Subscriber Strategies

**Concepts:** DropNewest, DropOldest, Block strategies

**Problem:** Extend the event bus to support different overflow strategies per subscriber.

```go
package main

import "fmt"

type Strategy int

const (
	Block      Strategy = iota
	DropNewest          // drop new message if buffer full
	DropOldest          // drop oldest message in buffer
)

type SmartSubscriber struct {
	ch       chan string
	strategy Strategy
}

func NewSmartSubscriber(bufSize int, strategy Strategy) *SmartSubscriber {
	return &SmartSubscriber{ch: make(chan string, bufSize), strategy: strategy}
}

func (s *SmartSubscriber) Send(msg string) {
	switch s.strategy {
	case Block:
		s.ch <- msg
	case DropNewest:
		select {
		case s.ch <- msg:
		default:
			fmt.Printf("    [drop-newest] dropped: %s\n", msg)
		}
	case DropOldest:
		select {
		case s.ch <- msg:
		default:
			dropped := <-s.ch // remove oldest
			fmt.Printf("    [drop-oldest] dropped old: %s\n", dropped)
			s.ch <- msg // add new
		}
	}
}

func main() {
	sub := NewSmartSubscriber(3, DropOldest)

	// Fill buffer
	sub.Send("msg-1")
	sub.Send("msg-2")
	sub.Send("msg-3")
	// Buffer full — next send drops oldest
	sub.Send("msg-4") // drops msg-1
	sub.Send("msg-5") // drops msg-2

	// Drain
	close(sub.ch)
	for msg := range sub.ch {
		fmt.Println("received:", msg)
	}
	// Should see: msg-3, msg-4, msg-5
}
```

---

## Problem 38: Future/Promise — Async Computation

**Concepts:** Future pattern, channel-based result, multiple waiters

**Problem:** Implement a generic Future that supports Get(), GetWithTimeout(), and can be awaited by multiple goroutines.

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

type Future[T any] struct {
	ch   chan struct{} // closed when result is ready
	val  T
	err  error
	once sync.Once
}

func Async[T any](fn func() (T, error)) *Future[T] {
	f := &Future[T]{ch: make(chan struct{})}
	go func() {
		f.val, f.err = fn()
		close(f.ch) // broadcast to all waiters
	}()
	return f
}

func (f *Future[T]) Get() (T, error) {
	<-f.ch
	return f.val, f.err
}

func (f *Future[T]) GetWithTimeout(d time.Duration) (T, error) {
	select {
	case <-f.ch:
		return f.val, f.err
	case <-time.After(d):
		var zero T
		return zero, fmt.Errorf("timeout after %v", d)
	}
}

func main() {
	// Create an async computation
	future := Async(func() (int, error) {
		time.Sleep(500 * time.Millisecond)
		return 42, nil
	})

	// Multiple goroutines can wait on the same future
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			val, err := future.Get()
			fmt.Printf("  goroutine %d got: %d, err: %v\n", id, val, err)
		}(i)
	}
	wg.Wait()

	// With timeout
	slowFuture := Async(func() (string, error) {
		time.Sleep(5 * time.Second)
		return "slow result", nil
	})
	_, err := slowFuture.GetWithTimeout(100 * time.Millisecond)
	fmt.Println("slow future:", err) // timeout
}
```

---

## Problem 39: WhenAll / WhenAny

**Concepts:** Future composition, parallel await, first-result pattern

**Problem:** WhenAll waits for all futures. WhenAny returns the first result.

```go
package main

import (
	"fmt"
	"time"
)

// Reusing Future from Problem 38
type Future[T any] struct {
	ch  chan struct{}
	val T
	err error
}

func Async[T any](fn func() (T, error)) *Future[T] {
	f := &Future[T]{ch: make(chan struct{})}
	go func() {
		f.val, f.err = fn()
		close(f.ch)
	}()
	return f
}

func (f *Future[T]) Get() (T, error) {
	<-f.ch
	return f.val, f.err
}

func WhenAll[T any](futures ...*Future[T]) *Future[[]T] {
	return Async(func() ([]T, error) {
		results := make([]T, len(futures))
		for i, f := range futures {
			val, err := f.Get()
			if err != nil {
				return nil, err
			}
			results[i] = val
		}
		return results, nil
	})
}

func WhenAny[T any](futures ...*Future[T]) *Future[T] {
	return Async(func() (T, error) {
		ch := make(chan struct {
			val T
			err error
		}, 1) // buffered: first result wins
		for _, f := range futures {
			go func(fut *Future[T]) {
				val, err := fut.Get()
				select {
				case ch <- struct {
					val T
					err error
				}{val, err}:
				default:
				}
			}(f)
		}
		r := <-ch
		return r.val, r.err
	})
}

func main() {
	f1 := Async(func() (string, error) { time.Sleep(300 * time.Millisecond); return "fast", nil })
	f2 := Async(func() (string, error) { time.Sleep(500 * time.Millisecond); return "medium", nil })
	f3 := Async(func() (string, error) { time.Sleep(100 * time.Millisecond); return "fastest", nil })

	// WhenAny: returns first
	start := time.Now()
	first, _ := WhenAny(f1, f2, f3).Get()
	fmt.Printf("first result: %q in %v\n", first, time.Since(start).Truncate(time.Millisecond))

	// WhenAll: waits for all
	fa := Async(func() (int, error) { time.Sleep(100 * time.Millisecond); return 1, nil })
	fb := Async(func() (int, error) { time.Sleep(200 * time.Millisecond); return 2, nil })
	fc := Async(func() (int, error) { time.Sleep(150 * time.Millisecond); return 3, nil })

	start = time.Now()
	all, _ := WhenAll(fa, fb, fc).Get()
	fmt.Printf("all results: %v in %v\n", all, time.Since(start).Truncate(time.Millisecond))
}
```

---

## Problem 40: Context Value — Request-Scoped Data

**Concepts:** context.WithValue, type-safe keys, request tracing

**Problem:** Implement request-scoped trace ID propagation through context.

```go
package main

import (
	"context"
	"fmt"
	"math/rand"
)

// Use unexported type as key to prevent collisions
type contextKey struct{ name string }

var traceIDKey = contextKey{"traceID"}
var userIDKey = contextKey{"userID"}

func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

func TraceID(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok {
		return v
	}
	return "unknown"
}

func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

func UserID(ctx context.Context) string {
	if v, ok := ctx.Value(userIDKey).(string); ok {
		return v
	}
	return "anonymous"
}

// Simulated middleware and handlers
func handleRequest(ctx context.Context) {
	fmt.Printf("[%s] user=%s: handling request\n", TraceID(ctx), UserID(ctx))
	fetchData(ctx)
}

func fetchData(ctx context.Context) {
	fmt.Printf("[%s] user=%s: fetching data from DB\n", TraceID(ctx), UserID(ctx))
	callExternalAPI(ctx)
}

func callExternalAPI(ctx context.Context) {
	fmt.Printf("[%s] user=%s: calling external API\n", TraceID(ctx), UserID(ctx))
}

func main() {
	// Simulate incoming requests
	for i := 0; i < 3; i++ {
		ctx := context.Background()
		ctx = WithTraceID(ctx, fmt.Sprintf("trace-%d", rand.Intn(10000)))
		ctx = WithUserID(ctx, fmt.Sprintf("user-%d", i))

		go handleRequest(ctx) // each request has its own context
	}

	// Wait
	fmt.Scanln()
}
```

---

# PRODUCTION PATTERNS (Problems 41-50)

---

## Problem 41: Graceful HTTP Server Shutdown

**Concepts:** signal.NotifyContext, http.Server.Shutdown, resource cleanup ordering

```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second) // simulate slow request
		fmt.Fprintln(w, "hello")
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})

	server := &http.Server{Addr: ":8080", Handler: mux}

	// Start server
	go func() {
		fmt.Println("server starting on :8080")
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Printf("server error: %v\n", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	fmt.Println("\nshutdown signal received...")

	// Give in-flight requests 30 seconds to complete
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("shutdown error: %v\n", err)
	}

	// Close other resources here (DB, cache, etc.) in reverse init order
	fmt.Println("server stopped cleanly")
}
```

---

## Problem 42: Component Lifecycle Manager

**Concepts:** Dependency-ordered startup, reverse-order shutdown, per-component timeout

```go
package main

import (
	"context"
	"fmt"
	"time"
)

type Component struct {
	Name  string
	Start func(ctx context.Context) error
	Stop  func(ctx context.Context) error
}

type Lifecycle struct {
	components []Component
	started    []Component
}

func (lc *Lifecycle) Register(c Component) {
	lc.components = append(lc.components, c)
}

func (lc *Lifecycle) StartAll(ctx context.Context) error {
	for _, c := range lc.components {
		fmt.Printf("starting %s...\n", c.Name)
		if err := c.Start(ctx); err != nil {
			fmt.Printf("  failed to start %s: %v\n", c.Name, err)
			lc.StopAll(5 * time.Second) // rollback
			return err
		}
		lc.started = append(lc.started, c)
		fmt.Printf("  %s started\n", c.Name)
	}
	return nil
}

func (lc *Lifecycle) StopAll(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// Stop in REVERSE order
	for i := len(lc.started) - 1; i >= 0; i-- {
		c := lc.started[i]
		fmt.Printf("stopping %s...\n", c.Name)
		if err := c.Stop(ctx); err != nil {
			fmt.Printf("  error stopping %s: %v\n", c.Name, err)
		} else {
			fmt.Printf("  %s stopped\n", c.Name)
		}
	}
}

func main() {
	lc := &Lifecycle{}

	lc.Register(Component{
		Name:  "database",
		Start: func(ctx context.Context) error { time.Sleep(100 * time.Millisecond); return nil },
		Stop:  func(ctx context.Context) error { fmt.Println("  closing DB connections"); return nil },
	})
	lc.Register(Component{
		Name:  "cache",
		Start: func(ctx context.Context) error { return nil },
		Stop:  func(ctx context.Context) error { fmt.Println("  flushing cache"); return nil },
	})
	lc.Register(Component{
		Name:  "http-server",
		Start: func(ctx context.Context) error { return nil },
		Stop:  func(ctx context.Context) error { fmt.Println("  draining requests"); return nil },
	})

	ctx := context.Background()
	if err := lc.StartAll(ctx); err != nil {
		fmt.Println("startup failed:", err)
		return
	}

	fmt.Println("\n--- simulating shutdown ---\n")
	lc.StopAll(10 * time.Second)
}
```

---

## Problem 43: Circuit Breaker

**Concepts:** State machine (closed/open/half-open), failure counting, recovery

```go
package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type State int

const (
	Closed   State = iota // normal operation
	Open                  // blocking calls
	HalfOpen              // testing recovery
)

type CircuitBreaker struct {
	mu           sync.Mutex
	state        State
	failures     int
	maxFailures  int
	lastFailure  time.Time
	cooldown     time.Duration
	halfOpenMax  int
	halfOpenSucc int
}

func NewCircuitBreaker(maxFailures int, cooldown time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures: maxFailures,
		cooldown:    cooldown,
		halfOpenMax: 1,
	}
}

func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	state := cb.state
	if state == Open {
		if time.Since(cb.lastFailure) > cb.cooldown {
			cb.state = HalfOpen
			cb.halfOpenSucc = 0
			state = HalfOpen
			fmt.Println("  [circuit] Open -> HalfOpen (trying recovery)")
		} else {
			cb.mu.Unlock()
			return errors.New("circuit breaker is OPEN")
		}
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()
	if err != nil {
		cb.failures++
		cb.lastFailure = time.Now()
		if cb.failures >= cb.maxFailures {
			cb.state = Open
			fmt.Printf("  [circuit] -> Open (failures=%d)\n", cb.failures)
		}
		return err
	}

	if cb.state == HalfOpen {
		cb.halfOpenSucc++
		if cb.halfOpenSucc >= cb.halfOpenMax {
			cb.state = Closed
			cb.failures = 0
			fmt.Println("  [circuit] HalfOpen -> Closed (recovered!)")
		}
	} else {
		cb.failures = 0 // reset on success
	}
	return nil
}

func main() {
	cb := NewCircuitBreaker(3, 1*time.Second)

	callCount := 0
	unreliableService := func() error {
		callCount++
		if callCount <= 4 {
			return fmt.Errorf("service error #%d", callCount)
		}
		return nil // recovers after 4 failures
	}

	for i := 0; i < 10; i++ {
		err := cb.Execute(unreliableService)
		if err != nil {
			fmt.Printf("call %d: %v\n", i+1, err)
		} else {
			fmt.Printf("call %d: SUCCESS\n", i+1)
		}
		time.Sleep(300 * time.Millisecond)
	}
}
```

---

## Problem 44: Singleflight Cache — Thundering Herd Protection

**Concepts:** singleflight deduplication, cache stampede prevention, concurrent cache access

```go
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Singleflight: ensures only one call in-flight for a given key
type SingleFlight struct {
	mu    sync.Mutex
	calls map[string]*call
}

type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

func NewSingleFlight() *SingleFlight {
	return &SingleFlight{calls: make(map[string]*call)}
}

func (sf *SingleFlight) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	sf.mu.Lock()
	if c, ok := sf.calls[key]; ok {
		sf.mu.Unlock()
		c.wg.Wait() // wait for in-flight call
		return c.val, c.err
	}
	c := &call{}
	c.wg.Add(1)
	sf.calls[key] = c
	sf.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	sf.mu.Lock()
	delete(sf.calls, key)
	sf.mu.Unlock()

	return c.val, c.err
}

// Cache with singleflight protection
type Cache struct {
	mu    sync.RWMutex
	store map[string]string
	sf    *SingleFlight
}

func NewCache() *Cache {
	return &Cache{store: make(map[string]string), sf: NewSingleFlight()}
}

var dbCallCount atomic.Int32

func (c *Cache) Get(key string) string {
	c.mu.RLock()
	if val, ok := c.store[key]; ok {
		c.mu.RUnlock()
		return val
	}
	c.mu.RUnlock()

	// Cache miss: use singleflight to deduplicate concurrent fetches
	val, _ := c.sf.Do(key, func() (interface{}, error) {
		dbCallCount.Add(1)
		fmt.Printf("  fetching from DB for key=%s (call #%d)\n", key, dbCallCount.Load())
		time.Sleep(200 * time.Millisecond) // simulate slow DB query
		return "value-for-" + key, nil
	})

	result := val.(string)
	c.mu.Lock()
	c.store[key] = result
	c.mu.Unlock()
	return result
}

func main() {
	cache := NewCache()
	var wg sync.WaitGroup

	// 50 goroutines all requesting the same key simultaneously
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache.Get("popular-key")
		}()
	}
	wg.Wait()

	fmt.Printf("\n50 concurrent requests, only %d DB call(s) made (singleflight!)\n",
		dbCallCount.Load())
}
```

---

## Problem 45: Concurrent LRU Cache with TTL

**Concepts:** LRU eviction, TTL expiry, thread-safety, background cleanup

```go
package main

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type entry struct {
	key       string
	value     interface{}
	expiresAt time.Time
}

type LRUCache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	order    *list.List
	ttl      time.Duration
	stopCh   chan struct{}
}

func NewLRUCache(capacity int, ttl time.Duration) *LRUCache {
	c := &LRUCache{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		order:    list.New(),
		ttl:      ttl,
		stopCh:   make(chan struct{}),
	}
	go c.cleanupLoop()
	return c
}

func (c *LRUCache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		e := elem.Value.(*entry)
		if time.Now().After(e.expiresAt) {
			c.removeElement(elem)
			return nil, false
		}
		c.order.MoveToFront(elem)
		return e.value, true
	}
	return nil, false
}

func (c *LRUCache) Put(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.order.MoveToFront(elem)
		e := elem.Value.(*entry)
		e.value = value
		e.expiresAt = time.Now().Add(c.ttl)
		return
	}
	if c.order.Len() >= c.capacity {
		c.removeOldest()
	}
	e := &entry{key: key, value: value, expiresAt: time.Now().Add(c.ttl)}
	elem := c.order.PushFront(e)
	c.items[key] = elem
}

func (c *LRUCache) removeOldest() {
	if oldest := c.order.Back(); oldest != nil {
		c.removeElement(oldest)
	}
}

func (c *LRUCache) removeElement(elem *list.Element) {
	c.order.Remove(elem)
	delete(c.items, elem.Value.(*entry).key)
}

func (c *LRUCache) cleanupLoop() {
	ticker := time.NewTicker(c.ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			for c.order.Len() > 0 {
				oldest := c.order.Back()
				if oldest == nil {
					break
				}
				if now.After(oldest.Value.(*entry).expiresAt) {
					c.removeElement(oldest)
				} else {
					break
				}
			}
			c.mu.Unlock()
		case <-c.stopCh:
			return
		}
	}
}

func (c *LRUCache) Stop() { close(c.stopCh) }

func main() {
	cache := NewLRUCache(3, 500*time.Millisecond)
	defer cache.Stop()

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	v, ok := cache.Get("a")
	fmt.Printf("get(a): %v, found: %v\n", v, ok) // 1, true

	cache.Put("d", 4) // evicts "b" (LRU)
	_, ok = cache.Get("b")
	fmt.Printf("get(b) after eviction: found=%v\n", ok) // false

	// Wait for TTL to expire
	time.Sleep(600 * time.Millisecond)
	_, ok = cache.Get("a")
	fmt.Printf("get(a) after TTL: found=%v\n", ok) // false (expired)
}
```

---

## Problem 46: Concurrent Web Crawler

**Concepts:** BFS with concurrency, visited set (sync.Map), bounded concurrency, rate limiting

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type CrawlResult struct {
	URL   string
	Links []string
	Depth int
}

// Simulated web — returns links for a given URL
func fakeFetch(url string) []string {
	pages := map[string][]string{
		"https://example.com":       {"https://example.com/about", "https://example.com/blog"},
		"https://example.com/about": {"https://example.com", "https://example.com/team"},
		"https://example.com/blog":  {"https://example.com", "https://example.com/blog/post1"},
		"https://example.com/team":  {"https://example.com/about"},
		"https://example.com/blog/post1": {},
	}
	time.Sleep(50 * time.Millisecond) // simulate network
	return pages[url]
}

func crawl(ctx context.Context, seed string, maxDepth, maxConcurrency int) []CrawlResult {
	var (
		visited sync.Map
		results []CrawlResult
		mu      sync.Mutex
		wg      sync.WaitGroup
		sem     = make(chan struct{}, maxConcurrency)
		count   atomic.Int32
	)

	type task struct {
		url   string
		depth int
	}

	tasks := make(chan task, 1000)
	tasks <- task{url: seed, depth: 0}
	count.Add(1)

	for i := 0; i < maxConcurrency; i++ {
		go func() {
			for t := range tasks {
				if _, loaded := visited.LoadOrStore(t.url, true); loaded {
					count.Add(-1)
					if count.Load() == 0 {
						close(tasks)
					}
					continue
				}

				sem <- struct{}{} // acquire
				links := fakeFetch(t.url)
				<-sem // release

				mu.Lock()
				results = append(results, CrawlResult{URL: t.url, Links: links, Depth: t.depth})
				mu.Unlock()

				if t.depth < maxDepth {
					for _, link := range links {
						if _, exists := visited.Load(link); !exists {
							count.Add(1)
							select {
							case tasks <- task{url: link, depth: t.depth + 1}:
							default:
							}
						}
					}
				}

				if count.Add(-1) == 0 {
					close(tasks)
				}
			}
		}()
	}

	// Simple approach: wait for channel close
	wg.Wait()
	time.Sleep(500 * time.Millisecond) // let crawlers finish

	return results
}

func main() {
	ctx := context.Background()
	results := crawl(ctx, "https://example.com", 2, 3)

	fmt.Printf("crawled %d pages:\n", len(results))
	for _, r := range results {
		fmt.Printf("  [depth %d] %s -> %v\n", r.Depth, r.URL, r.Links)
	}
}
```

---

## Problem 47: Timeout Budget Propagation

**Concepts:** Deadline arithmetic, remaining budget, cascading timeouts

```go
package main

import (
	"context"
	"fmt"
	"time"
)

func serviceA(ctx context.Context) (string, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return "", fmt.Errorf("no deadline set")
	}

	// Step 1: local processing (200ms)
	time.Sleep(200 * time.Millisecond)

	// Step 2: call serviceB with remaining budget minus 50ms buffer
	remaining := time.Until(deadline)
	fmt.Printf("  serviceA: %v remaining, calling serviceB\n", remaining.Truncate(time.Millisecond))

	if remaining < 100*time.Millisecond {
		return "", fmt.Errorf("insufficient budget: %v", remaining)
	}

	childCtx, cancel := context.WithTimeout(ctx, remaining-50*time.Millisecond)
	defer cancel()

	return serviceB(childCtx)
}

func serviceB(ctx context.Context) (string, error) {
	remaining := time.Until(func() time.Time {
		d, _ := ctx.Deadline()
		return d
	}())
	fmt.Printf("  serviceB: %v remaining\n", remaining.Truncate(time.Millisecond))

	select {
	case <-time.After(100 * time.Millisecond): // simulated work
		return "result from B", nil
	case <-ctx.Done():
		return "", fmt.Errorf("serviceB: %w", ctx.Err())
	}
}

func main() {
	// Scenario 1: Enough budget
	fmt.Println("=== Scenario 1: 1s budget (should succeed) ===")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	result, err := serviceA(ctx)
	cancel()
	if err != nil {
		fmt.Println("failed:", err)
	} else {
		fmt.Println("success:", result)
	}

	// Scenario 2: Tight budget
	fmt.Println("\n=== Scenario 2: 250ms budget (might fail) ===")
	ctx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
	result, err = serviceA(ctx)
	cancel()
	if err != nil {
		fmt.Println("failed:", err)
	} else {
		fmt.Println("success:", result)
	}
}
```

---

## Problem 48: Goroutine Leak Detector

**Concepts:** runtime.NumGoroutine(), testing for leaks, goroutine lifecycle verification

```go
package main

import (
	"context"
	"fmt"
	"runtime"
	"time"
)

// LeakChecker monitors goroutine count
type LeakChecker struct {
	baseline int
}

func NewLeakChecker() *LeakChecker {
	runtime.GC() // clean up before measuring
	time.Sleep(50 * time.Millisecond)
	return &LeakChecker{baseline: runtime.NumGoroutine()}
}

func (lc *LeakChecker) Check() (int, bool) {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	current := runtime.NumGoroutine()
	leaked := current - lc.baseline
	return leaked, leaked > 0
}

// LEAKY function — goroutine blocks forever
func leakyFunction() {
	ch := make(chan int)
	go func() {
		ch <- 42 // nobody reads from ch — goroutine leaks!
	}()
	// ch is abandoned, goroutine blocks forever
}

// FIXED function — uses context for cancellation
func fixedFunction() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	ch := make(chan int, 1) // buffered: goroutine can write without blocking
	go func() {
		select {
		case ch <- 42:
		case <-ctx.Done():
			return // properly exits
		}
	}()
}

func main() {
	// Test leaky function
	checker := NewLeakChecker()
	for i := 0; i < 10; i++ {
		leakyFunction()
	}
	leaked, hasLeak := checker.Check()
	fmt.Printf("leaky: %d goroutines leaked, hasLeak=%v\n", leaked, hasLeak)

	// Test fixed function
	checker2 := NewLeakChecker()
	for i := 0; i < 10; i++ {
		fixedFunction()
	}
	time.Sleep(200 * time.Millisecond) // wait for timeouts
	leaked2, hasLeak2 := checker2.Check()
	fmt.Printf("fixed: %d goroutines leaked, hasLeak=%v\n", leaked2, hasLeak2)
}
```

---

## Problem 49: Deadlock-Free Bank Transfer with Timeout

**Concepts:** Lock ordering, TryLock pattern with timeout, deadlock prevention

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

type BankAccount struct {
	id      int
	mu      sync.Mutex
	balance int
}

// tryLockWithTimeout attempts to acquire the lock within a deadline
func tryLockWithTimeout(mu *sync.Mutex, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		mu.Lock()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func safeTransfer(from, to *BankAccount, amount int) error {
	// Lock ordering: always lock lower ID first
	first, second := from, to
	if from.id > to.id {
		first, second = to, from
	}

	first.mu.Lock()
	defer first.mu.Unlock()
	second.mu.Lock()
	defer second.mu.Unlock()

	if from.balance < amount {
		return fmt.Errorf("insufficient funds: have %d, need %d", from.balance, amount)
	}
	from.balance -= amount
	to.balance += amount
	return nil
}

func main() {
	accounts := []*BankAccount{
		{id: 1, balance: 1000},
		{id: 2, balance: 1000},
		{id: 3, balance: 1000},
	}

	var wg sync.WaitGroup
	// Concurrent transfers in all directions — no deadlock due to ordering
	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); safeTransfer(accounts[0], accounts[1], 1) }()
		go func() { defer wg.Done(); safeTransfer(accounts[1], accounts[2], 1) }()
		go func() { defer wg.Done(); safeTransfer(accounts[2], accounts[0], 1) }()
	}
	wg.Wait()

	total := 0
	for _, a := range accounts {
		fmt.Printf("account %d: $%d\n", a.id, a.balance)
		total += a.balance
	}
	fmt.Printf("total: $%d (expected $3000 — money is conserved)\n", total)
}
```

---

## Problem 50: High-Throughput Sharded Counter

**Concepts:** Lock contention, sharding, false sharing, cache line padding, atomic operations

```go
package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Naive: single atomic — cache line bouncing under contention
type NaiveCounter struct {
	val atomic.Int64
}

func (c *NaiveCounter) Inc() { c.val.Add(1) }
func (c *NaiveCounter) Val() int64 { return c.val.Load() }

// Optimized: sharded counter with cache line padding
const cacheLineSize = 64

type paddedCounter struct {
	val atomic.Int64
	_   [cacheLineSize - int(unsafe.Sizeof(atomic.Int64{}))]byte // pad to full cache line
}

type ShardedCounter struct {
	shards []paddedCounter
	mask   uint64
}

func NewShardedCounter(shards int) *ShardedCounter {
	// Round up to power of 2 for fast modulo via bitmask
	n := 1
	for n < shards {
		n *= 2
	}
	return &ShardedCounter{
		shards: make([]paddedCounter, n),
		mask:   uint64(n - 1),
	}
}

// Fast hash to pick a shard — use goroutine-local hint
var shardHint atomic.Uint64

func (c *ShardedCounter) Inc() {
	// Use a simple incrementing hint to distribute across shards
	idx := shardHint.Add(1) & c.mask
	c.shards[idx].val.Add(1)
}

func (c *ShardedCounter) Val() int64 {
	var total int64
	for i := range c.shards {
		total += c.shards[i].val.Load()
	}
	return total
}

func benchmark(name string, inc func(), val func() int64) {
	numGoroutines := runtime.GOMAXPROCS(0) * 4
	opsPerGoroutine := 1_000_000

	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				inc()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	totalOps := numGoroutines * opsPerGoroutine
	fmt.Printf("%s: %d ops in %v (%.1f M ops/sec), value=%d\n",
		name, totalOps, elapsed, float64(totalOps)/elapsed.Seconds()/1e6, val())
}

func main() {
	fmt.Printf("GOMAXPROCS=%d\n\n", runtime.GOMAXPROCS(0))

	naive := &NaiveCounter{}
	benchmark("naive-atomic", naive.Inc, naive.Val)

	sharded := NewShardedCounter(64)
	benchmark("sharded-64  ", sharded.Inc, sharded.Val)

	// Sharded counter should be significantly faster under high contention
}
```

**Key Takeaway:** Under high contention (many cores), a single atomic counter causes cache-line bouncing. Sharding distributes updates across different cache lines, dramatically improving throughput. This is the same technique used by Java's `LongAdder` and Go's runtime for internal counters.

---

# CONCEPT COVERAGE VERIFICATION

Every major Go concurrency concept appears in at least one problem:

| Concept | Problems |
|---------|----------|
| Goroutines | 1, 2, 3 |
| sync.WaitGroup | 1, 11, 21, 25, 27 |
| Closure variable capture | 2 |
| Panic recovery in goroutines | 3 |
| Unbuffered channels | 4, 10 |
| Buffered channels | 5, 28 |
| Channel directions | 5, 6 |
| Range over channel | 5, 9, 27 |
| Generator pattern | 6 |
| Pipeline pattern | 6, 23, 24 |
| Select statement | 7, 8, 9 |
| Select with timeout | 8, 30 |
| Nil channel in select | 9 |
| Done channel pattern | 10 |
| sync.Mutex | 11, 12, 49 |
| Lock ordering (deadlock prevention) | 12, 49 |
| sync.RWMutex | 13, 14 |
| Double-checked locking | 14 |
| sync/atomic (Add, Load, Store) | 15, 50 |
| Compare-and-Swap (CAS loop) | 15 |
| atomic.Pointer | 16 |
| sync.Once | 17 |
| RetryableOnce (custom) | 17 |
| sync.Cond | 18 |
| sync.Pool | 19 |
| sync.Map | 20 |
| Worker pool | 21 |
| errgroup (bounded concurrency) | 22, 31 |
| Pipeline with error handling | 24 |
| Fan-out | 25, 26 |
| Fan-in | 25 |
| Order-preserving parallel processing | 26 |
| Multiple producers, multiple consumers | 27 |
| Batch consumer (size + time trigger) | 28 |
| context.WithCancel | 29 |
| context.WithTimeout | 30, 47 |
| context.WithValue | 40 |
| Error aggregation (errors.Join) | 32 |
| Semaphore (channel-based) | 33 |
| Token bucket rate limiter | 34 |
| Per-key rate limiter | 35 |
| Pub-Sub event bus | 36 |
| Slow subscriber strategies | 37 |
| Future/Promise | 38 |
| WhenAll / WhenAny | 39 |
| Graceful shutdown | 41 |
| Component lifecycle management | 42 |
| Circuit breaker | 43 |
| Singleflight (thundering herd) | 44 |
| LRU cache with TTL | 45 |
| Concurrent web crawler | 46 |
| Timeout budget propagation | 47 |
| Goroutine leak detection | 48 |
| Deadlock prevention | 49 |
| High-throughput sharded counter | 50 |
| False sharing / cache line padding | 50 |
