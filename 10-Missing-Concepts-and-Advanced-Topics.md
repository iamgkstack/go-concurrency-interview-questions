# Part 10: Missing Concepts & Advanced Topics

> This file fills every gap identified in Parts 1-9. Every section has **complete, runnable code** and explains exactly what interviewers look for.

---

## Table of Contents

1. [Testing Concurrent Code](#1-testing-concurrent-code)
2. [Benchmarking Concurrent Code](#2-benchmarking-concurrent-code)
3. [Retry with Exponential Backoff + Jitter](#3-retry-with-exponential-backoff--jitter)
4. [Channel vs Mutex — Decision Framework](#4-channel-vs-mutex--decision-framework)
5. [reflect.Select — Dynamic Fan-In](#5-reflectselect--dynamic-fan-in)
6. [net/http Concurrency Model](#6-nethttp-concurrency-model)
7. [Ticker Lifecycle Management](#7-ticker-lifecycle-management)
8. [Barrier Synchronization](#8-barrier-synchronization)
9. [Immutable Data Pattern](#9-immutable-data-pattern)
10. [runtime/trace — Step-by-Step Walkthrough](#10-runtimetrace--step-by-step-walkthrough)
11. [GODEBUG Environment Variables](#11-godebug-environment-variables)
12. [Lock-Free Stack (Treiber Stack)](#12-lock-free-stack-treiber-stack)
13. [singleflight Package — Real Usage](#13-singleflight-package--real-usage)
14. [errgroup Advanced Patterns](#14-errgroup-advanced-patterns)
15. [Distributed Tracing Context Propagation](#15-distributed-tracing-context-propagation)

---

# 1. Testing Concurrent Code

> **Interview frequency: VERY HIGH.** Every interviewer asks "how would you test this?" after you write concurrent code. This section covers every testing technique.

---

## 1.1 The Race Detector — `go test -race`

The race detector is your first line of defense. It uses ThreadSanitizer (TSan) to detect data races at runtime.

```go
// counter_test.go
package counter

import (
	"sync"
	"testing"
)

// BROKEN: This will fail with -race
type UnsafeCounter struct {
	n int
}

func (c *UnsafeCounter) Inc() { c.n++ }
func (c *UnsafeCounter) Val() int { return c.n }

func TestUnsafeCounter_Race(t *testing.T) {
	c := &UnsafeCounter{}
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Inc() // DATA RACE: concurrent read+write without synchronization
		}()
	}
	wg.Wait()
	// Run: go test -race -run TestUnsafeCounter_Race
	// Output: WARNING: DATA RACE
}

// FIXED: Thread-safe counter
type SafeCounter struct {
	mu sync.Mutex
	n  int
}

func (c *SafeCounter) Inc() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

func (c *SafeCounter) Val() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func TestSafeCounter_NoRace(t *testing.T) {
	c := &SafeCounter{}
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Inc()
		}()
	}
	wg.Wait()
	if c.Val() != 1000 {
		t.Errorf("expected 1000, got %d", c.Val())
	}
	// Run: go test -race -run TestSafeCounter_NoRace
	// Output: PASS
}
```

**Key facts about `-race`:**
- Adds 2-20x CPU overhead, 5-10x memory overhead
- Only catches races that actually execute (dynamic analysis, not static)
- Always run CI tests with `-race`
- Race detector is enabled per-binary, not per-test
- Exits with code 66 on race detection

---

## 1.2 `testing.T.Parallel()` — Concurrent Test Execution

```go
// parallel_test.go
package mypackage

import (
	"sync"
	"testing"
	"time"
)

// Tests marked with t.Parallel() run concurrently with each other
func TestFeatureA(t *testing.T) {
	t.Parallel() // mark as parallel
	time.Sleep(100 * time.Millisecond)
	// test logic
}

func TestFeatureB(t *testing.T) {
	t.Parallel()
	time.Sleep(100 * time.Millisecond)
	// test logic — runs concurrently with TestFeatureA
}

// Table-driven concurrent tests — the gold standard
func TestConcurrentOperations(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{"double_1", 1, 2},
		{"double_5", 5, 10},
		{"double_100", 100, 200},
	}

	for _, tt := range tests {
		tt := tt // capture range variable (Go <1.22)
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel() // each subtest runs concurrently
			result := tt.input * 2
			if result != tt.expected {
				t.Errorf("got %d, want %d", result, tt.expected)
			}
		})
	}
}

// Testing concurrent access patterns
func TestMapConcurrentAccess(t *testing.T) {
	t.Parallel()

	m := &sync.Map{}
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			m.Store(v, v*2)
		}(i)
	}

	// Concurrent reads while writes are happening
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			m.Load(v)
		}(i)
	}

	wg.Wait()
}
```

---

## 1.3 Goroutine Leak Detection in Tests with `goleak`

```go
// leak_test.go
package mypackage

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

// Option 1: Check every test in the package
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// Option 2: Check individual tests
func TestNoGoroutineLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	ch := make(chan int, 1)
	go func() {
		ch <- 42 // buffered: goroutine won't block
	}()
	<-ch
	// goleak verifies no goroutines are still running after test
}

// This test would FAIL goleak because the goroutine leaks
func TestGoroutineLeak_BAD(t *testing.T) {
	// defer goleak.VerifyNone(t) // uncomment to see it fail

	ch := make(chan int) // unbuffered
	go func() {
		ch <- 42 // blocks forever — nobody reads
	}()
	// goroutine leaked!
}
```

**Install:** `go get go.uber.org/goleak`

---

## 1.4 Test Helpers for Concurrent Code

```go
// helpers_test.go
package mypackage

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Helper: assert that a function completes within a timeout
func assertCompletes(t *testing.T, timeout time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
		// success
	case <-time.After(timeout):
		t.Fatal("timed out — possible deadlock")
	}
}

// Helper: assert that a channel receives a value within timeout
func assertReceives[T any](t *testing.T, ch <-chan T, timeout time.Duration) T {
	t.Helper()
	select {
	case val := <-ch:
		return val
	case <-time.After(timeout):
		t.Fatal("channel did not receive within timeout")
		var zero T
		return zero
	}
}

// Helper: assert channel is closed (drains to zero value)
func assertClosed[T any](t *testing.T, ch <-chan T, timeout time.Duration) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed, but received a value")
		}
	case <-time.After(timeout):
		t.Fatal("channel was not closed within timeout")
	}
}

// Helper: run a function concurrently N times and wait
func concurrentRun(n int, fn func(id int)) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fn(id)
		}(i)
	}
	wg.Wait()
}

// Usage in tests
func TestWorkerPool_Completes(t *testing.T) {
	assertCompletes(t, 5*time.Second, func() {
		results := make(chan int, 10)
		go func() {
			for i := 0; i < 10; i++ {
				results <- i
			}
			close(results)
		}()
		for range results {
		}
	})
}

func TestConcurrentAccess_NoRace(t *testing.T) {
	counter := 0
	var mu sync.Mutex

	concurrentRun(100, func(id int) {
		mu.Lock()
		counter++
		mu.Unlock()
	})

	mu.Lock()
	if counter != 100 {
		t.Errorf("expected 100, got %d", counter)
	}
	mu.Unlock()
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(ch)
	}()

	cancel()
	assertClosed(t, ch, 1*time.Second)
}
```

---

## 1.5 Stress Testing Concurrent Code

```go
// stress_test.go
package mypackage

import (
	"sync"
	"sync/atomic"
	"testing"
)

// Stress test: run hundreds of goroutines hitting the same data structure
// Run with: go test -race -count=100 -run TestStress
func TestStress_ConcurrentMap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	m := &sync.Map{}
	var ops atomic.Int64
	var wg sync.WaitGroup

	// 50 writers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				m.Store(id*1000+j, j)
				ops.Add(1)
			}
		}(i)
	}

	// 50 readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				m.Load(id*1000 + j)
				ops.Add(1)
			}
		}(i)
	}

	// 10 deleters
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				m.Delete(id*1000 + j)
				ops.Add(1)
			}
		}(i)
	}

	wg.Wait()
	t.Logf("completed %d operations without race", ops.Load())
}
```

**Interview tip:** When an interviewer asks "how would you test this?", answer:
1. Unit tests with `-race` flag
2. Table-driven subtests with `t.Parallel()`
3. Stress tests with high goroutine counts
4. `goleak` to verify no goroutine leaks
5. Timeout assertions to catch deadlocks

---

# 2. Benchmarking Concurrent Code

> **Interview frequency: HIGH at Staff+ level.** Interviewers expect you to know how to measure concurrent performance and identify contention.

---

## 2.1 `b.RunParallel` — Benchmark Under Contention

```go
// bench_test.go
package bench

import (
	"sync"
	"sync/atomic"
	"testing"
)

// Mutex-protected counter
type MutexCounter struct {
	mu sync.Mutex
	n  int64
}

func (c *MutexCounter) Inc() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

// RWMutex counter (write-heavy = worse than Mutex)
type RWMutexCounter struct {
	mu sync.RWMutex
	n  int64
}

func (c *RWMutexCounter) Inc() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

func (c *RWMutexCounter) Val() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.n
}

// Atomic counter
type AtomicCounter struct {
	n atomic.Int64
}

func (c *AtomicCounter) Inc() { c.n.Add(1) }

// Channel counter
type ChannelCounter struct {
	ch chan struct{}
}

func NewChannelCounter() *ChannelCounter {
	c := &ChannelCounter{ch: make(chan struct{}, 1)}
	c.ch <- struct{}{} // initial token
	return c
}

func (c *ChannelCounter) Inc(n *int64) {
	<-c.ch   // acquire
	*n++
	c.ch <- struct{}{} // release
}

// --- BENCHMARKS ---

// Single-goroutine baseline
func BenchmarkMutexCounter_Single(b *testing.B) {
	c := &MutexCounter{}
	for i := 0; i < b.N; i++ {
		c.Inc()
	}
}

func BenchmarkAtomicCounter_Single(b *testing.B) {
	c := &AtomicCounter{}
	for i := 0; i < b.N; i++ {
		c.Inc()
	}
}

// Multi-goroutine: THIS IS WHERE CONTENTION SHOWS
func BenchmarkMutexCounter_Parallel(b *testing.B) {
	c := &MutexCounter{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}

func BenchmarkAtomicCounter_Parallel(b *testing.B) {
	c := &AtomicCounter{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}

func BenchmarkRWMutexCounter_ReadHeavy(b *testing.B) {
	c := &RWMutexCounter{}
	c.n = 42
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Val() // read-heavy: RWMutex shines here
		}
	})
}

// Run: go test -bench=. -benchmem -cpu=1,4,8,16
// Expected results (approximate):
//
// BenchmarkMutexCounter_Single        ~20ns/op
// BenchmarkAtomicCounter_Single       ~5ns/op
// BenchmarkMutexCounter_Parallel-8    ~200ns/op (contention!)
// BenchmarkAtomicCounter_Parallel-8   ~50ns/op
// BenchmarkRWMutexCounter_ReadHeavy-8 ~30ns/op
```

---

## 2.2 Benchmarking with `-cpu` Flag and Contention Analysis

```
# Run benchmarks at different GOMAXPROCS values
go test -bench=. -benchmem -cpu=1,2,4,8,16

# Compare two implementations
go test -bench=BenchmarkMutex -count=10 > old.txt
go test -bench=BenchmarkAtomic -count=10 > new.txt
# go install golang.org/x/perf/cmd/benchstat@latest
benchstat old.txt new.txt
```

## 2.3 Benchmark Memory Allocations

```go
func BenchmarkPoolVsNew(b *testing.B) {
	pool := sync.Pool{New: func() interface{} { return make([]byte, 1024) }}

	b.Run("sync.Pool", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := pool.Get().([]byte)
				buf[0] = 1 // use it
				pool.Put(buf)
			}
		})
	})

	b.Run("make-new", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := make([]byte, 1024)
				buf[0] = 1
				_ = buf
			}
		})
	})
}
// sync.Pool: ~0 allocs/op under steady state
// make-new: 1 alloc/op always
```

---

## 2.4 Profiling Lock Contention

```go
// main.go — run then profile
package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" // import for side effects
	"runtime"
	"sync"
)

func main() {
	// Enable mutex profiling
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)

	// Start pprof server
	go func() {
		http.ListenAndServe(":6060", nil)
	}()

	// Hot mutex
	var mu sync.Mutex
	counter := 0
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100000; j++ {
				mu.Lock()
				counter++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	fmt.Println("done, counter:", counter)
}

// Then profile:
// go tool pprof http://localhost:6060/debug/pprof/mutex
// (pprof) top       — shows which mutexes have most contention
// (pprof) list main — shows source lines with contention
//
// go tool pprof http://localhost:6060/debug/pprof/block
// (pprof) top       — shows where goroutines block most
```

**Interview tip:** When asked "your service is slow under load", the answer is:
1. Enable mutex + block profiling
2. Look at `pprof` mutex profile for contention hot spots
3. Consider sharding, atomics, or reducing critical section size
4. Benchmark before and after with `b.RunParallel`

---

# 3. Retry with Exponential Backoff + Jitter

> **Interview frequency: VERY HIGH.** This is asked in almost every production Go interview. It combines concurrency (context, goroutines) with resilience.

---

## 3.1 Production-Grade Implementation

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// RetryConfig holds retry parameters
type RetryConfig struct {
	MaxRetries  int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64       // exponential growth factor
	JitterRange float64       // 0.0 to 1.0 — fraction of delay to randomize
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:  5,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    30 * time.Second,
		Multiplier:  2.0,
		JitterRange: 0.3, // +/- 30% jitter
	}
}

// RetryableError signals that the operation can be retried
type RetryableError struct {
	Err       error
	RetryAfter time.Duration // optional: server-specified retry delay
}

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// IsRetryable checks if an error should be retried
func IsRetryable(err error) bool {
	var re *RetryableError
	return errors.As(err, &re)
}

// Retry executes fn with exponential backoff + jitter
func Retry(ctx context.Context, cfg RetryConfig, fn func(ctx context.Context) error) error {
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		lastErr = fn(ctx)
		if lastErr == nil {
			return nil // success
		}

		// Check if error is retryable
		if !IsRetryable(lastErr) {
			return lastErr // permanent error — don't retry
		}

		// Last attempt — don't sleep
		if attempt == cfg.MaxRetries {
			break
		}

		// Calculate delay with exponential backoff
		delay := cfg.BaseDelay * time.Duration(math.Pow(cfg.Multiplier, float64(attempt)))

		// Check for server-specified retry delay
		var re *RetryableError
		if errors.As(lastErr, &re) && re.RetryAfter > 0 {
			delay = re.RetryAfter
		}

		// Cap at max delay
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}

		// Add jitter: delay * (1 +/- jitterRange)
		jitter := 1.0 + cfg.JitterRange*(2*rand.Float64()-1)
		delay = time.Duration(float64(delay) * jitter)

		fmt.Printf("  attempt %d failed: %v, retrying in %v\n", attempt+1, lastErr, delay)

		// Wait with context awareness
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
			// continue to next attempt
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("retry cancelled: %w", ctx.Err())
		}
	}

	return fmt.Errorf("all %d retries exhausted: %w", cfg.MaxRetries+1, lastErr)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	callCount := 0
	err := Retry(ctx, DefaultRetryConfig(), func(ctx context.Context) error {
		callCount++
		if callCount < 4 {
			return &RetryableError{Err: fmt.Errorf("service unavailable (attempt %d)", callCount)}
		}
		fmt.Printf("  attempt %d: SUCCESS\n", callCount)
		return nil
	})

	if err != nil {
		fmt.Println("failed:", err)
	} else {
		fmt.Println("succeeded after", callCount, "attempts")
	}

	// Demonstrate non-retryable error
	fmt.Println("\n--- non-retryable error ---")
	err = Retry(ctx, DefaultRetryConfig(), func(ctx context.Context) error {
		return fmt.Errorf("permission denied") // not wrapped in RetryableError
	})
	fmt.Println("result:", err) // fails immediately, no retries
}
```

---

## 3.2 Why Jitter Matters

```
WITHOUT JITTER (thundering herd):
  Client A retries at: 100ms, 200ms, 400ms, 800ms
  Client B retries at: 100ms, 200ms, 400ms, 800ms
  Client C retries at: 100ms, 200ms, 400ms, 800ms
  → All 3 clients hit the server at the SAME time, every time

WITH JITTER (spread out):
  Client A retries at: 87ms, 215ms, 380ms, 720ms
  Client B retries at: 112ms, 189ms, 430ms, 850ms
  Client C retries at: 95ms, 220ms, 410ms, 780ms
  → Load is distributed across time — server can recover
```

**Three jitter strategies:**
- **Full jitter:** `delay = random(0, baseDelay * 2^attempt)` — best for most cases
- **Equal jitter:** `delay = baseDelay * 2^attempt / 2 + random(0, baseDelay * 2^attempt / 2)`
- **Decorrelated jitter:** `delay = random(baseDelay, lastDelay * 3)` — AWS recommendation

---

# 4. Channel vs Mutex — Decision Framework

> **Interview frequency: HIGH.** This is a conceptual question that separates seniors from juniors.

---

## 4.1 The Decision Tree

```
START: "I need to protect shared state or coordinate goroutines"
  │
  ├── Is the shared state a simple counter or flag?
  │     YES → Use sync/atomic
  │     NO  ↓
  │
  ├── Are you TRANSFERRING OWNERSHIP of data between goroutines?
  │     YES → Use channels
  │     │     (producer-consumer, pipeline, results, jobs)
  │     NO  ↓
  │
  ├── Are you PROTECTING internal state of a struct?
  │     YES → Use Mutex/RWMutex
  │     │     (cache, map, counter, config)
  │     NO  ↓
  │
  ├── Are you SIGNALING or COORDINATING lifecycle events?
  │     YES → Use channels (done/quit channel) or context
  │     NO  ↓
  │
  ├── Do you need to BROADCAST to multiple goroutines?
  │     YES → Use close(channel) or sync.Cond.Broadcast()
  │     NO  ↓
  │
  └── Do you need bounded concurrency?
        YES → Channel-based semaphore or worker pool
        NO  → Reconsider your design
```

## 4.2 The Go Proverb, Explained

> "Share memory by communicating, don't communicate by sharing memory."

This does NOT mean "always use channels." It means:

```go
// COMMUNICATING BY SHARING MEMORY (often fine for internal state)
type Cache struct {
    mu    sync.RWMutex
    store map[string]string  // shared memory, protected by mutex
}

func (c *Cache) Get(key string) string {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return c.store[key]
}

// SHARING MEMORY BY COMMUNICATING (better for data flow)
func pipeline(in <-chan Data) <-chan Result {
    out := make(chan Result)
    go func() {
        defer close(out)
        for data := range in {
            out <- process(data)  // ownership transfers via channel
        }
    }()
    return out
}
```

## 4.3 Quick Reference Table

| Scenario | Use | Why |
|----------|-----|-----|
| Protecting a map/cache | `sync.Mutex` / `sync.RWMutex` | Internal state, no ownership transfer |
| Simple counter | `sync/atomic` | Lowest overhead |
| Worker pool job queue | `chan Job` | Transferring ownership of work |
| Pipeline stages | `chan T` | Data flows through stages |
| Configuration updates | `atomic.Pointer[Config]` or `sync.RWMutex` | Read-heavy, rare writes |
| Shutdown signaling | `chan struct{}` + `close()` | Broadcast to all listeners |
| Request cancellation | `context.Context` | Carries deadline + cancel through call chain |
| Rate limiting | Channel or `time.Ticker` | Natural token bucket |
| Bounded concurrency | `chan struct{}` (semaphore) | Channel-as-semaphore |
| One-time init | `sync.Once` | Guaranteed single execution |
| Object reuse | `sync.Pool` | Reduce GC pressure |
| Wait for N goroutines | `sync.WaitGroup` | Simple counter with Wait |
| Complex condition wait | `sync.Cond` | Wait for arbitrary condition |

## 4.4 Anti-Patterns

```go
// ANTI-PATTERN 1: Channel as mutex (needlessly complex)
// BAD:
lock := make(chan struct{}, 1)
lock <- struct{}{} // "lock"
// critical section
<-lock // "unlock"
// GOOD: Just use sync.Mutex

// ANTI-PATTERN 2: Mutex for data transfer (fights the language)
// BAD:
var mu sync.Mutex
var result string
go func() {
    mu.Lock()
    result = "done" // writing shared memory
    mu.Unlock()
}()
// How do I know when to read result? Polling?
// GOOD: Use a channel to transfer the result

// ANTI-PATTERN 3: Channel for protecting struct internals
// BAD:
type BadCache struct {
    ops chan func() // serializing via channel
}
// GOOD: Just use a mutex. Channels add latency and complexity.
```

---

# 5. reflect.Select — Dynamic Fan-In

> **Interview frequency: MEDIUM.** Asked when designing frameworks or handling dynamic channel sets.

---

## 5.1 The Problem

`select` in Go requires knowing the cases at compile time. What if you have a slice of N channels (N determined at runtime)?

```go
package main

import (
	"fmt"
	"reflect"
	"time"
)

func dynamicFanIn(channels []<-chan string) <-chan string {
	out := make(chan string)

	go func() {
		defer close(out)

		// Build reflect.SelectCase slice
		cases := make([]reflect.SelectCase, len(channels))
		for i, ch := range channels {
			cases[i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			}
		}

		// Process until all channels are closed
		remaining := len(cases)
		for remaining > 0 {
			chosen, value, ok := reflect.Select(cases)
			if !ok {
				// Channel closed — disable it by setting to zero Value
				cases[chosen].Chan = reflect.ValueOf(nil)
				remaining--
				continue
			}
			out <- value.String()
		}
	}()

	return out
}

func makeSource(name string, count int, delay time.Duration) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		for i := 0; i < count; i++ {
			ch <- fmt.Sprintf("%s-%d", name, i)
			time.Sleep(delay)
		}
	}()
	return ch
}

func main() {
	// Create N channels at runtime
	sources := []<-chan string{
		makeSource("alpha", 3, 100*time.Millisecond),
		makeSource("beta", 2, 150*time.Millisecond),
		makeSource("gamma", 4, 80*time.Millisecond),
	}

	merged := dynamicFanIn(sources)
	for msg := range merged {
		fmt.Println("received:", msg)
	}
	fmt.Println("all sources drained")
}
```

**Key Takeaway:**
- `reflect.Select` is ~10x slower than native `select` (~300ns vs ~30ns)
- Use it only when the number of channels is truly dynamic
- For fixed N, use goroutine-per-channel fan-in (see Part 9 Problem 25)

---

## 5.2 When to Use Each Approach

| Approach | When | Performance |
|----------|------|-------------|
| Native `select` | Fixed, known channels (2-5 cases) | Fastest (~30ns) |
| Goroutine-per-channel fan-in | Known at init time, any N | Good (~100ns + goroutine cost) |
| `reflect.Select` | Truly dynamic, channels added/removed at runtime | Slowest (~300ns) |

---

# 6. net/http Concurrency Model

> **Interview frequency: HIGH.** Understanding how Go's HTTP server handles concurrency is essential for backend roles.

---

## 6.1 How Go HTTP Server Works Internally

```
                    ┌──────────────────────────┐
                    │      net.Listener         │
                    │    (TCP accept loop)      │
                    └──────────┬───────────────┘
                               │
                    For each connection:
                    go srv.serve(conn)
                               │
                    ┌──────────▼───────────────┐
                    │   conn.serve()            │
                    │   (one goroutine per conn)│
                    │                           │
                    │   for {                   │
                    │     read request          │
                    │     find handler (mux)    │
                    │     handler.ServeHTTP()   │
                    │     write response        │
                    │   }                       │
                    │   // keep-alive loop      │
                    └───────────────────────────┘
```

**Key facts:**
1. One goroutine per connection (not per request, due to keep-alive)
2. Within a connection, requests are handled sequentially
3. HTTP/2 multiplexing: multiple streams per connection, but Go handles this internally
4. No goroutine pool — new goroutine per `Accept()`
5. Default: no limit on concurrent connections (must add yourself)

## 6.2 Concurrency-Safe HTTP Handlers

```go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
)

// DANGER: Handler state is shared across goroutines
type CounterHandler struct {
	mu    sync.Mutex
	count int64
}

// CORRECT: Protect shared state
func (h *CounterHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.count++
	current := h.count
	h.mu.Unlock()
	fmt.Fprintf(w, "count: %d\n", current)
}

// BETTER: Use atomics for simple counters
type AtomicHandler struct {
	count atomic.Int64
}

func (h *AtomicHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	current := h.count.Add(1)
	fmt.Fprintf(w, "count: %d\n", current)
}

// PATTERN: Limiting concurrent requests
func maxConcurrencyMiddleware(next http.Handler, max int) http.Handler {
	sem := make(chan struct{}, max)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			next.ServeHTTP(w, r)
		default:
			http.Error(w, "too many requests", http.StatusServiceUnavailable)
		}
	})
}

// PATTERN: Request-scoped goroutines respect context
func handleSearch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context() // already has deadline from client

	results := make(chan string, 3)
	go func() {
		// This goroutine MUST respect ctx
		select {
		case results <- searchDatabase(ctx):
		case <-ctx.Done():
			return // client disconnected
		}
	}()

	select {
	case result := <-results:
		json.NewEncoder(w).Encode(result)
	case <-ctx.Done():
		// Client gave up — don't waste resources
		return
	}
}

func searchDatabase(ctx context.Context) string {
	return "result" // placeholder
}

func main() {
	mux := http.NewServeMux()
	mux.Handle("/count", &AtomicHandler{})
	mux.HandleFunc("/search", handleSearch)

	// Wrap with concurrency limit
	limited := maxConcurrencyMiddleware(mux, 100) // max 100 concurrent

	fmt.Println("server on :8080")
	http.ListenAndServe(":8080", limited)
}
```

---

## 6.3 Goroutine-per-Connection Trade-offs

| Aspect | Goroutine-per-Conn (Go) | Event Loop (Node/Nginx) |
|--------|------------------------|-------------------------|
| Model | M:N threading, 1 goroutine per conn | Single-threaded event loop |
| Memory per conn | 2-8 KB (goroutine stack) | ~1-2 KB (state machine) |
| 10K connections | ~80 MB goroutine stacks | ~20 MB |
| 1M connections | ~8 GB (problematic) | ~2 GB (feasible) |
| Code style | Synchronous (blocking) | Callback/async |
| CPU-bound work | Naturally parallel (GOMAXPROCS) | Blocks event loop! |
| Complexity | Simple, linear code flow | Callback hell / state machines |
| Context switching | Go scheduler (fast, ~100ns) | OS-level or manual |

**When goroutine-per-connection becomes a problem:**
- 100K+ idle connections (WebSocket servers)
- Solution: `net/netpoll` or `gnet` (epoll-based, not goroutine-per-conn)

---

# 7. Ticker Lifecycle Management

> **Interview frequency: MEDIUM.** `time.Ticker` leaks are a common production bug.

---

```go
package main

import (
	"context"
	"fmt"
	"time"
)

// WRONG: Ticker is never stopped — goroutine + ticker leak
func leakyTicker() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			fmt.Println("tick") // runs forever
		}
	}()
	// ticker.Stop() never called!
	// The goroutine blocks on ticker.C forever = leak
}

// CORRECT: Ticker with proper lifecycle
func properTicker(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop() // CRITICAL: always stop the ticker

	for {
		select {
		case t := <-ticker.C:
			fmt.Println("tick at", t.Format("15:04:05"))
		case <-ctx.Done():
			fmt.Println("ticker stopped")
			return
		}
	}
}

// PATTERN: Ticker with Reset (Go 1.15+)
func adaptiveTicker(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	count := 0
	for {
		select {
		case <-ticker.C:
			count++
			fmt.Printf("tick %d\n", count)

			// Slow down after 5 ticks
			if count == 5 {
				ticker.Reset(3 * time.Second)
				fmt.Println("slowed down to 3s interval")
			}
		case <-ctx.Done():
			return
		}
	}
}

// PATTERN: Immediate first tick (ticker doesn't fire immediately)
func immediateFirstTick(ctx context.Context, interval time.Duration, fn func()) {
	fn() // execute immediately first time

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fn()
		case <-ctx.Done():
			return
		}
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	properTicker(ctx)
}
```

**Key facts:**
- `time.NewTicker` starts ticking immediately (but first tick arrives after the interval)
- `ticker.Stop()` does NOT close `ticker.C` — the channel stays open
- Always pair ticker creation with `defer ticker.Stop()`
- Use `ticker.Reset(d)` (Go 1.15+) to change the interval dynamically
- If you need an immediate first tick, call the function once before starting the ticker

---

# 8. Barrier Synchronization

> **Interview frequency: MEDIUM.** CyclicBarrier is common in Java interviews, and Go interviewers sometimes ask for an equivalent.

---

## 8.1 Using WaitGroup as a Barrier

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

// Phase barrier: all goroutines must complete phase N before any starts phase N+1
func main() {
	const workers = 5

	for phase := 1; phase <= 3; phase++ {
		var wg sync.WaitGroup
		wg.Add(workers)

		for id := 0; id < workers; id++ {
			go func(workerID, phaseNum int) {
				defer wg.Done()
				// Simulate work
				time.Sleep(time.Duration(workerID*50) * time.Millisecond)
				fmt.Printf("  worker %d completed phase %d\n", workerID, phaseNum)
			}(id, phase)
		}

		wg.Wait() // barrier: all workers must finish before next phase
		fmt.Printf("=== Phase %d complete ===\n", phase)
	}
}
```

---

## 8.2 Reusable CyclicBarrier

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

type CyclicBarrier struct {
	parties   int
	count     int
	mu        sync.Mutex
	cond      *sync.Cond
	generation int
}

func NewCyclicBarrier(parties int) *CyclicBarrier {
	b := &CyclicBarrier{parties: parties, count: parties}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *CyclicBarrier) Await() {
	b.mu.Lock()
	gen := b.generation
	b.count--
	if b.count == 0 {
		// Last one in — release everyone and reset
		b.count = b.parties
		b.generation++
		b.cond.Broadcast()
		b.mu.Unlock()
		return
	}
	// Wait for all others
	for gen == b.generation {
		b.cond.Wait()
	}
	b.mu.Unlock()
}

func main() {
	barrier := NewCyclicBarrier(4)

	for id := 0; id < 4; id++ {
		go func(workerID int) {
			for phase := 1; phase <= 3; phase++ {
				// Do work
				time.Sleep(time.Duration(workerID*30) * time.Millisecond)
				fmt.Printf("  worker %d done with phase %d\n", workerID, phase)

				barrier.Await() // wait for everyone before next phase

				if workerID == 0 {
					fmt.Printf("=== Phase %d barrier released ===\n", phase)
				}
				barrier.Await() // second barrier so worker 0's print completes
			}
		}(id)
	}

	time.Sleep(2 * time.Second) // wait for demo
}
```

---

# 9. Immutable Data Pattern

> **Interview frequency: MEDIUM.** Eliminates the need for locks entirely by making data immutable after creation.

---

```go
package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Immutable: once created, this struct is NEVER modified
type AppConfig struct {
	DBHost     string
	DBPort     int
	MaxConns   int
	FeatureFlags map[string]bool // safe because we never modify the map after creation
}

// ConfigProvider uses atomic pointer swap for lock-free reads
type ConfigProvider struct {
	current atomic.Pointer[AppConfig]
}

func NewConfigProvider(initial *AppConfig) *ConfigProvider {
	cp := &ConfigProvider{}
	cp.current.Store(initial)
	return cp
}

// Get is lock-free and safe for concurrent use
func (cp *ConfigProvider) Get() *AppConfig {
	return cp.current.Load()
}

// Update atomically swaps the entire config (no partial updates possible)
func (cp *ConfigProvider) Update(newConfig *AppConfig) {
	cp.current.Store(newConfig) // atomic swap
}

// UpdateWith creates a new config based on the current one
func (cp *ConfigProvider) UpdateWith(modifier func(old AppConfig) *AppConfig) {
	for {
		old := cp.current.Load()
		newCfg := modifier(*old) // copy old, modify the copy
		if cp.current.CompareAndSwap(old, newCfg) {
			return // success
		}
		// CAS failed: another goroutine updated first, retry
	}
}

func main() {
	cp := NewConfigProvider(&AppConfig{
		DBHost:   "localhost",
		DBPort:   5432,
		MaxConns: 10,
		FeatureFlags: map[string]bool{
			"dark_mode": false,
			"new_ui":    true,
		},
	})

	// 50 concurrent readers — no locks needed
	for i := 0; i < 50; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				cfg := cp.Get() // lock-free read
				_ = cfg.FeatureFlags["dark_mode"]
			}
		}()
	}

	// Writer: creates a completely new config object
	cp.UpdateWith(func(old AppConfig) *AppConfig {
		// Copy the flags map (never modify the original)
		newFlags := make(map[string]bool, len(old.FeatureFlags))
		for k, v := range old.FeatureFlags {
			newFlags[k] = v
		}
		newFlags["dark_mode"] = true // enable feature flag
		return &AppConfig{
			DBHost:       old.DBHost,
			DBPort:       old.DBPort,
			MaxConns:     20, // increased
			FeatureFlags: newFlags,
		}
	})

	time.Sleep(100 * time.Millisecond)
	fmt.Printf("config: %+v\n", cp.Get())
}
```

**The rule:** Create a NEW struct for every change. Never modify in place. Readers always see a consistent snapshot.

---

# 10. runtime/trace — Step-by-Step Walkthrough

> **Interview frequency: MEDIUM at Staff+ level.** Shows you can diagnose scheduling and GC issues.

---

## 10.1 Generating a Trace

```go
package main

import (
	"fmt"
	"os"
	"runtime/trace"
	"sync"
	"time"
)

func main() {
	// Step 1: Create trace file
	f, err := os.Create("trace.out")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Step 2: Start tracing
	if err := trace.Start(f); err != nil {
		panic(err)
	}
	defer trace.Stop()

	// Step 3: Run concurrent code
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Create a named task region (visible in trace viewer)
			ctx, task := trace.NewTask(nil, fmt.Sprintf("worker-%d", id))
			defer task.End()

			// Create a sub-region within the task
			trace.WithRegion(ctx, "computation", func() {
				time.Sleep(50 * time.Millisecond)
			})

			trace.WithRegion(ctx, "io", func() {
				time.Sleep(30 * time.Millisecond)
			})
		}(i)
	}
	wg.Wait()
}

// Step 4: Analyze
// go run main.go           → generates trace.out
// go tool trace trace.out   → opens browser with interactive viewer
```

## 10.2 What to Look For in the Trace Viewer

```
┌─────────────────────────────────────────────────────────┐
│ go tool trace                                           │
├─────────────────────────────────────────────────────────┤
│                                                         │
│ View trace         → Timeline of goroutine execution    │
│                      Look for:                          │
│                      - Long gaps (blocked goroutines)   │
│                      - GC pauses (STW events)           │
│                      - Uneven P utilization             │
│                                                         │
│ Goroutine analysis → Per-goroutine execution stats      │
│                      Look for:                          │
│                      - Time spent in syscalls            │
│                      - Time blocked on channels/mutexes │
│                      - High scheduling latency          │
│                                                         │
│ Network/Sync       → Blocking events                    │
│ blocking profile     Look for:                          │
│                      - Mutex contention hot spots       │
│                      - Channel blocking patterns        │
│                                                         │
│ Scheduler latency  → Time between runnable and running  │
│ profile              Look for:                          │
│                      - High latency = too many          │
│                        goroutines for available Ps      │
│                                                         │
│ User-defined tasks → Your trace.NewTask regions         │
│ and regions          Look for:                          │
│                      - Which phases take longest        │
│                      - Parallelism of task execution    │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## 10.3 Programmatic HTTP Trace Endpoint

```go
import (
	"net/http"
	_ "net/http/pprof"
	"runtime/trace"
)

// Add to your server for production trace capture:
// curl http://localhost:6060/debug/pprof/trace?seconds=5 > trace.out
// go tool trace trace.out
func init() {
	go http.ListenAndServe(":6060", nil)
}
```

---

# 11. GODEBUG Environment Variables

> **Interview frequency: LOW but impressive when mentioned.** Shows deep runtime knowledge.

---

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ GODEBUG Variable           │ What It Does                                  │
├─────────────────────────────┼───────────────────────────────────────────────┤
│ schedtrace=1000            │ Print scheduler state every 1000ms            │
│                            │ Shows: gomaxprocs, idle/running/blocked Ps,   │
│                            │ runqueue lengths, global runqueue             │
│                            │                                               │
│ scheddetail=1              │ Print per-P details (use with schedtrace)     │
│                            │                                               │
│ gctrace=1                  │ Print GC activity (pause times, heap sizes)   │
│                            │ Example output:                               │
│                            │ gc 1 @0.012s 2%: 0.013+0.24+0.003 ms clock   │
│                            │ (gc# @time %cpu: Tgc_sweep+Tgc_mark+Tgc_stop)│
│                            │                                               │
│ madvdontneed=1             │ Use MADV_DONTNEED for memory return to OS     │
│                            │ (default in Go 1.16+)                         │
│                            │                                               │
│ asyncpreemptoff=1          │ Disable async preemption (Go 1.14+)           │
│                            │ Useful for debugging preemption-related bugs  │
│                            │                                               │
│ tracebackancestors=N       │ Print N ancestor goroutine stacks in panics   │
│                            │ Helps trace goroutine creation chains         │
│                            │                                               │
│ invalidptr=1               │ Crash on invalid pointer in cgo or unsafe     │
│                            │                                               │
│ cgocheck=2                 │ Extra strict cgo pointer checks               │
└─────────────────────────────┴───────────────────────────────────────────────┘

# Usage examples:

# Watch scheduler in real-time
GODEBUG=schedtrace=1000 go run main.go

# Watch GC activity
GODEBUG=gctrace=1 go run main.go

# Trace goroutine ancestry in panics
GODEBUG=tracebackancestors=5 go run main.go

# Combined
GODEBUG=schedtrace=500,gctrace=1 go run main.go
```

## Reading schedtrace output

```
SCHED 1004ms: gomaxprocs=8 idleprocs=6 threads=10 spinningthreads=1
  idlethreads=3 runqueue=0 [0 0 2 0 0 0 0 0]

# Interpretation:
# gomaxprocs=8    → 8 logical processors
# idleprocs=6     → 6 of them idle (only 2 doing work)
# runqueue=0      → global run queue is empty (good)
# [0 0 2 0 0 0 0 0] → per-P local run queues (P2 has 2 goroutines queued)
# spinningthreads=1 → 1 thread looking for work (work stealing)
```

---

# 12. Lock-Free Stack (Treiber Stack)

> **Interview frequency: HIGH at Staff+.** Classic lock-free data structure using CAS.

---

```go
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

type node struct {
	value int
	next  *node
}

type LockFreeStack struct {
	top unsafe.Pointer // *node
}

func NewLockFreeStack() *LockFreeStack {
	return &LockFreeStack{}
}

func (s *LockFreeStack) Push(val int) {
	newNode := &node{value: val}
	for {
		oldTop := atomic.LoadPointer(&s.top)
		newNode.next = (*node)(oldTop)
		if atomic.CompareAndSwapPointer(&s.top, oldTop, unsafe.Pointer(newNode)) {
			return // CAS succeeded
		}
		// CAS failed: another goroutine modified top, retry
	}
}

func (s *LockFreeStack) Pop() (int, bool) {
	for {
		oldTop := atomic.LoadPointer(&s.top)
		if oldTop == nil {
			return 0, false // stack is empty
		}
		oldNode := (*node)(oldTop)
		newTop := unsafe.Pointer(oldNode.next)
		if atomic.CompareAndSwapPointer(&s.top, oldTop, newTop) {
			return oldNode.value, true // CAS succeeded
		}
		// CAS failed: retry
	}
}

func (s *LockFreeStack) IsEmpty() bool {
	return atomic.LoadPointer(&s.top) == nil
}

func main() {
	stack := NewLockFreeStack()
	var wg sync.WaitGroup

	// 100 goroutines push
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			stack.Push(val)
		}(i)
	}
	wg.Wait()

	// 100 goroutines pop
	var popped sync.Map
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if val, ok := stack.Pop(); ok {
				popped.Store(val, true)
			}
		}()
	}
	wg.Wait()

	count := 0
	popped.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	fmt.Printf("pushed 100, popped %d, stack empty: %v\n", count, stack.IsEmpty())
}
```

**Key concepts:**
- **CAS loop**: Try the operation, if another goroutine interfered, retry
- **ABA problem**: Node A is popped, a new node A is pushed. CAS succeeds but the stack state is wrong. Solution: tagged pointers (version counter)
- **No locks**: Progress guarantee — at least one goroutine always makes progress (lock-free, not wait-free)

---

# 13. singleflight Package — Real Usage

> **Interview frequency: HIGH.** Part 9 had a from-scratch implementation. Here's how you actually use the real package.

---

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

type UserService struct {
	sf    singleflight.Group
	cache sync.Map
	dbCalls atomic.Int32
}

func (s *UserService) GetUser(ctx context.Context, userID string) (string, error) {
	// Check cache first
	if val, ok := s.cache.Load(userID); ok {
		return val.(string), nil
	}

	// Deduplicate concurrent requests for the same user
	result, err, shared := s.sf.Do(userID, func() (interface{}, error) {
		// Only ONE goroutine executes this, even if 100 are waiting
		s.dbCalls.Add(1)
		fmt.Printf("  DB query for user %s (call #%d)\n", userID, s.dbCalls.Load())
		time.Sleep(200 * time.Millisecond) // simulate DB

		user := fmt.Sprintf("User<%s>", userID)
		s.cache.Store(userID, user) // populate cache
		return user, nil
	})

	if err != nil {
		return "", err
	}

	if shared {
		fmt.Printf("  result shared for user %s\n", userID)
	}
	return result.(string), nil
}

func main() {
	svc := &UserService{}
	var wg sync.WaitGroup

	// 50 goroutines all requesting the same user simultaneously
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			user, _ := svc.GetUser(context.Background(), "user-123")
			_ = user
		}()
	}
	wg.Wait()

	fmt.Printf("\n50 concurrent requests, only %d DB call(s)\n", svc.dbCalls.Load())

	// DoChan: non-blocking variant
	fmt.Println("\n--- DoChan example ---")
	var sf singleflight.Group
	ch := sf.DoChan("key", func() (interface{}, error) {
		time.Sleep(100 * time.Millisecond)
		return "async result", nil
	})

	select {
	case result := <-ch:
		fmt.Printf("got: %v, shared: %v\n", result.Val, result.Shared)
	case <-time.After(1 * time.Second):
		fmt.Println("timeout")
	}

	// Forget: allow a new call even if one is in-flight
	// sf.Forget("key") // use when you want to force a refresh
}
```

**`singleflight` methods:**
- `Do(key, fn)` — blocking, returns result + error + shared flag
- `DoChan(key, fn)` — returns a channel, non-blocking
- `Forget(key)` — removes key from in-flight map (allows new call)

**Install:** `go get golang.org/x/sync`

---

# 14. errgroup Advanced Patterns

> **Interview frequency: HIGH.** errgroup is the standard tool for structured concurrency in Go.

---

## 14.1 SetLimit — Bounded Concurrency

```go
package main

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(3) // at most 3 goroutines running concurrently

	for i := 0; i < 10; i++ {
		id := i
		g.Go(func() error {
			fmt.Printf("  processing %d (ctx err: %v)\n", id, ctx.Err())
			time.Sleep(200 * time.Millisecond)
			if id == 7 {
				return fmt.Errorf("task %d failed", id)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		fmt.Println("error:", err)
	}
}
```

## 14.2 TryGo — Non-Blocking Task Submission

```go
package main

import (
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	g := &errgroup.Group{}
	g.SetLimit(2)

	submitted := 0
	rejected := 0

	for i := 0; i < 10; i++ {
		id := i
		// TryGo returns false if the limit would be exceeded
		if g.TryGo(func() error {
			time.Sleep(100 * time.Millisecond)
			fmt.Printf("  ran task %d\n", id)
			return nil
		}) {
			submitted++
		} else {
			rejected++
			fmt.Printf("  rejected task %d (at capacity)\n", id)
		}
	}

	g.Wait()
	fmt.Printf("submitted: %d, rejected: %d\n", submitted, rejected)
}
```

## 14.3 Common Pattern: Parallel Fetch with Results

```go
package main

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
)

type APIResponse struct {
	Source string
	Data   string
}

func main() {
	g, ctx := errgroup.WithContext(context.Background())

	var mu sync.Mutex
	var results []APIResponse

	sources := []string{"users-api", "orders-api", "products-api", "reviews-api"}
	for _, src := range sources {
		src := src
		g.Go(func() error {
			// Simulate API call
			data := fmt.Sprintf("data from %s", src)
			_ = ctx // use for downstream calls

			mu.Lock()
			results = append(results, APIResponse{Source: src, Data: data})
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		fmt.Println("failed:", err)
		return
	}

	for _, r := range results {
		fmt.Printf("  %s: %s\n", r.Source, r.Data)
	}
}
```

---

# 15. Distributed Tracing Context Propagation

> **Interview frequency: MEDIUM-HIGH for microservices roles.** Understanding how trace context flows through goroutines and across services.

---

## 15.1 Concept: How Trace Context Flows

```
Service A (HTTP Handler)
  │
  │ context.Context carries:
  │   - trace_id: "abc123"
  │   - span_id: "span-1"
  │   - baggage: { "user_id": "u42" }
  │
  ├── goroutine 1: calls Service B
  │     │ passes context → HTTP headers:
  │     │   traceparent: 00-abc123-span2-01
  │     │
  │     └── Service B receives → extracts → creates child span
  │
  ├── goroutine 2: calls Service C (same trace_id!)
  │     └── ...
  │
  └── goroutine 3: writes to DB (same trace_id!)
```

## 15.2 Manual Trace Propagation (No OpenTelemetry Dependency)

```go
package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
)

// Trace context keys (unexported to prevent collisions)
type traceKey struct{}
type spanKey struct{}

type TraceInfo struct {
	TraceID string
	SpanID  string
	ParentSpanID string
}

func WithTrace(ctx context.Context, info TraceInfo) context.Context {
	return context.WithValue(ctx, traceKey{}, info)
}

func TraceFromContext(ctx context.Context) (TraceInfo, bool) {
	info, ok := ctx.Value(traceKey{}).(TraceInfo)
	return info, ok
}

// Create a child span (new span ID, same trace ID)
func ChildSpan(ctx context.Context, operationName string) (context.Context, TraceInfo) {
	parent, _ := TraceFromContext(ctx)
	child := TraceInfo{
		TraceID:      parent.TraceID,
		SpanID:       fmt.Sprintf("span-%d", rand.Intn(10000)),
		ParentSpanID: parent.SpanID,
	}
	fmt.Printf("  [%s] started span %s (parent: %s, trace: %s)\n",
		operationName, child.SpanID, child.ParentSpanID, child.TraceID)
	return WithTrace(ctx, child), child
}

// Simulated services
func handleRequest(ctx context.Context) {
	ctx, _ = ChildSpan(ctx, "handleRequest")

	var wg sync.WaitGroup

	// Fan out to multiple goroutines — context carries trace
	wg.Add(2)
	go func() {
		defer wg.Done()
		fetchFromDB(ctx)
	}()
	go func() {
		defer wg.Done()
		callExternalAPI(ctx)
	}()
	wg.Wait()
}

func fetchFromDB(ctx context.Context) {
	ctx, _ = ChildSpan(ctx, "fetchFromDB")
	// DB query here — trace ID is propagated
}

func callExternalAPI(ctx context.Context) {
	ctx, _ = ChildSpan(ctx, "callExternalAPI")
	// HTTP call would inject trace headers:
	// req.Header.Set("traceparent", fmt.Sprintf("00-%s-%s-01", info.TraceID, info.SpanID))
}

// Propagation to HTTP headers (outgoing)
func injectTraceHeaders(ctx context.Context, headers map[string]string) {
	info, ok := TraceFromContext(ctx)
	if !ok {
		return
	}
	// W3C Trace Context format
	headers["traceparent"] = fmt.Sprintf("00-%s-%s-01", info.TraceID, info.SpanID)
}

// Extract from HTTP headers (incoming)
func extractTraceFromHeaders(headers map[string]string) TraceInfo {
	// Parse traceparent header
	tp := headers["traceparent"]
	if tp == "" {
		return TraceInfo{TraceID: fmt.Sprintf("trace-%d", rand.Intn(100000))}
	}
	// Simplified parsing
	var version, traceID, spanID, flags string
	fmt.Sscanf(tp, "%s-%s-%s-%s", &version, &traceID, &spanID, &flags)
	return TraceInfo{TraceID: traceID, ParentSpanID: spanID}
}

func main() {
	// Incoming request creates root span
	rootTrace := TraceInfo{
		TraceID: "abc-123-def",
		SpanID:  "root-span",
	}
	ctx := WithTrace(context.Background(), rootTrace)

	fmt.Println("=== Request trace ===")
	handleRequest(ctx)
	fmt.Println("\nAll spans share trace_id: abc-123-def")
}
```

**Key rules for trace propagation in Go:**
1. Always pass `context.Context` as the first parameter
2. When spawning goroutines, pass the parent context (trace propagates automatically)
3. When making HTTP calls, inject trace headers from context
4. When receiving HTTP requests, extract trace headers into context
5. Never lose the context — every function in the call chain should accept and forward it

---

# Quick Reference: Interview Checklist for This File

| Topic | Section | Key Interview Question |
|-------|---------|----------------------|
| Testing | 1 | "How would you test this concurrent code?" |
| Race detector | 1.1 | "What does `go test -race` do? What are its limitations?" |
| goleak | 1.3 | "How do you detect goroutine leaks in tests?" |
| Benchmarking | 2 | "How would you benchmark this under contention?" |
| b.RunParallel | 2.1 | "Show me mutex vs atomic performance" |
| Profiling | 2.4 | "Service is slow under load — how do you diagnose?" |
| Retry + backoff | 3 | "Implement retry with exponential backoff and jitter" |
| Jitter | 3.2 | "Why add jitter? What's the thundering herd problem?" |
| Chan vs Mutex | 4 | "When do you use channels vs mutexes?" |
| reflect.Select | 5 | "How do you select on N channels where N is dynamic?" |
| HTTP concurrency | 6 | "How does Go's HTTP server handle concurrent requests?" |
| Goroutine-per-conn | 6.3 | "Trade-offs of goroutine-per-connection vs event loop?" |
| Ticker | 7 | "How do you properly manage a time.Ticker?" |
| Barrier | 8 | "Implement a CyclicBarrier in Go" |
| Immutable data | 9 | "How do you avoid locks entirely?" |
| runtime/trace | 10 | "How do you trace goroutine scheduling issues?" |
| GODEBUG | 11 | "How do you debug the Go scheduler?" |
| Lock-free stack | 12 | "Implement a lock-free data structure" |
| singleflight | 13 | "How do you prevent thundering herd in a cache?" |
| errgroup | 14 | "How do you run N tasks with bounded concurrency?" |
| Distributed tracing | 15 | "How does trace context propagate through goroutines?" |
