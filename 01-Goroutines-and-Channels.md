# Part 1: Goroutines, Channels, Buffered vs Unbuffered, Select, WaitGroups

> Go Concurrency Interview Guide — From Beginner to Staff Engineer

---

## Table of Contents

1. [Goroutines](#1-goroutines)
2. [Channels](#2-channels)
3. [Buffered vs Unbuffered Channels](#3-buffered-vs-unbuffered-channels)
4. [Select Statement](#4-select-statement)
5. [WaitGroups](#5-waitgroups)

---

# 1. GOROUTINES

## Concept Overview

Goroutines are lightweight threads of execution managed by the Go runtime, not by the OS. They are the foundational unit of concurrency in Go.

**Key properties:**
- **Lightweight:** Initial stack size of ~2-8 KB (vs ~1-8 MB for OS threads), grows and shrinks dynamically.
- **M:N Threading Model:** M goroutines are multiplexed onto N OS threads by the Go scheduler.
- **G-M-P Model:**
  - **G (Goroutine):** The goroutine itself, holding stack, instruction pointer, and state.
  - **M (Machine):** An OS thread. Executes goroutines.
  - **P (Processor):** A logical processor context with a local run queue. There are GOMAXPROCS Ps.
  - Each P has a local run queue of Gs. Ms must acquire a P to run Gs.
- **Cooperative + Preemptive Scheduling:**
  - Before Go 1.14: Cooperative only (goroutines yield at function calls, channel ops, syscalls).
  - Go 1.14+: Asynchronous preemption via signals. Tight loops can be preempted.
- **Cost:** Creating a goroutine costs ~2-4 KB of memory. You can run millions of goroutines on a modern machine.
- **GOMAXPROCS:** Controls the number of OS threads that can execute user-level Go code simultaneously. Defaults to the number of CPU cores.

**Goroutine Lifecycle:**
1. Created with `go func()` — placed on the local run queue of the current P.
2. Scheduled by the Go runtime — picked up by an M bound to a P.
3. Runs until it blocks (channel, mutex, syscall), yields, or is preempted.
4. Terminates when the function returns.

## Why Interviewers Ask It

- Foundation of **all** Go concurrency — cannot discuss any pattern without understanding goroutines.
- Tests whether you understand Go's concurrency model vs. parallelism.
- Reveals awareness of goroutine costs, limits, and scheduling behavior.
- Shows if you know how to manage goroutine lifecycles (no leaks, proper shutdown).
- Differentiates candidates who have built production systems from those who have only read tutorials.

## Common Interview Questions

### Q1: What is the difference between concurrency and parallelism in Go?

**Answer:** Concurrency is about structuring a program to handle multiple tasks by interleaving their execution, while parallelism is about executing multiple tasks simultaneously on different CPU cores. Go enables concurrency through goroutines and channels as first-class features. A concurrent program may run on a single core by context-switching between goroutines, whereas parallelism requires multiple cores running goroutines at the same time. Rob Pike summarized it: "Concurrency is about dealing with lots of things at once; parallelism is about doing lots of things at once."

**Example:**
```go
func main() {
    runtime.GOMAXPROCS(4) // enable parallelism on 4 cores
    var wg sync.WaitGroup
    for i := 0; i < 4; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            fmt.Printf("Goroutine %d running\n", id)
        }(i)
    }
    wg.Wait()
}
```

**Real-time Scenario:** In an API gateway handling 10,000 concurrent HTTP connections, concurrency allows all connections to be managed with goroutines on a few cores, while parallelism ensures CPU-bound tasks like JSON serialization execute simultaneously across cores.

---

### Q2: How does Go's goroutine scheduler work? Explain the G-M-P model.

**Answer:** Go uses an M:N scheduler that multiplexes M goroutines onto N OS threads. The G-M-P model: G (goroutine — unit of work), M (machine — OS thread that executes goroutines), P (processor — logical processor with a local run queue). Each P is bound to an M, and the M executes goroutines from P's local queue. When P's queue is empty, the scheduler uses work-stealing from other P's or the global run queue. GOMAXPROCS controls the number of P's.

**Example:**
```go
func main() {
    fmt.Println("CPUs:", runtime.NumCPU())
    fmt.Println("GOMAXPROCS (P count):", runtime.GOMAXPROCS(0))
    fmt.Println("Goroutines:", runtime.NumGoroutine())
    go func() {
        fmt.Println("This G is scheduled on a P, which runs on an M")
    }()
    runtime.Gosched()
}
```

**Real-time Scenario:** In a high-throughput microservice processing 100K requests/sec, understanding G-M-P helps tune GOMAXPROCS and diagnose latency spikes caused by scheduler contention or excessive thread creation from blocking syscalls.

---

### Q3: What is the default stack size of a goroutine? How does it grow?

**Answer:** A goroutine starts with a 2 KB stack (since Go 1.4), compared to 1-8 MB for OS threads. When a goroutine's stack needs to grow, the runtime detects overflow at function entry, allocates a new stack at double the size, copies the old stack, and updates all pointers. This "copyable stack" approach replaced segmented stacks which suffered from the "hot-split" problem. Stacks can also shrink during GC if largely unused.

**Example:**
```go
func recurse(depth int) {
    var buf [1024]byte // force stack growth
    _ = buf
    if depth > 0 { recurse(depth - 1) }
}

func main() {
    go recurse(1000) // stack will grow dynamically from 2KB
    runtime.Gosched()
}
```

**Real-time Scenario:** In a web server spawning millions of goroutines for WebSocket connections, the small 2 KB initial stack ensures minimal memory overhead per idle connection, while dynamic growth handles deep call stacks during message processing.

---

### Q4: What happens if you launch 1 million goroutines?

**Answer:** Go can handle 1 million goroutines because each starts with ~2 KB stack, so 1M goroutines consume roughly 2 GB of stack memory. The scheduler efficiently multiplexes them across GOMAXPROCS OS threads. However, if each goroutine holds additional memory (buffers, connections), memory can explode. The primary bottleneck is memory, not scheduling. This is feasible if goroutines are mostly idle (I/O-bound) rather than CPU-bound.

**Example:**
```go
func main() {
    var wg sync.WaitGroup
    for i := 0; i < 1_000_000; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            runtime.Gosched() // simulate idle work
        }()
    }
    fmt.Println("Active goroutines:", runtime.NumGoroutine())
    wg.Wait()
}
```

**Real-time Scenario:** A chat server like Slack may use one goroutine per connected user. With 1 million concurrent users, you'd have 1 million goroutines — each mostly idle waiting for messages, making this pattern viable in Go.

---

### Q5: How does Go handle goroutine preemption? What changed in Go 1.14?

**Answer:** Before Go 1.14, goroutines were cooperatively preempted — they only yielded at function calls, channel operations, or I/O. A tight CPU-bound loop without function calls could starve other goroutines. Go 1.14 introduced asynchronous preemption using OS signals (SIGURG on Unix). The runtime can now preempt any goroutine at virtually any point, solving the starvation problem and making the scheduler fair.

**Example:**
```go
func main() {
    runtime.GOMAXPROCS(1)
    // Before Go 1.14, this tight loop would never yield
    go func() {
        for {} // CPU-bound loop — now preempted via async signals
    }()
    go func() {
        time.Sleep(100 * time.Millisecond)
        fmt.Println("Runs thanks to async preemption in Go 1.14+")
    }()
    time.Sleep(200 * time.Millisecond)
}
```

**Real-time Scenario:** In a data processing pipeline where some goroutines run tight computational loops (hashing, compression), async preemption ensures health-check endpoints and monitoring goroutines remain responsive.

---

### Q6: What is GOMAXPROCS? When would you change it?

**Answer:** GOMAXPROCS sets the maximum number of OS threads (P's) that can execute goroutines simultaneously. Since Go 1.5, it defaults to `runtime.NumCPU()`. You might lower it for CPU-bound workloads sharing a machine, or in containers where the runtime detects more cores than allocated. In Kubernetes, `go.uber.org/automaxprocs` automatically sets it based on CPU quota.

**Example:**
```go
func main() {
    fmt.Println("Current:", runtime.GOMAXPROCS(0))
    runtime.GOMAXPROCS(2) // set to 2 for a 2-core container
    fmt.Println("Updated:", runtime.GOMAXPROCS(0))
}
```

**Real-time Scenario:** In a Kubernetes pod with a 2-core CPU limit on a 64-core node, Go may set GOMAXPROCS=64, causing excessive context switching. Using `automaxprocs` sets it to 2, improving throughput by 30-50%.

---

### Q7: What is the difference between goroutines and OS threads?

**Answer:** Goroutines are user-space lightweight threads managed by the Go runtime with 2 KB initial stacks, while OS threads are kernel-level with 1-8 MB fixed stacks. Goroutines use M:N scheduling (multiplexed onto fewer OS threads), making creation (~0.3us) and context switching much cheaper than threads (~10us). OS threads are preemptively scheduled by the kernel; goroutines use cooperative + async preemption by the Go runtime.

**Example:**
```go
func main() {
    var wg sync.WaitGroup
    for i := 0; i < 10000; i++ {
        wg.Add(1)
        go func() { defer wg.Done(); runtime.Gosched() }()
    }
    wg.Wait()
    fmt.Println("10,000 goroutines with", runtime.GOMAXPROCS(0), "OS threads")
}
```

**Real-time Scenario:** A reverse proxy like Caddy handles tens of thousands of concurrent connections using goroutines. Doing the same with one OS thread per connection would require massive memory and hit OS thread limits.

---

### Q8: Can a goroutine run on multiple OS threads during its lifetime?

**Answer:** Yes, a goroutine can migrate between OS threads. When descheduled (preemption point, channel operation, syscall), it goes back into the run queue and may be picked up by a different P on a different M. This is why goroutine-local storage doesn't exist. If you need to pin a goroutine to a specific thread, use `runtime.LockOSThread()`.

**Example:**
```go
func main() {
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        runtime.LockOSThread()   // pin to current OS thread
        defer runtime.UnlockOSThread()
        // Useful for C libraries, GPU calls, or UI frameworks
        fmt.Println("Locked to one OS thread")
    }()
    wg.Wait()
}
```

**Real-time Scenario:** When using CGo to call C libraries that rely on thread-local storage (e.g., OpenGL, certain database drivers), you must call `runtime.LockOSThread()` to prevent goroutine migration mid-execution.

---

### Q9: What happens to a goroutine when it performs a blocking syscall?

**Answer:** When a goroutine makes a blocking syscall, the runtime detaches the P from the blocked M and hands the P to a new or idle M so other goroutines continue running. When the syscall completes, the M tries to reacquire a P. If unavailable, the goroutine goes to the global run queue and the M sleeps. This "hand-off" mechanism prevents blocking syscalls from stalling the scheduler.

**Example:**
```go
func main() {
    runtime.GOMAXPROCS(1)
    go func() {
        f, _ := os.Create("test.txt") // blocking syscall — M parked, P handed off
        f.Write([]byte("data"))
        f.Close()
        os.Remove("test.txt")
    }()
    go func() {
        fmt.Println("Running concurrently despite blocking syscall")
    }()
    time.Sleep(100 * time.Millisecond)
}
```

**Real-time Scenario:** In a microservice reading config files from disk at startup while accepting gRPC requests, the hand-off mechanism ensures file I/O doesn't block request handling goroutines.

---

### Q10: How does the Go runtime handle goroutine starvation?

**Answer:** The runtime uses several anti-starvation mechanisms: (1) the scheduler checks the global run queue every 61 ticks, (2) since Go 1.14, async preemption via OS signals forces long-running goroutines to yield, (3) work-stealing distributes goroutines from busy P's to idle ones, (4) the `sysmon` background thread monitors goroutines running >10ms and flags them for preemption.

**Example:**
```go
func main() {
    runtime.GOMAXPROCS(1)
    go func() {
        for i := 0; i < 1000000; i++ {} // compute-heavy
        fmt.Println("Compute done")
    }()
    go func() {
        fmt.Println("I got scheduled despite the busy goroutine!")
    }()
    time.Sleep(200 * time.Millisecond)
}
```

**Real-time Scenario:** In a real-time bidding system where some goroutines run expensive ad-scoring algorithms, anti-starvation mechanisms ensure monitoring endpoints remain responsive within strict SLAs.

---

### Q11: What is work stealing in the Go scheduler?

**Answer:** Work stealing is a load-balancing strategy. When a P exhausts its local run queue, it steals half of the goroutines from another P's queue. If no P has goroutines, it checks the global run queue, then the network poller for ready goroutines. This minimizes contention on a single global queue and keeps all P's busy, maximizing CPU utilization.

**Example:**
```go
func main() {
    runtime.GOMAXPROCS(4) // 4 P's
    var wg sync.WaitGroup
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            sum := 0
            for j := 0; j < 1000; j++ { sum += j }
        }(i)
    }
    wg.Wait()
    fmt.Println("Tasks distributed via work-stealing across 4 P's")
}
```

**Real-time Scenario:** In an ETL pipeline where one stage produces bursts of goroutines, work stealing distributes them across all cores automatically, avoiding bottlenecks on a single processor.

---

### Q12: How do you debug goroutine leaks?

**Answer:** Goroutine leaks occur when goroutines are started but never terminate — typically blocked on channels, waiting for locks, or stuck in infinite loops. Detect with `runtime.NumGoroutine()` monitoring. For deeper analysis, use `pprof` — the `goroutine` profile shows stack traces of all live goroutines. In tests, Uber's `goleak` package detects leaks automatically. Common causes: forgetting to close channels, missing context cancellation, sending to channels with no receiver.

**Example:**
```go
func leakyFunction() {
    ch := make(chan int)
    go func() {
        <-ch // LEAK: no one sends to ch, goroutine blocked forever
    }()
}

func main() {
    go func() { http.ListenAndServe("localhost:6060", nil) }()
    for i := 0; i < 100; i++ { leakyFunction() }
    fmt.Println("Goroutines:", runtime.NumGoroutine()) // growing!
    // Visit http://localhost:6060/debug/pprof/goroutine?debug=1
}
```

**Real-time Scenario:** In a long-running microservice, goroutine leaks from uncancelled HTTP client requests accumulate over days, causing memory exhaustion and OOM kills. Monitoring `runtime.NumGoroutine()` in Prometheus provides early warning.

---

### Q13: What is `runtime.Gosched()` and when would you use it?

**Answer:** `runtime.Gosched()` voluntarily yields execution, placing the current goroutine back into the run queue and letting others run. Unlike `time.Sleep()`, it introduces no delay — the goroutine is immediately eligible for rescheduling. With modern Go (post 1.14 async preemption), `Gosched()` is rarely needed but can be useful in testing to expose race conditions.

**Example:**
```go
func main() {
    runtime.GOMAXPROCS(1)
    go func() {
        for i := 0; i < 3; i++ {
            fmt.Println("Goroutine A:", i)
            runtime.Gosched() // yield to let others run
        }
    }()
    for i := 0; i < 3; i++ {
        fmt.Println("Main:", i)
        runtime.Gosched()
    }
}
```

**Real-time Scenario:** In test suites for concurrent code, `runtime.Gosched()` helps expose race conditions by forcing goroutine interleavings that might not occur naturally during short test runs.

---

### Q14: What is `runtime.Goexit()` and how does it differ from `return`?

**Answer:** `runtime.Goexit()` terminates the current goroutine but first runs all deferred functions. Unlike `return`, which returns from the current function, `Goexit()` terminates the entire goroutine regardless of call depth. Unlike `os.Exit()`, it doesn't terminate the program. It doesn't trigger a panic, so `recover()` won't catch it. The `testing` package uses `Goexit()` internally for `t.Fatal()` and `t.FailNow()`.

**Example:**
```go
func innerFunction() {
    defer fmt.Println("Deferred in inner runs!")
    fmt.Println("About to call Goexit")
    runtime.Goexit() // terminates goroutine, defers still run
    fmt.Println("Never executes")
}

func main() {
    go func() {
        defer fmt.Println("Deferred in goroutine runs!")
        innerFunction()
    }()
    time.Sleep(100 * time.Millisecond)
    fmt.Println("Main goroutine still running")
}
```

**Real-time Scenario:** In integration tests, `t.Fatal()` calls `runtime.Goexit()` under the hood to stop the test immediately while still executing cleanup defers (like closing database connections).

---

### Q15: What happens if the main goroutine exits before other goroutines finish?

**Answer:** When the main goroutine returns, the entire program terminates immediately — all other goroutines are killed without running their deferred functions. There is no implicit waiting for spawned goroutines. This is why `sync.WaitGroup`, channels, or `select` are essential to ensure goroutines complete before exit.

**Example:**
```go
func main() {
    // BAD: goroutine may never print
    go func() { fmt.Println("I might never run!") }()

    // GOOD: use WaitGroup
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        fmt.Println("I will definitely run!")
    }()
    wg.Wait()
}
```

**Real-time Scenario:** In a CLI tool that spawns goroutines to upload files concurrently, forgetting to wait means some files may never upload before the program exits, causing silent data loss.

---

### Q16: How does the GC interact with goroutines (stop-the-world pauses)?

**Answer:** Go uses a concurrent, tri-color mark-and-sweep GC. Most GC work happens concurrently, but two brief stop-the-world (STW) phases exist: one at mark start (enable write barrier) and one at mark end (cleanup). Since Go 1.8, STW pauses are typically under 100 microseconds. Having millions of goroutines slightly increases STW time because each goroutine's stack must be scanned for root pointers.

**Example:**
```go
func main() {
    debug.SetGCPercent(100) // default GC trigger
    var data [][]byte
    for i := 0; i < 1000; i++ {
        data = append(data, make([]byte, 1024))
    }
    runtime.GC() // force GC
    var stats debug.GCStats
    debug.ReadGCStats(&stats)
    fmt.Println("Last GC pause:", stats.Pause[0])
}
```

**Real-time Scenario:** In a low-latency trading system, tuning `GOGC` and minimizing heap allocations reduces GC pause frequency, preventing latency spikes during order execution.

---

### Q17: Why doesn't Go have goroutine-local storage (like thread-local storage)?

**Answer:** Go intentionally omits goroutine-local storage. The Go team believes GLS encourages implicit state passing that makes programs harder to understand. Since goroutines migrate between threads, traditional TLS doesn't map cleanly. Instead, Go encourages explicit state passing through function parameters, struct fields, or `context.Context` for request-scoped values.

**Example:**
```go
func processRequest(ctx context.Context) {
    traceID := ctx.Value("traceID").(string)
    userID := ctx.Value("userID").(string)
    fmt.Printf("Processing: trace=%s user=%s\n", traceID, userID)
}

func main() {
    ctx := context.WithValue(context.Background(), "traceID", "abc-123")
    ctx = context.WithValue(ctx, "userID", "user-456")
    var wg sync.WaitGroup
    wg.Add(1)
    go func() { defer wg.Done(); processRequest(ctx) }()
    wg.Wait()
}
```

**Real-time Scenario:** In a distributed microservices architecture, passing trace IDs via `context.Context` enables end-to-end request tracing across services using Jaeger or Zipkin, without relying on hidden thread-local state.

---

### Q18: How do you handle panics in goroutines?

**Answer:** A panic in a goroutine crashes the entire program if not recovered. A `recover()` in one goroutine cannot catch a panic from another. Each goroutine must handle its own panics using `defer` + `recover()` at the top of its function. This is critical in server applications where an unrecovered panic in a request handler would bring down the entire service.

**Example:**
```go
func safeGo(wg *sync.WaitGroup, fn func()) {
    wg.Add(1)
    go func() {
        defer wg.Done()
        defer func() {
            if r := recover(); r != nil {
                fmt.Println("Recovered panic:", r)
            }
        }()
        fn()
    }()
}

func main() {
    var wg sync.WaitGroup
    safeGo(&wg, func() { panic("something went wrong!") })
    wg.Wait()
    fmt.Println("Program continues after recovered panic")
}
```

**Real-time Scenario:** In an HTTP server, `net/http` recovers panics in handler goroutines to prevent a single bad request from crashing the server, logging the panic and returning a 500 error instead.

---

### Q19: What is `runtime.NumGoroutine()` and how would you use it for monitoring?

**Answer:** `runtime.NumGoroutine()` returns the number of goroutines that currently exist. It's a lightweight call that can be polled to detect leaks — a steadily increasing count indicates goroutines are created but not terminated. It's commonly exported as a Prometheus gauge metric. A healthy service should have a stable count; sudden spikes may indicate a burst or leak.

**Example:**
```go
func monitorGoroutines() {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for range ticker.C {
        count := runtime.NumGoroutine()
        fmt.Printf("[Monitor] Active goroutines: %d\n", count)
        if count > 10000 {
            fmt.Println("[ALERT] Possible goroutine leak!")
        }
    }
}
```

**Real-time Scenario:** In a production Kubernetes deployment, exporting `runtime.NumGoroutine()` as a Prometheus metric with alerts at 50,000 enables SRE teams to detect goroutine leaks before they cause OOM crashes.

---

### Q20: How do you profile goroutine usage in production with pprof?

**Answer:** Go's `net/http/pprof` package exposes profiling endpoints. The `/debug/pprof/goroutine` endpoint shows all active goroutines with stack traces, invaluable for diagnosing leaks and deadlocks. View as text (`?debug=1`) or download binary profiles for `go tool pprof` analysis. For non-HTTP services, use `runtime/pprof` to write profiles to files. Always protect pprof endpoints behind authentication in production.

**Example:**
```go
import (
    "net/http"
    _ "net/http/pprof" // registers /debug/pprof/ handlers
)

func main() {
    go func() {
        http.ListenAndServe("localhost:6060", nil)
    }()
    // Analyze: go tool pprof http://localhost:6060/debug/pprof/goroutine
    // View: curl localhost:6060/debug/pprof/goroutine?debug=1
}
```

**Real-time Scenario:** During a production incident where memory keeps growing, an SRE hits the pprof endpoint to see thousands of goroutines stuck on a database connection pool, pinpointing connection exhaustion as the root cause.

---

## Coding Problems

### Problem 1: Basic Goroutine Launch

**Difficulty:** Easy  
**Problem:** Write a program that launches 5 goroutines, each printing its ID (0-4). Ensure all complete before the program exits.

**Concepts Tested:** Goroutine creation, synchronization with WaitGroup  
**Expected Approach:**
```go
func main() {
    var wg sync.WaitGroup
    for i := 0; i < 5; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            fmt.Println("goroutine:", id)
        }(i)
    }
    wg.Wait()
}
```
**Variations:**
- What if you need to collect results from each goroutine? (Use a channel or mutex-protected slice)
- What if goroutines can fail? (Use `errgroup.Group`)

**Real-world Relevance:** Parallel task execution in microservices, batch processing.

---

### Problem 2: Goroutine Closure Bug

**Difficulty:** Easy-Medium  
**Problem:** The following code prints "5 5 5 5 5" instead of "0 1 2 3 4". Fix it and explain why.

```go
for i := 0; i < 5; i++ {
    go func() {
        fmt.Println(i)
    }()
}
time.Sleep(time.Second)
```

**Concepts Tested:** Closure variable capture, goroutine scheduling  
**Expected Approach:**
```go
// Fix 1: Pass as parameter
for i := 0; i < 5; i++ {
    go func(id int) {
        fmt.Println(id)
    }(i)
}

// Fix 2: Local copy (pre-Go 1.22)
for i := 0; i < 5; i++ {
    i := i
    go func() {
        fmt.Println(i)
    }()
}
```
**Why it happens:** The closure captures the variable `i`, not its value. By the time the goroutines execute, the loop has completed and `i` is 5.

**Variations:** What changes in Go 1.22+ with loop variable semantics? (Each iteration creates a new variable — the bug is fixed by default.)

**Real-world Relevance:** One of the most common production bugs in Go.

---

### Problem 3: Goroutine Leak Detection

**Difficulty:** Medium  
**Problem:** Identify the goroutine leak and fix it:

```go
func processRequests(urls []string) []Result {
    results := make(chan Result)
    for _, url := range urls {
        go func(u string) {
            results <- fetch(u) // blocks if nobody reads
        }(url)
    }
    var out []Result
    for i := 0; i < 3; i++ { // only reads 3
        out = append(out, <-results)
    }
    return out
}
```

**Concepts Tested:** Goroutine lifecycle, channel blocking, resource leaks  
**Root Cause:** If `len(urls) > 3`, the extra goroutines are blocked forever on `results <- fetch(u)`.  
**Expected Fix:**
```go
func processRequests(ctx context.Context, urls []string) []Result {
    results := make(chan Result, len(urls)) // buffered to prevent blocking
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    for _, url := range urls {
        go func(u string) {
            select {
            case results <- fetch(u):
            case <-ctx.Done():
                return
            }
        }(url)
    }

    var out []Result
    for i := 0; i < 3; i++ {
        out = append(out, <-results)
    }
    return out
}
```

**Variations:** How would you add a timeout? How would you limit concurrency to N simultaneous fetches?

**Real-world Relevance:** HTTP client goroutine leaks are one of the most common production issues.

---

### Problem 4: Concurrent Counter (Race Condition)

**Difficulty:** Medium  
**Problem:** Implement a thread-safe counter that can be incremented by 1000 goroutines simultaneously. Verify the final count is exactly 1000.

**Concepts Tested:** Race conditions, synchronization  
**Expected Approach:**
```go
// Approach 1: Mutex
type Counter struct {
    mu    sync.Mutex
    count int
}

func (c *Counter) Increment() {
    c.mu.Lock()
    c.count++
    c.mu.Unlock()
}

// Approach 2: Atomic (preferred for simple counters)
type Counter struct {
    count atomic.Int64
}

func (c *Counter) Increment() {
    c.count.Add(1)
}
```

**Variations:**
- Compare mutex vs atomic performance (benchmark them)
- When to use which? (Atomic for simple operations; mutex when you need to protect multiple fields)

**Real-world Relevance:** Metrics collection, request counting, rate limiting counters.

---

### Problem 5: Goroutine Scheduler Behavior

**Difficulty:** Hard  
**Problem:** Predict the output and explain scheduling behavior:

```go
func main() {
    runtime.GOMAXPROCS(1)
    go func() {
        for i := 0; i < 10; i++ {
            fmt.Println("goroutine:", i)
        }
    }()
    for i := 0; i < 10; i++ {
        fmt.Println("main:", i)
    }
}
```

**Concepts Tested:** Scheduler behavior, GOMAXPROCS, preemption  
**Expected Approach:**
- With `GOMAXPROCS(1)`, only one goroutine runs at a time.
- Since Go 1.14, the runtime can preempt goroutines asynchronously, so output is non-deterministic.
- Pre-Go 1.14: The main goroutine would likely run to completion first (cooperative scheduling, no preemption point in `fmt.Println` tight loops — though `fmt.Println` does involve I/O which is a yield point).
- The spawned goroutine may or may not run before main finishes.

**Variations:**
- What changes with `GOMAXPROCS(2)`? (True parallelism — interleaved output)
- What if you add `runtime.Gosched()` in the loop? (Explicit yield)

**Real-world Relevance:** Understanding scheduling for latency-sensitive applications, debugging non-deterministic behavior.

---

### Problem 6: Parallel Matrix Multiplication

**Difficulty:** Hard  
**Problem:** Implement parallel matrix multiplication. Each goroutine computes one row of the result matrix.

**Concepts Tested:** Data parallelism, goroutine coordination, no shared mutable state  
**Expected Approach:**
```go
func multiply(A, B [][]int) [][]int {
    n := len(A)
    m := len(B[0])
    k := len(B)
    C := make([][]int, n)

    var wg sync.WaitGroup
    for i := 0; i < n; i++ {
        C[i] = make([]int, m)
        wg.Add(1)
        go func(row int) {
            defer wg.Done()
            for j := 0; j < m; j++ {
                for x := 0; x < k; x++ {
                    C[row][j] += A[row][x] * B[x][j]
                }
            }
        }(i)
    }
    wg.Wait()
    return C
}
```

**Key insight:** No locks needed because each goroutine writes to a different row — no shared mutable state.

**Variations:**
- What if the matrix is 10000x10000? (Use bounded concurrency — worker pool of GOMAXPROCS workers, not 10000 goroutines)
- How to optimize for cache locality?

**Real-world Relevance:** Compute-intensive parallel tasks, ML inference, scientific computing.

---

### Problem 7: Task Scheduler with Priorities

**Difficulty:** Hard (FAANG-level)  
**Problem:** Build a task scheduler that:
- Accepts tasks with priorities (high, medium, low)
- Runs up to N tasks concurrently
- Supports task cancellation via context
- Reports task completion status

**Concepts Tested:** Goroutine management, priority queues, synchronization, design  
**Expected Approach:**
```go
type Task struct {
    ID       string
    Priority int
    Fn       func(ctx context.Context) error
}

type Scheduler struct {
    workers   int
    taskQueue chan Task // or use a heap for priority
    ctx       context.Context
    cancel    context.CancelFunc
    wg        sync.WaitGroup
}

func NewScheduler(workers int) *Scheduler {
    ctx, cancel := context.WithCancel(context.Background())
    s := &Scheduler{
        workers:   workers,
        taskQueue: make(chan Task, 100),
        ctx:       ctx,
        cancel:    cancel,
    }
    for i := 0; i < workers; i++ {
        s.wg.Add(1)
        go s.worker()
    }
    return s
}

func (s *Scheduler) worker() {
    defer s.wg.Done()
    for {
        select {
        case task := <-s.taskQueue:
            task.Fn(s.ctx)
        case <-s.ctx.Done():
            return
        }
    }
}
```

For true priority scheduling, use a `container/heap`-backed priority queue protected by a mutex, with a condition variable to signal workers.

**Variations:** Add task dependencies (DAG), retry logic, timeout per task, persistent queue.

**Real-world Relevance:** Job scheduling systems (Kubernetes job controller, Temporal, Celery).

---

## Follow-up Questions

1. How would you profile goroutine usage in production? (`pprof` goroutine profile, `runtime.NumGoroutine()`)
2. How does the GC interact with goroutines? (STW pauses affect all goroutines)
3. What is goroutine-local storage and why doesn't Go have it? (Go philosophy: pass data explicitly, not implicitly)
4. How do you handle panics in goroutines? (`defer recover()` — an unrecovered panic in any goroutine crashes the entire process)
5. What is the maximum number of goroutines you can run? (Limited by memory — each goroutine ~2-8 KB initial, grows as needed)

## Common Mistakes

| Mistake | Consequence | Fix |
|---------|-------------|-----|
| Not waiting for goroutines | Program exits before goroutines finish | Use `sync.WaitGroup` or channels |
| Closure variable capture in loops | All goroutines see the same (final) value | Pass as parameter or use Go 1.22+ |
| Launching unlimited goroutines | OOM, excessive scheduling overhead | Use worker pools or semaphores |
| Ignoring goroutine leaks | Memory leak, file descriptor exhaustion | Use context cancellation, `goleak` in tests |
| Assuming execution order | Non-deterministic bugs | Never assume order without synchronization |
| Not recovering panics | One goroutine panic crashes the process | `defer func() { recover() }()` |
| Using `time.Sleep` for sync | Flaky, fragile timing | Use proper primitives (WaitGroup, channel) |

## Expected Optimal Solutions

- Use `sync.WaitGroup` for simple fan-out/fan-in.
- Use `errgroup.Group` for goroutines that can fail.
- Use semaphore pattern (buffered channel) for bounded concurrency.
- Use `context.Context` for cancellation and timeouts.
- Always have a **clear ownership model** for goroutine lifecycle — every goroutine should have a known way to terminate.

---

# 2. CHANNELS

## Concept Overview

Channels are Go's primary mechanism for communication between goroutines. They embody the principle: **"Don't communicate by sharing memory; share memory by communicating."**

**Key properties:**
- **First-class values:** Can be passed to functions, stored in structs, sent over other channels.
- **Typed:** `chan int`, `chan string`, `chan MyStruct`.
- **Directional:** `chan<- int` (send-only), `<-chan int` (receive-only), `chan int` (bidirectional).
- **Thread-safe:** All channel operations are goroutine-safe by design.

**Channel Operations:**
| Operation | Unbuffered | Buffered (not full) | Buffered (full) | Nil Channel | Closed Channel |
|-----------|-----------|-------------------|----------------|-------------|---------------|
| `ch <- v` (send) | Blocks until receiver | Succeeds immediately | Blocks | Blocks forever | **PANIC** |
| `v := <-ch` (receive) | Blocks until sender | Succeeds immediately | Succeeds immediately | Blocks forever | Returns zero value |
| `close(ch)` | Succeeds | Succeeds | Succeeds | **PANIC** | **PANIC** |

**Channel Axioms (Memorize these):**
1. Send to a **nil** channel blocks forever.
2. Receive from a **nil** channel blocks forever.
3. Send to a **closed** channel panics.
4. Receive from a **closed** channel returns the zero value immediately (with `ok = false`).
5. Closing a **nil** channel panics.
6. Closing an **already-closed** channel panics.

**Internal structure (hchan):**
- Circular buffer (for buffered channels)
- Send and receive wait queues (linked lists of `sudog` structs)
- Mutex for synchronization
- Closed flag

## Why Interviewers Ask It

- Core communication mechanism — used in virtually every concurrent Go program.
- Channel axioms are a frequent source of bugs (panics, deadlocks).
- Reveals understanding of "share memory by communicating."
- Tests design thinking: when to use channels vs. mutexes.
- Shows awareness of performance characteristics.

## Common Interview Questions

### Q1: What happens when you send on a nil channel?

**Answer:** Sending on a nil channel blocks the goroutine forever. A nil channel is the zero value of a channel variable — it has not been initialized with `make()`. Both send and receive operations on a nil channel block indefinitely because there is no underlying data structure to facilitate the transfer. This behavior is useful in `select` statements where you can dynamically disable a case by setting its channel to nil.

**Example:**
```go
var ch chan int // nil channel
go func() {
    ch <- 42 // blocks forever — no underlying channel structure
}()
// goroutine leaked — blocked on nil channel send
```

**Real-time Scenario:** In a fan-in multiplexer, setting a source channel to nil after it's closed prevents the select from spinning on that closed channel, effectively disabling that input source at runtime.

---

### Q2: What happens when you receive from a closed channel?

**Answer:** Receiving from a closed channel returns the zero value of the channel's element type immediately and never blocks. If there are still values in the buffer, those are returned first. The "comma ok" idiom (`v, ok := <-ch`) is essential — `ok` is `false` when the channel is closed and drained, letting you distinguish between a real zero value and a closed-channel zero value.

**Example:**
```go
ch := make(chan int, 2)
ch <- 10
ch <- 20
close(ch)

fmt.Println(<-ch)          // 10 (buffered value)
fmt.Println(<-ch)          // 20 (buffered value)
v, ok := <-ch
fmt.Println(v, ok)         // 0 false (closed and drained)
```

**Real-time Scenario:** In a worker pool, the dispatcher closes the jobs channel when no more work is available, and workers use `for job := range jobs` to process remaining buffered jobs then exit cleanly.

---

### Q3: What happens when you send on a closed channel?

**Answer:** Sending on a closed channel causes a runtime panic: `panic: send on closed channel`. This is not recoverable in the normal program flow. This is why the convention in Go is that only the sender should close a channel. If multiple goroutines send to a channel, you need coordination (e.g., `sync.WaitGroup`) to ensure the channel is closed only after all senders are done.

**Example:**
```go
ch := make(chan int)
close(ch)
defer func() {
    if r := recover(); r != nil {
        fmt.Println("Recovered:", r) // send on closed channel
    }
}()
ch <- 42 // PANIC: send on closed channel
```

**Real-time Scenario:** In a pub/sub system with multiple publisher goroutines writing to a shared channel, premature channel closure by one publisher will panic the others. Using a WaitGroup to wait for all publishers before closing prevents this crash.

---

### Q4: How do you check if a channel is closed?

**Answer:** You cannot reliably check if a channel is closed without consuming a value. The only way is the "comma ok" receive: `v, ok := <-ch`. If `ok` is `false`, the channel is closed and drained. There is no `isClosed()` function because checking and then acting would be inherently racy. The idiomatic approach is to use `range` loops or `select` with the comma-ok pattern.

**Example:**
```go
ch := make(chan string, 1)
ch <- "hello"
close(ch)

if v, ok := <-ch; ok {
    fmt.Println("Received:", v)
}
if v, ok := <-ch; !ok {
    fmt.Println("Channel closed, got zero value:", v)
}
```

**Real-time Scenario:** In a stream processing pipeline, a consumer goroutine uses `for msg := range inCh` to keep processing until the upstream producer signals completion by closing the channel.

---

### Q5: When should you close a channel? Who should close it?

**Answer:** A channel should be closed when the sender is done sending data and wants to signal this to receivers. **The sender should always close the channel**, never the receiver. Closing is a broadcast signal — all goroutines blocked on a receive will be unblocked. You only need to close a channel when receivers need to know no more data is coming (e.g., to exit a `range` loop). If you never range over it, you don't need to close it — channels are garbage collected when unreferenced.

**Example:**
```go
func producer(ch chan<- int, wg *sync.WaitGroup) {
    defer wg.Done()
    for i := 0; i < 5; i++ { ch <- i }
}

func main() {
    ch := make(chan int)
    var wg sync.WaitGroup
    for i := 0; i < 3; i++ {
        wg.Add(1)
        go producer(ch, &wg)
    }
    go func() { wg.Wait(); close(ch) }() // close after ALL producers done
    for v := range ch { fmt.Println(v) }
}
```

**Real-time Scenario:** In a log aggregation service, multiple producer goroutines write log entries to a channel. A coordinator waits for all producers to finish, then closes the channel so the consumer can flush remaining logs and shut down cleanly.

---

### Q6: What is the difference between `v := <-ch` and `v, ok := <-ch`?

**Answer:** `v := <-ch` receives a value but cannot distinguish between a legitimately sent value and a zero value from a closed channel. `v, ok := <-ch` adds the boolean `ok` — `true` if the value was from a send, `false` if the channel is closed and drained. The single-value form is fine when you know the channel won't be closed, but the two-value form is essential for proper termination logic.

**Example:**
```go
ch := make(chan int, 2)
ch <- 0  // intentional zero value
close(ch)

v1 := <-ch
fmt.Println("v1:", v1) // 0 — but was it sent or closed?

v2, ok := <-ch
fmt.Println("v2:", v2, "ok:", ok) // 0 false — channel is closed
```

**Real-time Scenario:** In a task queue where 0 is a valid task ID, using `v, ok := <-ch` prevents the consumer from processing a phantom "task 0" when the channel is actually closed.

---

### Q7: Can you close a channel multiple times?

**Answer:** No. Closing an already-closed channel causes a runtime panic: `panic: close of closed channel`. To avoid this, ensure only one goroutine is responsible for closing the channel, or use `sync.Once` to guarantee the close happens exactly once. Never close a channel from the receiver side.

**Example:**
```go
ch := make(chan int)
var once sync.Once
safeClose := func() {
    once.Do(func() {
        close(ch)
        fmt.Println("Channel closed safely")
    })
}
safeClose() // closes
safeClose() // no-op, no panic
```

**Real-time Scenario:** In a graceful shutdown handler where multiple signals (SIGTERM, SIGINT) may trigger shutdown concurrently, using `sync.Once` to close the done channel prevents a double-close panic.

---

### Q8: What is a directional channel and why use it?

**Answer:** Directional channels restrict a channel to send-only (`chan<- T`) or receive-only (`<-chan T`) at the type level. They enforce API contracts at compile time — a function taking `chan<- int` can only send, not receive or close improperly. Bidirectional channels are implicitly convertible to directional ones, but not vice versa. This prevents misuse and makes data flow direction explicit.

**Example:**
```go
func producer(out chan<- int) {
    for i := 0; i < 5; i++ { out <- i }
    close(out) // sender closes
}

func consumer(in <-chan int) {
    for v := range in { fmt.Println("Got:", v) }
    // close(in) // compile error: cannot close receive-only channel
}

func main() {
    ch := make(chan int)
    go producer(ch)
    consumer(ch)
}
```

**Real-time Scenario:** In a pipeline architecture, each stage receives from `<-chan Image` and sends to `chan<- Image`, making the data flow direction clear and preventing accidental reads from the output channel.

---

### Q9: How do channels work internally?

**Answer:** Internally, a channel is the `hchan` struct in the Go runtime. It contains a circular buffer (for buffered channels), a mutex for synchronization, and two wait queues (`sendq` and `recvq`) as linked lists of `sudog` structs. When a goroutine sends on a full channel, it parks on `sendq`. When a receiver arrives, it dequeues the sender, copies data directly, and wakes it. This direct goroutine-to-goroutine copy avoids unnecessary buffer operations.

**Example:**
```go
// Conceptual hchan structure:
// hchan {
//   qcount:   current items in buffer
//   dataqsiz: buffer capacity
//   buf:      pointer to circular buffer
//   sendx:    send index into buffer
//   recvx:    receive index into buffer
//   sendq:    list of waiting senders (sudog)
//   recvq:    list of waiting receivers (sudog)
//   lock:     mutex
// }
ch := make(chan int, 5)
ch <- 42
fmt.Println("len:", len(ch), "cap:", cap(ch)) // 1 5
```

**Real-time Scenario:** Understanding channel internals helps diagnose performance — knowing that each operation acquires a mutex explains why high-contention channels become bottlenecks in systems processing millions of events per second.

---

### Q10: When should you use channels vs mutexes?

**Answer:** Use channels when you need to transfer ownership of data between goroutines, coordinate goroutine lifecycle, or implement pipeline patterns. Use mutexes when protecting shared state accessed by multiple goroutines without transferring ownership. The Go proverb: "Don't communicate by sharing memory; share memory by communicating." For simple counter increments or cache access, a mutex or `sync/atomic` is simpler and faster.

**Example:**
```go
// Mutex: protecting shared state
type SafeCounter struct {
    mu sync.Mutex
    v  map[string]int
}
func (c *SafeCounter) Inc(key string) {
    c.mu.Lock()
    c.v[key]++
    c.mu.Unlock()
}

// Channel: passing data between goroutines
func generate(nums ...int) <-chan int {
    out := make(chan int)
    go func() { for _, n := range nums { out <- n }; close(out) }()
    return out
}
```

**Real-time Scenario:** In a web server, use a mutex to protect an in-memory session store, but use channels to coordinate a pipeline that reads requests, processes them, and writes responses.

---

### Q11: What is the performance difference between channels and mutexes?

**Answer:** Channels are 2-5x slower than mutexes for simple synchronization because channels involve goroutine scheduling, `sudog` allocation, and potential context switches. A `sync.Mutex` lock/unlock is essentially an atomic CAS in the uncontended case (~20ns vs ~50-200ns for channels). For simple shared state protection, mutexes are the better choice. Channels shine when you need to coordinate communication and lifecycle.

**Example:**
```go
func benchMutex(n int) time.Duration {
    var mu sync.Mutex
    counter := 0
    start := time.Now()
    for i := 0; i < n; i++ {
        mu.Lock(); counter++; mu.Unlock()
    }
    return time.Since(start)
}

func benchChannel(n int) time.Duration {
    ch := make(chan int, 1)
    ch <- 0
    start := time.Now()
    for i := 0; i < n; i++ {
        v := <-ch; v++; ch <- v
    }
    return time.Since(start)
}
```

**Real-time Scenario:** In a rate limiter tracking millions of requests per second, using `sync/atomic` for the counter is far more efficient than routing every increment through a channel, reducing CPU overhead by 3-5x.

---

### Q12: How do you implement a channel timeout?

**Answer:** Use `select` with `time.After(d)` or `context.WithTimeout`. `time.After` returns a channel that fires after duration `d`. For production code, `context.WithTimeout` is preferred because it integrates with the broader context cancellation ecosystem and propagates timeouts across function boundaries.

**Example:**
```go
ch := make(chan string)
go func() {
    time.Sleep(2 * time.Second)
    ch <- "data"
}()

// Using context.WithTimeout (preferred)
ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
defer cancel()
select {
case result := <-ch:
    fmt.Println("Got:", result)
case <-ctx.Done():
    fmt.Println("Timeout:", ctx.Err())
}
```

**Real-time Scenario:** In a microservice calling a downstream API, a 5-second channel timeout ensures the caller doesn't hang indefinitely if the downstream is unresponsive, preventing cascading failures.

---

### Q13: How do you drain a channel?

**Answer:** Draining means consuming all remaining values without processing them, to unblock goroutines stuck on sends. The simplest way is `for range ch` which drains until the channel is closed. For channels that might not close, use `select` with `default` to drain non-blockingly. Draining is important during shutdown to prevent goroutine leaks.

**Example:**
```go
func drain(ch <-chan int) {
    for range ch {} // discard all values until closed
}

func drainNonBlocking(ch <-chan int) {
    for {
        select {
        case _, ok := <-ch:
            if !ok { return }
        default:
            return // no more values in buffer
        }
    }
}
```

**Real-time Scenario:** During graceful shutdown of a job queue, draining the jobs channel ensures all pending worker goroutines are unblocked and can terminate, preventing the shutdown from hanging.

---

### Q14: What is the "comma ok" idiom for channels?

**Answer:** The "comma ok" idiom uses the two-value receive: `v, ok := <-ch`. The boolean `ok` is `true` when the value was from a successful send, and `false` when the channel is closed and drained (with `v` being the zero value). This is essential for distinguishing real values from zero values returned by closed channels. It's analogous to the comma-ok idiom for map lookups and type assertions.

**Example:**
```go
ch := make(chan int, 3)
ch <- 100
ch <- 200
close(ch)

for {
    v, ok := <-ch
    if !ok {
        fmt.Println("Channel closed")
        break
    }
    fmt.Println("Received:", v)
}
```

**Real-time Scenario:** In an event streaming consumer, the comma-ok idiom lets the consumer distinguish between a valid event with value 0 (e.g., a reset event) and the stream being terminated by the producer.

---

### Q15: Can you range over a channel?

**Answer:** Yes, `for v := range ch` receives values from a channel until it is closed. The loop terminates automatically when the channel is closed and all buffered values are consumed. This is the most idiomatic way to consume all values. However, if the channel is never closed, the loop blocks forever, causing a goroutine leak.

**Example:**
```go
func fibonacci(n int, ch chan<- int) {
    a, b := 0, 1
    for i := 0; i < n; i++ {
        ch <- a
        a, b = b, a+b
    }
    close(ch) // must close for range to terminate
}

func main() {
    ch := make(chan int)
    go fibonacci(10, ch)
    for v := range ch { fmt.Println(v) }
}
```

**Real-time Scenario:** In a log tailing service, a producer reads log lines and sends them to a channel. The consumer uses `for line := range logCh` to process each line, terminating cleanly when the file is fully read and the channel closes.

---

### Q16: What is the zero-value of a channel?

**Answer:** The zero value of a channel is `nil`. A nil channel has not been initialized with `make()`. Send blocks forever, receive blocks forever, close panics. This behavior is intentionally useful in `select` — setting a channel to nil disables that case, which is a powerful pattern for dynamically controlling which channels are active.

**Example:**
```go
var ch chan int // nil channel
fmt.Println(ch == nil) // true
// ch <- 1    // blocks forever
// <-ch       // blocks forever
// close(ch)  // panic

// Useful: disable a select case by setting channel to nil
active := make(chan int, 1)
active <- 42
var disabled chan int // nil — this case is never selected
select {
case v := <-active: fmt.Println("active:", v)
case v := <-disabled: fmt.Println("never reached:", v)
}
```

**Real-time Scenario:** In a multiplexer that merges input streams, setting a channel to nil after it closes prevents the select from busy-looping on zero-value receives from that closed source.

---

### Q17: How do you create a channel of channels?

**Answer:** A channel of channels (`chan chan T`) is used for request-response communication. The requester creates a response channel, sends it along with the request through the main channel, and waits for a reply. This gives each requester a private reply channel, implementing the Go equivalent of a callback or future/promise pattern.

**Example:**
```go
type Request struct {
    Query    string
    RespChan chan string
}

func server(reqCh <-chan Request) {
    for req := range reqCh {
        req.RespChan <- "Result for: " + req.Query
    }
}

func main() {
    reqCh := make(chan Request)
    go server(reqCh)
    respCh := make(chan string)
    reqCh <- Request{Query: "user:123", RespChan: respCh}
    fmt.Println(<-respCh) // "Result for: user:123"
    close(reqCh)
}
```

**Real-time Scenario:** In a database connection pool manager, worker goroutines send requests with private response channels to the pool manager, which assigns available connections and sends them back — ensuring each worker gets its own dedicated connection.

---

### Q18: What is `chan struct{}` and why is it used?

**Answer:** `chan struct{}` is a channel of empty structs, used purely for signaling without transmitting data. `struct{}` occupies zero bytes of memory, making it the most memory-efficient choice for signal-only channels. It clearly communicates intent — when you see `chan struct{}`, you know it's for synchronization, not data transfer. Common uses: done channels, cancellation signals, and semaphore implementations.

**Example:**
```go
func worker(done chan<- struct{}) {
    fmt.Println("Working...")
    time.Sleep(500 * time.Millisecond)
    done <- struct{}{} // signal completion, zero memory
}

func main() {
    done := make(chan struct{})
    go worker(done)
    <-done // wait for signal
}
```

**Real-time Scenario:** In a graceful shutdown system, a `chan struct{}` named `quit` is closed to broadcast a shutdown signal to all goroutines simultaneously — each listens with `case <-quit:` in their select loops.

---

### When to Use Channels vs Mutexes

| Use Channels When | Use Mutexes When |
|-------------------|------------------|
| Transferring ownership of data | Protecting shared state (cache, map) |
| Coordinating goroutines | Simple critical sections |
| Implementing pipelines | Performance-critical hot paths |
| Signaling (done, ready) | Internal struct field access |
| Fan-in/fan-out patterns | When channel overhead matters |

## Coding Problems

### Problem 1: Ping-Pong with Channels

**Difficulty:** Easy  
**Problem:** Implement a ping-pong game between two goroutines. Each sends a counter back and forth, incrementing it. Stop at 10.

**Concepts Tested:** Bidirectional communication, goroutine coordination  
**Expected Approach:**
```go
func main() {
    ping := make(chan int)
    pong := make(chan int)

    // Player 1: Ping
    go func() {
        for val := range ping {
            fmt.Println("Ping:", val)
            pong <- val + 1
        }
    }()

    // Player 2: Pong
    go func() {
        for val := range pong {
            fmt.Println("Pong:", val)
            if val >= 10 {
                close(ping)
                return
            }
            ping <- val + 1
        }
    }()

    ping <- 0 // Start the game
    time.Sleep(time.Second)
}
```

**Variations:** Extend to N players in a ring topology.  
**Real-world Relevance:** Request-response patterns, turn-based protocols.

---

### Problem 2: Generator Pattern (Fibonacci)

**Difficulty:** Easy-Medium  
**Problem:** Implement a Fibonacci number generator using channels that produces numbers on demand.

**Concepts Tested:** Generator pattern, lazy evaluation, channel lifecycle  
**Expected Approach:**
```go
func fibonacci(ctx context.Context) <-chan int {
    ch := make(chan int)
    go func() {
        defer close(ch)
        a, b := 0, 1
        for {
            select {
            case ch <- a:
                a, b = b, a+b
            case <-ctx.Done():
                return
            }
        }
    }()
    return ch
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    for n := range fibonacci(ctx) {
        fmt.Println(n)
        if n > 100 {
            break
        }
    }
}
```

**Variations:** Make it generic for any sequence. Add `Take(n)` helper.  
**Real-world Relevance:** Stream processing, lazy data sources, iterator pattern.

---

### Problem 3: Channel-Based Mutex

**Difficulty:** Medium  
**Problem:** Implement a mutex using only channels (no `sync` package).

**Concepts Tested:** Deep channel understanding, synchronization from first principles  
**Expected Approach:**
```go
type ChanMutex struct {
    ch chan struct{}
}

func NewChanMutex() *ChanMutex {
    m := &ChanMutex{ch: make(chan struct{}, 1)}
    m.ch <- struct{}{} // initially "unlocked"
    return m
}

func (m *ChanMutex) Lock() {
    <-m.ch // take the token
}

func (m *ChanMutex) Unlock() {
    m.ch <- struct{}{} // put the token back
}
```

**Key insight:** A buffered channel of size 1 acts as a binary semaphore.

**Variations:** Implement a read-write lock using channels. Implement a counting semaphore.  
**Real-world Relevance:** Understanding primitives from first principles, showing depth.

---

### Problem 4: Merge Sorted Channels

**Difficulty:** Medium-Hard  
**Problem:** Given N sorted channels (each producing values in ascending order), merge them into a single sorted output channel.

**Concepts Tested:** Channel coordination, merge algorithms, heap usage  
**Expected Approach:**
```go
type Item struct {
    Val int
    Ch  <-chan int
}

func mergeSorted(channels ...<-chan int) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        h := &MinHeap{}
        heap.Init(h)

        // Seed the heap with first value from each channel
        for _, ch := range channels {
            if val, ok := <-ch; ok {
                heap.Push(h, Item{Val: val, Ch: ch})
            }
        }

        for h.Len() > 0 {
            min := heap.Pop(h).(Item)
            out <- min.Val
            if val, ok := <-min.Ch; ok {
                heap.Push(h, Item{Val: val, Ch: min.Ch})
            }
        }
    }()
    return out
}
```

**Variations:** Handle channels closing at different times, add context cancellation.  
**Real-world Relevance:** Merging sorted streams from multiple database shards, merge-sort in distributed systems.

---

### Problem 5: Implement an Unbuffered Channel from Scratch

**Difficulty:** Hard (Staff-level)  
**Problem:** Implement a basic unbuffered channel using mutexes and condition variables (no actual Go channels).

**Concepts Tested:** Deep understanding of channel internals, synchronization  
**Expected Approach:**
```go
type MyChannel[T any] struct {
    mu       sync.Mutex
    cond     *sync.Cond
    value    T
    hasValue bool
    closed   bool
}

func NewMyChannel[T any]() *MyChannel[T] {
    ch := &MyChannel[T]{}
    ch.cond = sync.NewCond(&ch.mu)
    return ch
}

func (ch *MyChannel[T]) Send(v T) {
    ch.mu.Lock()
    defer ch.mu.Unlock()
    for ch.hasValue && !ch.closed {
        ch.cond.Wait()
    }
    if ch.closed {
        panic("send on closed channel")
    }
    ch.value = v
    ch.hasValue = true
    ch.cond.Broadcast()
}

func (ch *MyChannel[T]) Receive() (T, bool) {
    ch.mu.Lock()
    defer ch.mu.Unlock()
    for !ch.hasValue && !ch.closed {
        ch.cond.Wait()
    }
    if !ch.hasValue && ch.closed {
        var zero T
        return zero, false
    }
    v := ch.value
    ch.hasValue = false
    ch.cond.Broadcast()
    return v, true
}
```

**Variations:** Add buffering support, add select-like multiplexing.  
**Real-world Relevance:** Understanding Go runtime internals, systems programming interviews.

---

## Common Mistakes

| Mistake | Consequence | Fix |
|---------|-------------|-----|
| Sending on a closed channel | Panic | Only sender should close; use sync.Once if multiple senders |
| Closing a channel from receiver | Panic if sender still active | Convention: **sender closes** |
| Closing a channel multiple times | Panic | Use `sync.Once` for close |
| Not closing channels | Goroutine leaks (receivers block forever) | Always close when done sending |
| Deadlock from single-goroutine unbuffered ops | Program hangs | Use separate goroutine or buffered channel |
| Using channels when mutex is simpler | Unnecessary complexity, worse performance | Choose the right tool |
| Not draining channels on shutdown | Goroutine leaks | Drain or use context cancellation |
| Ignoring `ok` from receive | Processing zero values as real data | Always check `ok` when needed |

---

# 3. BUFFERED VS UNBUFFERED CHANNELS

## Concept Overview

| Property | Unbuffered (`make(chan T)`) | Buffered (`make(chan T, N)`) |
|----------|---------------------------|----------------------------|
| Synchronization | Synchronous — rendezvous point | Asynchronous up to buffer size |
| Send blocks when | No receiver is waiting | Buffer is full |
| Receive blocks when | No sender is waiting | Buffer is empty |
| Backpressure | Immediate | Delayed until buffer fills |
| Use case | Synchronization, handoff | Decoupling, batching, smoothing bursts |
| Happens-before | Send happens-before corresponding receive | Kth receive happens-before (K+C)th send (C = capacity) |

**Choosing buffer size:**
- **0 (unbuffered):** When you need synchronization guarantees (sender knows receiver got it).
- **1:** Common for signaling ("fire and forget" with one pending item).
- **N (known):** When you can calculate the max in-flight items.
- **len(producers):** When all producers send exactly once.
- **Large N:** Smoothing bursts, but hides problems — prefer fixing the consumer.

## Why Interviewers Ask It

- Buffer size affects **correctness**, not just performance.
- Choosing wrong buffer size causes deadlocks or goroutine leaks.
- Reveals understanding of backpressure and flow control.
- Tests ability to reason about concurrent system behavior.

## Common Interview Questions

### Q1: What is the difference between `make(chan int)` and `make(chan int, 1)`?

**Answer:** `make(chan int)` creates an unbuffered channel where every send blocks until a receiver is ready, and every receive blocks until a sender is ready — providing synchronous communication. `make(chan int, 1)` creates a buffered channel with capacity 1, where one send can proceed without a receiver (the value is stored in the buffer). With an unbuffered channel, the sender and receiver rendezvous at the exact same point in time. With a buffer of 1, the sender can "fire and forget" one value.

**Example:**
```go
func main() {
    // Unbuffered: send blocks until someone receives
    unbuf := make(chan int)
    go func() {
        time.Sleep(500 * time.Millisecond)
        fmt.Println("Unbuffered received:", <-unbuf)
    }()
    unbuf <- 1 // blocks here until receiver is ready

    // Buffered(1): send doesn't block if buffer has space
    buf := make(chan int, 1)
    buf <- 2 // doesn't block — stored in buffer
    fmt.Println("Buffered received:", <-buf)
}
```

**Real-time Scenario:** In an event notification system, an unbuffered channel ensures the publisher confirms delivery to a subscriber before continuing, while a buffered channel allows the publisher to queue events and continue without waiting.

---

### Q2: When would you use a buffered channel vs unbuffered?

**Answer:** Use unbuffered channels when you need synchronous handoff — both sender and receiver must be ready at the same time, providing strong synchronization guarantees. Use buffered channels when you want to decouple sender and receiver speeds, absorb temporary bursts, or implement patterns like semaphores and worker pools. If you're unsure, start with unbuffered and only add a buffer when you have a specific performance or design reason.

**Example:**
```go
// Unbuffered: synchronization guarantee
done := make(chan struct{})
go func() {
    fmt.Println("Work done")
    done <- struct{}{} // blocks until main receives
}()
<-done // guaranteed work is complete

// Buffered: absorb bursts in a worker pool
jobs := make(chan int, 100) // buffer 100 jobs
for w := 0; w < 3; w++ {
    go func() { for j := range jobs { process(j) } }()
}
```

**Real-time Scenario:** In a web server, use a buffered channel as a job queue between HTTP handlers (fast producers) and database writers (slow consumers), allowing the server to handle traffic bursts without blocking request goroutines.

---

### Q3: What happens when a buffered channel is full?

**Answer:** When a buffered channel is full (`len(ch) == cap(ch)`), any subsequent send operation blocks the sending goroutine until a receiver removes a value from the buffer, freeing up space. The sender is placed on the channel's `sendq` wait list and is parked by the scheduler. This blocking behavior creates natural backpressure — fast producers are automatically slowed down to match consumer speed.

**Example:**
```go
func main() {
    ch := make(chan int, 3)
    go func() {
        for i := 0; i < 6; i++ {
            fmt.Printf("Sending %d (buffer: %d/%d)\n", i, len(ch), cap(ch))
            ch <- i // blocks when buffer is full (3/3)
            fmt.Printf("Sent %d\n", i)
        }
        close(ch)
    }()
    time.Sleep(1 * time.Second) // let buffer fill up
    for v := range ch {
        fmt.Println("Received:", v)
        time.Sleep(200 * time.Millisecond)
    }
}
```

**Real-time Scenario:** In a log aggregation pipeline, a buffered channel with capacity 10,000 absorbs log bursts, but when the buffer fills (e.g., disk I/O is slow), the sending goroutines slow down, applying backpressure up to the log source.

---

### Q4: How do you choose the right buffer size?

**Answer:** Start with unbuffered (0) unless you have a clear reason for buffering. For worker pools, the buffer size often equals the number of workers or expected burst size. For producer-consumer patterns, size the buffer based on the difference in production and consumption rates during peak load. A buffer of 1 is useful for hand-off patterns. Large buffers waste memory and can mask design problems (hiding backpressure signals). Profile with real traffic and adjust — there's no universal formula.

**Example:**
```go
numWorkers := 5
burstSize := 20

// Buffer = burst size to absorb temporary spikes
jobs := make(chan int, burstSize)
var wg sync.WaitGroup
for w := 0; w < numWorkers; w++ {
    wg.Add(1)
    go func(id int) {
        defer wg.Done()
        for j := range jobs { process(j) }
    }(w)
}
for j := 0; j < burstSize; j++ {
    jobs <- j // none block because buffer = burstSize
}
close(jobs)
wg.Wait()
```

**Real-time Scenario:** In a metrics collection system that samples 1000 metrics/sec but flushes every 10 seconds, a buffer of 10,000 ensures no samples are dropped during flush pauses without over-allocating memory.

---

### Q5: Can a buffered channel of size 1 replace a mutex?

**Answer:** Yes, a buffered channel of size 1 can act as a binary semaphore (mutex). You initialize it with one value, and acquiring the "lock" means receiving from the channel (blocking if empty), while releasing means sending a value back. This works but is slower than `sync.Mutex` (roughly 2-5x) because of channel overhead. The channel-as-mutex pattern is useful when you want to combine locking with `select` (e.g., try-lock with a `default` case).

**Example:**
```go
type ChanMutex struct {
    ch chan struct{}
}

func NewChanMutex() *ChanMutex {
    m := &ChanMutex{ch: make(chan struct{}, 1)}
    m.ch <- struct{}{} // "unlocked" state
    return m
}

func (m *ChanMutex) Lock()   { <-m.ch }             // acquire
func (m *ChanMutex) Unlock() { m.ch <- struct{}{} }  // release

// Try-lock: only possible with channel-based mutex
func (m *ChanMutex) TryLock() bool {
    select {
    case <-m.ch: return true
    default: return false
    }
}
```

**Real-time Scenario:** In a resource manager where you want a "try-lock" that falls back immediately if the lock isn't available, using a channel mutex with `select + default` provides a non-blocking lock attempt that `sync.Mutex` doesn't natively support.

---

### Q6: What is the "happens-before" guarantee of buffered vs unbuffered channels?

**Answer:** For unbuffered channels, the receive happens before the send completes — the sender is guaranteed that the receiver has executed by the time the send unblocks. For buffered channels, the k-th receive happens before the (k+N)-th send completes, where N is the buffer capacity. Unbuffered channels provide the strongest synchronization: the send and receive are a rendezvous point. Buffered channels decouple the two operations.

**Example:**
```go
// Unbuffered: receive happens-before send completes
ch := make(chan int)
done := false
go func() {
    done = true // guaranteed visible after <-ch
    ch <- 1
}()
<-ch
fmt.Println("done is guaranteed true:", done) // true

// Buffered: send completes when value enters buffer
bch := make(chan int, 1)
flag := false
go func() {
    flag = true
    bch <- 1 // completes immediately (buffer has space)
}()
<-bch
// flag is likely true but the guarantee is weaker
```

**Real-time Scenario:** In a service initialization sequence, using an unbuffered channel to signal "database is ready" guarantees that the connection pool is fully initialized before the HTTP server starts accepting requests.

---

### Q7: What is the difference between `len(ch)` and `cap(ch)`?

**Answer:** `len(ch)` returns the number of elements currently queued in the channel's buffer (sent but not yet received). `cap(ch)` returns the total buffer capacity as specified in `make()`. For unbuffered channels, both return 0. Note that `len(ch)` is a snapshot and can be stale immediately — don't use it for synchronization decisions. It's primarily useful for monitoring and debugging (e.g., tracking queue depth in metrics).

**Example:**
```go
ch := make(chan string, 5)
ch <- "a"
ch <- "b"
ch <- "c"

fmt.Println("len:", len(ch)) // 3 (items in buffer)
fmt.Println("cap:", cap(ch)) // 5 (total capacity)

<-ch // consume one
fmt.Println("len after receive:", len(ch)) // 2

uch := make(chan int)
fmt.Println("unbuffered len:", len(uch)) // 0
fmt.Println("unbuffered cap:", cap(uch)) // 0
```

**Real-time Scenario:** In a monitoring dashboard, periodically recording `len(jobsCh)` vs `cap(jobsCh)` as Prometheus metrics reveals queue saturation — when `len` approaches `cap`, it signals that workers can't keep up.

---

### Q8: How does buffer size affect backpressure?

**Answer:** Buffer size directly controls how much work can be queued before the producer is forced to slow down. A larger buffer absorbs more bursts but delays backpressure signals — the producer doesn't know the consumer is falling behind until the buffer is completely full. A smaller buffer provides immediate backpressure, forcing the producer to match consumer pace. Zero buffer means instant backpressure (synchronous). Oversized buffers can mask performance problems and increase latency.

**Example:**
```go
// Small buffer: backpressure kicks in quickly
ch := make(chan int, 2)
go func() {
    for i := 0; ; i++ {
        start := time.Now()
        ch <- i
        if elapsed := time.Since(start); elapsed > time.Millisecond {
            fmt.Printf("Send %d blocked for %v (backpressure!)\n", i, elapsed)
        }
    }
}()
// Slow consumer
for i := 0; i < 10; i++ {
    time.Sleep(100 * time.Millisecond)
    fmt.Println("Consumed:", <-ch)
}
```

**Real-time Scenario:** In an API gateway with a request queue, a buffer of 100 means 100 requests can queue before the gateway starts rejecting traffic with 503 errors, preventing downstream services from being overwhelmed.

---

### Q9: What is an unbounded channel and how would you implement one?

**Answer:** An unbounded channel is a channel with no capacity limit — sends never block. Go doesn't provide this natively because unbounded buffering can lead to unbounded memory growth. You implement one using a goroutine that moves values from an input channel to an output channel via an intermediate dynamically-growing slice. Use with extreme caution — if the consumer can't keep up, memory will grow without limit.

**Example:**
```go
func unboundedChan[T any](in <-chan T) <-chan T {
    out := make(chan T)
    go func() {
        var buf []T
        defer close(out)
        for {
            if len(buf) == 0 {
                v, ok := <-in
                if !ok { return }
                buf = append(buf, v)
            }
            select {
            case v, ok := <-in:
                if !ok {
                    for _, b := range buf { out <- b }
                    return
                }
                buf = append(buf, v)
            case out <- buf[0]:
                buf = buf[1:]
            }
        }
    }()
    return out
}
```

**Real-time Scenario:** In an event sourcing system, an unbounded channel buffers events during snapshot creation (when the consumer pauses), but you must monitor buffer size to trigger alerts before memory exhaustion occurs.

---

### Q10: What are the memory implications of large channel buffers?

**Answer:** A buffered channel pre-allocates memory for the entire buffer capacity at creation time. For `make(chan T, N)`, the runtime allocates `N * sizeof(T)` bytes plus `hchan` overhead. If `T` is a large struct, even a modest buffer can consume significant memory. Values in the buffer are not garbage collected until received. Channels of pointers are more memory-friendly since only pointer-sized slots are allocated.

**Example:**
```go
type LargePayload struct {
    Data [4096]byte // 4 KB per element
}

// This allocates 4096 * 1000 = ~4 MB for buffer alone
ch := make(chan LargePayload, 1000)

// Better: use pointers — only 8 bytes * 1000 = 8 KB buffer
chPtr := make(chan *LargePayload, 1000)
// Actual payload memory managed separately (e.g., sync.Pool)
```

**Real-time Scenario:** In a video processing pipeline using `chan [1920*1080*3]byte` (6 MB per frame), a buffer of 30 frames would consume ~180 MB. Using `chan *Frame` with pointer-sized buffer slots and a `sync.Pool` for frame reuse drastically reduces memory footprint.

---

## Coding Problems

### Problem 1: Deadlock Identification

**Difficulty:** Easy  
**Problem:** Identify which will deadlock and explain why:

```go
// A
ch := make(chan int)
ch <- 1
fmt.Println(<-ch)

// B
ch := make(chan int, 1)
ch <- 1
fmt.Println(<-ch)

// C
ch := make(chan int)
go func() { ch <- 1 }()
fmt.Println(<-ch)

// D
ch := make(chan int, 1)
ch <- 1
ch <- 2 // will this deadlock?
fmt.Println(<-ch)
```

**Answer:**
- **A: DEADLOCKS.** Unbuffered — `ch <- 1` blocks because no receiver exists yet.
- **B: Works.** Buffered — `ch <- 1` succeeds (buffer has space), then `<-ch` reads it.
- **C: Works.** Separate goroutine sends while main goroutine receives.
- **D: DEADLOCKS.** Buffer size 1 — first send succeeds, second blocks (buffer full, no receiver).

---

### Problem 2: Bounded Queue with Try Operations

**Difficulty:** Medium  
**Problem:** Implement a bounded queue supporting:
- `Enqueue(v)` — blocks when full
- `Dequeue()` — blocks when empty
- `TryEnqueue(v)` — non-blocking, returns false if full
- `TryDequeue()` — non-blocking, returns zero value and false if empty

**Expected Approach:**
```go
type BoundedQueue[T any] struct {
    ch chan T
}

func NewBoundedQueue[T any](capacity int) *BoundedQueue[T] {
    return &BoundedQueue[T]{ch: make(chan T, capacity)}
}

func (q *BoundedQueue[T]) Enqueue(v T)    { q.ch <- v }
func (q *BoundedQueue[T]) Dequeue() T     { return <-q.ch }

func (q *BoundedQueue[T]) TryEnqueue(v T) bool {
    select {
    case q.ch <- v:
        return true
    default:
        return false
    }
}

func (q *BoundedQueue[T]) TryDequeue() (T, bool) {
    select {
    case v := <-q.ch:
        return v, true
    default:
        var zero T
        return zero, false
    }
}
```

**Variations:** Add timeout variants (`EnqueueWithTimeout`, `DequeueWithTimeout`).

---

### Problem 3: Unbounded Channel

**Difficulty:** Medium-Hard  
**Problem:** Implement an unbounded channel that never blocks on send.

**Concepts Tested:** Channel bridging, internal buffering  
**Expected Approach:**
```go
func Unbounded[T any]() (chan<- T, <-chan T) {
    in := make(chan T)
    out := make(chan T)

    go func() {
        var buf []T
        defer close(out)

        for {
            if len(buf) == 0 {
                val, ok := <-in
                if !ok {
                    return
                }
                buf = append(buf, val)
            }

            select {
            case val, ok := <-in:
                if !ok {
                    // Drain buffer
                    for _, v := range buf {
                        out <- v
                    }
                    return
                }
                buf = append(buf, val)
            case out <- buf[0]:
                buf = buf[1:]
            }
        }
    }()
    return in, out
}
```

**Variations:** Add memory limit, add monitoring (buffer depth metric).  
**Real-world Relevance:** Event bus implementations, log aggregation buffers.

---

### Problem 4: Buffer Size Impact Analysis

**Difficulty:** Hard  
**Problem:** A producer generates items every 1ms. A consumer processes each in 10ms. Analyze:
1. What buffer size prevents the producer from ever blocking?
2. What happens with buffer sizes 0, 1, 10, 100?
3. Design a system that handles this rate mismatch.

**Expected Analysis:**
1. Over time, no finite buffer prevents blocking. Producer rate (1000/s) >> consumer rate (100/s). Buffer only absorbs bursts.
2. Buffer 0: Producer blocks 90% of the time. Buffer 1: Marginal improvement. Buffer 10: Absorbs 10ms of burst. Buffer 100: Absorbs 100ms.
3. Solution: Multiple consumers. Need at least 10 consumers to keep up (each processing 100/s, total 1000/s).

**Real-world Relevance:** Message queue sizing, Kafka partition consumers, load balancing.

---

## Common Mistakes

1. Using unbuffered where buffered would prevent deadlocks.
2. Using large buffers to "hide" concurrency problems — buffer fills eventually.
3. Not understanding that buffer size affects **correctness**, not just performance.
4. Using `len(ch)` for synchronization decisions (race condition — value changes between check and use).
5. Assuming buffered channels provide any guarantee beyond FIFO ordering.

---

# 4. SELECT STATEMENT

## Concept Overview

`select` lets a goroutine wait on multiple channel operations simultaneously. It is the **multiplexer** for channels.

**Key rules:**
- If multiple cases are ready, one is chosen **at random** (uniform pseudorandom).
- If no case is ready and there is a `default`, the `default` runs immediately (non-blocking).
- If no case is ready and there is no `default`, the select blocks until one is ready.
- A nil channel in a select case is **never ready** (effectively disables that case).
- `select {}` blocks forever (useful for keeping main alive or parking goroutines).

```go
select {
case v := <-ch1:
    // received from ch1
case ch2 <- value:
    // sent to ch2
case <-time.After(5 * time.Second):
    // timeout
default:
    // non-blocking: no channel ready
}
```

## Why Interviewers Ask It

- Essential for real-world concurrent Go code.
- Used in timeouts, cancellation, multiplexing, non-blocking operations.
- Tests understanding of channel semantics at a deeper level.
- Reveals awareness of common pitfalls (time.After leak, priority misconception).

## Common Interview Questions

### Q1: How does `select` choose between multiple ready cases?

**Answer:** When multiple cases are ready simultaneously, Go's `select` chooses one **uniformly at random**. This is a deliberate design decision to prevent starvation — no case gets priority over another. The runtime shuffles the case list and checks them in random order. This means you cannot rely on case ordering for priority; if you need priority, use nested selects.

**Example:**
```go
ch1 := make(chan string, 1)
ch2 := make(chan string, 1)
ch1 <- "one"
ch2 <- "two"

select {
case v := <-ch1:
    fmt.Println(v) // might print "one"
case v := <-ch2:
    fmt.Println(v) // might print "two"
}
// Result is non-deterministic when both are ready
```

**Real-time Scenario:** A message broker reads from multiple topic channels using select — random selection ensures fair consumption across topics even when all have messages available.

---

### Q2: What does an empty `select {}` do?

**Answer:** An empty `select {}` blocks the current goroutine **forever**. It has no cases to evaluate, so it can never proceed. This is useful for keeping the main goroutine alive when all work is done in background goroutines, or for blocking a goroutine that should never exit (like a long-running server).

**Example:**
```go
func main() {
    go startHTTPServer()
    go startGRPCServer()
    go startMetricsServer()
    select {} // block main forever; servers run in background
}
```

**Real-time Scenario:** A microservice's `main()` starts HTTP and gRPC servers in goroutines, then uses `select{}` to keep the process alive. In production, you'd typically use `signal.NotifyContext` instead for graceful shutdown.

---

### Q3: How do you implement a non-blocking channel send/receive?

**Answer:** Use `select` with a `default` case. If the channel operation would block (channel full for send, empty for receive), the `default` case executes immediately instead. This turns blocking operations into polling operations.

**Example:**
```go
// Non-blocking receive
select {
case msg := <-ch:
    fmt.Println("received:", msg)
default:
    fmt.Println("no message available")
}

// Non-blocking send
select {
case ch <- value:
    fmt.Println("sent")
default:
    fmt.Println("channel full, dropping")
}
```

**Real-time Scenario:** A metrics collector tries to send to a channel but drops the data point if the buffer is full, ensuring the hot path (HTTP handler) is never blocked by slow metric consumers.

---

### Q4: How do you implement a timeout with select?

**Answer:** Use `time.After(duration)` which returns a channel that receives after the specified duration, or better, use `context.WithTimeout` for composable cancellation. The `time.After` approach is simpler but has a memory leak risk in loops (see Q11). For production code, prefer contexts.

**Example:**
```go
// Using time.After
select {
case result := <-ch:
    fmt.Println("got result:", result)
case <-time.After(5 * time.Second):
    fmt.Println("timeout!")
}

// Using context (preferred)
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
select {
case result := <-ch:
    fmt.Println("got result:", result)
case <-ctx.Done():
    fmt.Println("timeout:", ctx.Err())
}
```

**Real-time Scenario:** An API gateway gives each upstream service 3 seconds to respond. If the service is slow, the timeout fires and the gateway returns a 504 to the client.

---

### Q5: What happens with nil channels in select?

**Answer:** A nil channel in a `select` case is **never selected** — the case is effectively disabled. This is a powerful pattern for dynamically enabling/disabling cases at runtime. Set a channel variable to `nil` to stop selecting on it, and restore it to re-enable it.

**Example:**
```go
func merge(ch1, ch2 <-chan int) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        for ch1 != nil || ch2 != nil {
            select {
            case v, ok := <-ch1:
                if !ok { ch1 = nil; continue }
                out <- v
            case v, ok := <-ch2:
                if !ok { ch2 = nil; continue }
                out <- v
            }
        }
    }()
    return out
}
```

**Real-time Scenario:** A stream processor merges two input streams. When one stream ends (channel closed), setting it to nil prevents the select from busy-looping on the closed channel while the other stream continues.

---

### Q6: How do you implement priority select in Go?

**Answer:** Go's `select` doesn't support priority directly. To prioritize one channel over another, use **nested selects**: first try the high-priority channel with a `default`, and only if it's empty, use a regular select on both. This pattern checks the priority channel first on every iteration.

**Example:**
```go
for {
    // Always check high-priority first
    select {
    case msg := <-highPriority:
        handleUrgent(msg)
        continue
    default:
    }
    // Then check both
    select {
    case msg := <-highPriority:
        handleUrgent(msg)
    case msg := <-normalPriority:
        handleNormal(msg)
    case <-ctx.Done():
        return
    }
}
```

**Real-time Scenario:** A job scheduler always processes cancellation requests (high priority) before new job submissions (normal priority) to ensure cancelled jobs don't start executing.

---

### Q7: Can you use select with non-channel operations?

**Answer:** No, `select` only works with channel send and receive operations. You cannot use it with mutex locks, file I/O, network calls, or any other operations. For non-channel async operations, wrap them in a goroutine that sends the result to a channel, then select on that channel.

**Example:**
```go
// WRONG: select can't use non-channel operations
// select {
// case result := doHTTPCall():  // compile error
// }

// CORRECT: wrap in goroutine + channel
resultCh := make(chan Result, 1)
go func() { resultCh <- doHTTPCall() }()
select {
case result := <-resultCh:
    handle(result)
case <-ctx.Done():
    return ctx.Err()
}
```

**Real-time Scenario:** A database query needs a timeout — wrap the blocking `db.Query()` call in a goroutine that sends to a channel, then select between the result channel and a timeout.

---

### Q8: What is the performance of select with many cases?

**Answer:** `select` is O(N) where N is the number of cases. The runtime shuffles the cases randomly, then checks each one sequentially. For most use cases (2-10 cases), this is negligible. For very high-frequency loops with many cases, consider restructuring to reduce the case count or using goroutine-per-channel fan-in.

**Example:**
```go
// O(N) — runtime shuffles and scans all cases each iteration
select {
case v := <-ch1: handle(v)
case v := <-ch2: handle(v)
case v := <-ch3: handle(v)
// ... 50 more cases — each select iteration checks all 50+
}

// Better for many channels: goroutine-per-channel fan-in
merged := fanIn(ch1, ch2, ch3, ..., ch50)
for v := range merged { handle(v) }
```

**Real-time Scenario:** A monitoring system originally used a select with 100 cases for 100 metrics sources. After profiling showed select overhead, they switched to fan-in, reducing CPU usage by 40%.

---

### Q9: How do you use select for graceful shutdown?

**Answer:** Include `case <-ctx.Done()` in every select statement within long-running goroutines. When the context is cancelled (via signal handler or parent), all goroutines exit their loops. Combined with WaitGroup, this ensures clean shutdown with in-flight work completion.

**Example:**
```go
func worker(ctx context.Context, jobs <-chan Job) {
    for {
        select {
        case job, ok := <-jobs:
            if !ok { return }
            process(job)
        case <-ctx.Done():
            log.Println("shutting down worker")
            return
        }
    }
}
```

**Real-time Scenario:** A Kubernetes pod receives SIGTERM, triggering context cancellation. All worker goroutines exit their select loops, finish in-flight requests, and the pod shuts down within the 30-second grace period.

---

### Q10: What is the "done channel" pattern?

**Answer:** The done channel pattern uses a `chan struct{}` that is closed to signal cancellation or completion. All goroutines listen on this channel with `select`. When `close(done)` is called, all listeners receive the zero value immediately. This was the standard pre-context pattern and is essentially what `context.Context` does internally.

**Example:**
```go
func worker(done <-chan struct{}, jobs <-chan Job) {
    for {
        select {
        case <-done:
            return
        case job := <-jobs:
            process(job)
        }
    }
}

// Cancellation
done := make(chan struct{})
go worker(done, jobs)
close(done) // signals ALL listeners instantly
```

**Real-time Scenario:** Before Go 1.7 (which introduced `context`), a web server used done channels to propagate request cancellation to all downstream goroutines handling a single request.

---

### Q11: What is the memory leak with `time.After` in a for-select loop?

**Answer:** `time.After` creates a new `time.Timer` on each call that isn't garbage collected until it fires. In a tight loop, thousands of timers accumulate, leaking memory. The fix is to create a single `time.Timer` with `time.NewTimer()` outside the loop and reset it with `timer.Reset()` on each iteration.

**Example:**
```go
// LEAKY: new timer every iteration, old ones live until they fire
for {
    select {
    case msg := <-ch:
        handle(msg)
    case <-time.After(5 * time.Second): // new timer each loop!
        handleTimeout()
    }
}

// FIXED: reuse a single timer
timer := time.NewTimer(5 * time.Second)
defer timer.Stop()
for {
    select {
    case msg := <-ch:
        if !timer.Stop() { <-timer.C }
        timer.Reset(5 * time.Second)
        handle(msg)
    case <-timer.C:
        handleTimeout()
        timer.Reset(5 * time.Second)
    }
}
```

**Real-time Scenario:** A message consumer using `time.After` in a high-throughput loop (10K msg/sec) leaked 500MB of memory over 24 hours. Switching to `time.NewTimer` + `Reset` fixed it completely.

---

### Q12: How do you disable a select case at runtime?

**Answer:** Set the channel variable to `nil`. A nil channel is never ready, so its select case is effectively disabled. This is the idiomatic Go way to dynamically enable/disable cases without restructuring the select. Restore the original channel reference to re-enable it.

**Example:**
```go
func process(input <-chan int, toggle <-chan bool) {
    active := input
    for {
        select {
        case v := <-active:
            fmt.Println("processing:", v)
        case enabled := <-toggle:
            if enabled {
                active = input // re-enable
            } else {
                active = nil   // disable — this case is never selected
            }
        }
    }
}
```

**Real-time Scenario:** A rate-limited API consumer disables the request channel when the rate limit is hit, re-enabling it after the reset window expires, without stopping other select cases.

---

## Coding Problems

### Problem 1: First Response Wins (Hedged Requests)

**Difficulty:** Easy-Medium  
**Problem:** Send a request to 3 replicas concurrently and return the first response. Cancel the others.

**Concepts Tested:** Select, context cancellation, first-wins pattern  
**Expected Approach:**
```go
func queryWithHedging(ctx context.Context, replicas []string) (Result, error) {
    ctx, cancel := context.WithCancel(ctx)
    defer cancel() // cancels losing goroutines

    results := make(chan Result, len(replicas))
    for _, replica := range replicas {
        go func(addr string) {
            if r, err := query(ctx, addr); err == nil {
                results <- r
            }
        }(replica)
    }

    select {
    case r := <-results:
        return r, nil
    case <-ctx.Done():
        return Result{}, ctx.Err()
    }
}
```

**Variations:** What if you need quorum (2 of 3 must agree)? Add staggered launch (send to 2nd replica only after 50ms delay).  
**Real-world Relevance:** Google's "tail at scale" technique, S3 hedged requests, Cassandra speculative execution.

---

### Problem 2: Heartbeat Monitor with Timeout

**Difficulty:** Medium  
**Problem:** Monitor heartbeats from a service. If no heartbeat within 5 seconds, declare dead. If heartbeat resumes, declare alive again.

**Expected Approach:**
```go
func monitorHeartbeat(heartbeat <-chan struct{}, status chan<- string) {
    timeout := 5 * time.Second
    timer := time.NewTimer(timeout)
    defer timer.Stop()

    alive := true
    for {
        select {
        case _, ok := <-heartbeat:
            if !ok {
                status <- "stopped"
                return
            }
            if !alive {
                status <- "alive"
                alive = true
            }
            if !timer.Stop() {
                select {
                case <-timer.C:
                default:
                }
            }
            timer.Reset(timeout)
        case <-timer.C:
            if alive {
                status <- "dead"
                alive = false
            }
            timer.Reset(timeout)
        }
    }
}
```

**Variations:** Multiple services, reconnection logic, health score (not just alive/dead).  
**Real-world Relevance:** Health checking, failure detection, circuit breakers, Kubernetes liveness probes.

---

### Problem 3: Priority Channel (No Starvation)

**Difficulty:** Medium-Hard  
**Problem:** Process messages from high-priority and low-priority channels. Always prefer high-priority, but don't completely starve low-priority.

**Concepts Tested:** Select, priority handling, fairness  
**Expected Approach:**
```go
func processPriority(high, low <-chan Message, done <-chan struct{}) {
    for {
        // First: try high-priority only
        select {
        case msg := <-high:
            handle(msg)
            continue
        case <-done:
            return
        default:
        }

        // Then: either channel
        select {
        case msg := <-high:
            handle(msg)
        case msg := <-low:
            handle(msg)
        case <-done:
            return
        }
    }
}
```

**Why this works:** The first select drains all pending high-priority messages (non-blocking). If none are pending, the second select waits on both — giving low-priority a chance.

**Variations:** Multiple priority levels, weighted fair queuing, add per-priority metrics.  
**Real-world Relevance:** QoS in message processing, network packet scheduling, request prioritization.

---

### Problem 4: Dynamic Fan-In with Select

**Difficulty:** Hard  
**Problem:** Implement fan-in where channels can be added and removed at runtime.

**Concepts Tested:** `reflect.Select` for dynamic cases, goroutine-per-channel alternative  
**Expected Approach (goroutine-per-channel — idiomatic):**
```go
type DynamicFanIn[T any] struct {
    out    chan T
    add    chan <-chan T
    remove chan <-chan T
}

func NewDynamicFanIn[T any]() *DynamicFanIn[T] {
    d := &DynamicFanIn[T]{
        out:    make(chan T),
        add:    make(chan (<-chan T)),
        remove: make(chan (<-chan T)),
    }
    go d.run()
    return d
}

func (d *DynamicFanIn[T]) run() {
    var wg sync.WaitGroup
    cancels := make(map[<-chan T]context.CancelFunc)

    for {
        select {
        case ch := <-d.add:
            ctx, cancel := context.WithCancel(context.Background())
            cancels[ch] = cancel
            wg.Add(1)
            go func(c <-chan T, ctx context.Context) {
                defer wg.Done()
                for {
                    select {
                    case v, ok := <-c:
                        if !ok {
                            return
                        }
                        d.out <- v
                    case <-ctx.Done():
                        return
                    }
                }
            }(ch, ctx)

        case ch := <-d.remove:
            if cancel, ok := cancels[ch]; ok {
                cancel()
                delete(cancels, ch)
            }
        }
    }
}
```

**Alternative:** Use `reflect.Select` for truly dynamic select cases (useful when channels change frequently).  
**Real-world Relevance:** Dynamic service discovery, plugin systems, multi-source log aggregation.

---

### Problem 5: Custom Ticker

**Difficulty:** Medium  
**Problem:** Implement a ticker that sends ticks at a specified interval with ability to stop, reset, and add jitter.

**Expected Approach:**
```go
type CustomTicker struct {
    C      <-chan time.Time
    c      chan time.Time
    done   chan struct{}
    mu     sync.Mutex
    interval time.Duration
    jitter   time.Duration
}

func NewCustomTicker(interval, jitter time.Duration) *CustomTicker {
    c := make(chan time.Time, 1)
    t := &CustomTicker{
        C:        c,
        c:        c,
        done:     make(chan struct{}),
        interval: interval,
        jitter:   jitter,
    }
    go t.run()
    return t
}

func (t *CustomTicker) run() {
    for {
        jitter := time.Duration(rand.Int63n(int64(t.jitter)))
        select {
        case <-time.After(t.interval + jitter):
            select {
            case t.c <- time.Now():
            default: // drop if nobody is listening
            }
        case <-t.done:
            return
        }
    }
}

func (t *CustomTicker) Stop() { close(t.done) }
```

**Variations:** Add drift compensation, add pause/resume.  
**Real-world Relevance:** Polling mechanisms, scheduled tasks, heartbeat senders.

---

## Common Mistakes

| Mistake | Consequence | Fix |
|---------|-------------|-----|
| `time.After` in a for-select loop | Memory leak — creates new timer each iteration | Use `time.NewTimer` + `Reset()` |
| Assuming select has priority ordering | Non-deterministic behavior | Use nested selects for priority |
| Forgetting `default` causes blocking | Unintended block when non-blocking was desired | Add `default` for non-blocking |
| Not handling `ctx.Done()` in select | Goroutine leak on cancellation | Always include `case <-ctx.Done()` |
| Creating channels in select cases | Confusion about which channel is used | Declare channels before select |
| Breaking out of select in loops | `break` only breaks out of select, not the loop | Use labeled breaks or return |

**Important: `time.After` memory leak:**
```go
// BAD — leaks a timer every iteration
for {
    select {
    case <-ch:
        handle()
    case <-time.After(5 * time.Second):
        handleTimeout()
    }
}

// GOOD — reuse timer
timer := time.NewTimer(5 * time.Second)
defer timer.Stop()
for {
    select {
    case <-ch:
        handle()
        if !timer.Stop() {
            <-timer.C
        }
        timer.Reset(5 * time.Second)
    case <-timer.C:
        handleTimeout()
        timer.Reset(5 * time.Second)
    }
}
```

---

# 5. WAITGROUPS

## Concept Overview

`sync.WaitGroup` provides a simple mechanism to wait for a collection of goroutines to finish. It maintains an internal counter.

**Methods:**
- `Add(delta int)` — Increments the counter by delta.
- `Done()` — Decrements the counter by 1 (equivalent to `Add(-1)`).
- `Wait()` — Blocks until the counter reaches 0.

**Rules:**
- Call `Add()` **before** launching the goroutine (not inside it).
- If the counter goes negative, it **panics**.
- `WaitGroup` must not be copied after first use (pass by pointer).
- A `WaitGroup` can be reused after `Wait()` returns.

## Why Interviewers Ask It

- Most basic synchronization primitive after channels.
- Tests understanding of goroutine lifecycle management.
- Common source of bugs when misused.
- Gateway question — if you can't use WaitGroup correctly, deeper questions won't go well.

## Common Interview Questions

### Q1: What happens if WaitGroup counter goes negative?

**Answer:** The program panics with `sync: negative WaitGroup counter`. This happens when `Done()` is called more times than `Add()`, indicating a logic error. The runtime panics immediately because a negative counter is always a bug — it means you're signaling completion for work that was never registered.

**Example:**
```go
var wg sync.WaitGroup
wg.Add(1)
wg.Done()
wg.Done() // panic: sync: negative WaitGroup counter
```

**Real-time Scenario:** A bug in a retry loop calls `wg.Done()` on each retry attempt instead of once after all retries complete, causing a negative counter panic when retries exceed the initial `Add()` count.

---

### Q2: Where should you call `wg.Add()` — before or inside the goroutine?

**Answer:** Always call `wg.Add()` **before** launching the goroutine, typically in the parent goroutine. If you call `Add()` inside the child goroutine, there's a race between `Add()` and `Wait()` — `Wait()` might return before `Add()` runs, causing the program to proceed prematurely. This is a subtle race condition the race detector can catch.

**Example:**
```go
var wg sync.WaitGroup

// CORRECT: Add before launching
for i := 0; i < 5; i++ {
    wg.Add(1)
    go func() {
        defer wg.Done()
        doWork()
    }()
}
wg.Wait()

// WRONG: Add inside goroutine — race with Wait()
for i := 0; i < 5; i++ {
    go func() {
        wg.Add(1) // race: Wait() might run before this
        defer wg.Done()
        doWork()
    }()
}
wg.Wait()
```

**Real-time Scenario:** A test spawns goroutines in a loop with `wg.Add(1)` inside each goroutine. The test passes locally but flakes in CI — `Wait()` returns early 1% of the time, causing assertion failures on incomplete results.

---

### Q3: Can you reuse a WaitGroup after `Wait()` returns?

**Answer:** Yes, a `WaitGroup` can be reused after `Wait()` returns, provided the counter has reached zero. You can call `Add()` again to start a new batch. However, you must not call `Add()` while a concurrent `Wait()` is still blocked — that races. In practice, it's cleaner to use a new WaitGroup per batch.

**Example:**
```go
var wg sync.WaitGroup
// Batch 1
wg.Add(2)
go func() { defer wg.Done(); processA() }()
go func() { defer wg.Done(); processB() }()
wg.Wait()

// Batch 2 — reusing same WaitGroup
wg.Add(3)
go func() { defer wg.Done(); processC() }()
go func() { defer wg.Done(); processD() }()
go func() { defer wg.Done(); processE() }()
wg.Wait()
```

**Real-time Scenario:** A batch processing system reuses a WaitGroup across multiple batches of database migrations, waiting for each batch to complete before starting the next.

---

### Q4: What is the difference between WaitGroup and errgroup?

**Answer:** `sync.WaitGroup` only waits for completion — it has no mechanism for error handling or cancellation. `errgroup.Group` (from `golang.org/x/sync/errgroup`) adds: (1) error collection — returns the first error from any goroutine, (2) context cancellation — cancels the context when any goroutine returns an error, (3) `SetLimit` — controls max concurrent goroutines. Use `errgroup` when you need to know if any goroutine failed.

**Example:**
```go
// WaitGroup: no error handling
var wg sync.WaitGroup
for _, url := range urls {
    wg.Add(1)
    go func() { defer wg.Done(); fetch(url) }() // error is lost
}
wg.Wait()

// errgroup: error handling + cancellation
g, ctx := errgroup.WithContext(ctx)
for _, url := range urls {
    g.Go(func() error { return fetch(ctx, url) })
}
if err := g.Wait(); err != nil {
    log.Fatal("fetch failed:", err)
}
```

**Real-time Scenario:** A health check system uses `errgroup` to query 10 dependencies in parallel — if any fails, the context cancels remaining checks and the first error is returned to the caller.

---

### Q5: How do you combine WaitGroup with error handling?

**Answer:** The idiomatic approach is to use `errgroup.Group` instead of manually combining WaitGroup with error channels. If you must use WaitGroup directly, pair it with a mutex-protected error slice or a buffered error channel. But `errgroup` is cleaner and well-tested.

**Example:**
```go
// Manual approach (avoid if possible)
var wg sync.WaitGroup
var mu sync.Mutex
var errs []error
for _, task := range tasks {
    wg.Add(1)
    go func() {
        defer wg.Done()
        if err := process(task); err != nil {
            mu.Lock()
            errs = append(errs, err)
            mu.Unlock()
        }
    }()
}
wg.Wait()

// Better: use errgroup
g, ctx := errgroup.WithContext(ctx)
for _, task := range tasks {
    g.Go(func() error { return process(ctx, task) })
}
return g.Wait()
```

**Real-time Scenario:** A data migration tool processes 10,000 records concurrently using `errgroup.SetLimit(50)`, collecting the first error and aborting the batch.

---

### Q6: Can you pass a WaitGroup by value?

**Answer:** Technically yes, but you must **never** do this — `WaitGroup` contains internal state (atomic counters) that gets copied, creating two independent counters. The copy's `Done()` won't affect the original's `Wait()`, causing deadlocks or premature returns. Always pass `*sync.WaitGroup` (pointer). The `go vet` tool detects this mistake.

**Example:**
```go
// WRONG: pass by value — wg copy is independent
func worker(wg sync.WaitGroup) { // copies the WaitGroup!
    defer wg.Done() // decrements the COPY, not the original
    doWork()
}

// CORRECT: pass by pointer
func worker(wg *sync.WaitGroup) {
    defer wg.Done()
    doWork()
}
```

**Real-time Scenario:** A junior developer passes WaitGroup by value to a helper function. The program deadlocks in production because `Wait()` on the original never returns — `go vet` would have caught this.

---

### Q7: How does WaitGroup work internally?

**Answer:** WaitGroup uses a 64-bit atomic value packed into a `state` field: the upper 32 bits store the counter, the lower 32 bits store the number of waiters. `Add()` atomically increments the counter. `Done()` decrements it. When the counter reaches zero, `Wait()` releases all blocked waiters via a runtime semaphore. This lock-free design makes it very efficient.

**Example:**
```go
// Conceptual internal structure:
type WaitGroup struct {
    state atomic.Uint64 // upper 32 bits: counter, lower 32 bits: waiter count
    sema  uint32        // runtime semaphore for blocking Wait()
}
// Add(n): atomic add n to counter bits
// Done(): Add(-1)
// Wait(): increment waiter count, then park on semaphore until counter == 0
```

**Real-time Scenario:** Understanding the internal atomic+semaphore design helps explain why WaitGroup is faster than channel-based synchronization for simple "wait for N things" patterns — no channel overhead or goroutine scheduling.

---

### Q8: When would you use a channel instead of WaitGroup?

**Answer:** Use a channel when you need to **collect results** from goroutines, not just wait for them. WaitGroup only synchronizes completion — it doesn't transfer data. If each goroutine produces a result you need, use channels (or `errgroup` + a shared slice with mutex). Channels also enable streaming results as they arrive rather than waiting for all to finish.

**Example:**
```go
// WaitGroup: just wait for completion
var wg sync.WaitGroup
for _, url := range urls {
    wg.Add(1)
    go func() { defer wg.Done(); fetch(url) }()
}
wg.Wait()

// Channel: collect results as they arrive
results := make(chan Result, len(urls))
for _, url := range urls {
    go func() { results <- fetch(url) }()
}
for range urls {
    r := <-results
    fmt.Println(r)
}
```

**Real-time Scenario:** A price comparison service queries 10 vendors — it uses channels to display prices as they arrive (sub-second UX) rather than WaitGroup which would wait for the slowest vendor before showing anything.

---

## Coding Problems

### Problem 1: Parallel URL Fetcher

**Difficulty:** Easy  
**Problem:** Fetch 10 URLs concurrently using WaitGroup. Collect all results and any errors.

**Expected Approach:**
```go
type FetchResult struct {
    URL   string
    Body  []byte
    Error error
}

func fetchAll(urls []string) []FetchResult {
    results := make([]FetchResult, len(urls))
    var wg sync.WaitGroup

    for i, url := range urls {
        wg.Add(1)
        go func(idx int, u string) {
            defer wg.Done()
            resp, err := http.Get(u)
            if err != nil {
                results[idx] = FetchResult{URL: u, Error: err}
                return
            }
            defer resp.Body.Close()
            body, err := io.ReadAll(resp.Body)
            results[idx] = FetchResult{URL: u, Body: body, Error: err}
        }(i, url)
    }

    wg.Wait()
    return results
}
```

**Key insight:** No mutex needed because each goroutine writes to a different index in the results slice.

**Variations:** Add timeout with context, add concurrency limit, use errgroup for first-error-cancellation.  
**Real-world Relevance:** API aggregation, web scraping, health checking multiple services.

---

### Problem 2: WaitGroup Bug Hunt

**Difficulty:** Medium  
**Problem:** Find and fix ALL bugs in this code:

```go
func process(items []int) {
    var wg sync.WaitGroup
    results := make([]int, 0)

    for i := 0; i < len(items); i++ {
        go func() {
            wg.Add(1)
            defer wg.Done()
            result := expensive(items[i])
            results = append(results, result)
        }()
    }
    wg.Wait()
    fmt.Println(results)
}
```

**Bugs:**
1. **`wg.Add(1)` inside goroutine** — races with `wg.Wait()`. If `Wait()` is called before any goroutine runs, it returns immediately.
2. **Closure captures `i`** — all goroutines see the same (final) `i`, and `items[i]` is out of bounds.
3. **`results = append(results, result)` without mutex** — data race on the slice.

**Fixed:**
```go
func process(items []int) {
    var wg sync.WaitGroup
    var mu sync.Mutex
    results := make([]int, 0, len(items))

    for i := 0; i < len(items); i++ {
        wg.Add(1) // before goroutine
        go func(idx int) { // pass i as parameter
            defer wg.Done()
            result := expensive(items[idx])
            mu.Lock()
            results = append(results, result)
            mu.Unlock()
        }(i)
    }
    wg.Wait()
    fmt.Println(results)
}
```

---

### Problem 3: Parallel Map-Reduce with Bounded Concurrency

**Difficulty:** Medium-Hard  
**Problem:** Implement parallel map-reduce:
- Map phase processes items concurrently with at most N goroutines.
- Reduce phase combines all results.
- If any map operation fails, stop early and return the error.

**Expected Approach:**
```go
func mapReduce[T, U, V any](
    items []T,
    mapper func(T) (U, error),
    reducer func([]U) V,
    concurrency int,
) (V, error) {
    g, ctx := errgroup.WithContext(context.Background())
    g.SetLimit(concurrency)

    results := make([]U, len(items))
    for i, item := range items {
        i, item := i, item
        g.Go(func() error {
            select {
            case <-ctx.Done():
                return ctx.Err()
            default:
            }
            r, err := mapper(item)
            if err != nil {
                return err
            }
            results[i] = r
            return nil
        })
    }

    if err := g.Wait(); err != nil {
        var zero V
        return zero, err
    }
    return reducer(results), nil
}
```

**Real-world Relevance:** Data processing pipelines, batch API calls, ETL jobs.

---

### Problem 4: WaitGroup vs Channel — Implement Both

**Difficulty:** Medium  
**Problem:** Implement "wait for N goroutines to complete" using:
1. `sync.WaitGroup`
2. A channel (no WaitGroup)

Compare trade-offs.

**Channel-based approach:**
```go
func waitWithChannel(n int, work func(id int)) {
    done := make(chan struct{}, n)
    for i := 0; i < n; i++ {
        go func(id int) {
            work(id)
            done <- struct{}{}
        }(i)
    }
    for i := 0; i < n; i++ {
        <-done
    }
}
```

**Trade-offs:**
| WaitGroup | Channel |
|-----------|---------|
| Simpler for "just wait" | Can carry results |
| No result collection | Natural for result aggregation |
| Reusable | One-shot per use |
| More efficient (no channel overhead) | More flexible |

---

## Common Mistakes

| Mistake | Consequence | Fix |
|---------|-------------|-----|
| `wg.Add()` inside goroutine | Race with `Wait()` — may return early | Call `Add()` before `go` |
| Passing WaitGroup by value | Copy of WaitGroup — `Done()` on copy doesn't affect original | Pass `*sync.WaitGroup` |
| Not calling `Done()` | `Wait()` hangs forever | Always use `defer wg.Done()` |
| Counter goes negative | Panic | Match `Add` and `Done` calls exactly |
| Not using `defer` for `Done()` | Panic in goroutine skips `Done()` | Always `defer wg.Done()` |
| Using WaitGroup when errgroup is better | No error handling, no cancellation | Use `errgroup.Group` for fallible work |

---

## Summary: Choosing the Right Primitive

| Need | Use |
|------|-----|
| Wait for goroutines | `sync.WaitGroup` |
| Wait + error handling | `errgroup.Group` |
| Wait + collect results | Channels |
| Wait + first error cancels all | `errgroup.WithContext` |
| Bounded concurrency | `errgroup.SetLimit(N)` or semaphore |
| Signaling (done, ready) | `chan struct{}` |
| Data transfer | Typed channels |
| Protecting shared state | `sync.Mutex` |
| Timeout/cancellation | `context.Context` |
