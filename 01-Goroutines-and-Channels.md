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

1. What is the difference between concurrency and parallelism in Go?
2. How does Go's goroutine scheduler work? Explain the G-M-P model.
3. What is the default stack size of a goroutine? How does it grow?
4. What happens if you launch 1 million goroutines?
5. How does Go handle goroutine preemption? What changed in Go 1.14?
6. What is GOMAXPROCS? When would you change it?
7. What is the difference between goroutines and OS threads?
8. Can a goroutine run on multiple OS threads during its lifetime?
9. What happens to a goroutine when it performs a blocking syscall?
10. How does the Go runtime handle goroutine starvation?
11. What is work stealing in the Go scheduler?
12. How do you debug goroutine leaks?
13. What is `runtime.Gosched()` and when would you use it?
14. What is `runtime.Goexit()` and how does it differ from `return`?
15. What happens if the main goroutine exits before other goroutines finish?
16. How does the GC interact with goroutines (stop-the-world pauses)?
17. Why doesn't Go have goroutine-local storage (like thread-local storage)?
18. How do you handle panics in goroutines?
19. What is `runtime.NumGoroutine()` and how would you use it for monitoring?
20. How do you profile goroutine usage in production with pprof?

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

1. What happens when you send on a nil channel?
2. What happens when you receive from a closed channel?
3. What happens when you send on a closed channel?
4. How do you check if a channel is closed? (You can't reliably — use `v, ok := <-ch`)
5. When should you close a channel? Who should close it? (**The sender closes.**)
6. What is the difference between `v := <-ch` and `v, ok := <-ch`?
7. Can you close a channel multiple times? (No — panic)
8. What is a directional channel and why use it? (API safety — prevents misuse)
9. How do channels work internally? (hchan struct, circular queue, sudog waitlists)
10. When should you use channels vs mutexes?
11. What is the performance difference between channels and mutexes? (Channels are slower due to scheduling overhead)
12. How do you implement a channel timeout?
13. How do you drain a channel?
14. What is the "comma ok" idiom for channels?
15. Can you range over a channel? (Yes — loops until channel is closed)
16. What is the zero-value of a channel? (nil)
17. How do you create a channel of channels? (For request-response patterns)
18. What is `chan struct{}` and why is it used? (Zero-memory signaling)

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

1. What is the difference between `make(chan int)` and `make(chan int, 1)`?
2. When would you use a buffered channel vs unbuffered?
3. What happens when a buffered channel is full?
4. How do you choose the right buffer size?
5. Can a buffered channel of size 1 replace a mutex? (Yes, as a binary semaphore)
6. What is the "happens-before" guarantee of buffered vs unbuffered channels?
7. What is the difference between `len(ch)` and `cap(ch)`?
8. How does buffer size affect backpressure?
9. What is an unbounded channel and how would you implement one?
10. What are the memory implications of large channel buffers?

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

1. How does `select` choose between multiple ready cases? (Randomly)
2. What does an empty `select {}` do? (Blocks forever)
3. How do you implement a non-blocking channel send/receive? (`select` with `default`)
4. How do you implement a timeout with select? (`time.After` or `context.WithTimeout`)
5. What happens with nil channels in select? (Case is never selected — useful for dynamic enable/disable)
6. How do you implement priority select in Go? (You can't directly — use nested selects)
7. Can you use select with non-channel operations? (No — only channel sends/receives)
8. What is the performance of select with many cases? (O(N) — runtime shuffles and checks each case)
9. How do you use select for graceful shutdown? (`case <-ctx.Done()`)
10. What is the "done channel" pattern?
11. What is the memory leak with `time.After` in a for-select loop?
12. How do you disable a select case at runtime? (Set the channel to nil)

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

1. What happens if WaitGroup counter goes negative? (Panic)
2. Where should you call `wg.Add()` — before or inside the goroutine? (Before — racing with `Wait()` otherwise)
3. Can you reuse a WaitGroup after `Wait()` returns? (Yes)
4. What is the difference between WaitGroup and errgroup? (errgroup adds error handling and context cancellation)
5. How do you combine WaitGroup with error handling? (Use errgroup.Group instead)
6. Can you pass a WaitGroup by value? (Yes, but DON'T — it copies the state, leading to bugs. Always pass by pointer.)
7. How does WaitGroup work internally? (Atomic counter + semaphore)
8. When would you use a channel instead of WaitGroup? (When you need to collect results, not just wait)

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
