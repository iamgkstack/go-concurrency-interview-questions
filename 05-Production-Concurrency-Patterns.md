# Part 5: Production Concurrency Patterns — Context, Cancellation, Timeouts, Error Propagation, Graceful Shutdown

> Go Concurrency Interview Guide — From Beginner to Staff Engineer

---

## Table of Contents

1. [Context Package](#1-context-package)
2. [Cancellation Patterns](#2-cancellation-patterns)
3. [Timeouts and Deadlines](#3-timeouts-and-deadlines)
4. [Error Propagation Across Goroutines](#4-error-propagation-across-goroutines)
5. [Graceful Shutdown](#5-graceful-shutdown)

---

# 1. CONTEXT PACKAGE

## Concept Overview

`context.Context` is Go's mechanism for carrying deadlines, cancellation signals, and request-scoped values across API boundaries and between goroutines. It is arguably the most important production concurrency tool in Go.

**Context tree:**
```
context.Background()
    └── context.WithTimeout(parent, 10s)           // request timeout
        ├── context.WithValue(parent, traceIDKey)   // trace ID
        │   └── context.WithCancel(parent)          // sub-task cancellation
        └── context.WithTimeout(parent, 3s)         // downstream call timeout
```

When a parent context is cancelled, **all children are cancelled automatically**.

**Context types:**
| Factory | Purpose | Key Method |
|---------|---------|------------|
| `context.Background()` | Root context for main, init, tests | — |
| `context.TODO()` | Placeholder when unsure which context to use | — |
| `context.WithCancel(parent)` | Manual cancellation | `cancel()` |
| `context.WithTimeout(parent, d)` | Auto-cancel after duration | `cancel()` |
| `context.WithDeadline(parent, t)` | Auto-cancel at absolute time | `cancel()` |
| `context.WithValue(parent, k, v)` | Attach request-scoped data | — |
| `context.WithoutCancel(parent)` | Child that survives parent cancel (Go 1.21) | — |
| `context.AfterFunc(ctx, f)` | Call f when ctx is done (Go 1.21) | `stop()` |

**Golden rules:**
1. Context is **always the first parameter**: `func DoSomething(ctx context.Context, ...)`
2. **Never store context in a struct** (usually — there are rare exceptions).
3. **Always call cancel()** — typically `defer cancel()`.
4. **Don't pass nil context** — use `context.TODO()` if unsure.
5. **Context values are for request-scoped data** (trace IDs, auth tokens), NOT for function parameters.

## Common Interview Questions

1. What is the purpose of context.Context?
2. Difference between `context.Background()` and `context.TODO()`?
3. How does cancellation propagate through the context tree?
4. When should you use `context.WithValue`? What are the dangers?
5. What happens if you don't check `ctx.Done()`?
6. Should you store a context in a struct? Why/why not?
7. How does `context.WithTimeout` work internally?
8. Difference between `context.WithDeadline` and `context.WithTimeout`?
9. How do you check why a context was cancelled? (`context.Cause`, Go 1.20+)
10. What is `context.WithoutCancel` and when would you use it? (Background work that should survive request cancellation)
11. How does context interact with select?
12. Performance cost of context cancellation checking? (~1ns — just reading a channel)
13. How do you pass request-scoped data? (WithValue with unexported key types)
14. What is `context.AfterFunc`? (Register cleanup when context is done)
15. How do you handle context cancellation in database queries?

## Coding Problems

### Problem 1: HTTP Handler with Timeout

**Difficulty:** Easy-Medium  
```go
func handleRequest(w http.ResponseWriter, r *http.Request) {
    // Create a 3-second timeout derived from the request context
    ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
    defer cancel()

    // Call slow external service
    result, err := callExternalService(ctx)
    if err != nil {
        if errors.Is(err, context.DeadlineExceeded) {
            w.WriteHeader(http.StatusGatewayTimeout)
            w.Write([]byte("upstream timeout"))
            return
        }
        w.WriteHeader(http.StatusInternalServerError)
        return
    }

    json.NewEncoder(w).Encode(result)
}

func callExternalService(ctx context.Context) (Result, error) {
    req, err := http.NewRequestWithContext(ctx, "GET", "https://slow.api.com/data", nil)
    if err != nil {
        return Result{}, err
    }
    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return Result{}, err
    }
    defer resp.Body.Close()
    // parse response...
    return Result{}, nil
}
```

---

### Problem 2: Multi-Service Aggregator with Cascading Timeouts

**Difficulty:** Hard  
**Problem:** Aggregate data from 5 services. Overall timeout 10s. Per-service 3s. Return partial results if 3 of 5 respond.

```go
func aggregate(ctx context.Context, services []Service) ([]Result, error) {
    ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
    defer cancel()

    type indexed struct {
        idx    int
        result Result
        err    error
    }

    ch := make(chan indexed, len(services))

    for i, svc := range services {
        go func(idx int, s Service) {
            svcCtx, svcCancel := context.WithTimeout(ctx, 3*time.Second)
            defer svcCancel()
            res, err := s.Call(svcCtx)
            ch <- indexed{idx: idx, result: res, err: err}
        }(i, svc)
    }

    results := make([]Result, len(services))
    var successCount int
    quorum := 3

    for i := 0; i < len(services); i++ {
        select {
        case r := <-ch:
            if r.err == nil {
                results[r.idx] = r.result
                successCount++
                if successCount >= quorum {
                    cancel() // cancel remaining services
                    return results, nil
                }
            }
        case <-ctx.Done():
            if successCount > 0 {
                return results, nil // partial results
            }
            return nil, ctx.Err()
        }
    }

    if successCount >= quorum {
        return results, nil
    }
    return results, fmt.Errorf("only %d of %d services responded", successCount, quorum)
}
```

**Real-world Relevance:** API gateways, BFF services, search aggregation (query multiple backends).

---

### Problem 3: Context-Aware Cache with Stale-While-Revalidate

**Difficulty:** Hard (FAANG-level)  
```go
type CacheEntry struct {
    Value     interface{}
    ExpiresAt time.Time
    StaleAt   time.Time // still usable but should be refreshed
}

type SWRCache struct {
    mu      sync.RWMutex
    entries map[string]*CacheEntry
    sf      singleflight.Group
}

func (c *SWRCache) Get(ctx context.Context, key string, fetch func(context.Context) (interface{}, error)) (interface{}, error) {
    c.mu.RLock()
    entry, ok := c.entries[key]
    c.mu.RUnlock()

    now := time.Now()

    if ok && now.Before(entry.ExpiresAt) {
        if now.After(entry.StaleAt) {
            // Stale: return cached value but trigger background refresh
            // Use WithoutCancel so refresh survives request cancellation
            bgCtx := context.WithoutCancel(ctx)
            go c.refresh(bgCtx, key, fetch)
        }
        return entry.Value, nil
    }

    // Cache miss or expired: fetch synchronously
    val, err, _ := c.sf.Do(key, func() (interface{}, error) {
        return fetch(ctx)
    })
    if err != nil {
        // If we have a stale entry, return it as fallback
        if ok {
            return entry.Value, nil
        }
        return nil, err
    }

    c.set(key, val, 5*time.Minute, 4*time.Minute)
    return val, nil
}

func (c *SWRCache) refresh(ctx context.Context, key string, fetch func(context.Context) (interface{}, error)) {
    val, err, _ := c.sf.Do("refresh:"+key, func() (interface{}, error) {
        return fetch(ctx)
    })
    if err == nil {
        c.set(key, val, 5*time.Minute, 4*time.Minute)
    }
}
```

**Key insight:** `context.WithoutCancel` ensures the background refresh continues even if the triggering HTTP request completes.

---

## Common Mistakes

| Mistake | Consequence | Fix |
|---------|-------------|-----|
| Not calling `cancel()` | Context resources leak | Always `defer cancel()` |
| Storing context in struct | Stale contexts, unclear lifecycle | Pass as first parameter |
| Using `context.WithValue` for config | Implicit dependencies, hard to test | Use explicit parameters |
| Ignoring `ctx.Done()` in loops | Goroutine doesn't stop on cancellation | Check `ctx.Done()` regularly |
| Using `context.Background()` for requests | No timeout, no cancellation | Use request's context |
| Not propagating context to I/O | Downstream calls can't be cancelled | Pass ctx to all I/O calls |

---

# 2. CANCELLATION PATTERNS

## Concept Overview

Go uses **cooperative cancellation** — goroutines must explicitly check for cancellation signals. No goroutine is forcibly stopped.

**Cancellation mechanisms:**
1. **Done channel:** `ctx.Done()` returns a channel that closes when cancelled.
2. **Context cancellation:** `context.WithCancel`, `WithTimeout`, `WithDeadline`.
3. **Custom done channel:** `done := make(chan struct{})` + `close(done)`.

**Cancellation propagation in pipelines:**
```
[Source] → [Stage 1] → [Stage 2] → [Stage 3] → [Sink]
    ↑           ↑           ↑           ↑          ↑
    └───────────┴───────────┴───────────┴──────────┘
                    ctx.Done() propagates to all
```

## Common Interview Questions

1. How do you implement cooperative cancellation?
2. What is the "done channel" pattern?
3. How do you propagate cancellation through a pipeline?
4. What cleanup should happen after cancellation?
5. How do you distinguish between cancellation and error?
6. Difference between cancelling and timing out?
7. How do you cancel a blocked I/O operation? (Pass context to I/O call)
8. Difference between context cancellation and goroutine preemption?
9. How do you test cancellation behavior?
10. What is `context.Cause` and how does it help?

## Coding Problems

### Problem 1: Cancellable Long Computation

**Difficulty:** Medium  
```go
func search(ctx context.Context, data []int, target int) (int, error) {
    for i, val := range data {
        // Check cancellation every 1000 iterations
        if i%1000 == 0 {
            select {
            case <-ctx.Done():
                return -1, ctx.Err()
            default:
            }
        }
        if val == target {
            return i, nil
        }
    }
    return -1, errors.New("not found")
}
```

**Why check every N iterations:** Checking `ctx.Done()` on every iteration wastes CPU. The `select` has non-trivial overhead (~20ns). Every 1000 iterations is a good balance.

---

### Problem 2: Cascading Cancellation with Cleanup

**Difficulty:** Medium-Hard  
```go
func handleOrder(ctx context.Context, order Order) error {
    // Step 1: Reserve inventory
    reservationID, err := reserveInventory(ctx, order)
    if err != nil {
        return err
    }
    // Cleanup: release reservation if later steps fail
    defer func() {
        if err != nil {
            // Use background context — cleanup should complete even if request cancelled
            cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            defer cancel()
            releaseInventory(cleanupCtx, reservationID)
        }
    }()

    // Step 2: Charge payment
    paymentID, err := chargePayment(ctx, order)
    if err != nil {
        return err
    }
    defer func() {
        if err != nil {
            cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            defer cancel()
            refundPayment(cleanupCtx, paymentID)
        }
    }()

    // Step 3: Ship
    err = shipOrder(ctx, order)
    return err
}
```

**Key insight:** Cleanup uses `context.Background()`, not the request context. If the request is cancelled, we still need to clean up.

---

### Problem 3: Cancel-Safe Resource Acquisition

**Difficulty:** Hard  
```go
type Pool struct {
    resources chan *Resource
}

func (p *Pool) Acquire(ctx context.Context) (*Resource, error) {
    select {
    case r := <-p.resources:
        if r.IsHealthy() {
            return r, nil
        }
        // Unhealthy: discard and try again
        r.Close()
        return p.Acquire(ctx) // recursive retry
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}

func (p *Pool) Release(r *Resource) {
    // Always return resource, even if context is cancelled
    // The pool outlives any single request
    select {
    case p.resources <- r:
    default:
        r.Close() // pool is full, discard
    }
}

// Usage — resource must be returned regardless of context state
func useResource(ctx context.Context, pool *Pool) error {
    res, err := pool.Acquire(ctx)
    if err != nil {
        return err
    }
    defer pool.Release(res) // MUST release, even if context cancelled

    return res.DoWork(ctx)
}
```

---

# 3. TIMEOUTS AND DEADLINES

## Concept Overview

**Timeout:** Relative duration ("within 5 seconds").  
**Deadline:** Absolute time ("by 3:00 PM UTC").

`context.WithTimeout(parent, 5*time.Second)` is equivalent to `context.WithDeadline(parent, time.Now().Add(5*time.Second))`.

**Timeout types in HTTP:**
```
Client Timeout (http.Client.Timeout)
├── DNS Resolution
├── TCP Dial (net.Dialer.Timeout)
├── TLS Handshake (tls.Config.HandshakeTimeout)
├── Request Write
├── Response Header Wait (http.Transport.ResponseHeaderTimeout)
└── Response Body Read (http.Transport.IdleConnTimeout)
```

**Timeout budgeting:** When service A calls B calls C, each must account for time already spent:
```
Request arrives with 10s budget
├── Service A uses 2s for local work → passes 8s to B
│   ├── Service B uses 1s → passes 7s to C
│   │   └── Service C has 7s budget
```

## Common Interview Questions

1. Difference between a timeout and a deadline?
2. How do you implement a timeout with select?
3. How do you propagate timeout budgets across service calls?
4. What are the different timeout types in HTTP?
5. How do you handle timeout in database operations?
6. What is a cascading timeout failure? (Short timeout → retry → more load → more timeouts)
7. How do you choose timeout values? (P99 latency + buffer, or SLA-derived)
8. Memory leak with `time.After` in a loop?
9. How do you implement exponential backoff with deadline?
10. What is deadline propagation in gRPC? (gRPC automatically propagates deadlines)

## Coding Problems

### Problem 1: Timeout with Guaranteed Cleanup

**Difficulty:** Medium  
```go
func processWithTimeout(ctx context.Context, timeout time.Duration) (Result, error) {
    ctx, cancel := context.WithTimeout(ctx, timeout)
    defer cancel()

    resultCh := make(chan Result, 1)
    errCh := make(chan error, 1)

    go func() {
        result, err := expensiveOperation(ctx)
        if err != nil {
            errCh <- err
            return
        }
        resultCh <- result
    }()

    select {
    case result := <-resultCh:
        return result, nil
    case err := <-errCh:
        return Result{}, err
    case <-ctx.Done():
        // Timeout! But we must wait for cleanup
        // The goroutine will eventually notice ctx.Done() and exit
        return Result{}, fmt.Errorf("timeout after %v: %w", timeout, ctx.Err())
    }
}
```

---

### Problem 2: Timeout Budget Propagation

**Difficulty:** Hard  
```go
func serviceA(ctx context.Context, req Request) (Response, error) {
    deadline, ok := ctx.Deadline()
    if !ok {
        // No deadline set — add a default
        var cancel context.CancelFunc
        ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
        defer cancel()
        deadline, _ = ctx.Deadline()
    }

    // Step 1: Local processing (takes ~1s)
    partialResult, err := localProcess(ctx, req)
    if err != nil {
        return Response{}, err
    }

    // Step 2: Call Service B with remaining budget
    remaining := time.Until(deadline)
    if remaining < 100*time.Millisecond {
        return Response{}, fmt.Errorf("insufficient time budget: %v remaining", remaining)
    }

    bCtx, bCancel := context.WithTimeout(ctx, remaining-50*time.Millisecond) // keep 50ms buffer
    defer bCancel()

    bResult, err := serviceB(bCtx, partialResult)
    if err != nil {
        return Response{}, fmt.Errorf("service B: %w", err)
    }

    return combineResults(partialResult, bResult), nil
}
```

**Real-world Relevance:** gRPC deadline propagation, microservice chains, API gateway timeout management.

---

### Problem 3: Hedged Requests

**Difficulty:** Hard (FAANG-level)  
```go
func hedgedRequest(ctx context.Context, replicas []string, p95Latency time.Duration) (Response, error) {
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    type result struct {
        resp    Response
        err     error
        replica string
    }

    results := make(chan result, len(replicas))

    // Send to first replica immediately
    go func() {
        resp, err := callReplica(ctx, replicas[0])
        results <- result{resp, err, replicas[0]}
    }()

    // Send to additional replicas after p95 latency if no response yet
    for i := 1; i < len(replicas); i++ {
        replica := replicas[i]
        delay := time.Duration(i) * p95Latency

        go func() {
            select {
            case <-time.After(delay):
                resp, err := callReplica(ctx, replica)
                results <- result{resp, err, replica}
            case <-ctx.Done():
                return
            }
        }()
    }

    // Return first successful response
    var lastErr error
    for i := 0; i < len(replicas); i++ {
        select {
        case r := <-results:
            if r.err == nil {
                return r.resp, nil
            }
            lastErr = r.err
        case <-ctx.Done():
            return Response{}, ctx.Err()
        }
    }
    return Response{}, lastErr
}
```

**Real-world Relevance:** Google's "tail at scale," AWS S3 request hedging, Cassandra speculative execution.

---

# 4. ERROR PROPAGATION ACROSS GOROUTINES

## Concept Overview

**Challenge:** Errors in goroutines are isolated. A goroutine's error doesn't automatically propagate to its parent.

**Solutions:**
1. **Error channel:** `errs := make(chan error, 1)`
2. **errgroup.Group:** Standard library solution — `golang.org/x/sync/errgroup`
3. **Result type:** `chan Result[T]` carrying value + error
4. **Multi-error:** Collect all errors — `errors.Join` (Go 1.20+) or `hashicorp/go-multierror`

**errgroup.Group features:**
- Wait for all goroutines to complete.
- Returns the first error.
- With `errgroup.WithContext`, cancels all goroutines on first error.
- `SetLimit(N)` for bounded concurrency.

## Common Interview Questions

1. How do you propagate errors from goroutines?
2. What is errgroup and how does it work?
3. How do you handle multiple goroutines returning errors?
4. What happens when a goroutine panics?
5. How do you aggregate errors from concurrent operations?
6. Difference between errgroup and WaitGroup?
7. How do you implement first-error-cancellation?
8. How do you add context to errors from goroutines?
9. How do you test error scenarios in concurrent code?
10. What is the error channel pattern?

## Coding Problems

### Problem 1: Parallel Tasks with Error Collection

**Difficulty:** Medium  
```go
func runAll(ctx context.Context, tasks []func(context.Context) error) error {
    var (
        mu   sync.Mutex
        errs []error
        wg   sync.WaitGroup
    )

    for _, task := range tasks {
        wg.Add(1)
        go func(t func(context.Context) error) {
            defer wg.Done()
            if err := t(ctx); err != nil {
                mu.Lock()
                errs = append(errs, err)
                mu.Unlock()
            }
        }(task)
    }

    wg.Wait()
    return errors.Join(errs...) // Go 1.20+
}
```

---

### Problem 2: First-Error Cancellation with errgroup

**Difficulty:** Medium-Hard  
```go
func fetchAllData(ctx context.Context) (*AggregatedData, error) {
    g, ctx := errgroup.WithContext(ctx)

    var users []User
    var orders []Order
    var inventory []Item

    g.Go(func() error {
        var err error
        users, err = fetchUsers(ctx)
        return err
    })

    g.Go(func() error {
        var err error
        orders, err = fetchOrders(ctx)
        return err
    })

    g.Go(func() error {
        var err error
        inventory, err = fetchInventory(ctx)
        return err
    })

    if err := g.Wait(); err != nil {
        return nil, err // first error; other goroutines were cancelled
    }

    return &AggregatedData{users, orders, inventory}, nil
}
```

---

### Problem 3: Panic-Safe Goroutine Launcher

**Difficulty:** Medium  
```go
type SafeGroup struct {
    wg   sync.WaitGroup
    errs chan error
}

func NewSafeGroup(maxErrors int) *SafeGroup {
    return &SafeGroup{errs: make(chan error, maxErrors)}
}

func (sg *SafeGroup) Go(fn func() error) {
    sg.wg.Add(1)
    go func() {
        defer sg.wg.Done()
        defer func() {
            if r := recover(); r != nil {
                err := fmt.Errorf("panic recovered: %v\n%s", r, debug.Stack())
                select {
                case sg.errs <- err:
                default:
                }
            }
        }()

        if err := fn(); err != nil {
            select {
            case sg.errs <- err:
            default:
            }
        }
    }()
}

func (sg *SafeGroup) Wait() []error {
    sg.wg.Wait()
    close(sg.errs)
    var errs []error
    for err := range sg.errs {
        errs = append(errs, err)
    }
    return errs
}
```

**Real-world Relevance:** Production goroutine management — an unrecovered panic crashes the entire process.

---

### Problem 4: Error Propagation in Pipeline

**Difficulty:** Hard  
```go
type PipelineError struct {
    Stage string
    Err   error
}

func (pe *PipelineError) Error() string {
    return fmt.Sprintf("stage %s: %v", pe.Stage, pe.Err)
}

func buildPipeline(ctx context.Context, input <-chan Data) (<-chan Result, <-chan error) {
    ctx, cancel := context.WithCancel(ctx)
    errs := make(chan error, 1)

    sendErr := func(stage string, err error) {
        select {
        case errs <- &PipelineError{Stage: stage, Err: err}:
            cancel() // cancel entire pipeline on first error
        default:
        }
    }

    // Stage 1: Validate
    validated := make(chan Data)
    go func() {
        defer close(validated)
        for d := range input {
            if err := validate(d); err != nil {
                sendErr("validate", err)
                return
            }
            select {
            case validated <- d:
            case <-ctx.Done():
                return
            }
        }
    }()

    // Stage 2: Transform
    transformed := make(chan Result)
    go func() {
        defer close(transformed)
        for d := range validated {
            r, err := transform(d)
            if err != nil {
                sendErr("transform", err)
                return
            }
            select {
            case transformed <- r:
            case <-ctx.Done():
                return
            }
        }
    }()

    return transformed, errs
}
```

---

# 5. GRACEFUL SHUTDOWN

## Concept Overview

Graceful shutdown ensures in-flight work completes and resources are cleaned up before the process exits. This is critical for:
- **Data integrity:** Don't lose in-flight requests/transactions.
- **Resource cleanup:** Close DB connections, flush logs, deregister from service discovery.
- **Kubernetes:** Pods receive SIGTERM and have `terminationGracePeriodSeconds` (default 30s) before SIGKILL.

**Shutdown sequence:**
1. Receive signal (SIGTERM/SIGINT).
2. Stop accepting new work (close listeners, deregister from load balancer).
3. Wait for in-flight work to complete (with timeout).
4. Close resources in reverse initialization order.
5. Exit.

**Go tools:**
- `signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)` — Returns a context that cancels on signal. (Go 1.16+)
- `http.Server.Shutdown(ctx)` — Gracefully shuts down without interrupting active connections.

## Common Interview Questions

1. How do you implement graceful shutdown in a Go HTTP server?
2. What signals should you handle? (SIGTERM for orchestrators, SIGINT for Ctrl+C)
3. How do you drain in-flight requests?
4. What is `signal.NotifyContext`?
5. How do you handle shutdown timeout?
6. In what order should you shut down components? (Reverse of startup)
7. How does graceful shutdown work with Kubernetes?
8. How do you handle long-running requests during shutdown?
9. How do you test graceful shutdown?
10. Difference between SIGTERM and SIGKILL? (SIGTERM is catchable; SIGKILL is not)
11. How do you handle shutdown with multiple servers?
12. How do you prevent new work during shutdown?

## Coding Problems

### Problem 1: HTTP Server Graceful Shutdown

**Difficulty:** Medium  
```go
func main() {
    // Initialize resources
    db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Fatal(err)
    }

    mux := http.NewServeMux()
    mux.HandleFunc("/api/data", handleData)

    server := &http.Server{
        Addr:    ":8080",
        Handler: mux,
    }

    // Start server in background
    go func() {
        log.Println("starting server on :8080")
        if err := server.ListenAndServe(); err != http.ErrServerClosed {
            log.Fatalf("server error: %v", err)
        }
    }()

    // Wait for shutdown signal
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()
    <-ctx.Done()

    log.Println("shutting down...")

    // Graceful shutdown with 30-second timeout
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    // Phase 1: Stop accepting new connections, drain in-flight
    if err := server.Shutdown(shutdownCtx); err != nil {
        log.Printf("server shutdown error: %v", err)
    }

    // Phase 2: Close resources (reverse order of initialization)
    if err := db.Close(); err != nil {
        log.Printf("db close error: %v", err)
    }

    log.Println("shutdown complete")
}
```

---

### Problem 2: Worker Pool Graceful Shutdown

**Difficulty:** Medium-Hard  
```go
type Pool struct {
    jobs    chan Job
    results chan Result
    wg      sync.WaitGroup
    quit    chan struct{}
}

func (p *Pool) Shutdown(timeout time.Duration) []Job {
    close(p.jobs) // stop accepting new jobs

    done := make(chan struct{})
    go func() {
        p.wg.Wait() // wait for in-progress jobs
        close(done)
    }()

    select {
    case <-done:
        log.Println("all workers finished cleanly")
        return nil
    case <-time.After(timeout):
        log.Println("shutdown timeout — some jobs may be incomplete")
        close(p.quit) // signal workers to abort

        // Drain remaining jobs from channel
        var remaining []Job
        for job := range p.jobs {
            remaining = append(remaining, job)
        }
        return remaining
    }
}
```

---

### Problem 3: Full Application Lifecycle Manager

**Difficulty:** Hard (FAANG-level)  
```go
type Component interface {
    Name() string
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    HealthCheck() error
}

type LifecycleManager struct {
    components []Component
    started    []Component
    mu         sync.Mutex
}

func (lm *LifecycleManager) Register(c Component) {
    lm.components = append(lm.components, c)
}

func (lm *LifecycleManager) Start(ctx context.Context) error {
    for _, c := range lm.components {
        log.Printf("starting %s...", c.Name())
        if err := c.Start(ctx); err != nil {
            log.Printf("failed to start %s: %v", c.Name(), err)
            // Rollback: stop already-started components in reverse
            lm.stopStarted(ctx)
            return fmt.Errorf("start %s: %w", c.Name(), err)
        }
        lm.mu.Lock()
        lm.started = append(lm.started, c)
        lm.mu.Unlock()
        log.Printf("started %s", c.Name())
    }
    return nil
}

func (lm *LifecycleManager) Shutdown(timeout time.Duration) {
    ctx, cancel := context.WithTimeout(context.Background(), timeout)
    defer cancel()
    lm.stopStarted(ctx)
}

func (lm *LifecycleManager) stopStarted(ctx context.Context) {
    lm.mu.Lock()
    started := make([]Component, len(lm.started))
    copy(started, lm.started)
    lm.mu.Unlock()

    // Stop in reverse order
    for i := len(started) - 1; i >= 0; i-- {
        c := started[i]
        log.Printf("stopping %s...", c.Name())
        perComponentCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
        if err := c.Stop(perComponentCtx); err != nil {
            log.Printf("error stopping %s: %v", c.Name(), err)
        } else {
            log.Printf("stopped %s", c.Name())
        }
        cancel()
    }
}

// Usage
func main() {
    lm := &LifecycleManager{}
    lm.Register(&DatabaseComponent{})
    lm.Register(&CacheComponent{})
    lm.Register(&HTTPServer{})
    lm.Register(&GRPCServer{})
    lm.Register(&MetricsServer{})

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    if err := lm.Start(ctx); err != nil {
        log.Fatal(err)
    }

    <-ctx.Done()
    log.Println("shutdown signal received")

    lm.Shutdown(30 * time.Second)
    log.Println("bye")
}
```

**Real-world Relevance:** Service mesh sidecar lifecycle, complex service bootstrap (Uber's fx, Google Wire).

---

## Common Mistakes

| Mistake | Consequence | Fix |
|---------|-------------|-----|
| Not handling signals | Hard kill on SIGTERM (data loss) | Use `signal.NotifyContext` |
| No shutdown timeout | Hang forever if goroutine stuck | `context.WithTimeout` for shutdown |
| Wrong shutdown order | Close DB before HTTP server drains | Reverse of startup order |
| Using request ctx for cleanup | Cleanup cancelled with request | Use `context.Background()` for cleanup |
| Not draining channels | Goroutine leaks | Close channels, drain remaining items |
| Not deregistering from LB | Load balancer still sends traffic | Deregister before stopping listener |
