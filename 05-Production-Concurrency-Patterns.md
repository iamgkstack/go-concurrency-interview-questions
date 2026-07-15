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

### Q1: What is the purpose of context.Context?

**Answer:** `context.Context` carries deadlines, cancellation signals, and request-scoped values across API boundaries and goroutines. It solves three problems: (1) cancellation propagation — when a client disconnects, all downstream work stops, (2) deadline/timeout management — enforce time limits across call chains, (3) request-scoped data — pass trace IDs, user IDs without changing function signatures. Every production Go function that does I/O or runs concurrently should accept a `context.Context` as its first parameter.

**Example:**
```go
func fetchUser(ctx context.Context, id string) (*User, error) {
    // Respect cancellation:
    req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
    resp, err := client.Do(req) // cancelled if ctx is done
    if err != nil { return nil, err }
    defer resp.Body.Close()
    var user User
    json.NewDecoder(resp.Body).Decode(&user)
    return &user, nil
}
```

**Real-time Scenario:** A user refreshes a slow-loading page. The first request's context is cancelled, stopping the database query, HTTP calls, and all goroutines spawned for that request — preventing wasted CPU and database connections.

---

### Q2: Difference between `context.Background()` and `context.TODO()`?

**Answer:** Both return an empty, non-nil context with no deadline or cancellation. **`Background()`** is the root context for main, init, and top-level operations. **`TODO()`** is a placeholder for when you know you should pass a context but don't have one yet — it signals to code reviewers "I need to plumb context here." Functionally identical, but semantically different: `Background` = intentionally root, `TODO` = needs refactoring.

**Example:**
```go
// context.Background(): intentional root
func main() {
    ctx := context.Background()
    srv.Serve(ctx)
}

// context.TODO(): needs refactoring
func legacyFunction() {
    // TODO: accept context parameter when we refactor the caller
    ctx := context.TODO()
    db.QueryContext(ctx, query)
}
```

**Real-time Scenario:** During a migration from non-context-aware to context-aware code, `TODO()` marks 50 call sites that need context plumbing. A `grep context.TODO` shows the remaining migration work.

---

### Q3: How does cancellation propagate through the context tree?

**Answer:** Contexts form a tree. `WithCancel`, `WithTimeout`, and `WithDeadline` create child contexts. Cancelling a parent cancels ALL descendants (children, grandchildren, etc.), but NOT siblings or ancestors. Each child's `Done()` channel is closed when the parent is cancelled. This tree structure ensures that cancelling a request cancels all its sub-operations without affecting other requests.

**Example:**
```go
root := context.Background()
parent, cancelParent := context.WithCancel(root)
child1, cancelChild1 := context.WithCancel(parent)
child2, _ := context.WithCancel(parent)

cancelParent() // cancels parent, child1, AND child2
// root is NOT cancelled

// Alternatively:
cancelChild1() // cancels only child1
// parent and child2 are NOT cancelled
```

**Real-time Scenario:** An API request creates a parent context. It spawns goroutines for database, cache, and external API calls — each with its own child context. If the database call fails and the handler returns, cancelling the parent stops the cache and API calls too.

---

### Q4: When should you use `context.WithValue`? What are the dangers?

**Answer:** Use `WithValue` for request-scoped data that crosses API boundaries: trace IDs, request IDs, authentication tokens. Dangers: (1) it's an untyped `interface{}` — no compile-time safety, (2) keys must be unexported types to avoid collisions, (3) it's not for passing function parameters (use explicit args), (4) values are not visible in function signatures — hidden dependencies. Keep values small and read-only.

**Example:**
```go
// GOOD: request-scoped, cross-cutting concerns
type contextKey string
const traceIDKey contextKey = "traceID"

func WithTraceID(ctx context.Context, id string) context.Context {
    return context.WithValue(ctx, traceIDKey, id)
}
func TraceIDFromContext(ctx context.Context) string {
    id, _ := ctx.Value(traceIDKey).(string)
    return id
}

// BAD: using context to pass function arguments
ctx = context.WithValue(ctx, "userID", 42) // DON'T — use function params
```

**Real-time Scenario:** Every HTTP request gets a trace ID via middleware (`context.WithValue`). All downstream services, database calls, and log entries include this trace ID for distributed tracing across 20+ microservices.

---

### Q5: What happens if you don't check `ctx.Done()`?

**Answer:** The goroutine continues running even after the context is cancelled — wasting CPU, memory, and potentially holding resources (database connections, file handles). The cancellation signal is cooperative: if you don't check, the goroutine never stops. This causes goroutine leaks where goroutines accumulate until the process runs out of memory.

**Example:**
```go
// BAD: never checks cancellation — goroutine leak
func leakyWorker(ctx context.Context) {
    for {
        result := expensiveComputation() // runs forever
        ch <- result
    }
}

// GOOD: checks cancellation
func goodWorker(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return // stop when cancelled
        default:
            result := expensiveComputation()
            select {
            case ch <- result:
            case <-ctx.Done():
                return
            }
        }
    }
}
```

**Real-time Scenario:** A web server spawns a goroutine per request but never checks `ctx.Done()`. When clients disconnect, goroutines keep running. After 24 hours, 500,000 leaked goroutines consume 4GB of memory, and the server OOMs.

---

### Q6: Should you store a context in a struct? Why/why not?

**Answer:** Generally NO — the Go documentation explicitly says "Do not store Contexts inside a struct type." Context is request-scoped and should flow through function parameters. Storing it in a struct makes the lifecycle unclear (when does it expire? who cancels it?). Exception: `http.Request` stores a context because the request IS the scope. If you must store one, document the lifecycle clearly.

**Example:**
```go
// BAD: stored context — unclear lifecycle
type Service struct {
    ctx context.Context // which request? when does it expire?
}

// GOOD: pass context through parameters
type Service struct {}
func (s *Service) Process(ctx context.Context, data Data) error {
    return s.db.QueryContext(ctx, query, data.ID)
}
```

**Real-time Scenario:** A developer stored a context in a struct during initialization. The context was the background context, so it never expired. When they tried to add request timeouts, they couldn't because all requests shared the stored context.

---

### Q7: How does `context.WithTimeout` work internally?

**Answer:** `context.WithTimeout(parent, d)` calls `context.WithDeadline(parent, time.Now().Add(d))`. Internally, it creates a `timerCtx` that starts a `time.AfterFunc` to close the `Done()` channel when the deadline arrives. If the parent is cancelled first, the child is also cancelled (parent-child propagation). Calling `cancel()` stops the timer and closes `Done()` immediately. Always call `cancel()` to release the timer even if the operation completes early.

**Example:**
```go
ctx, cancel := context.WithTimeout(parent, 5*time.Second)
defer cancel() // MUST call — releases the internal timer

// Internally equivalent to:
deadline := time.Now().Add(5 * time.Second)
ctx, cancel := context.WithDeadline(parent, deadline)

// The timer fires after 5 seconds:
// time.AfterFunc(5*time.Second, func() { close(ctx.done) })
```

**Real-time Scenario:** Without calling `cancel()`, the internal timer runs for the full 5 seconds even if the operation completes in 10ms. In a high-throughput server handling 100K requests/sec, this leaks 100K timers per second, degrading performance.

---

### Q8: Difference between `context.WithDeadline` and `context.WithTimeout`?

**Answer:** `WithDeadline` takes an absolute time (`time.Time`). `WithTimeout` takes a duration (`time.Duration`). `WithTimeout` is syntactic sugar: `WithTimeout(ctx, 5s)` = `WithDeadline(ctx, time.Now().Add(5s))`. Use `WithDeadline` when you have a fixed deadline (SLA boundary). Use `WithTimeout` when you want a relative duration. If the parent's deadline is earlier, the child inherits the parent's deadline.

**Example:**
```go
// WithTimeout: "I want this to take at most 5 seconds"
ctx, cancel := context.WithTimeout(parent, 5*time.Second)

// WithDeadline: "This must complete by 3:00 PM"
deadline := time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC)
ctx, cancel := context.WithDeadline(parent, deadline)

// Parent's deadline wins if earlier:
parent, _ := context.WithTimeout(context.Background(), 2*time.Second)
child, _ := context.WithTimeout(parent, 10*time.Second)
// child's effective deadline = 2 seconds (parent's deadline)
```

**Real-time Scenario:** A batch job must complete before midnight (use `WithDeadline`). Each individual item should take at most 30 seconds (use `WithTimeout`). The parent midnight deadline automatically caps all child timeouts.

---

### Q9: How do you check why a context was cancelled?

**Answer:** `ctx.Err()` returns `context.Canceled` or `context.DeadlineExceeded`. For more detail, Go 1.20+ adds `context.Cause(ctx)` which returns the specific error passed to `context.WithCancelCause`'s cancel function. This enables rich error reporting: not just "cancelled" but "cancelled because: upstream service returned 503."

**Example:**
```go
// Go 1.20+: cancel with cause
ctx, cancel := context.WithCancelCause(parent)
cancel(fmt.Errorf("user service returned 503"))

err := ctx.Err()             // context.Canceled
cause := context.Cause(ctx)  // "user service returned 503"

// Pre-1.20: only generic reason
ctx, cancel := context.WithCancel(parent)
cancel()
err := ctx.Err() // context.Canceled — no specific reason
```

**Real-time Scenario:** An observability platform logs the cause of every cancelled request. Instead of "10,000 cancelled requests" (useless), they see "8,000 cancelled: database timeout, 2,000 cancelled: user disconnected" — actionable insight.

---

### Q10: What is `context.WithoutCancel` and when would you use it?

**Answer:** `context.WithoutCancel(parent)` (Go 1.21+) creates a child context that carries the parent's values but is NOT cancelled when the parent is cancelled. Use it for background work that must survive request cancellation: audit logging, metrics flushing, cleanup operations. The child keeps the parent's values (trace ID, user ID) but runs independently of the request lifecycle.

**Example:**
```go
func handler(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()

    // This should survive request cancellation:
    auditCtx := context.WithoutCancel(ctx) // keeps trace ID, ignores cancel
    go func() {
        auditLog(auditCtx, "user accessed resource") // runs even if client disconnects
    }()

    // This should respect request cancellation:
    data, err := fetchData(ctx) // stops if client disconnects
}
```

**Real-time Scenario:** A payment handler must log the audit trail even if the client disconnects. `WithoutCancel` ensures the audit goroutine has the trace ID for correlation but doesn't stop when the HTTP request context is cancelled.

---

### Q11: How does context interact with select?

**Answer:** `ctx.Done()` returns a channel that is closed when the context is cancelled or its deadline expires. Use it in `select` alongside other channels to make any operation cancellable. The `select` unblocks on whichever channel is ready first. This is the fundamental pattern for context-aware concurrent code in Go.

**Example:**
```go
select {
case result := <-resultCh:
    return result, nil
case <-ctx.Done():
    return nil, ctx.Err() // cancelled or timed out
}

// Multiple channels with cancellation:
select {
case data := <-dataCh:
    process(data)
case err := <-errCh:
    return err
case <-ctx.Done():
    return ctx.Err()
}
```

**Real-time Scenario:** A WebSocket reader selects on the message channel AND `ctx.Done()`. When the server shuts down (context cancelled), the reader exits immediately instead of blocking forever on the next message.

---

### Q12: Performance cost of context cancellation checking?

**Answer:** Checking `ctx.Done()` in a non-blocking `select` (with `default`) costs ~1-2ns — it's just checking if a channel is closed (reading a pointer). This is negligible. Don't check on every loop iteration in tight inner loops — check every 1,000 or 10,000 iterations. For I/O-bound operations, the check cost is zero relative to the I/O latency.

**Example:**
```go
// DON'T: check every iteration in a tight loop
for i := 0; i < 1e9; i++ {
    select {
    case <-ctx.Done(): return // ~1ns overhead * 1B = 1 second overhead!
    default:
    }
    x += i
}

// DO: check periodically
for i := 0; i < 1e9; i++ {
    if i%10000 == 0 {
        select {
        case <-ctx.Done(): return
        default:
        }
    }
    x += i
}
```

**Real-time Scenario:** A JSON parser checking ctx.Done() on every byte slowed down from 1GB/s to 200MB/s. Checking every 64KB restored performance to 950MB/s with cancellation latency under 1ms.

---

### Q13: How do you pass request-scoped data?

**Answer:** Use `context.WithValue` with unexported key types to avoid key collisions. Create helper functions to set and get values. Keep values read-only, small, and request-scoped (trace IDs, auth tokens, not database connections or loggers). The key type should be unexported to prevent other packages from accidentally using the same key.

**Example:**
```go
type contextKey int
const (
    userIDKey contextKey = iota
    traceIDKey
)

func WithUserID(ctx context.Context, id string) context.Context {
    return context.WithValue(ctx, userIDKey, id)
}

func UserIDFromContext(ctx context.Context) (string, bool) {
    id, ok := ctx.Value(userIDKey).(string)
    return id, ok
}

// Middleware:
func authMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        userID := extractUserID(r)
        ctx := WithUserID(r.Context(), userID)
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}
```

**Real-time Scenario:** An auth middleware extracts the user ID from a JWT and puts it in context. Every downstream handler, service call, and database query can access the user ID without passing it as an explicit parameter through 10 layers.

---

### Q14: What is `context.AfterFunc`?

**Answer:** `context.AfterFunc(ctx, fn)` (Go 1.21+) registers a function that runs after the context is done (cancelled or deadline exceeded). The function runs in its own goroutine. It returns a `stop` function to prevent the callback if no longer needed. This replaces the pattern of spawning a goroutine that blocks on `<-ctx.Done()`.

**Example:**
```go
// Old pattern:
go func() {
    <-ctx.Done()
    cleanup()
}()

// New pattern (Go 1.21+):
stop := context.AfterFunc(ctx, func() {
    cleanup() // runs in new goroutine when ctx is done
})
defer stop() // cancel if we don't need cleanup anymore

// Use case: auto-close resources
conn := openConnection()
stop := context.AfterFunc(ctx, func() {
    conn.Close() // auto-close when context expires
})
```

**Real-time Scenario:** A streaming response registers `AfterFunc` to flush and close the response writer when the client disconnects. This replaces a dedicated goroutine that was watching `ctx.Done()`.

---

### Q15: How do you handle context cancellation in database queries?

**Answer:** Use `QueryContext`, `ExecContext`, `PrepareContext` — all accept `context.Context`. When the context is cancelled, the driver sends a cancel signal to the database server (e.g., `pg_cancel_backend` for PostgreSQL). The query is interrupted server-side, and the connection is returned to the pool. Always pass the request context to prevent long-running queries from surviving request cancellation.

**Example:**
```go
func getOrders(ctx context.Context, db *sql.DB, userID string) ([]Order, error) {
    ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
    defer cancel()

    rows, err := db.QueryContext(ctx,
        "SELECT id, total FROM orders WHERE user_id = $1", userID)
    if err != nil {
        if errors.Is(err, context.DeadlineExceeded) {
            return nil, fmt.Errorf("query timed out: %w", err)
        }
        return nil, err
    }
    defer rows.Close()
    // ... scan rows
}
```

**Real-time Scenario:** A dashboard query joining 5 tables takes 30 seconds. The user navigates away after 2 seconds, context cancels, PostgreSQL receives the cancel signal and stops the query — freeing the database connection and CPU for other queries.

---

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

### Q1: How do you implement cooperative cancellation?

**Answer:** Cooperative cancellation means goroutines periodically check for cancellation signals and voluntarily stop. In Go, pass `context.Context` and check `ctx.Done()` in select statements or at regular intervals in loops. The goroutine itself decides when and how to stop — the canceller only signals intent. This is cooperative because the goroutine isn't forcefully killed.

**Example:**
```go
func longTask(ctx context.Context) error {
    for i := 0; i < 1000000; i++ {
        // Check cancellation every 1000 iterations
        if i%1000 == 0 {
            select {
            case <-ctx.Done():
                return ctx.Err() // cooperative: we chose to stop
            default:
            }
        }
        processItem(i)
    }
    return nil
}

// Caller:
ctx, cancel := context.WithCancel(context.Background())
go longTask(ctx)
cancel() // signals cancellation — goroutine checks and stops
```

**Real-time Scenario:** A search engine goroutine scanning 1M documents checks cancellation every 1,000 documents. When the user navigates away and the request context is cancelled, the scan stops within 1,000 iterations rather than completing all 1M.

---

### Q2: What is the "done channel" pattern?

**Answer:** The done channel pattern uses a `chan struct{}` that is closed to signal goroutines to stop. All goroutines select on the done channel — closing it broadcasts the signal to all listeners. This was the standard pre-`context.Context` cancellation pattern and is still used for simple cases. Closing a channel wakes all receivers simultaneously.

**Example:**
```go
func worker(done <-chan struct{}, tasks <-chan Task) {
    for {
        select {
        case <-done:
            return // shutdown signal received
        case task := <-tasks:
            process(task)
        }
    }
}

// Signal all workers to stop:
done := make(chan struct{})
for i := 0; i < 10; i++ {
    go worker(done, tasks)
}
close(done) // all 10 workers receive the signal simultaneously
```

**Real-time Scenario:** A file watcher uses a done channel to stop all monitoring goroutines when the application exits. Closing the channel is simpler than calling `cancel()` on 50 separate contexts.

---

### Q3: How do you propagate cancellation through a pipeline?

**Answer:** Pass the same context to all pipeline stages. When the context is cancelled, each stage checks `ctx.Done()` and stops processing. For channel-based pipelines, use select on both the input channel and `ctx.Done()`. This ensures the entire pipeline shuts down cleanly, not just one stage.

**Example:**
```go
func stage(ctx context.Context, in <-chan Data) <-chan Data {
    out := make(chan Data)
    go func() {
        defer close(out)
        for {
            select {
            case <-ctx.Done():
                return // pipeline cancelled
            case data, ok := <-in:
                if !ok { return } // input closed
                result := transform(data)
                select {
                case out <- result:
                case <-ctx.Done():
                    return
                }
            }
        }
    }()
    return out
}
```

**Real-time Scenario:** A 5-stage data processing pipeline is cancelled when the upstream data source disconnects. The context cancellation propagates through all 5 stages, each goroutine cleaning up and exiting within milliseconds.

---

### Q4: What cleanup should happen after cancellation?

**Answer:** After cancellation: (1) flush buffered data (partial results, logs), (2) release resources (close files, return connections to pool), (3) send acknowledgements (tell upstream work was not completed), (4) update state (mark in-progress items as incomplete). Use `defer` for cleanup in each goroutine. Don't skip cleanup just because the context was cancelled.

**Example:**
```go
func processWithCleanup(ctx context.Context, conn *Connection) error {
    defer conn.Release()      // always release connection
    defer metrics.Flush()     // always flush metrics

    for {
        select {
        case <-ctx.Done():
            // Clean up: save partial progress
            saveCheckpoint(progress)
            return ctx.Err()
        case item := <-items:
            if err := process(item); err != nil {
                return err
            }
            progress++
        }
    }
}
```

**Real-time Scenario:** A batch import job cancelled mid-way saves a checkpoint at record 50,000 of 100,000. When restarted, it resumes from record 50,001 rather than re-processing from the beginning.

---

### Q5: How do you distinguish between cancellation and error?

**Answer:** Check `ctx.Err()`: returns `context.Canceled` for explicit cancellation, `context.DeadlineExceeded` for timeout. In Go 1.20+, `context.Cause(ctx)` returns the specific error passed to `context.WithCancelCause`. For application errors, compare with `errors.Is`. This distinction matters for logging (cancellation is normal, errors are not) and retry logic (don't retry cancellations).

**Example:**
```go
result, err := doWork(ctx)
if err != nil {
    switch {
    case errors.Is(err, context.Canceled):
        log.Info("request cancelled by client") // normal — don't alert
        return
    case errors.Is(err, context.DeadlineExceeded):
        log.Warn("request timed out")           // may need attention
        metrics.Increment("timeouts")
        return
    default:
        log.Error("unexpected error", "err", err) // real error — alert
        return
    }
}
```

**Real-time Scenario:** An alerting system fires on "unexpected errors" but not on cancellations. Without distinguishing them, the team got paged every time a user closed their browser tab mid-request.

---

### Q6: Difference between cancelling and timing out?

**Answer:** **Cancellation** is explicit and intentional — someone called `cancel()` (e.g., user navigated away, parent operation decided to stop). **Timeout** is implicit — the deadline was reached without the operation completing. Both cancel the context, but for different reasons. Check `ctx.Err()`: `context.Canceled` vs `context.DeadlineExceeded`. Timeouts may warrant retries; cancellations usually shouldn't be retried.

**Example:**
```go
// Cancellation: explicit, intentional
ctx, cancel := context.WithCancel(parent)
cancel() // explicit: "I don't need this anymore"
// ctx.Err() == context.Canceled

// Timeout: implicit, time ran out
ctx, cancel := context.WithTimeout(parent, 5*time.Second)
// ... 5 seconds pass ...
// ctx.Err() == context.DeadlineExceeded

// In error handling:
if ctx.Err() == context.Canceled {
    // don't retry — caller intentionally cancelled
} else if ctx.Err() == context.DeadlineExceeded {
    // maybe retry with a longer timeout
}
```

**Real-time Scenario:** A file download is cancelled by the user (Canceled — don't retry) vs times out due to slow network (DeadlineExceeded — retry with a longer timeout or different server).

---

### Q7: How do you cancel a blocked I/O operation?

**Answer:** Pass context to context-aware I/O functions: `http.NewRequestWithContext`, `db.QueryContext`, `net.Dialer.DialContext`. When the context is cancelled, these functions return immediately with `context.Canceled` or `context.DeadlineExceeded`. For low-level I/O, set deadlines on the connection: `conn.SetDeadline(time.Now().Add(timeout))`. Non-context-aware I/O requires wrapping with a goroutine and channel.

**Example:**
```go
// Context-aware HTTP call:
req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
resp, err := client.Do(req) // returns immediately if ctx is cancelled

// Context-aware DB query:
rows, err := db.QueryContext(ctx, query, args...)

// Low-level I/O with deadline:
conn, _ := net.Dial("tcp", "host:port")
conn.SetDeadline(time.Now().Add(5 * time.Second))
n, err := conn.Read(buf) // returns after 5s if no data
```

**Real-time Scenario:** A reverse proxy sets the client's context on upstream requests. When the client disconnects, the context cancels, and the upstream HTTP request is aborted immediately — freeing the upstream connection.

---

### Q8: Difference between context cancellation and goroutine preemption?

**Answer:** **Context cancellation** is cooperative — the goroutine must check `ctx.Done()` to notice it. **Goroutine preemption** is done by the Go scheduler — it can preempt a goroutine at any async preemption point (Go 1.14+) to allow other goroutines to run. Cancellation is application-level (stop this work). Preemption is scheduler-level (share CPU time). Cancellation requires explicit code; preemption happens automatically.

**Example:**
```go
// Context cancellation: cooperative — goroutine must check
for {
    select {
    case <-ctx.Done():
        return // goroutine voluntarily stops
    default:
        doWork() // if doWork never checks ctx, it never stops
    }
}

// Preemption: automatic by scheduler (Go 1.14+)
for {
    doWork() // scheduler can preempt this goroutine at safe points
    // even without select/channel ops, scheduler can interrupt
}
```

**Real-time Scenario:** A CPU-intensive goroutine without any channel operations was starving other goroutines in Go 1.13. After upgrading to Go 1.14, async preemption allows the scheduler to interrupt it, but it still doesn't respond to context cancellation until it checks `ctx.Done()`.

---

### Q9: How do you test cancellation behavior?

**Answer:** (1) Create a context, cancel it before or during the operation, verify the function returns `context.Canceled`. (2) Use `context.WithTimeout` with a very short timeout. (3) Verify cleanup happened (resources released, state saved). (4) Use `-race` to catch data races in cancellation paths. (5) Test that partial results are correct.

**Example:**
```go
func TestCancellation(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())

    // Cancel after 100ms
    go func() {
        time.Sleep(100 * time.Millisecond)
        cancel()
    }()

    err := longOperation(ctx)
    if !errors.Is(err, context.Canceled) {
        t.Errorf("expected context.Canceled, got: %v", err)
    }

    // Verify cleanup happened
    if !resourceReleased {
        t.Error("resource was not released after cancellation")
    }
}
```

**Real-time Scenario:** A test suite verifies that when a database query context is cancelled, the connection is returned to the pool (not leaked), partial transaction is rolled back, and the function returns within 50ms of cancellation.

---

### Q10: What is `context.Cause` and how does it help?

**Answer:** `context.Cause(ctx)` (Go 1.20+) returns the specific error that caused the context to be cancelled. Use `context.WithCancelCause(parent)` to get a `cancel(cause)` function that accepts an error. This is more informative than `ctx.Err()` which only returns `Canceled` or `DeadlineExceeded`. The cause tells you WHY the context was cancelled, not just that it was.

**Example:**
```go
ctx, cancel := context.WithCancelCause(context.Background())

// Cancel with specific reason:
cancel(fmt.Errorf("database connection lost"))

// Check the cause:
err := ctx.Err()                  // context.Canceled (generic)
cause := context.Cause(ctx)       // "database connection lost" (specific)

// Useful in error handling:
if cause != nil {
    log.Printf("cancelled because: %v", cause)
}
```

**Real-time Scenario:** A request handler cancels all downstream operations when the database fails. With `Cause`, the monitoring system logs "cancelled because: database connection lost" instead of just "context canceled" — enabling faster root cause analysis.

---

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

### Q1: Difference between a timeout and a deadline?

**Answer:** A **timeout** is a duration ("wait at most 5 seconds"). A **deadline** is an absolute point in time ("complete by 3:00:05 PM"). In Go, `context.WithTimeout(ctx, 5*time.Second)` internally calls `context.WithDeadline(ctx, time.Now().Add(5*time.Second))`. Deadlines compose better across service calls because the absolute time doesn't change as it passes through layers — each layer can see how much time remains.

**Example:**
```go
// Timeout: relative duration
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()

// Deadline: absolute point in time
deadline := time.Now().Add(5 * time.Second)
ctx, cancel := context.WithDeadline(ctx, deadline)
defer cancel()

// Check remaining time:
if dl, ok := ctx.Deadline(); ok {
    remaining := time.Until(dl)
    log.Printf("time remaining: %v", remaining)
}
```

**Real-time Scenario:** A request has a 10-second SLA. It calls Service A (takes 3s), then Service B. Using a deadline, Service B automatically gets 7 seconds. Using a timeout of 10s for each call would allow 20 seconds total, violating the SLA.

---

### Q2: How do you implement a timeout with select?

**Answer:** Use `select` with `time.After(duration)` or a context's `Done()` channel. `select` blocks until one of the channels is ready. If the work completes first, you get the result. If the timeout fires first, you handle the timeout. Always use context-based timeouts in production; `time.After` is for simple cases.

**Example:**
```go
// With time.After:
select {
case result := <-resultCh:
    return result, nil
case <-time.After(5 * time.Second):
    return nil, errors.New("operation timed out")
}

// With context (preferred):
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
select {
case result := <-resultCh:
    return result, nil
case <-ctx.Done():
    return nil, ctx.Err()
}
```

**Real-time Scenario:** A DNS resolver gives each upstream server 2 seconds to respond. If no response comes, it tries the next server. The `select` with `time.After` implements this per-server timeout.

---

### Q3: How do you propagate timeout budgets across service calls?

**Answer:** Pass the context (with its deadline) to downstream calls. Each downstream service checks `ctx.Deadline()` to see remaining time and can adjust its own behavior. In gRPC, deadlines propagate automatically through metadata. For HTTP, include the deadline in a header (`X-Request-Deadline`) and reconstruct the context on the receiving side.

**Example:**
```go
func serviceA(ctx context.Context) error {
    // Original deadline: 10s from now
    // After serviceA's work (3s), 7s remain in ctx
    resp, err := httpClient.Do(req.WithContext(ctx)) // passes remaining deadline
    return err
}

// Propagate via HTTP header:
func propagateDeadline(ctx context.Context, req *http.Request) {
    if dl, ok := ctx.Deadline(); ok {
        req.Header.Set("X-Request-Deadline", dl.Format(time.RFC3339Nano))
    }
}

// Reconstruct on receiving side:
func handler(w http.ResponseWriter, r *http.Request) {
    if dlStr := r.Header.Get("X-Request-Deadline"); dlStr != "" {
        dl, _ := time.Parse(time.RFC3339Nano, dlStr)
        ctx, cancel := context.WithDeadline(r.Context(), dl)
        defer cancel()
        // use ctx for downstream calls
    }
}
```

**Real-time Scenario:** A 3-tier architecture (API → Service → Database) with a 10-second SLA propagates the deadline. If the API layer takes 2 seconds, the service layer gets 8 seconds. If the service takes 3 seconds, the database gets 5 seconds.

---

### Q4: What are the different timeout types in HTTP?

**Answer:** Go's `http.Server` has multiple timeout settings: **ReadTimeout** (max time to read the entire request, including body), **WriteTimeout** (max time to write the response), **IdleTimeout** (max time between requests on keep-alive connections), **ReadHeaderTimeout** (max time to read request headers). Each protects against different attack vectors and failure modes.

**Example:**
```go
srv := &http.Server{
    Addr:              ":8080",
    ReadHeaderTimeout: 5 * time.Second,   // slowloris protection
    ReadTimeout:       10 * time.Second,  // slow client body
    WriteTimeout:      30 * time.Second,  // slow response generation
    IdleTimeout:       120 * time.Second, // keep-alive cleanup
    Handler:           mux,
}

// Client-side timeouts:
client := &http.Client{
    Timeout: 30 * time.Second, // total request timeout
    Transport: &http.Transport{
        DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
        TLSHandshakeTimeout: 5 * time.Second,
        ResponseHeaderTimeout: 10 * time.Second,
    },
}
```

**Real-time Scenario:** A server without `ReadHeaderTimeout` is vulnerable to Slowloris attacks — an attacker sends headers very slowly, holding connections open indefinitely and exhausting server resources.

---

### Q5: How do you handle timeout in database operations?

**Answer:** Pass `context.Context` to all database operations. Go's `database/sql` package supports `QueryContext`, `ExecContext`, etc. The context deadline is sent to the database driver, which cancels the query server-side. Also set connection-level timeouts in the DSN and pool-level timeouts (`SetConnMaxLifetime`, `SetConnMaxIdleTime`).

**Example:**
```go
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()

rows, err := db.QueryContext(ctx, "SELECT * FROM orders WHERE user_id = $1", userID)
if err != nil {
    if errors.Is(err, context.DeadlineExceeded) {
        return nil, fmt.Errorf("query timed out after 5s: %w", err)
    }
    return nil, err
}

// Pool-level timeouts:
db.SetConnMaxLifetime(30 * time.Minute)
db.SetConnMaxIdleTime(5 * time.Minute)
db.SetMaxOpenConns(25)
```

**Real-time Scenario:** A runaway query (missing index, full table scan) is automatically cancelled after 5 seconds by the context, preventing it from holding a connection for minutes and starving the connection pool.

---

### Q6: What is a cascading timeout failure?

**Answer:** A cascading timeout failure occurs when: service slows down → callers timeout → callers retry → more load on the slow service → even slower → more timeouts → more retries → total collapse. The retry amplification creates a positive feedback loop. Prevention: use exponential backoff with jitter, circuit breakers, and set retry budgets (max 3 retries, not unlimited).

**Example:**
```go
// BAD: immediate retry creates cascade
for i := 0; i < 10; i++ {
    ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
    resp, err := callService(ctx)
    cancel()
    if err == nil { return resp }
    // immediate retry → doubles load on struggling service
}

// GOOD: exponential backoff with jitter
for i := 0; i < 3; i++ {
    ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
    resp, err := callService(ctx)
    cancel()
    if err == nil { return resp }
    backoff := time.Duration(1<<i) * time.Second
    jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
    time.Sleep(backoff + jitter)
}
```

**Real-time Scenario:** A Black Friday traffic spike causes the payment service to slow from 100ms to 2s. 100 callers timeout at 1s and retry 3 times each, creating 300 additional requests → service collapses under 4x load.

---

### Q7: How do you choose timeout values?

**Answer:** Base timeouts on measured P99 latency + a safety buffer (typically 2-3x P99). For SLA-driven timeouts, work backward from the SLA. Guidelines: connection timeout = 1-5s, read/write timeout = 5-30s, total request timeout = SLA minus overhead. Always measure in production — don't guess. Too tight: spurious failures during normal variance. Too loose: slow failure detection.

**Example:**
```go
// Measured P99 latency: 200ms
// Timeout: 3x P99 = 600ms, round up to 1s
ctx, cancel := context.WithTimeout(ctx, 1*time.Second)

// SLA-driven: 5s total SLA
// API layer overhead: 500ms
// Service call: 4.5s budget
// DB query: use remaining time
if dl, ok := ctx.Deadline(); ok {
    remaining := time.Until(dl)
    dbTimeout := remaining - 500*time.Millisecond // reserve 500ms for response
    dbCtx, cancel := context.WithTimeout(ctx, dbTimeout)
}
```

**Real-time Scenario:** A team set all timeouts to 30 seconds "to be safe." When the database went down, every request waited 30 seconds before failing. Setting timeouts to 2 seconds (3x the P99 of 600ms) made failures visible in 2 seconds.

---

### Q8: Memory leak with `time.After` in a loop?

**Answer:** `time.After` creates a new timer that isn't garbage collected until it fires. In a loop processing millions of messages, this creates millions of timers — each consuming ~200 bytes — causing an OOM. Fix: use `time.NewTimer` and `Reset()` it each iteration, or use `context.WithTimeout` which cleans up via `cancel()`.

**Example:**
```go
// MEMORY LEAK: each iteration creates a timer that lives until it fires
for msg := range messages {
    select {
    case process <- msg:
    case <-time.After(5 * time.Second): // leaked timer!
        log.Println("timeout")
    }
}

// FIXED: reuse timer
timer := time.NewTimer(5 * time.Second)
for msg := range messages {
    timer.Reset(5 * time.Second)
    select {
    case process <- msg:
        if !timer.Stop() { <-timer.C }
    case <-timer.C:
        log.Println("timeout")
    }
}
```

**Real-time Scenario:** A Kafka consumer processing 100K messages/sec used `time.After` in its processing loop. Memory grew by 20MB/sec (100K timers * 200 bytes) and OOM'd after 10 minutes. Switching to `time.NewTimer` + `Reset()` fixed it.

---

### Q9: How do you implement exponential backoff with deadline?

**Answer:** Combine exponential backoff (doubling wait time on each retry) with a context deadline. Before each retry, check if the remaining time is enough for the next attempt. Add jitter to prevent thundering herd (all retriers retrying at the same time). Cap the maximum backoff to avoid excessively long waits.

**Example:**
```go
func retryWithBackoff(ctx context.Context, fn func(context.Context) error) error {
    backoff := 100 * time.Millisecond
    maxBackoff := 10 * time.Second

    for attempt := 0; ; attempt++ {
        err := fn(ctx)
        if err == nil { return nil }

        // Check if deadline allows another retry
        if dl, ok := ctx.Deadline(); ok {
            if time.Until(dl) < backoff {
                return fmt.Errorf("deadline too close for retry: %w", err)
            }
        }

        jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
        select {
        case <-time.After(backoff + jitter):
        case <-ctx.Done():
            return ctx.Err()
        }

        backoff *= 2
        if backoff > maxBackoff { backoff = maxBackoff }
    }
}
```

**Real-time Scenario:** A service calling a flaky third-party API retries with backoff (100ms, 200ms, 400ms, ...) but stops retrying when the 5-second request deadline approaches — returning an error rather than starting a retry that can't complete in time.

---

### Q10: What is deadline propagation in gRPC?

**Answer:** gRPC automatically propagates deadlines from client to server via metadata. When a client sets a deadline, the server's context automatically has the same deadline (adjusted for clock skew). If the client cancels or the deadline expires, the server's context is cancelled too. This is built into the gRPC protocol — no application-level code needed. HTTP doesn't have this; you must implement it manually.

**Example:**
```go
// Client: set deadline
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
resp, err := client.GetUser(ctx, &pb.GetUserRequest{Id: userID})

// Server: deadline is automatically in ctx
func (s *Server) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.User, error) {
    // ctx already has the client's deadline!
    dl, _ := ctx.Deadline()
    log.Printf("must respond by: %v", dl)

    // Pass ctx to downstream — deadline propagates further
    user, err := s.db.QueryContext(ctx, "SELECT ...")
    return user, err
}
```

**Real-time Scenario:** A 3-layer gRPC call chain (gateway → service → database service) automatically propagates a 5-second deadline. Each layer sees the remaining time and can make decisions about whether to attempt slow operations.

---

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

### Q1: How do you propagate errors from goroutines?

**Answer:** Goroutines can't return errors directly. Use channels: send `error` on a channel, receive in the parent goroutine. Or use `errgroup.Group` which handles this pattern. For fire-and-forget goroutines, log the error. Never silently swallow errors from goroutines — they represent work that may have failed.

**Example:**
```go
// Channel pattern:
errCh := make(chan error, 1)
go func() {
    errCh <- doWork() // send error (or nil)
}()
if err := <-errCh; err != nil {
    log.Printf("worker failed: %v", err)
}

// errgroup pattern:
g, ctx := errgroup.WithContext(context.Background())
g.Go(func() error { return doWork() })
if err := g.Wait(); err != nil {
    log.Printf("worker failed: %v", err)
}
```

**Real-time Scenario:** An API handler launches 3 concurrent database queries. Each sends errors on a channel. The handler collects all 3 results and returns a combined error if any query failed.

---

### Q2: What is errgroup and how does it work?

**Answer:** `errgroup.Group` (from `golang.org/x/sync/errgroup`) manages a group of goroutines that return errors. `g.Go(func() error{})` launches a goroutine. `g.Wait()` blocks until all goroutines complete and returns the first non-nil error. With `errgroup.WithContext`, the context is cancelled when any goroutine returns an error, enabling first-error-cancellation of sibling goroutines.

**Example:**
```go
g, ctx := errgroup.WithContext(ctx)

g.Go(func() error {
    return fetchUser(ctx, userID)
})
g.Go(func() error {
    return fetchOrders(ctx, userID)
})
g.Go(func() error {
    return fetchPreferences(ctx, userID)
})

if err := g.Wait(); err != nil {
    return fmt.Errorf("data fetch failed: %w", err)
}
```

**Real-time Scenario:** A product page loads user data, reviews, and recommendations concurrently with errgroup. If the user service is down, the context cancels the other two calls immediately — failing fast rather than waiting for all three.

---

### Q3: How do you handle multiple goroutines returning errors?

**Answer:** Collect all errors in a slice (protected by mutex) or use a buffered error channel. Then combine them: use `errors.Join` (Go 1.20+), or a custom multi-error type. Don't just return the first error — the caller needs to know ALL failures. `errgroup` only returns the first error; for all errors, use a custom collector.

**Example:**
```go
func runAll(tasks []func() error) error {
    var (
        mu   sync.Mutex
        errs []error
        wg   sync.WaitGroup
    )
    for _, task := range tasks {
        wg.Add(1)
        go func(t func() error) {
            defer wg.Done()
            if err := t(); err != nil {
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

**Real-time Scenario:** A data migration runs 50 concurrent record updates. 3 fail with constraint violations. The error report shows all 3 failures with specific record IDs, not just the first one.

---

### Q4: What happens when a goroutine panics?

**Answer:** If a goroutine panics and the panic is not recovered, the entire program crashes — all goroutines are terminated. The panic is NOT propagated to the parent goroutine. To prevent crashes: use `defer recover()` in every goroutine that might panic, convert the panic to an error, and send it through the error channel. In HTTP servers, the default handler already recovers panics per-request.

**Example:**
```go
func safeGo(fn func() error) <-chan error {
    errCh := make(chan error, 1)
    go func() {
        defer func() {
            if r := recover(); r != nil {
                errCh <- fmt.Errorf("panic: %v\n%s", r, debug.Stack())
            }
        }()
        errCh <- fn()
    }()
    return errCh
}

// Usage:
errCh := safeGo(func() error {
    return riskyOperation() // if this panics, we catch it
})
```

**Real-time Scenario:** A worker pool processing user-uploaded data has one corrupt file that causes an index-out-of-range panic. Without recovery, the entire server crashes. With `safeGo`, only that job fails while the pool continues.

---

### Q5: How do you aggregate errors from concurrent operations?

**Answer:** Three approaches: (1) `errors.Join(errs...)` (Go 1.20+) — joins multiple errors, supports `errors.Is`/`errors.As`. (2) Custom multi-error type that implements `Error()` and `Unwrap() []error`. (3) Structured error with individual results. Use a mutex-protected slice to collect errors from goroutines, then join after `wg.Wait()`.

**Example:**
```go
type MultiError struct {
    errors []error
}

func (me *MultiError) Error() string {
    msgs := make([]string, len(me.errors))
    for i, e := range me.errors {
        msgs[i] = e.Error()
    }
    return strings.Join(msgs, "; ")
}

func (me *MultiError) Unwrap() []error { return me.errors }

// Go 1.20+: errors.Join is built-in
err := errors.Join(err1, err2, err3)
if errors.Is(err, sql.ErrNoRows) { /* checks all wrapped errors */ }
```

**Real-time Scenario:** A bulk API endpoint creates 100 users concurrently. 5 fail (duplicate email). The aggregated error response lists all 5 failures with their indices so the client can retry just those.

---

### Q6: Difference between errgroup and WaitGroup?

**Answer:** `sync.WaitGroup` waits for goroutines to finish but doesn't handle errors — you must manage error collection yourself. `errgroup.Group` adds error handling: `Go()` accepts `func() error`, `Wait()` returns the first error, and `WithContext` cancels siblings on first error. Use `WaitGroup` for fire-and-forget goroutines; use `errgroup` when you need to know if work succeeded.

**Example:**
```go
// WaitGroup: no error handling
var wg sync.WaitGroup
wg.Add(1)
go func() {
    defer wg.Done()
    doWork() // error is lost
}()
wg.Wait() // no way to know if doWork failed

// errgroup: built-in error handling
g, ctx := errgroup.WithContext(ctx)
g.Go(func() error {
    return doWork() // error is captured
})
err := g.Wait() // returns the error
```

**Real-time Scenario:** A team replaced all `WaitGroup` + error channel patterns with `errgroup` during a refactor — the code was 40% shorter and eliminated 3 bugs where errors were being silently dropped.

---

### Q7: How do you implement first-error-cancellation?

**Answer:** Use `errgroup.WithContext`. When any goroutine returns a non-nil error, the shared context is cancelled, signaling all other goroutines to stop. Each goroutine must check `ctx.Done()` to actually respond to cancellation. This prevents wasted work: if one of 10 parallel tasks fails, the other 9 stop immediately.

**Example:**
```go
g, ctx := errgroup.WithContext(ctx)

for _, url := range urls {
    url := url
    g.Go(func() error {
        req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return err // cancels ctx → other goroutines see ctx.Done()
        }
        defer resp.Body.Close()
        return processResponse(resp)
    })
}

err := g.Wait() // returns first error; all others are cancelled
```

**Real-time Scenario:** A health check pings 20 services concurrently. If the critical database health check fails, the context cancels all other checks — the system is unhealthy regardless of other services' status.

---

### Q8: How do you add context to errors from goroutines?

**Answer:** Wrap errors with `fmt.Errorf("context: %w", err)` inside each goroutine before sending them. Include the goroutine's identity (worker ID, task name, item being processed) so the caller can identify which concurrent operation failed. This is especially important when running the same function in multiple goroutines.

**Example:**
```go
g, ctx := errgroup.WithContext(ctx)
for i, item := range items {
    i, item := i, item
    g.Go(func() error {
        if err := process(ctx, item); err != nil {
            return fmt.Errorf("worker %d processing %q: %w", i, item.ID, err)
        }
        return nil
    })
}
// Error: "worker 3 processing 'order-456': connection refused"
```

**Real-time Scenario:** A batch processor running 100 goroutines reports "worker 47 processing 'invoice-789': foreign key constraint violation" — the operator knows exactly which item failed and why.

---

### Q9: How do you test error scenarios in concurrent code?

**Answer:** (1) Inject errors using interfaces/function parameters (dependency injection). (2) Use `sync.WaitGroup` to synchronize test assertions. (3) Use `context.WithCancel` to test cancellation paths. (4) Use `testing.T.Deadline()` to prevent tests from hanging. (5) Test race conditions with `-race` flag. (6) Use `errgroup` results to verify error propagation.

**Example:**
```go
func TestConcurrentErrors(t *testing.T) {
    failingFn := func(ctx context.Context) error {
        return errors.New("injected error")
    }
    successFn := func(ctx context.Context) error {
        return nil
    }

    g, ctx := errgroup.WithContext(context.Background())
    g.Go(func() error { return failingFn(ctx) })
    g.Go(func() error { return successFn(ctx) })

    err := g.Wait()
    if err == nil {
        t.Fatal("expected error, got nil")
    }
    if !strings.Contains(err.Error(), "injected error") {
        t.Errorf("unexpected error: %v", err)
    }
}
```

**Real-time Scenario:** A CI pipeline runs tests with `-race -count=100` to catch intermittent race conditions in error handling paths that only appear under specific goroutine scheduling.

---

### Q10: What is the error channel pattern?

**Answer:** The error channel pattern sends `error` values on a dedicated channel from goroutines to the coordinator. The coordinator selects on the error channel alongside other channels (done, results). This allows non-blocking error checking and integration with `select`-based event loops. Buffer the channel to prevent goroutines from blocking on send.

**Example:**
```go
func process(ctx context.Context, items []Item) error {
    results := make(chan Result, len(items))
    errs := make(chan error, len(items))

    for _, item := range items {
        go func(it Item) {
            result, err := transform(it)
            if err != nil {
                errs <- err
                return
            }
            results <- result
        }(item)
    }

    for range items {
        select {
        case r := <-results:
            collect(r)
        case err := <-errs:
            return fmt.Errorf("processing failed: %w", err)
        case <-ctx.Done():
            return ctx.Err()
        }
    }
    return nil
}
```

**Real-time Scenario:** A file processing pipeline uses error channels to report per-file errors back to the orchestrator, which can decide to stop early (fail-fast) or continue processing the remaining files.

---

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

### Q1: How do you implement graceful shutdown in a Go HTTP server?

**Answer:** Use `http.Server.Shutdown(ctx)` which stops accepting new connections and waits for active requests to complete (up to the context deadline). Listen for OS signals (`SIGTERM`, `SIGINT`) to trigger shutdown. The pattern: start server in a goroutine, block on signal, call `Shutdown` with a timeout context, then clean up resources.

**Example:**
```go
func main() {
    srv := &http.Server{Addr: ":8080", Handler: mux}
    go func() {
        if err := srv.ListenAndServe(); err != http.ErrServerClosed {
            log.Fatal(err)
        }
    }()

    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
    <-quit

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    if err := srv.Shutdown(ctx); err != nil {
        log.Printf("forced shutdown: %v", err)
    }
    log.Println("server stopped gracefully")
}
```

**Real-time Scenario:** A Kubernetes pod receives SIGTERM during rolling deployment. The server stops accepting new requests, finishes processing the 50 in-flight requests over 10 seconds, then exits cleanly — zero dropped requests.

---

### Q2: What signals should you handle?

**Answer:** **SIGTERM**: sent by orchestrators (Kubernetes, Docker, systemd) for graceful shutdown — always handle this. **SIGINT**: sent by Ctrl+C — handle for development convenience. **SIGQUIT**: causes core dump (don't catch — useful for debugging). **SIGKILL**: cannot be caught — the OS force-kills the process. In production, SIGTERM is the primary signal; Kubernetes sends SIGTERM, waits `terminationGracePeriodSeconds`, then sends SIGKILL.

**Example:**
```go
quit := make(chan os.Signal, 1)
signal.Notify(quit,
    syscall.SIGTERM, // orchestrator shutdown
    syscall.SIGINT,  // Ctrl+C
)
// Do NOT catch SIGKILL (can't) or SIGQUIT (want core dump)

sig := <-quit
log.Printf("received signal: %v", sig)
// begin graceful shutdown
```

**Real-time Scenario:** A Go service on Kubernetes handles SIGTERM with a 25-second graceful shutdown period. Kubernetes is configured with `terminationGracePeriodSeconds: 30`, giving 5 seconds of buffer before SIGKILL.

---

### Q3: How do you drain in-flight requests?

**Answer:** `http.Server.Shutdown()` automatically drains in-flight requests — it stops the listener (no new connections), then waits for active handlers to return. For non-HTTP workloads, use a `sync.WaitGroup`: increment when starting work, decrement when done, and `Wait()` during shutdown. Also close input channels to stop accepting new work.

**Example:**
```go
type Worker struct {
    wg   sync.WaitGroup
    jobs chan Job
}

func (w *Worker) Process(job Job) {
    w.wg.Add(1)
    go func() {
        defer w.wg.Done()
        handle(job)
    }()
}

func (w *Worker) Shutdown() {
    close(w.jobs)  // stop accepting new jobs
    w.wg.Wait()    // wait for in-flight jobs to complete
}
```

**Real-time Scenario:** A message queue consumer stops polling for new messages on SIGTERM, then waits for the 20 currently-processing messages to finish before acknowledging them and exiting.

---

### Q4: What is `signal.NotifyContext`?

**Answer:** `signal.NotifyContext` (Go 1.16+) creates a context that is automatically cancelled when the specified OS signal is received. It simplifies the signal-handling boilerplate — instead of creating a channel and selecting on it, you get a context that integrates with your existing context-based code. The returned `stop` function must be called to release resources.

**Example:**
```go
func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
    defer stop()

    srv := &http.Server{Addr: ":8080", Handler: mux}
    go func() { srv.ListenAndServe() }()

    <-ctx.Done() // blocks until SIGTERM/SIGINT
    stop()       // stop receiving signals

    shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    srv.Shutdown(shutdownCtx)
}
```

**Real-time Scenario:** A CLI tool uses `signal.NotifyContext` to pass a cancellable context to all goroutines. When the user presses Ctrl+C, the context cancels, all goroutines clean up, and the tool exits gracefully.

---

### Q5: How do you handle shutdown timeout?

**Answer:** Wrap the shutdown in a context with a deadline. If active requests don't complete within the deadline, `Shutdown` returns the context error and you force-close. Always have a maximum shutdown duration — otherwise a stuck request can prevent your process from ever exiting. Log which requests are still active when the timeout fires.

**Example:**
```go
func gracefulShutdown(srv *http.Server) {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    if err := srv.Shutdown(ctx); err != nil {
        log.Printf("shutdown timed out, forcing close: %v", err)
        srv.Close() // force close all connections
    }

    // Also close other resources with timeout:
    if err := db.Close(); err != nil {
        log.Printf("db close error: %v", err)
    }
}
```

**Real-time Scenario:** A streaming endpoint has a client with a long-lived connection. During shutdown, the 30-second timeout fires, the server force-closes the streaming connection, logs the client IP, and exits — preventing the deploy from hanging.

---

### Q6: In what order should you shut down components?

**Answer:** Shut down in **reverse order of startup** (LIFO): (1) Stop accepting new requests (close listeners). (2) Drain in-flight requests. (3) Close application-level resources (caches, message consumers). (4) Close infrastructure (database connections, message queues). (5) Flush logs/metrics. This ensures dependencies are available while work is being drained.

**Example:**
```go
func shutdown() {
    // 1. Stop accepting new work
    srv.Shutdown(ctx)

    // 2. Stop background workers
    close(workerChan)
    workerWg.Wait()

    // 3. Close application resources
    cache.Close()

    // 4. Close infrastructure
    db.Close()
    mqClient.Close()

    // 5. Flush observability
    tracer.Shutdown(ctx)
    logger.Sync()
}
```

**Real-time Scenario:** Shutting down the database before draining requests causes errors. Shutting down in reverse startup order ensures the database is available while the last requests complete, then closes the database cleanly.

---

### Q7: How does graceful shutdown work with Kubernetes?

**Answer:** Kubernetes sends SIGTERM, then waits `terminationGracePeriodSeconds` (default 30s) before sending SIGKILL. Your app should: (1) catch SIGTERM, (2) stop accepting new traffic, (3) fail health checks (readiness probe returns 503 — k8s removes pod from service), (4) drain in-flight requests, (5) clean up resources, (6) exit. The key nuance: there's a race between SIGTERM and the endpoint controller removing the pod from the service.

**Example:**
```go
func main() {
    var healthy atomic.Bool
    healthy.Store(true)

    // Readiness probe: return 503 during shutdown
    mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
        if !healthy.Load() {
            w.WriteHeader(503)
            return
        }
        w.WriteHeader(200)
    })

    // On SIGTERM: mark unhealthy, wait for k8s to remove from service
    <-sigChan
    healthy.Store(false)
    time.Sleep(5 * time.Second) // wait for endpoint propagation
    srv.Shutdown(ctx)
}
```

**Real-time Scenario:** Without the 5-second sleep after marking unhealthy, some load balancer nodes still route traffic to the shutting-down pod (race condition). The sleep ensures Kubernetes fully removes the pod from all endpoints before connections are closed.

---

### Q8: How do you handle long-running requests during shutdown?

**Answer:** Options: (1) **Wait with timeout** — give them a deadline and force-close after. (2) **Cancel their context** — pass a cancellable context to all handlers so they can check `ctx.Done()`. (3) **Migrate** — for long-running WebSocket/streaming connections, send a "reconnect" message so clients reconnect to healthy pods. (4) **Track and log** — identify which requests are slow to inform timeout tuning.

**Example:**
```go
func longRunningHandler(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    for i := 0; i < 1000; i++ {
        select {
        case <-ctx.Done():
            // server is shutting down — clean up and return
            log.Printf("request cancelled at iteration %d", i)
            return
        default:
            processChunk(i)
        }
    }
}
```

**Real-time Scenario:** A report generation endpoint takes 5 minutes. During shutdown, the handler detects context cancellation, saves partial results to a queue, and returns. A new pod picks up the partial work and completes it.

---

### Q9: How do you test graceful shutdown?

**Answer:** (1) Start the server in a test goroutine. (2) Send requests. (3) Send SIGTERM (or call `Shutdown` directly). (4) Verify in-flight requests complete successfully. (5) Verify new requests are rejected. (6) Verify resources are cleaned up. Use `httptest.Server` for unit tests and actual signal sending for integration tests.

**Example:**
```go
func TestGracefulShutdown(t *testing.T) {
    srv := startServer()

    // Start a long request
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        resp, err := http.Get(srv.URL + "/slow")
        assert.NoError(t, err)
        assert.Equal(t, 200, resp.StatusCode) // should complete
    }()

    time.Sleep(100 * time.Millisecond) // ensure request is in-flight
    srv.Shutdown(context.Background()) // trigger shutdown
    wg.Wait()                          // verify request completed

    // Verify new requests are rejected
    _, err := http.Get(srv.URL + "/test")
    assert.Error(t, err) // connection refused
}
```

**Real-time Scenario:** A CI pipeline runs graceful shutdown tests that start the server, fire 100 concurrent requests, trigger shutdown mid-flight, and verify all 100 requests complete successfully with no errors.

---

### Q10: Difference between SIGTERM and SIGKILL?

**Answer:** **SIGTERM** (signal 15): a polite request to terminate — the process can catch it, run cleanup code, and exit gracefully. **SIGKILL** (signal 9): an immediate, uncatchable kill — the OS terminates the process instantly with no cleanup. You can never handle SIGKILL in your code. SIGTERM is always sent first; SIGKILL is the last resort when a process doesn't respond to SIGTERM within the grace period.

**Example:**
```go
// SIGTERM: catchable — you can clean up
signal.Notify(ch, syscall.SIGTERM)
<-ch
cleanup()  // runs: close connections, flush logs, etc.
os.Exit(0)

// SIGKILL: NOT catchable — this code never runs
signal.Notify(ch, syscall.SIGKILL) // this registration is ignored
<-ch
cleanup() // NEVER executes — process is already dead
```

**Real-time Scenario:** A stuck Go process ignores SIGTERM (buggy signal handling). Kubernetes waits 30 seconds, then sends SIGKILL, which immediately terminates the process — database connections are leaked, in-flight writes may be corrupted.

---

### Q11: How do you handle shutdown with multiple servers?

**Answer:** Shut down all servers concurrently (not sequentially) to minimize total shutdown time. Use an `errgroup` or launch shutdown goroutines for each server with a shared context. Wait for all servers to finish draining. If one server takes too long, the shared context deadline cancels all others.

**Example:**
```go
func shutdownAll(ctx context.Context, servers ...*http.Server) error {
    g, ctx := errgroup.WithContext(ctx)
    for _, srv := range servers {
        srv := srv
        g.Go(func() error {
            return srv.Shutdown(ctx)
        })
    }
    return g.Wait()
}

// Usage:
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
shutdownAll(ctx, apiServer, adminServer, metricsServer)
```

**Real-time Scenario:** A service runs an API server (:8080), admin server (:8081), and metrics server (:9090). All three shut down concurrently — total shutdown time is the max of the three, not the sum.

---

### Q12: How do you prevent new work during shutdown?

**Answer:** Use an atomic flag or closed channel to signal "shutting down." Check this flag before accepting new work. For HTTP servers, `Shutdown()` handles this automatically (stops the listener). For custom work queues, close the input channel and check the shutdown flag before enqueuing. For background goroutines, select on a done channel.

**Example:**
```go
type Service struct {
    shuttingDown atomic.Bool
    jobs         chan Job
}

func (s *Service) Submit(job Job) error {
    if s.shuttingDown.Load() {
        return errors.New("service is shutting down")
    }
    s.jobs <- job
    return nil
}

func (s *Service) Shutdown() {
    s.shuttingDown.Store(true)  // reject new work
    close(s.jobs)               // drain existing work
    s.wg.Wait()                 // wait for completion
}
```

**Real-time Scenario:** A task scheduler returns HTTP 503 for new job submissions during shutdown while allowing the 200 currently-running tasks to complete, preventing a buildup of jobs that would never be processed.

---

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
