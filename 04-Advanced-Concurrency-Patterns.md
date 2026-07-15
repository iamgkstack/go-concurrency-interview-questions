# Part 4: Advanced Concurrency Patterns — Pub-Sub, Rate Limiting, Backpressure, Semaphore, Future/Promise, Event Processing

> Go Concurrency Interview Guide — From Beginner to Staff Engineer

---

## Table of Contents

1. [Publish-Subscribe Pattern](#1-publish-subscribe-pattern)
2. [Rate Limiting](#2-rate-limiting)
3. [Backpressure](#3-backpressure)
4. [Semaphore Pattern](#4-semaphore-pattern)
5. [Future/Promise Pattern](#5-futurepromise-pattern)
6. [Event Processing](#6-event-processing)

---

# 1. PUBLISH-SUBSCRIBE PATTERN

## Concept Overview

Pub-sub decouples message producers (publishers) from consumers (subscribers). Publishers broadcast messages to a topic; all subscribers on that topic receive a copy.

**Key differences from Fan-Out:**
- Fan-Out: one message goes to ONE worker (load balancing).
- Pub-Sub: one message goes to ALL subscribers (broadcast).

**Design decisions:**
- **Slow subscriber handling:** Drop messages? Buffer? Block publisher? Disconnect subscriber?
- **Delivery guarantees:** At-most-once (fire and forget) vs at-least-once (with ack).
- **Ordering:** Per-topic ordering? Global ordering?
- **Subscriber lifecycle:** Dynamic subscribe/unsubscribe.

## Common Interview Questions

### Q1: How does pub-sub differ from fan-out?

**Answer:** In **Fan-Out**, one message goes to exactly ONE worker (load balancing — work is distributed). In **Pub-Sub**, one message goes to ALL subscribers (broadcast — every subscriber gets a copy). Fan-out divides work; pub-sub replicates events. Fan-out uses a single shared channel consumed by multiple workers. Pub-sub gives each subscriber their own channel.

**Example:**
```go
// Fan-Out: one message, one worker
jobs := make(chan Job, 100)
for i := 0; i < 10; i++ {
    go worker(jobs) // 10 workers share one channel
}
jobs <- job // only ONE worker processes this

// Pub-Sub: one message, ALL subscribers
func (bus *EventBus) Publish(topic string, msg interface{}) {
    for _, subCh := range bus.subs[topic] {
        subCh <- msg // every subscriber gets a copy
    }
}
```

**Real-time Scenario:** An order service uses fan-out to distribute orders across 10 processing workers (load balance), and pub-sub to notify the analytics, notification, and inventory services of each new order (broadcast).

---

### Q2: How do you handle slow subscribers?

**Answer:** Four strategies: (1) **Drop messages** — use non-blocking send (`select` with `default`); fast subscribers get all messages, slow ones miss some. (2) **Buffer** — give each subscriber a buffered channel to absorb bursts. (3) **Backpressure** — block the publisher until all subscribers consume (slows the whole system). (4) **Disconnect** — detect slow subscribers and remove them. Usually combine buffering + drop for production systems.

**Example:**
```go
func (bus *EventBus) Publish(topic string, msg interface{}) {
    for id, subCh := range bus.subs[topic] {
        select {
        case subCh <- msg:
            // delivered
        default:
            // subscriber too slow — drop message
            bus.dropCount[id]++
            if bus.dropCount[id] > 100 {
                bus.Unsubscribe(topic, id) // auto-disconnect
            }
        }
    }
}
```

**Real-time Scenario:** A real-time stock ticker drops stale price updates for slow WebSocket clients rather than buffering them — showing a 5-second-old price is worse than showing the latest price with gaps.

---

### Q3: How do you implement topic-based routing?

**Answer:** Use a map of topic string to subscriber list. Each subscriber registers for specific topics. On publish, look up the topic in the map and deliver to all registered subscribers. For hierarchical topics (e.g., `orders.created.us-east`), support prefix matching. Protect the map with `RWMutex` for concurrent access.

**Example:**
```go
type EventBus struct {
    mu   sync.RWMutex
    subs map[string][]chan interface{} // topic -> subscribers
}

func (bus *EventBus) Subscribe(topic string) <-chan interface{} {
    bus.mu.Lock()
    defer bus.mu.Unlock()
    ch := make(chan interface{}, 100)
    bus.subs[topic] = append(bus.subs[topic], ch)
    return ch
}

func (bus *EventBus) Publish(topic string, msg interface{}) {
    bus.mu.RLock()
    defer bus.mu.RUnlock()
    for _, ch := range bus.subs[topic] {
        ch <- msg
    }
}
```

**Real-time Scenario:** A microservices event bus routes `user.created` events to the email service, `order.created` to the fulfillment service, and `payment.*` to the finance service using topic-based routing.

---

### Q4: How do you handle subscriber disconnection?

**Answer:** Detect disconnection via: (1) context cancellation (subscriber's context is cancelled), (2) channel close (subscriber closes their channel), or (3) heartbeat timeout (subscriber stops acknowledging). On disconnection, remove the subscriber from the topic's subscriber list and close their channel. Use a cleanup goroutine or detect during publish (failed send).

**Example:**
```go
func (bus *EventBus) Subscribe(ctx context.Context, topic string) <-chan interface{} {
    ch := make(chan interface{}, 100)
    bus.mu.Lock()
    id := bus.nextID
    bus.nextID++
    bus.subs[topic][id] = ch
    bus.mu.Unlock()

    go func() {
        <-ctx.Done() // subscriber disconnected
        bus.mu.Lock()
        delete(bus.subs[topic], id)
        close(ch)
        bus.mu.Unlock()
    }()
    return ch
}
```

**Real-time Scenario:** A WebSocket server subscribes each client to topics. When a client disconnects (context cancelled), the subscription is automatically cleaned up, preventing goroutine leaks and sends to closed channels.

---

### Q5: What is the difference between at-most-once and at-least-once in pub-sub?

**Answer:** **At-most-once**: fire and forget — the message is sent once with no retry. If the subscriber misses it, the message is lost. Fast but unreliable. **At-least-once**: the publisher retries until the subscriber acknowledges. The message is guaranteed to arrive but may be delivered multiple times (requiring idempotent handling). **Exactly-once** requires deduplication on top of at-least-once.

**Example:**
```go
// At-most-once: non-blocking send, no retry
select {
case subCh <- msg:
default:
    // lost — no retry
}

// At-least-once: retry until acknowledged
func publishWithAck(ch chan Msg, msg Msg, ackCh chan bool) {
    for {
        ch <- msg
        select {
        case <-ackCh:
            return // acknowledged
        case <-time.After(5 * time.Second):
            // retry — subscriber may process twice
        }
    }
}
```

**Real-time Scenario:** A logging system uses at-most-once (losing a log line is acceptable). A payment notification system uses at-least-once (losing a payment confirmation is unacceptable, so the handler must be idempotent).

---

### Q6: How do you scale a pub-sub system?

**Answer:** Scaling strategies: (1) **Shard by topic** — different topics handled by different nodes. (2) **Fan-out within subscribers** — each subscriber runs a worker pool. (3) **Partition subscribers** — for high-volume topics, partition subscribers across nodes. (4) **Use a message broker** (Kafka, NATS, Redis Pub/Sub) for cross-process pub-sub. (5) **Batch messages** — publish batches instead of individual messages.

**Example:**
```go
// Sharded pub-sub: topics distributed across buses
type ShardedBus struct {
    shards []*EventBus
}

func (sb *ShardedBus) getShard(topic string) *EventBus {
    h := fnv.New32a()
    h.Write([]byte(topic))
    return sb.shards[h.Sum32()%uint32(len(sb.shards))]
}

func (sb *ShardedBus) Publish(topic string, msg interface{}) {
    sb.getShard(topic).Publish(topic, msg)
}
```

**Real-time Scenario:** A chat application with 1M users shards chat room topics across 100 event bus instances — each instance handles ~10K rooms, and subscribing to a room routes to the correct shard.

---

### Q7: How do you implement wildcard subscriptions?

**Answer:** Support pattern matching on topic strings. Common patterns: `*` matches one level (`orders.*` matches `orders.created` but not `orders.us.created`), `**` or `#` matches multiple levels (`orders.#` matches `orders.created.us`). On publish, iterate through all subscriptions and check pattern matches. Use a trie data structure for efficient matching with many subscriptions.

**Example:**
```go
func topicMatches(pattern, topic string) bool {
    patParts := strings.Split(pattern, ".")
    topParts := strings.Split(topic, ".")
    for i, p := range patParts {
        if p == "#" { return true } // match rest
        if i >= len(topParts) { return false }
        if p != "*" && p != topParts[i] { return false }
    }
    return len(patParts) == len(topParts)
}

// Subscribe("orders.*") matches:
//   "orders.created" ✓
//   "orders.updated" ✓
//   "orders.created.us" ✗
```

**Real-time Scenario:** A monitoring system subscribes to `servers.*.cpu` to receive CPU metrics from all servers, and `servers.prod-web-01.#` to receive all metrics from a specific server.

---

### Q8: How do you prevent a slow subscriber from affecting others?

**Answer:** Give each subscriber its own buffered channel and publish in a goroutine per subscriber (or use non-blocking sends). This way, a slow subscriber's full buffer doesn't block the publisher or delay messages to other subscribers. Additionally, set a per-subscriber channel capacity and drop/disconnect when full.

**Example:**
```go
func (bus *EventBus) Publish(topic string, msg interface{}) {
    bus.mu.RLock()
    subs := bus.subs[topic] // snapshot
    bus.mu.RUnlock()

    for _, sub := range subs {
        sub := sub
        go func() { // separate goroutine per subscriber
            select {
            case sub.ch <- msg:
            case <-time.After(100 * time.Millisecond):
                // subscriber too slow — drop this message
            }
        }()
    }
}
```

**Real-time Scenario:** A notification system publishes to email, SMS, and push notification subscribers. The SMS gateway is slow (500ms), but email and push subscribers receive messages instantly because each has an independent delivery goroutine.

---

### Q9: How do you implement message replay?

**Answer:** Store published messages in an ordered log (slice, ring buffer, or persistent storage) with sequence numbers. New subscribers or reconnecting subscribers specify a "last seen" sequence number and receive all messages from that point forward. Add a retention policy (time-based or size-based) to bound storage. This is the core idea behind Kafka's consumer offset model.

**Example:**
```go
type ReplayableBus struct {
    mu       sync.RWMutex
    messages []Message
    subs     map[int]chan Message
}

func (rb *ReplayableBus) ReplayFrom(seq int) <-chan Message {
    ch := make(chan Message, 100)
    go func() {
        rb.mu.RLock()
        for _, msg := range rb.messages[seq:] {
            ch <- msg // replay historical messages
        }
        rb.mu.RUnlock()
        // then continue with live messages...
    }()
    return ch
}
```

**Real-time Scenario:** A real-time dashboard reconnects after a network blip and replays the last 30 seconds of events to fill in the gap, ensuring the dashboard accurately reflects the current state.

---

### Q10: What is a consumer group and how does it differ from pub-sub?

**Answer:** In pub-sub, every subscriber gets every message (broadcast). In a consumer group, multiple consumers share the load — each message goes to exactly ONE consumer in the group (load balancing). Different consumer groups each get all messages (like pub-sub between groups). This combines pub-sub (between groups) and fan-out (within a group). Kafka uses this model extensively.

**Example:**
```go
type ConsumerGroup struct {
    members []chan Message
    next    int
}

func (cg *ConsumerGroup) Deliver(msg Message) {
    cg.members[cg.next%len(cg.members)] <- msg // round-robin
    cg.next++
}

// Group A: analytics (3 consumers share load)
// Group B: notification (2 consumers share load)
// Each message goes to ONE consumer in A AND ONE consumer in B
```

**Real-time Scenario:** An e-commerce platform has consumer group "analytics" (10 workers processing events for reports) and consumer group "notifications" (3 workers sending emails). Each order event reaches one analytics worker AND one notification worker.

---

## Coding Problems

### Problem 1: In-Process Event Bus

**Difficulty:** Medium  
**Problem:** Implement a thread-safe event bus with Subscribe, Unsubscribe, Publish.

```go
type EventBus struct {
    mu     sync.RWMutex
    subs   map[string]map[int]chan interface{}
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
    id := eb.nextID
    eb.nextID++
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
        // Non-blocking send — drop if subscriber buffer is full
        select {
        case ch <- msg:
        default:
            // subscriber is slow — message dropped
        }
    }
}
```

**Variations:** Wildcard topics (`events.*`), message filtering, replay buffer.  
**Real-world Relevance:** Event-driven architectures, notification systems, microservice communication.

---

### Problem 2: Pub-Sub with Slow Subscriber Strategies

**Difficulty:** Hard  
**Problem:** Support three strategies: DropOldest, DropNewest, Block.

```go
type OverflowStrategy int
const (
    DropNewest OverflowStrategy = iota
    DropOldest
    Block
)

type Subscriber struct {
    ch       chan interface{}
    strategy OverflowStrategy
}

func (s *Subscriber) Send(msg interface{}) {
    switch s.strategy {
    case Block:
        s.ch <- msg // blocks if full
    case DropNewest:
        select {
        case s.ch <- msg:
        default: // drop this message
        }
    case DropOldest:
        select {
        case s.ch <- msg:
        default:
            <-s.ch     // discard oldest
            s.ch <- msg // send new
        }
    }
}
```

**Real-world Relevance:** Real-time data feeds (stock tickers), WebSocket broadcast, log streaming.

---

### Problem 3: Partitioned Pub-Sub (Kafka-style)

**Difficulty:** Hard (Staff-level)  
**Problem:** Design pub-sub with partitions and consumer groups.

```go
type Partition struct {
    mu       sync.Mutex
    messages []Message
    offset   int64
}

type Topic struct {
    partitions []*Partition
    numParts   int
}

type ConsumerGroup struct {
    mu          sync.Mutex
    assignments map[string][]int // consumerID -> partition indices
    committed   map[int]int64    // partition -> committed offset
}

// Publish to a partition based on key hash
func (t *Topic) Publish(key string, msg Message) {
    partIdx := hash(key) % t.numParts
    p := t.partitions[partIdx]
    p.mu.Lock()
    defer p.mu.Unlock()
    msg.Offset = p.offset
    p.messages = append(p.messages, msg)
    p.offset++
}

// Consumer reads from assigned partitions
func (cg *ConsumerGroup) Consume(consumerID string, t *Topic) <-chan Message {
    out := make(chan Message)
    go func() {
        defer close(out)
        cg.mu.Lock()
        partitions := cg.assignments[consumerID]
        cg.mu.Unlock()
        for _, pIdx := range partitions {
            p := t.partitions[pIdx]
            offset := cg.committed[pIdx]
            // Read from offset forward
            p.mu.Lock()
            for i := offset; i < int64(len(p.messages)); i++ {
                out <- p.messages[i]
            }
            p.mu.Unlock()
        }
    }()
    return out
}
```

**Real-world Relevance:** Kafka consumer group implementation, NATS JetStream, Pulsar.

---

# 2. RATE LIMITING

## Concept Overview

Rate limiting controls the frequency of operations — essential for protecting services and respecting API quotas.

**Algorithms:**

| Algorithm | How It Works | Burst Handling | Memory |
|-----------|-------------|----------------|--------|
| **Token Bucket** | Tokens added at fixed rate; request takes a token | Yes (bucket can fill up) | O(1) |
| **Leaky Bucket** | Requests enter bucket, leak at fixed rate | No (smooths bursts) | O(N) |
| **Fixed Window** | Count requests per time window | Boundary burst (2x at window edge) | O(1) |
| **Sliding Window** | Count requests in sliding time range | Accurate but more complex | O(N) |

**Go standard library:**
- `golang.org/x/time/rate` — Token bucket implementation with `Allow()`, `Wait()`, `Reserve()`.
- `time.Ticker` — Simple but no burst support.

## Common Interview Questions

### Q1: What is the difference between token bucket and leaky bucket?

**Answer:** **Token bucket**: tokens accumulate at a fixed rate (up to a burst limit). Each request consumes a token. If no tokens available, request is rejected or waits. Allows bursts up to the bucket size. **Leaky bucket**: requests enter a queue (bucket) and are processed at a fixed output rate. Excess requests overflow (are dropped). Provides a perfectly smooth output rate. Token bucket allows controlled bursts; leaky bucket enforces strict smoothing.

**Example:**
```go
// Token bucket: allows bursts
type TokenBucket struct {
    tokens    float64
    maxTokens float64
    rate      float64 // tokens/sec
    lastTime  time.Time
}

func (tb *TokenBucket) Allow() bool {
    now := time.Now()
    tb.tokens += tb.rate * now.Sub(tb.lastTime).Seconds()
    if tb.tokens > tb.maxTokens { tb.tokens = tb.maxTokens }
    tb.lastTime = now
    if tb.tokens >= 1 {
        tb.tokens--
        return true // burst allowed
    }
    return false
}
```

**Real-time Scenario:** An API uses token bucket (allows 100-request bursts) for user-facing endpoints (good UX during page loads that fire multiple requests), and leaky bucket (strict 10 req/sec) for background batch jobs.

---

### Q2: How do you implement rate limiting with `time.Ticker`?

**Answer:** `time.Ticker` creates a channel that receives ticks at a fixed interval. Each tick represents permission to process one request. By reading from the ticker channel before processing, you limit throughput to the tick rate. This is a simple leaky-bucket implementation. For burst support, use a buffered channel or `time.Ticker` + token counter.

**Example:**
```go
func rateLimitedProcessor(requests <-chan Request) {
    ticker := time.NewTicker(time.Millisecond * 10) // 100 req/sec
    defer ticker.Stop()

    for req := range requests {
        <-ticker.C // wait for next tick — enforces rate
        process(req)
    }
}

// With burst support:
func burstLimiter(burstSize int) <-chan struct{} {
    ch := make(chan struct{}, burstSize)
    go func() {
        ticker := time.NewTicker(time.Millisecond * 10)
        for range ticker.C {
            select {
            case ch <- struct{}{}: // add token
            default: // bucket full
            }
        }
    }()
    return ch
}
```

**Real-time Scenario:** A web scraper limits requests to 10/sec per domain using a `time.Ticker` to respect robots.txt rate limits and avoid being blocked.

---

### Q3: How do you handle burst traffic?

**Answer:** Use a token bucket with a burst size larger than 1. The bucket accumulates tokens during idle periods (up to the max burst size), allowing a burst of requests when traffic arrives. After the burst exhausts tokens, requests are rate-limited to the refill rate. The burst size is a tuning parameter: too small wastes available capacity, too large causes downstream spikes.

**Example:**
```go
import "golang.org/x/time/rate"

// 100 requests/sec sustained, with bursts up to 200
limiter := rate.NewLimiter(100, 200)

func handleRequest(ctx context.Context) error {
    if err := limiter.Wait(ctx); err != nil {
        return err // context cancelled
    }
    return processRequest()
}
// After idle: 200 tokens accumulated → can handle 200 requests instantly
// Then rate-limited to 100/sec until idle again
```

**Real-time Scenario:** A CDN origin allows burst traffic during cache misses (many simultaneous requests for the same new content) but limits sustained throughput to prevent origin overload.

---

### Q4: What is the sliding window algorithm?

**Answer:** Sliding window rate limiting tracks requests within a moving time window. Two variants: **Fixed window** — counts requests in fixed intervals (e.g., 0:00-0:01, 0:01-0:02), but allows 2x burst at boundaries. **Sliding window** — counts requests in the last N seconds from the current time, providing smoother rate limiting. Implementation: use a sorted set of timestamps, or approximate with weighted average of two fixed windows.

**Example:**
```go
type SlidingWindow struct {
    mu       sync.Mutex
    requests []time.Time
    limit    int
    window   time.Duration
}

func (sw *SlidingWindow) Allow() bool {
    sw.mu.Lock()
    defer sw.mu.Unlock()
    now := time.Now()
    cutoff := now.Add(-sw.window)
    // Remove expired entries
    i := sort.Search(len(sw.requests), func(i int) bool {
        return sw.requests[i].After(cutoff)
    })
    sw.requests = sw.requests[i:]
    if len(sw.requests) >= sw.limit {
        return false
    }
    sw.requests = append(sw.requests, now)
    return true
}
```

**Real-time Scenario:** A payment API uses sliding window (100 transactions per 60 seconds) to prevent the boundary-doubling problem of fixed windows — no user can make 200 transactions by timing requests around the minute boundary.

---

### Q5: How do you implement distributed rate limiting?

**Answer:** Use a centralized store (Redis, etcd) to share rate limit state across multiple instances. Common approaches: (1) Redis `INCR` + `EXPIRE` for fixed window, (2) Redis sorted sets for sliding window, (3) Lua scripts for atomic check-and-increment, (4) token bucket with periodic sync to central store. Trade-off: centralized is precise but adds latency; local with periodic sync is fast but slightly inaccurate.

**Example:**
```go
// Redis-based rate limiter (Lua script for atomicity):
const luaScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local current = redis.call("INCR", key)
if current == 1 then
    redis.call("EXPIRE", key, window)
end
return current <= limit
`

func (rl *RedisLimiter) Allow(ctx context.Context, key string) bool {
    result, _ := rl.client.Eval(ctx, luaScript, []string{key}, rl.limit, rl.window).Bool()
    return result
}
```

**Real-time Scenario:** A multi-region API with 10 instances uses Redis-based rate limiting to enforce per-customer quotas. All instances check the same Redis counter, ensuring accurate limits regardless of which instance handles the request.

---

### Q6: How does `golang.org/x/time/rate` work?

**Answer:** It implements a token bucket rate limiter. `rate.NewLimiter(r, b)` creates a limiter that generates `r` tokens per second with burst size `b`. Methods: `Allow()` (non-blocking check), `Wait(ctx)` (blocks until token available), `Reserve()` (returns reservation with delay info). It's goroutine-safe and production-ready. The internal implementation uses lazy token calculation (doesn't need a background goroutine).

**Example:**
```go
import "golang.org/x/time/rate"

limiter := rate.NewLimiter(10, 20) // 10/sec, burst 20

// Block until allowed:
if err := limiter.Wait(ctx); err != nil { return err }

// Non-blocking check:
if limiter.Allow() { process() }

// Reserve (check without consuming):
r := limiter.Reserve()
if !r.OK() { return errors.New("rate limit exceeded") }
time.Sleep(r.Delay()) // wait the required time
process()
```

**Real-time Scenario:** An HTTP middleware wraps each endpoint with `limiter.Wait(ctx)` — during normal traffic, requests pass through instantly; during spikes, goroutines block briefly until tokens are available, providing natural backpressure.

---

### Q7: How do you rate limit per-user vs globally?

**Answer:** **Global**: one limiter shared by all users (protects backend capacity). **Per-user**: a separate limiter per user/API key (ensures fairness). Per-user requires a map of limiters with cleanup for inactive users. Often both are used: global limit protects the system, per-user limit ensures fairness. Use `sync.Map` or a mutex-protected map with TTL-based eviction.

**Example:**
```go
type PerUserLimiter struct {
    mu       sync.Mutex
    limiters map[string]*rate.Limiter
    rate     rate.Limit
    burst    int
}

func (p *PerUserLimiter) GetLimiter(userID string) *rate.Limiter {
    p.mu.Lock()
    defer p.mu.Unlock()
    if lim, ok := p.limiters[userID]; ok {
        return lim
    }
    lim := rate.NewLimiter(p.rate, p.burst)
    p.limiters[userID] = lim
    return lim
}

// Global + per-user:
var globalLimiter = rate.NewLimiter(10000, 1000)
func handler(userID string, req Request) {
    globalLimiter.Wait(ctx)                   // global limit
    perUser.GetLimiter(userID).Wait(ctx)      // per-user limit
}
```

**Real-time Scenario:** A SaaS API enforces per-tenant rate limits (100 req/sec per tenant) and a global limit (50,000 req/sec total). A heavy tenant can't starve others, and total load can't exceed backend capacity.

---

### Q8: What is adaptive rate limiting?

**Answer:** Adaptive rate limiting dynamically adjusts the allowed rate based on system health signals (error rate, latency, CPU usage, downstream response times). When the system is healthy, the rate increases. When degradation is detected (high latency, errors), the rate decreases. This avoids the static rate-limit problem of being too conservative during normal operation or too permissive during degradation.

**Example:**
```go
type AdaptiveLimiter struct {
    mu       sync.Mutex
    limiter  *rate.Limiter
    minRate  rate.Limit
    maxRate  rate.Limit
}

func (al *AdaptiveLimiter) Adjust(errorRate float64) {
    al.mu.Lock()
    defer al.mu.Unlock()
    if errorRate > 0.05 {
        newRate := al.limiter.Limit() * 0.5  // halve on errors (AIMD)
        if newRate < al.minRate { newRate = al.minRate }
        al.limiter.SetLimit(newRate)
    } else {
        newRate := al.limiter.Limit() + 10   // slowly increase
        if newRate > al.maxRate { newRate = al.maxRate }
        al.limiter.SetLimit(newRate)
    }
}
```

**Real-time Scenario:** A microservice reduces its request rate to a downstream API from 1000/sec to 100/sec when it detects the downstream's P99 latency exceeds 500ms, preventing cascading failure.

---

### Q9: What is the difference between rate limiting and throttling?

**Answer:** In practice, they're often used interchangeably, but strictly: **Rate limiting** rejects requests that exceed the limit (hard cap, returns 429). **Throttling** slows down requests by delaying them (soft cap, adds latency but eventually processes). In Go: `limiter.Allow()` + reject = rate limiting. `limiter.Wait(ctx)` + block = throttling. Rate limiting protects the system; throttling smooths traffic.

**Example:**
```go
// Rate limiting: reject excess requests
if !limiter.Allow() {
    http.Error(w, "rate limit exceeded", 429)
    return
}

// Throttling: slow down excess requests
if err := limiter.Wait(ctx); err != nil {
    http.Error(w, "request cancelled", 503)
    return
}
process(req) // request is processed, just delayed
```

**Real-time Scenario:** A public API rate-limits (429 response) to protect itself, while an internal job queue throttles (delays) to smooth out bursts — jobs eventually complete, just slower during peak times.

---

### Q10: How do you implement rate limiting with Redis?

**Answer:** Three common Redis approaches: (1) **INCR + EXPIRE** (fixed window) — increment a key per window, expire after window duration. (2) **Sorted set** (sliding window) — add timestamp to sorted set, count entries in window. (3) **Lua script** — atomic check-and-increment to avoid race conditions. All approaches use Redis as the shared state store for distributed rate limiting.

**Example:**
```go
// Approach 1: INCR + EXPIRE (fixed window)
func (rl *RedisLimiter) Allow(ctx context.Context, key string) bool {
    pipe := rl.client.Pipeline()
    incr := pipe.Incr(ctx, key)
    pipe.Expire(ctx, key, rl.window)
    pipe.Exec(ctx)
    return incr.Val() <= int64(rl.limit)
}

// Approach 2: Sorted set (sliding window)
func (rl *RedisLimiter) AllowSliding(ctx context.Context, key string) bool {
    now := time.Now().UnixNano()
    rl.client.ZRemRangeByScore(ctx, key, "0", fmt.Sprint(now-rl.window.Nanoseconds()))
    count := rl.client.ZCard(ctx, key).Val()
    if count < int64(rl.limit) {
        rl.client.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: now})
        return true
    }
    return false
}
```

**Real-time Scenario:** A payment gateway uses Redis sorted-set sliding window to enforce "max 5 transactions per minute per card" across all API servers, preventing fraud while allowing legitimate burst purchases.

---

### Q11: What is AIMD (Additive Increase Multiplicative Decrease)?

**Answer:** AIMD is a congestion control algorithm (used in TCP): increase the rate linearly when things are going well (additive increase), and decrease it multiplicatively when problems are detected (multiplicative decrease). This produces a sawtooth pattern that converges to the optimal rate. In rate limiting, AIMD adapts the allowed rate to match the system's actual capacity.

**Example:**
```go
type AIMDLimiter struct {
    mu      sync.Mutex
    rate    float64
    minRate float64
    maxRate float64
}

func (a *AIMDLimiter) OnSuccess() {
    a.mu.Lock()
    a.rate += 1 // additive increase
    if a.rate > a.maxRate { a.rate = a.maxRate }
    a.mu.Unlock()
}

func (a *AIMDLimiter) OnFailure() {
    a.mu.Lock()
    a.rate *= 0.5 // multiplicative decrease (halve)
    if a.rate < a.minRate { a.rate = a.minRate }
    a.mu.Unlock()
}
```

**Real-time Scenario:** A service calling an external API starts at 100 req/sec, increases by 1 req/sec each second (101, 102, ...), and when a timeout occurs, drops to 50 req/sec, then climbs back up — automatically finding the optimal rate.

---

### Q12: How do you handle rate limit errors gracefully?

**Answer:** Return HTTP 429 (Too Many Requests) with a `Retry-After` header indicating when the client can retry. Include rate limit headers (`X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`) so clients can proactively throttle themselves. On the client side, implement exponential backoff with jitter. Never let rate limit errors cascade — fail fast with clear feedback.

**Example:**
```go
func rateLimitMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        reservation := limiter.Reserve()
        if !reservation.OK() || reservation.Delay() > maxWait {
            w.Header().Set("Retry-After", "5")
            w.Header().Set("X-RateLimit-Limit", "100")
            w.Header().Set("X-RateLimit-Remaining", "0")
            http.Error(w, "rate limit exceeded", 429)
            return
        }
        time.Sleep(reservation.Delay())
        next.ServeHTTP(w, r)
    })
}
```

**Real-time Scenario:** A well-behaved API client reads the `Retry-After` header and backs off for 5 seconds, then retries. A poorly written client that ignores 429 and retries immediately gets progressively longer `Retry-After` values.

---

## Coding Problems

### Problem 1: Token Bucket Rate Limiter from Scratch

**Difficulty:** Medium  
**Problem:** Implement a token bucket with configurable rate and burst.

```go
type TokenBucket struct {
    mu         sync.Mutex
    tokens     float64
    maxTokens  float64
    refillRate float64 // tokens per second
    lastRefill time.Time
}

func NewTokenBucket(rate float64, burst int) *TokenBucket {
    return &TokenBucket{
        tokens:     float64(burst),
        maxTokens:  float64(burst),
        refillRate: rate,
        lastRefill: time.Now(),
    }
}

func (tb *TokenBucket) refill() {
    now := time.Now()
    elapsed := now.Sub(tb.lastRefill).Seconds()
    tb.tokens = min(tb.maxTokens, tb.tokens+elapsed*tb.refillRate)
    tb.lastRefill = now
}

// Allow: non-blocking — returns true if allowed
func (tb *TokenBucket) Allow() bool {
    tb.mu.Lock()
    defer tb.mu.Unlock()
    tb.refill()
    if tb.tokens >= 1 {
        tb.tokens--
        return true
    }
    return false
}

// Wait: blocking — waits until a token is available or context cancelled
func (tb *TokenBucket) Wait(ctx context.Context) error {
    for {
        tb.mu.Lock()
        tb.refill()
        if tb.tokens >= 1 {
            tb.tokens--
            tb.mu.Unlock()
            return nil
        }
        // Calculate wait time for next token
        waitTime := time.Duration((1 - tb.tokens) / tb.refillRate * float64(time.Second))
        tb.mu.Unlock()

        select {
        case <-time.After(waitTime):
            // retry
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}

func min(a, b float64) float64 {
    if a < b { return a }
    return b
}
```

**Real-world Relevance:** API rate limiting, resource access control.

---

### Problem 2: Per-Key Rate Limiter with Cleanup

**Difficulty:** Medium-Hard  
**Problem:** Rate limit per API key with automatic cleanup of inactive keys.

```go
type PerKeyLimiter struct {
    mu       sync.Mutex
    limiters map[string]*entry
    rate     rate.Limit
    burst    int
}

type entry struct {
    limiter  *rate.Limiter
    lastSeen time.Time
}

func NewPerKeyLimiter(r rate.Limit, burst int) *PerKeyLimiter {
    pkl := &PerKeyLimiter{
        limiters: make(map[string]*entry),
        rate:     r,
        burst:    burst,
    }
    go pkl.cleanup()
    return pkl
}

func (pkl *PerKeyLimiter) GetLimiter(key string) *rate.Limiter {
    pkl.mu.Lock()
    defer pkl.mu.Unlock()
    if e, ok := pkl.limiters[key]; ok {
        e.lastSeen = time.Now()
        return e.limiter
    }
    limiter := rate.NewLimiter(pkl.rate, pkl.burst)
    pkl.limiters[key] = &entry{limiter: limiter, lastSeen: time.Now()}
    return limiter
}

func (pkl *PerKeyLimiter) cleanup() {
    ticker := time.NewTicker(time.Minute)
    defer ticker.Stop()
    for range ticker.C {
        pkl.mu.Lock()
        for key, e := range pkl.limiters {
            if time.Since(e.lastSeen) > 10*time.Minute {
                delete(pkl.limiters, key)
            }
        }
        pkl.mu.Unlock()
    }
}
```

**Variations:** Configurable per-key rates (free vs paid tier), distributed with Redis.  
**Real-world Relevance:** API gateway rate limiting (Kong, Envoy).

---

### Problem 3: Sliding Window Rate Limiter

**Difficulty:** Hard  
**Problem:** Accurate sliding window counter.

```go
type SlidingWindowLimiter struct {
    mu         sync.Mutex
    timestamps []time.Time
    limit      int
    window     time.Duration
}

func NewSlidingWindow(limit int, window time.Duration) *SlidingWindowLimiter {
    return &SlidingWindowLimiter{limit: limit, window: window}
}

func (s *SlidingWindowLimiter) Allow() bool {
    s.mu.Lock()
    defer s.mu.Unlock()
    now := time.Now()
    cutoff := now.Add(-s.window)

    // Remove expired timestamps
    idx := sort.Search(len(s.timestamps), func(i int) bool {
        return s.timestamps[i].After(cutoff)
    })
    s.timestamps = s.timestamps[idx:]

    if len(s.timestamps) >= s.limit {
        return false
    }
    s.timestamps = append(s.timestamps, now)
    return true
}
```

**Trade-off:** O(N) memory per window, but most accurate. For memory efficiency, use weighted fixed windows.

---

### Problem 4: Adaptive Rate Limiter (AIMD)

**Difficulty:** Hard (FAANG-level)  
**Problem:** Rate limiter that adjusts based on downstream responses.

```go
type AdaptiveLimiter struct {
    mu          sync.Mutex
    currentRate float64
    minRate     float64
    maxRate     float64
    limiter     *rate.Limiter
    burst       int
}

func NewAdaptiveLimiter(initialRate, minRate, maxRate float64, burst int) *AdaptiveLimiter {
    al := &AdaptiveLimiter{
        currentRate: initialRate,
        minRate:     minRate,
        maxRate:     maxRate,
        burst:       burst,
        limiter:     rate.NewLimiter(rate.Limit(initialRate), burst),
    }
    return al
}

// Call after a successful response
func (al *AdaptiveLimiter) RecordSuccess() {
    al.mu.Lock()
    defer al.mu.Unlock()
    // Additive increase
    al.currentRate = math.Min(al.currentRate+1, al.maxRate)
    al.limiter.SetLimit(rate.Limit(al.currentRate))
}

// Call after a rate-limit error (429) or server error (503)
func (al *AdaptiveLimiter) RecordFailure() {
    al.mu.Lock()
    defer al.mu.Unlock()
    // Multiplicative decrease
    al.currentRate = math.Max(al.currentRate*0.5, al.minRate)
    al.limiter.SetLimit(rate.Limit(al.currentRate))
}

func (al *AdaptiveLimiter) Wait(ctx context.Context) error {
    return al.limiter.Wait(ctx)
}
```

**Real-world Relevance:** Netflix Concurrency Limits library, TCP congestion control, adaptive client throttling.

---

# 3. BACKPRESSURE

## Concept Overview

Backpressure is a flow-control mechanism where downstream components signal upstream to slow down when overwhelmed.

**Go channels naturally provide backpressure:**
- Bounded channel: when full, sender blocks → producer slows down.
- This propagates up the pipeline: if stage 3 is slow, stage 2 fills its output buffer, then stage 1 fills its buffer, etc.

**Strategies when backpressure is needed:**

| Strategy | Behavior | Use When |
|----------|----------|----------|
| Block | Sender blocks when buffer full | Data cannot be lost |
| Drop newest | Discard new items when full | Latest data is less important |
| Drop oldest | Discard old items when full | Latest data is more important |
| Load shed | Reject with error (503) | Client can retry |
| Sample | Process every Nth item | High volume, approximate is OK |

## Common Interview Questions

### Q1: What is backpressure and why is it important?

**Answer:** Backpressure is a feedback mechanism where a slow consumer signals the producer to slow down. Without backpressure, a fast producer overwhelms a slow consumer, causing unbounded memory growth (buffered messages pile up), increased latency, and eventually OOM crashes. Backpressure ensures system stability by propagating slowness upstream rather than hiding it with unbounded buffers.

**Example:**
```go
// WITHOUT backpressure: unbounded growth
ch := make(chan Data) // unbuffered blocks producer, but...
// if producer wraps in goroutine to avoid blocking: memory leak

// WITH backpressure: bounded buffer, producer slows down
ch := make(chan Data, 100) // producer blocks when buffer full
func produce(ch chan<- Data) {
    for data := range source {
        ch <- data // blocks when buffer full — backpressure!
    }
}
```

**Real-time Scenario:** A log aggregation pipeline without backpressure kept buffering logs in memory during a traffic spike, consuming 32GB RAM before crashing. Adding backpressure (bounded channels) caused the producer to slow down, keeping memory stable at 500MB.

---

### Q2: How do Go channels naturally provide backpressure?

**Answer:** Buffered channels in Go naturally implement backpressure: when the buffer is full, sends block the sender (producer). This blocking propagates upstream through the pipeline — each stage blocks when its output channel is full, automatically throttling the entire pipeline to the speed of the slowest stage. Unbuffered channels provide immediate backpressure (every send blocks until a receiver is ready).

**Example:**
```go
// Pipeline with natural backpressure:
stage1 := make(chan Data, 10)
stage2 := make(chan Data, 10)

go producer(stage1)    // blocks when stage1 buffer full
go transform(stage1, stage2) // blocks when stage2 buffer full
go consumer(stage2)    // processes at its own pace

// The entire pipeline runs at the speed of the slowest stage
```

**Real-time Scenario:** An ETL pipeline processing 1M records/sec has a slow "enrich" stage (1K/sec). Buffered channels between stages automatically throttle the parser to 1K/sec without any explicit rate limiting code.

---

### Q3: What are the strategies for handling backpressure?

**Answer:** (1) **Block** — let the producer wait (Go channels default). (2) **Drop** — discard excess items (use `select` with `default`). (3) **Buffer** — absorb bursts with bounded buffers (but buffer must drain). (4) **Sample** — keep only every Nth item. (5) **Batch** — accumulate items and process in bulk. (6) **Signal** — return errors/status codes upstream (HTTP 429). Choose based on data importance and latency requirements.

**Example:**
```go
// Strategy 1: Block (default Go channel behavior)
ch <- data // blocks when full

// Strategy 2: Drop when full
select {
case ch <- data:
    // sent
default:
    dropped.Add(1) // track drops
}

// Strategy 3: Buffer with overflow detection
if len(ch) > cap(ch)*80/100 {
    log.Warn("buffer 80% full, approaching backpressure")
}
ch <- data
```

**Real-time Scenario:** A real-time metrics system uses "drop oldest" strategy — if the consumer is slow, the oldest unprocessed metrics are dropped because only the latest values matter for dashboards.

---

### Q4: How does buffer size relate to backpressure?

**Answer:** Buffer size determines how much burst a system absorbs before backpressure kicks in. Too small: backpressure triggers on every minor fluctuation, reducing throughput. Too large: masks problems (producer thinks it's fast, but consumer is falling behind), increases memory usage, and delays backpressure signals. Ideal size: enough to absorb normal variance (typically a few seconds of throughput) without masking sustained overload.

**Example:**
```go
// Buffer too small (1): every slight variation triggers blocking
ch := make(chan Data, 1)

// Buffer too large (1M): masks problems, uses 1M * sizeof(Data) memory
ch := make(chan Data, 1000000)

// Right-sized buffer: absorb 1 second of burst
// If producer rate = 1000/sec, buffer = 1000
ch := make(chan Data, 1000)
```

**Real-time Scenario:** A message queue consumer uses a buffer of 2x the expected processing time variance. During a 200ms GC pause, the buffer absorbs incoming messages. During a 10-second outage, backpressure correctly propagates upstream.

---

### Q5: What is the difference between backpressure and rate limiting?

**Answer:** Backpressure is **reactive** — the consumer signals the producer to slow down based on current load. Rate limiting is **proactive** — it enforces a maximum rate regardless of consumer capacity. Backpressure adjusts dynamically (fast when consumer is fast, slow when consumer is slow). Rate limiting caps at a fixed rate even if the consumer could handle more. They're often used together: rate limiting at the edge, backpressure internally.

**Example:**
```go
// Rate limiting: fixed max rate regardless of capacity
limiter := rate.NewLimiter(1000, 100) // 1000/sec max, burst 100
limiter.Wait(ctx) // blocks to enforce rate

// Backpressure: adapts to consumer speed
ch <- data // blocks only when consumer is actually slow
// If consumer speeds up, producer speeds up automatically
```

**Real-time Scenario:** An API gateway rate-limits clients to 1000 req/sec (proactive). Internally, the pipeline uses backpressure — if the database slows from 500us to 50ms, the pipeline automatically processes fewer requests without changing any rate limit configuration.

---

### Q6: How do you implement backpressure in a pipeline?

**Answer:** Use bounded channels between pipeline stages. Each stage reads from its input channel, processes data, and writes to its output channel. When a downstream stage is slow, its input channel fills up, causing the upstream stage's write to block. This blocking cascades back through the pipeline, automatically throttling the entire system to the speed of the slowest stage.

**Example:**
```go
func pipeline(ctx context.Context) {
    raw := make(chan []byte, 100)
    parsed := make(chan Record, 50)
    enriched := make(chan Record, 20)

    go fetch(ctx, raw)           // produces raw data
    go parse(ctx, raw, parsed)   // blocks when parsed buffer full
    go enrich(ctx, parsed, enriched) // slowest stage
    go store(ctx, enriched)      // final stage

    // Backpressure flows: store <- enrich <- parse <- fetch
}
```

**Real-time Scenario:** A data ingestion pipeline reads from Kafka, parses JSON, enriches with user data (slow DB lookup), and writes to Elasticsearch. Backpressure through bounded channels keeps each stage's memory usage predictable.

---

### Q7: How do you monitor backpressure?

**Answer:** Monitor channel fill level: `len(ch)/cap(ch)` as a percentage. Track metrics: (1) channel fill percentage per stage, (2) time spent blocking on sends, (3) items processed per second per stage, (4) drop count (if using drop strategy). Alert when fill levels sustain above 80% — this indicates the downstream consumer can't keep up.

**Example:**
```go
func monitorPipeline(name string, ch interface{ Len() int; Cap() int }) {
    ticker := time.NewTicker(time.Second)
    for range ticker.C {
        fill := float64(ch.Len()) / float64(ch.Cap()) * 100
        metrics.Gauge("pipeline.fill_pct", fill, "stage", name)
        if fill > 80 {
            log.Warnf("stage %s at %.0f%% capacity", name, fill)
        }
    }
}

// For raw channels:
fill := float64(len(ch)) / float64(cap(ch)) * 100
```

**Real-time Scenario:** A Grafana dashboard shows pipeline stage fill levels as gauges. When the "enrich" stage hits 90%, the on-call engineer sees it and scales up the enrichment workers before backpressure cascades to the ingestion stage.

---

### Q8: What happens when backpressure is not handled?

**Answer:** Without backpressure, three failure modes emerge: (1) **OOM** — unbounded buffers grow until memory is exhausted and the process crashes. (2) **Cascading failure** — overwhelmed services timeout, callers retry, creating an amplification loop. (3) **Unbounded latency** — items queue for so long that they're stale by the time they're processed. These failures are insidious because they only appear under load.

**Example:**
```go
// NO backpressure — unbounded growth:
queue := []Data{} // grows forever
go func() {
    for data := range source {
        queue = append(queue, data) // 10K/sec production
    }
}()
go func() {
    for len(queue) > 0 {
        process(queue[0])  // 1K/sec consumption
        queue = queue[1:]  // queue grows by 9K/sec!
    }
}()
// After 1 hour: queue has 32M items, ~32GB memory → OOM
```

**Real-time Scenario:** A startup's log processing system worked perfectly in dev (low volume) but OOM'd in production because logs arrived 100x faster than they could be processed, and there was no backpressure to slow the log shipper.

---

## Coding Problems

### Problem 1: Backpressure-Aware Pipeline

**Difficulty:** Medium  
```go
type PipelineMonitor struct {
    stages []StageInfo
}

type StageInfo struct {
    Name     string
    Channel  interface{} // channel to monitor
    Len      func() int
    Cap      func() int
}

func (pm *PipelineMonitor) Report() {
    for _, s := range pm.stages {
        used := s.Len()
        total := s.Cap()
        pct := float64(used) / float64(total) * 100
        status := "OK"
        if pct > 80 {
            status = "BACKPRESSURE"
        }
        fmt.Printf("[%s] %d/%d (%.1f%%) %s\n", s.Name, used, total, pct, status)
    }
}
```

---

### Problem 2: Load Shedder

**Difficulty:** Hard  
**Problem:** Implement an HTTP handler that sheds load under pressure.

```go
type LoadShedder struct {
    queue    chan func()
    workers  int
    maxQueue int
}

func NewLoadShedder(workers, maxQueue int) *LoadShedder {
    ls := &LoadShedder{
        queue:    make(chan func(), maxQueue),
        workers:  workers,
        maxQueue: maxQueue,
    }
    for i := 0; i < workers; i++ {
        go func() {
            for fn := range ls.queue {
                fn()
            }
        }()
    }
    return ls
}

func (ls *LoadShedder) Handle(w http.ResponseWriter, r *http.Request) {
    done := make(chan struct{})
    task := func() {
        defer close(done)
        // process request
        w.Write([]byte("OK"))
    }

    select {
    case ls.queue <- task:
        <-done // wait for processing
    default:
        // Queue full — shed load
        w.WriteHeader(http.StatusServiceUnavailable)
        w.Write([]byte("server overloaded, try again later"))
    }
}
```

**Real-world Relevance:** HTTP server overload protection, API gateway load shedding, Envoy circuit breaking.

---

# 4. SEMAPHORE PATTERN

## Concept Overview

A semaphore controls access to a resource by maintaining a count of available permits.

**Binary semaphore** (1 permit) = mutex.  
**Counting semaphore** (N permits) = at most N concurrent accesses.

**Go implementations:**
1. **Buffered channel** — simple, idiomatic.
2. **`golang.org/x/sync/semaphore`** — weighted, context-aware, production-grade.

## Common Interview Questions

### Q1: What is a semaphore and how do you implement one in Go?

**Answer:** A semaphore limits the number of goroutines that can access a resource concurrently. In Go, the simplest implementation is a buffered channel — the buffer size is the concurrency limit. Sending to the channel "acquires" a slot; receiving "releases" it. When the buffer is full, additional acquires block. This is more idiomatic than porting traditional OS semaphore concepts.

**Example:**
```go
type Semaphore struct {
    ch chan struct{}
}

func NewSemaphore(n int) *Semaphore {
    return &Semaphore{ch: make(chan struct{}, n)}
}

func (s *Semaphore) Acquire() { s.ch <- struct{}{} }
func (s *Semaphore) Release() { <-s.ch }

// Usage: limit to 10 concurrent DB queries
sem := NewSemaphore(10)
for _, query := range queries {
    sem.Acquire()
    go func(q string) {
        defer sem.Release()
        db.Exec(q)
    }(query)
}
```

**Real-time Scenario:** An API gateway limits each downstream service to 50 concurrent requests using a semaphore, preventing a thundering herd from overwhelming backend services during traffic spikes.

---

### Q2: Difference between binary semaphore and mutex?

**Answer:** A binary semaphore (max count = 1) and a mutex both allow only one goroutine to proceed, but they differ in ownership: a mutex must be unlocked by the same goroutine that locked it, while a semaphore can be released by any goroutine. This makes semaphores suitable for producer-consumer signaling (one goroutine produces, another consumes the signal). In Go, channels serve this purpose more idiomatically.

**Example:**
```go
// Mutex: same goroutine must lock and unlock
var mu sync.Mutex
mu.Lock()
// only this goroutine can mu.Unlock()

// Binary semaphore: different goroutines can acquire/release
sem := make(chan struct{}, 1)
// Goroutine A:
sem <- struct{}{} // acquire
// Goroutine B:
<-sem             // release (different goroutine — OK)
```

**Real-time Scenario:** A work coordinator uses a binary semaphore: the scheduler "locks" it when assigning work, and the worker "unlocks" it when done — different goroutines, which would panic with a mutex but works fine with a semaphore.

---

### Q3: How do you implement a weighted semaphore?

**Answer:** A weighted semaphore allows acquiring multiple units at once (e.g., acquire 5 of 100 slots for a heavy operation). The standard `golang.org/x/sync/semaphore` package provides `Weighted` which supports this. With channels, you'd send/receive multiple tokens. Weighted semaphores are useful when operations consume different amounts of a shared resource.

**Example:**
```go
import "golang.org/x/sync/semaphore"

var sem = semaphore.NewWeighted(100) // 100 units total

func heavyOperation(ctx context.Context) error {
    if err := sem.Acquire(ctx, 10); err != nil { // acquire 10 units
        return err
    }
    defer sem.Release(10)
    return doHeavyWork()
}

func lightOperation(ctx context.Context) error {
    if err := sem.Acquire(ctx, 1); err != nil { // acquire 1 unit
        return err
    }
    defer sem.Release(1)
    return doLightWork()
}
```

**Real-time Scenario:** A memory-constrained image processing service uses a weighted semaphore: large images acquire 50MB of the 500MB budget, small images acquire 5MB — ensuring total memory stays within bounds regardless of the image mix.

---

### Q4: When would you use a semaphore vs worker pool?

**Answer:** Use a semaphore when work arrives dynamically and you want to cap concurrency (each request spawns its own goroutine, semaphore limits how many run simultaneously). Use a worker pool when you have a fixed set of workers processing from a queue (pre-allocated goroutines). Semaphores are simpler but create/destroy goroutines; worker pools reuse goroutines and can batch work.

**Example:**
```go
// Semaphore: dynamic goroutines, bounded concurrency
sem := NewSemaphore(10)
for req := range requests {
    sem.Acquire()
    go func(r Request) {
        defer sem.Release()
        handle(r) // new goroutine per request, max 10 concurrent
    }(req)
}

// Worker pool: fixed goroutines
jobs := make(chan Request, 100)
for i := 0; i < 10; i++ {
    go func() {
        for job := range jobs {
            handle(job) // reuses goroutine
        }
    }()
}
```

**Real-time Scenario:** A web scraper uses a semaphore (10 max) for dynamic URL discovery — new URLs spawn goroutines on-the-fly. A message queue consumer uses a worker pool — fixed workers poll the queue continuously.

---

### Q5: How do you implement a semaphore with timeout?

**Answer:** Use `context.Context` with the semaphore acquisition. With a channel-based semaphore, use `select` with `ctx.Done()`. With `golang.org/x/sync/semaphore`, pass a context with a deadline or timeout. If the timeout expires before a slot is available, the acquire fails with the context error.

**Example:**
```go
func (s *Semaphore) AcquireTimeout(ctx context.Context) error {
    select {
    case s.ch <- struct{}{}:
        return nil // acquired
    case <-ctx.Done():
        return ctx.Err() // timeout or cancellation
    }
}

// Usage:
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := sem.AcquireTimeout(ctx); err != nil {
    return fmt.Errorf("resource busy: %w", err)
}
defer sem.Release()
```

**Real-time Scenario:** A database query handler gives up after 2 seconds if the connection pool semaphore isn't available, returning a 503 Service Unavailable rather than queueing indefinitely.

---

### Q6: What is `golang.org/x/sync/semaphore`?

**Answer:** It's the official Go extended library's weighted semaphore implementation. It provides `semaphore.NewWeighted(n)` with `Acquire(ctx, n)`, `TryAcquire(n)`, and `Release(n)`. It supports context-based cancellation, weighted acquisition, and is more feature-complete than a simple channel-based semaphore. It's the recommended approach when you need a production-grade semaphore.

**Example:**
```go
import "golang.org/x/sync/semaphore"

var sem = semaphore.NewWeighted(10)

func handler(ctx context.Context) error {
    if err := sem.Acquire(ctx, 1); err != nil {
        return err // context cancelled or deadline exceeded
    }
    defer sem.Release(1)
    return processRequest()
}

// TryAcquire: non-blocking
if sem.TryAcquire(1) {
    defer sem.Release(1)
    doWork()
} else {
    fallback() // semaphore full, use fallback
}
```

**Real-time Scenario:** A production file upload service uses `semaphore.NewWeighted(1000)` — each upload acquires weight proportional to file size in MB, capping total concurrent upload volume at 1GB.

---

### Q7: How do you handle semaphore acquisition failures?

**Answer:** Acquisition can fail due to timeout, context cancellation, or non-blocking TryAcquire. Handle failures with: (1) return an error to the caller (HTTP 429/503), (2) queue the request for retry, (3) use a fallback/degraded path, or (4) drop the request with logging. Never silently ignore acquisition failures — they indicate resource exhaustion.

**Example:**
```go
func handleRequest(ctx context.Context, req Request) Response {
    if err := sem.Acquire(ctx, 1); err != nil {
        // Option 1: return error
        return Response{Status: 503, Body: "service busy, try again"}
        // Option 2: queue for retry
        // retryQueue <- req
        // Option 3: degraded mode
        // return cachedResponse(req)
    }
    defer sem.Release(1)
    return processRequest(req)
}
```

**Real-time Scenario:** An e-commerce checkout page returns cached product data (degraded mode) when the product service semaphore is full, rather than showing an error page during peak traffic.

---

### Q8: Relationship between semaphores and resource pools?

**Answer:** A semaphore controls access COUNT; a resource pool manages actual RESOURCES. A connection pool contains real database connections and hands them out; a semaphore just counts how many goroutines are allowed in. Often they work together: a pool uses a semaphore to limit waiters, and the pool manages the actual resources. Pools add lifecycle management (create, validate, destroy) that semaphores don't provide.

**Example:**
```go
type ConnPool struct {
    sem   *semaphore.Weighted // limits concurrent access
    conns chan *sql.Conn      // actual connections
}

func (p *ConnPool) Get(ctx context.Context) (*sql.Conn, error) {
    if err := p.sem.Acquire(ctx, 1); err != nil {
        return nil, err // too many waiters
    }
    return <-p.conns, nil // get actual connection
}

func (p *ConnPool) Put(conn *sql.Conn) {
    p.conns <- conn // return connection to pool
    p.sem.Release(1)
}
```

**Real-time Scenario:** A database connection pool with 50 connections uses a semaphore with weight 100 to limit waiters — if 100 goroutines are already waiting for connections, new requests get rejected immediately rather than queueing.

---

## Coding Problems

### Problem 1: Channel-Based Semaphore

**Difficulty:** Easy-Medium  
```go
type Semaphore struct {
    ch chan struct{}
}

func NewSemaphore(n int) *Semaphore {
    return &Semaphore{ch: make(chan struct{}, n)}
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

func (s *Semaphore) TryAcquire() bool {
    select {
    case s.ch <- struct{}{}:
        return true
    default:
        return false
    }
}

// Usage: limit concurrent downloads
func downloadAll(ctx context.Context, urls []string, maxConcurrent int) {
    sem := NewSemaphore(maxConcurrent)
    var wg sync.WaitGroup

    for _, url := range urls {
        wg.Add(1)
        go func(u string) {
            defer wg.Done()
            if err := sem.Acquire(ctx); err != nil {
                return
            }
            defer sem.Release()
            download(u)
        }(url)
    }
    wg.Wait()
}
```

---

### Problem 2: Weighted Semaphore

**Difficulty:** Medium  
**Problem:** Different operations consume different numbers of permits.

```go
type WeightedSemaphore struct {
    mu       sync.Mutex
    cond     *sync.Cond
    permits  int64
    maxPerms int64
}

func NewWeightedSemaphore(maxPermits int64) *WeightedSemaphore {
    ws := &WeightedSemaphore{permits: maxPermits, maxPerms: maxPermits}
    ws.cond = sync.NewCond(&ws.mu)
    return ws
}

func (ws *WeightedSemaphore) Acquire(ctx context.Context, weight int64) error {
    ws.mu.Lock()
    defer ws.mu.Unlock()
    for ws.permits < weight {
        // Check context before waiting
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }
        ws.cond.Wait()
    }
    ws.permits -= weight
    return nil
}

func (ws *WeightedSemaphore) Release(weight int64) {
    ws.mu.Lock()
    defer ws.mu.Unlock()
    ws.permits += weight
    ws.cond.Broadcast()
}
```

**Use case:** Read operations cost 1 permit, write operations cost 5 permits, from a pool of 10.

**Real-world Relevance:** Database connection pools with query cost, memory-bounded operations, I/O scheduling.

---

### Problem 3: Resource Governor

**Difficulty:** Hard  
**Problem:** Multi-resource semaphore with priority and preemption.

```go
type ResourceRequest struct {
    CPU      int
    Memory   int
    IO       int
    Priority int
}

type ResourceGovernor struct {
    mu        sync.Mutex
    cond      *sync.Cond
    available ResourceRequest // currently available
    capacity  ResourceRequest
}

func (rg *ResourceGovernor) Acquire(ctx context.Context, req ResourceRequest) error {
    rg.mu.Lock()
    defer rg.mu.Unlock()
    for !rg.canFulfill(req) {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }
        rg.cond.Wait()
    }
    rg.available.CPU -= req.CPU
    rg.available.Memory -= req.Memory
    rg.available.IO -= req.IO
    return nil
}

func (rg *ResourceGovernor) Release(req ResourceRequest) {
    rg.mu.Lock()
    defer rg.mu.Unlock()
    rg.available.CPU += req.CPU
    rg.available.Memory += req.Memory
    rg.available.IO += req.IO
    rg.cond.Broadcast()
}

func (rg *ResourceGovernor) canFulfill(req ResourceRequest) bool {
    return rg.available.CPU >= req.CPU &&
        rg.available.Memory >= req.Memory &&
        rg.available.IO >= req.IO
}
```

**Real-world Relevance:** Container resource management, query admission control (CockroachDB), cloud resource scheduling.

---

# 5. FUTURE/PROMISE PATTERN

## Concept Overview

A Future represents a value that will be available asynchronously. Go doesn't have built-in futures, but they're easily implemented with channels.

**Go idiomatic alternatives:**
- Channels (`<-chan Result`)
- `errgroup.Group`
- `sync.OnceValue` (Go 1.21+)

**Why futures matter in interviews:** Tests understanding of async composition, which is key in microservice architectures.

## Common Interview Questions

### Q1: How do you implement the future/promise pattern in Go?

**Answer:** A Future in Go is typically a struct containing a channel for signaling completion, the result value, and an error. A goroutine executes the async work, stores the result, and closes the channel. The caller blocks on the channel to get the result. With Go 1.18+ generics, you can create type-safe `Future[T]` implementations.

**Example:**
```go
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

func (f *Future[T]) Await() (T, error) {
    <-f.ch
    return f.val, f.err
}
```

**Real-time Scenario:** A service aggregates data from 5 microservices — each call is wrapped in `Async()`, all run concurrently, and results are collected with `Await()`, reducing total latency from 5x serial to 1x (the slowest call).

---

### Q2: What is the idiomatic Go alternative to futures?

**Answer:** Idiomatic Go uses goroutines + channels directly rather than a formal Future abstraction. Launch a goroutine, send the result on a channel, and receive it when needed. This is simpler and more explicit. Futures are useful when you want a reusable abstraction (e.g., a library), but for application code, raw channels + goroutines are preferred.

**Example:**
```go
// Future-style (works but less idiomatic):
f := Async(func() (int, error) { return fetchUserCount() })
count, err := f.Await()

// Idiomatic Go (preferred):
ch := make(chan result)
go func() {
    count, err := fetchUserCount()
    ch <- result{count, err}
}()
r := <-ch
```

**Real-time Scenario:** In code reviews at Go-heavy companies, using a Future library often gets a comment: "just use a channel" — unless the codebase has a standard Future type or the composition benefits are clear.

---

### Q3: How do you compose multiple futures?

**Answer:** Common composition patterns: `WhenAll` (wait for all futures to complete), `WhenAny` (wait for the first to complete), and `Then` (chain a callback on completion). In Go, `WhenAll` maps to a `sync.WaitGroup` or receiving from all channels. `WhenAny` maps to a `select` statement. Chaining maps to goroutines that read from one channel and write to another.

**Example:**
```go
func WhenAll[T any](futures ...*Future[T]) []T {
    results := make([]T, len(futures))
    var wg sync.WaitGroup
    for i, f := range futures {
        wg.Add(1)
        go func(idx int, fut *Future[T]) {
            defer wg.Done()
            results[idx], _ = fut.Await()
        }(i, f)
    }
    wg.Wait()
    return results
}

func WhenAny[T any](futures ...*Future[T]) (T, error) {
    ch := make(chan result[T], 1)
    for _, f := range futures {
        go func(fut *Future[T]) {
            val, err := fut.Await()
            select {
            case ch <- result[T]{val, err}:
            default:
            }
        }(f)
    }
    r := <-ch
    return r.val, r.err
}
```

**Real-time Scenario:** A search aggregator sends queries to 3 search engines using `WhenAny` — it returns the fastest result to the user while the other two continue (and are discarded).

---

### Q4: How do you implement async/await semantics in Go?

**Answer:** Go doesn't have `async/await` keywords because goroutines already provide equivalent functionality. Every goroutine is implicitly "async," and every channel receive is implicitly "await." The `go` keyword replaces `async`, and `<-ch` replaces `await`. This is more flexible than async/await because any function can be made async by calling it in a goroutine — no function coloring problem.

**Example:**
```go
// JavaScript: async/await
// const data = await fetchData();
// const processed = await processData(data);

// Go equivalent:
ch := make(chan Data)
go func() { ch <- fetchData() }()  // "async"
data := <-ch                        // "await"

processed := make(chan Result)
go func() { processed <- processData(data) }()
result := <-processed
```

**Real-time Scenario:** Go avoids the "function coloring" problem that plagues JavaScript/Python — in those languages, calling an async function from sync code requires refactoring the entire call chain. In Go, any function can be called from a goroutine without changes.

---

### Q5: Difference between a future and a channel?

**Answer:** A Future resolves exactly once with a single value — it's a one-shot value container. A channel is a stream — it can carry multiple values and is used for ongoing communication. Once a Future is resolved, all subsequent `Await()` calls return the same value instantly. A channel, once consumed, the value is gone (unless you close it for broadcast). Futures represent a single async computation; channels represent a communication pipe.

**Example:**
```go
// Future: resolves once, readable by many
f := Async(func() (int, error) { return computeExpensiveValue() })
v1, _ := f.Await() // blocks until ready
v2, _ := f.Await() // returns immediately — same value

// Channel: stream of values, each consumed once
ch := make(chan int)
go func() { ch <- 1; ch <- 2; ch <- 3; close(ch) }()
<-ch // 1
<-ch // 2 (each value consumed once)
```

**Real-time Scenario:** A config loader uses a Future (load once, read many times), while a work queue uses a channel (many tasks, each processed by one worker).

---

### Q6: How do you handle future cancellation?

**Answer:** Integrate `context.Context` into the Future. The async function accepts a context and checks for cancellation. The caller can cancel the context to abort the computation. The Future should also select on `ctx.Done()` so that `Await()` returns immediately on cancellation rather than blocking indefinitely.

**Example:**
```go
func AsyncWithCtx[T any](ctx context.Context, fn func(context.Context) (T, error)) *Future[T] {
    f := &Future[T]{ch: make(chan struct{})}
    go func() {
        f.val, f.err = fn(ctx)
        close(f.ch)
    }()
    return f
}

func (f *Future[T]) AwaitCtx(ctx context.Context) (T, error) {
    select {
    case <-f.ch:
        return f.val, f.err
    case <-ctx.Done():
        var zero T
        return zero, ctx.Err()
    }
}
```

**Real-time Scenario:** A request handler creates 5 futures for downstream calls with a 2-second timeout context. If the client disconnects, the context is cancelled, and all futures return immediately without waiting for slow backends.

---

### Q7: How do you handle future errors?

**Answer:** Store the error alongside the result in the Future struct. `Await()` returns both `(T, error)`, following Go's standard error convention. The caller checks the error after awaiting. For `WhenAll`, collect all errors (use `errgroup` or an error slice). For `WhenAny`, return the first successful result or all errors if all failed.

**Example:**
```go
type Future[T any] struct {
    ch  chan struct{}
    val T
    err error
}

func (f *Future[T]) Await() (T, error) {
    <-f.ch
    return f.val, f.err // caller checks error
}

// Usage:
f := Async(func() (User, error) {
    return db.GetUser(id) // may return error
})
user, err := f.Await()
if err != nil {
    log.Printf("failed to get user: %v", err)
}
```

**Real-time Scenario:** A dashboard aggregates data from 10 services. Each Future may fail independently — the dashboard shows available data and "unavailable" for failed sources rather than failing entirely.

---

### Q8: How do you implement a future that can be awaited multiple times?

**Answer:** Use a closed channel as the signaling mechanism. Once a channel is closed, all receive operations return immediately with the zero value. Multiple goroutines can `<-ch` and all will unblock when the Future resolves. The result is stored in the struct fields (not sent through the channel), so all awaiters read the same result.

**Example:**
```go
type SharedFuture[T any] struct {
    ch  chan struct{} // closed when ready
    val T
    err error
}

func (f *SharedFuture[T]) Await() (T, error) {
    <-f.ch // all callers unblock when channel is closed
    return f.val, f.err // read from struct — same value for all
}

// 100 goroutines can await the same future:
f := Async(func() (Config, error) { return loadConfig() })
for i := 0; i < 100; i++ {
    go func() {
        cfg, err := f.Await() // all 100 get the same result
        useConfig(cfg)
    }()
}
```

**Real-time Scenario:** A service starts with 100 goroutines all needing the loaded configuration. A single SharedFuture loads the config once, and all 100 goroutines await it — avoiding 100 redundant config loads.

---

## Coding Problems

### Problem 1: Generic Future Implementation

**Difficulty:** Medium  
```go
type Future[T any] struct {
    ch   chan struct{}
    val  T
    err  error
    once sync.Once
}

func NewFuture[T any](fn func() (T, error)) *Future[T] {
    f := &Future[T]{ch: make(chan struct{})}
    go func() {
        f.val, f.err = fn()
        close(f.ch) // signal completion
    }()
    return f
}

func (f *Future[T]) Get() (T, error) {
    <-f.ch // block until done
    return f.val, f.err
}

func (f *Future[T]) GetWithTimeout(d time.Duration) (T, error) {
    select {
    case <-f.ch:
        return f.val, f.err
    case <-time.After(d):
        var zero T
        return zero, errors.New("timeout")
    }
}

func (f *Future[T]) Done() bool {
    select {
    case <-f.ch:
        return true
    default:
        return false
    }
}
```

**Key insight:** Closing a channel broadcasts to all waiters. This is why `close(f.ch)` works for multiple concurrent `Get()` calls — once closed, all `<-f.ch` return immediately.

---

### Problem 2: WhenAll / WhenAny

**Difficulty:** Medium-Hard  
```go
func WhenAll[T any](futures ...*Future[T]) *Future[[]T] {
    return NewFuture(func() ([]T, error) {
        results := make([]T, len(futures))
        var firstErr error
        var mu sync.Mutex
        var wg sync.WaitGroup

        for i, f := range futures {
            wg.Add(1)
            go func(idx int, fut *Future[T]) {
                defer wg.Done()
                val, err := fut.Get()
                mu.Lock()
                defer mu.Unlock()
                if err != nil && firstErr == nil {
                    firstErr = err
                }
                results[idx] = val
            }(i, f)
        }
        wg.Wait()
        return results, firstErr
    })
}

func WhenAny[T any](futures ...*Future[T]) *Future[T] {
    return NewFuture(func() (T, error) {
        ch := make(chan struct {
            val T
            err error
        }, 1)

        for _, f := range futures {
            go func(fut *Future[T]) {
                val, err := fut.Get()
                select {
                case ch <- struct{ val T; err error }{val, err}:
                default:
                }
            }(f)
        }

        result := <-ch
        return result.val, result.err
    })
}
```

**Real-world Relevance:** Parallel API aggregation, hedged requests, microservice orchestration.

---

### Problem 3: Async Pipeline with Futures

**Difficulty:** Hard  
**Problem:** Chain async operations: `fetchUser → fetchOrders → calculateTotal → formatReport`

```go
func Then[T, U any](f *Future[T], fn func(T) (U, error)) *Future[U] {
    return NewFuture(func() (U, error) {
        val, err := f.Get()
        if err != nil {
            var zero U
            return zero, err
        }
        return fn(val)
    })
}

// Usage
func buildReport(userID string) *Future[string] {
    userFuture := NewFuture(func() (User, error) {
        return fetchUser(userID)
    })

    ordersFuture := Then(userFuture, func(user User) ([]Order, error) {
        return fetchOrders(user.ID)
    })

    totalFuture := Then(ordersFuture, func(orders []Order) (float64, error) {
        return calculateTotal(orders), nil
    })

    reportFuture := Then(totalFuture, func(total float64) (string, error) {
        return formatReport(total), nil
    })

    return reportFuture
}
```

**Real-world Relevance:** API orchestration, BFF patterns, microservice composition.

---

# 6. EVENT PROCESSING

## Concept Overview

Event-driven architecture processes discrete events (state changes, messages, signals) asynchronously. In Go, this typically involves goroutines + channels as the event bus.

**Patterns:**
- **Event Loop:** Single goroutine processing events sequentially from a channel.
- **Event Dispatcher:** Routes events to registered handlers by type.
- **Event Sourcing:** Store all events; rebuild state by replaying.
- **CQRS:** Separate read and write models, synchronized via events.
- **Complex Event Processing (CEP):** Detect patterns across event streams.

## Common Interview Questions

### Q1: How do you implement an event loop in Go?

**Answer:** An event loop in Go is a goroutine running a `for-select` loop that processes events from channels. Unlike Node.js or Python's asyncio (which need event loops because they're single-threaded), Go's goroutines already provide concurrency. You implement an event loop when you need to serialize event processing or maintain ordering guarantees — a single goroutine processes all events sequentially from a channel.

**Example:**
```go
type EventLoop struct {
    events chan Event
    quit   chan struct{}
}

func (el *EventLoop) Run() {
    for {
        select {
        case evt := <-el.events:
            el.handle(evt) // sequential processing — maintains order
        case <-el.quit:
            return
        }
    }
}

func (el *EventLoop) Submit(evt Event) {
    el.events <- evt // blocks if event loop is busy (backpressure)
}
```

**Real-time Scenario:** A game server's main loop processes player actions sequentially to avoid race conditions on game state — all mutations happen in one goroutine via an event loop, while I/O happens concurrently in separate goroutines.

---

### Q2: What is event sourcing and how do you implement it concurrently?

**Answer:** Event sourcing stores every state change as an immutable event rather than storing current state. To get current state, you replay all events. In Go, events are published to a channel, an event store appends them (single writer goroutine for ordering), and projections (read models) are built by separate goroutines consuming the event stream. This naturally maps to Go's channel-based concurrency.

**Example:**
```go
type EventStore struct {
    mu     sync.RWMutex
    events []Event
    subs   []chan Event
}

func (es *EventStore) Append(evt Event) {
    es.mu.Lock()
    es.events = append(es.events, evt)
    es.mu.Unlock()
    for _, sub := range es.subs {
        sub <- evt // notify projections
    }
}

func (es *EventStore) Replay(from int) []Event {
    es.mu.RLock()
    defer es.mu.RUnlock()
    return es.events[from:]
}
```

**Real-time Scenario:** A banking system uses event sourcing: `AccountCreated`, `DepositMade`, `WithdrawalMade` events are stored. The current balance is derived by replaying events. Concurrent projections build different read models (balance view, audit log, analytics).

---

### Q3: How do you handle event ordering across goroutines?

**Answer:** Ordering is guaranteed within a single channel (FIFO), but events processed by multiple goroutines have no ordering guarantee. Strategies: (1) use a single goroutine per partition/entity for ordering, (2) include sequence numbers in events and re-order at the consumer, (3) use a causal ordering scheme (vector clocks or Lamport timestamps). The most common Go pattern is partition-by-key to a specific goroutine.

**Example:**
```go
func partitionedProcessor(events <-chan Event, numWorkers int) {
    workers := make([]chan Event, numWorkers)
    for i := range workers {
        workers[i] = make(chan Event, 100)
        go func(ch chan Event) {
            for evt := range ch {
                process(evt) // events for same key are ordered
            }
        }(workers[i])
    }
    for evt := range events {
        idx := hash(evt.Key) % numWorkers
        workers[idx] <- evt // same key always goes to same worker
    }
}
```

**Real-time Scenario:** An order processing system partitions events by order ID — all events for order #12345 go to the same goroutine, ensuring `Created` is processed before `Paid` before `Shipped`.

---

### Q4: What is the reactor pattern and how does it map to Go?

**Answer:** The reactor pattern multiplexes I/O events on a single thread using an event demultiplexer (epoll/kqueue). In Go, the runtime already implements this internally — the netpoller uses epoll/kqueue to multiplex thousands of goroutines across OS threads. You don't need to implement a reactor in Go because goroutines + the runtime scheduler already provide the same benefits. The `select` statement is Go's user-level event demultiplexer.

**Example:**
```go
// Go's select IS the reactor pattern at user level:
func handler(conn net.Conn, timeout <-chan time.Time, cancel <-chan struct{}) {
    for {
        select {
        case data := <-readCh:   // I/O event
            process(data)
        case <-timeout:          // timer event
            conn.Close()
            return
        case <-cancel:           // cancellation event
            conn.Close()
            return
        }
    }
}
// Go's runtime netpoller handles the epoll/kqueue multiplexing transparently
```

**Real-time Scenario:** A TCP server handling 100K concurrent connections in Go doesn't need an explicit reactor — each connection gets a goroutine, and Go's runtime multiplexes them on ~8 OS threads via the internal netpoller.

---

### Q5: How do you implement idempotent event handling?

**Answer:** Idempotent handling means processing the same event twice produces the same result as processing it once. Strategies: (1) use a deduplication map of event IDs (check before processing), (2) design handlers to be naturally idempotent (SET instead of INCREMENT), (3) use version numbers / optimistic concurrency control. For distributed systems, store processed event IDs in a persistent store with TTL.

**Example:**
```go
type IdempotentHandler struct {
    mu        sync.Mutex
    processed map[string]bool
    handler   func(Event) error
}

func (h *IdempotentHandler) Handle(evt Event) error {
    h.mu.Lock()
    if h.processed[evt.ID] {
        h.mu.Unlock()
        return nil // already processed — skip
    }
    h.processed[evt.ID] = true
    h.mu.Unlock()
    return h.handler(evt)
}
```

**Real-time Scenario:** A payment processing system receives webhook notifications that may be delivered multiple times. An idempotency check on the event ID prevents double-charging customers.

---

### Q6: How do you handle event replay?

**Answer:** Event replay re-processes historical events to rebuild state or create new projections. Implementation: store all events with sequence numbers, allow consumers to specify a starting position, and replay events from that position. Key considerations: replay at a different speed than real-time, handle side effects (don't re-send emails during replay), and maintain replay position for restartability.

**Example:**
```go
func (es *EventStore) ReplayFrom(seq int64, handler func(Event)) {
    es.mu.RLock()
    events := es.events[seq:]
    es.mu.RUnlock()

    for _, evt := range events {
        handler(evt) // replay mode — handler should skip side effects
    }
}

// Usage: rebuild a projection
var balances = make(map[string]int64)
store.ReplayFrom(0, func(evt Event) {
    switch e := evt.(type) {
    case DepositEvent:
        balances[e.AccountID] += e.Amount
    case WithdrawalEvent:
        balances[e.AccountID] -= e.Amount
    }
})
```

**Real-time Scenario:** After deploying a bug fix to the recommendation engine, events from the past 7 days are replayed to rebuild the recommendation index with correct data — without affecting live traffic.

---

### Q7: How do you handle out-of-order events?

**Answer:** Out-of-order events occur in distributed systems where events from different sources arrive in unpredictable order. Strategies: (1) buffer and reorder by sequence number (wait for gaps to fill), (2) use event timestamps with a watermark (process when confident no earlier events will arrive), (3) design handlers to be commutative (order doesn't matter), or (4) use Lamport/vector clocks for causal ordering.

**Example:**
```go
type ReorderBuffer struct {
    mu       sync.Mutex
    expected int64
    buffer   map[int64]Event
    out      chan Event
}

func (rb *ReorderBuffer) Add(evt Event) {
    rb.mu.Lock()
    defer rb.mu.Unlock()
    rb.buffer[evt.SeqNum] = evt
    for {
        if next, ok := rb.buffer[rb.expected]; ok {
            rb.out <- next
            delete(rb.buffer, rb.expected)
            rb.expected++
        } else {
            break // gap — wait for missing event
        }
    }
}
```

**Real-time Scenario:** A stock trading system receives market data from multiple exchanges. Events arrive out of order, but the reorder buffer ensures they're processed in sequence-number order before updating the order book.

---

### Q8: How do you implement CQRS with goroutines?

**Answer:** CQRS (Command Query Responsibility Segregation) separates write (command) and read (query) paths. In Go, commands go through a channel to a single writer goroutine that updates the write model and publishes events. Events fan out to multiple reader goroutines that build optimized read models. This maps naturally to Go's concurrency model — channels for commands, goroutines for projections.

**Example:**
```go
type CQRSSystem struct {
    commands chan Command
    events   chan Event
    writeDB  *WriteStore
    readDB   *ReadStore
}

func (s *CQRSSystem) Run() {
    // Command handler (single writer)
    go func() {
        for cmd := range s.commands {
            evt := s.writeDB.Execute(cmd) // write model
            s.events <- evt               // publish event
        }
    }()
    // Read model updater (projection)
    go func() {
        for evt := range s.events {
            s.readDB.Project(evt) // update read model
        }
    }()
}
```

**Real-time Scenario:** An e-commerce platform uses CQRS: the write path handles order creation through a single command goroutine (consistency), while multiple read goroutines build the product catalog, search index, and analytics dashboard (performance).

---

## Coding Problems

### Problem 1: Type-Safe Event Dispatcher

**Difficulty:** Medium  
```go
type Event interface {
    EventType() string
}

type Dispatcher struct {
    mu       sync.RWMutex
    handlers map[string][]func(Event)
}

func NewDispatcher() *Dispatcher {
    return &Dispatcher{handlers: make(map[string][]func(Event))}
}

func (d *Dispatcher) On(eventType string, handler func(Event)) {
    d.mu.Lock()
    defer d.mu.Unlock()
    d.handlers[eventType] = append(d.handlers[eventType], handler)
}

func (d *Dispatcher) Dispatch(event Event) {
    d.mu.RLock()
    handlers := make([]func(Event), len(d.handlers[event.EventType()]))
    copy(handlers, d.handlers[event.EventType()])
    d.mu.RUnlock()

    for _, h := range handlers {
        go func(handler func(Event)) {
            defer func() {
                if r := recover(); r != nil {
                    log.Printf("handler panic: %v", r)
                }
            }()
            handler(event)
        }(h)
    }
}
```

---

### Problem 2: Event Store with Concurrent Readers

**Difficulty:** Hard  
```go
type EventStore struct {
    mu     sync.RWMutex
    events []Event
    subs   []chan Event
}

func (es *EventStore) Append(event Event) {
    es.mu.Lock()
    defer es.mu.Unlock()
    es.events = append(es.events, event)
    // Notify subscribers
    for _, sub := range es.subs {
        select {
        case sub <- event:
        default: // don't block on slow subscribers
        }
    }
}

func (es *EventStore) Subscribe(fromOffset int) <-chan Event {
    ch := make(chan Event, 100)
    es.mu.Lock()
    defer es.mu.Unlock()
    // Send historical events
    go func() {
        es.mu.RLock()
        for i := fromOffset; i < len(es.events); i++ {
            ch <- es.events[i]
        }
        es.mu.RUnlock()
    }()
    es.subs = append(es.subs, ch)
    return ch
}

func (es *EventStore) ReadFrom(offset int) []Event {
    es.mu.RLock()
    defer es.mu.RUnlock()
    if offset >= len(es.events) {
        return nil
    }
    result := make([]Event, len(es.events)-offset)
    copy(result, es.events[offset:])
    return result
}
```

---

### Problem 3: Complex Event Processing — Pattern Detection

**Difficulty:** Hard (FAANG-level)  
**Problem:** Detect "3 or more error events within 1 minute" across a stream.

```go
type PatternMatcher struct {
    window    time.Duration
    threshold int
    events    []time.Time
    mu        sync.Mutex
    alertCh   chan Alert
}

func NewPatternMatcher(window time.Duration, threshold int) *PatternMatcher {
    return &PatternMatcher{
        window:    window,
        threshold: threshold,
        alertCh:   make(chan Alert, 10),
    }
}

func (pm *PatternMatcher) Process(event Event) {
    if event.EventType() != "error" {
        return
    }

    pm.mu.Lock()
    defer pm.mu.Unlock()

    now := time.Now()
    pm.events = append(pm.events, now)

    // Remove events outside window
    cutoff := now.Add(-pm.window)
    i := 0
    for i < len(pm.events) && pm.events[i].Before(cutoff) {
        i++
    }
    pm.events = pm.events[i:]

    // Check threshold
    if len(pm.events) >= pm.threshold {
        select {
        case pm.alertCh <- Alert{
            Message: fmt.Sprintf("%d errors in %v", len(pm.events), pm.window),
            Time:    now,
        }:
        default:
        }
        pm.events = nil // reset after alert
    }
}
```

**Variations:** Pattern "A followed by B within 5 seconds," session windowing, temporal joins.  
**Real-world Relevance:** Fraud detection, monitoring alerting, IoT event processing, security event correlation.

---

## Summary: Pattern Selection Guide

| Need | Pattern | Key Primitive |
|------|---------|---------------|
| Broadcast to many | Pub-Sub | Channel per subscriber |
| Control request rate | Rate Limiting | Token bucket / `x/time/rate` |
| Slow down producers | Backpressure | Bounded channels |
| Limit concurrent access | Semaphore | Buffered channel / `x/sync/semaphore` |
| Async result | Future/Promise | Channel + sync.Once |
| React to events | Event Processing | Dispatcher + goroutines |
| Transform data in stages | Pipeline | Channel chain |
| Distribute work | Fan-Out / Worker Pool | Shared job channel |
| Merge results | Fan-In | Goroutine per source + WaitGroup |
