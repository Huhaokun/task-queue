# taskq

A small, thread-safe, in-memory task queue for synchronous producers and async
or synchronous consumers.

## Features

- [x] thread-safe submission and consumption
- [x] async task consumption without blocking runtime workers
- [x] synchronous non-blocking task polling
- [x] queue-local, internally generated `i64` task IDs
- [x] optional idempotency with generic task keys
- [x] task lifecycle status and timestamps
- [x] bounded capacity with explicit `QueueFull` signaling
- [x] generic payload types
- [x] finished task cleanup after a configurable TTL
- [x] running task timeout detection
- [x] single-process, multi-threaded operation
- [x] graceful, drain-before-disconnect queue closure
- [ ] metrics and tracing
- [ ] task priorities
- [ ] task cancellation
- [ ] persistent or distributed backends

## Installation

```bash
cargo add taskq
```

## Usage

```rust
use taskq::{SimpleTaskQueue, TaskError};

async fn run() -> Result<(), TaskError> {
    let queue = SimpleTaskQueue::<&str>::new();

    // Without a key, every submission creates a new task.
    let task_id = queue.submit_task("payload")?;

    // Supplying a key enables deduplication for that task.
    queue.submit_task_with_key("request-123", "keyed payload")?;

    // Wait asynchronously without blocking the runtime worker.
    let task = queue.pop_task().await?;
    assert_eq!(task.id(), task_id);
    assert_eq!(task.payload(), &"payload");
    queue.mark_task_success(task.id())?;

    // Stop new submissions while allowing already queued tasks to drain.
    assert!(queue.close());
    assert!(queue.is_closed());

    // Task keys can use any supported hashable type.
    let numeric_key_queue = SimpleTaskQueue::<&str, u64>::new();
    numeric_key_queue.submit_task_with_key(123_u64, "payload")?;

    Ok(())
}
```

Use `try_pop_task` when a synchronous consumer must poll without waiting.
Received tasks can be consumed with `into_payload` or `into_parts` when the
consumer needs ownership of the payload.

## Queue semantics

- The queue is bounded. `submit_task` and `submit_task_with_key` never wait for
  capacity; they return `TaskError::QueueFull` so callers can retry, back off,
  or reject work according to their own policy.
- Task IDs are unique only within one queue instance. They are not durable or
  globally unique identifiers.
- A task key remains reserved until its terminal status is removed after the
  configured finished-task TTL.
- `pop_task` waits asynchronously, while `try_pop_task` returns immediately.
- `close` rejects new submissions, preserves already queued tasks for draining,
  and wakes consumers waiting on an empty queue. After the queue is drained,
  `pop_task` returns `TaskError::Disconnected`; `try_pop_task` returns `None`.
- Tasks and statuses live only in memory and are lost when the process exits.
- Every queue instance owns one background maintenance thread for status
  cleanup and running-task timeout detection.

## Benchmark

```bash
cargo run --release --bin benchmark -- \
  --producers 4 --consumers 4 --tasks 100000 \
  --payload-bytes 128 --capacity 1024
```

The benchmark retries submissions that receive `QueueFull`, so producer-heavy
configurations measure bounded-queue contention instead of terminating early.

## License

Licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT), at your option.
