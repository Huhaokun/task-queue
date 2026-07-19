# taskq A simple task queue


### Key Features
- [x] task submit and consumption thread-safe
- [x] async task consumption without blocking runtime workers
- [x] internally generated unique `i64` task IDs
- [x] optional deduplication with a separate task key
- [x] record task status 
- [x] auto back-pressure
- [x] support self-define type task
- [x] completed task status will be cleaned if reach ttl
- [ ] single-process with multi-thread support and distributed mode (backend by redis / postgre / mysql)
- [x] zombie task detection
- [ ] metrics and tracing
- [ ] task with priority
- [ ] task cancelation

### Usage

```rust
use taskq::SimpleTaskQueue;

let queue: SimpleTaskQueue<&str> = SimpleTaskQueue::new();

// Without a key, every submission creates a new task.
let task_id = queue.submit_task("payload")?;

// Supplying a key enables deduplication for that task.
let keyed_task_id = queue.submit_task_with_key("request-123", "payload")?;

// Wait asynchronously until a task is available.
let task = queue.pop_task().await?;

// The task key type is generic; String is only the default.
let numeric_key_queue = SimpleTaskQueue::<&str, u64>::new();
numeric_key_queue.submit_task_with_key(123_u64, "payload")?;
# Ok::<(), taskq::TaskError>(())
```

### Benchmark

```bash
cargo run --release --bin benchmark -- --producers 4 --consumers 4 --tasks 100000 --payload-bytes 128
```

### License

Licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT), at your option.
