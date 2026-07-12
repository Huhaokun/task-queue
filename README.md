# A Simple Task Queue

### Key Features
- [ ] task submit and consumption thread-safe
- [ ] deduplicate with task_id
- [ ] record task created_at and finished_at
- [ ] single-process with multi-thread support and distributed mode (backend by redis)
- [ ] timeout handle strategy
- [ ] metrics and tracing

### Benchmark

```bash
cargo run --release --bin benchmark -- --producers 4 --consumers 4 --tasks 100000 --payload-bytes 128
```
