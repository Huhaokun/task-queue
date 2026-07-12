# A Simple Task Queue

### Key Features
- [x] task submit and consumption thread-safe
- [x] deduplicate with task_id
- [x] record task status 
- [x] auto back-pressure
- [x] support self-define type task
- [ ] single-process with multi-thread support and distributed mode (backend by redis)
- [ ] timeout handle strategy
- [ ] metrics and tracing

### Benchmark

```bash
cargo run --release --bin benchmark -- --producers 4 --consumers 4 --tasks 100000 --payload-bytes 128
```
