# A Simple Task Queue

### 核心功能
任务提交和消费：多 producer 多 consumer 并发安全的任务提交和消费
任务去重：对于相同的 task id 重复提交会被拒绝
TODO：支持单机和分布式（backed by redis）两种模式
TODO：任务超时后策略（重试，丢弃）
TODO：任务状态的监控

### Benchmark
运行多 producer 多 consumer benchmark：

```bash
cargo run --release --bin benchmark -- --producers 4 --consumers 4 --tasks 100000 --payload-bytes 128
```
