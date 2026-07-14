use std::{
    env,
    ops::Range,
    process,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use task_queue::SimpleTaskQueue;

#[derive(Clone, Copy, Debug)]
struct BenchmarkConfig {
    producers: usize,
    consumers: usize,
    tasks: usize,
    payload_bytes: usize,
}

#[derive(Debug)]
struct BenchmarkResult {
    elapsed: Duration,
    produced: usize,
    consumed: usize,
    payload_bytes_consumed: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            producers: 4,
            consumers: 4,
            tasks: 100_000,
            payload_bytes: 128,
        }
    }
}

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("{}", usage());
        return;
    }

    let config = match BenchmarkConfig::from_args(args.into_iter()) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            eprintln!();
            eprintln!("{}", usage());
            process::exit(2);
        }
    };

    let result = run_benchmark(config);
    let elapsed_secs = result.elapsed.as_secs_f64();
    let throughput = result.consumed as f64 / elapsed_secs;

    println!("producers: {}", config.producers);
    println!("consumers: {}", config.consumers);
    println!("tasks: {}", config.tasks);
    println!("payload_bytes: {}", config.payload_bytes);
    println!("produced: {}", result.produced);
    println!("consumed: {}", result.consumed);
    println!("payload_bytes_consumed: {}", result.payload_bytes_consumed);
    println!("elapsed_ms: {:.3}", elapsed_secs * 1000.0);
    println!("throughput_tasks_per_sec: {:.2}", throughput);
}

impl BenchmarkConfig {
    fn from_args(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut config = Self::default();
        let mut args = args.peekable();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--producers" => {
                    config.producers = parse_positive_usize("--producers", args.next())?;
                }
                "--consumers" => {
                    config.consumers = parse_positive_usize("--consumers", args.next())?;
                }
                "--tasks" => {
                    config.tasks = parse_positive_usize("--tasks", args.next())?;
                }
                "--payload-bytes" => {
                    config.payload_bytes = parse_usize("--payload-bytes", args.next())?;
                }
                _ => return Err(format!("unknown argument: {arg}")),
            }
        }

        Ok(config)
    }
}

fn parse_positive_usize(flag: &str, value: Option<String>) -> Result<usize, String> {
    let parsed = parse_usize(flag, value)?;
    if parsed == 0 {
        Err(format!("{flag} must be greater than zero"))
    } else {
        Ok(parsed)
    }
}

fn parse_usize(flag: &str, value: Option<String>) -> Result<usize, String> {
    value
        .ok_or_else(|| format!("{flag} requires a value"))?
        .parse()
        .map_err(|_| format!("{flag} must be a non-negative integer"))
}

fn usage() -> &'static str {
    "usage: benchmark [--producers N] [--consumers N] [--tasks N] [--payload-bytes N]"
}

fn run_benchmark(config: BenchmarkConfig) -> BenchmarkResult {
    let queue = Arc::new(SimpleTaskQueue::<Vec<u8>>::new());
    let start = Arc::new(Barrier::new(config.producers + config.consumers + 1));
    let produced = Arc::new(AtomicUsize::new(0));
    let consumed = Arc::new(AtomicUsize::new(0));
    let mut producer_handles = Vec::with_capacity(config.producers);
    let mut consumer_handles = Vec::with_capacity(config.consumers);

    for producer_id in 0..config.producers {
        let queue = Arc::clone(&queue);
        let start = Arc::clone(&start);
        let produced = Arc::clone(&produced);
        let task_range = task_range(producer_id, config.producers, config.tasks);
        let payload = vec![producer_id as u8; config.payload_bytes];

        producer_handles.push(thread::spawn(move || {
            start.wait();

            for _ in task_range {
                queue.submit_task(payload.clone()).unwrap();
                produced.fetch_add(1, Ordering::Release);
            }
        }));
    }

    for _ in 0..config.consumers {
        let queue = Arc::clone(&queue);
        let start = Arc::clone(&start);
        let consumed = Arc::clone(&consumed);
        let total_tasks = config.tasks;

        consumer_handles.push(thread::spawn(move || {
            let mut local_payload_bytes = 0;

            start.wait();

            loop {
                if consumed.load(Ordering::Acquire) >= total_tasks {
                    break local_payload_bytes;
                }

                let task = queue.pop_task();

                if let Some(task) = task {
                    local_payload_bytes += task.payload().len();
                    queue.mark_task_success(task.id());

                    if consumed.fetch_add(1, Ordering::AcqRel) + 1 >= total_tasks {
                        break local_payload_bytes;
                    }
                } else {
                    thread::yield_now();
                }
            }
        }));
    }

    let started = std::time::Instant::now();
    start.wait();

    for handle in producer_handles {
        handle.join().expect("producer thread panicked");
    }

    let mut payload_bytes_consumed = 0;
    for handle in consumer_handles {
        payload_bytes_consumed += handle.join().expect("consumer thread panicked");
    }

    BenchmarkResult {
        elapsed: started.elapsed(),
        produced: produced.load(Ordering::Acquire),
        consumed: consumed.load(Ordering::Acquire),
        payload_bytes_consumed,
    }
}

fn task_range(producer_id: usize, producer_count: usize, total_tasks: usize) -> Range<usize> {
    let base = total_tasks / producer_count;
    let remainder = total_tasks % producer_count;
    let start = producer_id * base + producer_id.min(remainder);
    let len = base + usize::from(producer_id < remainder);

    start..start + len
}
