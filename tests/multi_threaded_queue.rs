use std::sync::{
    Arc, Barrier, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;

use taskq::{SimpleTaskQueue, TaskState};

#[test]
fn public_queue_api_supports_multiple_producers_and_consumers() {
    let producer_count = 4;
    let consumer_count = 4;
    let tasks_per_producer = 250;
    let total_tasks = producer_count * tasks_per_producer;

    let queue = Arc::new(SimpleTaskQueue::<Vec<u8>>::new());
    let start = Arc::new(Barrier::new(producer_count + consumer_count));
    let produced = Arc::new(AtomicUsize::new(0));
    let consumed = Arc::new(AtomicUsize::new(0));
    let task_ids = Arc::new(Mutex::new(Vec::with_capacity(total_tasks)));

    thread::scope(|scope| {
        for producer_id in 0..producer_count {
            let queue = Arc::clone(&queue);
            let start = Arc::clone(&start);
            let produced = Arc::clone(&produced);
            let task_ids = Arc::clone(&task_ids);

            scope.spawn(move || {
                start.wait();

                for _ in 0..tasks_per_producer {
                    let task_id = queue.submit_task(vec![producer_id as u8]).unwrap();
                    task_ids.lock().unwrap().push(task_id);
                    produced.fetch_add(1, Ordering::Release);
                }
            });
        }

        for _ in 0..consumer_count {
            let queue = Arc::clone(&queue);
            let start = Arc::clone(&start);
            let consumed = Arc::clone(&consumed);

            scope.spawn(move || {
                start.wait();

                while consumed.load(Ordering::Acquire) < total_tasks {
                    let task = queue.try_pop_task();

                    if let Some(task) = task {
                        queue.mark_task_success(task.id()).unwrap();
                        consumed.fetch_add(1, Ordering::AcqRel);
                    } else {
                        thread::yield_now();
                    }
                }
            });
        }
    });

    assert_eq!(produced.load(Ordering::Acquire), total_tasks);
    assert_eq!(consumed.load(Ordering::Acquire), total_tasks);

    let mut task_ids = task_ids.lock().unwrap().clone();
    task_ids.sort_unstable();
    task_ids.dedup();
    assert_eq!(task_ids.len(), total_tasks);
    for task_id in task_ids {
        let status = queue.get_task_status(task_id).unwrap();
        assert_eq!(status.state(), TaskState::Success);
        assert!(status.finished_at().is_some());
    }
}
