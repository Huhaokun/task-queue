use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;

use task_queue::{SimpleTaskQueue, Task, TaskState};

#[test]
fn public_queue_api_supports_multiple_producers_and_consumers() {
    let producer_count = 4;
    let consumer_count = 4;
    let tasks_per_producer = 250;
    let total_tasks = producer_count * tasks_per_producer;

    let queue = Arc::new(SimpleTaskQueue::new());
    let start = Arc::new(Barrier::new(producer_count + consumer_count));
    let produced = Arc::new(AtomicUsize::new(0));
    let consumed = Arc::new(AtomicUsize::new(0));

    thread::scope(|scope| {
        for producer_id in 0..producer_count {
            let queue = Arc::clone(&queue);
            let start = Arc::clone(&start);
            let produced = Arc::clone(&produced);

            scope.spawn(move || {
                start.wait();

                for task_index in 0..tasks_per_producer {
                    let task_id = format!("producer-{producer_id}-task-{task_index}");
                    queue
                        .submit_task(Task::new(task_id, vec![producer_id as u8]))
                        .unwrap();
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
                    let task = queue.pop_task();

                    if let Some(task) = task {
                        queue.mark_task_success(task.id().to_owned());
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

    let first_status = queue
        .get_task_status("producer-0-task-0".to_owned())
        .unwrap();
    assert_eq!(first_status.state(), TaskState::Success);
    assert!(first_status.finished_at().is_some());

    let last_status = queue
        .get_task_status("producer-3-task-249".to_owned())
        .unwrap();
    assert_eq!(last_status.state(), TaskState::Success);
    assert!(last_status.finished_at().is_some());
}
