use std::sync::Arc;
use std::time::Duration;

use task_queue::{SimpleTaskQueue, TaskState};

#[tokio::test]
async fn pop_task_awaits_without_blocking_the_runtime() {
    let queue = Arc::new(SimpleTaskQueue::<u8>::new());
    let consumer_queue = Arc::clone(&queue);
    let consumer = tokio::spawn(async move { consumer_queue.pop_task().await });

    tokio::task::yield_now().await;
    assert!(!consumer.is_finished());

    let task_id = queue.submit_task(42).unwrap();
    let task = tokio::time::timeout(Duration::from_secs(1), consumer)
        .await
        .expect("consumer did not wake after task submission")
        .expect("consumer task panicked")
        .expect("queue disconnected");

    assert_eq!(task.id(), task_id);
    assert_eq!(task.payload(), &42);
    assert_eq!(
        queue.get_task_status(task_id).unwrap().state(),
        TaskState::Running
    );
}
