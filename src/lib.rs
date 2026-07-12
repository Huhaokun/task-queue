use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam::channel::{Receiver, Sender, TrySendError};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use thiserror::Error;

// identify an unique task, task with same TaskId will be reject when submit
pub type TaskId = String;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Task<T> {
    id: TaskId,
    payload: T,
}

impl<T> Task<T> {
    pub fn new(id: impl Into<TaskId>, payload: T) -> Self {
        Self {
            id: id.into(),
            payload,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TaskState {
    Queued,
    Running,
    Success,
    Failed,
}

pub fn is_finished(s: TaskState) -> bool {
    s == TaskState::Success || s == TaskState::Failed
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TaskStatus {
    state: TaskState,
    created_at: i64,
    running_at: Option<i64>,
    finished_at: Option<i64>,
}

impl TaskStatus {
    pub fn state(&self) -> TaskState {
        self.state
    }

    pub fn created_at(&self) -> i64 {
        self.created_at
    }

    pub fn running_at(&self) -> Option<i64> {
        self.running_at
    }

    pub fn finished_at(&self) -> Option<i64> {
        self.finished_at
    }

    fn init() -> TaskStatus {
        TaskStatus {
            state: TaskState::Queued,
            created_at: now_millis(),
            running_at: None,
            finished_at: None,
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before unix epoch")
        .as_millis() as i64
}

#[derive(Error, Debug)]
pub enum TaskError {
    #[error("task id not found")]
    NotFound,
    #[error("duplicate task id")]
    Duplicate,
    #[error("task queue is full")]
    QueueFulled,
    #[error("task queue is closed")]
    Disconnected,
}

pub trait TaskQueue<T> {
    fn submit_task(&self, task: Task<T>) -> Result<(), TaskError>;

    fn get_task_status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError>;

    fn pop_task(&self) -> Option<Task<T>>;

    fn mark_task_success(&self, task_id: TaskId);

    fn mark_task_failed(&self, task_id: TaskId);
}

pub struct SimpleTaskQueue<T> {
    status_map: Arc<DashMap<TaskId, TaskStatus>>,
    sender: Sender<Task<T>>,
    receiver: Receiver<Task<T>>,
    keep_cleanup_running: Arc<AtomicBool>,
    cleanup_thread: Option<JoinHandle<()>>,
}

impl<T> SimpleTaskQueue<T> {
    pub fn new_with_capacity(capacity: usize) -> SimpleTaskQueue<T> {
        const DEFAULT_FINISHED_TASK_TTL: Duration = Duration::from_secs(60);
        const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

        SimpleTaskQueue::new_with_cleanup_config(
            capacity,
            DEFAULT_FINISHED_TASK_TTL,
            DEFAULT_CLEANUP_INTERVAL,
        )
    }

    pub fn new_with_cleanup_config(
        capacity: usize,
        finished_task_ttl: Duration,
        cleanup_interval: Duration,
    ) -> SimpleTaskQueue<T> {
        let (sender, receiver) = crossbeam::channel::bounded::<Task<T>>(capacity);

        let status_map = Arc::new(DashMap::new());
        let keep_cleanup_running = Arc::new(AtomicBool::new(true));
        let cleanup_thread = spawn_cleanup_thread(
            Arc::clone(&status_map),
            Arc::clone(&keep_cleanup_running),
            finished_task_ttl,
            cleanup_interval,
        );

        SimpleTaskQueue {
            status_map,
            sender,
            receiver,
            keep_cleanup_running,
            cleanup_thread: Some(cleanup_thread),
        }
    }

    pub fn new() -> SimpleTaskQueue<T> {
        const DEFAULT_CAPACITY: usize = 1024;
        SimpleTaskQueue::new_with_capacity(DEFAULT_CAPACITY)
    }

    pub fn submit_task(&self, task: Task<T>) -> Result<(), TaskError> {
        let task_id = task.id.clone();

        match self.status_map.entry(task_id.clone()) {
            Entry::Occupied(_) => Err(TaskError::Duplicate),
            Entry::Vacant(entry) => {
                entry.insert(TaskStatus::init());

                match self.sender.try_send(task) {
                    Ok(()) => Ok(()),
                    Err(TrySendError::Full(_)) => {
                        self.status_map.remove(&task_id);
                        Err(TaskError::QueueFulled)
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        self.status_map.remove(&task_id);
                        Err(TaskError::Disconnected)
                    }
                }
            }
        }
    }

    pub fn get_task_status(&self, task_id: impl AsRef<str>) -> Result<TaskStatus, TaskError> {
        self.status_map
            .get(task_id.as_ref())
            .map(|state| *state)
            .ok_or(TaskError::NotFound)
    }

    pub fn pop_task(&self) -> Option<Task<T>> {
        match self.receiver.try_recv() {
            Ok(t) => {
                if let Some(mut status) = self.status_map.get_mut(&t.id) {
                    status.state = TaskState::Running;
                    status.running_at = Some(now_millis());
                }
                Some(t)
            }
            Err(_) => None,
        }
    }

    pub fn mark_task_success(&self, task_id: impl AsRef<str>) {
        if let Some(mut status) = self.status_map.get_mut(task_id.as_ref()) {
            finish_task(&mut status, TaskState::Success);
        }
    }

    pub fn mark_task_failed(&self, task_id: impl AsRef<str>) {
        if let Some(mut status) = self.status_map.get_mut(task_id.as_ref()) {
            finish_task(&mut status, TaskState::Failed);
        }
    }
}

fn spawn_cleanup_thread(
    status_map: Arc<DashMap<TaskId, TaskStatus>>,
    keep_running: Arc<AtomicBool>,
    finished_task_ttl: Duration,
    cleanup_interval: Duration,
) -> JoinHandle<()> {
    thread::spawn(move || {
        while keep_running.load(Ordering::Acquire) {
            cleanup_finished_tasks(&status_map, now_millis(), finished_task_ttl);
            thread::park_timeout(cleanup_interval);
        }
    })
}

fn cleanup_finished_tasks(
    status_map: &DashMap<TaskId, TaskStatus>,
    now_millis: i64,
    finished_task_ttl: Duration,
) {
    let ttl_millis = duration_millis_i64(finished_task_ttl);
    status_map.retain(|_, task_status| {
        !matches!(
            task_status.finished_at(),
            Some(finished_at)
                if is_finished(task_status.state())
                    && finished_at.saturating_add(ttl_millis) <= now_millis
        )
    });
}

fn duration_millis_i64(duration: Duration) -> i64 {
    duration.as_millis().min(i64::MAX as u128) as i64
}

impl<T> Drop for SimpleTaskQueue<T> {
    fn drop(&mut self) {
        self.keep_cleanup_running.store(false, Ordering::Release);

        if let Some(cleanup_thread) = self.cleanup_thread.take() {
            cleanup_thread.thread().unpark();
            let _ = cleanup_thread.join();
        }
    }
}

fn finish_task(status: &mut TaskStatus, finished_state: TaskState) {
    if status.state == TaskState::Running {
        status.state = finished_state;
        status.finished_at = Some(now_millis());
    }
}

impl<T> Default for SimpleTaskQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> TaskQueue<T> for SimpleTaskQueue<T> {
    fn submit_task(&self, task: Task<T>) -> Result<(), TaskError> {
        SimpleTaskQueue::submit_task(self, task)
    }

    fn get_task_status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError> {
        SimpleTaskQueue::get_task_status(self, task_id)
    }

    fn pop_task(&self) -> Option<Task<T>> {
        SimpleTaskQueue::pop_task(self)
    }

    fn mark_task_success(&self, task_id: TaskId) {
        SimpleTaskQueue::mark_task_success(self, task_id);
    }

    fn mark_task_failed(&self, task_id: TaskId) {
        SimpleTaskQueue::mark_task_failed(self, task_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: &str) -> Task<Vec<u8>> {
        Task {
            id: id.to_string(),
            payload: Vec::new(),
        }
    }

    fn submit_task(queue: &SimpleTaskQueue<Vec<u8>>, id: &str) -> Result<(), TaskError> {
        queue.submit_task(task(id))
    }

    #[test]
    fn submit_task_sets_status_to_queued() {
        let queue = SimpleTaskQueue::new();

        submit_task(&queue, "task-1").unwrap();

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap().state(),
            TaskState::Queued
        );
    }

    #[test]
    fn pop_task_marks_status_as_running() {
        let queue = SimpleTaskQueue::new();
        submit_task(&queue, "task-1").unwrap();

        let task = queue.pop_task().unwrap();

        assert_eq!(task.id, "task-1");
        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap().state(),
            TaskState::Running
        );
    }

    #[test]
    fn running_task_can_be_marked_success() {
        let queue = SimpleTaskQueue::new();
        submit_task(&queue, "task-1").unwrap();

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap().state(),
            TaskState::Queued
        );

        queue.pop_task().unwrap();

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap().state(),
            TaskState::Running
        );

        queue.mark_task_success("task-1".to_string());

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap().state(),
            TaskState::Success
        );
    }

    #[test]
    fn running_task_can_be_marked_failed() {
        let queue = SimpleTaskQueue::new();
        submit_task(&queue, "task-1").unwrap();

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap().state(),
            TaskState::Queued
        );

        queue.pop_task().unwrap();

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap().state(),
            TaskState::Running
        );

        queue.mark_task_failed("task-1".to_string());

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap().state(),
            TaskState::Failed
        );
    }

    #[test]
    fn submit_task_rejects_duplicate_id() {
        let queue = SimpleTaskQueue::new();
        submit_task(&queue, "task-1").unwrap();

        assert!(matches!(
            submit_task(&queue, "task-1"),
            Err(TaskError::Duplicate)
        ));

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap().state(),
            TaskState::Queued
        );
    }

    #[test]
    fn submit_task_rejects_when_queue_is_full_without_status_leak() {
        let queue = SimpleTaskQueue::new_with_capacity(1);
        submit_task(&queue, "task-1").unwrap();

        assert!(matches!(
            submit_task(&queue, "task-2"),
            Err(TaskError::QueueFulled)
        ));
        assert!(matches!(
            queue.get_task_status("task-2".to_string()),
            Err(TaskError::NotFound)
        ));
    }

    #[test]
    fn submitted_task_records_created_at_without_finished_at() {
        let queue = SimpleTaskQueue::new();
        let before_submit = now_millis();

        submit_task(&queue, "task-1").unwrap();

        let status = queue.get_task_status("task-1".to_string()).unwrap();
        assert_eq!(status.state(), TaskState::Queued);
        assert!(status.created_at() >= before_submit);
        assert!(status.finished_at().is_none());
    }

    #[test]
    fn successful_task_records_finished_at() {
        let queue = SimpleTaskQueue::new();
        submit_task(&queue, "task-1").unwrap();
        queue.pop_task().unwrap();

        queue.mark_task_success("task-1".to_string());

        let status = queue.get_task_status("task-1".to_string()).unwrap();
        assert_eq!(status.state(), TaskState::Success);
        assert!(status.finished_at().unwrap() >= status.created_at());
    }

    #[test]
    fn failed_task_records_finished_at() {
        let queue = SimpleTaskQueue::new();
        submit_task(&queue, "task-1").unwrap();
        queue.pop_task().unwrap();

        queue.mark_task_failed("task-1".to_string());

        let status = queue.get_task_status("task-1".to_string()).unwrap();
        assert_eq!(status.state(), TaskState::Failed);
        assert!(status.finished_at().unwrap() >= status.created_at());
    }

    #[test]
    fn cleanup_removes_only_finished_tasks_older_than_ttl() {
        let status_map = DashMap::new();
        status_map.insert(
            "old-success".to_string(),
            TaskStatus {
                state: TaskState::Success,
                created_at: 0,
                running_at: Some(10),
                finished_at: Some(100),
            },
        );
        status_map.insert(
            "recent-failed".to_string(),
            TaskStatus {
                state: TaskState::Failed,
                created_at: 0,
                running_at: Some(10),
                finished_at: Some(950),
            },
        );
        status_map.insert(
            "running".to_string(),
            TaskStatus {
                state: TaskState::Running,
                created_at: 0,
                running_at: Some(10),
                finished_at: None,
            },
        );

        cleanup_finished_tasks(&status_map, 1_100, Duration::from_millis(500));

        assert!(status_map.get("old-success").is_none());
        assert!(status_map.get("recent-failed").is_some());
        assert!(status_map.get("running").is_some());
    }

    #[test]
    fn background_cleanup_removes_finished_tasks_after_ttl() {
        let queue = SimpleTaskQueue::new_with_cleanup_config(
            1,
            Duration::from_millis(0),
            Duration::from_millis(1),
        );
        submit_task(&queue, "task-1").unwrap();
        let task = queue.pop_task().unwrap();
        queue.mark_task_success(task.id());

        for _ in 0..100 {
            if matches!(
                queue.get_task_status("task-1".to_string()),
                Err(TaskError::NotFound)
            ) {
                return;
            }

            thread::sleep(Duration::from_millis(1));
        }

        panic!("finished task status was not cleaned");
    }
}
