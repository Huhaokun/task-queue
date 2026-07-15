use std::collections::HashSet;
use std::future::Future;
use std::hash::Hash;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_channel::{Receiver, Sender, TrySendError};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use thiserror::Error;

/// Identifies a task uniquely within a queue instance.
pub type TaskId = i64;
pub type TaskKey<K = String> = K;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Task<T, K = String> {
    id: TaskId,
    task_key: Option<TaskKey<K>>,
    payload: T,
}

impl<T, K> Task<T, K> {
    fn new(id: TaskId, task_key: Option<TaskKey<K>>, payload: T) -> Self {
        Self {
            id,
            task_key,
            payload,
        }
    }

    pub fn id(&self) -> TaskId {
        self.id
    }

    pub fn task_key(&self) -> Option<&K> {
        self.task_key.as_ref()
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
    #[error("duplicate task key")]
    DuplicateTaskKey,
    #[error("task id space exhausted")]
    TaskIdExhausted,
    #[error("task queue is full")]
    QueueFulled,
    #[error("task queue is closed")]
    Disconnected,
}

pub trait TaskQueue<T, K = String> {
    fn submit_task(&self, payload: T) -> Result<TaskId, TaskError>;

    fn submit_task_with_key(&self, task_key: TaskKey<K>, payload: T) -> Result<TaskId, TaskError>;

    fn get_task_status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError>;

    fn try_pop_task(&self) -> Option<Task<T, K>>;

    fn pop_task(&self) -> impl Future<Output = Result<Task<T, K>, TaskError>> + Send
    where
        T: Send,
        K: Send;

    fn mark_task_success(&self, task_id: TaskId);

    fn mark_task_failed(&self, task_id: TaskId);
}

pub struct SimpleTaskQueue<T, K = String> {
    status_map: Arc<DashMap<TaskId, TaskStatus>>,
    task_keys: Arc<DashMap<TaskKey<K>, TaskId>>,
    next_task_id: AtomicI64,
    sender: Sender<Task<T, K>>,
    receiver: Receiver<Task<T, K>>,
    keep_background_task_running: Arc<AtomicBool>,
    background_task: Option<JoinHandle<()>>,
}

impl<T, K> SimpleTaskQueue<T, K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    pub fn new_with_capacity(capacity: usize) -> SimpleTaskQueue<T, K> {
        const DEFAULT_FINISHED_TASK_TTL: Duration = Duration::from_secs(60);
        const DEFAULT_RUNNING_TASK_TIMEOUT: Duration = Duration::from_secs(60);
        const DEFAULT_BACKGROUND_TASK_INTERVAL: Duration = Duration::from_secs(60);

        SimpleTaskQueue::new_with_background_task_config(
            capacity,
            DEFAULT_FINISHED_TASK_TTL,
            DEFAULT_RUNNING_TASK_TIMEOUT,
            DEFAULT_BACKGROUND_TASK_INTERVAL,
        )
    }

    pub fn new_with_background_task_config(
        capacity: usize,
        finished_task_ttl: Duration,
        running_task_timeout: Duration,
        background_task_interval: Duration,
    ) -> SimpleTaskQueue<T, K> {
        let (sender, receiver) = async_channel::bounded::<Task<T, K>>(capacity);

        let status_map = Arc::new(DashMap::new());
        let task_keys = Arc::new(DashMap::new());
        let keep_background_task_running = Arc::new(AtomicBool::new(true));
        let background_task = spawn_background_task(
            Arc::clone(&status_map),
            Arc::clone(&task_keys),
            Arc::clone(&keep_background_task_running),
            finished_task_ttl,
            running_task_timeout,
            background_task_interval,
        );

        SimpleTaskQueue {
            status_map,
            task_keys,
            next_task_id: AtomicI64::new(1),
            sender,
            receiver,
            keep_background_task_running,
            background_task: Some(background_task),
        }
    }

    pub fn new() -> SimpleTaskQueue<T, K> {
        const DEFAULT_CAPACITY: usize = 1024;
        SimpleTaskQueue::new_with_capacity(DEFAULT_CAPACITY)
    }

    pub fn submit_task(&self, payload: T) -> Result<TaskId, TaskError> {
        self.submit(payload, None)
    }

    pub fn submit_task_with_key(
        &self,
        task_key: impl Into<TaskKey<K>>,
        payload: T,
    ) -> Result<TaskId, TaskError> {
        self.submit(payload, Some(task_key.into()))
    }

    fn submit(&self, payload: T, task_key: Option<TaskKey<K>>) -> Result<TaskId, TaskError> {
        let task_id = self
            .next_task_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| TaskError::TaskIdExhausted)?;

        if let Some(key) = task_key.as_ref() {
            match self.task_keys.entry(key.clone()) {
                Entry::Occupied(_) => return Err(TaskError::DuplicateTaskKey),
                Entry::Vacant(entry) => {
                    entry.insert(task_id);
                }
            }
        }

        self.status_map.insert(task_id, TaskStatus::init());
        let task = Task::new(task_id, task_key.clone(), payload);

        match self.sender.try_send(task) {
            Ok(()) => Ok(task_id),
            Err(TrySendError::Full(_)) => {
                self.rollback_submission(task_id, task_key.as_ref());
                Err(TaskError::QueueFulled)
            }
            Err(TrySendError::Closed(_)) => {
                self.rollback_submission(task_id, task_key.as_ref());
                Err(TaskError::Disconnected)
            }
        }
    }

    fn rollback_submission(&self, task_id: TaskId, task_key: Option<&K>) {
        self.status_map.remove(&task_id);
        if let Some(key) = task_key {
            self.task_keys
                .remove_if(key, |_, stored_id| *stored_id == task_id);
        }
    }

    pub fn get_task_status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError> {
        self.status_map
            .get(&task_id)
            .map(|state| *state)
            .ok_or(TaskError::NotFound)
    }

    pub fn try_pop_task(&self) -> Option<Task<T, K>> {
        match self.receiver.try_recv() {
            Ok(t) => {
                self.mark_task_running(t.id);
                Some(t)
            }
            Err(_) => None,
        }
    }

    pub async fn pop_task(&self) -> Result<Task<T, K>, TaskError> {
        let t = self
            .receiver
            .recv()
            .await
            .map_err(|_| TaskError::Disconnected)?;
        self.mark_task_running(t.id);
        Ok(t)
    }

    fn mark_task_running(&self, task_id: TaskId) {
        if let Some(mut status) = self.status_map.get_mut(&task_id) {
            status.state = TaskState::Running;
            status.running_at = Some(now_millis());
        }
    }

    pub fn mark_task_success(&self, task_id: TaskId) {
        if let Some(mut status) = self.status_map.get_mut(&task_id) {
            finish_task(&mut status, TaskState::Success);
        }
    }

    pub fn mark_task_failed(&self, task_id: TaskId) {
        if let Some(mut status) = self.status_map.get_mut(&task_id) {
            finish_task(&mut status, TaskState::Failed);
        }
    }
}

fn spawn_background_task<K>(
    status_map: Arc<DashMap<TaskId, TaskStatus>>,
    task_keys: Arc<DashMap<TaskKey<K>, TaskId>>,
    keep_running: Arc<AtomicBool>,
    finished_task_ttl: Duration,
    running_task_timeout: Duration,
    background_task_interval: Duration,
) -> JoinHandle<()>
where
    K: Eq + Hash + Send + Sync + 'static,
{
    thread::spawn(move || {
        while keep_running.load(Ordering::Acquire) {
            run_background_task(
                &status_map,
                &task_keys,
                now_millis(),
                finished_task_ttl,
                running_task_timeout,
            );
            thread::park_timeout(background_task_interval);
        }
    })
}

fn run_background_task<K>(
    status_map: &DashMap<TaskId, TaskStatus>,
    task_keys: &DashMap<TaskKey<K>, TaskId>,
    now_millis: i64,
    finished_task_ttl: Duration,
    running_task_timeout: Duration,
) where
    K: Eq + Hash,
{
    let removed_task_ids = cleanup_finished_tasks(status_map, now_millis, finished_task_ttl);
    if !removed_task_ids.is_empty() {
        task_keys.retain(|_, task_id| !removed_task_ids.contains(task_id));
    }
    fail_timed_out_running_tasks(status_map, now_millis, running_task_timeout);
}

fn cleanup_finished_tasks(
    status_map: &DashMap<TaskId, TaskStatus>,
    now_millis: i64,
    finished_task_ttl: Duration,
) -> HashSet<TaskId> {
    let ttl_millis = duration_millis_i64(finished_task_ttl);
    let mut removed_task_ids = HashSet::new();
    status_map.retain(|task_id, task_status| {
        let should_remove = matches!(
            task_status.finished_at(),
            Some(finished_at)
                if is_finished(task_status.state())
                    && finished_at.saturating_add(ttl_millis) <= now_millis
        );
        if should_remove {
            removed_task_ids.insert(*task_id);
        }
        !should_remove
    });
    removed_task_ids
}

fn fail_timed_out_running_tasks(
    status_map: &DashMap<TaskId, TaskStatus>,
    now_millis: i64,
    running_task_timeout: Duration,
) {
    let timeout_millis = duration_millis_i64(running_task_timeout);
    status_map.retain(|_, task_status| {
        if task_status.state() == TaskState::Running
            && matches!(
                task_status.running_at(),
                Some(running_at) if running_at.saturating_add(timeout_millis) <= now_millis
            )
        {
            task_status.state = TaskState::Failed;
            task_status.finished_at = Some(now_millis);
        }

        true
    });
}

fn duration_millis_i64(duration: Duration) -> i64 {
    duration.as_millis().min(i64::MAX as u128) as i64
}

impl<T, K> Drop for SimpleTaskQueue<T, K> {
    fn drop(&mut self) {
        self.keep_background_task_running
            .store(false, Ordering::Release);

        if let Some(background_task) = self.background_task.take() {
            background_task.thread().unpark();
            let _ = background_task.join();
        }
    }
}

fn finish_task(status: &mut TaskStatus, finished_state: TaskState) {
    if status.state == TaskState::Running {
        status.state = finished_state;
        status.finished_at = Some(now_millis());
    }
}

impl<T, K> Default for SimpleTaskQueue<T, K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T, K> TaskQueue<T, K> for SimpleTaskQueue<T, K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    fn submit_task(&self, payload: T) -> Result<TaskId, TaskError> {
        SimpleTaskQueue::submit_task(self, payload)
    }

    fn submit_task_with_key(&self, task_key: TaskKey<K>, payload: T) -> Result<TaskId, TaskError> {
        SimpleTaskQueue::submit_task_with_key(self, task_key, payload)
    }

    fn get_task_status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError> {
        SimpleTaskQueue::get_task_status(self, task_id)
    }

    fn try_pop_task(&self) -> Option<Task<T, K>> {
        SimpleTaskQueue::try_pop_task(self)
    }

    fn pop_task(&self) -> impl Future<Output = Result<Task<T, K>, TaskError>> + Send
    where
        T: Send,
        K: Send,
    {
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

    #[test]
    fn submit_task_sets_status_to_queued() {
        let queue = SimpleTaskQueue::<_, String>::new();
        let task_id = queue.submit_task(Vec::<u8>::new()).unwrap();

        assert_eq!(
            queue.get_task_status(task_id).unwrap().state(),
            TaskState::Queued
        );
    }

    #[test]
    fn generated_task_ids_are_unique() {
        let queue = SimpleTaskQueue::<_, String>::new();
        let first_id = queue.submit_task(Vec::<u8>::new()).unwrap();
        let second_id = queue.submit_task(Vec::<u8>::new()).unwrap();

        assert_eq!(first_id, 1);
        assert_eq!(second_id, 2);
    }

    #[test]
    fn pop_task_exposes_generated_id_and_marks_status_as_running() {
        let queue = SimpleTaskQueue::<_, String>::new();
        let task_id = queue.submit_task(vec![42]).unwrap();
        let task = queue.try_pop_task().unwrap();

        assert_eq!(task.id(), task_id);
        assert_eq!(task.payload(), &vec![42]);
        assert_eq!(task.task_key(), None);
        assert_eq!(
            queue.get_task_status(task_id).unwrap().state(),
            TaskState::Running
        );
    }

    #[test]
    fn running_tasks_can_be_finished() {
        let queue = SimpleTaskQueue::<_, String>::new();
        let success_id = queue.submit_task(Vec::<u8>::new()).unwrap();
        let failed_id = queue.submit_task(Vec::<u8>::new()).unwrap();
        queue.try_pop_task().unwrap();
        queue.mark_task_success(success_id);
        queue.try_pop_task().unwrap();
        queue.mark_task_failed(failed_id);

        assert_eq!(
            queue.get_task_status(success_id).unwrap().state(),
            TaskState::Success
        );
        assert_eq!(
            queue.get_task_status(failed_id).unwrap().state(),
            TaskState::Failed
        );
    }

    #[test]
    fn task_key_is_optional() {
        let queue = SimpleTaskQueue::<_, String>::new();
        let first_id = queue.submit_task(Vec::<u8>::new()).unwrap();
        let second_id = queue.submit_task(Vec::<u8>::new()).unwrap();

        assert_ne!(first_id, second_id);
    }

    #[test]
    fn duplicate_task_key_is_rejected() {
        let queue = SimpleTaskQueue::<_, String>::new();
        let task_id = queue
            .submit_task_with_key("request-1", Vec::<u8>::new())
            .unwrap();

        assert!(matches!(
            queue.submit_task_with_key("request-1", Vec::<u8>::new()),
            Err(TaskError::DuplicateTaskKey)
        ));
        assert_eq!(
            queue.get_task_status(task_id).unwrap().state(),
            TaskState::Queued
        );
        let task = queue.try_pop_task().unwrap();
        assert_eq!(task.task_key().map(String::as_str), Some("request-1"));
    }

    #[test]
    fn task_key_supports_generic_types() {
        let queue = SimpleTaskQueue::<Vec<u8>, u64>::new();
        let task_id = queue.submit_task_with_key(42_u64, Vec::new()).unwrap();

        assert!(matches!(
            queue.submit_task_with_key(42_u64, Vec::new()),
            Err(TaskError::DuplicateTaskKey)
        ));
        let task = queue.try_pop_task().unwrap();
        assert_eq!(task.id(), task_id);
        assert_eq!(task.task_key(), Some(&42_u64));
    }

    #[test]
    fn concurrent_duplicate_task_key_accepts_only_one_task() {
        let queue = Arc::new(SimpleTaskQueue::<_, String>::new());
        let start = Arc::new(std::sync::Barrier::new(8));

        let accepted = thread::scope(|scope| {
            let handles = (0..8)
                .map(|_| {
                    let queue = Arc::clone(&queue);
                    let start = Arc::clone(&start);
                    scope.spawn(move || {
                        start.wait();
                        queue.submit_task_with_key("same-key", Vec::<u8>::new())
                    })
                })
                .collect::<Vec<_>>();

            handles
                .into_iter()
                .map(|handle| usize::from(handle.join().unwrap().is_ok()))
                .sum::<usize>()
        });

        assert_eq!(accepted, 1);
    }

    #[test]
    fn full_queue_does_not_reserve_task_key_or_status() {
        let queue = SimpleTaskQueue::<_, String>::new_with_capacity(1);
        queue.submit_task(Vec::<u8>::new()).unwrap();

        assert!(matches!(
            queue.submit_task_with_key("retryable", Vec::<u8>::new()),
            Err(TaskError::QueueFulled)
        ));
        assert!(queue.task_keys.get("retryable").is_none());

        queue.try_pop_task().unwrap();
        assert!(
            queue
                .submit_task_with_key("retryable", Vec::<u8>::new())
                .is_ok()
        );
    }

    #[test]
    fn submitted_task_records_created_at_without_finished_at() {
        let queue = SimpleTaskQueue::<_, String>::new();
        let before_submit = now_millis();
        let task_id = queue.submit_task(Vec::<u8>::new()).unwrap();

        let status = queue.get_task_status(task_id).unwrap();
        assert_eq!(status.state(), TaskState::Queued);
        assert!(status.created_at() >= before_submit);
        assert!(status.finished_at().is_none());
    }

    #[test]
    fn successful_task_records_finished_at() {
        let queue = SimpleTaskQueue::<_, String>::new();
        let task_id = queue.submit_task(Vec::<u8>::new()).unwrap();
        queue.try_pop_task().unwrap();
        queue.mark_task_success(task_id);

        let status = queue.get_task_status(task_id).unwrap();
        assert_eq!(status.state(), TaskState::Success);
        assert!(status.finished_at().unwrap() >= status.created_at());
    }

    #[test]
    fn cleanup_removes_only_finished_tasks_older_than_ttl() {
        let status_map = DashMap::new();
        status_map.insert(
            1,
            TaskStatus {
                state: TaskState::Success,
                created_at: 0,
                running_at: Some(10),
                finished_at: Some(100),
            },
        );
        status_map.insert(
            2,
            TaskStatus {
                state: TaskState::Failed,
                created_at: 0,
                running_at: Some(10),
                finished_at: Some(950),
            },
        );
        status_map.insert(
            3,
            TaskStatus {
                state: TaskState::Running,
                created_at: 0,
                running_at: Some(10),
                finished_at: None,
            },
        );

        let _ = cleanup_finished_tasks(&status_map, 1_100, Duration::from_millis(500));

        assert!(status_map.get(&1).is_none());
        assert!(status_map.get(&2).is_some());
        assert!(status_map.get(&3).is_some());
    }

    #[test]
    fn background_cleanup_removes_finished_tasks_after_ttl() {
        let queue = SimpleTaskQueue::<_, String>::new_with_background_task_config(
            1,
            Duration::from_millis(0),
            Duration::from_secs(60),
            Duration::from_millis(1),
        );
        let task_id = queue
            .submit_task_with_key("reusable-after-ttl", Vec::<u8>::new())
            .unwrap();
        let task = queue.try_pop_task().unwrap();
        queue.mark_task_success(task.id());

        for _ in 0..100 {
            if matches!(queue.get_task_status(task_id), Err(TaskError::NotFound)) {
                assert!(
                    queue
                        .submit_task_with_key("reusable-after-ttl", Vec::<u8>::new())
                        .is_ok()
                );
                return;
            }

            thread::sleep(Duration::from_millis(1));
        }

        panic!("finished task status was not cleaned");
    }

    #[test]
    fn background_task_marks_running_tasks_failed_after_timeout() {
        let status_map = DashMap::new();
        status_map.insert(
            1,
            TaskStatus {
                state: TaskState::Running,
                created_at: 0,
                running_at: Some(100),
                finished_at: None,
            },
        );
        status_map.insert(
            2,
            TaskStatus {
                state: TaskState::Running,
                created_at: 0,
                running_at: Some(900),
                finished_at: None,
            },
        );

        fail_timed_out_running_tasks(&status_map, 1_100, Duration::from_millis(500));

        let old_status = status_map.get(&1).unwrap();
        assert_eq!(old_status.state(), TaskState::Failed);
        assert_eq!(old_status.finished_at(), Some(1_100));

        let recent_status = status_map.get(&2).unwrap();
        assert_eq!(recent_status.state(), TaskState::Running);
        assert!(recent_status.finished_at().is_none());
    }

    #[test]
    fn background_task_fails_timed_out_running_task() {
        let queue = SimpleTaskQueue::<_, String>::new_with_background_task_config(
            1,
            Duration::from_secs(60),
            Duration::from_millis(0),
            Duration::from_millis(1),
        );
        queue.submit_task(Vec::<u8>::new()).unwrap();
        let task = queue.try_pop_task().unwrap();

        for _ in 0..100 {
            let status = queue.get_task_status(task.id()).unwrap();
            if status.state() == TaskState::Failed {
                assert!(status.finished_at().is_some());
                return;
            }

            thread::sleep(Duration::from_millis(1));
        }

        panic!("running task was not marked failed after timeout");
    }
}
