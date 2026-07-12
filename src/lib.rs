use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use thiserror::Error;

// identify an unique task, task with same TaskId will be reject when submit
pub type TaskId = String;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Task {
    id: TaskId,
    payload: Vec<u8>,
}

impl Task {
    pub fn new(id: impl Into<TaskId>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            id: id.into(),
            payload: payload.into(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn payload(&self) -> &[u8] {
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
}

pub trait TaskQueue {
    fn submit_task(&self, task: Task) -> Result<(), TaskError>;

    fn get_task_status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError>;

    fn pop_task(&self) -> Option<Task>;

    fn mark_task_success(&self, task_id: TaskId);

    fn mark_task_failed(&self, task_id: TaskId);
}

pub struct SimpleTaskQueue {
    status_map: DashMap<TaskId, TaskStatus>,
    wait_queue: SegQueue<Task>,
}

impl SimpleTaskQueue {
    pub fn new() -> SimpleTaskQueue {
        SimpleTaskQueue {
            status_map: DashMap::new(),
            wait_queue: SegQueue::new(),
        }
    }

    pub fn submit_task(&self, task: Task) -> Result<(), TaskError> {
        match self.status_map.entry(task.id.clone()) {
            Entry::Occupied(_) => Err(TaskError::Duplicate),
            Entry::Vacant(entry) => {
                entry.insert(TaskStatus::init());
                self.wait_queue.push(task);
                Ok(())
            }
        }
    }

    pub fn get_task_status(&self, task_id: impl AsRef<str>) -> Result<TaskStatus, TaskError> {
        self.status_map
            .get(task_id.as_ref())
            .map(|state| *state)
            .ok_or(TaskError::NotFound)
    }

    pub fn pop_task(&self) -> Option<Task> {
        self.wait_queue.pop().inspect(|t| {
            if let Some(mut status) = self.status_map.get_mut(&t.id) {
                status.state = TaskState::Running;
                status.running_at = Some(now_millis());
            }
        })
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

fn finish_task(status: &mut TaskStatus, finished_state: TaskState) {
    if status.state == TaskState::Running {
        status.state = finished_state;
        status.finished_at = Some(now_millis());
    }
}

impl Default for SimpleTaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskQueue for SimpleTaskQueue {
    fn submit_task(&self, task: Task) -> Result<(), TaskError> {
        SimpleTaskQueue::submit_task(self, task)
    }

    fn get_task_status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError> {
        SimpleTaskQueue::get_task_status(self, task_id)
    }

    fn pop_task(&self) -> Option<Task> {
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

    fn task(id: &str) -> Task {
        Task {
            id: id.to_string(),
            payload: Vec::new(),
        }
    }

    #[test]
    fn submit_task_sets_status_to_queued() {
        let queue = SimpleTaskQueue::new();

        queue.submit_task(task("task-1")).unwrap();

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap().state(),
            TaskState::Queued
        );
    }

    #[test]
    fn pop_task_marks_status_as_running() {
        let queue = SimpleTaskQueue::new();
        queue.submit_task(task("task-1")).unwrap();

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
        queue.submit_task(task("task-1")).unwrap();

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
        queue.submit_task(task("task-1")).unwrap();

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
        queue.submit_task(task("task-1")).unwrap();

        assert!(matches!(
            queue.submit_task(task("task-1")),
            Err(TaskError::Duplicate)
        ));

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap().state(),
            TaskState::Queued
        );
    }

    #[test]
    fn submitted_task_records_created_at_without_finished_at() {
        let queue = SimpleTaskQueue::new();
        let before_submit = now_millis();

        queue.submit_task(task("task-1")).unwrap();

        let status = queue.get_task_status("task-1".to_string()).unwrap();
        assert_eq!(status.state(), TaskState::Queued);
        assert!(status.created_at() >= before_submit);
        assert!(status.finished_at().is_none());
    }

    #[test]
    fn successful_task_records_finished_at() {
        let queue = SimpleTaskQueue::new();
        queue.submit_task(task("task-1")).unwrap();
        queue.pop_task().unwrap();

        queue.mark_task_success("task-1".to_string());

        let status = queue.get_task_status("task-1".to_string()).unwrap();
        assert_eq!(status.state(), TaskState::Success);
        assert!(status.finished_at().unwrap() >= status.created_at());
    }

    #[test]
    fn failed_task_records_finished_at() {
        let queue = SimpleTaskQueue::new();
        queue.submit_task(task("task-1")).unwrap();
        queue.pop_task().unwrap();

        queue.mark_task_failed("task-1".to_string());

        let status = queue.get_task_status("task-1".to_string()).unwrap();
        assert_eq!(status.state(), TaskState::Failed);
        assert!(status.finished_at().unwrap() >= status.created_at());
    }
}
