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

#[derive(Error, Debug)]
pub enum TaskError {
    #[error("task id not found")]
    NotFound,
    #[error("duplicate task id")]
    Duplicate,
}

pub trait TaskQueue {
    fn submit_task(&self, task: Task) -> Result<(), TaskError>;

    fn get_task_status(&self, task_id: TaskId) -> Result<TaskState, TaskError>;

    fn pop_task(&self) -> Option<Task>;

    fn mark_task_success(&self, task_id: TaskId);

    fn mark_task_failed(&self, task_id: TaskId);
}

pub struct SimpleTaskQueue {
    status_map: DashMap<TaskId, TaskState>,
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
                entry.insert(TaskState::Queued);
                self.wait_queue.push(task);
                Ok(())
            }
        }
    }

    pub fn get_task_status(&self, task_id: impl AsRef<str>) -> Result<TaskState, TaskError> {
        self.status_map
            .get(task_id.as_ref())
            .map(|state| *state)
            .ok_or(TaskError::NotFound)
    }

    pub fn pop_task(&self) -> Option<Task> {
        self.wait_queue.pop().inspect(|t| {
            self.status_map.insert(t.id.clone(), TaskState::Running);
        })
    }

    pub fn mark_task_success(&self, task_id: impl AsRef<str>) {
        self.status_map.alter(task_id.as_ref(), |_key, old_state| {
            if old_state == TaskState::Running {
                TaskState::Success
            } else {
                old_state
            }
        });
    }

    pub fn mark_task_failed(&self, task_id: impl AsRef<str>) {
        self.status_map.alter(task_id.as_ref(), |_key, old_state| {
            if old_state == TaskState::Running {
                TaskState::Failed
            } else {
                old_state
            }
        });
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

    fn get_task_status(&self, task_id: TaskId) -> Result<TaskState, TaskError> {
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
            queue.get_task_status("task-1".to_string()).unwrap(),
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
            queue.get_task_status("task-1".to_string()).unwrap(),
            TaskState::Running
        );
    }

    #[test]
    fn running_task_can_be_marked_success() {
        let queue = SimpleTaskQueue::new();
        queue.submit_task(task("task-1")).unwrap();

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap(),
            TaskState::Queued
        );

        queue.pop_task().unwrap();

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap(),
            TaskState::Running
        );

        queue.mark_task_success("task-1".to_string());

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap(),
            TaskState::Success
        );
    }

    #[test]
    fn running_task_can_be_marked_failed() {
        let queue = SimpleTaskQueue::new();
        queue.submit_task(task("task-1")).unwrap();

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap(),
            TaskState::Queued
        );

        queue.pop_task().unwrap();

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap(),
            TaskState::Running
        );

        queue.mark_task_failed("task-1".to_string());

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap(),
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
            queue.get_task_status("task-1".to_string()).unwrap(),
            TaskState::Queued
        );
    }
}
