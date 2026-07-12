use dashmap::DashMap;
use std::collections::LinkedList;
use thiserror::Error;

// identify an unique task, task with same TaskId will be reject when submit
type TaskId = String;

pub struct Task {
    id: TaskId,
    payload: Vec<u8>,
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

trait TaskQueue {
    fn submit_task(&mut self, task: Task) -> Result<(), TaskError>;

    fn get_task_status(&self, task_id: TaskId) -> Result<TaskState, TaskError>;

    fn pop_task(&mut self) -> Option<Task>;

    fn mark_task_success(&mut self, task_id: TaskId);

    fn mark_task_failed(&mut self, task_id: TaskId);
}

struct SimpleTaskQueue {
    status_map: DashMap<TaskId, TaskState>,
    wait_list: LinkedList<Task>,
}

impl TaskQueue for SimpleTaskQueue {
    fn submit_task(&mut self, task: Task) -> Result<(), TaskError> {
        if self.status_map.contains_key(&task.id) {
            Err(TaskError::Duplicate)
        } else {
            let task_id = task.id.clone();
            self.status_map.insert(task_id.clone(), TaskState::Queued);
            self.wait_list.push_back(task);
            Ok(())
        }
    }

    fn get_task_status(&self, task_id: TaskId) -> Result<TaskState, TaskError> {
        self.status_map
            .get(&task_id)
            .map(|state| *state)
            .ok_or(TaskError::NotFound)
    }

    fn pop_task(&mut self) -> Option<Task> {
        self.wait_list.pop_front().inspect(|t| {
            self.status_map.insert(t.id.clone(), TaskState::Running);
        })
    }

    fn mark_task_success(&mut self, task_id: TaskId) {
        self.status_map.alter(&task_id, |_key, old_state| {
            if old_state == TaskState::Running {
                TaskState::Success
            } else {
                old_state
            }
        });
    }

    fn mark_task_failed(&mut self, task_id: TaskId) {
        self.status_map.alter(&task_id, |_key, old_state| {
            if old_state == TaskState::Running {
                TaskState::Failed
            } else {
                old_state
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue() -> SimpleTaskQueue {
        SimpleTaskQueue {
            status_map: DashMap::new(),
            wait_list: LinkedList::new(),
        }
    }

    fn task(id: &str) -> Task {
        Task {
            id: id.to_string(),
            payload: Vec::new(),
        }
    }

    #[test]
    fn submit_task_sets_status_to_queued() {
        let mut queue = queue();

        queue.submit_task(task("task-1")).unwrap();

        assert_eq!(
            queue.get_task_status("task-1".to_string()).unwrap(),
            TaskState::Queued
        );
    }

    #[test]
    fn pop_task_marks_status_as_running() {
        let mut queue = queue();
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
        let mut queue = queue();
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
        let mut queue = queue();
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
        let mut queue = queue();
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
