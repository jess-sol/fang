use std::io::Error as IoError;
use std::sync::PoisonError;
use thiserror::Error;
use crate::executor::TaskError;

#[derive(Error, Debug)]
pub enum FangError {
    #[error("The shared state in an executor thread became poisoned")]
    PoisonedLock,

    #[error("Database error: {0:?}")]
    DbError(#[from] diesel::result::Error),

    #[error("Task execution error: {0:?}")]
    TaskError(TaskError),

    #[error("Failed to create executor thread")]
    ExecutorThreadCreationFailed {
        #[from]
        source: IoError,
    },
}

impl<T> From<PoisonError<T>> for FangError {
    fn from(_: PoisonError<T>) -> Self {
        Self::PoisonedLock
    }
}

impl From<TaskError> for FangError {
    fn from(x: TaskError) -> Self {
        Self::TaskError(x)
    }
}
