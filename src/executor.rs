use crate::error::FangError;
use crate::queue::Queue;
use crate::queue::Task;
use crate::worker_pool::{SharedState, WorkerState};
use diesel::pg::PgConnection;
use log::error;
use std::thread;
use std::time::Duration;
use std::ops::Deref;

pub struct Executor<Conn>
where
    Conn: Deref<Target=PgConnection>,
{
    pub pooled_connection: Conn,
    pub task_type: Option<String>,
    pub sleep_params: SleepParams,
    pub retention_mode: RetentionMode,
    shared_state: Option<SharedState>,
}

#[derive(Clone)]
pub enum RetentionMode {
    KeepAll,
    RemoveAll,
    RemoveFinished,
}

#[derive(Clone)]
pub struct SleepParams {
    pub sleep_period: u64,
    pub max_sleep_period: u64,
    pub min_sleep_period: u64,
    pub sleep_step: u64,
}

impl SleepParams {
    pub fn maybe_reset_sleep_period(&mut self) {
        if self.sleep_period != self.min_sleep_period {
            self.sleep_period = self.min_sleep_period;
        }
    }

    pub fn maybe_increase_sleep_period(&mut self) {
        if self.sleep_period < self.max_sleep_period {
            self.sleep_period += self.sleep_step;
        }
    }
}

impl Default for SleepParams {
    fn default() -> Self {
        SleepParams {
            sleep_period: 5,
            max_sleep_period: 15,
            min_sleep_period: 5,
            sleep_step: 5,
        }
    }
}

#[derive(Debug)]
pub struct Error {
    pub description: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f,"{}", self.description)
    }
}

impl std::error::Error for Error { }

#[derive(Debug)]
pub struct TaskError(Task, Error);

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f,"{}; {:?}", self.1, self.0)
    }
}

impl std::error::Error for TaskError { }

#[typetag::serde(tag = "type")]
pub trait Runnable {
    fn run(&self, connection: &PgConnection) -> Result<(), Error>;

    fn task_type(&self) -> String {
        "common".to_string()
    }
}

impl<Conn> Executor<Conn>
where
    Conn: Deref<Target=PgConnection>
{
    pub fn new(pooled_connection: Conn) -> Self {
        Self {
            pooled_connection,
            sleep_params: SleepParams::default(),
            retention_mode: RetentionMode::RemoveFinished,
            task_type: None,
            shared_state: None,
        }
    }

    pub fn set_shared_state(&mut self, shared_state: SharedState) {
        self.shared_state = Some(shared_state);
    }

    pub fn set_task_type(&mut self, task_type: String) {
        self.task_type = Some(task_type);
    }

    pub fn set_sleep_params(&mut self, sleep_params: SleepParams) {
        self.sleep_params = sleep_params;
    }

    pub fn set_retention_mode(&mut self, retention_mode: RetentionMode) {
        self.retention_mode = retention_mode;
    }

    pub fn run(&self, task: Task) -> Result<Task, TaskError> {
        let result = self.execute_task(task);
        self.finalize_task(&result);
        result
    }

    pub fn run_tasks(&mut self) -> Result<(), FangError> {
        loop {
            if let Some(ref shared_state) = self.shared_state {
                let shared_state = shared_state.read()?;
                if let WorkerState::Shutdown = *shared_state {
                    return Ok(());
                }
            }

            match self.run_task() {
                Ok(Some(_)) => {
                    self.maybe_reset_sleep_period();
                }
                Ok(None) => {
                    self.sleep();
                }
                Err(error) => {
                    error!("Error while processing task: {:?}", error);
                    self.sleep();
                }
            };
        }
    }

    pub fn run_task(&mut self) -> Result<Option<Task>, FangError> {
        let result = Queue::fetch_and_touch_query(&*self.pooled_connection, &self.task_type.clone());
        if let Ok(Some(ref task)) = result {
            self.run(task.clone())?;
        }
        result.map_err(FangError::DbError)
    }

    pub fn maybe_reset_sleep_period(&mut self) {
        self.sleep_params.maybe_reset_sleep_period();
    }

    pub fn sleep(&mut self) {
        self.sleep_params.maybe_increase_sleep_period();

        thread::sleep(Duration::from_secs(self.sleep_params.sleep_period));
    }

    fn execute_task(&self, task: Task) -> Result<Task, TaskError> {
        let actual_task: Box<dyn Runnable> = serde_json::from_value(task.metadata.clone()).unwrap();
        let task_result = actual_task.run(&self.pooled_connection);

        match task_result {
            Ok(()) => Ok(task),
            Err(error) => Err(TaskError(task, error)),
        }
    }

    fn finalize_task(&self, result: &Result<Task, TaskError>) {
        match self.retention_mode {
            RetentionMode::KeepAll => {
                match result {
                    Ok(task) => Queue::finish_task_query(&*self.pooled_connection, task).unwrap(),
                    Err(TaskError(task, error)) => {
                        Queue::fail_task_query(&*self.pooled_connection, task, error.description.to_owned()).unwrap()
                    }
                };
            }
            RetentionMode::RemoveAll => {
                match result {
                    Ok(task) => Queue::remove_task_query(&*self.pooled_connection, task.id).unwrap(),
                    Err(TaskError(task, _)) => {
                        Queue::remove_task_query(&*self.pooled_connection, task.id).unwrap()
                    }
                };
            }
            RetentionMode::RemoveFinished => match result {
                Ok(task) => {
                    Queue::remove_task_query(&*self.pooled_connection, task.id).unwrap();
                }
                Err(TaskError(task, error)) => {
                    Queue::fail_task_query(&*self.pooled_connection, task, error.description.to_owned()).unwrap();
                }
            },
        }
    }
}

#[cfg(test)]
mod executor_tests {
    use super::{Error, TaskError};
    use super::Executor;
    use super::RetentionMode;
    use super::Runnable;
    use crate::queue::NewTask;
    use crate::queue::Queue;
    use crate::schema::FangTaskState;
    use crate::typetag;
    use assert_matches::assert_matches;
    use diesel::connection::Connection;
    use diesel::pg::PgConnection;
    use diesel::r2d2::{ConnectionManager, PooledConnection};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct ExecutorJobTest {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for ExecutorJobTest {
        fn run(&self, _connection: &PgConnection) -> Result<(), Error> {
            println!("the number is {}", self.number);

            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct FailedJob {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for FailedJob {
        fn run(&self, _connection: &PgConnection) -> Result<(), Error> {
            let message = format!("the number is {}", self.number);

            Err(Error {
                description: message,
            })
        }
    }

    #[derive(Serialize, Deserialize)]
    struct JobType1 {}

    #[typetag::serde]
    impl Runnable for JobType1 {
        fn run(&self, _connection: &PgConnection) -> Result<(), Error> {
            Ok(())
        }

        fn task_type(&self) -> String {
            "type1".to_string()
        }
    }

    #[derive(Serialize, Deserialize)]
    struct JobType2 {}

    #[typetag::serde]
    impl Runnable for JobType2 {
        fn run(&self, _connection: &PgConnection) -> Result<(), Error> {
            Ok(())
        }

        fn task_type(&self) -> String {
            "type2".to_string()
        }
    }

    pub fn serialize(job: &dyn Runnable) -> serde_json::Value {
        serde_json::to_value(job).unwrap()
    }

    #[test]
    fn executes_and_finishes_task() {
        let job = ExecutorJobTest { number: 10 };

        let new_task = NewTask {
            metadata: serialize(&job),
            task_type: "common".to_string(),
        };

        let mut executor = Executor::new(pooled_connection());
        executor.set_retention_mode(RetentionMode::KeepAll);

        executor
            .pooled_connection
            .test_transaction::<(), Error, _>(|| {
                let task = Queue::insert_query(&*executor.pooled_connection, &new_task).unwrap();

                assert_eq!(FangTaskState::New, task.state);

                executor.run(task.clone()).unwrap();

                let found_task =
                    Queue::find_task_by_id_query(&*executor.pooled_connection, task.id).unwrap();

                assert_eq!(FangTaskState::Finished, found_task.state);

                Ok(())
            });
    }

    #[test]
    #[ignore]
    fn executes_task_only_of_specific_type() {
        let job1 = JobType1 {};
        let job2 = JobType2 {};

        let new_task1 = NewTask {
            metadata: serialize(&job1),
            task_type: "type1".to_string(),
        };

        let new_task2 = NewTask {
            metadata: serialize(&job2),
            task_type: "type2".to_string(),
        };

        let executor = Executor::new(pooled_connection());

        let task1 = Queue::insert_query(&*executor.pooled_connection, &new_task1).unwrap();
        let task2 = Queue::insert_query(&*executor.pooled_connection, &new_task2).unwrap();

        assert_eq!(FangTaskState::New, task1.state);
        assert_eq!(FangTaskState::New, task2.state);

        std::thread::spawn(move || {
            let mut executor = Executor::new(pooled_connection());
            executor.set_retention_mode(RetentionMode::KeepAll);
            executor.set_task_type("type1".to_string());

            executor.run_tasks().unwrap();
        });

        std::thread::sleep(std::time::Duration::from_millis(1000));

        let found_task1 =
            Queue::find_task_by_id_query(&*executor.pooled_connection, task1.id).unwrap();
        assert_eq!(FangTaskState::Finished, found_task1.state);

        let found_task2 =
            Queue::find_task_by_id_query(&*executor.pooled_connection, task2.id).unwrap();
        assert_eq!(FangTaskState::New, found_task2.state);
    }

    #[test]
    fn saves_error_for_failed_task() {
        let job = FailedJob { number: 10 };

        let new_task = NewTask {
            metadata: serialize(&job),
            task_type: "common".to_string(),
        };

        let executor = Executor::new(pooled_connection());

        executor
            .pooled_connection
            .test_transaction::<(), Error, _>(|| {
                let task = Queue::insert_query(&*executor.pooled_connection, &new_task).unwrap();

                assert_eq!(FangTaskState::New, task.state);

                let result = executor.run(task.clone());
                assert_matches!(result, Err(TaskError(_, _)));

                let found_task =
                    Queue::find_task_by_id_query(&*executor.pooled_connection, task.id).unwrap();

                assert_eq!(FangTaskState::Failed, found_task.state);
                assert_eq!(
                    "the number is 10".to_string(),
                    found_task.error_message.unwrap()
                );

                Ok(())
            });
    }

    fn pooled_connection() -> PooledConnection<ConnectionManager<PgConnection>> {
        Queue::connection_pool(5).get().unwrap()
    }
}
