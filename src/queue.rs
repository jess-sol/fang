use crate::executor::Runnable;
use crate::schema::fang_periodic_tasks;
use crate::schema::fang_tasks;
use crate::schema::FangTaskState;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2;
use diesel::result::Error;
use dotenv::dotenv;
use std::env;
use uuid::Uuid;
use std::borrow::Borrow;

#[derive(Queryable, Identifiable, Debug, Eq, PartialEq, Clone)]
#[table_name = "fang_tasks"]
pub struct Task {
    pub id: Uuid,
    pub metadata: serde_json::Value,
    pub error_message: Option<String>,
    pub state: FangTaskState,
    pub task_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Queryable, Identifiable, Debug, Eq, PartialEq, Clone)]
#[table_name = "fang_periodic_tasks"]
pub struct PeriodicTask {
    pub id: Uuid,
    pub metadata: serde_json::Value,
    pub period_in_seconds: i32,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Insertable)]
#[table_name = "fang_tasks"]
pub struct NewTask {
    pub metadata: serde_json::Value,
    pub task_type: String,
}

#[derive(Insertable)]
#[table_name = "fang_periodic_tasks"]
pub struct NewPeriodicTask {
    pub metadata: serde_json::Value,
    pub period_in_seconds: i32,
}

pub struct Queue<Conn>
where
    Conn: Borrow<PgConnection>,
{
    pub connection: Conn,
}

impl Default for Queue<PgConnection> {
    fn default() -> Self {
        Self::new()
    }
}

impl Queue<PgConnection> {
    pub fn new() -> Self {
        let connection = Self::pg_connection(None);

        Self { connection }
    }

    pub fn new_with_url(database_url: String) -> Self {
        let connection = Self::pg_connection(Some(database_url));

        Self { connection }
    }

    pub fn connection_pool(pool_size: u32) -> r2d2::Pool<r2d2::ConnectionManager<PgConnection>> {
        dotenv().ok();

        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

        let manager = r2d2::ConnectionManager::<PgConnection>::new(database_url);

        r2d2::Pool::builder()
            .max_size(pool_size)
            .build(manager)
            .unwrap()
    }
}

impl<Conn> Queue<Conn>
where
    Conn: Borrow<PgConnection>,
{
    pub fn new_with_connection(connection: Conn) -> Self {
        Self { connection }
    }

    pub fn push_task(&self, job: &dyn Runnable) -> Result<Task, Error> {
        Self::push_task_query(&self.connection, job)
    }

    pub fn push_task_query(connection: &Conn, job: &dyn Runnable) -> Result<Task, Error> {
        let json_job = serde_json::to_value(job).unwrap();

        match Self::find_task_by_metadata_query(connection, &json_job) {
            Some(task) => Ok(task),
            None => {
                let new_task = NewTask {
                    metadata: json_job.clone(),
                    task_type: job.task_type(),
                };
                Self::insert_query(connection, &new_task)
            }
        }
    }

    pub fn push_periodic_task(
        &self,
        job: &dyn Runnable,
        period: i32,
    ) -> Result<PeriodicTask, Error> {
        Self::push_periodic_task_query(&self.connection, job, period)
    }

    pub fn push_periodic_task_query(
        connection: &Conn,
        job: &dyn Runnable,
        period: i32,
    ) -> Result<PeriodicTask, Error> {
        let json_job = serde_json::to_value(job).unwrap();

        match Self::find_periodic_task_by_metadata_query(connection, &json_job) {
            Some(task) => Ok(task),
            None => {
                let new_task = NewPeriodicTask {
                    metadata: json_job,
                    period_in_seconds: period,
                };

                diesel::insert_into(fang_periodic_tasks::table)
                    .values(new_task)
                    .get_result::<PeriodicTask>(connection.borrow())
            }
        }
    }

    pub fn enqueue_task(job: &dyn Runnable) -> Result<Task, Error> {
        Queue::new().push_task(job)
    }

    pub fn insert(&self, params: &NewTask) -> Result<Task, Error> {
        Self::insert_query(&self.connection, params)
    }

    pub fn insert_query(connection: &Conn, params: &NewTask) -> Result<Task, Error> {
        diesel::insert_into(fang_tasks::table)
            .values(params)
            .get_result::<Task>(connection.borrow())
    }

    pub fn fetch_task(&self, task_type: &Option<String>) -> Option<Task> {
        Self::fetch_task_query(&self.connection, task_type)
    }

    pub fn fetch_task_query(connection: &Conn, task_type: &Option<String>) -> Option<Task> {
        match task_type {
            None => Self::fetch_any_task_query(connection),
            Some(task_type_str) => Self::fetch_task_of_type_query(connection, task_type_str),
        }
    }

    pub fn fetch_and_touch(&self, task_type: &Option<String>) -> Result<Option<Task>, Error> {
        Self::fetch_and_touch_query(&self.connection, task_type)
    }

    pub fn fetch_and_touch_query(
        connection: &Conn,
        task_type: &Option<String>,
    ) -> Result<Option<Task>, Error> {
        connection.borrow().transaction::<Option<Task>, Error, _>(|| {
            let found_task = Self::fetch_task_query(connection, task_type);

            if found_task.is_none() {
                return Ok(None);
            }

            match Self::start_processing_task_query(connection, &found_task.unwrap()) {
                Ok(updated_task) => Ok(Some(updated_task)),
                Err(err) => Err(err),
            }
        })
    }

    pub fn find_task_by_id(&self, id: Uuid) -> Option<Task> {
        Self::find_task_by_id_query(&self.connection, id)
    }

    pub fn find_task_by_id_query(connection: &Conn, id: Uuid) -> Option<Task> {
        fang_tasks::table
            .filter(fang_tasks::id.eq(id))
            .first::<Task>(connection.borrow())
            .ok()
    }

    pub fn find_periodic_task_by_id(&self, id: Uuid) -> Option<PeriodicTask> {
        Self::find_periodic_task_by_id_query(&self.connection, id)
    }

    pub fn find_periodic_task_by_id_query(
        connection: &Conn,
        id: Uuid,
    ) -> Option<PeriodicTask> {
        fang_periodic_tasks::table
            .filter(fang_periodic_tasks::id.eq(id))
            .first::<PeriodicTask>(connection.borrow())
            .ok()
    }

    pub fn fetch_periodic_tasks(&self, error_margin_seconds: i64) -> Option<Vec<PeriodicTask>> {
        Self::fetch_periodic_tasks_query(&self.connection, error_margin_seconds)
    }

    pub fn fetch_periodic_tasks_query(
        connection: &Conn,
        error_margin_seconds: i64,
    ) -> Option<Vec<PeriodicTask>> {
        let current_time = Self::current_time();

        let low_limit = current_time - Duration::seconds(error_margin_seconds);
        let high_limit = current_time + Duration::seconds(error_margin_seconds);

        fang_periodic_tasks::table
            .filter(
                fang_periodic_tasks::scheduled_at
                    .gt(low_limit)
                    .and(fang_periodic_tasks::scheduled_at.lt(high_limit)),
            )
            .or_filter(fang_periodic_tasks::scheduled_at.is_null())
            .load::<PeriodicTask>(connection.borrow())
            .ok()
    }

    pub fn schedule_next_task_execution(&self, task: &PeriodicTask) -> Result<PeriodicTask, Error> {
        let current_time = Self::current_time();
        let scheduled_at = current_time + Duration::seconds(task.period_in_seconds.into());

        diesel::update(task)
            .set((
                fang_periodic_tasks::scheduled_at.eq(scheduled_at),
                fang_periodic_tasks::updated_at.eq(current_time),
            ))
            .get_result::<PeriodicTask>(self.connection.borrow())
    }

    pub fn remove_all_tasks(&self) -> Result<usize, Error> {
        Self::remove_all_tasks_query(&self.connection)
    }

    pub fn remove_all_tasks_query(connection: &Conn) -> Result<usize, Error> {
        diesel::delete(fang_tasks::table).execute(connection.borrow())
    }

    pub fn remove_tasks_of_type(&self, task_type: &str) -> Result<usize, Error> {
        Self::remove_tasks_of_type_query(&self.connection, task_type)
    }

    pub fn remove_tasks_of_type_query(
        connection: &Conn,
        task_type: &str,
    ) -> Result<usize, Error> {
        let query = fang_tasks::table.filter(fang_tasks::task_type.eq(task_type));

        diesel::delete(query).execute(connection.borrow())
    }

    pub fn remove_all_periodic_tasks(&self) -> Result<usize, Error> {
        Self::remove_all_periodic_tasks_query(&self.connection)
    }

    pub fn remove_all_periodic_tasks_query(connection: &Conn) -> Result<usize, Error> {
        diesel::delete(fang_periodic_tasks::table).execute(connection.borrow())
    }

    pub fn remove_task(&self, id: Uuid) -> Result<usize, Error> {
        Self::remove_task_query(&self.connection, id)
    }

    pub fn remove_task_query(connection: &Conn, id: Uuid) -> Result<usize, Error> {
        let query = fang_tasks::table.filter(fang_tasks::id.eq(id));

        diesel::delete(query).execute(connection.borrow())
    }

    pub fn finish_task(&self, task: &Task) -> Result<Task, Error> {
        Self::finish_task_query(&self.connection, task)
    }

    pub fn finish_task_query(connection: &Conn, task: &Task) -> Result<Task, Error> {
        diesel::update(task)
            .set((
                fang_tasks::state.eq(FangTaskState::Finished),
                fang_tasks::updated_at.eq(Self::current_time()),
            ))
            .get_result::<Task>(connection.borrow())
    }

    pub fn start_processing_task(&self, task: &Task) -> Result<Task, Error> {
        Self::start_processing_task_query(&self.connection, task)
    }

    pub fn start_processing_task_query(
        connection: &Conn,
        task: &Task,
    ) -> Result<Task, Error> {
        diesel::update(task)
            .set((
                fang_tasks::state.eq(FangTaskState::InProgress),
                fang_tasks::updated_at.eq(Self::current_time()),
            ))
            .get_result::<Task>(connection.borrow())
    }

    pub fn fail_task(&self, task: &Task, error: String) -> Result<Task, Error> {
        Self::fail_task_query(&self.connection, task, error)
    }

    pub fn fail_task_query(
        connection: &Conn,
        task: &Task,
        error: String,
    ) -> Result<Task, Error> {
        diesel::update(task)
            .set((
                fang_tasks::state.eq(FangTaskState::Failed),
                fang_tasks::error_message.eq(error),
                fang_tasks::updated_at.eq(Self::current_time()),
            ))
            .get_result::<Task>(connection.borrow())
    }

    fn current_time() -> DateTime<Utc> {
        Utc::now()
    }

    fn pg_connection(database_url: Option<String>) -> PgConnection {
        dotenv().ok();

        let url = match database_url {
            Some(string_url) => string_url,
            None => env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
        };

        PgConnection::establish(&url).unwrap_or_else(|_| panic!("Error connecting to {}", url))
    }

    fn fetch_any_task_query(connection: &Conn) -> Option<Task> {
        fang_tasks::table
            .order(fang_tasks::created_at.asc())
            .limit(1)
            .filter(fang_tasks::state.eq(FangTaskState::New))
            .for_update()
            .skip_locked()
            .get_result::<Task>(connection.borrow())
            .ok()
    }

    fn fetch_task_of_type_query(connection: &Conn, task_type: &str) -> Option<Task> {
        fang_tasks::table
            .order(fang_tasks::created_at.asc())
            .limit(1)
            .filter(fang_tasks::state.eq(FangTaskState::New))
            .filter(fang_tasks::task_type.eq(task_type))
            .for_update()
            .skip_locked()
            .get_result::<Task>(connection.borrow())
            .ok()
    }

    fn find_periodic_task_by_metadata_query(
        connection: &Conn,
        metadata: &serde_json::Value,
    ) -> Option<PeriodicTask> {
        fang_periodic_tasks::table
            .filter(fang_periodic_tasks::metadata.eq(metadata))
            .first::<PeriodicTask>(connection.borrow())
            .ok()
    }

    fn find_task_by_metadata_query(
        connection: &Conn,
        metadata: &serde_json::Value,
    ) -> Option<Task> {
        fang_tasks::table
            .filter(fang_tasks::metadata.eq(metadata))
            .filter(
                fang_tasks::state
                    .eq(FangTaskState::New)
                    .or(fang_tasks::state.eq(FangTaskState::InProgress)),
            )
            .first::<Task>(connection.borrow())
            .ok()
    }
}

#[cfg(test)]
mod queue_tests {
    use super::NewTask;
    use super::PeriodicTask;
    use super::Queue;
    use super::Task;
    use crate::executor::Error as ExecutorError;
    use crate::executor::Runnable;
    use crate::schema::fang_periodic_tasks;
    use crate::schema::fang_tasks;
    use crate::schema::FangTaskState;
    use crate::typetag;
    use chrono::prelude::*;
    use chrono::{DateTime, Duration, Utc};
    use diesel::connection::Connection;
    use diesel::prelude::*;
    use diesel::result::Error;
    use serde::{Deserialize, Serialize};

    #[test]
    fn insert_inserts_task() {
        let queue = Queue::new();

        let new_task = NewTask {
            metadata: serde_json::json!(true),
            task_type: "common".to_string(),
        };

        let result = queue
            .connection
            .test_transaction::<Task, Error, _>(|| queue.insert(&new_task));

        assert_eq!(result.state, FangTaskState::New);
        assert_eq!(result.error_message, None);
    }

    #[test]
    fn fetch_task_fetches_the_oldest_task() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let timestamp1 = Utc::now() - Duration::hours(40);

            let task1 = insert_job(serde_json::json!(true), timestamp1, &queue.connection);

            let timestamp2 = Utc::now() - Duration::hours(20);

            insert_job(serde_json::json!(false), timestamp2, &queue.connection);

            let found_task = queue.fetch_task(&None).unwrap();

            assert_eq!(found_task.id, task1.id);

            Ok(())
        });
    }

    #[test]
    fn finish_task_updates_state_field() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let task = insert_new_job(&queue.connection);

            let updated_task = queue.finish_task(&task).unwrap();

            assert_eq!(FangTaskState::Finished, updated_task.state);

            Ok(())
        });
    }

    #[test]
    fn fail_task_updates_state_field_and_sets_error_message() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let task = insert_new_job(&queue.connection);
            let error = "Failed".to_string();

            let updated_task = queue.fail_task(&task, error.clone()).unwrap();

            assert_eq!(FangTaskState::Failed, updated_task.state);
            assert_eq!(error, updated_task.error_message.unwrap());

            Ok(())
        });
    }

    #[test]
    fn fetch_and_touch_updates_state() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let _task = insert_new_job(&queue.connection);

            let updated_task = queue.fetch_and_touch(&None).unwrap().unwrap();

            assert_eq!(FangTaskState::InProgress, updated_task.state);

            Ok(())
        });
    }

    #[test]
    fn fetch_and_touch_returns_none() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let task = queue.fetch_and_touch(&None).unwrap();

            assert_eq!(None, task);

            Ok(())
        });
    }

    #[test]
    fn push_task_serializes_and_inserts_task() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let job = Job { number: 10 };
            let task = queue.push_task(&job).unwrap();

            let mut m = serde_json::value::Map::new();
            m.insert(
                "number".to_string(),
                serde_json::value::Value::Number(10.into()),
            );
            m.insert(
                "type".to_string(),
                serde_json::value::Value::String("Job".to_string()),
            );

            assert_eq!(task.metadata, serde_json::value::Value::Object(m));

            Ok(())
        });
    }

    #[test]
    fn push_task_does_not_insert_the_same_task() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let job = Job { number: 10 };
            let task2 = queue.push_task(&job).unwrap();

            let task1 = queue.push_task(&job).unwrap();

            assert_eq!(task1.id, task2.id);

            Ok(())
        });
    }

    #[test]
    fn push_periodic_task() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let job = Job { number: 10 };
            let task = queue.push_periodic_task(&job, 60).unwrap();

            assert_eq!(task.period_in_seconds, 60);
            assert!(queue.find_periodic_task_by_id(task.id).is_some());

            Ok(())
        });
    }

    #[test]
    fn push_periodic_task_returns_existing_job() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let job = Job { number: 10 };
            let task1 = queue.push_periodic_task(&job, 60).unwrap();

            let task2 = queue.push_periodic_task(&job, 60).unwrap();

            assert_eq!(task1.id, task2.id);

            Ok(())
        });
    }

    #[test]
    fn fetch_periodic_tasks_fetches_periodic_task_without_scheduled_at() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let job = Job { number: 10 };
            let task = queue.push_periodic_task(&job, 60).unwrap();

            let schedule_in_future = Utc::now() + Duration::hours(100);

            insert_periodic_job(
                serde_json::json!(true),
                schedule_in_future,
                100,
                &queue.connection,
            );

            let tasks = queue.fetch_periodic_tasks(100).unwrap();

            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].id, task.id);

            Ok(())
        });
    }

    #[test]
    fn schedule_next_task_execution() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let task =
                insert_periodic_job(serde_json::json!(true), Utc::now(), 100, &queue.connection);

            let updated_task = queue.schedule_next_task_execution(&task).unwrap();

            let next_schedule = (task.scheduled_at.unwrap()
                + Duration::seconds(task.period_in_seconds.into()))
            .round_subsecs(0);

            assert_eq!(
                next_schedule,
                updated_task.scheduled_at.unwrap().round_subsecs(0)
            );

            Ok(())
        });
    }

    #[test]
    fn remove_all_periodic_tasks() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let task =
                insert_periodic_job(serde_json::json!(true), Utc::now(), 100, &queue.connection);

            let result = queue.remove_all_periodic_tasks().unwrap();

            assert_eq!(1, result);

            assert_eq!(None, queue.find_periodic_task_by_id(task.id));

            Ok(())
        });
    }

    #[test]
    fn remove_all_tasks() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let task = insert_job(serde_json::json!(true), Utc::now(), &queue.connection);
            let result = queue.remove_all_tasks().unwrap();

            assert_eq!(1, result);

            assert_eq!(None, queue.find_task_by_id(task.id));

            Ok(())
        });
    }

    #[test]
    fn fetch_periodic_tasks() {
        let queue = Queue::new();

        queue.connection.test_transaction::<(), Error, _>(|| {
            let schedule_in_future = Utc::now() + Duration::hours(100);

            insert_periodic_job(
                serde_json::json!(true),
                schedule_in_future,
                100,
                &queue.connection,
            );

            let task =
                insert_periodic_job(serde_json::json!(true), Utc::now(), 100, &queue.connection);

            let tasks = queue.fetch_periodic_tasks(100).unwrap();

            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].id, task.id);

            Ok(())
        });
    }

    #[test]
    fn remove_task() {
        let queue = Queue::new();

        let new_task1 = NewTask {
            metadata: serde_json::json!(true),
            task_type: "common".to_string(),
        };

        let new_task2 = NewTask {
            metadata: serde_json::json!(true),
            task_type: "common".to_string(),
        };

        queue.connection.test_transaction::<(), Error, _>(|| {
            let task1 = queue.insert(&new_task1).unwrap();
            assert!(queue.find_task_by_id(task1.id).is_some());

            let task2 = queue.insert(&new_task2).unwrap();
            assert!(queue.find_task_by_id(task2.id).is_some());

            queue.remove_task(task1.id).unwrap();
            assert!(queue.find_task_by_id(task1.id).is_none());
            assert!(queue.find_task_by_id(task2.id).is_some());

            queue.remove_task(task2.id).unwrap();
            assert!(queue.find_task_by_id(task2.id).is_none());

            Ok(())
        });
    }

    #[test]
    fn remove_task_of_type() {
        let queue = Queue::new();

        let new_task1 = NewTask {
            metadata: serde_json::json!(true),
            task_type: "type1".to_string(),
        };

        let new_task2 = NewTask {
            metadata: serde_json::json!(true),
            task_type: "type2".to_string(),
        };

        queue.connection.test_transaction::<(), Error, _>(|| {
            let task1 = queue.insert(&new_task1).unwrap();
            assert!(queue.find_task_by_id(task1.id).is_some());

            let task2 = queue.insert(&new_task2).unwrap();
            assert!(queue.find_task_by_id(task2.id).is_some());

            queue.remove_tasks_of_type("type1").unwrap();
            assert!(queue.find_task_by_id(task1.id).is_none());
            assert!(queue.find_task_by_id(task2.id).is_some());

            Ok(())
        });
    }

    // this test is ignored because it commits data to the db
    #[test]
    #[ignore]
    fn fetch_task_locks_the_record() {
        let queue = Queue::new();
        let timestamp1 = Utc::now() - Duration::hours(40);

        let task1 = insert_job(
            serde_json::json!(Job { number: 12 }),
            timestamp1,
            &queue.connection,
        );

        let task1_id = task1.id;

        let timestamp2 = Utc::now() - Duration::hours(20);

        let task2 = insert_job(
            serde_json::json!(Job { number: 11 }),
            timestamp2,
            &queue.connection,
        );

        let thread = std::thread::spawn(move || {
            let queue = Queue::new();

            queue.connection.transaction::<(), Error, _>(|| {
                let found_task = queue.fetch_task(&None).unwrap();

                assert_eq!(found_task.id, task1.id);

                std::thread::sleep(std::time::Duration::from_millis(5000));

                Ok(())
            })
        });

        std::thread::sleep(std::time::Duration::from_millis(1000));

        let found_task = queue.fetch_task(&None).unwrap();

        assert_eq!(found_task.id, task2.id);

        let _result = thread.join();

        // returns unlocked record

        let found_task = queue.fetch_task(&None).unwrap();

        assert_eq!(found_task.id, task1_id);
    }

    #[derive(Serialize, Deserialize)]
    struct Job {
        pub number: u16,
    }

    #[typetag::serde]
    impl Runnable for Job {
        fn run(&self, _connection: &PgConnection) -> Result<(), ExecutorError> {
            println!("the number is {}", self.number);

            Ok(())
        }
    }

    fn insert_job(
        metadata: serde_json::Value,
        timestamp: DateTime<Utc>,
        connection: &PgConnection,
    ) -> Task {
        diesel::insert_into(fang_tasks::table)
            .values(&vec![(
                fang_tasks::metadata.eq(metadata),
                fang_tasks::created_at.eq(timestamp),
            )])
            .get_result::<Task>(connection)
            .unwrap()
    }

    fn insert_periodic_job(
        metadata: serde_json::Value,
        timestamp: DateTime<Utc>,
        period_in_seconds: i32,
        connection: &PgConnection,
    ) -> PeriodicTask {
        diesel::insert_into(fang_periodic_tasks::table)
            .values(&vec![(
                fang_periodic_tasks::metadata.eq(metadata),
                fang_periodic_tasks::scheduled_at.eq(timestamp),
                fang_periodic_tasks::period_in_seconds.eq(period_in_seconds),
            )])
            .get_result::<PeriodicTask>(connection)
            .unwrap()
    }

    fn insert_new_job(connection: &PgConnection) -> Task {
        diesel::insert_into(fang_tasks::table)
            .values(&vec![(fang_tasks::metadata.eq(serde_json::json!(true)),)])
            .get_result::<Task>(connection)
            .unwrap()
    }
}
