use crate::executor::Runnable;
use crate::queue::PeriodicTask;
use crate::queue::Queue;
use std::thread;
use std::time::Duration;
use std::borrow::Borrow;
use diesel::PgConnection;

pub struct Scheduler<Conn>
where
    Conn: Borrow<PgConnection> + Send + 'static
{
    pub check_period: u64,
    pub error_margin_seconds: u64,
    pub queue: Queue<Conn>,
}

impl<Conn> Drop for Scheduler<Conn>
where
    Conn: Borrow<PgConnection> + Send + 'static
{
    fn drop(&mut self) {
        Scheduler::start_new(self.check_period, self.error_margin_seconds)
    }
}

impl Scheduler<PgConnection> {
    pub fn start_new(check_period: u64, error_margin_seconds: u64) {
        let builder = thread::Builder::new().name("scheduler".to_string());

        builder
            .spawn(move || {
                let queue = Queue::new();
                let scheduler = Self::new(check_period, error_margin_seconds, queue);

                scheduler.schedule_loop();
            })
            .unwrap();
    }
}

impl<Conn> Scheduler<Conn>
where
    Conn: Borrow<PgConnection> + Send + 'static
{
    pub fn new(check_period: u64, error_margin_seconds: u64, queue: Queue<Conn>) -> Self {
        Self {
            check_period,
            queue,
            error_margin_seconds,
        }
    }

    pub fn start(self) {
        let builder = thread::Builder::new().name("scheduler".to_string());

        builder
            .spawn(move || {
                self.schedule_loop();
            })
            .unwrap();
    }

    pub fn schedule_loop(&self) {
        let sleep_duration = Duration::from_secs(self.check_period);

        loop {
            self.schedule();

            thread::sleep(sleep_duration);
        }
    }

    pub fn schedule(&self) {
        if let Some(tasks) = self
            .queue
            .fetch_periodic_tasks(self.error_margin_seconds as i64)
        {
            for task in tasks {
                self.process_task(task);
            }
        };
    }

    fn process_task(&self, task: PeriodicTask) {
        match task.scheduled_at {
            None => {
                self.queue.schedule_next_task_execution(&task).unwrap();
            }
            Some(_) => {
                let actual_task: Box<dyn Runnable> =
                    serde_json::from_value(task.metadata.clone()).unwrap();

                self.queue.push_task(&(*actual_task)).unwrap();

                self.queue.schedule_next_task_execution(&task).unwrap();
            }
        }
    }
}

#[cfg(test)]
mod job_scheduler_tests {
    use super::Scheduler;
    use crate::executor::Error;
    use crate::executor::Runnable;
    use crate::queue::Queue;
    use crate::queue::Task;
    use crate::schema::fang_tasks;
    use crate::typetag;
    use diesel::pg::PgConnection;
    use diesel::prelude::*;
    use serde::{Deserialize, Serialize};
    use std::thread;
    use std::time::Duration;

    #[derive(Serialize, Deserialize)]
    struct ScheduledJob {}

    #[typetag::serde]
    impl Runnable for ScheduledJob {
        fn run(&self, _connection: &PgConnection) -> Result<(), Error> {
            Ok(())
        }

        fn task_type(&self) -> String {
            "schedule".to_string()
        }
    }

    #[test]
    #[ignore]
    fn schedules_jobs() {
        let queue = Queue::new();

        queue.push_periodic_task(&ScheduledJob {}, 10).unwrap();
        Scheduler::start_new(1, 2);

        let sleep_duration = Duration::from_secs(15);
        thread::sleep(sleep_duration);

        let tasks = get_all_tasks(&queue.connection);

        assert_eq!(1, tasks.len());
    }

    fn get_all_tasks(conn: &PgConnection) -> Vec<Task> {
        fang_tasks::table
            .filter(fang_tasks::task_type.eq("schedule"))
            .get_results::<Task>(conn)
            .unwrap()
    }
}
