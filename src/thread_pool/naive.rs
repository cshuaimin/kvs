use super::ThreadPool;
use crate::Result;
use std::thread;

pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self> {
        Ok(NaiveThreadPool)
    }

    fn spawn(&self, job: impl FnOnce() + Send + 'static) {
        thread::spawn(job);
    }
}
