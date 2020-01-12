use crate::Result;

pub trait ThreadPool: Sized {
    fn new(threads: u32) -> Result<Self>;
    fn spawn(&self, job: impl FnOnce() + Send + 'static);
}

mod naive;

pub use naive::NaiveThreadPool;

pub struct SharedQueueThreadPool;
pub struct RayonThreadPool;
impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(SharedQueueThreadPool)
    }
    fn spawn(&self, job: impl FnOnce() + Send + 'static) {}
}
impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(RayonThreadPool)
    }
    fn spawn(&self, job: impl FnOnce() + Send + 'static) {}
}
