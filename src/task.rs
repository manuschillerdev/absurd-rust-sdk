use crate::types::{CancellationPolicy, TaskOptions};
use std::marker::PhantomData;

#[derive(Debug)]
pub struct Task<P, R> {
    pub(crate) options: TaskOptions,
    _marker: PhantomData<fn(P) -> R>,
}

impl<P, R> Clone for Task<P, R> {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            _marker: PhantomData,
        }
    }
}

impl<P, R> Task<P, R> {
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_options(TaskOptions::new(name))
    }

    pub fn with_options(options: TaskOptions) -> Self {
        Self {
            options,
            _marker: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.options.name
    }

    pub fn options(&self) -> &TaskOptions {
        &self.options
    }

    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.options.queue = Some(queue.into());
        self
    }

    pub fn default_max_attempts(mut self, default_max_attempts: i32) -> Self {
        self.options.default_max_attempts = Some(default_max_attempts);
        self
    }

    pub fn default_cancellation(mut self, default_cancellation: CancellationPolicy) -> Self {
        self.options.default_cancellation = Some(default_cancellation);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn task_builder_sets_default_cancellation() {
        let cancellation = CancellationPolicy::new().max_delay(Duration::from_secs(30));
        let task = Task::<(), ()>::new("send-email").default_cancellation(cancellation.clone());

        assert_eq!(task.options().default_cancellation, Some(cancellation));
    }
}
