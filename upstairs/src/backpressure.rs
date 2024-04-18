// Copyright 2024 Oxide Computer Company

use tokio::time::Duration;

/// Configuration for host-side backpressure
///
/// Backpressure adds an artificial delay to host write messages (which are
/// otherwise acked immediately, before actually being complete).  The delay is
/// varied based on two metrics:
///
/// - number of write bytes outstanding
/// - number of jobs in the queue
///
/// The backpressure delay is the max of byte-based delay and queue-based delay.
#[derive(Copy, Clone, Debug)]
pub(crate) struct BackpressureConfig {
    /// Configuration for byte-based backpressure
    pub bytes: BackpressureScale,
    /// Configuration for job-based backpressure
    pub jobs: BackpressureScale,
}

/// Single backpressure curve configuration
#[derive(Copy, Clone, Debug)]
pub(crate) struct BackpressureScale {
    pub start: u64,
    pub max: u64,
    pub scale: Duration,
}

impl BackpressureScale {
    // Our chosen backpressure curve is quadratic for 1/2 of its range, then
    // goes to infinity in the second half.  This gives C0 + C1 continuity.
    pub fn curve(&self, n: u64) -> Duration {
        // Calculate a value from 0-2
        let frac = n.saturating_sub(self.start) as f64
            / (self.max - self.start) as f64
            * 2.0;

        let v = if frac < 1.0 {
            frac
        } else {
            1.0 / (1.0 - (frac - 1.0))
        };
        self.scale.mul_f64(v.powi(2))
    }
}

impl BackpressureConfig {
    #[cfg(test)]
    pub fn disable(&mut self) {
        self.bytes.scale = Duration::ZERO;
        self.jobs.scale = Duration::ZERO;
    }

    pub fn get_backpressure_us(&self, bytes: u64, jobs: u64) -> u64 {
        // Special case if backpressure is disabled
        if self.bytes.scale == Duration::ZERO
            && self.jobs.scale == Duration::ZERO
        {
            return 0;
        }

        // Saturate at 1 hour per job, which is basically infinite
        if bytes >= self.bytes.max || jobs >= self.jobs.max {
            return Duration::from_secs(60 * 60).as_micros() as u64;
        }

        let delay_bytes = self.bytes.curve(bytes).as_micros() as u64;
        let delay_jobs = self.jobs.curve(jobs).as_micros() as u64;

        delay_bytes.max(delay_jobs)
    }
}
