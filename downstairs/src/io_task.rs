// Copyright 2023 Oxide Computer Company
use crate::extent::{ExtentRequest, ExtentRunner};
use slog::{debug, info, o, Logger};
use tokio::sync::mpsc;

/// Number of IO threads to spawn
pub(crate) const IO_THREAD_COUNT: usize = 32;

/// Request to an IO thread
pub(crate) enum IoRequest {
    /// Perform an IO request to the given extent
    ///
    /// The index is the (absolute) extent number.  Each IO task handles every
    /// `IO_THREAD_COUNT`'th extent.
    Io(u32, ExtentRequest),

    /// Add a new extent to the set of extents handled by this thread
    ///
    /// The index is the absolute extent number.
    NewExtent(u32, ExtentRunner),
}

/// Data structure used by the IO task
pub(crate) struct IoTask {
    rx: mpsc::Receiver<IoRequest>,
    log: Logger,
    extents: Vec<Option<ExtentRunner>>,
}

impl IoTask {
    pub(crate) fn spawn_workers(log: &Logger) -> Vec<mpsc::Sender<IoRequest>> {
        (0..IO_THREAD_COUNT)
            .map(|_i| {
                let (tx, rx) = mpsc::channel(1);
                let mut task = IoTask::new(rx, log);
                std::thread::spawn(move || task.run());
                tx
            })
            .collect()
    }
    pub(crate) fn new(rx: mpsc::Receiver<IoRequest>, log: &Logger) -> Self {
        let log = log.new(o!("task" => "io".to_string()));
        Self {
            rx,
            log,
            extents: vec![],
        }
    }
    pub(crate) fn run(&mut self) {
        while let Some(m) = self.rx.blocking_recv() {
            match m {
                IoRequest::Io(index, req) => {
                    let i = index as usize / IO_THREAD_COUNT;
                    if !self.extents[i]
                        .as_mut()
                        .expect("extent should not be closed")
                        .dispatch(req)
                    {
                        info!(self.log, "closing extent {index}");
                        self.extents[i].take();
                    }
                }
                IoRequest::NewExtent(index, e) => {
                    let i = index as usize / IO_THREAD_COUNT;
                    if i >= self.extents.len() {
                        self.extents.resize_with(i + 1, || None);
                    }
                    let prev = self.extents[i].replace(e);
                    assert!(prev.is_none(), "cannot replace open extent");
                    debug!(self.log, "IO task accepted extent {index}");
                }
            }
        }
        debug!(self.log, "extent worker is done")
    }
}
