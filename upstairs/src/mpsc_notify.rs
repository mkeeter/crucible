// Copyright 2023 Oxide Computer Company
//! Notification channel with MPSC characteristics
//!
//! This channel allows posting of notifications from one-or-more senders to a
//! single receiver.  Unlike `tokio::sync::mpsc`, `send` is synchronous and only
//! a single permit is stored in the channel at a time.  Compared with
//! `tokio::sync::Notify`, it's possible to detect when the last sender has been
//! dropped.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Sender side of a MPSC notify channel
///
/// This is a lightweight handle that can be cloned and passed by value
#[derive(Debug, Clone)]
pub struct Sender(Arc<SenderInner>);

/// Handle to the actual notify data
///
/// This is a separate data structure so that we can close the channel once all
/// senders are dropped.
#[derive(Debug)]
struct SenderInner(Arc<NotifyData>);

impl Drop for SenderInner {
    fn drop(&mut self) {
        self.0.running.store(false, Ordering::SeqCst);
        self.0.notify.notify_one();
    }
}

#[derive(Debug)]
struct NotifyData {
    notify: Notify,
    running: AtomicBool,
}

impl Sender {
    /// Wakes up the receiver side of the channel
    pub fn send(&self) {
        let r: &SenderInner = self.0.as_ref();
        r.0.notify.notify_one()
    }
}

/// Receiver side of a MPSC notify channel
#[derive(Debug)]
pub struct Receiver(Arc<NotifyData>);

impl Receiver {
    /// Waits for a sender to call `send`, or for the last sender to be dropped
    ///
    /// Returns `true` if senders are still present and `false` otherwise
    pub async fn recv(&self) -> bool {
        self.0.notify.notified().await;
        self.0.running.load(Ordering::SeqCst)
    }
}

/// Builds a new MPSC notification channel
pub fn channel() -> (Sender, Receiver) {
    let n = Arc::new(NotifyData {
        notify: Notify::new(),
        running: AtomicBool::new(true),
    });
    let r = SenderInner(n.clone());
    (Sender(Arc::new(r)), Receiver(n))
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_mpsc_notify() {
        let (tx, rx) = channel();
        tx.send();
        assert!(rx.recv().await);
        let timeout = tokio::select! {
            _ = rx.recv() => false,
            _ = tokio::time::sleep(Duration::from_millis(10)) => true,
        };
        assert!(timeout);

        let tx2 = tx.clone();
        tx2.send();
        assert!(rx.recv().await);

        drop(tx);
        tx2.send();
        assert!(rx.recv().await);

        drop(tx2);
        assert!(!rx.recv().await);

        let timeout = tokio::select! {
            _ = rx.recv() => false,
            _ = tokio::time::sleep(Duration::from_millis(10)) => true,
        };
        assert!(timeout);
    }
}
