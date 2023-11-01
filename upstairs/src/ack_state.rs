use crate::JobId;
use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default)]
pub struct AckState {
    ack_state: BTreeMap<JobId, AckStatus>,
    ackable: BTreeSet<JobId>,
}

impl AckState {
    pub fn ackable_work(&self) -> BTreeSet<JobId> {
        self.ackable.clone()
    }

    pub fn insert(&mut self, job: JobId, ack: AckStatus) {
        match ack {
            AckStatus::NotAcked => {}
            AckStatus::AckReady => {
                self.ackable.insert(job);
            }
            AckStatus::Acked => panic!("new job must not already be acked"),
        }
        self.ack_state.insert(job, ack);
    }

    pub fn get(&self, job: &JobId) -> Option<&AckStatus> {
        self.ack_state.get(job)
    }

    pub fn remove(&mut self, job: &JobId) {
        self.ackable.remove(job);
        self.ack_state.remove(job);
    }

    /// Borrows the given job's ack status
    ///
    /// The handle will automatically update `self.ackable` when dropped
    pub fn get_mut(&mut self, job: &JobId) -> Option<AckStatusHandle> {
        Some(AckStatusHandle::new(
            *job,
            self.ack_state.get_mut(job)?,
            &mut self.ackable,
        ))
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum AckStatus {
    NotAcked,
    AckReady,
    Acked,
}

impl std::fmt::Display for AckStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Make sure to right-align output on 8 characters to match with
        // show_all_work
        match self {
            AckStatus::NotAcked => {
                write!(f, "{0:>8}", "NotAcked")
            }
            AckStatus::AckReady => {
                write!(f, "{0:>8}", "AckReady")
            }
            AckStatus::Acked => {
                write!(f, "{0:>8}", "Acked")
            }
        }
    }
}

/// Handle for a `DownstairsIO` that keeps secondary data in sync
///
/// Many parts of the code want to modify a `DownstairsIO` by directly poking
/// its fields.  This makes it hard to keep secondary data in sync, e.g.
/// maintaining a separate list of all ackable IOs.
pub struct AckStatusHandle<'a> {
    pub ack_status: &'a mut AckStatus,
    job_id: JobId,
    initial_status: AckStatus,
    ackable: &'a mut BTreeSet<JobId>,
}

impl<'a> std::fmt::Debug for AckStatusHandle<'a> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        self.ack_status.fmt(f)
    }
}

impl<'a> AckStatusHandle<'a> {
    fn new(
        job_id: JobId,
        ack_status: &'a mut AckStatus,
        ackable: &'a mut BTreeSet<JobId>,
    ) -> Self {
        let initial_status = *ack_status;
        Self {
            job_id,
            ack_status,
            initial_status,
            ackable,
        }
    }
}

impl<'a> std::ops::Drop for AckStatusHandle<'a> {
    fn drop(&mut self) {
        match (self.initial_status, *self.ack_status) {
            (AckStatus::NotAcked, AckStatus::AckReady) => {
                let prev = self.ackable.insert(self.job_id);
                assert!(prev);
            }
            (AckStatus::AckReady, AckStatus::Acked | AckStatus::NotAcked) => {
                let prev = self.ackable.remove(&self.job_id);
                assert!(prev);
            }
            // None transitions
            (AckStatus::AckReady, AckStatus::AckReady)
            | (AckStatus::Acked, AckStatus::Acked)
            | (AckStatus::NotAcked, AckStatus::NotAcked) => (),

            // Invalid transitions!
            (AckStatus::NotAcked, AckStatus::Acked)
            | (AckStatus::Acked, AckStatus::NotAcked)
            | (AckStatus::Acked, AckStatus::AckReady) => {
                panic!(
                    "invalid transition: {:?} => {:?}",
                    self.initial_status, self.ack_status
                )
            }
        }
    }
}
