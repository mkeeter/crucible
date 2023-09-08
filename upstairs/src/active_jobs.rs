// Copyright 2023 Oxide Computer Company

use crate::{AckStatus, DownstairsIO, IOop, ImpactedAddr, ImpactedBlocks};
use std::collections::{BTreeMap, BTreeSet};

/// `ActiveJobs` tracks active jobs by ID
///
/// It exposes an API that roughly matches a `BTreeMap<u64, DownstairsIO>`,
/// but leaves open the possibility for further optimization.
///
/// Notably, there is no way to directly modify a `DownstairsIO` contained in
/// `ActiveJobs`.  Bulk modification can be done with `for_each`, and individual
/// modification can be done with `get_mut`, which returns a
/// `DownstairsIOHandle` instead of a raw `&mut DownstairsIO`.  All of this
/// means that we can keep extra metadata in sync, e.g. a list of all ackable
/// jobs.
#[derive(Debug)]
pub(crate) struct ActiveJobs {
    jobs: BTreeMap<u64, DownstairsIO>,
    block_to_active: BlockToActive,
    ackable: BTreeSet<u64>,
}

impl ActiveJobs {
    pub fn new() -> Self {
        Self {
            jobs: BTreeMap::new(),
            ackable: BTreeSet::new(),
            block_to_active: BlockToActive::new(),
        }
    }

    /// Looks up a job by ID, returning a reference
    #[inline]
    pub fn get(&self, job_id: &u64) -> Option<&DownstairsIO> {
        self.jobs.get(job_id)
    }

    /// Looks up a job by ID, returning a mutable reference
    #[inline]
    pub fn get_mut(&mut self, job_id: &u64) -> Option<DownstairsIOHandle> {
        self.jobs
            .get_mut(job_id)
            .map(|job| DownstairsIOHandle::new(job, &mut self.ackable))
    }

    /// Returns the total number of active jobs
    #[inline]
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    /// Returns `true` if no jobs are active
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    /// Applies a function across all
    #[inline]
    pub fn for_each<F: FnMut(&u64, &mut DownstairsIO)>(&mut self, mut f: F) {
        for (job_id, job) in self.jobs.iter_mut() {
            let handle = DownstairsIOHandle::new(job, &mut self.ackable);
            f(job_id, handle.job);
        }
    }

    /// Inserts a new job ID and its associated IO work
    #[inline]
    pub fn insert(&mut self, job_id: u64, io: DownstairsIO) {
        let blocking = match &io.work {
            IOop::Write { .. }
            | IOop::WriteUnwritten { .. }
            | IOop::Flush { .. }
            | IOop::ExtentLiveNoOp { .. }
            | IOop::ExtentFlushClose { .. }
            | IOop::ExtentLiveReopen { .. } => true,
            IOop::Read { .. } => false,
            _ => panic!("unexpected io work {:?}", io.work),
        };
        let r = match (&io.work, &io.impacted_blocks) {
            (IOop::Flush { .. }, ImpactedBlocks::Empty) => 0..u64::MAX,
            (IOop::Flush { .. }, ImpactedBlocks::InclusiveRange(..)) => {
                panic!("cannot have flush with impacted blocks")
            }
            (_, ImpactedBlocks::Empty) => 0..0, // TODO is this avlid?
            (_, ImpactedBlocks::InclusiveRange(..)) => {
                self.to_lba_range(io.impacted_blocks)
            }
        };
        self.block_to_active.insert_range(r, job_id, blocking);
        self.jobs.insert(job_id, io);
    }

    /// Removes a job by ID, returning its IO work
    ///
    /// # Panics
    /// If the job was not inserted
    #[inline]
    pub fn remove(&mut self, job_id: &u64) -> DownstairsIO {
        let io = self.jobs.remove(job_id).unwrap();
        self.block_to_active.remove_job(*job_id);
        io
    }

    /// Returns an iterator over job IDs
    #[inline]
    pub fn keys(&self) -> std::collections::btree_map::Keys<u64, DownstairsIO> {
        self.jobs.keys()
    }

    /// Returns an iterator over job values
    #[cfg(test)]
    #[inline]
    pub fn values(
        &self,
    ) -> std::collections::btree_map::Values<u64, DownstairsIO> {
        self.jobs.values()
    }

    pub fn deps_for_flush(&self) -> Vec<u64> {
        self.block_to_active.check_range(0..u64::MAX, true)
    }

    pub fn deps_for_write(&self, impacted_blocks: ImpactedBlocks) -> Vec<u64> {
        let r = self.to_lba_range(impacted_blocks);
        self.block_to_active.check_range(r, true)
    }

    pub fn deps_for_read(&self, impacted_blocks: ImpactedBlocks) -> Vec<u64> {
        let r = self.to_lba_range(impacted_blocks);
        self.block_to_active.check_range(r, false)
    }

    // Build the list of dependencies for a live repair job.  These are jobs that
    // must finish before this repair job can move begin.  Because we need all
    // three repair jobs to happen lock step, we have to prevent any IO from
    // hitting the same extent, which means any IO going to our ImpactedBlocks
    // (the whole extent) must finish first (or come after) our job.
    pub fn deps_for_live_repair(
        &self,
        impacted_blocks: ImpactedBlocks,
        close_id: u64,
    ) -> Vec<u64> {
        let mut out = self.deps_for_write(impacted_blocks);
        out.retain(|v| *v <= close_id);
        out
    }

    pub fn ackable_work(&self) -> Vec<u64> {
        self.ackable.iter().cloned().collect()
    }

    /// Converts from an extent + relative block to a global block
    ///
    /// # Panics
    /// If the global block address overflows a `u64`
    fn to_lba(&self, block: ImpactedAddr) -> u64 {
        block
            .extent_id
            .checked_mul(1 << 32)
            .unwrap()
            .checked_add(block.block)
            .unwrap()
    }

    /// Converts from an (inclusive) block range to an (exclusive) LBA range
    ///
    /// # Panics
    /// If any address overflows a `u64`
    fn to_lba_range(&self, block: ImpactedBlocks) -> std::ops::Range<u64> {
        match block {
            ImpactedBlocks::Empty => 0..0,
            ImpactedBlocks::InclusiveRange(start, end) => {
                self.to_lba(start)..self.to_lba(end).checked_add(1).unwrap()
            }
        }
    }
}

impl<'a> IntoIterator for &'a ActiveJobs {
    type Item = (&'a u64, &'a DownstairsIO);
    type IntoIter = std::collections::btree_map::Iter<'a, u64, DownstairsIO>;

    fn into_iter(self) -> Self::IntoIter {
        self.jobs.iter()
    }
}

/// Handle for a `DownstairsIO` that keeps secondary data in sync
///
/// Many parts of the code want to modify a `DownstairsIO` by directly poking
/// its fields.  This makes it hard to keep secondary data in sync, e.g.
/// maintaining a separate list of all ackable IOs.
pub(crate) struct DownstairsIOHandle<'a> {
    pub job: &'a mut DownstairsIO,
    initial_status: AckStatus,
    ackable: &'a mut BTreeSet<u64>,
}

impl<'a> std::fmt::Debug for DownstairsIOHandle<'a> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        self.job.fmt(f)
    }
}

impl<'a> DownstairsIOHandle<'a> {
    fn new(job: &'a mut DownstairsIO, ackable: &'a mut BTreeSet<u64>) -> Self {
        let initial_status = job.ack_status;
        Self {
            job,
            initial_status,
            ackable,
        }
    }

    pub fn job(&mut self) -> &mut DownstairsIO {
        self.job
    }
}

impl<'a> std::ops::Drop for DownstairsIOHandle<'a> {
    fn drop(&mut self) {
        match (self.initial_status, self.job.ack_status) {
            (AckStatus::NotAcked, AckStatus::AckReady) => {
                let prev = self.ackable.insert(self.job.ds_id);
                assert!(prev);
            }
            (AckStatus::AckReady, AckStatus::Acked | AckStatus::NotAcked) => {
                let prev = self.ackable.remove(&self.job.ds_id);
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
                    self.initial_status, self.job.ack_status
                )
            }
        }
    }
}

/// Acceleration data structure to quickly look up dependencies
#[derive(Debug)]
struct BlockToActive {
    /// Mapping from an exclusive block range to a set of dependencies
    ///
    /// The start of the range is the key; the end of the range is the first
    /// item in the value tuple.
    ///
    /// The data structure must maintain the invariant that block ranges never
    /// overlap.
    lba_to_jobs: BTreeMap<u64, (u64, DependencySet)>,
    job_to_range: BTreeMap<u64, std::ops::Range<u64>>,
}

impl BlockToActive {
    fn new() -> Self {
        Self {
            lba_to_jobs: BTreeMap::new(),
            job_to_range: BTreeMap::new(),
        }
    }

    /// Returns all dependencies that are active across the given range
    fn check_range(&self, r: std::ops::Range<u64>, blocking: bool) -> Vec<u64> {
        let mut out = BTreeSet::new();
        for (_start, (_end, set)) in self.iter_overlapping(r) {
            if blocking {
                if set.nonblocking.is_empty() {
                    out.extend(set.blocking.iter().cloned());
                } else {
                    out.extend(set.nonblocking.iter().cloned());
                }
            } else {
                out.extend(set.blocking.iter().cloned());
            }
        }
        out.into_iter().collect()
    }

    fn insert_range(
        &mut self,
        r: std::ops::Range<u64>,
        job: u64,
        blocking: bool,
    ) {
        // Okay, this is a tricky one.  The incoming range `r` can overlap with
        // existing ranges in a variety of ways.
        //
        // The first operation is to split existing ranges at the incoming
        // endpoints.  There are three possibilities, with splits marked by X:
        //
        // existing range:   |-------|
        // new range:            |===========|
        // result:           |---X---|
        //                   ^ split_start
        //
        // existing range:      |-----------|
        // new range:        |===========|
        // result:              |--------X--|
        //                      ^ split_end
        //
        // existing range:   |-------------------|
        // new range:            |===========|
        // result:           |---X-----------X---|
        //                   ^ split_start
        //                   ^ split_end (initially)
        //                       ^ split_end (after split_start is done)
        //
        // The `split_start` and `split_end` variables represent the starting
        // point of an existing range that should be split.
        //
        // Notice that we only split a _maximum_ of two existing ranges here;
        // it's not unbounded.  Also note that a block which is entirely
        // contained within the new range does not require any splits:
        //
        // existing range:       |--------|
        // new range:          |============|
        // result:               |--------|

        let mut split_start = None;
        let mut split_end = None;
        for (start, (end, _set)) in self.iter_overlapping(r.clone()) {
            if r.start >= *start && r.start < *end {
                assert!(split_start.is_none());
                split_start = Some(*start);
            }
            if r.end >= *start && r.end < *end {
                assert!(split_end.is_none());
                split_end = Some(*start);
            }
        }

        // Now, we apply those splits
        if let Some(s) = split_start {
            let prev = self.lba_to_jobs.get_mut(&s).unwrap();
            let v = prev.clone();
            prev.0 = r.start;
            self.lba_to_jobs.insert(r.start, v);
            // Correct for the case where split_start and split_end point into
            // the same existing range (see the diagram above!)
            if split_start == split_end {
                split_end = Some(r.start);
            }
        }
        if let Some(s) = split_end {
            let prev = self.lba_to_jobs.get_mut(&s).unwrap();
            let v = prev.clone();
            prev.0 = r.end;
            self.lba_to_jobs.insert(r.end, v);
        }

        // Iterate over the range covered by our new job, either modifying
        // existing ranges or inserting new ranges as needed.
        let mut pos = r.start;
        while pos != r.end {
            let mut next_start = self
                .lba_to_jobs
                .range_mut(pos..)
                .next()
                .map(|(start, _)| *start)
                .unwrap_or(u64::MAX);
            if next_start == pos {
                // Modify existing range
                let (next_end, v) =
                    self.lba_to_jobs.get_mut(&next_start).unwrap();
                assert!(*next_end <= r.end);
                assert!(*next_end > pos);
                if blocking {
                    v.insert_blocking(job)
                } else {
                    v.insert_nonblocking(job)
                }
                pos = *next_end;
            } else {
                // Insert a new range
                assert!(next_start > pos);
                next_start = next_start.min(r.end);
                assert!(next_start > pos);
                self.lba_to_jobs.insert(
                    pos,
                    (
                        next_start,
                        if blocking {
                            DependencySet::new_blocking(job)
                        } else {
                            DependencySet::new_nonblocking(job)
                        },
                    ),
                );
                pos = next_start;
            }
        }
        self.job_to_range.insert(job, r);
    }

    fn iter_overlapping(
        &self,
        r: std::ops::Range<u64>,
    ) -> impl Iterator<Item = (&u64, &(u64, DependencySet))> {
        let start = self
            .lba_to_jobs
            .range(0..=r.start)
            .rev()
            .next()
            .map(|(start, _)| *start)
            .unwrap_or(r.start);
        self.lba_to_jobs
            .range(start..)
            .skip_while(move |(_start, (end, _set))| *end <= r.start)
            .take_while(move |(start, (_end, _set))| **start < r.end)
    }

    /// Removes the given job from its range
    fn remove_job(&mut self, job: u64) {
        let r = self.job_to_range.remove(&job).unwrap();
        let mut start = r.start;
        while start != r.end {
            let (end, v) = self.lba_to_jobs.get_mut(&start).unwrap();
            v.remove(job);
            start = *end;
        }
        self.lba_to_jobs.retain(|_start, (_end, v)| !v.is_empty());
    }
}

/// A set of dependencies, associated with a range in a [`BlockToActive`]
///
/// Each dependency set has 0-1 blocking dependencies (e.g. writes), and any
/// number of non-blocking dependencies.
#[derive(Clone, Debug, Default)]
struct DependencySet {
    blocking: Option<u64>,
    nonblocking: Vec<u64>,
}

impl DependencySet {
    fn new_blocking(job: u64) -> Self {
        Self {
            blocking: Some(job),
            nonblocking: vec![],
        }
    }
    fn new_nonblocking(job: u64) -> Self {
        Self {
            blocking: None,
            nonblocking: vec![job],
        }
    }

    fn insert_blocking(&mut self, job: u64) {
        if let Some(prev) = self.blocking {
            // Skip backfilling older jobs
            if job < prev {
                return;
            }
        }
        for prev in &self.nonblocking {
            assert!(job > *prev);
        }
        self.blocking = Some(job);
        self.nonblocking.clear();
    }

    fn insert_nonblocking(&mut self, job: u64) {
        if let Some(prev) = self.blocking {
            assert!(job > prev);
        }
        for prev in &self.nonblocking {
            assert!(job > *prev);
        }
        self.nonblocking.push(job);
    }

    fn remove(&mut self, job: u64) {
        if let Some(v) = self.blocking {
            if v == job {
                self.blocking = None;
            }
        }
        self.nonblocking.retain(|&v| v != job);
    }
    fn is_empty(&self) -> bool {
        self.blocking.is_none() && self.nonblocking.is_empty()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use proptest::prelude::*;

    struct Oracle(
        BTreeMap<u64, DependencySet>,
        BTreeMap<u64, std::ops::Range<u64>>,
    );

    impl Oracle {
        fn new() -> Self {
            Self(BTreeMap::new(), BTreeMap::new())
        }
        fn insert_range(
            &mut self,
            r: std::ops::Range<u64>,
            job: u64,
            blocking: bool,
        ) {
            for i in r.clone() {
                let v = self.0.entry(i).or_default();
                if blocking {
                    v.insert_blocking(job)
                } else {
                    v.insert_nonblocking(job)
                }
            }
            self.1.insert(job, r);
        }
        fn check_range(
            &self,
            r: std::ops::Range<u64>,
            blocking: bool,
        ) -> Vec<u64> {
            let mut out = BTreeSet::new();
            for i in r {
                if let Some(s) = self.0.get(&i) {
                    // TODO: refactor this into a DependencySet function?
                    if blocking {
                        if s.nonblocking.is_empty() {
                            out.extend(s.blocking.iter().cloned());
                        } else {
                            out.extend(s.nonblocking.iter().cloned());
                        }
                    } else {
                        out.extend(s.blocking.iter().cloned());
                    }
                }
            }
            out.into_iter().collect()
        }
        fn remove_job(&mut self, job: u64) {
            let r = self.1.remove(&job).unwrap();
            for i in r {
                if let Some(s) = self.0.get_mut(&i) {
                    s.remove(job);
                }
            }
        }
    }

    // write, write, write, read
    // each write is a range + bool
    // read is a range
    proptest! {
        #[test]
        fn test_active_jobs_insert(
            a_start in 0u64..100, a_size in 1u64..50, a_type in any::<bool>(),
            b_start in 0u64..100, b_size in 1u64..50, b_type in any::<bool>(),
            c_start in 0u64..100, c_size in 1u64..50, c_type in any::<bool>(),
            read_start in 0u64..100, read_size in 1u64..50, read_type in any::<bool>(),
        ) {
            let mut dut = BlockToActive::new();
            dut.insert_range(a_start..(a_start + a_size), 1000, a_type);
            dut.insert_range(b_start..(b_start + b_size), 1001, b_type);
            dut.insert_range(c_start..(c_start + c_size), 1002, c_type);

            let mut oracle = Oracle::new();
            oracle.insert_range(a_start..(a_start + a_size), 1000, a_type);
            oracle.insert_range(b_start..(b_start + b_size), 1001, b_type);
            oracle.insert_range(c_start..(c_start + c_size), 1002, c_type);

            let read_dut = dut.check_range(read_start..(read_start + read_size), read_type);
            let read_oracle = oracle.check_range(read_start..(read_start + read_size), read_type);
            assert_eq!(read_dut, read_oracle);
        }

        #[test]
        fn test_active_jobs_insert_remove(
            a_start in 0u64..100, a_size in 1u64..50, a_type in any::<bool>(),
            b_start in 0u64..100, b_size in 1u64..50, b_type in any::<bool>(),
            c_start in 0u64..100, c_size in 1u64..50, c_type in any::<bool>(),
            remove in 0usize..3,
            read_start in 0u64..100, read_size in 1u64..50, read_type in any::<bool>()
        ) {
            let mut dut = BlockToActive::new();
            dut.insert_range(a_start..(a_start + a_size), 1000, a_type);
            dut.insert_range(b_start..(b_start + b_size), 1001, b_type);
            dut.insert_range(c_start..(c_start + c_size), 1002, c_type);

            let mut oracle = Oracle::new();
            oracle.insert_range(a_start..(a_start + a_size), 1000, a_type);
            oracle.insert_range(b_start..(b_start + b_size), 1001, b_type);
            oracle.insert_range(c_start..(c_start + c_size), 1002, c_type);

            dut.remove_job( 1000 + remove as u64);
            oracle.remove_job( 1000 + remove as u64);

            let read_dut = dut.check_range(read_start..(read_start + read_size), read_type);
            let read_oracle = oracle.check_range(read_start..(read_start + read_size), read_type);
            assert_eq!(read_dut, read_oracle);
        }
    }
}
