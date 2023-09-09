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
#[derive(Debug, Default)]
pub(crate) struct ActiveJobs {
    jobs: BTreeMap<u64, DownstairsIO>,
    block_to_active: BlockToActive,
    ackable: BTreeSet<u64>,
}

impl ActiveJobs {
    pub fn new() -> Self {
        Self::default()
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
            | IOop::ExtentLiveReopen { .. }
            | IOop::ExtentLiveRepair { .. } => true,
            IOop::Read { .. } => false,
            _ => panic!("unexpected io work {:?}", io.work),
        };
        let r = match (&io.work, &io.impacted_blocks) {
            (IOop::Flush { .. }, ImpactedBlocks::Empty) => {
                ImpactedBlocks::InclusiveRange(
                    ImpactedAddr {
                        extent_id: 0,
                        block: 0,
                    },
                    ImpactedAddr {
                        extent_id: u64::MAX,
                        block: u64::MAX - 1,
                    },
                )
            }
            (_, b) => *b,
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
        let r = ImpactedBlocks::InclusiveRange(
            ImpactedAddr {
                extent_id: 0,
                block: 0,
            },
            ImpactedAddr {
                extent_id: u64::MAX,
                block: u64::MAX - 1,
            },
        );
        self.block_to_active.check_range(r, true)
    }

    pub fn deps_for_write(&self, impacted_blocks: ImpactedBlocks) -> Vec<u64> {
        self.block_to_active.check_range(impacted_blocks, true)
    }

    pub fn deps_for_read(&self, impacted_blocks: ImpactedBlocks) -> Vec<u64> {
        self.block_to_active.check_range(impacted_blocks, false)
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
}

impl<'a> IntoIterator for &'a ActiveJobs {
    type Item = (&'a u64, &'a DownstairsIO);
    type IntoIter = std::collections::btree_map::Iter<'a, u64, DownstairsIO>;

    fn into_iter(self) -> Self::IntoIter {
        self.jobs.iter()
    }
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

/// Acceleration data structure to quickly look up dependencies
#[derive(Debug, Default)]
struct BlockToActive {
    /// Mapping from an exclusive block range to a set of dependencies
    ///
    /// The start of the range is the key; the end of the range is the first
    /// item in the value tuple.
    ///
    /// The data structure must maintain the invariant that block ranges never
    /// overlap.
    addr_to_jobs: BTreeMap<ImpactedAddr, (ImpactedAddr, DependencySet)>,
    job_to_range: BTreeMap<u64, std::ops::Range<ImpactedAddr>>,
}

impl BlockToActive {
    /// Returns all dependencies that are active across the given range
    fn check_range(&self, r: ImpactedBlocks, blocking: bool) -> Vec<u64> {
        let mut out = BTreeSet::new();
        if let Some(r) = Self::blocks_to_range(r) {
            for (_start, (_end, set)) in self.iter_overlapping(r) {
                out.extend(set.iter_jobs(blocking));
            }
        }
        out.into_iter().collect()
    }

    fn blocks_to_range(
        r: ImpactedBlocks,
    ) -> Option<std::ops::Range<ImpactedAddr>> {
        match r {
            ImpactedBlocks::Empty => None,
            ImpactedBlocks::InclusiveRange(first, last) => Some(
                first..ImpactedAddr {
                    extent_id: last.extent_id,
                    block: last.block + 1,
                },
            ),
        }
    }

    fn insert_range(&mut self, r: ImpactedBlocks, job: u64, blocking: bool) {
        let Some(r) = Self::blocks_to_range(r) else { return; };
        self.insert_splits(r.clone());
        self.job_to_range.insert(job, r.clone());

        // Iterate over the range covered by our new job, either modifying
        // existing ranges or inserting new ranges as needed.
        let mut pos = r.start;
        while pos != r.end {
            let mut next_start = self
                .addr_to_jobs
                .range_mut(pos..)
                .next()
                .map(|(start, _)| *start)
                .unwrap_or(ImpactedAddr {
                    extent_id: u64::MAX,
                    block: u64::MAX,
                });
            if next_start == pos {
                // Modify existing range
                let (next_end, v) =
                    self.addr_to_jobs.get_mut(&next_start).unwrap();
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
                self.addr_to_jobs.insert(
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

        self.merge_adjacent_sections(r);
    }

    fn insert_splits(&mut self, r: std::ops::Range<ImpactedAddr>) {
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
            let prev = self.addr_to_jobs.get_mut(&s).unwrap();
            let v = prev.clone();
            prev.0 = r.start;
            self.addr_to_jobs.insert(r.start, v);
            // Correct for the case where split_start and split_end point into
            // the same existing range (see the diagram above!)
            if split_start == split_end {
                split_end = Some(r.start);
            }
        }
        if let Some(s) = split_end {
            let prev = self.addr_to_jobs.get_mut(&s).unwrap();
            let v = prev.clone();
            prev.0 = r.end;
            self.addr_to_jobs.insert(r.end, v);
        }
    }

    fn merge_adjacent_sections(&mut self, r: std::ops::Range<ImpactedAddr>) {
        let Some(mut pos) = self
            .addr_to_jobs
            .range(..r.start)
            .rev()
            .next()
            .map(|(start, _)| *start)
            .or_else(|| self.addr_to_jobs.first_key_value().map(|(k, _)| *k))
            else { return; };
        while pos < r.end {
            let (end, value) = self.addr_to_jobs.get(&pos).unwrap();
            let end = *end;
            // Check if the next range is adjacent
            if let Some((new_end, next)) = self.addr_to_jobs.get(&end) {
                // Check if the adjacent range is the same :O
                let new_end = *new_end;
                if value == next {
                    self.addr_to_jobs.get_mut(&pos).unwrap().0 = new_end;
                    self.addr_to_jobs.remove(&end).unwrap();
                    // Leave pos at the existing position, so that we can
                    // continue merging later blocks
                } else {
                    pos = end;
                }
            } else {
                pos = end;
            }
            pos = self
                .addr_to_jobs
                .range(pos..)
                .next()
                .map(|(start, _)| *start)
                .unwrap_or(r.end);
        }
    }

    fn iter_overlapping(
        &self,
        r: std::ops::Range<ImpactedAddr>,
    ) -> impl Iterator<Item = (&ImpactedAddr, &(ImpactedAddr, DependencySet))>
    {
        let start = self
            .addr_to_jobs
            .range(..=r.start)
            .rev()
            .next()
            .map(|(start, _)| *start)
            .unwrap_or(r.start);
        self.addr_to_jobs
            .range(start..)
            .skip_while(move |(_start, (end, _set))| *end <= r.start)
            .take_while(move |(start, (_end, _set))| **start < r.end)
    }

    /// Removes the given job from its range
    fn remove_job(&mut self, job: u64) {
        if let Some(r) = self.job_to_range.remove(&job) {
            self.insert_splits(r.clone());

            // Iterate over the range covered by our new job, either modifying
            // existing ranges or inserting new ranges as needed.
            let mut pos = r.start;
            while pos != r.end {
                let mut next_start = self
                    .addr_to_jobs
                    .range_mut(pos..)
                    .next()
                    .map(|(start, _)| *start)
                    .unwrap_or(ImpactedAddr {
                        extent_id: u64::MAX,
                        block: u64::MAX,
                    });
                if next_start == pos {
                    // Remove ourself from the existing range
                    let (next_end, v) =
                        self.addr_to_jobs.get_mut(&next_start).unwrap();
                    v.remove(job);
                    pos = *next_end;
                } else {
                    // This range is already gone; it was probably overwritten
                    // and then the overwritten range was removed.
                    assert!(next_start > pos);
                    next_start = next_start.min(r.end);
                    assert!(next_start > pos);
                    pos = next_start;
                }
            }
            self.merge_adjacent_sections(r);
        }
        self.addr_to_jobs.retain(|_start, (_end, v)| !v.is_empty());
    }

    /// Self-check routine for invariants
    #[cfg(test)]
    fn self_check(&self) {
        // Adjacent ranges with equal values should be merged
        for a @ (_, (end, va)) in self.addr_to_jobs.iter() {
            if let Some(b @ (_, vb)) = self.addr_to_jobs.get(end) {
                assert!(
                    va != vb,
                    "neighboring sections {a:?} {:?} should be merged",
                    (end, b)
                );
            }
        }
        // Ranges must be non-overlapping
        for ((_, (ae, _)), (b, _)) in self
            .addr_to_jobs
            .iter()
            .zip(self.addr_to_jobs.iter().skip(1))
        {
            assert!(b >= ae, "ranges must not overlap");
        }
        for (a, (ae, _)) in self.addr_to_jobs.iter() {
            assert!(ae > a, "ranges must not be empty");
        }
    }
}

/// A set of dependencies, associated with a range in a [`BlockToActive`]
///
/// Each dependency set has 0-1 blocking dependencies (e.g. writes), and any
/// number of non-blocking dependencies.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DependencySet {
    blocking: Option<u64>,
    nonblocking: BTreeSet<u64>,
}

impl DependencySet {
    fn new_blocking(job: u64) -> Self {
        Self {
            blocking: Some(job),
            nonblocking: BTreeSet::default(),
        }
    }
    fn new_nonblocking(job: u64) -> Self {
        Self {
            blocking: None,
            nonblocking: [job].into_iter().collect(),
        }
    }

    fn insert_blocking(&mut self, job: u64) {
        if let Some(prev) = self.blocking {
            // Skip backfilling older jobs
            if job < prev {
                return;
            }
        }
        if let Some(prev) = self.nonblocking.last() {
            if job < *prev {
                return;
            }
        }
        self.blocking = Some(job);
        self.nonblocking.clear();
    }

    fn insert_nonblocking(&mut self, job: u64) {
        if let Some(prev) = self.blocking {
            assert!(job > prev);
        }
        if let Some(prev) = self.nonblocking.last() {
            assert!(job > *prev);
        }
        self.nonblocking.insert(job);
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

    /// Returns jobs that must be treated as dependencies
    ///
    /// If this is a blocking job, then it inherently depends on all jobs;
    /// however, our non-blocking jobs _already_ depend on the existing blocking
    /// job, so we can return them if present and skip the blocking job.
    ///
    /// If this is a non-blocking job, then it only depends on our blocking job
    fn iter_jobs(&self, blocking: bool) -> impl Iterator<Item = u64> + '_ {
        self.blocking
            .iter()
            .filter(move |_| self.nonblocking.is_empty() | !blocking)
            .chain(self.nonblocking.iter().filter(move |_| blocking))
            .cloned()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{Block, RegionDefinition};
    use proptest::prelude::*;

    /// The Oracle is similar to a [`BlockToActive`], but doesn't do range stuff
    ///
    /// Instead, it stores a dependency set at every single impacted block.
    /// This is much less efficient, but provides a ground-truth oracle for
    /// property-based testing.
    struct Oracle {
        addr_to_jobs: BTreeMap<ImpactedAddr, DependencySet>,
        job_to_range: BTreeMap<u64, ImpactedBlocks>,
        ddef: RegionDefinition,
    }

    impl Oracle {
        fn new() -> Self {
            let mut ddef = RegionDefinition::default();
            // TODO these must be kept in sync manually, which seems bad
            ddef.set_extent_size(Block {
                value: BLOCKS_PER_EXTENT,
                shift: crucible_common::MIN_SHIFT, // unused
            });
            ddef.set_block_size(crucible_common::MIN_BLOCK_SIZE as u64);
            Self {
                addr_to_jobs: BTreeMap::new(),
                job_to_range: BTreeMap::new(),
                ddef,
            }
        }
        fn insert_range(
            &mut self,
            r: ImpactedBlocks,
            job: u64,
            blocking: bool,
        ) {
            for (i, b) in r.blocks(&self.ddef) {
                let addr = ImpactedAddr {
                    extent_id: i,
                    block: b.value,
                };
                let v = self.addr_to_jobs.entry(addr).or_default();
                if blocking {
                    v.insert_blocking(job)
                } else {
                    v.insert_nonblocking(job)
                }
            }
            self.job_to_range.insert(job, r);
        }
        fn check_range(&self, r: ImpactedBlocks, blocking: bool) -> Vec<u64> {
            let mut out = BTreeSet::new();
            for (i, b) in r.blocks(&self.ddef) {
                let addr = ImpactedAddr {
                    extent_id: i,
                    block: b.value,
                };
                if let Some(s) = self.addr_to_jobs.get(&addr) {
                    out.extend(s.iter_jobs(blocking));
                }
            }
            out.into_iter().collect()
        }
        fn remove_job(&mut self, job: u64) {
            let r = self.job_to_range.remove(&job).unwrap();
            // TODO: why does `blocks` not return an ImpactedAddr?
            for (i, b) in r.blocks(&self.ddef) {
                let addr = ImpactedAddr {
                    extent_id: i,
                    block: b.value,
                };
                if let Some(s) = self.addr_to_jobs.get_mut(&addr) {
                    s.remove(job);
                }
            }
        }
    }

    const BLOCKS_PER_EXTENT: u64 = 8;

    fn block_strat() -> impl Strategy<Value = ImpactedBlocks> {
        (0u64..100, 0u64..100).prop_map(|(start, len)| {
            if len == 0 {
                ImpactedBlocks::Empty
            } else {
                ImpactedBlocks::InclusiveRange(
                    ImpactedAddr {
                        extent_id: start / BLOCKS_PER_EXTENT,
                        block: start % BLOCKS_PER_EXTENT,
                    },
                    ImpactedAddr {
                        extent_id: (start + len - 1) / BLOCKS_PER_EXTENT,
                        block: (start + len - 1) % BLOCKS_PER_EXTENT,
                    },
                )
            }
        })
    }

    proptest! {
        #[test]
        fn test_active_jobs_insert(
            a in block_strat(), a_type in any::<bool>(),
            b in block_strat(), b_type in any::<bool>(),
            c in block_strat(), c_type in any::<bool>(),
            read in block_strat(), read_type in any::<bool>(),
        ) {
            let mut dut = BlockToActive::default();
            dut.self_check();
            dut.insert_range(a, 1000, a_type);
            dut.self_check();
            dut.insert_range(b, 1001, b_type);
            dut.self_check();
            dut.insert_range(c, 1002, c_type);
            dut.self_check();

            let mut oracle = Oracle::new();
            oracle.insert_range(a, 1000, a_type);
            oracle.insert_range(b, 1001, b_type);
            oracle.insert_range(c, 1002, c_type);

            let read_dut = dut.check_range(read, read_type);
            let read_oracle = oracle.check_range(read, read_type);
            assert_eq!(read_dut, read_oracle);
        }

        #[test]
        fn test_active_jobs_remove(
            a in block_strat(), a_type in any::<bool>(),
            b in block_strat(), b_type in any::<bool>(),
            c in block_strat(), c_type in any::<bool>(),
            remove in 1000u64..1003,
            read in block_strat(), read_type in any::<bool>(),
        ) {
            let mut dut = BlockToActive::default();
            dut.insert_range(a, 1000, a_type);
            dut.insert_range(b, 1001, b_type);
            dut.insert_range(c, 1002, c_type);

            let mut oracle = Oracle::new();
            oracle.insert_range(a, 1000, a_type);
            oracle.insert_range(b, 1001, b_type);
            oracle.insert_range(c, 1002, c_type);

            dut.remove_job(remove);
            oracle.remove_job(remove);

            let read_dut = dut.check_range(read, read_type);
            let read_oracle = oracle.check_range(read, read_type);
            assert_eq!(read_dut, read_oracle);

            dut.self_check();
        }

        #[test]
        fn test_active_jobs_remove_single(
            a in block_strat(), a_type in any::<bool>(),
            read in block_strat(), read_type in any::<bool>(),
        ) {
            let mut dut = BlockToActive::default();
            dut.insert_range(a, 1000, a_type);
            dut.self_check();
            dut.remove_job(1000);

            let mut oracle = Oracle::new();
            oracle.insert_range(a, 1000, a_type);
            oracle.remove_job(1000);

            assert!(dut.addr_to_jobs.is_empty());
            assert!(dut.job_to_range.is_empty());

            let read_dut = dut.check_range(read, read_type);
            let read_oracle = oracle.check_range(read, read_type);
            assert_eq!(read_dut, read_oracle);
        }

        #[test]
        fn test_active_jobs_remove_two(
            a in block_strat(), a_type in any::<bool>(),
            b in block_strat(), b_type in any::<bool>(),
            c in block_strat(), c_type in any::<bool>(),
            order in Just([1000, 1001, 1002]).prop_shuffle(),
            read in block_strat(), read_type in any::<bool>(),
        ) {
            let mut dut = BlockToActive::default();
            dut.insert_range(a, 1000, a_type);
            dut.insert_range(b, 1001, b_type);
            dut.insert_range(c, 1002, c_type);
            dut.remove_job(order[0]);
            dut.remove_job(order[1]);
            dut.self_check();

            let mut oracle = Oracle::new();
            oracle.insert_range(a, 1000, a_type);
            oracle.insert_range(b, 1001, b_type);
            oracle.insert_range(c, 1002, c_type);
            oracle.remove_job(order[0]);
            oracle.remove_job(order[1]);

            let read_dut = dut.check_range(read, read_type);
            let read_oracle = oracle.check_range(read, read_type);
            assert_eq!(read_dut, read_oracle);

            dut.remove_job(order[2]);
            assert!(dut.addr_to_jobs.is_empty());
            assert!(dut.job_to_range.is_empty());
        }

        #[test]
        fn test_active_jobs_remove_all(
            a in block_strat(), a_type in any::<bool>(),
            b in block_strat(), b_type in any::<bool>(),
            c in block_strat(), c_type in any::<bool>(),
            order in Just([1000, 1001, 1002]).prop_shuffle()
        ) {
            let mut dut = BlockToActive::default();
            dut.insert_range(a, 1000, a_type);
            dut.insert_range(b, 1001, b_type);
            dut.insert_range(c, 1002, c_type);

            dut.remove_job(order[0]);
            dut.self_check();
            dut.remove_job(order[1]);
            dut.self_check();
            dut.remove_job(order[2]);
            dut.self_check();

            assert!(dut.addr_to_jobs.is_empty());
            assert!(dut.job_to_range.is_empty());
        }
    }
}
