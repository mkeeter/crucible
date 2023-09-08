// Copyright 2023 Oxide Computer Company

use crucible_common::RegionDefinition;

use crate::{AckStatus, DownstairsIO, ImpactedAddr, ImpactedBlocks};
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

    /// The region definition allows us to go from extent + block to LBA
    ///
    /// This must always be populated by the time we call any functions;
    /// unfortunately, making it populated by construction requires a more
    /// significant refactoring of the upstairs loop to use the typestate
    /// pattern.
    ddef: Option<RegionDefinition>,
}

impl ActiveJobs {
    pub fn new() -> Self {
        Self {
            jobs: BTreeMap::new(),
            ackable: BTreeSet::new(),
            block_to_active: BlockToActive::new(),
            ddef: None,
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

    /// Sets the region definition
    ///
    /// # Panics
    /// If the region was already set to a different value
    pub fn set_ddef(&mut self, ddef: RegionDefinition) {
        if let Some(prev) = &self.ddef {
            assert_eq!(&ddef, prev);
        } else {
            self.ddef = Some(ddef);
        }
    }

    /// Inserts a new job ID and its associated IO work
    #[inline]
    pub fn insert(
        &mut self,
        job_id: u64,
        io: DownstairsIO,
    ) -> Option<DownstairsIO> {
        self.jobs.insert(job_id, io)
    }

    /// Removes a job by ID, returning its IO work
    #[inline]
    pub fn remove(&mut self, job_id: &u64) -> Option<DownstairsIO> {
        self.jobs.remove(job_id)
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

    #[inline]
    pub fn iter(&self) -> std::collections::btree_map::Iter<u64, DownstairsIO> {
        self.jobs.iter()
    }

    pub fn deps_for_flush(&self) -> Vec<u64> {
        /*
         * To build the dependency list for this flush, iterate from the end of
         * the downstairs work active list in reverse order and check each job
         * in that list to see if this new flush must depend on it.
         *
         * We can safely ignore everything before the last flush, because the
         * last flush will depend on jobs before it. But this flush must depend
         * on the last flush - flush and gen numbers downstairs need to be
         * sequential and the same for each downstairs.
         *
         * The downstairs currently assumes that all jobs previous to the last
         * flush have completed, so the Upstairs must set that flushes depend on
         * all jobs. It's currently important that flushes depend on everything,
         * and everything depends on flushes.
         */
        let num_jobs = self.len();
        let mut dep: Vec<u64> = Vec::with_capacity(num_jobs);

        for (id, job) in self.iter().rev() {
            // Flushes must depend on everything
            dep.push(*id);

            // Depend on the last flush, but then bail out
            if job.work.is_flush() {
                break;
            }
        }
        dep
    }

    pub fn deps_for_write(&self, impacted_blocks: ImpactedBlocks) -> Vec<u64> {
        /*
         * To build the dependency list for this write, iterate from the end
         * of the downstairs work active list in reverse order and
         * check each job in that list to see if this new write must
         * depend on it.
         *
         * Construct a list of dependencies for this write based on the
         * following rules:
         *
         * - writes have to depend on the last flush completing (because
         *   currently everything has to depend on flushes)
         * - any overlap of impacted blocks before the flush requires a
         *   dependency
         *
         * It's important to remember that jobs may arrive at different
         * Downstairs in different orders but they should still complete in job
         * dependency order.
         *
         * TODO: any overlap of impacted blocks will create a dependency.
         * take this an example (this shows three writes, all to the
         * same block, along with the dependency list for each
         * write):
         *
         *       block
         * op# | 0 1 2 | deps
         * ----|-------------
         *   0 | W     |
         *   1 | W     | 0
         *   2 | W     | 0,1
         *
         * op 2 depends on both op 1 and op 0. If dependencies are transitive
         * with an existing job, it would be nice if those were removed from
         * this job's dependencies.
         */
        let num_jobs = self.len();
        let mut dep: Vec<u64> = Vec::with_capacity(num_jobs);

        // Search backwards in the list of active jobs
        for (id, job) in self.iter().rev() {
            // Depend on the last flush - flushes are a barrier for
            // all writes.
            if job.work.is_flush() {
                dep.push(*id);
                break;
            }

            // If this job impacts the same blocks as something already active,
            // create a dependency.
            if impacted_blocks.conflicts(&job.impacted_blocks) {
                dep.push(*id);
            }
        }
        dep
    }

    pub fn deps_for_read(&self, impacted_blocks: ImpactedBlocks) -> Vec<u64> {
        /*
         * To build the dependency list for this read, iterate from the end
         * of the downstairs work active list in reverse order and
         * check each job in that list to see if this new read must
         * depend on it.
         *
         * Construct a list of dependencies for this read based on the
         * following rules:
         *
         * - reads depend on flushes (because currently everything has to depend
         *   on flushes)
         * - any write with an overlap of impacted blocks requires a
         *   dependency
         */
        let num_jobs = self.len();
        let mut dep: Vec<u64> = Vec::with_capacity(num_jobs);

        // Search backwards in the list of active jobs
        for (id, job) in self.iter().rev() {
            if job.work.is_flush() {
                dep.push(*id);
                break;
            } else if (job.work.is_write() | job.work.is_repair())
                && impacted_blocks.conflicts(&job.impacted_blocks)
            {
                // If this is a write or repair and it impacts the same blocks
                // as something already active, create a dependency.
                dep.push(*id);
            }
        }
        dep
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
        _ddef: RegionDefinition,
    ) -> Vec<u64> {
        let num_jobs = self.len();
        let mut deps: Vec<u64> = Vec::with_capacity(num_jobs);

        // Search backwards in the list of active jobs, stop at the
        // last flush
        for (id, job) in self.iter().rev() {
            // We are finding dependencies based on impacted blocks.
            // We may have reserved future job IDs for repair, and it's possible
            // that this job we are finding dependencies for now is actually a
            // future repair job we reserved.  If so, don't include ourself in
            // our own list of dependencies.
            if job.ds_id > close_id {
                continue;
            }
            // If this operation impacts the same blocks as something
            // already active, create a dependency.
            if impacted_blocks.conflicts(&job.impacted_blocks) {
                deps.push(*id);
            }

            // A flush job won't show impacted blocks. We can stop looking
            // for dependencies beyond the last flush.
            if job.work.is_flush() {
                deps.push(*id);
                break;
            }
        }

        deps
    }

    pub fn ackable_work(&self) -> Vec<u64> {
        self.ackable.iter().cloned().collect()
    }

    /// Converts from an extent + relative block to a global block
    ///
    /// # Panics
    /// If the global block address overflows a `u64`, or `self.ddef` has not
    /// yet been assigned.
    fn to_lba(&self, block: ImpactedAddr) -> u64 {
        block
            .extent_id
            .checked_mul(self.ddef.unwrap().extent_size().value)
            .unwrap()
            .checked_add(block.block)
            .unwrap()
    }

    /// Converts from an (inclusive) block range to an (exclusive) LBA range
    ///
    /// # Panics
    /// If any address overflows a `u64`, or `self.ddef` has not yet been
    /// assigned.
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
}

impl BlockToActive {
    fn new() -> Self {
        Self {
            lba_to_jobs: BTreeMap::new(),
        }
    }

    /// Returns all dependencies that are active across the given range
    fn check_range(&self, r: std::ops::Range<u64>) -> Vec<u64> {
        let mut out = BTreeSet::new();
        for (_start, (_end, set)) in self.iter_overlapping(r) {
            out.extend(set.blocking.iter().cloned());
            out.extend(set.nonblocking.iter().cloned());
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
                split_end = Some(*end);
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

        let mut pos = r.start;
        while pos != r.end {
            let mut next_start = self
                .lba_to_jobs
                .range_mut(pos..)
                .next()
                .map(|(start, _)| *start)
                .unwrap_or(u64::MAX);
            if next_start == pos {
                pos = next_start;
                // Modify existing range
            } else {
                assert!(next_start > pos);
                next_start = next_start.min(r.end);
                assert!(next_start > pos);
                self.lba_to_jobs.insert(pos, (next_start, todo!()));
                pos = next_start;
            }
        }
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
            .unwrap_or(u64::MAX);
        self.lba_to_jobs
            .range(start..)
            .skip_while(move |(_start, (end, _set))| *end <= r.start)
            .take_while(move |(start, (_end, _set))| **start < r.end)
    }
}

/// A set of dependencies, associated with a range in a [`BlockToActive`]
///
/// Each dependency set has 0-1 blocking dependencies (e.g. writes), and any
/// number of non-blocking dependencies.
#[derive(Clone, Debug)]
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
        self.blocking = Some(job);
        self.nonblocking.clear();
    }

    fn insert_nonblocking(&mut self, job: u64) {
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
}
