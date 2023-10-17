// Copyright 2023 Oxide Computer Company
use crate::{
    cdt, crucible_bail,
    extent::{
        check_input, DownstairsBlockContext, ExtentInner, EXTENT_META_RAW,
    },
    integrity_hash, mkdir_for_file,
    region::{BatchedPwritev, JobOrReconciliationId},
    Block, BlockContext, CrucibleError, JobId, ReadResponse, RegionDefinition,
};

use anyhow::{anyhow, bail, Result};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use slog::{error, Logger};

use std::cell::Cell;
use std::collections::{BTreeSet, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{IoSliceMut, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::path::Path;

/// Equivalent to `DownstairsBlockContext`, but without one's own block number
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnDiskDownstairsBlockContext {
    pub block_context: BlockContext,
    pub on_disk_hash: u64,
}

/// Equivalent to `ExtentMeta`, but ordered for efficient on-disk serialization
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct OnDiskMeta {
    pub dirty: bool,
    pub gen_number: u64,
    pub flush_number: u64,
    pub ext_version: u32,

    /// Generation for metadata itself
    ///
    /// Metadata is encoded at the beginning of every superblock, so that we can
    /// write it to a portion of the file that has already changed (instead of
    /// having to dirty an unrelated portion of the file).  When loading the
    /// file, we pick the `OnDiskMeta` with the highest `meta_gen`
    pub meta_gen: u64,
}

/// Size of block context slot
///
/// This must be large enough to contain an `Option<DownstairsBlockContext>`
/// serialized using `bincode`.
///
/// Each block has two context slots associated with it, so we can use a
/// ping-pong strategy to write to one while the other remains valid (in case of
/// a crash)
pub const BLOCK_CONTEXT_SLOT_SIZE_BYTES: u64 = 48;

/// Size of metadata region
///
/// This must be large enough to contain an `OnDiskMeta` serialized using
/// `bincode`.
pub const BLOCK_META_SIZE_BYTES: u64 = 32;

/// `RawInner` is a wrapper around a [`std::fs::File`] representing an extent
///
/// The file is structured into a set of "superblocks", which each contain
///
/// - An initial metadata block, built from
///     - [`BLOCK_META_SIZE_BYTES`], which contains an [`OnDiskMeta`] serialized
///       using `bincode`.  The first byte of this range is `dirty`, serialized
///       as a `u8` (where `1` is dirty and `0` is clean).
///     - 2 arrays, each containing _N_ bincode-serialized
///       `Option<OnDiskDownstairsBlockContext>`.  These are our ping-pong block
///       contexts data
/// - _N_ data blocks
///
/// The number of blocks per superblock is calculated by [`RawExtentLayout`];
/// the final superblock may have fewer blocks, but is guaranteed to have > 0.
#[derive(Debug)]
pub struct RawInner {
    file: File,

    /// Our extent number
    extent_number: u32,

    /// Extent size, in blocks
    extent_size: Block,

    /// Is the `ping` or `pong` context slot active?
    ///
    /// This is `None` if the state is unknown, e.g. upon initial startup or
    /// when a write to a context slot may have failed.  When known, it stores a
    /// `bool` indicating whether it points to the `A` or `B` slot.  Note that
    /// the `bool` is present even if this block is unwritten; in that case, it
    /// will point to a slot that deserializes to `None`.
    ///
    /// This `Vec` contains one item per block in the extent file.
    active_context: Vec<ContextSlot>,

    /// Active slot (`A` or `B`) for each superblock
    superblock_slot: Vec<ContextSlot>,

    /// Current metadata, cached here to avoid reading / writing unnecessarily
    meta: Cell<OnDiskMeta>,

    /// Index of the last modified superblock
    ///
    /// Storing this means that when we want to send a flush, we can edit the
    /// [`OnDiskMeta`] in a superblock that already has edits, which improves
    /// write locality.
    last_superblock_modified: Option<u64>,

    /// Superblock configuration and layout
    layout: RawExtentLayout,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ContextSlot {
    A,
    B,
}

impl std::ops::Not for ContextSlot {
    type Output = Self;
    fn not(self) -> Self {
        match self {
            ContextSlot::A => ContextSlot::B,
            ContextSlot::B => ContextSlot::A,
        }
    }
}

impl ExtentInner for RawInner {
    fn flush_number(&self) -> Result<u64, CrucibleError> {
        Ok(self.meta.get().flush_number)
    }

    fn gen_number(&self) -> Result<u64, CrucibleError> {
        Ok(self.meta.get().gen_number)
    }

    fn dirty(&self) -> Result<bool, CrucibleError> {
        Ok(self.meta.get().dirty)
    }

    fn write(
        &mut self,
        job_id: JobId,
        writes: &[&crucible_protocol::Write],
        only_write_unwritten: bool,
        iov_max: usize,
    ) -> Result<(), CrucibleError> {
        /*
         * In order to be crash consistent, perform the following steps in
         * order:
         *
         * 1) set the dirty bit
         * 2) for each write:
         *   a) write out encryption context and hashes first (to the inactive
         *     context slot)
         *   b) write out extent data second
         *
         * If encryption context is written after the extent data, a crash or
         * interruption before extent data is written would potentially leave
         * data on the disk that cannot be decrypted.
         *
         * If hash is written after extent data, same thing - a crash or
         * interruption would leave data on disk that would fail the
         * integrity hash check.
         *
         * Note that writing extent data here does not assume that it is
         * durably on disk - the only guarantee of that is returning
         * ok from fsync. The data is only potentially on disk and
         * this depends on operating system implementation.
         *
         * To minimize the performance hit of sending many transactions to the
         * filesystem, as much as possible is written at the same time. This
         * means multiple loops are required. The steps now look like:
         *
         * 1) set the dirty bit
         * 2) gather and write all encryption contexts + hashes
         * 3) write all extent data
         *
         * If "only_write_unwritten" is true, then we only issue a write for
         * a block if that block has not been written to yet.  Note
         * that we can have a write that is "sparse" if the range of
         * blocks it contains has a mix of written an unwritten
         * blocks.
         *
         * We define a block being written to or not has if that block has
         * `Some(...)` with a matching checksum serialized into a context slot
         * or not. So it is required that a written block has a checksum.
         */

        // If `only_write_written`, we need to skip writing to blocks that
        // already contain data. We'll first query the metadata to see which
        // blocks have hashes
        let mut writes_to_skip = HashSet::new();
        if only_write_unwritten {
            cdt::extent__write__get__hashes__start!(|| {
                (job_id.0, self.extent_number, writes.len() as u64)
            });

            // TODO Get block contexts in bulk here

            for w in writes {
                let ctx = self.get_block_context(w.offset.value)?;
                if ctx.is_some() {
                    writes_to_skip.insert(w.offset.value);
                }
            }
            cdt::extent__write__get__hashes__done!(|| {
                (job_id.0, self.extent_number, writes.len() as u64)
            });

            if writes_to_skip.len() == writes.len() {
                // Nothing to do
                return Ok(());
            }
        }

        // Set up `last_superblock_modified` so that when we modify metadata (by
        // calling `set_dirty` below), we edit the same superblock.
        if let Some(w) = writes.get(0) {
            self.last_superblock_modified =
                Some(w.offset.value / self.layout.blocks_per_superblock);
        }

        self.set_dirty()?;

        // Write all the metadata to the raw file
        //
        // TODO right now we're including the integrity_hash() time in the
        // measured time.  Is it small enough to be ignored?
        cdt::extent__write__raw__context__insert__start!(|| {
            (job_id.0, self.extent_number, writes.len() as u64)
        });

        let mut block_ctx = vec![];
        for write in writes {
            if writes_to_skip.contains(&write.offset.value) {
                continue;
            }

            // TODO it would be nice if we could profile what % of time we're
            // spending on hashes locally vs writing to disk
            let on_disk_hash = integrity_hash(&[&write.data[..]]);

            block_ctx.push(DownstairsBlockContext {
                block_context: write.block_context,
                block: write.offset.value,
                on_disk_hash,
            });
        }
        self.set_block_contexts(&block_ctx)?;

        cdt::extent__write__raw__context__insert__done!(|| {
            (job_id.0, self.extent_number, writes.len() as u64)
        });

        cdt::extent__write__file__start!(|| {
            (job_id.0, self.extent_number, writes.len() as u64)
        });

        // Until the write lands, we can't trust the context slot!  As such, we
        // can't return with `?` here; we have to check whether the write failed
        // and invalidate the slot in that case.
        let r = self.write_inner(writes, &writes_to_skip, iov_max);
        cdt::extent__write__file__done!(|| {
            (job_id.0, self.extent_number, writes.len() as u64)
        });

        // Now that writes have gone through, update our active context slots
        //
        // By definition, we have written to the inactive superblock slot
        if r.is_err() {
            for write in writes.iter() {
                let block = write.offset.value;
                if !writes_to_skip.contains(&block) {
                    // We can't recover if the context slots in the file have
                    // diverged from the file data, so unceremoniously bail out.
                    self.recompute_slot_from_file(block).unwrap();
                }
            }
        } else {
            for write in writes.iter() {
                let block = write.offset.value;
                if !writes_to_skip.contains(&block) {
                    let superblock = block / self.layout.blocks_per_superblock;
                    let slot = !self.superblock_slot[superblock as usize];
                    self.active_context[write.offset.value as usize] = slot;
                }
            }
        }
        r
    }

    fn read(
        &mut self,
        job_id: JobId,
        requests: &[&crucible_protocol::ReadRequest],
        responses: &mut Vec<crucible_protocol::ReadResponse>,
        iov_max: usize,
    ) -> Result<(), CrucibleError> {
        // This code batches up operations for contiguous regions of
        // ReadRequests, so we can perform larger read syscalls queries. This
        // significantly improves read throughput.

        // Keep track of the index of the first request in any contiguous run
        // of requests. Of course, a "contiguous run" might just be a single
        // request.
        let mut req_run_start = 0;
        let block_size = self.extent_size.block_size_in_bytes();
        while req_run_start < requests.len() {
            let first_req = requests[req_run_start];

            // Starting from the first request in the potential run, we scan
            // forward until we find a request with a block that isn't
            // contiguous with the request before it. Since we're counting
            // pairs, and the number of pairs is one less than the number of
            // requests, we need to add 1 to get our actual run length.
            let mut n_contiguous_requests = 1;

            for request_window in requests[req_run_start..].windows(2) {
                let block0 =
                    self.layout.block_pos(request_window[0].offset.value);
                let block1 =
                    self.layout.block_pos(request_window[1].offset.value);

                if (block0 + 1 == block1)
                    && ((n_contiguous_requests + 1) < iov_max)
                {
                    n_contiguous_requests += 1;
                } else {
                    break;
                }
            }

            // Create our responses and push them into the output. While we're
            // at it, check for overflows.
            let resp_run_start = responses.len();
            let mut iovecs = Vec::with_capacity(n_contiguous_requests);
            for req in requests[req_run_start..][..n_contiguous_requests].iter()
            {
                let resp = ReadResponse::from_request(req, block_size as usize);
                check_input(self.extent_size, req.offset, &resp.data)?;
                responses.push(resp);
            }

            // Create what amounts to an iovec for each response data buffer.
            for resp in
                &mut responses[resp_run_start..][..n_contiguous_requests]
            {
                iovecs.push(IoSliceMut::new(&mut resp.data[..]));
            }

            // Finally we get to read the actual data. That's why we're here
            cdt::extent__read__file__start!(|| {
                (job_id.0, self.extent_number, n_contiguous_requests as u64)
            });

            nix::sys::uio::preadv(
                self.file.as_raw_fd(),
                &mut iovecs,
                self.layout.block_pos(first_req.offset.value) as i64
                    * block_size as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;

            cdt::extent__read__file__done!(|| {
                (job_id.0, self.extent_number, n_contiguous_requests as u64)
            });

            // Query the block metadata
            cdt::extent__read__get__contexts__start!(|| {
                (job_id.0, self.extent_number, n_contiguous_requests as u64)
            });
            let block_contexts = self.get_block_contexts(
                first_req.offset.value,
                n_contiguous_requests as u64,
            )?;
            cdt::extent__read__get__contexts__done!(|| {
                (job_id.0, self.extent_number, n_contiguous_requests as u64)
            });

            // Now it's time to put block contexts into the responses.
            // We use into_iter here to move values out of enc_ctxts/hashes,
            // avoiding a clone(). For code consistency, we use iters for the
            // response and data chunks too. These iters will be the same length
            // (equal to n_contiguous_requests) so zipping is fine
            let resp_iter =
                responses[resp_run_start..][..n_contiguous_requests].iter_mut();
            let ctx_iter = block_contexts.into_iter();

            for (resp, r_ctx) in resp_iter.zip(ctx_iter) {
                resp.block_contexts =
                    r_ctx.into_iter().map(|x| x.block_context).collect();
            }

            req_run_start += n_contiguous_requests;
        }

        Ok(())
    }

    fn flush(
        &mut self,
        new_flush: u64,
        new_gen: u64,
        job_id: JobOrReconciliationId,
    ) -> Result<(), CrucibleError> {
        if !self.dirty()? {
            /*
             * If we have made no writes to this extent since the last flush,
             * we do not need to update the extent on disk
             */
            return Ok(());
        }

        // Make sure that block and superblock data is consistent
        let mut new_superblock_slots = vec![];
        for i in 0..self.layout.superblock_count {
            new_superblock_slots.push(self.ensure_superblock_is_consistent(i)?);
        }

        // We put all of our metadata updates into a single write to make this
        // operation atomic.
        self.set_flush_number(new_flush, new_gen)?;

        // Now, we fsync to ensure data is flushed to disk.  It's okay to crash
        // before this point, because setting the flush number is atomic.
        cdt::extent__flush__file__start!(|| {
            (job_id.get(), self.extent_number, 0)
        });
        if let Err(e) = self.file.sync_all() {
            /*
             * XXX Retry?  Mark extent as broken?
             */
            crucible_bail!(
                IoError,
                "extent {}: fsync 1 failure: {:?}",
                self.extent_number,
                e
            );
        }
        cdt::extent__flush__file__done!(|| {
            (job_id.get(), self.extent_number, 0)
        });

        for (i, n) in new_superblock_slots.iter().enumerate() {
            // TODO: we could only update superblock slots which changed
            self.set_superblock_slot(i as u64, *n);
        }

        cdt::extent__flush__done!(|| { (job_id.get(), self.extent_number, 0) });

        Ok(())
    }

    #[cfg(test)]
    fn set_dirty_and_block_context(
        &mut self,
        block_context: &DownstairsBlockContext,
    ) -> Result<(), CrucibleError> {
        self.set_dirty()?;
        let new_slot = self.set_block_context(block_context)?;
        self.active_context[block_context.block as usize] = new_slot;
        Ok(())
    }

    #[cfg(test)]
    fn get_block_contexts(
        &mut self,
        block: u64,
        count: u64,
    ) -> Result<Vec<Vec<DownstairsBlockContext>>, CrucibleError> {
        let out = RawInner::get_block_contexts(self, block, count)?;
        Ok(out.into_iter().map(|v| v.into_iter().collect()).collect())
    }
}

#[derive(Copy, Clone, Debug)]
pub struct RawExtentLayout {
    /// Number of data blocks in each superblock
    ///
    /// Each superblock also includes a leading metadata block, so the
    /// superblock is of size `(blocks_per_superblock + 1) * block_size_bytes`
    pub blocks_per_superblock: u64,

    /// Extent file size, in bytes
    pub extent_file_size: u64,

    /// Number of superblocks in the file
    ///
    /// The final superblock may have fewer than `blocks_per_superblock` blocks
    /// following it (but should have > 0).
    pub superblock_count: u64,

    /// Total number of data blocks
    pub block_count: u64,

    /// Size of a block, in bytes
    pub block_size_bytes: u64,
}

impl RawExtentLayout {
    pub fn new(def: &RegionDefinition) -> Self {
        const RECORD_SIZE: u64 = 128 * 1024;
        let block_count = def.extent_size().value;
        let block_size = def.block_size();
        let max_blocks_per_superblock = (block_size - BLOCK_META_SIZE_BYTES)
            / (BLOCK_CONTEXT_SLOT_SIZE_BYTES * 2);
        assert!(max_blocks_per_superblock > 0);

        let max_superblock_size_bytes =
            (max_blocks_per_superblock + 1) * block_size;
        let records_per_superblock = max_superblock_size_bytes / RECORD_SIZE;

        let blocks_per_superblock = if records_per_superblock == 0 {
            max_blocks_per_superblock
        } else {
            ((records_per_superblock * RECORD_SIZE) / block_size) - 1
        };

        let whole_superblocks = block_count / blocks_per_superblock;
        let trailing_blocks = block_count % blocks_per_superblock;

        let superblock_count = whole_superblocks + (trailing_blocks > 0) as u64;
        let extent_file_size = (superblock_count + block_count) * block_size;

        Self {
            blocks_per_superblock,
            extent_file_size,
            superblock_count,
            block_size_bytes: block_size,
            block_count,
        }
    }

    /// Calculates the offset block position, taking metadata into account
    ///
    /// The position is as a block number (not a byte offset), and is guaranteed
    /// to point to a data block within a superblock (i.e. *not* point to the
    /// metadata block).
    fn block_pos(&self, block: u64) -> u64 {
        let pos = block / self.blocks_per_superblock;
        let offset = block % self.blocks_per_superblock;

        pos * (self.blocks_per_superblock + 1) + offset + 1
    }

    /// Returns the byte offset of the given context slot
    ///
    /// Contexts slots are located after block data in the extent file.  They
    /// are packed as two arrays, each with `blocks_per_superblock` slots.
    /// We use a ping-pong strategy between arrays to ensure that one of the two
    /// context slots is always valid (i.e. matching the data in the file).
    fn context_slot_offset(&self, block: u64, slot: ContextSlot) -> u64 {
        let pos = block / self.blocks_per_superblock;
        let offset = block % self.blocks_per_superblock;

        pos * (self.blocks_per_superblock + 1) * self.block_size_bytes
            + BLOCK_META_SIZE_BYTES
            + (slot as u64 * self.blocks_per_superblock + offset)
                * BLOCK_CONTEXT_SLOT_SIZE_BYTES
    }
}

impl RawInner {
    pub fn create(
        path: &Path,
        def: &RegionDefinition,
        extent_number: u32,
    ) -> Result<Self> {
        Self::create_ext(path, def, extent_number, None)
    }

    pub fn create_ext(
        path: &Path,
        def: &RegionDefinition,
        extent_number: u32,
        suffix: Option<&str>,
    ) -> Result<Self> {
        let path = match suffix {
            Some(s) => path.with_extension(s),
            None => path.to_path_buf(),
        };

        let layout = RawExtentLayout::new(def);

        mkdir_for_file(&path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // All 0s are fine for everything except extent version in the metadata
        file.set_len(layout.extent_file_size)?;

        // Write metadata to the first superblock.  This metadata has
        // `meta_gen = 1`, so it will always be picked up.
        let meta = OnDiskMeta {
            dirty: false,
            gen_number: 0,
            flush_number: 0,
            ext_version: EXTENT_META_RAW,
            meta_gen: 1,
        };
        {
            let mut buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
            bincode::serialize_into(buf.as_mut_slice(), &meta)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&buf)?;
        }

        let out = Self {
            file,
            extent_size: def.extent_size(),
            extent_number,
            active_context: vec![
                ContextSlot::A;
                def.extent_size().value as usize
            ],
            superblock_slot: vec![
                ContextSlot::A;
                layout.superblock_count as usize
            ],
            meta: meta.into(),
            last_superblock_modified: None,
            layout,
        };

        // Sync the file to disk, to avoid any questions
        if let Err(e) = out.file.sync_all() {
            return Err(CrucibleError::IoError(format!(
                "extent {}: fsync 1 failure during initial sync: {e:?}",
                out.extent_number,
            ))
            .into());
        }
        Ok(out)
    }

    /// Constructs a new `Inner` object from files that already exist on disk
    pub fn open(
        path: &Path,
        def: &RegionDefinition,
        extent_number: u32,
        read_only: bool,
        log: &Logger,
    ) -> Result<Self> {
        let layout = RawExtentLayout::new(def);

        // Open the extent file and verify the size is as we expect.
        let file =
            match OpenOptions::new().read(true).write(!read_only).open(path) {
                Err(e) => {
                    error!(
                        log,
                        "Open of {path:?} for extent#{extent_number} \
                         returned: {e}",
                    );
                    bail!(
                        "Open of {path:?} for extent#{extent_number} \
                         returned: {e}",
                    );
                }
                Ok(f) => {
                    let cur_size = f.metadata().unwrap().len();
                    let expected_size = layout.extent_file_size;
                    if expected_size != cur_size {
                        bail!(
                            "File size {cur_size:?} does not match \
                             expected {expected_size:?}",
                        );
                    }
                    f
                }
            };

        // Just in case, let's be very sure that the file on disk is what it
        // should be
        if let Err(e) = file.sync_all() {
            return Err(CrucibleError::IoError(format!(
                "extent {extent_number}:
                 fsync 1 failure during initial rehash: {e:?}",
            ))
            .into());
        }

        let mut out = Self {
            file,
            // This value is not necessarily correct; we'll recompute it below!
            active_context: vec![
                ContextSlot::A;
                def.extent_size().value as usize
            ],
            // Bogus value for meta, initialized below
            meta: OnDiskMeta {
                dirty: false,
                gen_number: u64::MAX,
                flush_number: u64::MAX,
                ext_version: u32::MAX,
                meta_gen: u64::MAX,
            }
            .into(),
            last_superblock_modified: None,
            extent_number,
            extent_size: def.extent_size(),

            // Initializing this with `ContextSlot::A` may not be entirely
            // accurate, but it's easy!  The downside is that we could
            // potentially do a little more work on initial writes and flushes,
            // until this accurately reflects the real value.
            superblock_slot: vec![
                ContextSlot::A;
                layout.superblock_count as usize
            ],
            layout,
        };

        for b in 0..out.layout.block_count {
            out.recompute_slot_from_file(b)?;
        }
        out.recompute_meta()?;
        Ok(out)
    }

    fn set_dirty(&mut self) -> Result<(), CrucibleError> {
        let mut meta = self.meta.get();
        if !meta.dirty {
            meta.dirty = true;
            self.set_metadata(meta)?;
        }
        Ok(())
    }

    fn recompute_slot_from_file(
        &mut self,
        block: u64,
    ) -> Result<(), CrucibleError> {
        // Read the block data itself:
        let block_size = self.extent_size.block_size_in_bytes();
        let mut buf = vec![0; block_size as usize];
        self.file.seek(SeekFrom::Start(
            block_size as u64 * self.layout.block_pos(block),
        ))?;
        self.file.read_exact(&mut buf)?;
        let hash = integrity_hash(&[&buf]);

        // Then, read the slot data and decide if either slot
        // (1) is present and
        // (2) has a matching hash
        let mut buf = [0; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
        let mut matching_slot = None;
        let mut empty_slot = None;
        for slot in [ContextSlot::A, ContextSlot::B] {
            // TODO: is `pread` faster than seek + read_exact?
            self.file.seek(SeekFrom::Start(
                self.layout.context_slot_offset(block, slot),
            ))?;
            self.file.read_exact(&mut buf)?;
            let context: Option<OnDiskDownstairsBlockContext> =
                bincode::deserialize(&buf).map_err(|e| {
                    CrucibleError::IoError(format!(
                        "context deserialization failed: {e:?}"
                    ))
                })?;
            if let Some(context) = context {
                if context.on_disk_hash == hash {
                    matching_slot = Some(slot);
                }
            } else if empty_slot.is_none() {
                empty_slot = Some(slot);
            }
        }
        let value = matching_slot
            .or(empty_slot)
            .ok_or(anyhow!("no slot found for {block}"))?;
        self.active_context[block as usize] = value;
        Ok(())
    }

    fn set_block_contexts(
        &mut self,
        block_contexts: &[DownstairsBlockContext],
    ) -> Result<()> {
        let mut start = 0;
        for i in 0..block_contexts.len() {
            if i + 1 == block_contexts.len()
                || block_contexts[i].block + 1 != block_contexts[i + 1].block
            {
                self.set_block_contexts_contiguous(&block_contexts[start..=i])?;
                start = i + 1;
            }
        }
        Ok(())
    }

    /// Efficiently sets block contexts in bulk
    ///
    /// # Panics
    /// `block_contexts` must represent a contiguous set of blocks
    fn set_block_contexts_contiguous(
        &mut self,
        block_contexts: &[DownstairsBlockContext],
    ) -> Result<()> {
        for (a, b) in block_contexts.iter().zip(block_contexts.iter().skip(1)) {
            assert_eq!(a.block + 1, b.block, "blocks must be contiguous");
        }
        // Check whether _any_ of these writes would require a flush
        //
        // If so
        //      Load all the superblock slots
        //      Make relevant superblocks consistent
        //      Sync
        //      Update superblock slots

        // Iterate over our contexts, seeing whether any of them require a sync
        // (because the context will be written to the superblock slot, which is
        // not synched at this point).
        let mut superblocks_need_sync = BTreeSet::new();
        for b in block_contexts {
            // Get our currently-active slot; we'll be writing to the other one
            // for crash consistency (i.e. if we crash between writing the
            // context and writing out the block data)
            let active_slot = self.active_context[b.block as usize];
            let target_slot = !active_slot;

            let superblock = b.block / self.layout.blocks_per_superblock;
            if target_slot == self.superblock_slot[superblock as usize] {
                superblocks_need_sync.insert(superblock);
            }
        }

        // If some superblocks need to be synched, then we'll do that here!
        //
        // This should be uncommon: it will only happen if someone writes to the
        // same block 2x in a row without a flush (manual or automatic) in
        // between.
        if !superblocks_need_sync.is_empty() {
            let new_superblock_slots = superblocks_need_sync
                .iter()
                .map(|s| self.ensure_superblock_is_consistent(*s))
                .collect::<Result<Vec<_>>>()?;

            self.file.sync_all()?;

            for (superblock, new_slot) in
                superblocks_need_sync.into_iter().zip(new_superblock_slots)
            {
                self.set_superblock_slot(superblock, new_slot);
            }
        }

        // Okay, we're now done with setup!  We are ready to do bulk writes of
        // block contexts on a per-superblock basis (or finer).  For each
        // superblock, context data will be written into the slot
        // `!self.superblock_slots[superblock_index]`.
        for (superblock_index, group) in block_contexts
            .iter()
            .group_by(|b| b.block / self.layout.blocks_per_superblock)
            .into_iter()
        {
            let mut buf = vec![];
            let mut group = group.peekable();
            let start_block = group.peek().unwrap().block;
            for ctx in group {
                buf.extend(
                    [0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize].into_iter(),
                );
                let d = OnDiskDownstairsBlockContext {
                    block_context: ctx.block_context,
                    on_disk_hash: ctx.on_disk_hash,
                };
                let n = buf.len();
                bincode::serialize_into(
                    &mut buf[n - BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize..],
                    &Some(d),
                )?;
            }

            let offset = self.layout.context_slot_offset(
                start_block,
                !self.superblock_slot[superblock_index as usize],
            );
            nix::sys::uio::pwrite(self.file.as_raw_fd(), &buf, offset as i64)?;
        }
        Ok(())
    }

    /// Writes the inactive block context slot
    ///
    /// Returns the new slot which should be marked as active after the write
    #[cfg(test)]
    fn set_block_context(
        &mut self,
        block_context: &DownstairsBlockContext,
    ) -> Result<ContextSlot> {
        self.set_block_contexts(&[*block_context])?;
        let superblock =
            block_context.block / self.layout.blocks_per_superblock;
        Ok(!self.superblock_slot[superblock as usize])
    }

    fn recompute_meta(&self) -> Result<(), CrucibleError> {
        // Iterate over every superblock, reading metadata from the beginning of
        // the superblock.  The metadata with the largest `meta_gen` is the most
        // recent.
        let mut current: Option<OnDiskMeta> = None;
        for i in 0..self.layout.superblock_count {
            let offset = (self.layout.blocks_per_superblock + 1)
                * self.extent_size.block_size_in_bytes() as u64
                * i;
            let mut buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
            nix::sys::uio::pread(
                self.file.as_raw_fd(),
                &mut buf,
                offset as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
            let out: OnDiskMeta = bincode::deserialize(&buf)
                .map_err(|e| CrucibleError::IoError(e.to_string()))?;

            if current.map(|n| n.meta_gen < out.meta_gen).unwrap_or(true) {
                self.meta.set(out);
                current = Some(out);
            }
        }
        Ok(())
    }

    /// Sets raw metadata
    ///
    /// This function should only be called directly during a migration;
    /// otherwise, it's typical to call higher-level functions instead.
    pub fn set_metadata(
        &mut self,
        mut meta: OnDiskMeta,
    ) -> Result<(), CrucibleError> {
        let prev_meta = self.meta.get();

        let i = self.last_superblock_modified.unwrap_or(0);
        let offset = (self.layout.blocks_per_superblock + 1)
            * self.extent_size.block_size_in_bytes() as u64
            * i;
        meta.meta_gen = prev_meta.meta_gen.max(meta.meta_gen) + 1;

        let mut buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
        bincode::serialize_into(buf.as_mut_slice(), &meta).unwrap();

        //
        let r =
            nix::sys::uio::pwrite(self.file.as_raw_fd(), &buf, offset as i64)
                .map_err(|e| CrucibleError::IoError(e.to_string()));
        if r.is_err() {
            // The write failed, so reload the state of `meta` from the file
        } else {
            // The write is complete; we can update our local cache
            self.meta.set(meta);
        }

        Ok(())
    }

    fn get_block_context(
        &mut self,
        block: u64,
    ) -> Result<Option<DownstairsBlockContext>> {
        // Find the context slot for this block
        let slot = self.active_context[block as usize];

        // Read the context slot from the file on disk
        let offset = self.layout.context_slot_offset(block, slot);
        let mut buf = [0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
        nix::sys::uio::pread(self.file.as_raw_fd(), &mut buf, offset as i64)
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;

        // Deserialize and convert the struct format
        let out: Option<OnDiskDownstairsBlockContext> =
            bincode::deserialize(&buf)?;
        let out = out.map(|c| DownstairsBlockContext {
            block,
            block_context: c.block_context,
            on_disk_hash: c.on_disk_hash,
        });

        Ok(out)
    }

    /// Update the flush number, generation number, and clear the dirty bit
    fn set_flush_number(&mut self, new_flush: u64, new_gen: u64) -> Result<()> {
        let mut meta = self.meta.get();
        meta.flush_number = new_flush;
        meta.gen_number = new_gen;
        meta.dirty = false;
        self.set_metadata(meta)?;
        Ok(())
    }

    /// Returns the valid block contexts (or `None`) for the given block range
    fn get_block_contexts(
        &mut self,
        block: u64,
        count: u64,
    ) -> Result<Vec<Option<DownstairsBlockContext>>> {
        let mut out = vec![];
        for (_, mut group) in (block..block + count)
            .map(|block| (block, self.active_context[block as usize]))
            .group_by(|(block, slot)| {
                (block / self.layout.blocks_per_superblock, *slot)
            })
            .into_iter()
        {
            let (start_block, slot) = group.next().unwrap();
            let n = group.count() + 1;

            // Read the context slot from the file on disk
            let offset = self.layout.context_slot_offset(start_block, slot);
            let mut buf = vec![0u8; n * BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
            nix::sys::uio::pread(
                self.file.as_raw_fd(),
                &mut buf,
                offset as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;

            for chunk in
                buf.chunks_exact(BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize)
            {
                // Deserialize and convert the struct format
                let v: Option<OnDiskDownstairsBlockContext> =
                    bincode::deserialize(chunk)?;
                let v = v.map(|c| DownstairsBlockContext {
                    block,
                    block_context: c.block_context,
                    on_disk_hash: c.on_disk_hash,
                });
                out.push(v)
            }
        }
        Ok(out)
    }

    /// Ensures that all context data in the superblock is in the same slot
    ///
    /// Returns the slot in which context data is consistently stored.  This
    /// function **does not** sync the file to disk, and as such **does not**
    /// change `self.superblock_slot` (or `self.active_context`); that is all
    /// the caller's responsibility.
    ///
    /// In a "clean" state, all block context slots within this superblock are
    /// the same as `self.superblock_slot[superblock_index]`.  If this is the
    /// case, then the function will return that value.
    ///
    /// Otherwise, all context slots pointing to the **current** superblock slot
    /// will be copied to the alternate slot.
    fn ensure_superblock_is_consistent(
        &mut self,
        superblock_index: u64,
    ) -> Result<ContextSlot> {
        let n = self.layout.blocks_per_superblock;
        let block_slots = ((superblock_index * n)
            ..self.layout.block_count.min((superblock_index + 1) * n))
            .map(|b| self.active_context[b as usize])
            .collect::<Vec<_>>();

        let superblock_slot = self.superblock_slot[superblock_index as usize];

        // If every block slots matches the superblock slot, then we don't
        // need to do anything here.
        //
        // TODO: track a separate "superblock dirty" variable instead?
        if block_slots.iter().all(|v| *v == superblock_slot) {
            return Ok(superblock_slot);
        }

        // Otherwise, we need to copy over any context slots that would
        // otherwise be left behind.  We'll do this by copying chunks that
        // are as large as possible, using `group_by`.
        for (_slot, mut d) in block_slots
            .iter()
            .enumerate()
            .group_by(|b| b.1)
            .into_iter()
            .filter(|(slot, _group)| **slot == superblock_slot)
        {
            let start_offset = d.next().unwrap().0;
            let start_block = superblock_index
                * self.layout.blocks_per_superblock
                + start_offset as u64;
            let block_count = d.count() + 1;

            // Copy this chunk over into the soon-to-be-active slots
            let mut buf =
                vec![0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize * block_count];
            nix::sys::uio::pread(
                self.file.as_raw_fd(),
                &mut buf,
                self.layout
                    .context_slot_offset(start_block, superblock_slot)
                    as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
            nix::sys::uio::pwrite(
                self.file.as_raw_fd(),
                &buf,
                self.layout
                    .context_slot_offset(start_block, !superblock_slot)
                    as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
        }

        Ok(!superblock_slot)
    }

    fn set_superblock_slot(
        &mut self,
        superblock_index: u64,
        slot: ContextSlot,
    ) {
        let i = superblock_index as usize;
        self.superblock_slot[i] = slot;
        let n = self.layout.blocks_per_superblock as usize;
        self.active_context
            [i * n..(self.layout.block_count as usize).min((i + 1) * n)]
            .fill(slot)
    }

    fn write_inner(
        &self,
        writes: &[&crucible_protocol::Write],
        writes_to_skip: &HashSet<u64>,
        iov_max: usize,
    ) -> Result<(), CrucibleError> {
        // PERFORMANCE TODO:
        //
        // Something worth considering for small writes is that, based on
        // my memory of conversations we had with propolis folks about what
        // OSes expect out of an NVMe driver, I believe our contract with the
        // upstairs doesn't require us to have the writes inside the file
        // until after a flush() returns. If that is indeed true, we could
        // buffer a certain amount of writes, only actually writing that
        // buffer when either a flush is issued or the buffer exceeds some
        // set size (based on our memory constraints). This would have
        // benefits on any workload that frequently writes to the same block
        // between flushes, would have benefits for small contiguous writes
        // issued over multiple write commands by letting us batch them into
        // a larger write, and (speculation) may benefit non-contiguous writes
        // by cutting down the number of metadata writes. But, it introduces
        // complexity. The time spent implementing that would probably better be
        // spent switching to aio or something like that.

        // Now, batch writes into iovecs and use pwritev to write them all out.
        let mut batched_pwritev = BatchedPwritev::new(
            self.file.as_raw_fd(),
            writes.len(),
            self.extent_size.block_size_in_bytes().into(),
            iov_max,
        );

        for write in writes {
            let block = write.offset.value;

            if writes_to_skip.contains(&block) {
                continue;
            }

            batched_pwritev.add_write_block(
                write,
                self.layout.block_pos(write.offset.value),
            )?;
        }

        // Write any remaining data
        batched_pwritev.perform_writes()?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::extent::extent_path;
    use bytes::{Bytes, BytesMut};
    use crucible_common::RegionOptions;
    use crucible_protocol::EncryptionContext;
    use crucible_protocol::ReadRequest;
    use rand::Rng;
    use tempfile::tempdir;

    const IOV_MAX_TEST: usize = 1000;

    fn new_region_definition() -> RegionDefinition {
        let opt = crate::region::test::new_region_options();
        RegionDefinition::from_options(&opt).unwrap()
    }

    #[tokio::test]
    async fn encryption_context() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(&extent_path(dir, 0), &new_region_definition(), 0)
                .unwrap();

        // Encryption context for blocks 0 and 1 should start blank

        assert!(inner.get_block_contexts(0, 1)?[0].is_none());
        assert!(inner.get_block_contexts(1, 1)?[0].is_none());

        // Set and verify block 0's context
        inner.set_dirty_and_block_context(&DownstairsBlockContext {
            block_context: BlockContext {
                encryption_context: Some(EncryptionContext {
                    nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                    tag: [
                        4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                        19,
                    ],
                }),
                hash: 123,
            },
            block: 0,
            on_disk_hash: 456,
        })?;

        let ctxs = inner.get_block_contexts(0, 1)?;
        let ctx = ctxs[0].as_ref().unwrap();
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().nonce,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().tag,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );
        assert_eq!(ctx.block_context.hash, 123);
        assert_eq!(ctx.on_disk_hash, 456);

        // Block 1 should still be blank
        assert!(inner.get_block_contexts(1, 1)?[0].is_none());

        // Set and verify a new context for block 0
        let blob1 = rand::thread_rng().gen::<[u8; 12]>();
        let blob2 = rand::thread_rng().gen::<[u8; 16]>();

        // Set and verify block 0's context
        inner.set_dirty_and_block_context(&DownstairsBlockContext {
            block_context: BlockContext {
                encryption_context: Some(EncryptionContext {
                    nonce: blob1,
                    tag: blob2,
                }),
                hash: 1024,
            },
            block: 0,
            on_disk_hash: 65536,
        })?;

        let ctxs = inner.get_block_contexts(0, 1)?;
        let ctx = ctxs[0].as_ref().unwrap();

        // Second context was appended
        assert_eq!(ctx.block_context.encryption_context.unwrap().nonce, blob1);
        assert_eq!(ctx.block_context.encryption_context.unwrap().tag, blob2);
        assert_eq!(ctx.block_context.hash, 1024);
        assert_eq!(ctx.on_disk_hash, 65536);

        Ok(())
    }

    #[tokio::test]
    async fn multiple_context() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(&extent_path(dir, 0), &new_region_definition(), 0)
                .unwrap();

        // Encryption context for blocks 0 and 1 should start blank

        assert!(inner.get_block_contexts(0, 1)?[0].is_none());
        assert!(inner.get_block_contexts(1, 1)?[0].is_none());

        // Set block 0's and 1's context and dirty flag
        inner.set_dirty_and_block_context(&DownstairsBlockContext {
            block_context: BlockContext {
                encryption_context: Some(EncryptionContext {
                    nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                    tag: [
                        4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                        19,
                    ],
                }),
                hash: 123,
            },
            block: 0,
            on_disk_hash: 456,
        })?;
        inner.set_dirty_and_block_context(&DownstairsBlockContext {
            block_context: BlockContext {
                encryption_context: Some(EncryptionContext {
                    nonce: [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                    tag: [8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
                }),
                hash: 9999,
            },
            block: 1,
            on_disk_hash: 1234567890,
        })?;

        // Verify block 0's context
        let ctxs = inner.get_block_contexts(0, 1)?;
        let ctx = ctxs[0].as_ref().unwrap();
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().nonce,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().tag,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );
        assert_eq!(ctx.block_context.hash, 123);
        assert_eq!(ctx.on_disk_hash, 456);

        // Verify block 1's context
        let ctxs = inner.get_block_contexts(1, 1)?;
        let ctx = ctxs[0].as_ref().unwrap();

        assert_eq!(
            ctx.block_context.encryption_context.unwrap().nonce,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().tag,
            [8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        );
        assert_eq!(ctx.block_context.hash, 9999);
        assert_eq!(ctx.on_disk_hash, 1234567890);

        // Return both block 0's and block 1's context, and verify

        let ctxs = inner.get_block_contexts(0, 2)?;
        let ctx = ctxs[0].as_ref().unwrap();
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().nonce,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().tag,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );
        assert_eq!(ctx.block_context.hash, 123);
        assert_eq!(ctx.on_disk_hash, 456);

        let ctx = ctxs[1].as_ref().unwrap();
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().nonce,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().tag,
            [8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        );
        assert_eq!(ctx.block_context.hash, 9999);
        assert_eq!(ctx.on_disk_hash, 1234567890);

        // Append a whole bunch of block context rows
        for i in 0..10 {
            inner.set_dirty_and_block_context(&DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: Some(EncryptionContext {
                        nonce: rand::thread_rng().gen::<[u8; 12]>(),
                        tag: rand::thread_rng().gen::<[u8; 16]>(),
                    }),
                    hash: rand::thread_rng().gen::<u64>(),
                },
                block: 0,
                on_disk_hash: i,
            })?;
            inner.set_dirty_and_block_context(&DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: Some(EncryptionContext {
                        nonce: rand::thread_rng().gen::<[u8; 12]>(),
                        tag: rand::thread_rng().gen::<[u8; 16]>(),
                    }),
                    hash: rand::thread_rng().gen::<u64>(),
                },
                block: 1,
                on_disk_hash: i,
            })?;
        }

        let ctxs = inner.get_block_contexts(0, 2)?;

        assert!(ctxs[0].is_some());
        assert_eq!(ctxs[0].as_ref().unwrap().on_disk_hash, 9);

        assert!(ctxs[1].is_some());
        assert_eq!(ctxs[1].as_ref().unwrap().on_disk_hash, 9);

        Ok(())
    }

    #[test]
    fn test_write_unwritten_without_flush() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(&extent_path(dir, 0), &new_region_definition(), 0)
                .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = crucible_protocol::Write {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_context: BlockContext {
                encryption_context: None,
                hash,
            },
        };
        inner.write(JobId(10), &[&write], false, IOV_MAX_TEST)?;

        // The context should be in place, though we haven't flushed yet

        // Therefore, we expect that write_unwritten to the first block won't
        // do anything.
        {
            let data = Bytes::from(vec![0x66; 512]);
            let hash = integrity_hash(&[&data[..]]);
            let block_context = BlockContext {
                encryption_context: None,
                hash,
            };
            let write = crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(0),
                data: data.clone(),
                block_context,
            };
            inner.write(JobId(20), &[&write], true, IOV_MAX_TEST)?;

            let mut resp = Vec::new();
            let read = ReadRequest {
                eid: 0,
                offset: Block::new_512(0),
            };
            inner.read(JobId(21), &[&read], &mut resp, IOV_MAX_TEST)?;

            // We should not get back our data, because block 0 was written.
            assert_ne!(
                resp,
                vec![ReadResponse {
                    eid: 0,
                    offset: Block::new_512(0),
                    data: BytesMut::from(data.as_ref()),
                    block_contexts: vec![block_context]
                }]
            );
        }

        // But, writing to the second block still should!
        {
            let data = Bytes::from(vec![0x66; 512]);
            let hash = integrity_hash(&[&data[..]]);
            let block_context = BlockContext {
                encryption_context: None,
                hash,
            };
            let write = crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(1),
                data: data.clone(),
                block_context,
            };
            inner.write(JobId(30), &[&write], true, IOV_MAX_TEST)?;

            let mut resp = Vec::new();
            let read = ReadRequest {
                eid: 0,
                offset: Block::new_512(1),
            };
            inner.read(JobId(31), &[&read], &mut resp, IOV_MAX_TEST)?;

            // We should get back our data! Block 1 was never written.
            assert_eq!(
                resp,
                vec![ReadResponse {
                    eid: 0,
                    offset: Block::new_512(1),
                    data: BytesMut::from(data.as_ref()),
                    block_contexts: vec![block_context]
                }]
            );
        }

        Ok(())
    }

    #[test]
    fn test_auto_sync() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(&extent_path(dir, 0), &new_region_definition(), 0)
                .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = crucible_protocol::Write {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_context: BlockContext {
                encryption_context: None,
                hash,
            },
        };
        assert_eq!(inner.layout.blocks_per_superblock, 5);

        // Initial state: context and superblock slots are all A, because this
        // is a new (empty) file
        assert_eq!(inner.superblock_slot[0], ContextSlot::A);
        assert_eq!(inner.active_context[0], ContextSlot::A);

        // The context should be written to slot B
        inner.write(JobId(10), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.superblock_slot[0], ContextSlot::A);
        assert_eq!(inner.active_context[0], ContextSlot::B);

        // The context should be written to slot A, forcing a flush and copying
        // + changing every other context slot in this superblock
        inner.write(JobId(11), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.superblock_slot[0], ContextSlot::B);
        assert_eq!(inner.active_context[0], ContextSlot::A);
        assert_eq!(inner.active_context[1], ContextSlot::B);
        assert_eq!(inner.active_context[2], ContextSlot::B);
        assert_eq!(inner.active_context[3], ContextSlot::B);
        assert_eq!(inner.active_context[4], ContextSlot::B);
        // This shouldn't change other superblocks!
        assert_eq!(inner.superblock_slot[1], ContextSlot::A);

        // The context should be written to slot B, forcing another sync
        inner.write(JobId(12), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.active_context[0], ContextSlot::B);
        assert_eq!(inner.superblock_slot[0], ContextSlot::A);
        assert_eq!(inner.superblock_slot[1], ContextSlot::A);

        Ok(())
    }

    #[test]
    fn test_auto_sync_flush() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(&extent_path(dir, 0), &new_region_definition(), 0)
                .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = crucible_protocol::Write {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_context: BlockContext {
                encryption_context: None,
                hash,
            },
        };
        // The context should be written to slot B
        inner.write(JobId(10), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.superblock_slot[0], ContextSlot::A);
        assert_eq!(inner.active_context[0], ContextSlot::B);

        // Flush, which should force a sync (marking slot B as synched
        // in superblock 0, and leaving other superblocks unchanged)
        inner.flush(12, 12, JobId(11).into())?;
        assert_eq!(inner.superblock_slot[0], ContextSlot::B);
        assert_eq!(inner.active_context[0], ContextSlot::B);
        assert_eq!(inner.active_context[1], ContextSlot::B);
        assert_eq!(inner.active_context[2], ContextSlot::B);
        assert_eq!(inner.active_context[3], ContextSlot::B);
        assert_eq!(inner.active_context[4], ContextSlot::B);
        assert_eq!(inner.superblock_slot[1], ContextSlot::A);

        // The context should be written to slot A, without a sync
        inner.write(JobId(11), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.superblock_slot[0], ContextSlot::B);
        assert_eq!(inner.active_context[0], ContextSlot::A);

        // The context should be written to slot B, forcing a sync
        inner.write(JobId(12), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.superblock_slot[0], ContextSlot::A);
        assert_eq!(inner.active_context[0], ContextSlot::B);
        assert_eq!(inner.active_context[1], ContextSlot::A);
        assert_eq!(inner.active_context[2], ContextSlot::A);
        assert_eq!(inner.active_context[3], ContextSlot::A);
        assert_eq!(inner.active_context[4], ContextSlot::A);

        Ok(())
    }

    #[test]
    fn test_auto_sync_flush_2() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(&extent_path(dir, 0), &new_region_definition(), 0)
                .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = crucible_protocol::Write {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_context: BlockContext {
                encryption_context: None,
                hash,
            },
        };
        // The context should be written to slot B
        inner.write(JobId(10), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.superblock_slot[0], ContextSlot::A);
        assert_eq!(inner.active_context[0], ContextSlot::B);

        // The context should be written to slot A, forcing a sync
        inner.write(JobId(10), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.superblock_slot[0], ContextSlot::B);
        assert_eq!(inner.active_context[0], ContextSlot::A);
        assert_eq!(inner.active_context[1], ContextSlot::B);

        // Flush, which should bump the sync number (marking slot B as synched
        // in superblock 0, and leaving other superblocks unchanged)
        inner.flush(12, 12, JobId(11).into())?;
        assert_eq!(inner.superblock_slot[0], ContextSlot::A);
        assert_eq!(inner.active_context[0], ContextSlot::A);
        assert_eq!(inner.active_context[1], ContextSlot::A);
        assert_eq!(inner.active_context[2], ContextSlot::A);
        assert_eq!(inner.active_context[3], ContextSlot::A);
        assert_eq!(inner.active_context[4], ContextSlot::A);
        assert_eq!(inner.superblock_slot[0], ContextSlot::A);

        // The context should be written to slot B, without a sync
        inner.write(JobId(11), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.superblock_slot[0], ContextSlot::A);
        assert_eq!(inner.active_context[0], ContextSlot::B);
        assert_eq!(inner.active_context[1], ContextSlot::A);

        // The context should be written to slot A, forcing a sync
        inner.write(JobId(12), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.superblock_slot[0], ContextSlot::B);
        assert_eq!(inner.active_context[0], ContextSlot::A);
        assert_eq!(inner.active_context[1], ContextSlot::B);
        assert_eq!(inner.active_context[2], ContextSlot::B);
        assert_eq!(inner.active_context[3], ContextSlot::B);
        assert_eq!(inner.active_context[4], ContextSlot::B);

        Ok(())
    }

    /// If a write successfully put a context into a context slot, but it never
    /// actually got the data onto the disk, that block should revert back to
    /// being "unwritten". After all, the data never was truly written.
    ///
    /// This test is very similar to test_region_open_removes_partial_writes.
    #[test]
    fn test_reopen_marks_blocks_unwritten_if_data_never_hit_disk() -> Result<()>
    {
        let dir = tempdir()?;
        let mut inner = RawInner::create(
            &extent_path(&dir, 0),
            &new_region_definition(),
            0,
        )
        .unwrap();

        // Partial write, the data never hits disk, but there's a context
        // in the DB and the dirty flag is set.
        inner.set_dirty_and_block_context(&DownstairsBlockContext {
            block_context: BlockContext {
                encryption_context: None,
                hash: 1024,
            },
            block: 0,
            on_disk_hash: 65536,
        })?;
        drop(inner);

        // Reopen, which should note that the hash doesn't match on-disk values
        // and decide that block 0 is unwritten.
        let mut inner = RawInner::create(
            &extent_path(&dir, 0),
            &new_region_definition(),
            0,
        )
        .unwrap();

        // Writing to block 0 should succeed with only_write_unwritten
        {
            let data = Bytes::from(vec![0x66; 512]);
            let hash = integrity_hash(&[&data[..]]);
            let block_context = BlockContext {
                encryption_context: None,
                hash,
            };
            let write = crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(0),
                data: data.clone(),
                block_context,
            };
            inner.write(JobId(30), &[&write], true, IOV_MAX_TEST)?;

            let mut resp = Vec::new();
            let read = ReadRequest {
                eid: 0,
                offset: Block::new_512(0),
            };
            inner.read(JobId(31), &[&read], &mut resp, IOV_MAX_TEST)?;

            // We should get back our data! Block 1 was never written.
            assert_eq!(
                resp,
                vec![ReadResponse {
                    eid: 0,
                    offset: Block::new_512(0),
                    data: BytesMut::from(data.as_ref()),
                    block_contexts: vec![block_context]
                }]
            );
        }

        Ok(())
    }

    #[test]
    fn test_layout() {
        let mut options = RegionOptions::default();
        options.set_block_size(512); // bytes
        options.set_extent_size(Block::new_512(128)); // blocks
        let def = RegionDefinition::from_options(&options).unwrap();
        let sb = RawExtentLayout::new(&def);

        assert_eq!(sb.block_pos(0), 1);
        assert_eq!(sb.block_pos(1), 2);
        assert_eq!(sb.block_pos(2), 3);
        assert_eq!(sb.block_pos(3), 4);
        assert_eq!(sb.block_pos(4), 5);
        assert_eq!(sb.block_pos(5), 7); // offset!
        assert_eq!(sb.context_slot_offset(0, ContextSlot::A), 32);
        assert_eq!(sb.context_slot_offset(0, ContextSlot::B), 32 + 5 * 48);
        assert_eq!(sb.context_slot_offset(1, ContextSlot::A), 32 + 48);
        assert_eq!(sb.context_slot_offset(1, ContextSlot::B), 32 + 5 * 48 + 48);
        assert_eq!(sb.context_slot_offset(5, ContextSlot::A), 6 * 512 + 32);
        assert_eq!(
            sb.context_slot_offset(5, ContextSlot::B),
            6 * 512 + 32 + 5 * 48
        );

        // Check that an image with 4K blocks snaps to 128 KiB
        options.set_block_size(4096); // bytes
        options.set_extent_size(Block::new_512(128)); // blocks
        let def = RegionDefinition::from_options(&options).unwrap();
        let sb = RawExtentLayout::new(&def);

        assert_eq!(sb.blocks_per_superblock, 31);
        assert_eq!(
            sb.extent_file_size,
            ((128 / 31) * 32 + (128 % 31) + 1) * 4096
        );

        // No trailing blocks, two superblocks
        options.set_block_size(4096); // bytes
        options.set_extent_size(Block::new_512(62));
        let def = RegionDefinition::from_options(&options).unwrap();
        let sb = RawExtentLayout::new(&def);

        assert_eq!(sb.blocks_per_superblock, 31);
        assert_eq!(sb.extent_file_size, ((63 / 31) * 32) * 4096);

        // Even larger blocks!
        options.set_block_size(8192); // bytes
        options.set_extent_size(Block::new_512(128));
        let def = RegionDefinition::from_options(&options).unwrap();
        let sb = RawExtentLayout::new(&def);

        assert_eq!(sb.blocks_per_superblock, 79);
    }
}
