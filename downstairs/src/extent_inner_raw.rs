// Copyright 2023 Oxide Computer Company
use crate::{
    cdt, crucible_bail,
    extent::{check_input, extent_path, ExtentInner, EXTENT_META_RAW},
    mkdir_for_file,
    region::JobOrReconciliationId,
    Block, BlockContext, CrucibleError, JobId, ReadResponse, RegionDefinition,
};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use slog::{error, Logger};

use std::fs::{File, OpenOptions};
use std::io::{BufReader, IoSlice, IoSliceMut, Read};
use std::os::fd::AsRawFd;
use std::path::Path;

/// Equivalent to `ExtentMeta`, but ordered for efficient on-disk serialization
///
/// In particular, the `dirty` byte is first, so it's easy to read at a known
/// offset within the file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnDiskMeta {
    pub dirty: bool,
    pub gen_number: u64,
    pub flush_number: u64,
    pub ext_version: u32,
}

/// Size of backup data
///
/// This must be large enough to fit an `Option<BlockContext>`
/// serialized using `bincode`.
pub const BLOCK_CONTEXT_SLOT_SIZE_BYTES: u64 = 48;

/// Size of metadata region
///
/// This must be large enough to contain an `OnDiskMeta` serialized using
/// `bincode`.
pub const BLOCK_META_SIZE_BYTES: u64 = 32;

/// `RawInner` is a wrapper around a [`std::fs::File`] representing an extent
///
/// The file is structured as follows:
/// - Block and context data, structured as
///   `(block_size + BLOCK_CONTEXT_SLOT_SIZE_BYTES) × extent_size`
/// - Block occupancy data, stored as a bit-packed array (where 0 is
///   empty and 1 is occupied).  This array contains `(extent_size + 7) / 8`
///   bytes.  It is only valid when the `dirty` bit is cleared.  This is an
///   optimization that speeds up opening a clean extent file; otherwise, we
///   would have to read every context slot to determine if a block is present.
/// - [`BLOCK_META_SIZE_BYTES`], which contains an [`OnDiskMeta`] serialized
///   using `bincode`.  The first byte of this range is `dirty`, serialized as a
///   `u8` (where `1` is dirty and `0` is clean).
#[derive(Debug)]
pub struct RawInner {
    file: File,

    /// Our extent number
    extent_number: u32,

    /// Extent size, in blocks
    extent_size: Block,

    /// Local cache for the `dirty` value
    ///
    /// This allows us to only write the flag when the value changes
    dirty: bool,

    /// Local cache of whether a block is written
    ///
    /// This data mirrors values stored in the block occupancy section of the
    /// data file on disk; we cache it for convenience.  The values start as all
    /// `false` and can only transition from `false` to `true` (not back!)
    written: Vec<bool>,
}

impl ExtentInner for RawInner {
    fn flush_number(&self) -> Result<u64, CrucibleError> {
        self.get_metadata().map(|v| v.flush_number)
    }

    fn gen_number(&self) -> Result<u64, CrucibleError> {
        self.get_metadata().map(|v| v.gen_number)
    }

    fn dirty(&self) -> Result<bool, CrucibleError> {
        Ok(self.dirty)
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
         * 2) for each write, write out extent data and encryption context in a
         *    single syscall
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
         * 2) write extent + context data, grouping into contiguous regions
         *
         * If "only_write_unwritten" is true, then we only issue a write for
         * a block if that block has not been written to yet.  Note
         * that we can have a write that is "sparse" if the range of
         * blocks it contains has a mix of written an unwritten
         * blocks.
         *
         * We define a block being written to or not has if that block's context
         * is stored as `Some(...)`.  This value is cached locally!
         */

        // If `only_write_written`, we need to skip writing to blocks that
        // already contain data. We've cached this locally as a bitmask!
        let filtered_writes: Option<Vec<_>> = if only_write_unwritten {
            cdt::extent__write__get__hashes__start!(|| {
                (job_id.0, self.extent_number, writes.len() as u64)
            });
            let writes: Vec<_> = writes
                .iter()
                .filter(|w| !self.written[w.offset.value as usize])
                .copied()
                .collect();
            cdt::extent__write__get__hashes__done!(|| {
                (job_id.0, self.extent_number, writes.len() as u64)
            });
            Some(writes)
        } else {
            None
        };
        let writes = filtered_writes
            .as_ref()
            .map(|v| v.as_ref())
            .unwrap_or(writes);

        self.set_dirty()?;

        cdt::extent__write__file__start!(|| {
            (job_id.0, self.extent_number, writes.len() as u64)
        });
        let mut req_run_start = 0;
        while req_run_start < writes.len() {
            let first_req = writes[req_run_start];
            let mut n_contiguous_requests = 1;

            for request_window in writes[req_run_start..].windows(2) {
                if (request_window[0].offset.value + 1
                    == request_window[1].offset.value)
                    && (n_contiguous_requests + 1 < iov_max)
                {
                    n_contiguous_requests += 1;
                } else {
                    break;
                }
            }

            let mut write_data = Vec::with_capacity(n_contiguous_requests);
            for w in &writes[req_run_start..][..n_contiguous_requests] {
                // Mark this block as written locally.  We deliberately do not
                // write this to the file (to save on syscalls); it is written
                // to file in the same write which clears the dirty flag
                // (marking the `written` array in the file as valid).
                self.written[w.offset.value as usize] = true;

                // Collect the data to be written.  Unfortunately, this requires
                // an extra memcopy, because we need to write both block data
                // and context atomically.
                let mut data = Vec::with_capacity(
                    self.extent_size.block_size_in_bytes() as usize
                        + BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize,
                );
                data.extend(w.data.iter());
                data.extend([0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize]);
                bincode::serialize_into(
                    &mut data
                        [self.extent_size.block_size_in_bytes() as usize..],
                    &Some(w.block_context),
                )
                .map_err(|e| {
                    CrucibleError::IoError(format!(
                        "could not serialize context: {e}"
                    ))
                })?;
                write_data.push(data);
            }

            let iovecs: Vec<_> =
                write_data.iter().map(|v| IoSlice::new(v)).collect();
            nix::sys::uio::pwritev(
                self.file.as_raw_fd(),
                &iovecs[..],
                self.block_offset(first_req.offset.value) as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
            write_data.clear();

            req_run_start += n_contiguous_requests;
        }

        cdt::extent__write__file__done!(|| {
            (job_id.0, self.extent_number, writes.len() as u64)
        });

        Ok(())
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
                if (request_window[0].offset.value + 1
                    == request_window[1].offset.value)
                    && ((n_contiguous_requests + 1) * 2 < iov_max)
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

            // Create what amounts to an iovec for each response data buffer and
            // context array.
            let mut ctxs = vec![
                [0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
                n_contiguous_requests
            ];
            for (resp, ctx) in responses[resp_run_start..]
                [..n_contiguous_requests]
                .iter_mut()
                .zip(ctxs.iter_mut())
            {
                iovecs.push(IoSliceMut::new(&mut resp.data[..]));
                iovecs.push(IoSliceMut::new(ctx.as_mut()));
            }

            // Finally we get to read the actual data. That's why we're here
            cdt::extent__read__file__start!(|| {
                (job_id.0, self.extent_number, n_contiguous_requests as u64)
            });

            nix::sys::uio::preadv(
                self.file.as_raw_fd(),
                &mut iovecs,
                self.block_offset(first_req.offset.value) as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;

            cdt::extent__read__file__done!(|| {
                (job_id.0, self.extent_number, n_contiguous_requests as u64)
            });

            // Now it's time to put block contexts into the responses.
            // We use into_iter here to move values out of enc_ctxts/hashes,
            // avoiding a clone(). For code consistency, we use iters for the
            // response and data chunks too. These iters will be the same length
            // (equal to n_contiguous_requests) so zipping is fine
            let resp_iter =
                responses[resp_run_start..][..n_contiguous_requests].iter_mut();

            for (resp, ctx) in resp_iter.zip(ctxs) {
                let ctx: Option<BlockContext> = bincode::deserialize(&ctx)
                    .map_err(|e| {
                        CrucibleError::IoError(format!(
                            "could not deserialize context: {e}"
                        ))
                    })?;
                resp.block_contexts = ctx.into_iter().collect();
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

        cdt::extent__flush__start!(|| {
            (job_id.get(), self.extent_number, 0)
        });

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

        cdt::extent__flush__done!(|| { (job_id.get(), self.extent_number, 0) });

        Ok(())
    }
}

impl RawInner {
    fn file_size(def: &RegionDefinition) -> u64 {
        let bcount = def.extent_size().value;
        def.block_size()
            .checked_add(BLOCK_CONTEXT_SLOT_SIZE_BYTES)
            .unwrap()
            .checked_mul(bcount)
            .unwrap()
            .checked_add(BLOCK_META_SIZE_BYTES)
            .unwrap()
            .checked_add(bcount.checked_add(7).unwrap() / 8)
            .unwrap()
    }

    pub fn create(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: u32,
    ) -> Result<Self> {
        let path = extent_path(dir, extent_number);
        let bcount = def.extent_size().value;
        let size = Self::file_size(def);

        mkdir_for_file(&path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // All 0s are fine for everything except extent version in the metadata
        file.set_len(size)?;
        let mut out = Self {
            file,
            dirty: false,
            extent_size: def.extent_size(),
            extent_number,
            written: vec![false; bcount as usize],
        };
        // Setting the flush number also writes the extent version, since
        // they're serialized together in the same block.
        out.set_flush_number(0, 0)?;

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
        let extent_size = def.extent_size();
        let bcount = extent_size.value;
        let size = Self::file_size(def);

        /*
         * Open the extent file and verify the size is as we expect.
         */
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
                    if size != cur_size {
                        bail!(
                            "File size {size:?} does not match \
                             expected {cur_size:?}",
                        );
                    }
                    f
                }
            };

        // Just in case, let's be very sure that the file on disk is what it
        // should be
        if !read_only {
            if let Err(e) = file.sync_all() {
                return Err(CrucibleError::IoError(format!(
                    "extent {extent_number}:
                 fsync 1 failure during initial rehash: {e:?}",
                ))
                .into());
            }
        }

        // Read the `written` array and metadata block
        //
        // The written array is only valid if the block isn't dirty, but that
        // should be the most common case.
        let mut meta_buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
        let mut written = vec![0u8; (bcount as usize + 7) / 8];
        let mut iovecs = [
            IoSliceMut::new(&mut written),
            IoSliceMut::new(&mut meta_buf),
        ];
        nix::sys::uio::preadv(
            file.as_raw_fd(),
            &mut iovecs,
            (bcount * (def.block_size() + BLOCK_CONTEXT_SLOT_SIZE_BYTES))
                as i64,
        )
        .map_err(|e| CrucibleError::IoError(e.to_string()))?;
        let dirty = match meta_buf[0] {
            0 => false,
            1 => true,
            i => bail!("invalid dirty value: {i}"),
        };

        let written = if !dirty {
            let mut out = vec![];
            for b in written {
                // Unpack bits from each byte
                for i in 0..8 {
                    out.push(b & (1 << i) != 0);
                }
            }
            // It's possible that block count isn't a multiple of 8; in that
            // case, shrink down the active context array.
            assert!(bcount as usize <= out.len());
            out.resize(bcount as usize, false);
            out
        } else {
            let mut reader = BufReader::with_capacity(64 * 1024, &file);
            let mut out = vec![];
            let mut buf = [0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
            // Otherwise, we have to do this the hard way.  Read every single
            // metadata section in the file and check whether they are `None` or
            // `Some(..)`.
            for _block in 0..bcount {
                reader.seek_relative(def.block_size() as i64)?;
                reader.read_exact(&mut buf)?;
                let context: Option<BlockContext> =
                    bincode::deserialize_from(buf.as_slice())?;
                out.push(context.is_some());
            }
            out
        };

        Ok(Self {
            file,
            dirty,
            extent_number,
            extent_size: def.extent_size(),
            written,
        })
    }

    fn set_dirty(&mut self) -> Result<(), CrucibleError> {
        if !self.dirty {
            let offset = self.meta_offset();
            nix::sys::uio::pwrite(self.file.as_raw_fd(), &[1u8], offset as i64)
                .map_err(|e| CrucibleError::IoError(e.to_string()))?;
            self.dirty = true;
        }
        Ok(())
    }

    fn get_metadata(&self) -> Result<OnDiskMeta, CrucibleError> {
        let mut buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
        let offset = self.meta_offset();
        nix::sys::uio::pread(self.file.as_raw_fd(), &mut buf, offset as i64)
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
        let out: OnDiskMeta = bincode::deserialize(&buf)
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
        Ok(out)
    }

    /// Returns the byte offset of the metadata region
    ///
    /// The resulting offset points to serialized [`OnDiskMeta`] data.
    fn meta_offset(&self) -> u64 {
        Self::meta_offset_from_extent_size(self.extent_size)
    }

    /// Returns the byte offset of the given block
    fn block_offset(&self, block: u64) -> u64 {
        (self.extent_size.block_size_in_bytes() as u64
            + BLOCK_CONTEXT_SLOT_SIZE_BYTES)
            * block
    }

    fn meta_offset_from_extent_size(extent_size: Block) -> u64 {
        (extent_size.block_size_in_bytes() as u64
            + BLOCK_CONTEXT_SLOT_SIZE_BYTES)
            * extent_size.value
            + (extent_size.value + 7) / 8 // `written` array
    }

    /// Update the flush number, generation number, and clear the dirty bit
    fn set_flush_number(&mut self, new_flush: u64, new_gen: u64) -> Result<()> {
        let mut buf = Vec::with_capacity(
            ((self.extent_size.value + 7) / 8 + BLOCK_META_SIZE_BYTES) as usize,
        );

        for c in self.written.chunks(8) {
            let mut v = 0;
            for (i, slot) in c.iter().enumerate() {
                v |= (*slot as u8) << i;
            }
            buf.push(v);
        }

        let d = OnDiskMeta {
            dirty: false,
            flush_number: new_flush,
            gen_number: new_gen,
            ext_version: EXTENT_META_RAW,
        };
        let mut meta = [0u8; BLOCK_META_SIZE_BYTES as usize];
        bincode::serialize_into(meta.as_mut_slice(), &d)?;
        buf.extend(&meta);

        // Serialize bitpacked `written` array and metadata
        let offset = self.extent_size.value
            * (self.extent_size.block_size_in_bytes() as u64
                + BLOCK_CONTEXT_SLOT_SIZE_BYTES);

        nix::sys::uio::pwrite(self.file.as_raw_fd(), &buf, offset as i64)
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
        self.dirty = false;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::integrity_hash;
    use bytes::{Bytes, BytesMut};
    use crucible_protocol::EncryptionContext;
    use crucible_protocol::ReadRequest;
    use tempfile::tempdir;

    const IOV_MAX_TEST: usize = 1000;

    fn new_region_definition() -> RegionDefinition {
        let opt = crate::region::test::new_region_options();
        RegionDefinition::from_options(&opt).unwrap()
    }

    #[test]
    fn test_write_unwritten_without_flush() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(dir.as_ref(), &new_region_definition(), 0)
                .unwrap();

        // Write to block 0, but don't flush.
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
    fn test_write_unwritten_close_reopen() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(dir.as_ref(), &new_region_definition(), 0)
                .unwrap();

        // Write to block 0, but don't flush.
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

        // Reopen the file!  Because we never flushed, this will force us to
        // re-read metadata and notice that block 0 is written.
        let mut inner = RawInner::open(
            &extent_path(&dir, 0),
            &new_region_definition(),
            0,
            false,
            &crucible_common::build_logger(),
        )
        .unwrap();

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
    fn test_write_unwritten_flush_close_reopen() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(dir.as_ref(), &new_region_definition(), 0)
                .unwrap();

        // Write to block 0, but don't flush.
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
        inner.flush(2, 2, JobOrReconciliationId::JobId(JobId(10)))?;

        // Reopen the file!  Because the extent is no longer dirty, we will read
        // the `written` data from the file itself.
        let mut inner = RawInner::open(
            &extent_path(&dir, 0),
            &new_region_definition(),
            0,
            false,
            &crucible_common::build_logger(),
        )
        .unwrap();

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
    fn test_serialized_sizes() {
        let c = BlockContext {
            hash: u64::MAX,
            encryption_context: Some(EncryptionContext {
                nonce: [0xFF; 12],
                tag: [0xFF; 16],
            }),
        };
        let mut ctx_buf = [0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
        bincode::serialize_into(ctx_buf.as_mut_slice(), &Some(c)).unwrap();

        let m = OnDiskMeta {
            dirty: true,
            gen_number: u64::MAX,
            flush_number: u64::MAX,
            ext_version: u32::MAX,
        };
        let mut meta_buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
        bincode::serialize_into(meta_buf.as_mut_slice(), &Some(m)).unwrap();
    }
}
