// Copyright 2023 Oxide Computer Company
use crate::{
    cdt,
    extent::{check_input, extent_path, ExtentInner, EXTENT_META_RAW_V2},
    mkdir_for_file,
    region::JobOrReconciliationId,
    Block, CrucibleError, JobId, RegionDefinition,
};

use crucible_protocol::{RawReadResponse, ReadResponseBlockMetadata};
use serde::{Deserialize, Serialize};
use slog::{error, Logger};

use std::io::{BufReader, IoSliceMut, Read};
use std::os::fd::AsFd;
use std::path::Path;
use std::{
    fs::{File, OpenOptions},
    io::IoSlice,
};

/// Equivalent to `ExtentMeta`, but ordered for efficient on-disk serialization
///
/// In particular, the `dirty` byte is first, so it's easy to read at a known
/// offset within the file.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OnDiskMeta {
    dirty: bool,
    gen_number: u64,
    flush_number: u64,
    ext_version: u32,
}

/// Size of backup data
///
/// This must be large enough to fit an `Option<BlockContext>`
/// serialized using `bincode`.
const BLOCK_CONTEXT_SLOT_SIZE_BYTES: u64 = 40;

/// Size of metadata region
///
/// This must be large enough to contain an `OnDiskMeta` serialized using
/// `bincode`.
const BLOCK_META_SIZE_BYTES: u64 = 32;

/// `RawInner` is a wrapper around a [`std::fs::File`] representing an extent
///
/// The file is structured as follows:
/// - Block data and metadata, structured as an array of (context, data) tuples.
///   This has the total size
///   ([`BLOCK_CONTEXT_SLOT_SIZE_BYTES`] + block_size) × `extent_size`
/// - Written blocks, stored as a bit-packed array (where 0 is unwritten and 1
///   is written).  This array contains `(extent_size + 7) / 8` bytes.  It is
///   only valid when the `dirty` bit is cleared.  This is an optimization that
///   speeds up opening a clean extent file; otherwise, we would have to read
///   every block to find whether it has been written or not.
/// - [`BLOCK_META_SIZE_BYTES`], which contains an [`OnDiskMeta`] serialized
///   using `bincode`.  The first byte of this range is `dirty`, serialized as a
///   `u8` (where `1` is dirty and `0` is clean).
///
/// There are a few considerations that led to this particular ordering:
/// - Written blocks and metadata must be contiguous, because we want to write
///   them atomically when clearing the `dirty` flag
/// - The metadata contains an extent version (currently
///   [`EXTENT_META_RAW_V2`]). We will eventually have multiple raw file
///   formats, so it's convenient to always place the metadata at the end; this
///   lets us deserialize it without knowing anything else about the file, then
///   dispatch based on extent version.
#[derive(Debug)]
pub struct RawInner {
    file: File,

    /// Our extent number
    extent_number: u32,

    /// Extent size, in blocks
    extent_size: Block,

    /// Helper `struct` controlling layout within the file
    layout: RawLayout,

    /// Has this block been written?
    block_written: Vec<bool>,

    /// Local cache for the `dirty` value
    ///
    /// This allows us to only write the flag when the value changes
    dirty: bool,
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
        writes: &[crucible_protocol::Write],
        only_write_unwritten: bool,
        iov_max: usize,
    ) -> Result<(), CrucibleError> {
        // Find the largest contiguous write, as a slice of the `writes` input
        let mut start = 0;
        while start < writes.len() {
            // Skip written blocks if `only_write_unwritten` is true
            if only_write_unwritten && self.block_written[start] {
                start += 1;
                continue;
            }
            let mut end = start + 1;

            // Loop until
            // - We run off the end of the array
            // - We hit the max iovec size (with each write taking 2 ioops)
            // - We find a block which is not contiguous with its predecessor
            // - We find a block that should be skipped due to
            //   only_write_unwritten
            //
            // After this loop, `end` is the index of the first block that
            // **should not** be written.
            while end < writes.len().min(start + iov_max * 2)
                && writes[end - 1].offset.value + 1 == writes[end].offset.value
                && !(only_write_unwritten
                    && self.block_written[writes[end].offset.value as usize])
            {
                end += 1;
            }
            self.write_contiguous(job_id, &writes[start..end])?;
            start = end;
        }
        Ok(())
    }

    fn read_into(
        &mut self,
        job_id: JobId,
        requests: &[crucible_protocol::ReadRequest],
        out: &mut RawReadResponse,
        iov_max: usize,
    ) -> Result<(), CrucibleError> {
        // Find the largest contiguous read, as a slice of the `requests` input
        let mut start = 0;
        while start < requests.len() {
            let mut end = start + 1;

            // Loop until
            // - We run off the end of the array
            // - We hit the max iovec size (with each write taking 2 ioops)
            // - We find a block which is not contiguous with its predecessor
            // - We find a block that should be skipped due to
            //   only_write_unwritten
            //
            // After this loop, `end` is the index of the first block that
            // **should not** be written.
            while end < requests.len().min(start + iov_max * 2)
                && requests[end - 1].offset.value + 1
                    == requests[end].offset.value
            {
                end += 1;
            }
            self.read_contiguous_into(job_id, &requests[start..end], out)?;
            start = end;
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
            return Err(CrucibleError::IoError(format!(
                "extent {}: fsync 1 failure: {e:?}",
                self.extent_number,
            )));
        }
        cdt::extent__flush__file__done!(|| {
            (job_id.get(), self.extent_number, 0)
        });

        cdt::extent__flush__done!(|| { (job_id.get(), self.extent_number, 0) });

        Ok(())
    }
}

impl RawInner {
    pub fn create(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: u32,
    ) -> Result<Self, CrucibleError> {
        let path = extent_path(dir, extent_number);
        let extent_size = def.extent_size();
        let layout = RawLayout::new(extent_size);
        let size = layout.file_size();

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
            extent_size,
            block_written: vec![false; def.extent_size().value as usize],
            layout,
            extent_number,
        };
        // Setting the flush number also writes the extent version, since
        // they're serialized together in the same block.
        out.set_flush_number(0, 0)?;

        // Sync the file to disk, to avoid any questions
        if let Err(e) = out.file.sync_all() {
            return Err(CrucibleError::IoError(format!(
                "extent {}: fsync 1 failure during initial sync: {e}",
                out.extent_number,
            )));
        }
        Ok(out)
    }

    /// Constructs a new `Inner` object from files that already exist on disk
    pub fn open(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: u32,
        read_only: bool,
        log: &Logger,
    ) -> Result<Self, CrucibleError> {
        let path = extent_path(dir, extent_number);
        let extent_size = def.extent_size();
        let layout = RawLayout::new(extent_size);
        let size = layout.file_size();

        /*
         * Open the extent file and verify the size is as we expect.
         */
        let file =
            match OpenOptions::new().read(true).write(!read_only).open(&path) {
                Err(e) => {
                    error!(
                        log,
                        "Open of {path:?} for extent#{extent_number} \
                         returned: {e}",
                    );
                    return Err(CrucibleError::IoError(format!(
                        "extent {extent_number}: open of {path:?} failed: {e}",
                    )));
                }
                Ok(f) => {
                    let cur_size = f.metadata().unwrap().len();
                    if size != cur_size {
                        return Err(CrucibleError::IoError(format!(
                            "extent {extent_number}: file size {cur_size:?} \
                             does not match expected {size:?}",
                        )));
                    }
                    f
                }
            };

        // Just in case, let's be very sure that the file on disk is what it
        // should be
        if !read_only {
            if let Err(e) = file.sync_all() {
                return Err(CrucibleError::IoError(format!(
                    "extent {extent_number}: \
                     fsync 1 failure during initial rehash: {e}",
                )));
            }
        }

        let layout = RawLayout::new(def.extent_size());
        let meta = layout.get_metadata(&file)?;

        // If the file is dirty, then we have to recompute whether blocks are
        // written or not.  This is slow, but can't be avoided; we closed the
        // file without a flush so we can't be confident about the data that was
        // on disk.
        let block_written = if !meta.dirty {
            // Easy case first: if it's **not** dirty, then just assign active
            // slots based on the bitpacked active context buffer from the file.
            layout.get_block_written_array(&file)?
        } else {
            // Now that we've read the context slot arrays, read file data and
            // figure out which context slot is active.
            let mut file_buffered = BufReader::with_capacity(64 * 1024, &file);
            let mut block_written = vec![];
            for _ in 0..layout.block_count() {
                // Read the entire slot, though we only care about the first
                // byte.  We could deserialize, but it's easier just to check
                // the byte by hand, since we know the encoding.
                let mut buf = [0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
                file_buffered.read_exact(&mut buf)?;
                block_written.push(buf[0] != 0);

                // Skip the bulk data, on to the next block's context slot
                file_buffered
                    .seek_relative(extent_size.block_size_in_bytes() as i64)?;
            }
            block_written
        };

        Ok(Self {
            file,
            dirty: meta.dirty,
            extent_number,
            extent_size: def.extent_size(),
            block_written,
            layout: RawLayout::new(def.extent_size()),
        })
    }

    fn set_dirty(&mut self) -> Result<(), CrucibleError> {
        if !self.dirty {
            self.layout.set_dirty(&self.file)?;
            self.dirty = true;
        }
        Ok(())
    }

    /// Updates `self.block_written[block]` based on data read from the file
    ///
    /// Specifically, if the context is written (has a non-zero `Option` tag),
    /// then the block is guaranteed to be written, because they are always
    /// written together in an atomic operation.
    ///
    /// We expect to call this function rarely, so it does not attempt to
    /// minimize the number of syscalls it executes.
    fn recompute_block_written_from_file(
        &mut self,
        block: u64,
    ) -> Result<(), CrucibleError> {
        // Read the block data itself:
        let block_size = self.extent_size.block_size_in_bytes();
        let mut tag = [0u8];
        pread_all(
            self.file.as_fd(),
            &mut tag,
            ((block_size as u64 + BLOCK_CONTEXT_SLOT_SIZE_BYTES) * block)
                as i64,
        )
        .map_err(|e| {
            CrucibleError::IoError(format!(
                "extent {}: reading block {block} data failed: {e}",
                self.extent_number
            ))
        })?;

        self.block_written[block as usize] = tag[0] != 0;
        Ok(())
    }

    fn get_metadata(&self) -> Result<OnDiskMeta, CrucibleError> {
        self.layout.get_metadata(&self.file)
    }

    /// Update the flush number, generation number, and clear the dirty bit
    fn set_flush_number(
        &mut self,
        new_flush: u64,
        new_gen: u64,
    ) -> Result<(), CrucibleError> {
        self.layout.write_block_written_and_metadata(
            &self.file,
            &self.block_written,
            false, // dirty
            new_flush,
            new_gen,
        )?;
        self.dirty = false;
        Ok(())
    }

    /// Implementation details for `ExtentInner::write`
    ///
    /// This function requires that `writes` be a contiguous set of blocks of
    /// size `iov_max / 2` or smaller.
    fn write_contiguous(
        &mut self,
        job_id: JobId,
        writes: &[crucible_protocol::Write],
    ) -> Result<(), CrucibleError> {
        let block_size = self.extent_size.block_size_in_bytes();
        let n_blocks = writes.len();

        let mut ctxs =
            vec![[0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize]; n_blocks];
        for (ctx, w) in ctxs.iter_mut().zip(writes.iter()) {
            bincode::serialize_into(ctx.as_mut(), &w.block_context).unwrap();
        }

        let mut iovecs = Vec::with_capacity(n_blocks * 2);
        for (ctx, w) in ctxs.iter().zip(writes.iter()) {
            iovecs.push(IoSlice::new(ctx));
            iovecs.push(IoSlice::new(&w.data));
        }

        let start_block = writes[0].offset;
        let start_pos = start_block.value
            * (block_size as u64 + BLOCK_CONTEXT_SLOT_SIZE_BYTES);

        let expected_bytes = n_blocks as u64
            * (block_size as u64 + BLOCK_CONTEXT_SLOT_SIZE_BYTES);

        self.set_dirty()?;

        cdt::extent__write__file__start!(|| {
            (job_id.0, self.extent_number, writes.len() as u64)
        });

        let r = nix::sys::uio::pwritev(
            self.file.as_fd(),
            &iovecs,
            start_pos as i64,
        )
        .map_err(|e| {
            CrucibleError::IoError(format!(
                "extent {}: read failed: {e}",
                self.extent_number
            ))
        });
        let r = match r {
            Err(e) => Err(e),
            Ok(num_bytes_written)
                if num_bytes_written as u64 != expected_bytes =>
            {
                Err(CrucibleError::IoError(format!(
                    "extent {}: incomplete write \
                    (expected {expected_bytes}, got {num_bytes_written})",
                    self.extent_number
                )))
            }
            Ok(..) => Ok(()),
        };

        if r.is_err() {
            for write in writes.iter() {
                let block = write.offset.value;

                // Try to recompute the context slot from the file.  If this
                // fails, then we _really_ can't recover, so bail out
                // unceremoniously.
                self.recompute_block_written_from_file(block).unwrap();
            }
        } else {
            // Now that writes have gone through, mark as written
            self.block_written[start_block.value as usize..][..n_blocks]
                .fill(true);
        }
        cdt::extent__write__file__done!(|| {
            (job_id.0, self.extent_number, writes.len() as u64)
        });

        Ok(())
    }

    fn read_contiguous_into(
        &mut self,
        job_id: JobId,
        requests: &[crucible_protocol::ReadRequest],
        out: &mut RawReadResponse,
    ) -> Result<(), CrucibleError> {
        let block_size = self.extent_size.block_size_in_bytes();

        let mut buf = out.data.split_off(out.data.len());
        let n_blocks = requests.len();

        // Resizing the buffer should fill memory, but should not reallocate
        buf.resize(n_blocks * block_size as usize, 1u8);

        let start_block = requests[0].offset;
        let start_pos = start_block.value
            * (block_size as u64 + BLOCK_CONTEXT_SLOT_SIZE_BYTES);
        check_input(self.extent_size, start_block, &buf)?;

        let mut ctxs =
            vec![[0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize]; n_blocks];

        let mut iovecs = Vec::with_capacity(n_blocks * 2);
        for (ctx, chunk) in
            ctxs.iter_mut().zip(buf.chunks_mut(block_size as usize))
        {
            iovecs.push(IoSliceMut::new(ctx));
            iovecs.push(IoSliceMut::new(chunk));
        }

        let expected_bytes = n_blocks as u64
            * (block_size as u64 + BLOCK_CONTEXT_SLOT_SIZE_BYTES);

        // Finally we get to read the actual data. That's why we're here
        cdt::extent__read__file__start!(|| {
            (job_id.0, self.extent_number, n_blocks as u64)
        });
        let num_bytes_read = nix::sys::uio::preadv(
            self.file.as_fd(),
            &mut iovecs,
            start_pos as i64,
        )
        .map_err(|e| {
            CrucibleError::IoError(format!(
                "extent {}: read failed: {e}",
                self.extent_number
            ))
        })?;

        if num_bytes_read != expected_bytes as usize {
            return Err(CrucibleError::IoError(format!(
                "extent {}: incomplete read \
                 (expected {expected_bytes}, got {num_bytes_read})",
                self.extent_number
            )));
        }
        cdt::extent__read__file__done!(|| {
            (job_id.0, self.extent_number, n_blocks as u64)
        });

        for (i, c) in ctxs.into_iter().enumerate() {
            let ctx = bincode::deserialize(&c)
                .map_err(|e| CrucibleError::BadContextSlot(e.to_string()))?;
            out.blocks.push(ReadResponseBlockMetadata {
                eid: self.extent_number as u64,
                offset: Block {
                    value: start_block.value + i as u64,
                    ..start_block
                },
                block_contexts: vec![ctx],
            });
        }

        // Rejoin the data buffer (this should be zero-cost, because it was
        // split off at the beginning of the function and no allocations should
        // have happened)
        out.data.unsplit(buf);

        Ok(())
    }
}

/// Data structure that implements the on-disk layout of a raw extent file
struct RawLayout {
    extent_size: Block,
}

impl std::fmt::Debug for RawLayout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawLayout")
            .field("extent_size", &self.extent_size)
            .finish()
    }
}

impl RawLayout {
    fn new(extent_size: Block) -> Self {
        RawLayout { extent_size }
    }

    /// Sets the dirty flag in the file true
    ///
    /// This unconditionally writes to the file; to avoid extra syscalls, it
    /// would be wise to cache this at a higher level and only write if it has
    /// changed.
    fn set_dirty(&self, file: &File) -> Result<(), CrucibleError> {
        let offset = self.metadata_offset();
        pwrite_all(file.as_fd(), &[1u8], offset as i64).map_err(|e| {
            CrucibleError::IoError(format!("writing dirty byte failed: {e}",))
        })?;
        Ok(())
    }

    /// Returns the total size of the raw data file
    ///
    /// This includes block data, context slots, active slot array, and metadata
    fn file_size(&self) -> u64 {
        let block_count = self.block_count();
        (self.block_size() + BLOCK_CONTEXT_SLOT_SIZE_BYTES)
            .checked_mul(block_count)
            .unwrap()
            + (block_count + 7) / 8
            + BLOCK_META_SIZE_BYTES
    }

    /// Number of blocks in the extent file
    fn block_count(&self) -> u64 {
        self.extent_size.value
    }

    /// Returns the byte offset of the `block_written` bitpacked array
    fn block_written_array_offset(&self) -> u64 {
        self.block_count() * (self.block_size() + BLOCK_CONTEXT_SLOT_SIZE_BYTES)
    }

    /// Returns the size of the `block_written` bitpacked array, in bytes
    fn block_written_array_size(&self) -> u64 {
        (self.block_count() + 7) / 8
    }

    /// Returns the offset of the metadata chunk of the file
    fn metadata_offset(&self) -> u64 {
        self.block_written_array_offset() + self.block_written_array_size()
    }

    /// Number of bytes in each block
    fn block_size(&self) -> u64 {
        self.extent_size.block_size_in_bytes() as u64
    }

    fn get_metadata(&self, file: &File) -> Result<OnDiskMeta, CrucibleError> {
        let mut buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
        let offset = self.metadata_offset();
        pread_all(file.as_fd(), &mut buf, offset as i64).map_err(|e| {
            CrucibleError::IoError(format!("reading metadata failed: {e}"))
        })?;
        let out: OnDiskMeta = bincode::deserialize(&buf)
            .map_err(|e| CrucibleError::BadMetadata(e.to_string()))?;
        Ok(out)
    }

    /// Write out the metadata section of the file
    ///
    /// This is done in a single write, so it should be atomic.
    ///
    /// # Panics
    /// `block_written.len()` must match `self.block_count()`, and the function
    /// will panic otherwise.
    fn write_block_written_and_metadata(
        &self,
        file: &File,
        block_written: &[bool],
        dirty: bool,
        flush_number: u64,
        gen_number: u64,
    ) -> Result<(), CrucibleError> {
        assert_eq!(block_written.len(), self.block_count() as usize);

        let mut buf = vec![];
        for c in block_written.chunks(8) {
            let mut v = 0;
            for (i, w) in c.iter().enumerate() {
                v |= (*w as u8) << i;
            }
            buf.push(v);
        }

        let d = OnDiskMeta {
            dirty,
            flush_number,
            gen_number,
            ext_version: EXTENT_META_RAW_V2,
        };
        let mut meta = [0u8; BLOCK_META_SIZE_BYTES as usize];
        bincode::serialize_into(meta.as_mut_slice(), &d).unwrap();
        bincode::serialize_into(meta.as_mut_slice(), &d).unwrap();
        buf.extend(meta);

        let offset = self.block_written_array_offset();

        pwrite_all(file.as_fd(), &meta, offset as i64).map_err(|e| {
            CrucibleError::IoError(format!("writing metadata failed: {e}"))
        })?;

        Ok(())
    }

    /// Decodes the block written array from the given file
    ///
    /// The file descriptor offset is not changed by this function
    fn get_block_written_array(
        &self,
        file: &File,
    ) -> Result<Vec<bool>, CrucibleError> {
        let mut buf = vec![0u8; self.block_written_array_size() as usize];
        let offset = self.block_written_array_offset();
        pread_all(file.as_fd(), &mut buf, offset as i64).map_err(|e| {
            CrucibleError::IoError(format!(
                "could not read active contexts: {e}"
            ))
        })?;

        let mut block_written = vec![];
        for bit in buf
            .iter()
            .flat_map(|b| (0..8).map(move |i| b & (1 << i)))
            .take(self.block_count() as usize)
        {
            // Unpack bits from each byte
            block_written.push(bit != 0);
        }
        assert_eq!(block_written.len(), self.block_count() as usize);
        Ok(block_written)
    }
}

/// Call `pread` repeatedly to read an entire buffer
///
/// Quoth the standard,
///
/// > The value returned may be less than nbyte if the number of bytes left in
/// > the file is less than nbyte, if the read() request was interrupted by a
/// > signal, or if the file is a pipe or FIFO or special file and has fewer
/// > than nbyte bytes immediately available for reading. For example, a read()
/// > from a file associated with a terminal may return one typed line of data.
///
/// We don't have to worry about most of these conditions, but it may be
/// possible for Crucible to be interrupted by a signal, so let's play it safe.
fn pread_all<F: AsFd + Copy>(
    fd: F,
    mut buf: &mut [u8],
    mut offset: i64,
) -> Result<(), nix::errno::Errno> {
    while !buf.is_empty() {
        let n = nix::sys::uio::pread(fd, buf, offset)?;
        offset += n as i64;
        buf = &mut buf[n..];
    }
    Ok(())
}

/// Call `pwrite` repeatedly to write an entire buffer
///
/// See details for why this is necessary in [`pread_all`]
fn pwrite_all<F: AsFd + Copy>(
    fd: F,
    mut buf: &[u8],
    mut offset: i64,
) -> Result<(), nix::errno::Errno> {
    while !buf.is_empty() {
        let n = nix::sys::uio::pwrite(fd, buf, offset)?;
        offset += n as i64;
        buf = &buf[n..];
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use bytes::{Bytes, BytesMut};
    use crucible_common::integrity_hash;
    use crucible_protocol::BlockContext;
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
        inner.write(JobId(10), &[write], false, IOV_MAX_TEST)?;

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
            inner.write(JobId(20), &[write], true, IOV_MAX_TEST)?;

            let read = ReadRequest {
                eid: 0,
                offset: Block::new_512(0),
            };
            let resp = inner.read(JobId(21), &[read], IOV_MAX_TEST)?;

            // We should not get back our data, because block 0 was written.
            assert_ne!(
                resp.blocks,
                vec![ReadResponseBlockMetadata {
                    eid: 0,
                    offset: Block::new_512(0),
                    block_contexts: vec![block_context]
                }]
            );
            assert_ne!(resp.data, BytesMut::from(data.as_ref()));
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
            inner.write(JobId(30), &[write], true, IOV_MAX_TEST)?;

            let read = ReadRequest {
                eid: 0,
                offset: Block::new_512(1),
            };
            let resp = inner.read(JobId(31), &[read], IOV_MAX_TEST)?;

            // We should get back our data! Block 1 was never written.
            assert_eq!(
                resp.blocks,
                vec![ReadResponseBlockMetadata {
                    eid: 0,
                    offset: Block::new_512(1),
                    block_contexts: vec![block_context]
                }]
            );
            assert_eq!(resp.data, BytesMut::from(data.as_ref()));
        }

        Ok(())
    }

    #[test]
    fn test_serialized_sizes() {
        let ctx = BlockContext {
            hash: u64::MAX,
            encryption_context: Some(EncryptionContext {
                nonce: [0xFF; 12],
                tag: [0xFF; 16],
            }),
        };
        let mut ctx_buf = [0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
        bincode::serialize_into(ctx_buf.as_mut_slice(), &Some(ctx)).unwrap();

        let m = OnDiskMeta {
            dirty: true,
            gen_number: u64::MAX,
            flush_number: u64::MAX,
            ext_version: u32::MAX,
        };
        let mut meta_buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
        bincode::serialize_into(meta_buf.as_mut_slice(), &m).unwrap();
    }

    /// Test that multiple writes to the same location work
    #[test]
    fn test_multiple_writes_to_same_location_raw() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(dir.as_ref(), &new_region_definition(), 0)
                .unwrap();

        // Write the same block four times in the same write command.

        let writes: Vec<_> = (0..4)
            .map(|i| {
                let data = Bytes::from(vec![i as u8; 512]);
                let hash = integrity_hash(&[&data[..]]);

                crucible_protocol::Write {
                    eid: 0,
                    offset: Block::new_512(0),
                    data,
                    block_context: BlockContext {
                        encryption_context: None,
                        hash,
                    },
                }
            })
            .collect();

        inner.write(JobId(30), &writes, false, IOV_MAX_TEST)?;

        // The write should be split into four separate calls to
        // `write_without_overlaps`

        // Block 0 should be 0x03 repeated.
        let read = ReadRequest {
            eid: 0,
            offset: Block::new_512(0),
        };
        let resp = inner.read(JobId(31), &[read], IOV_MAX_TEST)?;

        let data = Bytes::from(vec![0x03; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let block_context = BlockContext {
            encryption_context: None,
            hash,
        };

        assert_eq!(
            resp.blocks,
            vec![ReadResponseBlockMetadata {
                eid: 0,
                offset: Block::new_512(0),
                // Only the most recent block context should be returned
                block_contexts: vec![block_context],
            }]
        );
        assert_eq!(resp.data, BytesMut::from(data.as_ref()));

        Ok(())
    }
}
