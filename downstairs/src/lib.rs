// Copyright 2023 Oxide Computer Company
#![cfg_attr(usdt_need_asm, feature(asm))]
#![cfg_attr(all(target_os = "macos", usdt_need_asm_sym), feature(asm_sym))]

use futures::lock::Mutex;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crucible::*;
use crucible_common::{build_logger, Block, CrucibleError, MAX_BLOCK_SIZE};

use anyhow::{bail, Result};
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use rand::prelude::*;
use slog::{debug, error, info, o, warn, Logger};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep_until, Instant};
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

pub mod admin;
mod dump;
mod dynamometer;
mod extent;
pub mod region;
pub mod repair;
mod stats;

mod extent_inner_raw;
mod extent_inner_sqlite;

use region::Region;

pub use admin::run_dropshot;
pub use dump::dump_region;
pub use dynamometer::*;
pub use stats::{DsCountStat, DsStatOuter};

fn deadline_secs(secs: u64) -> Instant {
    Instant::now()
        .checked_add(Duration::from_secs(secs))
        .unwrap()
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq)]
enum IOop {
    Write {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        writes: Vec<crucible_protocol::Write>,
    },
    WriteUnwritten {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        writes: Vec<crucible_protocol::Write>,
    },
    Read {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        requests: Vec<ReadRequest>,
    },
    Flush {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        flush_number: u64,
        gen_number: u64,
        snapshot_details: Option<SnapshotDetails>,
        extent_limit: Option<usize>,
    },
    /*
     * These operations are for repairing a bad downstairs
     */
    ExtentClose {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: usize,
    },
    ExtentFlushClose {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: usize,
        flush_number: u64,
        gen_number: u64,
        source_downstairs: ClientId,
        repair_downstairs: Vec<ClientId>,
    },
    ExtentLiveRepair {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: usize,
        source_downstairs: ClientId,
        source_repair_address: SocketAddr,
        repair_downstairs: Vec<ClientId>,
    },
    ExtentLiveReopen {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: usize,
    },
    ExtentLiveNoOp {
        dependencies: Vec<JobId>, // Jobs that must finish before this
    },
}

impl IOop {
    fn deps(&self) -> &[JobId] {
        match &self {
            IOop::Write { dependencies, .. }
            | IOop::Flush { dependencies, .. }
            | IOop::Read { dependencies, .. }
            | IOop::WriteUnwritten { dependencies, .. }
            | IOop::ExtentClose { dependencies, .. }
            | IOop::ExtentFlushClose { dependencies, .. }
            | IOop::ExtentLiveRepair { dependencies, .. }
            | IOop::ExtentLiveReopen { dependencies, .. }
            | IOop::ExtentLiveNoOp { dependencies } => dependencies,
        }
    }
}

/*
 * Export the contents or partial contents of a Downstairs Region to
 * the file indicated.
 *
 * We will start from the provided start_block.
 * We will stop after "count" blocks are written to the export_path.
 */
pub async fn downstairs_export<P: AsRef<Path> + std::fmt::Debug>(
    region: &mut Region,
    export_path: P,
    start_block: u64,
    mut count: u64,
) -> Result<()> {
    /*
     * Export an existing downstairs region to a file
     */
    let (block_size, extent_size, extent_count) = region.region_def();
    let space_per_extent = extent_size.byte_value();
    assert!(block_size > 0);
    assert!(space_per_extent > 0);
    assert!(extent_count > 0);
    assert!(space_per_extent > 0);
    let file_size = space_per_extent * extent_count as u64;

    if count == 0 {
        count = extent_size.value * extent_count as u64;
    }

    println!(
        "Export total_size: {}  Extent size:{}  Total Extents:{}",
        file_size, space_per_extent, extent_count
    );
    println!(
        "Exporting from start_block: {}  count:{}",
        start_block, count
    );

    let mut out_file = File::create(export_path)?;
    let mut blocks_copied = 0;

    'eid_loop: for eid in 0..extent_count {
        let extent_offset = space_per_extent * eid as u64;
        for block_offset in 0..extent_size.value {
            if (extent_offset + block_offset) >= start_block {
                blocks_copied += 1;

                let mut responses = region
                    .region_read(
                        &[ReadRequest {
                            eid: eid as u64,
                            offset: Block::new_with_ddef(
                                block_offset,
                                &region.def(),
                            ),
                        }],
                        JobId(0),
                    )
                    .await?;
                let response = responses.pop().unwrap();

                out_file.write_all(&response.data).unwrap();

                if blocks_copied >= count {
                    break 'eid_loop;
                }
            }
        }
    }

    println!("Read and wrote out {} blocks", blocks_copied);

    Ok(())
}

/*
 * Import the contents of a file into a new Region.
 * The total size of the region will be rounded up to the next largest
 * extent multiple.
 */
pub async fn downstairs_import<P: AsRef<Path> + std::fmt::Debug>(
    region: &mut Region,
    import_path: P,
) -> Result<()> {
    /*
     * Open the file to import and determine how many extents we will need
     * based on the length.
     */
    let mut f = File::open(&import_path)?;
    let file_size = f.metadata()?.len();
    let (_, extent_size, _) = region.region_def();
    let space_per_extent = extent_size.byte_value();

    let mut extents_needed = file_size / space_per_extent;
    if file_size % space_per_extent != 0 {
        extents_needed += 1;
    }
    println!(
        "Import file_size: {}  Extent size: {}  Needed extents: {}",
        file_size, space_per_extent, extents_needed
    );

    if extents_needed > region.def().extent_count().into() {
        /*
         * The file to import would require more extents than we have.
         * Extend the region to fit the file.
         */
        println!("Extending region to fit image");
        region.extend(extents_needed as u32).await?;
    } else {
        println!("Region already large enough for image");
    }

    println!("Importing {:?} to region", import_path);
    let rm = region.def();

    /*
     * We want to read and write large chunks of data, rather than individual
     * blocks, to improve import performance.  The chunk buffer must be a
     * whole number of the largest block size we are able to support.
     */
    const CHUNK_SIZE: usize = 32 * 1024 * 1024;
    assert_eq!(CHUNK_SIZE % MAX_BLOCK_SIZE, 0);

    let mut offset = Block::new_with_ddef(0, &region.def());
    loop {
        let mut buffer = vec![0; CHUNK_SIZE];

        /*
         * Read data into the buffer until it is full, or we hit EOF.
         */
        let mut total = 0;
        loop {
            assert!(total <= CHUNK_SIZE);
            if total == CHUNK_SIZE {
                break;
            }

            /*
             * Rust's read guarantees that if it returns Ok(n) then
             * `0 <= n <= buffer.len()`. We have to repeatedly read until our
             * buffer is full.
             */
            let n = f.read(&mut buffer[total..])?;

            if n == 0 {
                /*
                 * We have hit EOF.  Extend the read buffer with zeroes until
                 * it is a multiple of the block size.
                 */
                while !Block::is_valid_byte_size(total, &rm) {
                    buffer[total] = 0;
                    total += 1;
                }
                break;
            }

            total += n;
        }

        if total == 0 {
            /*
             * If we read zero bytes without error, then we are done.
             */
            break;
        }

        /*
         * Use the same function upstairs uses to decide where to put the
         * data based on the LBA offset.
         */
        let nblocks = Block::from_bytes(total, &rm);
        let mut pos = Block::from_bytes(0, &rm);
        let mut writes = vec![];
        for (eid, offset) in
            extent_from_offset(&rm, offset, nblocks).blocks(&rm)
        {
            let len = Block::new_with_ddef(1, &region.def());
            let data = &buffer[pos.bytes()..(pos.bytes() + len.bytes())];
            let mut buffer = BytesMut::with_capacity(data.len());
            buffer.resize(data.len(), 0);
            buffer.copy_from_slice(data);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data: buffer.freeze(),
                block_context: BlockContext {
                    hash: integrity_hash(&[data]),
                    encryption_context: None,
                },
            });

            pos.advance(len);
        }

        // We have no job ID, so it makes no sense for accounting.
        region.region_write(&writes, JobId(0), false).await?;

        assert_eq!(nblocks, pos);
        assert_eq!(total, pos.bytes());
        offset.advance(nblocks);
    }

    /*
     * As there is no EOF indication in the downstairs, print the
     * number of total blocks we wrote to so the caller can, if they
     * want, use that to extract just this imported file.
     */
    println!(
        "Populated {} extents by copying {} bytes ({} blocks)",
        extents_needed,
        offset.byte_value(),
        offset.value,
    );

    Ok(())
}

/*
 * Debug function to dump the work list.
 */
pub fn show_work(ds: &mut Downstairs) {
    let active_upstairs_connections = ds.active_upstairs.keys();
    println!(
        "Active Upstairs connections: {:?}",
        active_upstairs_connections
    );

    for upstairs_connection in ds.active_upstairs.values() {
        let Some(work) = ds.connections[upstairs_connection].work() else {
            continue;
        };

        let mut kvec: Vec<JobId> = work.active.keys().cloned().collect();

        if kvec.is_empty() {
            println!("Crucible Downstairs work queue:  Empty");
        } else {
            println!("Crucible Downstairs work queue:");
            kvec.sort_unstable();
            for id in kvec.iter() {
                let dsw = work.active.get(id).unwrap();
                let (dsw_type, dep_list) = match &dsw.work {
                    IOop::Read { dependencies, .. } => ("Read", dependencies),
                    IOop::Write { dependencies, .. } => ("Write", dependencies),
                    IOop::Flush { dependencies, .. } => ("Flush", dependencies),
                    IOop::WriteUnwritten { dependencies, .. } => {
                        ("WriteU", dependencies)
                    }
                    IOop::ExtentClose { dependencies, .. } => {
                        ("EClose", dependencies)
                    }
                    IOop::ExtentFlushClose { dependencies, .. } => {
                        ("EFClose", dependencies)
                    }
                    IOop::ExtentLiveRepair { dependencies, .. } => {
                        ("Repair", dependencies)
                    }
                    IOop::ExtentLiveReopen { dependencies, .. } => {
                        ("ReOpen", dependencies)
                    }
                    IOop::ExtentLiveNoOp { dependencies } => {
                        ("NoOp", dependencies)
                    }
                };
                println!(
                    "DSW:[{:04}] {:>07} {:>05} deps:{:?}",
                    id, dsw_type, dsw.state, dep_list,
                );
            }
        }

        println!("Done tasks {:?}", work.completed);
        println!("last_flush: {:?}", work.last_flush);
        println!("--------------------------------------");
    }
}

// DTrace probes for the downstairs
#[usdt::provider(provider = "crucible_downstairs")]
pub mod cdt {
    use crate::Arg;
    fn submit__read__start(_: u64) {}
    fn submit__writeunwritten__start(_: u64) {}
    fn submit__write__start(_: u64) {}
    fn submit__flush__start(_: u64) {}
    fn submit__el__close__start(_: u64) {}
    fn submit__el__flush__close__start(_: u64) {}
    fn submit__el__repair__start(_: u64) {}
    fn submit__el__reopen__start(_: u64) {}
    fn submit__el__noop__start(_: u64) {}
    fn work__start(_: u64) {}
    fn os__read__start(_: u64) {}
    fn os__writeunwritten__start(_: u64) {}
    fn os__write__start(_: u64) {}
    fn os__flush__start(_: u64) {}
    fn work__process(_: u64) {}
    fn os__read__done(_: u64) {}
    fn os__writeunwritten__done(_: u64) {}
    fn os__write__done(_: u64) {}
    fn os__flush__done(_: u64) {}
    fn submit__read__done(_: u64) {}
    fn submit__writeunwritten__done(_: u64) {}
    fn submit__write__done(_: u64) {}
    fn submit__flush__done(_: u64) {}
    fn extent__flush__start(job_id: u64, extent_id: u32, extent_size: u64) {}
    fn extent__flush__done(job_id: u64, extent_id: u32, extent_size: u64) {}
    fn extent__flush__file__start(
        job_id: u64,
        extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__flush__file__done(
        job_id: u64,
        extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__flush__collect__hashes__start(
        job_id: u64,
        extent_id: u32,
        num_dirty: u64,
    ) {
    }
    fn extent__flush__collect__hashes__done(
        job_id: u64,
        extent_id: u32,
        num_rehashed: u64,
    ) {
    }
    fn extent__flush__sqlite__insert__start(
        job_id: u64,
        extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__flush__sqlite__insert__done(
        _job_id: u64,
        _extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__write__start(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__write__done(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__write__get__hashes__start(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__write__get__hashes__done(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__write__file__start(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__write__file__done(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__write__sqlite__insert__start(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__write__sqlite__insert__done(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__write__raw__context__insert__start(
        job_id: u64,
        extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__write__raw__context__insert__done(
        _job_id: u64,
        _extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__read__start(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__read__done(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__read__get__contexts__start(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__read__get__contexts__done(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__read__file__start(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__read__file__done(job_id: u64, extent_id: u32, n_blocks: u64) {}

    fn extent__context__truncate__start(n_deletions: u64) {}
    fn extent__context__truncate__done() {}
    fn extent__set__block__contexts__write__count(
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn submit__el__close__done(_: u64) {}
    fn submit__el__flush__close__done(_: u64) {}
    fn submit__el__repair__done(_: u64) {}
    fn submit__el__reopen__done(_: u64) {}
    fn submit__el__noop__done(_: u64) {}
    fn work__done(_: u64) {}
}

// Check and see if this message is A LiveRepair, and if it has failed. If you
// change this, change how the Upstairs processes ErrorReports!
fn check_message_for_abort(m: &Message) -> bool {
    if let Message::ExtentLiveRepairAckId { result, .. } = m {
        if result.is_err() {
            return true;
        }
    }

    false
}

async fn socket_io_task(
    connection_id: ConnectionId,
    stream: WrappedStream,
    tx_to_main_task: mpsc::Sender<(Message, ConnectionId)>,
    rx_from_main_task: mpsc::Receiver<Message>,
    log: Logger,
) {
    match stream {
        WrappedStream::Http(sock) => {
            let (read, write) = sock.into_split();

            let fr = FramedRead::new(read, CrucibleDecoder::new());
            let fw = FramedWrite::new(write, CrucibleEncoder::new());

            socket_io_loop(
                connection_id,
                fr,
                fw,
                tx_to_main_task,
                rx_from_main_task,
                log,
            )
            .await
        }
        WrappedStream::Https(stream) => {
            let (read, write) = tokio::io::split(stream);

            let fr = FramedRead::new(read, CrucibleDecoder::new());
            let fw = FramedWrite::new(write, CrucibleEncoder::new());

            socket_io_loop(
                connection_id,
                fr,
                fw,
                tx_to_main_task,
                rx_from_main_task,
                log,
            )
            .await
        }
    }
}

/// Handles IO to and from the socket
///
/// Under the hood, this spawns a separate task to receive data from the socket,
/// to avoid blocking if either direction gets too clogged.
async fn socket_io_loop<RT, WT>(
    connection_id: ConnectionId,
    mut fr: FramedRead<RT, CrucibleDecoder>,
    mut fw: FramedWrite<WT, CrucibleEncoder>,
    tx_to_main_task: mpsc::Sender<(Message, ConnectionId)>,
    mut rx_from_main_task: mpsc::Receiver<Message>,
    log: Logger,
) where
    RT: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send + 'static,
    WT: tokio::io::AsyncWrite
        + std::marker::Unpin
        + std::marker::Send
        + 'static,
{
    let rx_log = log.new(o!("role" => "rx".to_string()));
    let mut rx_task = tokio::task::spawn(async move {
        loop {
            while let Some(m) = fr.next().await {
                let m = match m {
                    Ok(m) => m,
                    Err(e) => {
                        error!(rx_log, "got invalid message: {e}");
                        break;
                    }
                };

                if let Err(e) = tx_to_main_task.send((m, connection_id)).await {
                    error!(rx_log, "failed to send message to main task: {e}");
                    break;
                }
            }
        }
    });

    loop {
        tokio::select! {
            _ = &mut rx_task => {
                // rx task aborted, so time to close up shop
                warn!(log, "rx task stopped, tx task is exiting too");
                break;
            }
            m = rx_from_main_task.recv() => {
                let Some(m) = m else {
                    warn!(log, "rx_from_main_task closed, exiting");
                    break;
                };
                if let Err(e) = fw.send(m).await {
                    error!(log, "could not send packet: {e}");
                    break;
                }
            }
        }
    }

    rx_task.abort(); // no-op if the task already stopped
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UpstairsConnection {
    upstairs_id: Uuid,
    session_id: Uuid,
    gen: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum NegotiationState {
    Start,
    ConnectedToUpstairs,
    PromotedToActive,
    SentRegionInfo,
}

#[derive(Debug)]
enum UpstairsState {
    Negotiating {
        negotiated: NegotiationState,
        upstairs_connection: Option<UpstairsConnection>,
    },
    Running {
        work: Work,
        upstairs_connection: UpstairsConnection,
    },
}

impl UpstairsState {
    fn upstairs_connection(&self) -> Option<&UpstairsConnection> {
        match self {
            UpstairsState::Negotiating {
                upstairs_connection,
                ..
            } => upstairs_connection.as_ref(),
            UpstairsState::Running {
                upstairs_connection,
                ..
            } => Some(upstairs_connection),
        }
    }
}

/// Represents a single Upstairs connection
#[derive(Debug)]
struct Upstairs {
    /// Current state
    state: UpstairsState,

    /// Handle to send messages to the dedicated IO thread
    ///
    /// (messages are received on the shared `Downstairs` channel)
    message_tx: mpsc::Sender<Message>,

    /// IO worker task
    ///
    /// This should not join until we exit; if it does join, it indicates an
    /// error and that this Upstairs connection should be dropped.
    io_task: tokio::task::JoinHandle<()>,

    /// Deadline at which point we declare this Upstairs dead
    timeout_deadline: tokio::time::Instant,

    log: Logger,
}

impl Upstairs {
    /// Returns a reference to the upstairs connection
    ///
    /// # Panics
    /// If this upstairs is still negotiating
    fn upstairs_connection(&self) -> UpstairsConnection {
        *self.state.upstairs_connection().unwrap()
    }

    /// Returns a mutable reference to the work map for a running Upstairs
    fn work_mut(&mut self) -> Option<&mut Work> {
        if let UpstairsState::Running { work, .. } = &mut self.state {
            Some(work)
        } else {
            None
        }
    }

    /// Returns an immutable reference to the work map for a running Upstairs
    ///
    /// # Panics
    /// If the upstairs is still negotiating
    fn work(&self) -> Option<&Work> {
        if let UpstairsState::Running { work, .. } = &self.state {
            Some(work)
        } else {
            None
        }
    }

    /// Checks whether the given message IDs are valid
    ///
    /// Otherwise, sends a `UuidMismatch` on the channel and returns `false`
    ///
    /// Returns an error if we can't send anything on the channel.
    async fn is_message_valid(
        &mut self,
        upstairs_id: Uuid,
        session_id: Uuid,
    ) -> Result<bool> {
        let Some(upstairs_connection) = self.state.upstairs_connection() else {
            panic!("called is_message_valid without connection");
        };
        if upstairs_connection.upstairs_id != upstairs_id {
            self.message_tx
                .send(Message::UuidMismatch {
                    expected_id: upstairs_connection.upstairs_id,
                })
                .await?;
            Ok(false)
        } else if upstairs_connection.session_id != session_id {
            self.message_tx
                .send(Message::UuidMismatch {
                    expected_id: upstairs_connection.session_id,
                })
                .await?;
            Ok(false)
        } else {
            Ok(true)
        }
    }
}

/// Internal unique ID for each new connection
#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq)]
struct ConnectionId(u64);

/// Overall structure for things the downstairs is tracking.
/// This includes the extents and their status as well as the
/// downstairs work queue.
#[derive(Debug)]
pub struct Downstairs {
    region: Region,

    /// Test flag, enables pauses and skipped jobs
    lossy: bool,
    /// Test flag, enables random read errors
    read_errors: bool,
    /// Test flag, enables random write errors
    write_errors: bool,
    /// Test flag, enables random flush errors
    flush_errors: bool,

    /// All active connections
    ///
    /// Many Upstairs may be connected to a single Downstairs simultaneously
    /// (and in various states of negotiation).
    connections: HashMap<ConnectionId, Upstairs>,

    /// Active upstairs, as a map from upstairs IDs to the `connections` map
    active_upstairs: HashMap<Uuid, ConnectionId>,

    next_connection_id: u64,

    dss: DsStatOuter,
    read_only: bool,
    encrypted: bool,

    address: Option<SocketAddr>,
    repair_address: Option<SocketAddr>,

    /// Queue to receive new messages, tagged with their connection
    message_rx: mpsc::Receiver<(Message, ConnectionId)>,

    /// `Sender` counterpart to `message_rx`, stored here to pass into IO tasks
    message_tx_handle: mpsc::Sender<(Message, ConnectionId)>,

    /// Queue to receive new connections
    connection_rx: mpsc::Receiver<WrappedStream>,

    /// Marks whether `connection_rx` is still receiving values
    ///
    /// If not, then we can never receive an `Action::NewConnection`, and can
    /// safely exit if all other connections are closed.
    has_connection_rx: bool,

    log: Logger,
}

/// IO handle to communicate with the downstairs
pub struct DownstairsIoHandle {
    stream_tx: mpsc::Sender<WrappedStream>,
}

enum Action {
    /// The socket has accepted a new connection
    NewConnection(WrappedStream),

    /// The given connection timed out
    Timeout(ConnectionId),

    /// We received a message on the given connection
    Message(Message, ConnectionId),

    /// An invalid event was received and should be ignored
    ConnectionChannelClosed,

    /// The IO task stopped for the given connection
    IoTaskStopped(ConnectionId),
}

impl Downstairs {
    #[allow(clippy::too_many_arguments)]
    fn new(
        region: Region,
        lossy: bool,
        read_errors: bool,
        write_errors: bool,
        flush_errors: bool,
        read_only: bool,
        encrypted: bool,
        log: Logger,
    ) -> (Self, DownstairsIoHandle) {
        let dss = DsStatOuter {
            ds_stat_wrap: Arc::new(std::sync::Mutex::new(DsCountStat::new(
                region.def().uuid(),
            ))),
        };
        let (connection_tx, connection_rx) = mpsc::channel(500);
        let (message_tx, message_rx) = mpsc::channel(500);
        (
            Downstairs {
                region,
                lossy,
                read_errors,
                write_errors,
                flush_errors,
                active_upstairs: HashMap::new(),
                connections: HashMap::new(),
                dss,
                read_only,
                encrypted,
                address: None,
                repair_address: None,
                next_connection_id: 0,
                message_tx_handle: message_tx,
                message_rx,
                connection_rx,
                has_connection_rx: true,
                log,
            },
            DownstairsIoHandle {
                stream_tx: connection_tx,
            },
        )
    }

    /// Selects from many possible actions
    async fn select(&mut self) -> Action {
        let mut up_actions = futures::stream::FuturesUnordered::new();
        let has_connections = !self.connections.is_empty();
        for (&id, up) in &mut self.connections {
            up_actions.push(async move {
                tokio::select! {
                    _ = sleep_until(up.timeout_deadline) => {
                        Action::Timeout(id)
                    }
                    _ = &mut up.io_task => {
                        Action::IoTaskStopped(id)
                    }
                }
            });
        }

        tokio::select! {
            r = self.message_rx.recv() => {
                if let Some((message, id)) = r {
                    Action::Message(message, id)
                } else {
                    // This should never occur, even on exit (the usual time
                    // when such preconditions fail), because we hold a copy of
                    // the sender in **this very `Downstairs`**.
                    panic!("message_rx should never be dropped")
                }
            }
            w = self.connection_rx.recv(), if self.has_connection_rx => {
                if let Some(w) = w {
                    Action::NewConnection(w)
                } else {
                    warn!(self.log, "connection_tx was dropped");
                    Action::ConnectionChannelClosed
                }
            }
            a = up_actions.next(), if has_connections => {
                a.unwrap()
            }
        }
    }

    async fn apply(&mut self, action: Action) {
        match action {
            Action::NewConnection(stream) => {
                let connection_id = self.next_connection_id();
                let tx_to_main_task = self.message_tx_handle.clone();
                let (tx, rx) = mpsc::channel(500);
                let io_task = tokio::task::spawn(socket_io_task(
                    connection_id,
                    stream,
                    tx_to_main_task,
                    rx,
                    self.log.new(o!("conn" => format!("{}", connection_id.0))),
                ));
                self.connections.insert(
                    connection_id,
                    Upstairs {
                        state: UpstairsState::Negotiating {
                            negotiated: NegotiationState::Start,
                            upstairs_connection: None,
                        },
                        message_tx: tx,
                        io_task,
                        timeout_deadline: deadline_secs(5),
                        log: self.log.new(
                            o!("upstairs" => format!("{}", connection_id.0)),
                        ),
                    },
                );

                // spawn IO task
            }
            Action::Message(m, id) => {
                let r = self.handle_message(m, id).await;
                if let Err(e) = r {
                    warn!(self.log, "got error {e:?} while handling message");
                    self.drop_connection(id);
                }
            }
            Action::Timeout(id) => {
                warn!(self.log, "got timeout from connection {id:?}");
                self.drop_connection(id);
            }
            Action::IoTaskStopped(id) => {
                warn!(self.log, "IO task for connection {id:?} stopped");
                self.drop_connection(id);
            }
            Action::ConnectionChannelClosed => {
                // The main socket listener has stopped listening.  RIP.
                self.has_connection_rx = false;
            }
        }

        // Now, perform all pending IO work
        self.do_io_work().await;
    }

    /// Drops the given connection
    ///
    /// This means that all pending work from that connection is discarded, and
    /// the sockets to and from that Upstairs are closed.
    fn drop_connection(&mut self, id: ConnectionId) {
        let up = self.connections.remove(&id).unwrap();
        warn!(up.log, "removing connection {id:?}");

        if let Some(uc) = up.state.upstairs_connection() {
            warn!(
                up.log,
                "removing possibly-active upstairs for {id:?}: {uc:?}"
            );
            self.active_upstairs.remove(&uc.upstairs_id);
        }

        if let UpstairsState::Running {
            work,
            upstairs_connection,
        } = up.state
        {
            warn!(
                up.log,
                "dropping {} active jobs from {upstairs_connection:?}",
                work.active.len()
            );
        }

        // At this point, things happen automatically when `up` is dropped:
        //
        // - Dropping the `Upstairs` destroys the single `mpsc::Sender` that the
        //   IO task is listening on
        // - This causes the IO tx task to exit
        // - Upon exiting, the IO tx task also aborts the IO rx task
    }

    /// Runs the downstairs (forever)
    async fn run(&mut self) {
        while !self.done() {
            let action = self.select().await;
            self.apply(action).await
        }
    }

    /// Returns `true` if the main worker task can stop
    ///
    /// This is only true if we have lost our connection to the socket listener
    /// and have no active connections remaining, which means that we can never
    /// get another.
    fn done(&self) -> bool {
        !self.has_connection_rx
            && self.active_upstairs.is_empty()
            && self.connections.is_empty()
    }

    /// Returns the next valid connection ID, bumping our internal counter
    ///
    /// Connection IDs are never reused, since a `u64` will not roll over under
    /// even extraordinary circumstances.
    fn next_connection_id(&mut self) -> ConnectionId {
        let i = self.next_connection_id;
        self.next_connection_id += 1;
        ConnectionId(i)
    }

    /// Processes a single message coming from a particular upstairs
    ///
    /// Returns `Ok(())` if the message was handled, and `Err(..)` if something
    /// went wrong and we should bail out of this connection.
    async fn handle_message(
        &mut self,
        m: Message,
        target: ConnectionId,
    ) -> Result<()> {
        let Some(up) = self.connections.get_mut(&target) else {
            warn!(
                self.log,
                "received message on dropped connection {target:?}; ignoring"
            );
            return Ok(()); // the connection isn't present, so we can't drop it
        };
        match &mut up.state {
            UpstairsState::Negotiating { .. } => {
                let r = self.continue_negotiation(m, target).await;
                if let Err(e) = &r {
                    let up = &self.connections[&target];
                    error!(up.log, "negotiation failed: {e:?}");
                }
                r
            }
            UpstairsState::Running { .. } => {
                if matches!(m, Message::Ruok) {
                    // Respond instantly to pings, don't wait.
                    if let Err(e) = up.message_tx.send(Message::Imok).await {
                        bail!("Failed sending Imok: {}", e);
                    }
                } else if let Err(e) = self.proc_frame(m, target).await {
                    bail!("Failed sending message to proc_frame: {}", e);
                }
                Ok(())
            }
        }
    }

    async fn continue_negotiation(
        &mut self,
        m: Message,
        target: ConnectionId,
    ) -> Result<()> {
        let up = self.connections.get_mut(&target).unwrap();
        let UpstairsState::Negotiating {
            negotiated,
            upstairs_connection,
        } = &mut up.state
        else {
            panic!("invalid state");
        };

        match m {
            Message::Ruok => {
                if let Err(e) = up.message_tx.send(Message::Imok).await {
                    error!(up.log, "Failed to answer ping: {}", e);
                }
            }
            Message::HereIAm {
                version,
                upstairs_id,
                session_id,
                gen,
                read_only,
                encrypted,
                alternate_versions,
            } => {
                if *negotiated != NegotiationState::Start {
                    bail!("Received connect out of order {:?}", negotiated);
                }
                info!(
                    self.log,
                    "Connection request from {} with version {}",
                    upstairs_id,
                    version
                );

                // Verify we can communicate with the upstairs.  First
                // check our message version.  If that fails,  check
                // to see if our version is one of the supported
                // versions the upstairs has told us it can support.
                if version != CRUCIBLE_MESSAGE_VERSION {
                    if alternate_versions.contains(&CRUCIBLE_MESSAGE_VERSION) {
                        warn!(
                            self.log,
                            "downstairs and upstairs using different \
                                     but compatible versions, Upstairs is {}, \
                                     but supports {:?}, downstairs is {}",
                            version,
                            alternate_versions,
                            CRUCIBLE_MESSAGE_VERSION,
                        );
                    } else {
                        let m = Message::VersionMismatch {
                            version: CRUCIBLE_MESSAGE_VERSION,
                        };
                        if let Err(e) = up.message_tx.send(m).await {
                            warn!(
                                self.log,
                                "Failed to send VersionMismatch: {}", e
                            );
                        }
                        bail!(
                            "Required version {}, Or {:?} got {}",
                            CRUCIBLE_MESSAGE_VERSION,
                            alternate_versions,
                            version,
                        );
                    }
                }

                // Reject an Upstairs negotiation if there is a mismatch
                // of expectation, and terminate the connection - the
                // Upstairs will not be able to successfully negotiate.
                if self.read_only != read_only {
                    if let Err(e) = up
                        .message_tx
                        .send(Message::ReadOnlyMismatch {
                            expected: self.read_only,
                        })
                        .await
                    {
                        warn!(
                            self.log,
                            "Failed to send ReadOnlyMismatch: {}", e
                        );
                    }

                    bail!("closing connection due to read-only mismatch");
                }

                if self.encrypted != encrypted {
                    if let Err(e) = up
                        .message_tx
                        .send(Message::EncryptedMismatch {
                            expected: self.encrypted,
                        })
                        .await
                    {
                        warn!(
                            up.log,
                            "Failed to send EncryptedMismatch: {}", e
                        );
                    }

                    bail!("closing connection due to encryption mismatch");
                }

                *negotiated = NegotiationState::ConnectedToUpstairs;
                *upstairs_connection = Some(UpstairsConnection {
                    upstairs_id,
                    session_id,
                    gen,
                });
                info!(
                    up.log,
                    "upstairs {:?} connected, version {}",
                    upstairs_connection.unwrap(),
                    CRUCIBLE_MESSAGE_VERSION
                );

                if let Err(e) = up
                    .message_tx
                    .send(Message::YesItsMe {
                        version: CRUCIBLE_MESSAGE_VERSION,
                        repair_addr: self.repair_address.unwrap(),
                    })
                    .await
                {
                    bail!("Failed sending YesItsMe: {}", e);
                }
            }
            Message::PromoteToActive {
                upstairs_id,
                session_id,
                gen,
            } => {
                if *negotiated != NegotiationState::ConnectedToUpstairs {
                    bail!("Received activate out of order {negotiated:?}",);
                }

                // Only allowed to promote or demote self
                let mut upstairs_connection = upstairs_connection.unwrap();
                let matches_self = upstairs_connection.upstairs_id
                    == upstairs_id
                    && upstairs_connection.session_id == session_id;

                if !matches_self {
                    if let Err(e) = up
                        .message_tx
                        .send(Message::UuidMismatch {
                            expected_id: upstairs_connection.upstairs_id,
                        })
                        .await
                    {
                        warn!(up.log, "Failed sending UuidMismatch: {}", e);
                    }
                    bail!(
                        "Upstairs connection expected \
                         upstairs_id:{} session_id:{}; received \
                         upstairs_id:{} session_id:{}",
                        upstairs_connection.upstairs_id,
                        upstairs_connection.session_id,
                        upstairs_id,
                        session_id
                    );
                } else {
                    if upstairs_connection.gen != gen {
                        warn!(
                            up.log,
                            "warning: generation number at \
                             negotiation was {} and {} at \
                             activation, updating",
                            upstairs_connection.gen,
                            gen,
                        );

                        upstairs_connection.gen = gen;
                    }

                    let r = self.promote_to_active(target).await;

                    // Reborrow `up` and `negotiated`, which had to be
                    // unborrowed during `promote_to_active`
                    let up = self.connections.get_mut(&target).unwrap();
                    if let Err(e) = r {
                        bail!("promoting to active failed: {e:?}");
                    }

                    let UpstairsState::Negotiating { negotiated, .. } =
                        &mut up.state
                    else {
                        panic!("invalid up.state");
                    };
                    *negotiated = NegotiationState::PromotedToActive;

                    if let Err(e) = up
                        .message_tx
                        .send(Message::YouAreNowActive {
                            upstairs_id,
                            session_id,
                            gen,
                        })
                        .await
                    {
                        bail!("Failed sending YouAreNewActive: {e}");
                    }
                }
            }
            Message::RegionInfoPlease => {
                if *negotiated != NegotiationState::PromotedToActive {
                    bail!("Received RegionInfo out of order {:?}", negotiated);
                }
                *negotiated = NegotiationState::SentRegionInfo;
                let region_def = self.region.def();

                if let Err(e) =
                    up.message_tx.send(Message::RegionInfo { region_def }).await
                {
                    bail!("Failed sending RegionInfo: {}", e);
                }
            }
            Message::LastFlush { last_flush_number } => {
                if *negotiated != NegotiationState::SentRegionInfo {
                    bail!("Received LastFlush out of order {:?}", negotiated);
                }

                let mut work = Work::new();
                work.last_flush = last_flush_number;

                // Consume the Negotiating state; we're now running
                up.state = UpstairsState::Running {
                    work,
                    upstairs_connection: upstairs_connection.take().unwrap(),
                };
                info!(up.log, "Set last flush {}", last_flush_number);

                if let Err(e) = up
                    .message_tx
                    .send(Message::LastFlushAck { last_flush_number })
                    .await
                {
                    bail!("Failed sending LastFlushAck: {}", e);
                }

                /*
                 * Once this command is sent, we are ready to exit
                 * the loop and move forward with receiving IOs
                 */
            }
            Message::ExtentVersionsPlease => {
                if *negotiated != NegotiationState::SentRegionInfo {
                    bail!(
                        "Received ExtentVersions out of order {:?}",
                        negotiated
                    );
                }

                up.state = UpstairsState::Running {
                    work: Work::new(), // last flush?
                    upstairs_connection: upstairs_connection.take().unwrap(),
                };
                let meta_info = match self.region.meta_info().await {
                    Ok(meta_info) => meta_info,
                    Err(e) => {
                        bail!("could not get meta info: {e}");
                    }
                };

                let flush_numbers: Vec<_> =
                    meta_info.iter().map(|m| m.flush_number).collect();
                let gen_numbers: Vec<_> =
                    meta_info.iter().map(|m| m.gen_number).collect();
                let dirty_bits: Vec<_> =
                    meta_info.iter().map(|m| m.dirty).collect();
                if flush_numbers.len() > 12 {
                    info!(
                        up.log,
                        "Current flush_numbers [0..12]: {:?}",
                        &flush_numbers[0..12]
                    );
                } else {
                    info!(
                        up.log,
                        "Current flush_numbers [0..12]: {:?}", flush_numbers
                    );
                }

                if let Err(e) = up
                    .message_tx
                    .send(Message::ExtentVersions {
                        gen_numbers,
                        flush_numbers,
                        dirty_bits,
                    })
                    .await
                {
                    bail!("Failed sending ExtentVersions: {}", e);
                }

                /*
                 * Once this command is sent, we are ready to exit
                 * the loop and move forward with receiving IOs
                 */
            }
            _msg => {
                warn!(up.log, "Ignored message received during negotiation");
            }
        }
        Ok(())
    }

    async fn proc_frame(
        &mut self,
        m: Message,
        target: ConnectionId,
    ) -> Result<()> {
        let up = self.connections.get_mut(&target).unwrap();

        let new_ds_id = match m {
            Message::Write {
                upstairs_id,
                session_id,
                job_id,
                dependencies,
                writes,
            } => {
                if !up.is_message_valid(upstairs_id, session_id).await? {
                    return Ok(());
                }
                cdt::submit__write__start!(|| job_id.0);

                let new_write = IOop::Write {
                    dependencies,
                    writes,
                };

                self.add_work(target, job_id, new_write)?;
                Some(job_id)
            }
            Message::Flush {
                upstairs_id,
                session_id,
                job_id,
                dependencies,
                flush_number,
                gen_number,
                snapshot_details,
                extent_limit,
            } => {
                if !up.is_message_valid(upstairs_id, session_id).await? {
                    return Ok(());
                }
                cdt::submit__flush__start!(|| job_id.0);

                let new_flush = IOop::Flush {
                    dependencies,
                    flush_number,
                    gen_number,
                    snapshot_details,
                    extent_limit,
                };

                self.add_work(target, job_id, new_flush)?;
                Some(job_id)
            }
            Message::WriteUnwritten {
                upstairs_id,
                session_id,
                job_id,
                dependencies,
                writes,
            } => {
                if !up.is_message_valid(upstairs_id, session_id).await? {
                    return Ok(());
                }
                cdt::submit__writeunwritten__start!(|| job_id.0);

                let new_write = IOop::WriteUnwritten {
                    dependencies,
                    writes,
                };

                self.add_work(target, job_id, new_write)?;
                Some(job_id)
            }
            Message::ReadRequest {
                upstairs_id,
                session_id,
                job_id,
                dependencies,
                requests,
            } => {
                if !up.is_message_valid(upstairs_id, session_id).await? {
                    return Ok(());
                }
                cdt::submit__read__start!(|| job_id.0);

                let new_read = IOop::Read {
                    dependencies,
                    requests,
                };

                self.add_work(target, job_id, new_read)?;
                Some(job_id)
            }
            // These are for repair while taking live IO
            Message::ExtentLiveClose {
                upstairs_id,
                session_id,
                job_id,
                dependencies,
                extent_id,
            } => {
                if !up.is_message_valid(upstairs_id, session_id).await? {
                    return Ok(());
                }

                cdt::submit__el__close__start!(|| job_id.0);
                // TODO: Add dtrace probes
                let ext_close = IOop::ExtentClose {
                    dependencies,
                    extent: extent_id,
                };

                self.add_work(target, job_id, ext_close)?;
                Some(job_id)
            }
            Message::ExtentLiveFlushClose {
                upstairs_id,
                session_id,
                job_id,
                dependencies,
                extent_id,
                flush_number,
                gen_number,
            } => {
                if !up.is_message_valid(upstairs_id, session_id).await? {
                    return Ok(());
                }

                cdt::submit__el__flush__close__start!(|| job_id.0);
                // Do both the flush, and then the close
                let new_flush = IOop::ExtentFlushClose {
                    dependencies,
                    extent: extent_id,
                    flush_number,
                    gen_number,
                    source_downstairs: ClientId::new(0), // Unused in the downstairs
                    repair_downstairs: vec![], // Unused in the downstairs
                };

                self.add_work(target, job_id, new_flush)?;
                Some(job_id)
            }
            Message::ExtentLiveRepair {
                upstairs_id,
                session_id,
                job_id,
                dependencies,
                extent_id,
                source_client_id,
                source_repair_address,
            } => {
                if !up.is_message_valid(upstairs_id, session_id).await? {
                    return Ok(());
                }

                cdt::submit__el__repair__start!(|| job_id.0);
                // Do both the flush, and then the close
                let new_repair = IOop::ExtentLiveRepair {
                    dependencies,
                    extent: extent_id,
                    source_downstairs: source_client_id,
                    source_repair_address,
                    repair_downstairs: vec![],
                };

                debug!(up.log, "Received ExtentLiveRepair {}", job_id);
                self.add_work(target, job_id, new_repair)?;
                Some(job_id)
            }
            Message::ExtentLiveReopen {
                upstairs_id,
                session_id,
                job_id,
                dependencies,
                extent_id,
            } => {
                if !up.is_message_valid(upstairs_id, session_id).await? {
                    return Ok(());
                }

                cdt::submit__el__reopen__start!(|| job_id.0);
                let new_open = IOop::ExtentLiveReopen {
                    dependencies,
                    extent: extent_id,
                };

                self.add_work(target, job_id, new_open)?;
                Some(job_id)
            }
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                dependencies,
            } => {
                if !up.is_message_valid(upstairs_id, session_id).await? {
                    return Ok(());
                }
                cdt::submit__el__noop__start!(|| job_id.0);
                let new_open = IOop::ExtentLiveNoOp { dependencies };

                debug!(up.log, "Received NoOP {}", job_id);
                self.add_work(target, job_id, new_open)?;
                Some(job_id)
            }

            // These messages arrive during initial reconciliation.
            Message::ExtentFlush {
                repair_id,
                extent_id,
                client_id: _,
                flush_number,
                gen_number,
            } => {
                let msg = {
                    debug!(
                        up.log,
                        "{} Flush extent {} with f:{} g:{}",
                        repair_id,
                        extent_id,
                        flush_number,
                        gen_number
                    );

                    match self
                        .region
                        .region_flush_extent(
                            extent_id,
                            gen_number,
                            flush_number,
                            repair_id,
                        )
                        .await
                    {
                        Ok(()) => Message::RepairAckId { repair_id },
                        Err(error) => Message::ExtentError {
                            repair_id,
                            extent_id,
                            error,
                        },
                    }
                };
                up.message_tx.send(msg).await?;
                return Ok(());
            }
            Message::ExtentClose {
                repair_id,
                extent_id,
            } => {
                let msg = {
                    debug!(up.log, "{} Close extent {}", repair_id, extent_id);
                    match self.region.close_extent(extent_id).await {
                        Ok(_) => Message::RepairAckId { repair_id },
                        Err(error) => Message::ExtentError {
                            repair_id,
                            extent_id,
                            error,
                        },
                    }
                };
                up.message_tx.send(msg).await?;
                return Ok(());
            }
            Message::ExtentRepair {
                repair_id,
                extent_id,
                source_client_id,
                source_repair_address,
                dest_clients,
            } => {
                let msg = {
                    debug!(
                        up.log,
                        "{} Repair extent {} source:[{}] {:?} dest:{:?}",
                        repair_id,
                        extent_id,
                        source_client_id,
                        source_repair_address,
                        dest_clients
                    );
                    match self
                        .region
                        .repair_extent(extent_id, source_repair_address)
                        .await
                    {
                        Ok(()) => Message::RepairAckId { repair_id },
                        Err(error) => Message::ExtentError {
                            repair_id,
                            extent_id,
                            error,
                        },
                    }
                };
                up.message_tx.send(msg).await?;
                return Ok(());
            }
            Message::ExtentReopen {
                repair_id,
                extent_id,
            } => {
                let msg = {
                    debug!(up.log, "{} Reopen extent {}", repair_id, extent_id);
                    match self.region.reopen_extent(extent_id).await {
                        Ok(()) => Message::RepairAckId { repair_id },
                        Err(error) => Message::ExtentError {
                            repair_id,
                            extent_id,
                            error,
                        },
                    }
                };
                up.message_tx.send(msg).await?;
                return Ok(());
            }
            x => bail!("unexpected frame {:?}", x),
        };

        /*
         * If we added work, then fire a DTrace probe
         */
        if let Some(new_ds_id) = new_ds_id {
            cdt::work__start!(|| new_ds_id.0);
        }

        Ok(())
    }

    fn add_work(
        &mut self,
        target: ConnectionId,
        ds_id: JobId,
        work: IOop,
    ) -> Result<()> {
        // The Upstairs will send Flushes periodically, even in read only mode
        // we have to accept them. But read-only should never accept writes!
        if self.read_only {
            let is_write = match work {
                IOop::Write { .. }
                | IOop::WriteUnwritten { .. }
                | IOop::ExtentClose { .. }
                | IOop::ExtentFlushClose { .. }
                | IOop::ExtentLiveRepair { .. }
                | IOop::ExtentLiveReopen { .. }
                | IOop::ExtentLiveNoOp { .. } => true,
                IOop::Read { .. } | IOop::Flush { .. } => false,
            };

            if is_write {
                error!(self.log, "read-only but received write {:?}", work);
                bail!(CrucibleError::ModifyingReadOnlyRegion);
            }
        }

        let up = &mut self.connections.get_mut(&target).unwrap();
        let dsw = DownstairsWork {
            ds_id,
            work,
            state: WorkState::New,
        };
        up.work_mut().unwrap().add_work(ds_id, dsw);

        Ok(())
    }

    /// Attempts to promote the given connection to active
    ///
    /// The connection must be present in our `connections` map and have an
    /// `upstairs_connection` loaded (i.e. have made it to the appropriate point
    /// in negotiation).
    ///
    /// If successful, the other connection will be closed.
    async fn promote_to_active(
        &mut self,
        id_being_promoted: ConnectionId,
    ) -> Result<()> {
        let new_connection =
            self.connections[&id_being_promoted].upstairs_connection();

        if self.read_only {
            // Multiple active read-only sessions are allowed, but multiple
            // sessions for the same Upstairs UUID are not. Kick out a
            // previously active session for this UUID if one exists.
            if self
                .active_upstairs
                .contains_key(&new_connection.upstairs_id)
            {
                self.close_connection(
                    new_connection.upstairs_id,
                    &new_connection,
                )
                .await;

                // In the future, we may decide there is some way to continue
                // working on outstanding jobs, or a way to merge. But for now,
                // we just throw out what we have and let the upstairs resend
                // anything to us that it did not get an ACK for.
            }

            // Insert a new session.  This must create a new entry, because we
            // removed the entry above (if it existed).
            let prev = self
                .active_upstairs
                .insert(new_connection.upstairs_id, id_being_promoted);
            assert!(prev.is_none());

            Ok(())
        } else {
            // Only one active read-write session is allowed. Kick out the
            // currently active Upstairs session if one exists.
            match self.active_upstairs.len() {
                0 => (), // this is fine
                1 => {
                    // There is a single existing session.  Determine if this
                    // new request to promote to active should move forward or
                    // be blocked.
                    let prev_upstairs_id =
                        self.active_upstairs.values().next().unwrap();
                    let prev_upstairs = &self.connections[prev_upstairs_id];
                    let prev_connection = prev_upstairs.upstairs_connection();

                    warn!(
                        self.log,
                        "Attempting RW takeover from {:?} to {:?}",
                        prev_connection,
                        new_connection,
                    );

                    // Compare the new generaion number to what the existing
                    // connection is and take action based on that.
                    match new_connection.gen.cmp(&prev_connection.gen) {
                        Ordering::Less => {
                            // If the new connection has a lower generation
                            // number than the current connection, we don't
                            // allow it to take over.
                            bail!(
                                "Current gen {} is > requested gen of {}",
                                prev_connection.gen,
                                new_connection.gen,
                            );
                        }
                        Ordering::Equal => {
                            // The generation numbers match, the only way we
                            // allow this new connection to take over is if the
                            // upstairs_id and the session_id are the same,
                            // which means the whole structures need to be
                            // identical.
                            if prev_connection != new_connection {
                                bail!(
                                    "Same gen, but UUIDs {:?} don't match {:?}",
                                    prev_connection,
                                    new_connection,
                                );
                            }
                        }
                        // The only remaining case is the new generation
                        // number is higher than the existing.
                        Ordering::Greater => {}
                    }

                    // Close the connection, sending YouAreNoLongerActive
                    self.close_connection(
                        prev_connection.upstairs_id,
                        &new_connection,
                    )
                    .await;
                }
                _ => {
                    // Panic - we shouldn't be running with more than one
                    // active read-write Upstairs
                    panic!(
                        "More than one currently active r/w upstairs! {:?}",
                        self.active_upstairs.keys(),
                    );
                }
            }

            // We've cleared out a slot in `active_upstairs` for our new id
            let prev = self
                .active_upstairs
                .insert(new_connection.upstairs_id, id_being_promoted);
            assert!(prev.is_none());

            if !self.read_only {
                assert_eq!(self.active_upstairs.len(), 1);
            }

            // Re-open any closed extents
            self.region.reopen_all_extents().await?;

            info!(
                self.log,
                "{:?} is now active ({})",
                new_connection,
                if self.read_only {
                    "read-only"
                } else {
                    "read-write"
                }
            );

            Ok(())
        }
    }

    /// Closes a currently-running connection
    ///
    /// `old_upstairs_id` must be a member of `self.active_upstairs`.  It is
    /// removed from `active_upstairs` and `connections`, sent
    /// `YouAreNoLongerActive`, then dropped (which causes the IO task to halt).
    async fn close_connection(
        &mut self,
        old_upstairs_id: Uuid,
        new_connection: &UpstairsConnection, // only used for logging
    ) {
        let id = self.active_upstairs.remove(&old_upstairs_id).unwrap();
        let prev_upstairs = self.connections.remove(&id).unwrap();
        let old_connection = prev_upstairs.upstairs_connection();

        warn!(
            self.log,
            "closing {:?} handles because {:?} is being promoted ({})",
            old_connection,
            new_connection,
            if self.read_only {
                "read-only"
            } else {
                "read-write"
            }
        );

        if let Err(e) = prev_upstairs
            .message_tx
            .send(Message::YouAreNoLongerActive {
                new_upstairs_id: new_connection.upstairs_id,
                new_session_id: new_connection.session_id,
                new_gen: new_connection.gen,
            })
            .await
        {
            warn!(
                prev_upstairs.log,
                "Failed sending YouAreNoLongerActive: {}", e
            );
        }

        // Note: in the future, differentiate between new upstairs
        // connecting vs same upstairs reconnecting here.
        //
        // Clear out active jobs, the last flush, and completed
        // information, as that will not be valid any longer.
        //
        // TODO: Really work through this error case
        let work = prev_upstairs.work();
        if let Some(work) = work {
            if work.active.keys().len() > 0 {
                warn!(
                    self.log,
                    "Crucible Downstairs promoting {:?} to active, \
                     discarding {} jobs",
                    new_connection,
                    work.active.keys().len()
                );
            }
        }

        // In the future, we may decide there is some way to
        // continue working on outstanding jobs, or a way to merge.
        // But for now, we just throw out what we have and let the
        // upstairs resend anything to us that it did not get an ACK
        // for.
    }

    /// Process all available IO work
    async fn do_io_work(&mut self) {
        // Add a little time to completion for this operation.
        if self.lossy && random() && random() {
            info!(self.log, "[lossy] sleeping 1 second");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Clone our active upstairs values, because we're going to call a
        // function on &mut self below.
        let vs = self.active_upstairs.values().cloned().collect::<Vec<_>>();
        for i in vs {
            if let Err(e) = self.do_io_work_for(i).await {
                warn!(
                    self.log,
                    "got error {e:?} while doing io work for {i:?}"
                );
                self.drop_connection(i);
            }
        }
    }

    /// Perform all pending IO work for a single Upstairs connection
    async fn do_io_work_for(&mut self, target: ConnectionId) -> Result<()> {
        let up = &mut self.connections.get_mut(&target).unwrap();
        let upstairs_connection = up.upstairs_connection();
        let Some(work) = up.work_mut() else {
            return Ok(());
        };

        /*
         * Build ourselves a list of all the jobs on the work hashmap that
         * are New or DepWait.
         */
        let mut new_work = work.new_work();

        /*
         * We don't have to do jobs in order, but the dependencies are, at
         * least for now, always going to be in order of job id. `new_work` is
         * sorted before it is returned so this function iterates through jobs
         * in order.
         */
        while let Some(new_id) = new_work.pop_front() {
            if self.lossy && random() && random() {
                // Skip a job that needs to be done. Sometimes
                info!(self.log, "[lossy] skipping {}", new_id);
                new_work.push_back(new_id);
                continue;
            }

            // Reborrow `up` and `work`
            let up = &mut self.connections.get_mut(&target).unwrap();
            let work = up.work_mut().unwrap();

            /*
             * If this job is still new, take it and go to work. The
             * in_progress method will only return a job if all
             * dependencies are met.
             */
            // TODO should we really be making a new log for every job?
            let r = work.in_progress(
                new_id,
                self.log.new(o!("role" => "work".to_string())),
            );

            if let Some(ds_work) = r {
                let job_id = ds_work.ds_id;
                cdt::work__process!(|| job_id.0);

                let m = self.do_work(&ds_work, upstairs_connection).await;

                // If this is a repair job, and that repair failed, we
                // can do no more work on this downstairs and should
                // force everything to come down before more work arrives.
                //
                // However, we can respond to the upstairs with the failed
                // result, and let the upstairs take action that will
                // allow it to abort the repair and continue working in
                // some degraded state.
                let abort_needed = check_message_for_abort(&m);

                if let Some(error) = m.err() {
                    // Reborrow `up`
                    let up = &mut self.connections.get_mut(&target).unwrap();

                    up.message_tx
                        .send(Message::ErrorReport {
                            upstairs_id: upstairs_connection.upstairs_id,
                            session_id: upstairs_connection.session_id,
                            job_id: new_id,
                            error: error.clone(),
                        })
                        .await?;

                    // If the job errored, do not consider it completed.
                    // Retry it, putting it back in the map.
                    let work = up.work_mut().unwrap(); // reborrow
                    new_work.push_back(new_id);
                    work.active.insert(job_id, ds_work);
                } else {
                    // Update our stats
                    self.complete_work_stat(&m, job_id);

                    // Notify the upstairs before completing work
                    let is_flush = matches!(m, Message::FlushAck { .. });

                    // Send the reply
                    let up = &mut self.connections.get_mut(&target).unwrap();
                    up.message_tx.send(m).await?;

                    let work = up.work_mut().unwrap();
                    if is_flush {
                        work.last_flush = job_id;
                        work.completed.clear();
                    } else {
                        work.completed.push(job_id);
                    }

                    cdt::work__done!(|| job_id.0);
                }

                // Now, if the message requires an abort, we handle
                // that now by exiting this task with error.
                if abort_needed {
                    bail!("Repair has failed, exiting this Upstairs");
                }
            }
        }
        Ok(())
    }

    /// Perform actual IO work on the disk
    ///
    /// This is infallible, because any errors are encoded in the returned
    /// `Message` (and logged).
    async fn do_work(
        &mut self,
        job: &DownstairsWork,
        upstairs_connection: UpstairsConnection,
    ) -> Message {
        let job_id = job.ds_id;
        match &job.work {
            IOop::Read {
                dependencies,
                requests,
            } => {
                /*
                 * Any error from an IO should be intercepted here and passed
                 * back to the upstairs.
                 */
                let responses = if self.read_errors && random() && random() {
                    warn!(self.log, "returning error on read!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else {
                    self.region.region_read(requests, job_id).await
                };
                debug!(
                    self.log,
                    "Read      :{} deps:{:?} res:{}",
                    job_id,
                    dependencies,
                    responses.is_ok(),
                );

                Message::ReadResponse {
                    upstairs_id: upstairs_connection.upstairs_id,
                    session_id: upstairs_connection.session_id,
                    job_id,
                    responses,
                }
            }
            IOop::WriteUnwritten { writes, .. } => {
                /*
                 * Any error from an IO should be intercepted here and passed
                 * back to the upstairs.
                 */
                let result = if self.write_errors && random() && random() {
                    warn!(self.log, "returning error on writeunwritten!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else {
                    // The region_write will handle what happens to each block
                    // based on if they have data or not.
                    self.region.region_write(writes, job_id, true).await
                };

                Message::WriteUnwrittenAck {
                    upstairs_id: upstairs_connection.upstairs_id,
                    session_id: upstairs_connection.session_id,
                    job_id,
                    result,
                }
            }
            IOop::Write {
                dependencies,
                writes,
            } => {
                let result = if self.write_errors && random() && random() {
                    warn!(self.log, "returning error on write!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else {
                    self.region.region_write(writes, job_id, false).await
                };
                debug!(
                    self.log,
                    "Write     :{} deps:{:?} res:{}",
                    job_id,
                    dependencies,
                    result.is_ok(),
                );

                Message::WriteAck {
                    upstairs_id: upstairs_connection.upstairs_id,
                    session_id: upstairs_connection.session_id,
                    job_id,
                    result,
                }
            }
            IOop::Flush {
                dependencies,
                flush_number,
                gen_number,
                snapshot_details,
                extent_limit,
            } => {
                let result = if self.flush_errors && random() && random() {
                    warn!(self.log, "returning error on flush!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else {
                    self.region
                        .region_flush(
                            *flush_number,
                            *gen_number,
                            snapshot_details,
                            job_id,
                            *extent_limit,
                        )
                        .await
                };
                debug!(
                    self.log,
                    "Flush     :{} extent_limit {:?} deps:{:?} res:{} f:{} g:{}",
                    job_id, extent_limit, dependencies, result.is_ok(),
                    flush_number, gen_number,
                );

                Message::FlushAck {
                    upstairs_id: upstairs_connection.upstairs_id,
                    session_id: upstairs_connection.session_id,
                    job_id,
                    result,
                }
            }
            IOop::ExtentClose {
                dependencies,
                extent,
            } => {
                let result = self.region.close_extent(*extent).await;
                debug!(
                    self.log,
                    "JustClose :{} extent {} deps:{:?} res:{}",
                    job_id,
                    extent,
                    dependencies,
                    result.is_ok(),
                );

                Message::ExtentLiveCloseAck {
                    upstairs_id: upstairs_connection.upstairs_id,
                    session_id: upstairs_connection.session_id,
                    job_id: job.ds_id,
                    result,
                }
            }
            IOop::ExtentFlushClose {
                dependencies,
                extent,
                flush_number,
                gen_number,
                source_downstairs: _,
                repair_downstairs: _,
            } => {
                // If flush fails, return that result.
                // Else, if close fails, return that result.
                // Else, return the f/g/d from the close.
                let result = match self
                    .region
                    .region_flush_extent(
                        *extent,
                        *gen_number,
                        *flush_number,
                        job_id,
                    )
                    .await
                {
                    Err(f_res) => Err(f_res),
                    Ok(_) => self.region.close_extent(*extent).await,
                };

                debug!(
                    self.log,
                    "FlushClose:{} extent {} deps:{:?} res:{} f:{} g:{}",
                    job_id,
                    extent,
                    dependencies,
                    result.is_ok(),
                    flush_number,
                    gen_number,
                );

                Message::ExtentLiveCloseAck {
                    upstairs_id: upstairs_connection.upstairs_id,
                    session_id: upstairs_connection.session_id,
                    job_id: job.ds_id,
                    result,
                }
            }
            IOop::ExtentLiveRepair {
                dependencies,
                extent,
                source_downstairs: _,
                source_repair_address,
                repair_downstairs: _,
            } => {
                debug!(
                    self.log,
                    "ExtentLiveRepair: extent {} sra:{:?}",
                    extent,
                    source_repair_address
                );
                let result = self
                    .region
                    .repair_extent(*extent, *source_repair_address)
                    .await;
                debug!(
                    self.log,
                    "LiveRepair:{} extent {} deps:{:?} res:{}",
                    job_id,
                    extent,
                    dependencies,
                    result.is_ok(),
                );

                Message::ExtentLiveRepairAckId {
                    upstairs_id: upstairs_connection.upstairs_id,
                    session_id: upstairs_connection.session_id,
                    job_id,
                    result,
                }
            }
            IOop::ExtentLiveReopen {
                dependencies,
                extent,
            } => {
                let result = self.region.reopen_extent(*extent).await;
                debug!(
                    self.log,
                    "LiveReopen:{} extent {} deps:{:?} res:{}",
                    job_id,
                    extent,
                    dependencies,
                    result.is_ok(),
                );
                Message::ExtentLiveAckId {
                    upstairs_id: upstairs_connection.upstairs_id,
                    session_id: upstairs_connection.session_id,
                    job_id,
                    result,
                }
            }
            IOop::ExtentLiveNoOp { dependencies } => {
                debug!(self.log, "Work of: LiveNoOp {}", job_id);
                let result = Ok(());
                debug!(
                    self.log,
                    "LiveNoOp  :{} deps:{:?} res:{}",
                    job_id,
                    dependencies,
                    result.is_ok(),
                );
                Message::ExtentLiveAckId {
                    upstairs_id: upstairs_connection.upstairs_id,
                    session_id: upstairs_connection.session_id,
                    job_id,
                    result,
                }
            }
        }
    }

    /*
     * After we complete a read/write/flush on a region, update the
     * Oximeter counter for the operation.
     */
    fn complete_work_stat(&mut self, m: &Message, ds_id: JobId) {
        // XXX dss per upstairs connection?
        match m {
            Message::FlushAck { .. } => {
                cdt::submit__flush__done!(|| ds_id.0);
                self.dss.add_flush();
            }
            Message::WriteAck { .. } => {
                cdt::submit__write__done!(|| ds_id.0);
                self.dss.add_write();
            }
            Message::WriteUnwrittenAck { .. } => {
                cdt::submit__writeunwritten__done!(|| ds_id.0);
                self.dss.add_write();
            }
            Message::ReadResponse { .. } => {
                cdt::submit__read__done!(|| ds_id.0);
                self.dss.add_read();
            }
            Message::ExtentLiveClose { .. } => {
                cdt::submit__el__close__done!(|| ds_id.0);
            }
            Message::ExtentLiveFlushClose { .. } => {
                cdt::submit__el__flush__close__done!(|| ds_id.0);
            }
            Message::ExtentLiveRepair { .. } => {
                cdt::submit__el__repair__done!(|| ds_id.0);
            }
            Message::ExtentLiveReopen { .. } => {
                cdt::submit__el__reopen__done!(|| ds_id.0);
            }
            Message::ExtentLiveNoOp { .. } => {
                cdt::submit__el__noop__done!(|| ds_id.0);
            }
            _ => (),
        }
    }
}

#[cfg(test)]
impl Downstairs {
    /// Helper function to get new work for a specific connection
    ///
    /// This imitates the beginning of `do_io_work_for`
    fn new_work(&mut self, id: ConnectionId) -> VecDeque<JobId> {
        let up = &mut self.connections.get_mut(&id).unwrap();
        let work = up.work_mut().unwrap();
        work.new_work()
    }

    /// Helper function to mark a particular job as in-progress
    ///
    /// This imitates the middle of `do_io_work_for`
    #[must_use]
    fn in_progress(
        &mut self,
        id: ConnectionId,
        ds_id: JobId,
    ) -> Option<DownstairsWork> {
        let up = &mut self.connections.get_mut(&id).unwrap();
        let work = up.work_mut().unwrap();
        work.in_progress(ds_id, self.log.clone())
    }

    /// Helper function to complete a particular job
    ///
    /// This imitates the end of `do_io_work_for`
    fn complete_work(&mut self, id: ConnectionId, ds_id: JobId, m: Message) {
        let up = &mut self.connections.get_mut(&id).unwrap();
        let work = up.work_mut().unwrap();

        // Complete the job
        if matches!(m, Message::FlushAck { .. }) {
            work.last_flush = ds_id;
            work.completed = Vec::with_capacity(32);
        } else {
            work.completed.push(ds_id);
        }
    }

    fn is_active(&self, connection: UpstairsConnection) -> bool {
        let uuid = connection.upstairs_id;
        if let Some(id) = self.active_upstairs.get(&uuid) {
            self.connections[id].upstairs_connection() == connection
        } else {
            false
        }
    }

    fn get_job(&self, id: ConnectionId, ds_id: JobId) -> &DownstairsWork {
        let up = self.connections.get(&id).unwrap();
        let work = up.work().unwrap();
        work.get_job(ds_id)
    }
}

#[derive(Debug)]
pub struct ActiveUpstairs {
    pub upstairs_connection: UpstairsConnection,
    pub work: Mutex<Work>,
    pub terminate_sender: oneshot::Sender<UpstairsConnection>,
}

/*
 * The structure that tracks downstairs work in progress
 */
#[derive(Debug)]
pub struct Work {
    active: HashMap<JobId, DownstairsWork>,
    outstanding_deps: HashMap<JobId, usize>,

    /*
     * We have to keep track of all IOs that have been issued since
     * our last flush, as that is how we make sure dependencies are
     * respected. The last_flush is the downstairs job ID number (ds_id
     * typically) for the most recent flush.
     */
    last_flush: JobId,
    completed: Vec<JobId>,
}

#[derive(Debug)]
struct DownstairsWork {
    ds_id: JobId,
    work: IOop,
    state: WorkState,
}

impl Work {
    fn new() -> Self {
        Work {
            active: HashMap::new(),
            outstanding_deps: HashMap::new(),
            last_flush: JobId(0), // TODO(matt) make this an Option?
            completed: Vec::with_capacity(32),
        }
    }

    /**
     * Return a list of downstairs request IDs that are new or have
     * been waiting for other dependencies to finish.
     */
    fn new_work(&self) -> VecDeque<JobId> {
        let mut result = Vec::with_capacity(self.active.len());

        for job in self.active.values() {
            if job.state == WorkState::New || job.state == WorkState::DepWait {
                result.push(job.ds_id);
            }
        }

        result.sort_unstable();
        result.into_iter().collect()
    }

    fn add_work(&mut self, ds_id: JobId, dsw: DownstairsWork) {
        self.active.insert(ds_id, dsw);
    }

    #[cfg(test)]
    fn get_job(&self, ds_id: JobId) -> &DownstairsWork {
        self.active.get(&ds_id).unwrap()
    }

    /**
     * If the requested job is still new and the dependencies are all met,
     * or the job is InProgress, remove the job from the map and return the
     * DownstairsWork object.
     *
     * If the dependencies are not met, move the state to DepWait and keep it in
     * the map.
     */
    #[must_use]
    fn in_progress(
        &mut self,
        ds_id: JobId,
        log: Logger,
    ) -> Option<DownstairsWork> {
        let Some(mut job) = self.active.remove(&ds_id) else {
            panic!("can't call in_progress on missing job");
        };

        if job.state == WorkState::New || job.state == WorkState::DepWait {
            /*
             * Before we can make this in_progress, we have to, while
             * holding this locked, check the dep list if there is one
             * and make sure all dependencies are completed.
             */
            let dep_list = job.work.deps();

            /*
             * See which of our dependencies are met.
             * XXX Make this better/faster by removing the ones that
             * are met, so next lap we don't have to check again?  There
             * may be some debug value to knowing what the dep list was,
             * so consider that before making this faster.
             */
            let mut deps_outstanding: Vec<JobId> =
                Vec::with_capacity(dep_list.len());

            for dep in dep_list.iter() {
                // The Downstairs currently assumes that all jobs previous
                // to the last flush have completed, hence this early out.
                //
                // Currently `work.completed` is cleared out when
                // `Downstairs::complete_work` (or `complete` in mod test)
                // is called with a FlushAck so this early out cannot be
                // removed unless that is changed too.
                if dep <= &self.last_flush {
                    continue;
                }

                if !self.completed.contains(dep) {
                    deps_outstanding.push(*dep);
                }
            }

            if !deps_outstanding.is_empty() {
                let print = if let Some(existing_outstanding_deps) =
                    self.outstanding_deps.get(&ds_id)
                {
                    *existing_outstanding_deps != deps_outstanding.len()
                } else {
                    false
                };

                if print {
                    warn!(
                        log,
                        "{} job {} waiting on {} deps",
                        ds_id,
                        match &job.work {
                            IOop::Write { .. } => "Write",
                            IOop::WriteUnwritten { .. } => "WriteUnwritten",
                            IOop::Flush { .. } => "Flush",
                            IOop::Read { .. } => "Read",
                            IOop::ExtentClose { .. } => "ECLose",
                            IOop::ExtentFlushClose { .. } => "EFlushCLose",
                            IOop::ExtentLiveRepair { .. } => "ELiveRepair",
                            IOop::ExtentLiveReopen { .. } => "ELiveReopen",
                            IOop::ExtentLiveNoOp { .. } => "NoOp",
                        },
                        deps_outstanding.len(),
                    );
                }

                let _ =
                    self.outstanding_deps.insert(ds_id, deps_outstanding.len());

                /*
                 * If we got here, then the dep is not met.
                 * Set DepWait if not already set.
                 */
                if job.state == WorkState::New {
                    job.state = WorkState::DepWait;
                }

                // Return the job to the map
                self.active.insert(ds_id, job);
                return None;
            }

            /*
             * We had no dependencies, or they are all completed, we
             * can go ahead and work on this job.
             */
            job.state = WorkState::InProgress;

            Some(job)
        } else if job.state == WorkState::InProgress {
            // A previous call of this function put this job in progress, but
            // something went wrong and the job ended up back in the map. We've
            // taken it out again and will return it again for a second try.
            Some(job)
        } else {
            // job id is not new, we can't run it; return the job to the map
            self.active.insert(ds_id, job);
            None
        }
    }
}

/*
 * XXX We may not need Done. At the moment all we actually look at is New or
 * InProgress.
 */
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq)]
pub enum WorkState {
    New,
    DepWait,
    InProgress,
    Done,
}

impl fmt::Display for WorkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkState::New => {
                write!(f, " New")
            }
            WorkState::DepWait => {
                write!(f, "DepW")
            }
            WorkState::InProgress => {
                write!(f, "In P")
            }
            WorkState::Done => {
                write!(f, "Done")
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum WrappedStream {
    Http(tokio::net::TcpStream),
    Https(tokio_rustls::server::TlsStream<tokio::net::TcpStream>),
}

/// On-disk backend for downstairs storage
///
/// Normally, we only allow the most recent backend.  However, for integration
/// tests, it can be useful to create volumes using older backends.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Backend {
    RawFile,

    #[cfg(any(test, feature = "integration-tests"))]
    SQLite,
}

pub async fn create_region(
    block_size: u64,
    data: PathBuf,
    extent_size: u64,
    extent_count: u64,
    uuid: Uuid,
    encrypted: bool,
    log: Logger,
) -> Result<Region> {
    create_region_with_backend(
        data,
        Block {
            value: extent_size,
            shift: block_size.trailing_zeros(),
        },
        extent_count,
        uuid,
        encrypted,
        Backend::RawFile,
        log,
    )
    .await
}

pub async fn create_region_with_backend(
    data: PathBuf,
    extent_size: Block,
    extent_count: u64,
    uuid: Uuid,
    encrypted: bool,
    backend: Backend,
    log: Logger,
) -> Result<Region> {
    /*
     * Create the region options, then the region.
     */
    let mut region_options: crucible_common::RegionOptions = Default::default();
    region_options.set_block_size(extent_size.block_size_in_bytes().into());
    region_options.set_extent_size(extent_size);
    region_options.set_uuid(uuid);
    region_options.set_encrypted(encrypted);

    let mut region =
        Region::create_with_backend(data, region_options, backend, log).await?;
    region.extend(extent_count as u32).await?;

    Ok(region)
}

pub async fn build_downstairs_for_region(
    data: &Path,
    lossy: bool,
    read_errors: bool,
    write_errors: bool,
    flush_errors: bool,
    read_only: bool,
    log_request: Option<Logger>,
) -> Result<(Downstairs, DownstairsIoHandle)> {
    build_downstairs_for_region_with_backend(
        data,
        lossy,
        read_errors,
        write_errors,
        flush_errors,
        read_only,
        Backend::RawFile,
        log_request,
    )
    .await
}

// Build the downstairs struct given a region directory and some additional
// needed information.  If a logger is passed in, we will use that, otherwise
// a logger will be created.
#[allow(clippy::too_many_arguments)]
pub async fn build_downstairs_for_region_with_backend(
    data: &Path,
    lossy: bool,
    read_errors: bool,
    write_errors: bool,
    flush_errors: bool,
    read_only: bool,
    backend: Backend,
    log_request: Option<Logger>,
) -> Result<(Downstairs, DownstairsIoHandle)> {
    let log = match log_request {
        Some(log) => log,
        None => build_logger(),
    };
    let region = Region::open_with_backend(
        data,
        Default::default(),
        true,
        read_only,
        backend,
        &log,
    )
    .await?;

    info!(log, "UUID: {:?}", region.def().uuid());
    info!(
        log,
        "Blocks per extent:{} Total Extents: {}",
        region.def().extent_size().value,
        region.def().extent_count(),
    );

    let encrypted = region.encrypted();

    Ok(Downstairs::new(
        region,
        lossy,
        read_errors,
        write_errors,
        flush_errors,
        read_only,
        encrypted,
        log,
    ))
}

/// Returns Ok if everything spawned ok, Err otherwise
///
/// Return Ok(main task join handle) if all the necessary tasks spawned
/// successfully, and Err otherwise.
#[allow(clippy::too_many_arguments)]
pub async fn start_downstairs(
    mut ds: Downstairs,
    ds_channel: DownstairsIoHandle,
    address: IpAddr,
    oximeter: Option<SocketAddr>,
    port: u16,
    rport: u16,
    cert_pem: Option<String>,
    key_pem: Option<String>,
    root_cert_pem: Option<String>,
) -> Result<(SocketAddr, tokio::task::JoinHandle<Result<()>>)> {
    // Clone the stats, which can be shared between multiple tasks
    let dss = ds.dss.clone();
    if let Some(oximeter) = oximeter {
        let log = ds.log.new(o!("task" => "oximeter".to_string()));
        let dss = dss.clone();
        tokio::spawn(async move {
            let new_address = match address {
                IpAddr::V4(ipv4) => {
                    SocketAddr::new(std::net::IpAddr::V4(ipv4), 0)
                }
                IpAddr::V6(ipv6) => {
                    SocketAddr::new(std::net::IpAddr::V6(ipv6), 0)
                }
            };

            if let Err(e) =
                stats::ox_stats(dss, oximeter, new_address, &log).await
            {
                error!(log, "ERROR: oximeter failed: {:?}", e);
            } else {
                warn!(log, "OK: oximeter all done");
            }
        });
    }

    // Setup a log for this task
    let log = ds.log.new(o!("task" => "main".to_string()));

    let listen_on = match address {
        IpAddr::V4(ipv4) => SocketAddr::new(std::net::IpAddr::V4(ipv4), port),
        IpAddr::V6(ipv6) => SocketAddr::new(std::net::IpAddr::V6(ipv6), port),
    };

    // Establish a listen server on the port.
    let listener = TcpListener::bind(&listen_on).await?;
    let local_addr = listener.local_addr()?;
    ds.address = Some(local_addr);

    let info = crucible_common::BuildInfo::default();
    info!(log, "Crucible Version: {}", info);
    info!(
        log,
        "Upstairs <-> Downstairs Message Version: {}", CRUCIBLE_MESSAGE_VERSION
    );

    let repair_address = match address {
        IpAddr::V4(ipv4) => SocketAddr::new(std::net::IpAddr::V4(ipv4), rport),
        IpAddr::V6(ipv6) => SocketAddr::new(std::net::IpAddr::V6(ipv6), rport),
    };

    let repair_log = ds.log.new(o!("task" => "repair".to_string()));
    let repair_listener = match repair::repair_main(
        ds.region.dir.clone(),
        repair_address,
        &repair_log,
    )
    .await
    {
        Err(e) => {
            // TODO tear down other things if repair server can't be
            // started?
            bail!("got {:?} from repair main", e);
        }

        Ok(socket_addr) => socket_addr,
    };

    ds.repair_address = Some(repair_listener);
    info!(log, "Using repair address: {:?}", repair_listener);

    // Optionally require SSL connections
    let ssl_acceptor = if let Some(cert_pem_path) = cert_pem {
        let key_pem_path = key_pem.unwrap();
        let root_cert_pem_path = root_cert_pem.unwrap();

        let context = crucible_common::x509::TLSContext::from_paths(
            &cert_pem_path,
            &key_pem_path,
            &root_cert_pem_path,
        )?;

        let config = context.get_server_config()?;

        info!(log, "Configured SSL acceptor");

        Some(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
    } else {
        // unencrypted
        info!(log, "No SSL acceptor configured");
        None
    };

    // There are two main tasks: the listener accepts connections on the socket
    // and sends them to the downstairs task; the downstairs task does
    // everything else (including spawning IO tasks as needed);
    let mut ds_task = tokio::spawn(async move { ds.run().await });
    let join_handle = tokio::spawn(async move {
        /*
         * We now loop listening for a connection from the Upstairs. When we get
         * one, we then pass it to the Downstairs task and wait for another
         * connection. Downstairs can handle multiple Upstairs connecting but
         * only one active one.
         */
        info!(log, "listening on {}", listen_on);
        loop {
            tokio::select! {
                r = listener.accept() => {
                    let (sock, raddr) = r.map_err(anyhow::Error::from)?;
                    /*
                     * We have a new connection; before we wrap it, set
                     * TCP_NODELAY to assure that we don't get Nagle'd.
                     */
                    sock.set_nodelay(true).expect("could not set TCP_NODELAY");

                    let stream: WrappedStream = if let Some(ssl_acceptor) =
                        &ssl_acceptor
                    {
                        let ssl_acceptor = ssl_acceptor.clone();
                        WrappedStream::Https(match ssl_acceptor.accept(sock).await {
                            Ok(v) => v,
                            Err(e) => {
                                warn!(
                                    log,
                                    "rejecting connection from {:?}: {:?}", raddr, e,
                                );
                                continue;
                            }
                        })
                    } else {
                        WrappedStream::Http(sock)
                    };

                    info!(log, "accepted connection from {:?}", raddr);
                    /*
                     * Add one to the counter every time we have a connection
                     * from an upstairs
                     */
                    dss.add_connection();

                    if let Err(e) = ds_channel.stream_tx.send(stream).await {
                        error!(log, "ds_channel closed early: {e:?}");
                        break Err(e.into());
                    }
                }
                r = &mut ds_task => {
                    error!(log, "ds_task finished early: {r:?}");
                    break r.map_err(anyhow::Error::from);
                }
            }
        }
    });

    Ok((local_addr, join_handle))
}

#[cfg(test)]
mod test {
    use super::*;
    use rand_chacha::ChaCha20Rng;
    use std::net::Ipv4Addr;
    use tempfile::{tempdir, TempDir};
    use tokio::net::TcpSocket;

    // Create a simple logger
    fn csl() -> Logger {
        build_logger()
    }

    fn add_work(
        work: &mut Work,
        ds_id: JobId,
        deps: Vec<JobId>,
        is_flush: bool,
    ) {
        work.add_work(
            ds_id,
            DownstairsWork {
                ds_id,
                work: if is_flush {
                    IOop::Flush {
                        dependencies: deps,
                        flush_number: 10,
                        gen_number: 0,
                        snapshot_details: None,
                        extent_limit: None,
                    }
                } else {
                    IOop::Read {
                        dependencies: deps,
                        requests: vec![ReadRequest {
                            eid: 1,
                            offset: Block::new_512(1),
                        }],
                    }
                },
                state: WorkState::New,
            },
        );
    }

    fn add_work_rf(work: &mut Work, ds_id: JobId, deps: Vec<JobId>) {
        work.add_work(
            ds_id,
            DownstairsWork {
                ds_id,
                work: IOop::WriteUnwritten {
                    dependencies: deps,
                    writes: Vec::with_capacity(1),
                },
                state: WorkState::New,
            },
        );
    }

    fn complete(work: &mut Work, job: DownstairsWork) {
        let is_flush = {
            // validate that deps are done
            let dep_list = job.work.deps();
            for dep in dep_list {
                let last_flush_satisfied = dep <= &work.last_flush;
                let complete_satisfied = work.completed.contains(dep);

                assert!(last_flush_satisfied || complete_satisfied);
            }

            matches!(
                job.work,
                IOop::Flush {
                    dependencies: _,
                    flush_number: _,
                    gen_number: _,
                    snapshot_details: _,
                    extent_limit: _,
                }
            )
        };

        let ds_id = job.ds_id;
        assert!(!work.active.contains_key(&ds_id));

        if is_flush {
            work.last_flush = ds_id;
            work.completed.clear();
        } else {
            work.completed.push(ds_id);
        }
    }

    fn test_push_next_jobs(work: &mut Work) -> Vec<DownstairsWork> {
        let mut jobs = vec![];
        let new_work = work.new_work();

        for new_id in new_work.iter() {
            let ds_work = work.in_progress(*new_id, csl());
            match ds_work {
                Some(ds_work) => {
                    assert_eq!(ds_work.state, WorkState::InProgress);
                    jobs.push(ds_work);
                }
                None => {
                    continue;
                }
            }
        }

        jobs
    }

    fn create_test_upstairs(ds: &mut Downstairs) -> ConnectionId {
        let id = ds.next_connection_id();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 10,
        };
        ds.connections.insert(
            id,
            fake_upstairs(
                UpstairsState::Running {
                    work: Work::new(),
                    upstairs_connection,
                },
                &ds.log,
            ),
        );
        id
    }

    fn test_do_work(work: &mut Work, jobs: Vec<DownstairsWork>) {
        for job_id in jobs {
            complete(work, job_id);
        }
    }

    #[test]
    fn you_had_one_job() {
        let mut work = Work::new();
        add_work(&mut work, JobId(1000), vec![], false);

        assert_eq!(work.new_work(), vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        let ids = next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>();
        assert_eq!(ids, vec![JobId(1000)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        assert!(test_push_next_jobs(&mut work).is_empty());
    }

    #[tokio::test]
    async fn test_simple_read() -> Result<()> {
        // Test region create and a read of one block.
        let block_size: u64 = 512;
        let extent_size = 4;

        // create region
        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(2).await?;

        let path_dir = dir.as_ref().to_path_buf();
        let (mut ds, _io) = build_downstairs_for_region(
            &path_dir,
            false,
            false,
            false,
            false,
            false,
            Some(csl()),
        )
        .await?;

        // This happens in proc() function.
        let upstairs_id = create_test_upstairs(&mut ds);
        let upstairs_connection =
            ds.connections[&upstairs_id].upstairs_connection();

        let rio = IOop::Read {
            dependencies: Vec::new(),
            requests: vec![ReadRequest {
                eid: 0,
                offset: Block::new_512(1),
            }],
        };
        ds.add_work(upstairs_id, JobId(1000), rio)?;

        let deps = vec![JobId(1000)];
        let rio = IOop::Read {
            dependencies: deps,
            requests: vec![ReadRequest {
                eid: 1,
                offset: Block::new_512(1),
            }],
        };
        ds.add_work(upstairs_id, JobId(1001), rio)?;

        show_work(&mut ds);

        // Now we mimic what happens in the do_work_task()
        let new_work = ds.new_work(upstairs_id);
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 2);

        for id in new_work.iter() {
            let ds_work = ds.in_progress(upstairs_id, *id).unwrap();
            assert_eq!(ds_work.ds_id, *id);
            println!("Do IOop {}", *id);
            let m = ds.do_work(&ds_work, upstairs_connection).await;
            println!("Got m: {:?}", m);
            ds.complete_work(upstairs_id, *id, m);
        }
        show_work(&mut ds);
        Ok(())
    }

    // Test function to create a simple downstairs with the given block
    // and extent values.  Returns the Downstairs.
    async fn create_test_downstairs(
        block_size: u64,
        extent_size: u64,
        extent_count: u32,
        dir: &TempDir,
    ) -> Result<(Downstairs, DownstairsIoHandle)> {
        // create region
        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        mkdir_for_file(dir.path())?;
        let mut region = Region::create(dir, region_options, csl()).await?;
        region.extend(extent_count).await?;

        let path_dir = dir.as_ref().to_path_buf();
        let (ds, ds_channel) = build_downstairs_for_region(
            &path_dir,
            false,
            false,
            false,
            false,
            false,
            Some(csl()),
        )
        .await?;

        Ok((ds, ds_channel))
    }

    #[tokio::test]
    async fn test_extent_simple_close_flush_close() -> Result<()> {
        // Test creating these IOops:
        // IOop::ExtentClose
        // IOop::ExtentFlushClose
        // IOop::Read
        // IOop::ExtentLiveNoOp
        // IOop::ExtentLiveReopen
        // Put them on the work queue and verify they flow through the
        // queue as expected, No actual work results are checked.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;

        let (mut ds, _chan) =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;

        // This happens in proc() function.
        let upstairs_id = create_test_upstairs(&mut ds);
        let upstairs_connection =
            ds.connections[&upstairs_id].upstairs_connection();

        let rio = IOop::ExtentClose {
            dependencies: Vec::new(),
            extent: 0,
        };
        ds.add_work(upstairs_id, JobId(1000), rio)?;

        let rio = IOop::ExtentFlushClose {
            dependencies: vec![],
            extent: 1,
            flush_number: 1,
            gen_number: 2,
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
        };
        ds.add_work(upstairs_id, JobId(1001), rio)?;

        let deps = vec![JobId(1000), JobId(1001)];
        let rio = IOop::Read {
            dependencies: deps,
            requests: vec![ReadRequest {
                eid: 2,
                offset: Block::new_512(1),
            }],
        };
        ds.add_work(upstairs_id, JobId(1002), rio)?;

        let deps = vec![JobId(1000), JobId(1001), JobId(1002)];
        let rio = IOop::ExtentLiveNoOp { dependencies: deps };
        ds.add_work(upstairs_id, JobId(1003), rio)?;

        let deps = vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)];
        let rio = IOop::ExtentLiveReopen {
            dependencies: deps,
            extent: 0,
        };
        ds.add_work(upstairs_id, JobId(1004), rio)?;

        println!("Before doing work we have:");
        show_work(&mut ds);

        // Now we mimic what happens in the do_work_task()
        let new_work = ds.new_work(upstairs_id);
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 5);

        for id in new_work.iter() {
            let ds_work = ds.in_progress(upstairs_id, *id).unwrap();
            assert_eq!(ds_work.ds_id, *id);
            println!("Do IOop {}", *id);
            let m = ds.do_work(&ds_work, upstairs_connection).await;
            println!("Got m: {:?}", m);
            ds.complete_work(upstairs_id, *id, m);
        }

        let new_work = ds.new_work(upstairs_id);
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_extent_new_close_flush_close() -> Result<()> {
        // Test sending IOops for ExtentClose and ExtentFlushClose when we
        // have an unwritten block.  We are verifying here that only the
        // dirty bit is set, as the other values (gen/flush) should remain
        // as if the extent is unwritten.
        // After closing, reopen both extents and verify no issues.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let (mut ds, _chan) =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;
        let upstairs_id = create_test_upstairs(&mut ds);
        let upstairs_connection =
            ds.connections[&upstairs_id].upstairs_connection();

        let rio = IOop::ExtentClose {
            dependencies: Vec::new(),
            extent: 0,
        };
        ds.add_work(upstairs_id, JobId(1000), rio)?;

        let rio = IOop::ExtentFlushClose {
            dependencies: vec![],
            extent: 1,
            flush_number: 1,
            gen_number: gen,
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
        };
        ds.add_work(upstairs_id, JobId(1001), rio)?;

        // Add the two reopen commands for the two extents we closed.
        let rio = IOop::ExtentLiveReopen {
            dependencies: vec![JobId(1000)],
            extent: 0,
        };
        ds.add_work(upstairs_id, JobId(1002), rio)?;
        let rio = IOop::ExtentLiveReopen {
            dependencies: vec![JobId(1001)],
            extent: 1,
        };
        ds.add_work(upstairs_id, JobId(1003), rio)?;
        show_work(&mut ds);

        let new_work = ds.new_work(upstairs_id);
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 4);

        // Process the ExtentClose
        let ds_work = ds.in_progress(upstairs_id, JobId(1000)).unwrap();
        let m = ds.do_work(&ds_work, upstairs_connection).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was unwritten, the close would not have
        // changed the generation number nor flush number, and dirty bit
        // should be false.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1000));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 0);
                assert_eq!(*f, 0);
                assert!(!*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_id, JobId(1000), m);

        // Process the ExtentFlushClose
        let ds_work = ds.in_progress(upstairs_id, JobId(1001)).unwrap();
        let m = ds.do_work(&ds_work, upstairs_connection).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was unwritten, the close would not have
        // changed the generation number nor flush number, and dirty bit
        // should be false.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1001));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 0);
                assert_eq!(*f, 0);
                assert!(!*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_id, JobId(1001), m);

        // Process the two ExtentReopen commands
        for id in (1002..=1003).map(JobId) {
            let ds_work = ds.in_progress(upstairs_id, id).unwrap();
            let m = ds.do_work(&ds_work, upstairs_connection).await;
            match m {
                Message::ExtentLiveAckId {
                    upstairs_id,
                    session_id,
                    job_id,
                    ref result,
                } => {
                    assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                    assert_eq!(session_id, upstairs_connection.session_id);
                    assert_eq!(job_id, id);
                    assert!(result.is_ok());
                }
                _ => {
                    panic!("Incorrect message: {:?} for id: {}", m, id);
                }
            }
            ds.complete_work(upstairs_id, id, m);
        }

        // Nothing should be left on the queue.
        let new_work = ds.new_work(upstairs_id);
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    // A test function that will return a generic crucible write command
    // for use when building the IOop::Write structure.  The data (of 9's)
    // matches the hash.
    fn create_generic_test_write(eid: u64) -> Vec<crucible_protocol::Write> {
        let data = BytesMut::from(&[9u8; 512][..]);
        let offset = Block::new_512(1);

        vec![crucible_protocol::Write {
            eid,
            offset,
            data: data.freeze(),
            block_context: BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    },
                ),
                hash: 14137680576404864188, // Hash for all 9s
            },
        }]
    }

    #[tokio::test]
    async fn test_extent_write_flush_write_close() -> Result<()> {
        // Send IOops for write, flush, write, then ExtentClose
        // We are testing here that the flush will make the metadata for
        // the first write (the gen/flush) persistent.  The 2nd write won't
        // change what is returned by the ExtentClose, as that data
        // was specifically not flushed, but the dirty bit should be set.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let (mut ds, _chan) =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;
        let upstairs_id = create_test_upstairs(&mut ds);
        let upstairs_connection =
            ds.connections[&upstairs_id].upstairs_connection();

        let eid = 3;

        // Create and add the first write
        let writes = create_generic_test_write(eid);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.add_work(upstairs_id, JobId(1000), rio)?;

        // add work for flush 1001
        let rio = IOop::Flush {
            dependencies: vec![],
            flush_number: 3,
            gen_number: gen,
            snapshot_details: None,
            extent_limit: None,
        };
        ds.add_work(upstairs_id, JobId(1001), rio)?;

        // Add work for 2nd write 1002
        let writes = create_generic_test_write(eid);

        let rio = IOop::Write {
            dependencies: vec![JobId(1000), JobId(1001)],
            writes,
        };
        ds.add_work(upstairs_id, JobId(1002), rio)?;

        // Now close the extent
        let rio = IOop::ExtentClose {
            dependencies: vec![JobId(1000), JobId(1001), JobId(1002)],
            extent: eid as usize,
        };
        ds.add_work(upstairs_id, JobId(1003), rio)?;

        show_work(&mut ds);

        let new_work = ds.new_work(upstairs_id);
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 4);

        // Process the first Write
        let ds_work = ds.in_progress(upstairs_id, JobId(1000)).unwrap();
        let m = ds.do_work(&ds_work, upstairs_connection).await;
        ds.complete_work(upstairs_id, JobId(1000), m);

        // Process the flush
        let ds_work = ds.in_progress(upstairs_id, JobId(1001)).unwrap();
        let m = ds.do_work(&ds_work, upstairs_connection).await;
        ds.complete_work(upstairs_id, JobId(1001), m);

        // Process write 2
        let ds_work = ds.in_progress(upstairs_id, JobId(1002)).unwrap();
        let m = ds.do_work(&ds_work, upstairs_connection).await;
        ds.complete_work(upstairs_id, JobId(1002), m);

        let new_work = ds.new_work(upstairs_id);
        assert_eq!(new_work.len(), 1);

        // Process the ExtentClose
        let ds_work = ds.in_progress(upstairs_id, JobId(1003)).unwrap();
        let m = ds.do_work(&ds_work, upstairs_connection).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written but not flushed, the close would
        // not have changed the generation number nor flush number But, the
        // dirty bit should be true.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1003));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 10); // From the flush
                assert_eq!(*f, 3); // From the flush
                assert!(*d); // Dirty should be true.
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_id, JobId(1003), m);

        // Nothing should be left on the queue.
        let new_work = ds.new_work(upstairs_id);
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_extent_write_close() -> Result<()> {
        // Test sending IOops for Write then ExtentClose.
        // Because we are not sending a flush here, we expect only the
        // dirty bit to be set when we look at the close results.
        // For the write, we do verify that the return contents in the
        // message are as we expect.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;

        let (mut ds, _chan) =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;
        let upstairs_id = create_test_upstairs(&mut ds);
        let upstairs_connection =
            ds.connections[&upstairs_id].upstairs_connection();

        let eid = 0;

        // Create the write
        let writes = create_generic_test_write(eid);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.add_work(upstairs_id, JobId(1000), rio)?;

        let rio = IOop::ExtentClose {
            dependencies: vec![JobId(1000)],
            extent: eid as usize,
        };
        ds.add_work(upstairs_id, JobId(1001), rio)?;

        show_work(&mut ds);

        let new_work = ds.new_work(upstairs_id);
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 2);

        // Process the Write
        let ds_work = ds.in_progress(upstairs_id, JobId(1000)).unwrap();
        let m = ds.do_work(&ds_work, upstairs_connection).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.
        match m {
            Message::WriteAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1000));
                assert!(result.is_ok());
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_id, JobId(1000), m);

        let new_work = ds.new_work(upstairs_id);
        assert_eq!(new_work.len(), 1);

        // Process the ExtentClose
        let ds_work = ds.in_progress(upstairs_id, JobId(1001)).unwrap();
        let m = ds.do_work(&ds_work, upstairs_connection).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written but not flushed, the close would
        // not have changed the generation number nor flush number But, the
        // dirty bit should be true.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1001));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 0);
                assert_eq!(*f, 0);
                assert!(*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_id, JobId(1001), m);

        // Nothing should be left on the queue.
        let new_work = ds.new_work(upstairs_id);
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_extent_write_flush_close() -> Result<()> {
        // Test sending IOops for Write then ExtentFlushClose
        // Verify we get the expected results.  Because we will be
        // writing to the extents, we expect to get different results
        // than in the non-writing case.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let (mut ds, _chan) =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;
        let upstairs_id = create_test_upstairs(&mut ds);
        let upstairs_connection =
            ds.connections[&upstairs_id].upstairs_connection();

        let eid = 1;

        // Create the write
        let writes = create_generic_test_write(eid);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.add_work(upstairs_id, JobId(1000), rio)?;

        let rio = IOop::ExtentFlushClose {
            dependencies: vec![JobId(1000)],
            extent: eid as usize,
            flush_number: 3,
            gen_number: gen,
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
        };
        ds.add_work(upstairs_id, JobId(1001), rio)?;

        show_work(&mut ds);

        let new_work = ds.new_work(upstairs_id);
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 2);

        // Process the Write
        let ds_work = ds.in_progress(upstairs_id, JobId(1000)).unwrap();
        let m = ds.do_work(&ds_work, upstairs_connection).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.
        match m {
            Message::WriteAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1000));
                assert!(result.is_ok());
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_id, JobId(1000), m);

        let new_work = ds.new_work(upstairs_id);
        assert_eq!(new_work.len(), 1);

        // Process the ExtentFlushClose
        let ds_work = ds.in_progress(upstairs_id, JobId(1001)).unwrap();
        let m = ds.do_work(&ds_work, upstairs_connection).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written, and we sent a "flush and close"
        // the data that we wrote should be committed and our flush should
        // have persisted that data.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1001));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, gen);
                assert_eq!(*f, 3);
                assert!(!*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_id, JobId(1001), m);

        // Nothing should be left on the queue.
        let new_work = ds.new_work(upstairs_id);
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_extent_write_write_flush_close() -> Result<()> {
        // Test sending IOops for two different Writes then an
        // ExtentFlushClose on one extent, and then a ExtentClose
        // on the other extent.
        // Verify that the first extent close returns data including
        // a flush, and the second extent remains dirty (no flush pollution
        // from the flush on the first extent).
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let (mut ds, _chan) =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;
        let upstairs_id = create_test_upstairs(&mut ds);
        let upstairs_connection =
            ds.connections[&upstairs_id].upstairs_connection();

        let eid_one = 1;
        let eid_two = 2;

        // Create the write for extent 1
        let writes = create_generic_test_write(eid_one);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.add_work(upstairs_id, JobId(1000), rio)?;

        // Create the write for extent 2
        let writes = create_generic_test_write(eid_two);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.add_work(upstairs_id, JobId(1001), rio)?;

        // Flush and close extent 1
        let rio = IOop::ExtentFlushClose {
            dependencies: vec![JobId(1000)],
            extent: eid_one as usize,
            flush_number: 6,
            gen_number: gen,
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
        };
        ds.add_work(upstairs_id, JobId(1002), rio)?;

        // Just close extent 2
        let rio = IOop::ExtentClose {
            dependencies: vec![JobId(1001)],
            extent: eid_two as usize,
        };
        ds.add_work(upstairs_id, JobId(1003), rio)?;

        show_work(&mut ds);

        let new_work = ds.new_work(upstairs_id);
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 4);

        // Process the Writes
        for id in (1000..=1001).map(JobId) {
            let ds_work = ds.in_progress(upstairs_id, id).unwrap();
            let m = ds.do_work(&ds_work, upstairs_connection).await;
            ds.complete_work(upstairs_id, id, m);
        }

        // Process the ExtentFlushClose
        let ds_work = ds.in_progress(upstairs_id, JobId(1002)).unwrap();
        let m = ds.do_work(&ds_work, upstairs_connection).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written, and we sent a "flush and close"
        // the data that we wrote should be committed and our flush should
        // have persisted that data.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1002));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, gen);
                assert_eq!(*f, 6);
                assert!(!*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_id, JobId(1002), m);

        // Process the ExtentClose
        let ds_work = ds.in_progress(upstairs_id, JobId(1003)).unwrap();
        let m = ds.do_work(&ds_work, upstairs_connection).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written, and we sent a "flush and close"
        // the data that we wrote should be committed and our flush should
        // have persisted that data.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1003));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 0);
                assert_eq!(*f, 0);
                assert!(*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_id, JobId(1003), m);
        // Nothing should be left on the queue.
        let new_work = ds.new_work(upstairs_id);
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[test]
    fn jobs_write_unwritten() {
        // Verify WriteUnwritten jobs move through the work queue
        let mut work = Work::new();
        add_work_rf(&mut work, JobId(1000), vec![]);

        assert_eq!(work.new_work(), vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1000)]
        );

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        assert!(test_push_next_jobs(&mut work,).is_empty());
    }

    fn test_misc_work_through_work_queue(ds_id: JobId, ioop: IOop) {
        // Verify that a IOop work request will move through the work queue.
        let mut work = Work::new();
        work.add_work(
            ds_id,
            DownstairsWork {
                ds_id,
                work: ioop,
                state: WorkState::New,
            },
        );

        assert_eq!(work.new_work(), vec![ds_id]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![ds_id]
        );

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![ds_id]);

        assert!(test_push_next_jobs(&mut work,).is_empty());
    }

    #[test]
    fn jobs_extent_close() {
        // Verify ExtentClose jobs move through the work queue
        let eid = 1;
        let ioop = IOop::ExtentClose {
            dependencies: vec![],
            extent: eid,
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_extent_flush_close() {
        // Verify ExtentFlushClose jobs move through the work queue

        let eid = 1;
        let ioop = IOop::ExtentFlushClose {
            dependencies: vec![],
            extent: eid,
            flush_number: 1,
            gen_number: 2,
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_extent_live_repair() {
        // Verify ExtentLiveRepair jobs move through the work queue

        let eid = 1;
        let source_repair_address =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        let ioop = IOop::ExtentLiveRepair {
            dependencies: vec![],
            extent: eid,
            source_downstairs: ClientId::new(0),
            source_repair_address,
            repair_downstairs: vec![ClientId::new(1)],
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_extent_live_reopen() {
        // Verify ExtentLiveReopen jobs move through the work queue
        let eid = 1;

        let ioop = IOop::ExtentLiveReopen {
            dependencies: vec![],
            extent: eid,
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_extent_live_noop() {
        // Verify ExtentLiveNoOp jobs move through the work queue

        let ioop = IOop::ExtentLiveNoOp {
            dependencies: vec![],
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_independent() {
        let mut work = Work::new();

        // Add two independent jobs
        add_work(&mut work, JobId(1000), vec![], false);
        add_work(&mut work, JobId(1001), vec![], false);

        // new_work returns all new jobs
        assert_eq!(work.new_work(), vec![JobId(1000), JobId(1001)]);

        // should push both, they're independent
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1000), JobId(1001)]
        );

        // new work returns only jobs in new or dep wait
        assert!(work.new_work().is_empty());

        // do work
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000), JobId(1001)]);

        assert!(test_push_next_jobs(&mut work,).is_empty());
    }

    #[test]
    fn unblock_job() {
        let mut work = Work::new();

        // Add two jobs, one blocked on another
        add_work(&mut work, JobId(1000), vec![], false);
        add_work(&mut work, JobId(1001), vec![JobId(1000)], false);

        // new_work returns all new or dep wait jobs
        assert_eq!(work.new_work(), vec![JobId(1000), JobId(1001)]);

        // only one is ready to run
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1000)]
        );

        // new_work returns all new or dep wait jobs
        assert_eq!(work.new_work(), vec![JobId(1001)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1001)]
        );
    }

    #[test]
    fn unblock_job_chain() {
        let mut work = Work::new();

        // Add three jobs all blocked on each other in a chain
        add_work(&mut work, JobId(1000), vec![], false);
        add_work(&mut work, JobId(1001), vec![JobId(1000)], false);
        add_work(
            &mut work,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        // new_work returns all new or dep wait jobs
        assert_eq!(
            work.new_work(),
            vec![JobId(1000), JobId(1001), JobId(1002)]
        );

        // only one is ready to run at a time

        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1000)]
        );
        assert_eq!(work.new_work(), vec![JobId(1001), JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1001)]
        );
        assert_eq!(work.new_work(), vec![JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000), JobId(1001)]);
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1002)]
        );
        assert!(work.new_work().is_empty());

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000), JobId(1001), JobId(1002)]);
    }

    #[test]
    fn unblock_job_chain_first_is_flush() {
        let mut work = Work::new();

        // Add three jobs all blocked on each other in a chain, first is flush
        add_work(&mut work, JobId(1000), vec![], true);
        add_work(&mut work, JobId(1001), vec![JobId(1000)], false);
        add_work(
            &mut work,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        // new_work returns all new or dep wait jobs
        assert_eq!(
            work.new_work(),
            vec![JobId(1000), JobId(1001), JobId(1002)]
        );

        // only one is ready to run at a time

        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1000)]
        );
        assert_eq!(work.new_work(), vec![JobId(1001), JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1000));
        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1001)]
        );
        assert_eq!(work.new_work(), vec![JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1000));
        assert_eq!(work.completed, vec![JobId(1001)]);
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1002)]
        );
        assert!(work.new_work().is_empty());

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1000));
        assert_eq!(work.completed, vec![JobId(1001), JobId(1002)]);
    }

    #[test]
    fn unblock_job_chain_second_is_flush() {
        let mut work = Work::new();

        // Add three jobs all blocked on each other in a chain, second is flush
        add_work(&mut work, JobId(1000), vec![], false);
        add_work(&mut work, JobId(1001), vec![JobId(1000)], true);
        add_work(
            &mut work,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        // new_work returns all new or dep wait jobs
        assert_eq!(
            work.new_work(),
            vec![JobId(1000), JobId(1001), JobId(1002)]
        );

        // only one is ready to run at a time

        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1000)]
        );
        assert_eq!(work.new_work(), vec![JobId(1001), JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1001)]
        );
        assert_eq!(work.new_work(), vec![JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1001));
        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1002)]
        );
        assert!(work.new_work().is_empty());

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1001));
        assert_eq!(work.completed, vec![JobId(1002)]);
    }

    #[test]
    fn unblock_job_upstairs_sends_big_deps() {
        let mut work = Work::new();

        // Add three jobs all blocked on each other
        add_work(&mut work, JobId(1000), vec![], false);
        add_work(&mut work, JobId(1001), vec![JobId(1000)], false);
        add_work(&mut work, JobId(1002), vec![JobId(1000), JobId(1001)], true);

        // Downstairs is really fast!
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1000)]
        );
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1001)]
        );
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1002)]
        );
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1002));
        assert!(work.completed.is_empty());

        // Upstairs sends a job with these three in deps, not knowing Downstairs
        // has done the jobs already
        add_work(
            &mut work,
            JobId(1003),
            vec![JobId(1000), JobId(1001), JobId(1002)],
            false,
        );
        add_work(
            &mut work,
            JobId(1004),
            vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1003)]
        );
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1004)]
        );
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1002));
        assert_eq!(work.completed, vec![JobId(1003), JobId(1004)]);
    }

    #[test]
    fn job_dep_not_satisfied() {
        let mut work = Work::new();

        // Add three jobs all blocked on each other
        add_work(&mut work, JobId(1000), vec![], false);
        add_work(&mut work, JobId(1001), vec![JobId(1000)], false);
        add_work(&mut work, JobId(1002), vec![JobId(1000), JobId(1001)], true);

        // Add one that can't run yet
        add_work(&mut work, JobId(1003), vec![JobId(2000)], false);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1000)]
        );
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1001)]
        );
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1002)]
        );
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1002));
        assert!(work.completed.is_empty());

        assert_eq!(work.new_work(), vec![JobId(1003)]);
        assert_eq!(
            work.active.get(&JobId(1003)).unwrap().state,
            WorkState::DepWait
        );
    }

    #[test]
    fn two_job_chains() {
        let mut work = Work::new();

        // Add three jobs all blocked on each other
        add_work(&mut work, JobId(1000), vec![], false);
        add_work(&mut work, JobId(1001), vec![JobId(1000)], false);
        add_work(
            &mut work,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        // Add another set of jobs blocked on each other
        add_work(&mut work, JobId(2000), vec![], false);
        add_work(&mut work, JobId(2001), vec![JobId(2000)], false);
        add_work(&mut work, JobId(2002), vec![JobId(2000), JobId(2001)], true);

        // should do each chain in sequence
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1000), JobId(2000)]
        );
        test_do_work(&mut work, next_jobs);
        assert_eq!(work.completed, vec![JobId(1000), JobId(2000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1001), JobId(2001)]
        );
        test_do_work(&mut work, next_jobs);
        assert_eq!(
            work.completed,
            vec![JobId(1000), JobId(2000), JobId(1001), JobId(2001)]
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1002), JobId(2002)]
        );
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(2002));
        assert!(work.completed.is_empty());
    }

    #[test]
    fn out_of_order_arrives_after_first_push_next_jobs() {
        /*
         * Test that jobs arriving out of order still complete.
         */
        let mut work = Work::new();

        // Add three jobs all blocked on each other (missing 1002)
        add_work(&mut work, JobId(1000), vec![], false);
        add_work(&mut work, JobId(1001), vec![JobId(1000)], false);
        add_work(
            &mut work,
            JobId(1003),
            vec![JobId(1000), JobId(1001), JobId(1002)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1000)]
        );

        add_work(
            &mut work,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1001)]
        );
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1002)]
        );
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1003)]
        );
        test_do_work(&mut work, next_jobs);

        assert_eq!(
            work.completed,
            vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)]
        );
    }

    #[test]
    fn out_of_order_arrives_after_first_do_work() {
        /*
         * Test that jobs arriving out of order still complete.
         */
        let mut work = Work::new();

        // Add three jobs all blocked on each other (missing 1002)
        add_work(&mut work, JobId(1000), vec![], false);
        add_work(&mut work, JobId(1001), vec![JobId(1000)], false);
        add_work(
            &mut work,
            JobId(1003),
            vec![JobId(1000), JobId(1001), JobId(1002)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1000)]
        );

        test_do_work(&mut work, next_jobs);

        add_work(
            &mut work,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        assert_eq!(work.completed, vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1001)]
        );
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1002)]
        );
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1003)]
        );
        test_do_work(&mut work, next_jobs);

        assert_eq!(
            work.completed,
            vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)]
        );
    }

    #[test]
    fn out_of_order_arrives_after_1001_completes() {
        /*
         * Test that jobs arriving out of order still complete.
         */
        let mut work = Work::new();

        // Add three jobs all blocked on each other (missing 1002)
        add_work(&mut work, JobId(1000), vec![], false);
        add_work(&mut work, JobId(1001), vec![JobId(1000)], false);
        add_work(
            &mut work,
            JobId(1003),
            vec![JobId(1000), JobId(1001), JobId(1002)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1000)]
        );

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1001)]
        );
        test_do_work(&mut work, next_jobs);

        // can't run anything, dep not satisfied
        let next_jobs = test_push_next_jobs(&mut work);
        assert!(next_jobs.is_empty());
        test_do_work(&mut work, next_jobs);

        add_work(
            &mut work,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1002)]
        );
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(
            next_jobs.iter().map(|w| w.ds_id).collect::<Vec<_>>(),
            vec![JobId(1003)]
        );
        test_do_work(&mut work, next_jobs);

        assert_eq!(
            work.completed,
            vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)]
        );
    }

    #[tokio::test]
    async fn import_test_basic() -> Result<()> {
        /*
         * import + export test where data matches region size
         */

        let block_size: u64 = 512;
        let extent_size = 10;

        // create region

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(10).await?;

        // create random file

        let total_bytes = region.def().total_size();
        let mut random_data = vec![0; total_bytes as usize];
        random_data.resize(total_bytes as usize, 0);

        let mut rng = ChaCha20Rng::from_entropy();
        rng.fill_bytes(&mut random_data);

        // write random_data to file

        let tempdir = tempdir()?;
        mkdir_for_file(tempdir.path())?;

        let random_file_path = tempdir.path().join("random_data");
        let mut random_file = File::create(&random_file_path)?;
        random_file.write_all(&random_data[..])?;

        // import random_data to the region

        downstairs_import(&mut region, &random_file_path).await?;
        region.region_flush(1, 1, &None, JobId(0), None).await?;

        // export region to another file

        let export_path = tempdir.path().join("exported_data");
        downstairs_export(
            &mut region,
            &export_path,
            0,
            total_bytes / block_size,
        )
        .await?;

        // compare files

        let expected = std::fs::read(random_file_path)?;
        let actual = std::fs::read(export_path)?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn import_test_too_small() -> Result<()> {
        /*
         * import + export test where data is smaller than region size
         */
        let block_size: u64 = 512;
        let extent_size = 10;

        // create region

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(10).await?;

        // create random file (100 fewer bytes than region size)

        let total_bytes = region.def().total_size() - 100;
        let mut random_data = vec![0; total_bytes as usize];
        random_data.resize(total_bytes as usize, 0);

        let mut rng = ChaCha20Rng::from_entropy();
        rng.fill_bytes(&mut random_data);

        // write random_data to file

        let tempdir = tempdir()?;
        mkdir_for_file(tempdir.path())?;

        let random_file_path = tempdir.path().join("random_data");
        let mut random_file = File::create(&random_file_path)?;
        random_file.write_all(&random_data[..])?;

        // import random_data to the region

        downstairs_import(&mut region, &random_file_path).await?;
        region.region_flush(1, 1, &None, JobId(0), None).await?;

        // export region to another file (note: 100 fewer bytes imported than
        // region size still means the whole region is exported)

        let export_path = tempdir.path().join("exported_data");
        let region_size = region.def().total_size();
        downstairs_export(
            &mut region,
            &export_path,
            0,
            region_size / block_size,
        )
        .await?;

        // compare files

        let expected = std::fs::read(random_file_path)?;
        let actual = std::fs::read(export_path)?;

        // assert what was imported is correct

        let total_bytes = total_bytes as usize;
        assert_eq!(expected, actual[0..total_bytes]);

        // assert the rest is zero padded

        let padding_size = actual.len() - total_bytes;
        assert_eq!(padding_size, 100);

        let mut padding = vec![0; padding_size];
        padding.resize(padding_size, 0);
        assert_eq!(actual[total_bytes..], padding);

        Ok(())
    }

    #[tokio::test]
    async fn import_test_too_large() -> Result<()> {
        /*
         * import + export test where data is larger than region size
         */
        let block_size: u64 = 512;
        let extent_size = 10;

        // create region

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(10).await?;

        // create random file (100 more bytes than region size)

        let total_bytes = region.def().total_size() + 100;
        let mut random_data = vec![0; total_bytes as usize];
        random_data.resize(total_bytes as usize, 0);

        let mut rng = ChaCha20Rng::from_entropy();
        rng.fill_bytes(&mut random_data);

        // write random_data to file

        let tempdir = tempdir()?;
        mkdir_for_file(tempdir.path())?;

        let random_file_path = tempdir.path().join("random_data");
        let mut random_file = File::create(&random_file_path)?;
        random_file.write_all(&random_data[..])?;

        // import random_data to the region

        downstairs_import(&mut region, &random_file_path).await?;
        region.region_flush(1, 1, &None, JobId(0), None).await?;

        // export region to another file (note: 100 more bytes will have caused
        // 10 more extents to be added, but someone running the export command
        // will use the number of blocks copied by the import command)
        assert_eq!(region.def().extent_count(), 11);

        let export_path = tempdir.path().join("exported_data");
        downstairs_export(
            &mut region,
            &export_path,
            0,
            total_bytes / block_size + 1,
        )
        .await?;

        // compare files

        let expected = std::fs::read(random_file_path)?;
        let actual = std::fs::read(export_path)?;

        // assert what was imported is correct

        let total_bytes = total_bytes as usize;
        assert_eq!(expected, actual[0..total_bytes]);

        // assert the rest is zero padded
        // the export only exported the extra block, not the extra extent
        let padding_in_extra_block: usize = 512 - 100;

        let mut padding = vec![0; padding_in_extra_block];
        padding.resize(padding_in_extra_block, 0);
        assert_eq!(actual[total_bytes..], padding);

        Ok(())
    }

    #[tokio::test]
    async fn import_test_basic_read_blocks() -> Result<()> {
        /*
         * import + export test where data matches region size, and read the
         * blocks
         */
        let block_size: u64 = 512;
        let extent_size = 10;

        // create region

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(10).await?;

        // create random file

        let total_bytes = region.def().total_size();
        let mut random_data = vec![0u8; total_bytes as usize];
        random_data.resize(total_bytes as usize, 0u8);

        let mut rng = ChaCha20Rng::from_entropy();
        rng.fill_bytes(&mut random_data);

        // write random_data to file

        let tempdir = tempdir()?;
        mkdir_for_file(tempdir.path())?;

        let random_file_path = tempdir.path().join("random_data");
        let mut random_file = File::create(&random_file_path)?;
        random_file.write_all(&random_data[..])?;

        // import random_data to the region

        downstairs_import(&mut region, &random_file_path).await?;
        region.region_flush(1, 1, &None, JobId(0), None).await?;

        // read block by block
        let mut read_data = Vec::with_capacity(total_bytes as usize);
        for eid in 0..region.def().extent_count() {
            for offset in 0..region.def().extent_size().value {
                let responses = region
                    .region_read(
                        &[crucible_protocol::ReadRequest {
                            eid: eid.into(),
                            offset: Block::new_512(offset),
                        }],
                        JobId(0),
                    )
                    .await?;

                assert_eq!(responses.len(), 1);

                let response = &responses[0];
                assert_eq!(response.hashes().len(), 1);
                assert_eq!(
                    integrity_hash(&[&response.data[..]]),
                    response.hashes()[0],
                );

                read_data.extend_from_slice(&response.data[..]);
            }
        }

        assert_eq!(random_data, read_data);

        Ok(())
    }

    async fn build_test_downstairs(
        read_only: bool,
    ) -> Result<(Downstairs, DownstairsIoHandle)> {
        let block_size: u64 = 512;
        let extent_size = 4;

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(2).await?;

        let path_dir = dir.as_ref().to_path_buf();

        build_downstairs_for_region(
            &path_dir,
            false, // lossy
            false, // read errors
            false, // write errors
            false, // flush errors
            read_only,
            Some(csl()),
        )
        .await
    }

    /// Build a fake `Upstairs` that ignores all messages and sends nothing
    ///
    /// This is necessary because we need a `JoinHandle`.
    fn fake_upstairs(state: UpstairsState, log: &Logger) -> Upstairs {
        let (tx, mut rx) = mpsc::channel(500);
        let fake_io_task = tokio::task::spawn(async move {
            while let Some(_v) = rx.recv().await {
                // Nothing to do here
            }
        });

        Upstairs {
            state,
            message_tx: tx,
            timeout_deadline: deadline_secs(10),
            log: log.new(o!("role" => "test_upstairs")),
            io_task: fake_io_task,
        }
    }

    /// Inserts the given upstairs as a connection that's mid-negotiation
    fn insert_negotiating(
        ds: &mut Downstairs,
        c: UpstairsConnection,
    ) -> ConnectionId {
        let id = ds.next_connection_id();
        ds.connections.insert(
            id,
            fake_upstairs(
                UpstairsState::Negotiating {
                    negotiated: NegotiationState::ConnectedToUpstairs,
                    upstairs_connection: Some(c),
                },
                &ds.log,
            ),
        );
        id
    }

    /// Switch the given connection's state to Running
    fn make_running(ds: &mut Downstairs, id: ConnectionId) {
        let up = ds.connections.get_mut(&id).unwrap();
        let upstairs_connection = up.upstairs_connection();
        up.state = UpstairsState::Running {
            work: Work::new(),
            upstairs_connection,
        };
    }

    #[tokio::test]
    async fn test_promote_to_active_one_read_write() -> Result<()> {
        let (mut ds, _chan) = build_test_downstairs(false).await?;

        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };
        let id = ds.next_connection_id();
        ds.connections.insert(
            id,
            fake_upstairs(
                UpstairsState::Negotiating {
                    negotiated: NegotiationState::ConnectedToUpstairs,
                    upstairs_connection: Some(upstairs_connection),
                },
                &ds.log,
            ),
        );

        ds.promote_to_active(id).await?;
        assert_eq!(ds.active_upstairs.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_one_read_only() -> Result<()> {
        let (mut ds, _chan) = build_test_downstairs(true).await?;

        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };
        let id = ds.next_connection_id();
        ds.connections.insert(
            id,
            fake_upstairs(
                UpstairsState::Negotiating {
                    negotiated: NegotiationState::ConnectedToUpstairs,
                    upstairs_connection: Some(upstairs_connection),
                },
                &ds.log,
            ),
        );

        ds.promote_to_active(id).await?;
        assert_eq!(ds.active_upstairs.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_write_different_uuid_same_gen(
    ) -> Result<()> {
        // Attempting to activate multiple read-write (where it's different
        // Upstairs) but with the same gen should be blocked
        let (mut ds, _chan) = build_test_downstairs(false).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let id_1 = insert_negotiating(&mut ds, upstairs_connection_1);
        let id_2 = insert_negotiating(&mut ds, upstairs_connection_2);

        ds.promote_to_active(id_1).await?;
        assert_eq!(ds.active_upstairs.len(), 1);

        let res = ds.promote_to_active(id_2).await;
        assert!(res.is_err());

        assert_eq!(ds.active_upstairs.len(), 1);

        // Original connection is still active.
        assert!(ds.is_active(upstairs_connection_1));
        // New connection was blocked.
        assert!(!ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_write_different_uuid_lower_gen(
    ) -> Result<()> {
        // Attempting to activate multiple read-write (where it's different
        // Upstairs) but with a lower gen should be blocked.
        let (mut ds, _chan) = build_test_downstairs(false).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 2,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let id_1 = insert_negotiating(&mut ds, upstairs_connection_1);
        let id_2 = insert_negotiating(&mut ds, upstairs_connection_2);

        println!("ds1: {:?}", ds);
        ds.promote_to_active(id_1).await?;
        println!("\nds2: {:?}\n", ds);

        assert_eq!(ds.active_upstairs.len(), 1);

        let res = ds.promote_to_active(id_2).await;
        assert!(res.is_err());
        assert_eq!(ds.active_upstairs.len(), 1);

        // Original connection is still active.
        assert!(ds.is_active(upstairs_connection_1));
        // New connection was blocked.
        assert!(!ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_write_same_uuid_same_gen(
    ) -> Result<()> {
        // Attempting to activate multiple read-write (where it's the same
        // Upstairs but a different session) will block the "new" connection
        // if it has the same generation number.
        let (mut ds, _chan) = build_test_downstairs(false).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: upstairs_connection_1.upstairs_id,
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let id_1 = insert_negotiating(&mut ds, upstairs_connection_1);
        let id_2 = insert_negotiating(&mut ds, upstairs_connection_2);

        ds.promote_to_active(id_1).await?;

        assert_eq!(ds.active_upstairs.len(), 1);
        let res = ds.promote_to_active(id_2).await;
        assert!(res.is_err());

        assert_eq!(ds.active_upstairs.len(), 1);

        assert!(ds.is_active(upstairs_connection_1));
        assert!(!ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_write_same_uuid_larger_gen(
    ) -> Result<()> {
        // Attempting to activate multiple read-write where it's the same
        // Upstairs, but a different session, and with a larger generation
        // should allow the new connection to take over.
        let (mut ds, _chan) = build_test_downstairs(false).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: upstairs_connection_1.upstairs_id,
            session_id: Uuid::new_v4(),
            gen: 2,
        };

        let id_1 = insert_negotiating(&mut ds, upstairs_connection_1);
        let id_2 = insert_negotiating(&mut ds, upstairs_connection_2);

        ds.promote_to_active(id_1).await?;
        assert_eq!(ds.active_upstairs.len(), 1);

        ds.promote_to_active(id_2).await?;
        // TODO somehow check that upstairs_connection_1 was closed?

        assert_eq!(ds.active_upstairs.len(), 1);

        assert!(!ds.is_active(upstairs_connection_1));
        assert!(ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_only_different_uuid(
    ) -> Result<()> {
        // Activating multiple read-only with different Upstairs UUIDs should
        // work.
        let (mut ds, _chan) = build_test_downstairs(true).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let id_1 = insert_negotiating(&mut ds, upstairs_connection_1);
        let id_2 = insert_negotiating(&mut ds, upstairs_connection_2);

        ds.promote_to_active(id_1).await?;
        assert_eq!(ds.active_upstairs.len(), 1);

        ds.promote_to_active(id_2).await?;
        assert_eq!(ds.active_upstairs.len(), 2);

        assert!(ds.is_active(upstairs_connection_1));
        assert!(ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_only_same_uuid() -> Result<()> {
        // Activating multiple read-only with the same Upstairs UUID should
        // kick out the other active one.
        let (mut ds, _chan) = build_test_downstairs(true).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: upstairs_connection_1.upstairs_id,
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let id_1 = insert_negotiating(&mut ds, upstairs_connection_1);
        let id_2 = insert_negotiating(&mut ds, upstairs_connection_2);

        ds.promote_to_active(id_1).await?;

        assert_eq!(ds.active_upstairs.len(), 1);

        ds.promote_to_active(id_2).await?;
        // TODO Check that we've killed upstairs_connection_1?

        assert_eq!(ds.active_upstairs.len(), 1);

        assert!(!ds.is_active(upstairs_connection_1));
        assert!(ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_read_only_no_job_id_collision() -> Result<()> {
        // Two read-only Upstairs shouldn't see each other's jobs
        let (mut ds, _chan) = build_test_downstairs(true).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let id_1 = insert_negotiating(&mut ds, upstairs_connection_1);
        let id_2 = insert_negotiating(&mut ds, upstairs_connection_2);

        ds.promote_to_active(id_1).await?;
        assert_eq!(ds.active_upstairs.len(), 1);
        make_running(&mut ds, id_1);

        ds.promote_to_active(id_2).await?;
        assert_eq!(ds.active_upstairs.len(), 2);
        make_running(&mut ds, id_2);

        let read_1 = IOop::Read {
            dependencies: Vec::new(),
            requests: vec![ReadRequest {
                eid: 0,
                offset: Block::new_512(1),
            }],
        };
        ds.add_work(id_1, JobId(1000), read_1.clone())?;

        let read_2 = IOop::Read {
            dependencies: Vec::new(),
            requests: vec![ReadRequest {
                eid: 1,
                offset: Block::new_512(2),
            }],
        };
        ds.add_work(id_2, JobId(1000), read_2.clone())?;

        let work_1 = ds.new_work(id_1);
        let work_2 = ds.new_work(id_2);

        assert_eq!(work_1, work_2);

        let job_1 = ds.get_job(id_1, JobId(1000));
        let job_2 = ds.get_job(id_2, JobId(1000));

        assert_eq!(job_1.ds_id, JobId(1000));
        assert_eq!(job_1.work, read_1);
        assert_eq!(job_1.state, WorkState::New);

        assert_eq!(job_2.ds_id, JobId(1000));
        assert_eq!(job_2.work, read_2);
        assert_eq!(job_2.state, WorkState::New);

        Ok(())
    }

    /*
     * Test function that will start up a downstairs (at the provided port)
     * then create a tcp connection to that downstairs, returning the tcp
     * connection to the caller.
     */
    async fn start_ds_and_connect(
        listen_port: u16,
        repair_port: u16,
    ) -> Result<tokio::net::TcpStream> {
        /*
         * Pick some small enough values for what we need.
         */
        let bs = 512;
        let es = 4;
        let ec = 5;
        let dir = tempdir()?;

        let (ds, chan) = create_test_downstairs(bs, es, ec, &dir).await?;

        let _jh = start_downstairs(
            ds,
            chan,
            "127.0.0.1".parse().unwrap(),
            None,
            listen_port,
            repair_port,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let sock = TcpSocket::new_v4().unwrap();

        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            listen_port,
        );
        Ok(sock.connect(addr).await.unwrap())
    }

    #[tokio::test]
    async fn test_version_match() -> Result<()> {
        // A simple test to verify that sending the current crucible
        // message version to the downstairs will trigger a response
        // indicating the version is supported.
        let tcp = start_ds_and_connect(5555, 5556).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: Vec::new(),
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::YesItsMe {
                version,
                repair_addr,
            }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
                assert_eq!(repair_addr, "127.0.0.1:5556".parse().unwrap());
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_version_downrev() -> Result<()> {
        // Test that a newer crucible version will result in a message
        // indicating there is a version mismatch.
        let tcp = start_ds_and_connect(5557, 5558).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION - 1,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: vec![CRUCIBLE_MESSAGE_VERSION - 1],
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::VersionMismatch { version }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_version_uprev_only() -> Result<()> {
        // Test sending only the +1 version to the DS, verify it rejects
        // this version as unsupported.
        let tcp = start_ds_and_connect(5579, 5560).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION + 1,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: vec![CRUCIBLE_MESSAGE_VERSION + 1],
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::VersionMismatch { version }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn test_version_uprev_compatable() -> Result<()> {
        // Test sending the +1 version to the DS, but also include the
        // current version on the supported list.  The downstairs should
        // see that and respond with the version it does support.
        let tcp = start_ds_and_connect(5561, 5562).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION + 1,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: vec![
                CRUCIBLE_MESSAGE_VERSION,
                CRUCIBLE_MESSAGE_VERSION + 1,
            ],
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::YesItsMe {
                version,
                repair_addr,
            }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
                assert_eq!(repair_addr, "127.0.0.1:5562".parse().unwrap());
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn test_version_uprev_list() -> Result<()> {
        // Test sending an invalid version to the DS, but also include the
        // current version on the supported list, but with several
        // choices.
        let tcp = start_ds_and_connect(5563, 5564).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION + 4,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: vec![
                CRUCIBLE_MESSAGE_VERSION - 1,
                CRUCIBLE_MESSAGE_VERSION,
                CRUCIBLE_MESSAGE_VERSION + 1,
            ],
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::YesItsMe {
                version,
                repair_addr,
            }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
                assert_eq!(repair_addr, "127.0.0.1:5564".parse().unwrap());
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }
}
