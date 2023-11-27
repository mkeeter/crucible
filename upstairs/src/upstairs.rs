use crate::{
    cdt,
    client::{ClientAction, ClientRunResult, ClientStopReason},
    control::ControlRequest,
    deadline_secs,
    downstairs::{Downstairs, DownstairsAction},
    extent_from_offset, integrity_hash,
    stats::UpStatOuter,
    Block, BlockContext, BlockOp, BlockReq, Buffer, Bytes, ClientId, ClientMap,
    CrucibleOpts, DsState, EncryptionContext, GtoS, Guest, Message,
    RegionDefinition, RegionDefinitionStatus, SnapshotDetails, WQCounts,
};
use crucible_common::CrucibleError;

use std::{ops::DerefMut, sync::Arc};

use ringbuffer::RingBuffer;
use slog::{debug, error, info, o, warn, Logger};
use tokio::{
    sync::{mpsc, oneshot},
    time::{sleep_until, Instant},
};
use uuid::Uuid;

/// How often to log stats for DTrace
const STAT_INTERVAL_SECS: f32 = 1.0;

#[derive(Debug)]
pub(crate) enum UpstairsState {
    /// The upstairs is just coming online
    ///
    /// We can send IO on behalf of the upstairs (e.g. to handle negotiation),
    /// but not from the guest.
    Initializing,

    /// The guest has requested that the upstairs go active
    ///
    /// We should reply on the provided channel
    GoActive {
        reply: oneshot::Sender<Result<(), CrucibleError>>,
    },

    /// The upstairs is fully online and accepting guest IO
    Active,

    /// The upstairs is deactivating
    ///
    /// In-flight IO continues, but no new IO is allowed.  When all IO has been
    /// completed (including the final flush), the downstairs task should stop;
    /// when all three Downstairs have stopped, the upstairs should enter
    /// `UpstairsState::Initializing`.
    Deactivating,
}

pub(crate) struct Upstairs {
    /// Current state
    state: UpstairsState,

    /// Downstairs jobs and per-client state
    downstairs: Downstairs,

    /// Upstairs generation number
    ///
    /// This increases each time an Upstairs starts
    generation: u64,

    /// The guest struct keeps track of jobs accepted from the Guest
    ///
    /// A single job submitted can produce multiple downstairs requests.
    guest: Arc<Guest>,

    /// Region definition
    ///
    /// This is (optionally) provided on startup, and checked for consistency
    /// between all three Downstairs.
    ///
    /// The region definition allows us to translate an LBA to an extent and
    /// block offset.
    ddef: RegionDefinitionStatus,

    /// Marks whether a flush is needed
    ///
    /// The Upstairs keeps all IOs in memory until a flush is ACK'd back from
    /// all three downstairs.  If there are IOs we have accepted into the work
    /// queue that don't end with a flush, then we set this to indicate that the
    /// upstairs may need to issue a flush of its own to be sure that data is
    /// pushed to disk.  Note that this is not an indication of an ACK'd flush,
    /// just that the last IO command we put on the work queue was not a flush.
    need_flush: bool,

    /// Statistics for this upstairs
    ///
    /// Shared with the metrics producer, so this `struct` wraps a
    /// `std::sync::Mutex`
    pub(crate) stats: UpStatOuter,

    /// Fixed configuration
    cfg: Arc<UpstairsConfig>,

    /// Logger used by the upstairs
    pub(crate) log: Logger,

    /// Next time to check for repairs
    repair_check_interval: Option<Instant>,

    /// Next time to leak IOP / bandwidth tokens from the Guest
    leak_deadline: Instant,

    /// Next time to trigger an automatic flush
    flush_deadline: Instant,

    /// Next time to trigger a stats update
    stat_deadline: Instant,

    /// Interval between automatic flushes
    flush_timeout_secs: f32,

    /// Receiver queue for control requests
    control_rx: mpsc::Receiver<ControlRequest>,

    /// Sender handle for control requests
    ///
    /// This is public so that others can clone it to get a controller handle
    pub(crate) control_tx: mpsc::Sender<ControlRequest>,
}

#[derive(Debug)]
enum UpstairsAction {
    Downstairs(DownstairsAction),
    Guest(BlockReq),
    LeakCheck,
    FlushCheck,
    StatUpdate,
    RepairCheck,
    Control(ControlRequest),
}

#[derive(Debug)]
pub(crate) struct UpstairsConfig {
    /// Upstairs UUID
    pub upstairs_id: Uuid,

    /// Unique session ID
    pub session_id: Uuid,

    pub read_only: bool,

    /// Encryption context, if present
    ///
    /// This is `Some(..)` if a key is provided in the `CrucibleOpts`
    pub encryption_context: Option<EncryptionContext>,

    /// Does this Upstairs throw random errors?
    pub lossy: bool,
}

impl UpstairsConfig {
    pub(crate) fn encrypted(&self) -> bool {
        self.encryption_context.is_some()
    }
}

impl Upstairs {
    pub(crate) fn new(
        opt: &CrucibleOpts,
        gen: u64,
        expected_region_def: Option<RegionDefinition>,
        guest: Arc<Guest>,
        tls_context: Option<Arc<crucible_common::x509::TLSContext>>,
        log: Logger,
    ) -> Self {
        /*
         * XXX Make sure we have three and only three downstairs
         */
        #[cfg(not(test))]
        assert_eq!(opt.target.len(), 3, "bad targets {:?}", opt.target);

        // Build the target map, which is either empty (during some tests) or
        // fully populated with all three targets.
        let mut ds_target = ClientMap::new();
        for (i, v) in opt.target.iter().enumerate() {
            ds_target.insert(ClientId::new(i as u8), *v);
        }

        // Create an encryption context if a key is supplied.
        let encryption_context = opt.key_bytes().map(|key| {
            EncryptionContext::new(
                key,
                // XXX: Figure out what to do if no expected region definition
                // was supplied. It would be good to do BlockOp::QueryBlockSize
                // here, but this creates a deadlock. Upstairs::new runs before
                // up_ds_listen in up_main, and up_ds_listen needs to run to
                // answer BlockOp::QueryBlockSize. (Note that the downstairs
                // have not reported in yet, so if no expected definition was
                // supplied no downstairs information is available.)
                expected_region_def
                    .map(|rd| rd.block_size() as usize)
                    .unwrap_or(512),
            )
        });

        let uuid = opt.id;
        let stats = UpStatOuter::new(uuid);

        let rd_status = match expected_region_def {
            None => RegionDefinitionStatus::WaitingForDownstairs,
            Some(d) => RegionDefinitionStatus::ExpectingFromDownstairs(d),
        };

        let session_id = Uuid::new_v4();
        let log = log.new(o!("session_id" => session_id.to_string()));
        info!(log, "Crucible {} has session id: {}", uuid, session_id);
        info!(log, "Upstairs opts: {}", opt);

        let cfg = Arc::new(UpstairsConfig {
            encryption_context,
            upstairs_id: uuid,
            session_id,
            read_only: opt.read_only,
            lossy: opt.lossy,
        });

        info!(log, "Crucible stats registered with UUID: {}", uuid);
        let downstairs = Downstairs::new(
            cfg.clone(),
            ds_target,
            tls_context,
            log.new(o!("" => "downstairs")),
        );
        let flush_timeout_secs = opt.flush_timeout.unwrap_or(0.5);
        let (control_tx, control_rx) = tokio::sync::mpsc::channel(500);
        Upstairs {
            state: UpstairsState::Initializing,
            cfg,
            generation: gen,
            repair_check_interval: None,
            leak_deadline: deadline_secs(1.0),
            flush_deadline: deadline_secs(flush_timeout_secs),
            stat_deadline: deadline_secs(STAT_INTERVAL_SECS),
            flush_timeout_secs,
            guest,
            ddef: rd_status,
            need_flush: false,
            stats,
            log,
            downstairs,
            control_rx,
            control_tx,
        }
    }

    /// Build an Upstairs for simple tests
    #[cfg(test)]
    pub fn test_default(ddef: Option<RegionDefinition>) -> Self {
        let opts = CrucibleOpts {
            id: Uuid::new_v4(),
            target: vec![],
            lossy: false,
            flush_timeout: None,
            key: None,
            cert_pem: None,
            key_pem: None,
            root_cert_pem: None,
            control: None,
            read_only: false,
        };

        let log = crucible_common::build_logger();

        Self::new(&opts, 0, ddef, Arc::new(Guest::default()), None, log)
    }

    /// Runs the upstairs (forever)
    pub(crate) async fn run(&mut self) {
        loop {
            let action = self.select().await;
            self.apply(action).await
        }
    }

    /// Select an event from possible actions
    async fn select(&mut self) -> UpstairsAction {
        tokio::select! {
            d = self.downstairs.select() => {
                UpstairsAction::Downstairs(d)
            }
            d = self.guest.recv() => {
                UpstairsAction::Guest(d)
            }
            _ = async {
                if let Some(r) = self.repair_check_interval {
                    sleep_until(r).await
                } else {
                    futures::future::pending().await
                }
            } => {
                UpstairsAction::RepairCheck
            }
            _ = sleep_until(self.leak_deadline) => {
                UpstairsAction::LeakCheck
            }
            _ = sleep_until(self.flush_deadline) => {
                UpstairsAction::FlushCheck
            }
            _ = sleep_until(self.stat_deadline) => {
                UpstairsAction::StatUpdate
            }
            c = self.control_rx.recv() => {
                // We can always unwrap this, because we hold a handle to the tx
                // side as well (so the channel will never close)
                UpstairsAction::Control(c.unwrap())
            }
        }
    }

    /// Apply an action returned from [`Upstairs::select`]
    async fn apply(&mut self, action: UpstairsAction) {
        match action {
            UpstairsAction::Downstairs(d) => {
                self.apply_downstairs_action(d).await
            }
            UpstairsAction::Guest(b) => {
                self.apply_guest_request(b).await;
                self.gone_too_long();
            }
            UpstairsAction::LeakCheck => {
                const LEAK_MS: usize = 1000;
                let leak_tick =
                    tokio::time::Duration::from_millis(LEAK_MS as u64);
                if let Some(iop_limit) = self.guest.get_iop_limit() {
                    let tokens = iop_limit / (1000 / LEAK_MS);
                    self.guest.leak_iop_tokens(tokens);
                }

                if let Some(bw_limit) = self.guest.get_bw_limit() {
                    let tokens = bw_limit / (1000 / LEAK_MS);
                    self.guest.leak_bw_tokens(tokens);
                }

                self.leak_deadline =
                    Instant::now().checked_add(leak_tick).unwrap();
            }
            UpstairsAction::FlushCheck => {
                if self.need_flush {
                    self.submit_flush(None, None).await;
                }
                self.flush_deadline = deadline_secs(self.flush_timeout_secs);
            }
            UpstairsAction::StatUpdate => {
                self.on_stat_update().await;
                self.stat_deadline = deadline_secs(STAT_INTERVAL_SECS);
            }
            UpstairsAction::RepairCheck => {
                self.on_repair_check().await;
            }
            UpstairsAction::Control(c) => {
                self.on_control_req(c).await;
            }
        }
        // For now, check backpressure after every event.  We may want to make
        // this more nuanced in the future.
        self.set_backpressure();

        // Handle any jobs that have become ready for acks
        self.ack_ready().await;
    }

    /// Check outstanding IOops for each downstairs.
    ///
    /// If the number is too high, then mark that downstairs as failed, scrub
    /// any outstanding jobs, and restart the client IO task.
    fn gone_too_long(&mut self) {
        // If we are not active, then just exit.
        if !matches!(self.state, UpstairsState::Active) {
            return;
        }

        for cid in ClientId::iter() {
            // Only downstairs in these states are checked.
            match self.downstairs.clients[cid].state() {
                DsState::Active
                | DsState::LiveRepair
                | DsState::Offline
                | DsState::Replay => {
                    self.downstairs.check_gone_too_long(cid, &self.state);
                }
                _ => {}
            }
        }
    }

    /// Fires the `up-status` DTrace probe
    async fn on_stat_update(&self) {
        let up_count = self.guest.guest_work.lock().await.active.len() as u32;
        let ds_count = self.downstairs.active_count() as u32;
        let ds_state = self.downstairs.collect_stats(|c| c.state());

        let ds_io_count = self.downstairs.io_state_count();
        let ds_reconciled = self.downstairs.reconcile_repaired();
        let ds_reconcile_needed = self.downstairs.reconcile_repair_needed();
        let ds_live_repair_completed = self
            .downstairs
            .collect_stats(|c| c.stats.live_repair_completed);
        let ds_live_repair_aborted = self
            .downstairs
            .collect_stats(|c| c.stats.live_repair_aborted);
        let ds_connected = self.downstairs.collect_stats(|c| c.stats.connected);
        let ds_replaced = self.downstairs.collect_stats(|c| c.stats.replaced);
        let ds_flow_control =
            self.downstairs.collect_stats(|c| c.stats.flow_control);
        let ds_extents_repaired =
            self.downstairs.collect_stats(|c| c.stats.extents_repaired);
        let ds_extents_confirmed =
            self.downstairs.collect_stats(|c| c.stats.extents_confirmed);
        let ds_ro_lr_skipped =
            self.downstairs.collect_stats(|c| c.stats.ro_lr_skipped);

        let up_backpressure = self
            .guest
            .backpressure_us
            .load(std::sync::atomic::Ordering::Acquire);
        let write_bytes_out = self.downstairs.write_bytes_outstanding();

        cdt::up__status!(|| {
            let arg = Arg {
                up_count,
                up_backpressure,
                write_bytes_out,
                ds_count,
                ds_state,
                ds_io_count,
                ds_reconciled,
                ds_reconcile_needed,
                ds_live_repair_completed,
                ds_live_repair_aborted,
                ds_connected,
                ds_replaced,
                ds_flow_control,
                ds_extents_repaired,
                ds_extents_confirmed,
                ds_ro_lr_skipped,
            };
            ("stats", arg)
        });
    }

    /// Handles a request from the (optional) control server
    async fn on_control_req(&self, c: ControlRequest) {
        match c {
            ControlRequest::UpstairsStats(tx) => {
                let ds_state = self.downstairs.collect_stats(|c| c.state());
                let up_jobs = self.guest.guest_work.lock().await.active.len();
                let ds_jobs = self.downstairs.active_count();
                let repair_done = self.downstairs.reconcile_repaired();
                let repair_needed = self.downstairs.reconcile_repair_needed();
                let extents_repaired =
                    self.downstairs.collect_stats(|c| c.stats.extents_repaired);
                let extents_confirmed = self
                    .downstairs
                    .collect_stats(|c| c.stats.extents_confirmed);
                let extent_limit = self
                    .downstairs
                    .collect_stats(|c| c.extent_limit.map(|v| v as usize));
                let live_repair_completed = self
                    .downstairs
                    .collect_stats(|c| c.stats.live_repair_completed);
                let live_repair_aborted = self
                    .downstairs
                    .collect_stats(|c| c.stats.live_repair_aborted);

                // Translate from rich UpstairsState to simplified UpState
                // TODO: remove this distinction?
                let state = match &self.state {
                    UpstairsState::Initializing
                    | UpstairsState::GoActive { .. } => {
                        crate::UpState::Initializing
                    }
                    UpstairsState::Active => crate::UpState::Active,
                    UpstairsState::Deactivating => crate::UpState::Deactivating,
                };

                let r = tx.send(crate::control::UpstairsStats {
                    state,
                    ds_state: ds_state.to_vec(),
                    up_jobs,
                    ds_jobs,
                    repair_done,
                    repair_needed,
                    extents_repaired: extents_repaired.to_vec(),
                    extents_confirmed: extents_confirmed.to_vec(),
                    extent_limit: extent_limit.to_vec(),
                    live_repair_completed: live_repair_completed.to_vec(),
                    live_repair_aborted: live_repair_aborted.to_vec(),
                });
                if r.is_err() {
                    warn!(self.log, "control message reply failed");
                }
            }
            ControlRequest::DownstairsWorkQueue(tx) => {
                let out = self.downstairs.get_work_summary();
                let r = tx.send(out);
                if r.is_err() {
                    warn!(self.log, "control message reply failed");
                }
            }
        }
    }

    async fn on_repair_check(&mut self) {
        info!(self.log, "Checking if live repair is needed");
        if !matches!(self.state, UpstairsState::Active) {
            info!(self.log, "inactive, no live repair needed");
            self.repair_check_interval = None;
            return;
        }

        if self.cfg.read_only {
            info!(self.log, "read-only, no live repair needed");
            // Repair can't happen on a read-only downstairs, so short circuit
            // here. There's no state drift to repair anyway, this read-only
            // Upstairs wouldn't have caused any modifications.
            for c in self.downstairs.clients.iter_mut() {
                c.skip_live_repair(&self.state);
            }
            self.repair_check_interval = None;
            return;
        }

        // Verify that all downstairs and the upstairs are in the proper state
        // before we begin a live repair.
        let repair = self
            .downstairs
            .clients
            .iter()
            .filter(|c| c.state() == DsState::LiveRepair)
            .count();

        let repair_ready = self
            .downstairs
            .clients
            .iter()
            .filter(|c| c.state() == DsState::LiveRepairReady)
            .count();

        if repair_ready == 0 {
            if repair > 0 {
                info!(self.log, "Live Repair already running");
            } else {
                info!(self.log, "No Live Repair required at this time");
            }
            self.repair_check_interval = None;
        } else if repair > 0
            || !self.downstairs.start_live_repair(
                &self.state,
                self.guest.guest_work.lock().await.deref_mut(),
                self.ddef.get_def().unwrap().extent_count().into(),
                self.generation,
            )
        {
            // This also means repair_ready > 0
            // We can only have one live repair going at a time, so if a
            // downstairs has gone Faulted then to LiveRepairReady, it will have
            // to wait until the currently running LiveRepair has completed.
            warn!(self.log, "Upstairs already in repair, trying again later");
            self.repair_check_interval = Some(deadline_secs(60.0));
        }
    }

    /// Returns `true` if we're ready to accept guest IO
    fn guest_io_ready(&self) -> bool {
        matches!(self.state, UpstairsState::Active)
    }

    /// Apply a guest request
    ///
    /// For IO operations, we build the downstairs work and if required split
    /// the single IO into multiple IOs to the downstairs. The IO operations are
    /// pushed into downstairs work queues, and will later be sent over the
    /// network.
    ///
    /// This function can be called before the upstairs is active, so any
    /// operation that requires the upstairs to be active should check that
    /// and report an error.
    async fn apply_guest_request(&mut self, req: BlockReq) {
        // If any of the submit_* functions fail to send to the downstairs, they
        // return an error.  These are reported to the Guest.
        match req.op() {
            // These three options can be handled by this task directly,
            // and don't require the upstairs to be fully online.
            BlockOp::GoActive => {
                self.set_active_request(req.take_sender()).await;
            }
            BlockOp::GoActiveWithGen { gen } => {
                self.generation = gen;
                self.set_active_request(req.take_sender()).await;
            }
            BlockOp::QueryGuestIOReady { data } => {
                *data.lock().await = self.guest_io_ready();
                req.send_ok();
            }
            BlockOp::QueryUpstairsUuid { data } => {
                *data.lock().await = self.cfg.upstairs_id;
                req.send_ok();
            }
            BlockOp::Deactivate => {
                self.set_deactivate(req).await;
            }
            BlockOp::RepairOp => {
                warn!(self.log, "Ignoring external BlockOp::RepairOp");
            }

            // Query ops
            BlockOp::QueryBlockSize { data } => {
                match self.ddef.get_def() {
                    Some(rd) => {
                        *data.lock().await = rd.block_size();
                        req.send_ok();
                    }
                    None => {
                        warn!(
                            self.log,
                            "Block size not available (active: {})",
                            self.guest_io_ready()
                        );
                        req.send_err(CrucibleError::PropertyNotAvailable(
                            "block size".to_string(),
                        ));
                    }
                };
            }
            BlockOp::QueryTotalSize { data } => {
                match self.ddef.get_def() {
                    Some(rd) => {
                        *data.lock().await = rd.total_size();
                        req.send_ok();
                    }
                    None => {
                        warn!(
                            self.log,
                            "Total size not available (active: {})",
                            self.guest_io_ready()
                        );
                        req.send_err(CrucibleError::PropertyNotAvailable(
                            "total size".to_string(),
                        ));
                    }
                };
            }
            // Testing options
            BlockOp::QueryExtentSize { data } => {
                // Yes, test only
                match self.ddef.get_def() {
                    Some(rd) => {
                        *data.lock().await = rd.extent_size();
                        req.send_ok();
                    }
                    None => {
                        warn!(
                            self.log,
                            "Extent size not available (active: {})",
                            self.guest_io_ready()
                        );
                        req.send_err(CrucibleError::PropertyNotAvailable(
                            "extent size".to_string(),
                        ));
                    }
                };
            }
            BlockOp::QueryWorkQueue { data } => {
                // TODO should this first check if the Upstairs is active?
                let active_count = self
                    .downstairs
                    .clients
                    .iter()
                    .filter(|c| c.state() == DsState::Active)
                    .count();
                *data.lock().await = WQCounts {
                    up_count: self.guest.guest_work.lock().await.active.len(),
                    ds_count: self.downstairs.active_count(),
                    active_count,
                };
                req.send_ok();
            }

            BlockOp::ShowWork { data } => {
                // TODO should this first check if the Upstairs is active?
                *data.lock().await = self.show_all_work().await;
                req.send_ok();
            }

            BlockOp::Commit => {
                if !self.guest_io_ready() {
                    req.send_err(CrucibleError::UpstairsInactive);
                }
                // Nothing to do here; we always check for new work in `select!`
            }

            BlockOp::Read { offset, data } => {
                self.submit_read(offset, data, req).await
            }
            BlockOp::Write { offset, data } => {
                self.submit_write(offset, data, req, false).await
            }
            BlockOp::WriteUnwritten { offset, data } => {
                self.submit_write(offset, data, req, true).await
            }
            BlockOp::Flush { snapshot_details } => {
                /*
                 * Submit for read and write both check if the upstairs is
                 * ready for guest IO or not.  Because the Upstairs itself can
                 * call submit_flush, we have to check here that it is okay
                 * to accept IO from the guest before calling a guest requested
                 * flush command.
                 */
                if !self.guest_io_ready() {
                    req.send_err(CrucibleError::UpstairsInactive);
                    return;
                }
                self.submit_flush(Some(req), snapshot_details).await;
            }
            BlockOp::ReplaceDownstairs {
                id,
                old,
                new,
                result,
            } => match self.downstairs.replace(id, old, new, &self.state) {
                Ok(v) => {
                    *result.lock().await = v;
                    req.send_ok();
                }
                Err(e) => req.send_err(e),
            },
        }
    }

    async fn show_all_work(&self) -> WQCounts {
        let gior = self.guest_io_ready();
        let up_count = self.guest.guest_work.lock().await.active.len();

        let ds_count = self.downstairs.active_count();

        println!(
            "----------------------------------------------------------------"
        );
        println!(
            " Crucible gen:{} GIO:{} work queues:  Upstairs:{}  downstairs:{}",
            self.generation, gior, up_count, ds_count,
        );
        if ds_count == 0 {
            if up_count != 0 {
                crate::show_guest_work(&self.guest).await;
            }
        } else {
            self.downstairs.show_all_work()
        }

        print!("Downstairs last five completed:");
        self.downstairs.print_last_completed(5);
        println!();

        let active_count = self
            .downstairs
            .clients
            .iter()
            .filter(|c| c.state() == DsState::Active)
            .count();

        // TODO this is a ringbuffer, why are we turning it to a Vec to look at
        // the last five items?
        let up_done = self.guest.guest_work.lock().await.completed.to_vec();
        print!("Upstairs last five completed:  ");
        for j in up_done.iter().rev().take(5) {
            print!(" {:4}", j);
        }
        println!();

        WQCounts {
            up_count,
            ds_count,
            active_count,
        }
    }

    /// Request that the Upstairs go active
    async fn set_active_request(
        &mut self,
        reply: oneshot::Sender<Result<(), CrucibleError>>,
    ) {
        match self.state {
            UpstairsState::Initializing => {
                self.state = UpstairsState::GoActive { reply };
                info!(self.log, "{} active request set", self.cfg.upstairs_id);
            }
            UpstairsState::GoActive { .. } => {
                panic!("set_active_request called while already going active");
            }
            UpstairsState::Deactivating => {
                warn!(
                    self.log,
                    "{} active denied while Deactivating", self.cfg.upstairs_id
                );
                let _ = reply.send(Err(CrucibleError::UpstairsDeactivating));
            }
            UpstairsState::Active => {
                info!(
                    self.log,
                    "{} Request to activate upstairs already active",
                    self.cfg.upstairs_id
                );
                let _ = reply.send(Err(CrucibleError::UpstairsAlreadyActive));
            }
        }
        // Notify all clients that they should go active when they hit an
        // appropriate state in their negotiation.
        for c in self.downstairs.clients.iter_mut() {
            c.set_active_request(self.generation).await;
        }
    }

    /// Request that the Upstairs deactivate
    ///
    /// This will return immediately if all of the Downstairs clients are done;
    /// otherwise, it will schedule a final flush that triggers deactivation
    /// when complete.
    ///
    /// In either case, `self.state` is set to `UpstairsState::Deactivating`
    async fn set_deactivate(&mut self, req: BlockReq) {
        info!(self.log, "Request to deactivate this guest");
        match self.state {
            UpstairsState::Initializing | UpstairsState::GoActive { .. } => {
                req.send_err(CrucibleError::UpstairsInactive);
                return;
            }
            UpstairsState::Deactivating => {
                req.send_err(CrucibleError::UpstairsDeactivating);
                return;
            }
            UpstairsState::Active => (),
        }
        self.state = UpstairsState::Deactivating;

        if self.downstairs.set_deactivate() {
            debug!(self.log, "set_deactivate was successful");
            req.send_ok();
        } else {
            debug!(self.log, "not ready to deactivate; submitting final flush");
            self.submit_flush(Some(req), None).await
        }
    }

    async fn submit_flush(
        &mut self,
        req: Option<BlockReq>,
        snapshot_details: Option<SnapshotDetails>,
    ) {
        // Notice that unlike submit_read and submit_write, we do not check for
        // guest_io_ready here. The upstairs itself can call submit_flush
        // (without the guest being involved), so the check is handled at the
        // BlockOp::Flush level above.

        self.need_flush = false;

        /*
         * Get the next ID for our new guest work job. Note that the flush
         * ID and the next_id are connected here, in that all future writes
         * should be flushed at the next flush ID.
         */
        let mut gw = self.guest.guest_work.lock().await;
        let gw_id: u64 = gw.next_gw_id();
        cdt::gw__flush__start!(|| (gw_id));

        if snapshot_details.is_some() {
            info!(self.log, "flush with snap requested");
        }

        let next_id = self.downstairs.submit_flush(
            gw_id,
            self.generation,
            snapshot_details,
        );

        let new_gtos = GtoS::new(next_id, None, req);
        gw.active.insert(gw_id, new_gtos);

        cdt::up__to__ds__flush__start!(|| (gw_id));
    }

    /// Submit a read job to the downstairs
    async fn submit_read(
        &mut self,
        offset: Block,
        data: Buffer,
        req: BlockReq,
    ) {
        if !self.guest_io_ready() {
            req.send_err(CrucibleError::UpstairsInactive);
            return;
        }

        /*
         * Get the next ID for the guest work struct we will make at the
         * end. This ID is also put into the IO struct we create that
         * handles the operation(s) on the storage side.
         */
        let mut gw = self.guest.guest_work.lock().await;
        let ddef = self.ddef.get_def().unwrap();

        /*
         * Verify IO is in range for our region
         */
        if let Err(e) = ddef.validate_io(offset, data.len()) {
            req.send_err(e);
            return;
        }

        self.need_flush = true;

        /*
         * Given the offset and buffer size, figure out what extent and
         * byte offset that translates into. Keep in mind that an offset
         * and length may span many extents, and eventually, TODO, regions.
         */
        let impacted_blocks = crate::extent_from_offset(
            &ddef,
            offset,
            Block::from_bytes(data.len(), &ddef),
        );

        /*
         * Grab this ID after extent_from_offset: in case of Err we don't
         * want to create a gap in the IDs.
         */
        let gw_id: u64 = gw.next_gw_id();
        cdt::gw__read__start!(|| (gw_id));

        let next_id = self.downstairs.submit_read(gw_id, impacted_blocks, ddef);

        // New work created, add to the guest_work HM.  It's fine to do this
        // after submitting the job to the downstairs, because no one else is
        // modifying the Upstairs right now; even if the job finishes
        // instantaneously, it can't interrupt this function.
        let new_gtos = GtoS::new(next_id, Some(data), Some(req));
        gw.active.insert(gw_id, new_gtos);

        cdt::up__to__ds__read__start!(|| (gw_id));
    }

    async fn submit_write(
        &mut self,
        offset: Block,
        data: Bytes,
        req: BlockReq,
        is_write_unwritten: bool,
    ) {
        if !self.guest_io_ready() {
            req.send_err(CrucibleError::UpstairsInactive);
            return;
        }
        if self.cfg.read_only {
            req.send_err(CrucibleError::ModifyingReadOnlyRegion);
            return;
        }

        /*
         * Verify IO is in range for our region
         */
        let ddef = self.ddef.get_def().unwrap();
        if let Err(e) = ddef.validate_io(offset, data.len()) {
            req.send_err(e);
            return;
        }

        /*
         * Given the offset and buffer size, figure out what extent and
         * byte offset that translates into. Keep in mind that an offset
         * and length may span two extents.
         */
        let impacted_blocks = extent_from_offset(
            &ddef,
            offset,
            Block::from_bytes(data.len(), &ddef),
        );

        // Build up all of the Write operations, encrypting data here
        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(impacted_blocks.len(&ddef));

        let mut cur_offset: usize = 0;
        for (eid, offset) in impacted_blocks.blocks(&ddef) {
            let byte_len: usize = ddef.block_size() as usize;

            let (sub_data, encryption_context, hash) = if let Some(context) =
                &self.cfg.encryption_context
            {
                // Encrypt here
                let mut mut_data =
                    data.slice(cur_offset..(cur_offset + byte_len)).to_vec();

                let (nonce, tag, hash) =
                    match context.encrypt_in_place(&mut mut_data[..]) {
                        Err(e) => {
                            req.send_err(CrucibleError::EncryptionError(
                                e.to_string(),
                            ));
                            return;
                        }

                        Ok(v) => v,
                    };

                (
                    Bytes::copy_from_slice(&mut_data),
                    Some(crucible_protocol::EncryptionContext {
                        nonce: nonce.into(),
                        tag: tag.into(),
                    }),
                    hash,
                )
            } else {
                // Unencrypted
                let sub_data = data.slice(cur_offset..(cur_offset + byte_len));
                let hash = integrity_hash(&[&sub_data[..]]);

                (sub_data, None, hash)
            };

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data: sub_data,
                block_context: BlockContext {
                    hash,
                    encryption_context,
                },
            });

            cur_offset += byte_len;
        }

        /*
         * Get the next ID for the guest work struct we will make at the
         * end. This ID is also put into the IO struct we create that
         * handles the operation(s) on the storage side.
         */
        let mut gw = self.guest.guest_work.lock().await;
        self.need_flush = true;

        /*
         * Grab this ID after extent_from_offset: in case of Err we don't
         * want to create a gap in the IDs.
         */
        let gw_id: u64 = gw.next_gw_id();
        if is_write_unwritten {
            cdt::gw__write__unwritten__start!(|| (gw_id));
        } else {
            cdt::gw__write__start!(|| (gw_id));
        }

        let next_id = self.downstairs.submit_write(
            gw_id,
            impacted_blocks,
            writes,
            is_write_unwritten,
        );
        let new_gtos = GtoS::new(next_id, None, Some(req));

        // New work created, add to the guest_work HM
        gw.active.insert(gw_id, new_gtos);

        if is_write_unwritten {
            cdt::up__to__ds__write__unwritten__start!(|| (gw_id));
        } else {
            cdt::up__to__ds__write__start!(|| (gw_id));
        }
    }

    /// React to an event sent by one of the downstairs clients
    async fn apply_downstairs_action(&mut self, d: DownstairsAction) {
        match d {
            DownstairsAction::Client { client_id, action } => {
                self.apply_client_action(client_id, action).await;
            }
            DownstairsAction::LiveRepair(r) => {
                let mut gw = self.guest.guest_work.lock().await;
                self.downstairs.on_live_repair(
                    r,
                    &mut gw,
                    &self.state,
                    self.generation,
                );
            }
        }
    }

    async fn ack_ready(&mut self) {
        if self.downstairs.has_ackable_jobs() {
            let mut gw = self.guest.guest_work.lock().await;
            self.downstairs.ack_jobs(&mut gw, &self.stats).await;
        }
    }

    /// React to an event sent by one of the downstairs clients
    async fn apply_client_action(
        &mut self,
        client_id: ClientId,
        action: ClientAction,
    ) {
        match action {
            ClientAction::Connected => {
                self.downstairs.clients[client_id].send_here_i_am().await;
            }
            ClientAction::Ping => {
                self.downstairs.clients[client_id].send_ping().await;
            }
            ClientAction::Timeout => {
                // Ask the downstairs client task to stop, because the client
                // has hit a Crucible timeout.
                //
                // This will come back to `TaskStopped`, at which point we'll
                // clear out the task and restart it.
                self.downstairs.clients[client_id]
                    .halt_io_task(ClientStopReason::Timeout);
            }
            ClientAction::Response(m) => {
                self.on_client_message(client_id, m).await;
            }
            ClientAction::TaskStopped(r) => {
                self.on_client_task_stopped(client_id, r);
            }
            ClientAction::Work | ClientAction::MoreWork => {
                self.downstairs.perform_work(client_id).await;
            }
        }
    }

    async fn on_client_message(&mut self, client_id: ClientId, m: Message) {
        // We have received a message, so reset the timeout watchdog for this
        // particular client.
        self.downstairs.clients[client_id].reset_timeout();
        match m {
            Message::Imok => {
                // Nothing to do here, glad to hear that you're okay
            }

            // IO operation replies
            //
            // This may cause jobs to become ackable!
            Message::WriteAck { .. }
            | Message::WriteUnwrittenAck { .. }
            | Message::FlushAck { .. }
            | Message::ReadResponse { .. }
            | Message::ExtentLiveCloseAck { .. }
            | Message::ExtentLiveAckId { .. }
            | Message::ExtentLiveRepairAckId { .. }
            | Message::ErrorReport { .. } => {
                let r = self.downstairs.process_io_completion(
                    client_id,
                    m,
                    &self.state,
                );
                if let Err(e) = r {
                    warn!(
                        self.downstairs.clients[client_id].log,
                        "Error processing message: {}", e
                    );
                }
            }

            Message::YesItsMe { .. }
            | Message::VersionMismatch { .. }
            | Message::EncryptedMismatch { .. }
            | Message::ReadOnlyMismatch { .. }
            | Message::YouAreNowActive { .. }
            | Message::RegionInfo { .. }
            | Message::LastFlushAck { .. }
            | Message::ExtentVersions { .. } => {
                // negotiation and initial reconciliation
                let r = self.downstairs.clients[client_id]
                    .continue_negotiation(
                        m,
                        &self.state,
                        self.generation,
                        &mut self.ddef,
                    )
                    .await;

                match r {
                    // continue_negotiation returns an error if the upstairs
                    // should go inactive!
                    Err(e) => self.set_inactive(e),
                    Ok(false) => (),
                    Ok(true) => {
                        // Negotiation succeeded for this Downstairs, let's see
                        // what we can do from here
                        match self.downstairs.clients[client_id].state() {
                            DsState::Active => (),
                            DsState::WaitQuorum => {
                                // See if we have a quorum
                                if self.connect_region_set().await {
                                    // We connected, start periodic live-repair
                                    // checking in the main loop.
                                    self.repair_check_interval =
                                        Some(deadline_secs(1.0));
                                }
                            }
                            DsState::LiveRepairReady => {
                                // Immediately check for live-repair (?)
                                self.repair_check_interval =
                                    Some(deadline_secs(0.0));
                            }
                            s => panic!("bad state after negotiation: {s:?}"),
                        }
                    }
                }
            }

            Message::ExtentError { .. } => {
                self.downstairs.on_reconciliation_failed(
                    client_id,
                    m,
                    &self.state,
                );
            }
            Message::RepairAckId { .. } => {
                if self
                    .downstairs
                    .on_reconciliation_ack(client_id, m, &self.state)
                    .await
                {
                    // reconciliation is done, great work everyone
                    self.on_reconciliation_done(DsState::Repair);
                }
            }

            Message::YouAreNoLongerActive { .. } => {
                self.on_no_longer_active(client_id, m);
            }

            Message::UuidMismatch { .. } => {
                self.on_uuid_mismatch(client_id, m);
            }

            // These are all messages that we send out, so we shouldn't see them
            Message::HereIAm { .. }
            | Message::Ruok
            | Message::Flush { .. }
            | Message::LastFlush { .. }
            | Message::Write { .. }
            | Message::WriteUnwritten { .. }
            | Message::ReadRequest { .. }
            | Message::RegionInfoPlease { .. }
            | Message::ExtentLiveFlushClose { .. }
            | Message::ExtentLiveClose { .. }
            | Message::ExtentLiveRepair { .. }
            | Message::ExtentLiveNoOp { .. }
            | Message::ExtentLiveReopen { .. }
            | Message::ExtentClose { .. }
            | Message::ExtentFlush { .. }
            | Message::ExtentRepair { .. }
            | Message::ExtentReopen { .. }
            | Message::ExtentVersionsPlease
            | Message::PromoteToActive { .. }
            | Message::Unknown(..) => {
                panic!("invalid response {m:?}")
            }
        }
        if matches!(self.state, UpstairsState::Deactivating) {
            if self.downstairs.try_deactivate(client_id, &self.state) {
                info!(self.log, "deactivated client {client_id}");
            } else {
                info!(self.log, "not ready to deactivate client {client_id}");
            }
        }
    }

    /// Checks whether we can connect all three regions
    ///
    /// Returns `false` if we aren't ready, or if things failed.  If there's a
    /// failure, then we also update the client state.
    ///
    /// If we have enough downstairs and we can activate, then we should inform
    /// the requestor of activation; similarly, if we have enough downstairs and
    /// **can't** activate, then we should notify the requestor of failure.
    ///
    /// If we have a problem here, we can't activate the upstairs.
    async fn connect_region_set(&mut self) -> bool {
        /*
         * If reconciliation (also called Repair in DsState) is required, it
         * happens in three phases.  Typically an interruption of repair will
         * result in things starting over, but if actual repair work to an
         * extent is completed, that extent won't need to be repaired again.
         *
         * The three phases are:
         *
         * Collect:
         * When a Downstairs connects, the Upstairs collects the gen/flush/dirty
         * (GFD) info from all extents.  This GFD information is stored and the
         * Upstairs waits for all three Downstairs to attach.
         *
         * Compare:
         * In the compare phase, the upstairs will walk the list of all extents
         * and compare the G/F/D from each of the downstairs.  When there is a
         * mismatch between downstairs (The dirty bit counts as a mismatch and
         * will force a repair even if generation and flush numbers agree). For
         * each mismatch, the upstairs determines which downstairs has the
         * extent that should be the source, and which of the other downstairs
         * extents needs repair. This list of mismatches (source,
         * destination(s)) is collected. Once an upstairs has compiled its
         * repair list, it will then generates a sequence of Upstairs ->
         * Downstairs repair commands to repair each extent that needs to be
         * fixed.  For a given piece of repair work, the commands are:
         * - Send a flush to source extent.
         * - Close extent on all downstairs.
         * - Send repair command to destination extents (with source extent
         *   IP/Port).
         * (See DS-DS Repair)
         * - Reopen all extents.
         *
         * Repair:
         * During repair Each command issued from the upstairs must be completed
         * before the next will be sent. The Upstairs is responsible for walking
         * the repair commands and sending them to the required downstairs, and
         * waiting for them to finish.  The actual repair work for an extent
         * takes place on the downstairs being repaired.
         *
         * Repair (ds to ds)
         * Each downstairs runs a repair server (Dropshot) that listens for
         * repair requests from other downstairs.  A downstairs with an extent
         * that needs repair will contact the source downstairs and request the
         * list of files for an extent, then request each file.  Once all files
         * are local to the downstairs needing repair, it will replace the
         * existing extent files with the new ones.
         */
        let collate_status = {
            /*
             * Reconciliation only happens during initialization.
             * Look at all three downstairs region information collected.
             * Determine the highest flush number and make sure our generation
             * is high enough.
             */
            if !matches!(self.state, UpstairsState::GoActive { .. }) {
                info!(
                    self.log,
                    "could not connect region set due to bad state: {:?}",
                    self.state
                );
                return false;
            }
            /*
             * Make sure all downstairs are in the correct state before we
             * proceed.
             */
            let not_ready = self
                .downstairs
                .clients
                .iter()
                .filter(|c| c.state() != DsState::WaitQuorum)
                .count();
            if not_ready > 0 {
                info!(
                    self.log,
                    "Waiting for {} more clients to be ready", not_ready
                );
                return false;
            }

            /*
             * We figure out if there is any reconciliation to do, and if so, we
             * build the list of operations that will repair the extents that
             * are not in sync.
             *
             * If we fail to collate, then we need to kick out all the
             * downstairs out, forget any activation requests, and the
             * upstairs goes back to waiting for another activation request.
             */
            self.downstairs.collate(self.generation, &self.state)
        };

        match collate_status {
            Err(e) => {
                error!(self.log, "Failed downstairs collate with: {}", e);
                // We failed to collate the three downstairs, so we need
                // to reset that activation request.  The downstairs were
                // already set to FailedRepair in the call to
                // `Downstairs::collate`
                self.set_inactive(e);
                false
            }
            Ok(true) => {
                // We have populated all of the reconciliation requests in
                // `Downstairs::reconcile_task_list`.  Start reconciliation by
                // sending the first request.
                self.downstairs.send_next_reconciliation_req().await;
                true
            }
            Ok(false) => {
                info!(self.log, "No downstairs repair required");
                self.on_reconciliation_done(DsState::WaitQuorum);
                info!(self.log, "Set Active after no repair");
                true
            }
        }
    }

    /// Called when reconciliation is complete
    fn on_reconciliation_done(&mut self, from_state: DsState) {
        // This should only ever be called if reconciliation completed
        // successfully; make some assertions to that effect.
        self.downstairs.on_reconciliation_done(from_state);

        info!(self.log, "All required repair work is completed");
        info!(self.log, "Set Downstairs and Upstairs active after repairs");

        let UpstairsState::GoActive { reply } =
            std::mem::replace(&mut self.state, UpstairsState::Active)
        else {
            panic!("invalid state active state: {:?}", self.state);
        };
        reply.send(Ok(())).unwrap();
        info!(
            self.log,
            "{} is now active with session: {}",
            self.cfg.upstairs_id,
            self.cfg.session_id
        );
        self.stats.add_activation();
    }

    fn on_no_longer_active(&mut self, client_id: ClientId, m: Message) {
        let Message::YouAreNoLongerActive {
            new_upstairs_id,
            new_session_id,
            new_gen,
        } = m
        else {
            panic!("called on_no_longer_active on invalid message {m:?}");
        };

        let client_log = &self.downstairs.clients[client_id].log;
        error!(
            client_log,
            "received NoLongerActive {:?} {:?} {}",
            new_upstairs_id,
            new_session_id,
            new_gen,
        );

        // Before we restart the downstairs client, let's print a specific
        // warning or error message

        // What if the newly active upstairs has the same UUID?
        let uuid_desc = if self.cfg.upstairs_id == new_upstairs_id {
            "same upstairs UUID".to_owned()
        } else {
            format!("different upstairs UUID {new_upstairs_id:?}")
        };

        if new_gen <= self.generation {
            // Here, our generation number is greater than or equal to the newly
            // active Upstairs, which shares our UUID. We shouldn't have
            // received this message. The downstairs is confused.
            error!(
                client_log,
                "bad YouAreNoLongerActive with our gen {} >= {new_gen} \
                 and {uuid_desc}",
                self.generation
            );
        } else {
            // The next generation of this Upstairs connected, which is fine.
            warn!(
                client_log,
                "saw YouAreNoLongerActive with our gen {} < {new_gen} and \
                 {uuid_desc}",
                self.generation
            );
        };

        // Restart the state machine for this downstairs client
        self.downstairs.clients[client_id].disable(&self.state);
        self.set_inactive(CrucibleError::NoLongerActive);
    }

    fn on_uuid_mismatch(&mut self, client_id: ClientId, m: Message) {
        let Message::UuidMismatch { expected_id } = m else {
            panic!("called on_uuid_mismatch on invalid message {m:?}");
        };

        let client_log = &self.downstairs.clients[client_id].log;
        error!(
            client_log,
            "received UuidMismatch, expecting {expected_id:?}!"
        );

        // Restart the state machine for this downstairs client
        self.downstairs.clients[client_id].disable(&self.state);
        self.set_inactive(CrucibleError::UuidMismatch);
    }

    fn set_inactive(&mut self, err: CrucibleError) {
        let prev =
            std::mem::replace(&mut self.state, UpstairsState::Initializing);
        if let UpstairsState::GoActive { reply } = prev {
            let _ = reply.send(Err(err));
        }
        info!(self.log, "setting inactive!");
    }

    fn on_client_task_stopped(
        &mut self,
        client_id: ClientId,
        reason: ClientRunResult,
    ) {
        warn!(
            self.log,
            "downstairs task for {client_id} stopped due to {reason:?}"
        );

        // If the connection goes down here, we need to know what state we were
        // in to decide what state to transition to.  The ds_missing method will
        // do that for us!
        self.downstairs.clients[client_id].on_missing();

        // If we are deactivating, then check and see if this downstairs was the
        // final one required to deactivate; if so, switch the upstairs back to
        // initializing.
        self.deactivate_transition_check();

        // Restart the downstairs task.  If the upstairs is already active, then
        // the downstairs should automatically call PromoteToActive when it
        // reaches the relevant state.
        let auto_promote = match self.state {
            UpstairsState::Active | UpstairsState::GoActive { .. } => {
                // XXX is is correct to auto-promote if we're in GoActive?
                Some(self.generation)
            }
            UpstairsState::Initializing | UpstairsState::Deactivating => None,
        };
        self.downstairs.reinitialize(client_id, auto_promote);
    }

    fn deactivate_transition_check(&mut self) {
        if matches!(self.state, UpstairsState::Deactivating) {
            info!(self.log, "deactivate transition checking...");
            if self
                .downstairs
                .clients
                .iter()
                .all(|c| c.ready_to_deactivate())
            {
                info!(self.log, "All DS in the proper state! -> INIT");
                self.state = UpstairsState::Initializing;
            }
        }
    }

    fn set_backpressure(&self) {
        let dsw_max = self
            .downstairs
            .clients
            .iter()
            .map(|c| c.total_live_work())
            .max()
            .unwrap_or(0);
        let ratio = dsw_max as f64 / crate::IO_OUTSTANDING_MAX as f64;
        self.guest
            .set_backpressure(self.downstairs.write_bytes_outstanding(), ratio);
    }

    /// Returns the `RegionDefinition`
    ///
    /// # Panics
    /// If the region definition is not yet known (i.e. it wasn't provided on
    /// startup, and no Downstairs have started talking to us yet).
    #[cfg(test)]
    pub(crate) fn get_region_definition(&self) -> RegionDefinition {
        self.ddef.get_def().unwrap()
    }
}
