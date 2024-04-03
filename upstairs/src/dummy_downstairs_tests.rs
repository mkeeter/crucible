// Copyright 2023 Oxide Computer Company

#[cfg(test)]
pub(crate) mod protocol_test {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::guest::Guest;
    use crate::up_main;
    use crate::BlockContext;
    use crate::BlockIO;
    use crate::Buffer;
    use crate::CrucibleError;
    use crate::IO_OUTSTANDING_MAX_BYTES;
    use crucible_client_types::CrucibleOpts;
    use crucible_common::Block;
    use crucible_common::RegionDefinition;
    use crucible_common::RegionOptions;
    use crucible_common::IO_OUTSTANDING_MAX_JOBS;
    use crucible_protocol::ClientId;
    use crucible_protocol::CrucibleDecoder;
    use crucible_protocol::CrucibleEncoder;
    use crucible_protocol::JobId;
    use crucible_protocol::Message;
    use crucible_protocol::ReadResponseBlockMetadata;
    use crucible_protocol::ReadResponseHeader;
    use crucible_protocol::WriteHeader;

    use anyhow::bail;
    use anyhow::Result;
    use bytes::BytesMut;
    use futures::SinkExt;
    use futures::StreamExt;
    use slog::error;
    use slog::info;
    use slog::o;
    use slog::Drain;
    use slog::Logger;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicU8;
    use std::sync::atomic::Ordering;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio_util::codec::FramedRead;
    use tokio_util::codec::FramedWrite;
    use uuid::Uuid;

    // Create a simple logger
    fn csl() -> Logger {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!())
    }

    /// Handle to a running Downstairs loopback task
    ///
    /// The loopback task receives messages over the network (on `local_addr`)
    /// and sends them to `tx`; and receives messages on `rx` and sends them
    /// over the network.
    ///
    /// In other words, this lets us pretend to be a Downstairs.
    pub struct DownstairsHandle {
        log: Logger,

        /// When the loopback worker finishes, we return the original listener
        /// so that we can reconnect later on.
        loopback_worker:
            tokio::task::JoinHandle<Result<TcpListener, CrucibleError>>,
        rx: mpsc::UnboundedReceiver<Message>,
        tx: mpsc::UnboundedSender<Message>,
        stop: oneshot::Sender<()>,

        uuid: Uuid,
        local_addr: SocketAddr,
        repair_addr: SocketAddr,
        cfg: DownstairsConfig,
        upstairs_session_id: Option<Uuid>,
    }

    impl DownstairsHandle {
        /// Returns the next non-ping packet
        ///
        /// (pings are answered inline)
        async fn recv(&mut self) -> Option<Message> {
            loop {
                let packet = self.rx.recv().await?;
                match packet {
                    Message::Ruok => {
                        // Respond to pings right away
                        if let Err(e) = self.send(Message::Imok) {
                            error!(self.log, "could not send ping: {e:?}");
                        }
                        info!(self.log, "responded to ping");

                        continue;
                    }

                    x => break Some(x),
                }
            }
        }

        /// Tries to return the next non-ping packet
        fn try_recv(&mut self) -> Result<Message, mpsc::error::TryRecvError> {
            loop {
                let packet = self.rx.try_recv();
                match packet {
                    Ok(Message::Ruok) => {
                        // Respond to pings right away
                        if let Err(e) = self.send(Message::Imok) {
                            error!(self.log, "could not send ping: {e:?}");
                        }
                        info!(self.log, "responded to ping");

                        continue;
                    }

                    m => break m,
                }
            }
        }

        /// Send a message, pretending to be the Downstairs
        fn send(
            &mut self,
            m: Message,
        ) -> Result<(), mpsc::error::SendError<Message>> {
            self.tx.send(m)
        }

        pub async fn negotiate_start(&mut self) -> Result<()> {
            let packet = self.recv().await.unwrap();
            match &packet {
                Message::HereIAm {
                    version,
                    upstairs_id: _,
                    session_id: _,
                    gen: _,
                    read_only,
                    encrypted: _,
                    alternate_versions: _,
                } => {
                    info!(
                        self.log,
                        "negotiate packet {:?} (upstairs read-only {})",
                        packet,
                        read_only
                    );

                    if *read_only != self.cfg.read_only {
                        bail!("read only mismatch!");
                    }

                    self.send(Message::YesItsMe {
                        version: *version,
                        repair_addr: self.repair_addr,
                    })
                    .unwrap();
                }

                x => {
                    bail!("wrong packet {:?}, expected HereIAm", x)
                }
            }

            let packet = self.recv().await.unwrap();
            match &packet {
                Message::PromoteToActive {
                    upstairs_id,
                    session_id,
                    gen,
                } => {
                    assert!(*gen == 1);

                    info!(self.log, "negotiate packet {:?}", packet);

                    // Record the session id the upstairs sent us
                    self.upstairs_session_id = Some(*session_id);

                    self.send(Message::YouAreNowActive {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        gen: *gen,
                    })
                    .unwrap();
                }

                x => {
                    bail!("wrong packet {:?}, expected PromoteToActive", x)
                }
            }

            let packet = self.recv().await.unwrap();
            match &packet {
                Message::RegionInfoPlease => {
                    info!(self.log, "negotiate packet {:?}", packet);

                    self.send(Message::RegionInfo {
                        region_def: self.get_region_definition(),
                    })
                    .unwrap();
                    Ok(())
                }

                x => bail!("wrong packet: {:?}, expected RegionInfoPlease", x),
            }
        }

        pub async fn negotiate_step_extent_versions_please(
            &mut self,
        ) -> Result<()> {
            let packet = self.recv().await.unwrap();
            match &packet {
                Message::ExtentVersionsPlease => {
                    info!(self.log, "negotiate packet {:?}", packet);

                    self.send(Message::ExtentVersions {
                        gen_numbers: self.cfg.gen_numbers.clone(),
                        flush_numbers: self.cfg.flush_numbers.clone(),
                        dirty_bits: self.cfg.dirty_bits.clone(),
                    })
                    .unwrap();
                }

                x => bail!(
                    "wrong packet: {:?}, expected ExtentVersionsPlease",
                    x
                ),
            }

            Ok(())
        }

        pub async fn negotiate_step_last_flush(
            &mut self,
            last_flush_number: JobId,
        ) -> Result<()> {
            let packet = self.recv().await.unwrap();
            match &packet {
                Message::LastFlush { .. } => {
                    info!(self.log, "negotiate packet {:?}", packet);

                    self.send(Message::LastFlushAck { last_flush_number })
                        .unwrap();
                }

                x => bail!("wrong packet: {:?}, expected LastFlush", x),
            }

            Ok(())
        }

        pub fn get_region_options(&self) -> RegionOptions {
            let mut region_options = RegionOptions::default();
            region_options.set_block_size(512);
            region_options.set_extent_size(self.cfg.extent_size);
            region_options.set_uuid(self.uuid);
            region_options.set_encrypted(false);

            region_options.validate().unwrap();

            region_options
        }

        pub fn get_region_definition(&self) -> RegionDefinition {
            let mut def =
                RegionDefinition::from_options(&self.get_region_options())
                    .unwrap();

            def.set_extent_count(self.cfg.extent_count);

            def
        }

        /// Stops the loopback worker, returning the `TcpListener` for reuse
        pub async fn halt(self) -> TcpListener {
            self.stop.send(()).expect("could not stop worker");
            let r = self.loopback_worker.await;
            r.expect("failed to join loopback worker")
                .expect("loopback worker returned an error")
        }
    }

    #[derive(Clone)]
    pub struct DownstairsConfig {
        read_only: bool,

        extent_count: u32,
        extent_size: Block,

        gen_numbers: Vec<u64>,
        flush_numbers: Vec<u64>,
        dirty_bits: Vec<bool>,
    }

    impl DownstairsConfig {
        async fn start(self, log: Logger) -> DownstairsHandle {
            let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let listener = TcpListener::bind(&bind_addr).await.unwrap();

            self.start_with_listener(log, listener).await
        }

        async fn start_with_listener(
            self,
            log: Logger,
            listener: TcpListener,
        ) -> DownstairsHandle {
            let uuid = Uuid::new_v4();
            let local_addr = listener.local_addr().unwrap();

            // Dummy repair task, to get a SocketAddr for the `YesItsMe` reply
            let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let repair_listener = TcpListener::bind(&bind_addr).await.unwrap();
            let repair_addr = repair_listener.local_addr().unwrap();

            let (tx, mut loopback_rx) = mpsc::unbounded_channel();
            let (loopback_tx, rx) = mpsc::unbounded_channel();

            let (stop, mut stop_rx) = oneshot::channel();

            let log_ = log.clone();
            let loopback_worker = tokio::task::spawn(async move {
                let (sock, _raddr) = listener.accept().await.unwrap();
                info!(log, "loopback worker connected");

                let (read, write) = sock.into_split();

                let mut fr = FramedRead::new(read, CrucibleDecoder::new());
                let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

                loop {
                    tokio::select! {
                        _ = &mut stop_rx => {
                            info!(
                                log,
                                "stop sent, time to exit"
                            );
                            break;
                        }
                        r = fr.next() => {
                            if let Some(t) = r {
                                if let Err(e) = t {
                                    info!(
                                        log,
                                        "framed read error: {e:?}; exiting"
                                    );
                                    break;
                                } else if let Err(e) = loopback_tx.send(
                                    t.unwrap())
                                {
                                    info!(
                                        log,
                                        "loopback_tx error: {e:?}; exiting"
                                    );
                                    break;
                                }
                            } else {
                                info!(
                                    log,
                                    "framed read disconnected; exiting"
                                );
                                break;
                            }
                        }
                        t = loopback_rx.recv() => {
                            if let Some(t) = t {
                                if let Err(e) = fw.send(t).await {
                                    info!(
                                        log,
                                        "framed write error {e:?}; exiting"
                                    );
                                    break;
                                }
                            } else {
                                info!(
                                    log,
                                    "loopback mpsc rx channel closed, exiting"
                                );
                                break;
                            }
                        }
                    }
                }
                Ok(listener)
            });

            DownstairsHandle {
                log: log_,
                loopback_worker,
                rx,
                tx,

                stop,
                uuid,
                cfg: self,
                upstairs_session_id: None,
                local_addr,
                repair_addr,
            }
        }
    }

    pub struct TestHarness {
        log: Logger,

        ds1: Option<DownstairsHandle>,
        ds2: DownstairsHandle,
        ds3: DownstairsHandle,

        /// JoinHandle for the Crucible `up_main` task
        _join_handle: JoinHandle<()>,

        /// Handle to the guest, to submit IOs
        guest: Arc<Guest>,
    }

    impl TestHarness {
        pub async fn new() -> Result<TestHarness> {
            Self::new_(false).await
        }

        pub async fn new_ro() -> Result<TestHarness> {
            Self::new_(true).await
        }

        pub fn ds1(&mut self) -> &mut DownstairsHandle {
            self.ds1.as_mut().unwrap()
        }

        fn default_config(read_only: bool) -> DownstairsConfig {
            DownstairsConfig {
                read_only,

                extent_count: 10,
                extent_size: Block::new_512(10),

                gen_numbers: vec![0u64; 10],
                flush_numbers: vec![0u64; 10],
                dirty_bits: vec![false; 10],
            }
        }

        async fn new_(read_only: bool) -> Result<TestHarness> {
            let log = csl();

            let cfg = Self::default_config(read_only);

            let ds1 = cfg.clone().start(log.new(o!("downstairs" => 1))).await;
            let ds2 = cfg.clone().start(log.new(o!("downstairs" => 2))).await;
            let ds3 = cfg.clone().start(log.new(o!("downstairs" => 3))).await;

            // Configure our guest without backpressure, to speed up tests which
            // require triggering a timeout
            let (g, mut io) = Guest::new(Some(log.clone()));
            io.disable_queue_backpressure();
            io.disable_byte_backpressure();
            let guest = Arc::new(g);

            let crucible_opts = CrucibleOpts {
                id: Uuid::new_v4(),
                target: vec![ds1.local_addr, ds2.local_addr, ds3.local_addr],
                flush_timeout: Some(86400.0),
                read_only,

                ..Default::default()
            };

            let join_handle =
                up_main(crucible_opts, 1, None, io, None).unwrap();

            let mut handles: Vec<JoinHandle<Result<()>>> = vec![];

            {
                let guest = guest.clone();
                handles.push(tokio::spawn(async move {
                    guest.activate().await?;
                    Ok(())
                }));
            }

            // Connect all 3x Downstairs
            let mut fut = futures::stream::FuturesOrdered::new();
            for mut ds in [ds1, ds2, ds3] {
                fut.push_back(async move {
                    ds.negotiate_start().await?;
                    ds.negotiate_step_extent_versions_please().await?;
                    Result::<_>::Ok(ds)
                });
            }

            let ds1 = fut.next().await.unwrap().unwrap();
            let ds2 = fut.next().await.unwrap().unwrap();
            let ds3 = fut.next().await.unwrap().unwrap();

            for _ in 0..10 {
                if guest.query_is_active().await.unwrap() {
                    break;
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            assert!(guest.query_is_active().await.unwrap());

            Ok(TestHarness {
                log,
                ds1: Some(ds1),
                ds2,
                ds3,
                _join_handle: join_handle,
                guest,
            })
        }

        fn take_ds1(&mut self) -> DownstairsHandle {
            self.ds1.take().unwrap()
        }

        /// Restarts ds1, without doing any negotiation after it comes up
        pub async fn restart_ds1(&mut self) {
            let ds1 = self.take_ds1();
            let cfg = ds1.cfg.clone();
            let log = ds1.log.clone();
            let listener = ds1.halt().await;
            self.ds1 = Some(cfg.start_with_listener(log, listener).await);
        }
    }

    fn make_blank_read_response() -> (ReadResponseBlockMetadata, BytesMut) {
        let data = vec![0u8; 512];
        let hash = crucible_common::integrity_hash(&[&data]);

        (
            ReadResponseBlockMetadata {
                eid: 0,
                offset: Block::new_512(0),
                block_contexts: vec![BlockContext {
                    hash,
                    encryption_context: None,
                }],
            },
            BytesMut::from(&data[..]),
        )
    }

    /// Filter the first element that matches some predicate out of a list
    pub fn filter_out<T, P>(l: &mut Vec<T>, pred: P) -> Option<T>
    where
        P: FnMut(&T) -> bool,
    {
        let idx = l.iter().position(pred);
        idx.map(|i| l.remove(i))
    }

    /// Test that replay occurs after a downstairs disconnects and reconnects
    #[tokio::test]
    async fn test_replay_occurs() -> Result<()> {
        let mut harness = TestHarness::new().await?;

        // Send a read
        {
            let guest = harness.guest.clone();

            // We must tokio::spawn here because `read` will wait for the
            // response to come back before returning
            tokio::spawn(async move {
                let mut buffer = Buffer::new(1, 512);
                guest.read(Block::new_512(0), &mut buffer).await.unwrap();
            });
        }

        // Confirm all downstairs receive said read
        let ds1_message = harness.ds1().recv().await.unwrap();

        assert!(matches!(ds1_message, Message::ReadRequest { .. }));

        assert!(matches!(
            harness.ds2.recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        assert!(matches!(
            harness.ds3.recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        // If downstairs 1 disconnects and reconnects, it should get the exact
        // same message replayed to it.

        harness.restart_ds1().await;

        harness.ds1().negotiate_start().await?;
        harness.ds1().negotiate_step_last_flush(JobId(0)).await?;

        let mut ds1_message_second_time = None;

        for _ in 0..10 {
            if let Ok(m) = harness.ds1().try_recv() {
                ds1_message_second_time = Some(m);
                break;
            }

            eprintln!("waiting for ds1 message in test_replay_occurs");

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        assert_eq!(ds1_message, ds1_message_second_time.unwrap());

        Ok(())
    }

    /// Test that after giving up on a downstairs, setting it to faulted, and
    /// letting it reconnect, live repair occurs. Check that each extent is
    /// repaired with the correct source, and that extent limits are honoured if
    /// additional IO comes through.
    #[tokio::test]
    async fn test_successful_live_repair() -> Result<()> {
        let mut harness = TestHarness::new().await?;

        // Send 200 more than IO_OUTSTANDING_MAX_JOBS jobs, sending read
        // responses from two of the three downstairs. After we have sent
        // IO_OUTSTANDING_MAX_JOBS jobs, the Upstairs will set ds1 to faulted,
        // and send it no more work.
        const NUM_JOBS: usize = IO_OUTSTANDING_MAX_JOBS + 200;
        let mut job_ids = Vec::with_capacity(NUM_JOBS);

        for i in 0..NUM_JOBS {
            {
                let guest = harness.guest.clone();

                // We must tokio::spawn here because `read` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    let mut buffer = Buffer::new(1, 512);
                    guest.read(Block::new_512(0), &mut buffer).await.unwrap();
                });
            }

            if i < IO_OUTSTANDING_MAX_JOBS {
                // Before we're kicked out, assert we're seeing the read
                // requests
                assert!(matches!(
                    harness.ds1().recv().await.unwrap(),
                    Message::ReadRequest { .. },
                ));
            } else {
                // After ds1 is kicked out, we shouldn't see any more messages
                match harness.ds1().try_recv() {
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {}
                    x => {
                        info!(
                            harness.log,
                            "Read {i} should return EMPTY, but we got:{:?}", x
                        );

                        bail!(
                            "Read {i} should return EMPTY, but we got:{:?}",
                            x
                        );
                    }
                }
            }

            match harness.ds2.recv().await.unwrap() {
                Message::ReadRequest { job_id, .. } => {
                    // Record the job ids of the read requests
                    job_ids.push(job_id);
                }

                _ => bail!("saw non read request!"),
            }

            assert!(matches!(
                harness.ds3.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));

            // Respond with read responses for downstairs 2 and 3
            let (block, data) = make_blank_read_response();
            harness
                .ds2
                .send(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: harness.guest.get_uuid().await.unwrap(),
                        session_id: harness.ds2.upstairs_session_id.unwrap(),
                        job_id: job_ids[i],
                        blocks: Ok(vec![block.clone()]),
                    },
                    data: data.clone(),
                })
                .unwrap();

            harness
                .ds3
                .send(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: harness.guest.get_uuid().await.unwrap(),
                        session_id: harness.ds3.upstairs_session_id.unwrap(),
                        job_id: job_ids[i],
                        blocks: Ok(vec![block.clone()]),
                    },
                    data: data.clone(),
                })
                .unwrap();
        }

        // Confirm that's all the Upstairs sent us (only ds2 and ds3) - with the
        // flush_timeout set to 24 hours, we shouldn't see anything else
        assert!(matches!(harness.ds2.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(harness.ds3.try_recv(), Err(TryRecvError::Empty)));

        // Flush to clean out skipped jobs
        {
            let jh = {
                let guest = harness.guest.clone();

                // We must tokio::spawn here because `flush` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    guest.flush(None).await.unwrap();
                })
            };

            let flush_job_id = match harness.ds2.recv().await.unwrap() {
                Message::Flush { job_id, .. } => job_id,

                _ => bail!("saw non flush ack!"),
            };

            assert!(matches!(
                harness.ds3.recv().await.unwrap(),
                Message::Flush { .. },
            ));

            harness
                .ds2
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness.ds2.upstairs_session_id.unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .unwrap();

            harness
                .ds3
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness.ds3.upstairs_session_id.unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .unwrap();

            // Wait for the flush to come back
            jh.await.unwrap();
        }

        // Send ds1 responses for the jobs it saw
        for (i, job_id) in job_ids.iter().enumerate() {
            let (block, data) = make_blank_read_response();
            let session_id = harness.ds1().upstairs_session_id.unwrap();
            let upstairs_id = harness.guest.get_uuid().await.unwrap();
            match harness.ds1().send(Message::ReadResponse {
                header: ReadResponseHeader {
                    upstairs_id,
                    session_id,
                    job_id: *job_id,
                    blocks: Ok(vec![block.clone()]),
                },
                data: data.clone(),
            }) {
                Ok(()) => {}
                Err(e) => {
                    // We should be able to send a few, but at some point
                    // the Upstairs will disconnect us.
                    error!(
                        harness.log,
                        "could not send read response for job {} = {}: {}",
                        i,
                        job_id,
                        e
                    );
                    break;
                }
            }
        }

        // Assert the Upstairs isn't sending ds1 more work, because it is
        // Faulted
        let v = harness.ds1().try_recv();
        match v {
            // We're either disconnected, or the queue is empty.
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                // This is expected, continue on
            }

            _ => {
                // Any other error (or success!) is unexpected
                bail!("try_recv returned {:?}", v);
            }
        }

        // Reconnect ds1
        harness.restart_ds1().await;

        harness.ds1().negotiate_start().await?;
        harness
            .ds1()
            .negotiate_step_extent_versions_please()
            .await?;

        // The Upstairs will start sending LiveRepair related work, which may be
        // out of order. Buffer some here.

        let mut ds1_buffered_messages = vec![];
        let mut ds2_buffered_messages = vec![];
        let mut ds3_buffered_messages = vec![];

        for eid in 0..10 {
            // The Upstairs first sends the close and reopen jobs
            for _ in 0..2 {
                ds1_buffered_messages.push(harness.ds1().recv().await.unwrap());
                ds2_buffered_messages.push(harness.ds2.recv().await.unwrap());
                ds3_buffered_messages.push(harness.ds3.recv().await.unwrap());
            }

            assert!(ds1_buffered_messages
                .iter()
                .any(|m| matches!(m, Message::ExtentLiveClose { .. })));
            assert!(ds2_buffered_messages
                .iter()
                .any(|m| matches!(m, Message::ExtentLiveFlushClose { .. })));
            assert!(ds3_buffered_messages
                .iter()
                .any(|m| matches!(m, Message::ExtentLiveFlushClose { .. })));

            assert!(ds1_buffered_messages
                .iter()
                .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));
            assert!(ds2_buffered_messages
                .iter()
                .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));
            assert!(ds3_buffered_messages
                .iter()
                .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));

            // This is the reopen job id for extent eid

            let reopen_job_id = {
                let m = ds1_buffered_messages
                    .iter()
                    .position(|m| matches!(m, Message::ExtentLiveReopen { .. }))
                    .unwrap();
                match ds1_buffered_messages[m] {
                    Message::ExtentLiveReopen { job_id, .. } => {
                        job_id
                    }

                    _ => bail!("ds1_buffered_messages[m] not Message::ExtentLiveReopen"),
                }
            };

            // Extent limit is Some(eid), where eid is the current loop
            // iteration. It marks the extent at and below are clear to receive
            // IO. Issue some single extent reads and writes to make sure that
            // extent limit is honoured. Do this only after receiving the two
            // above messages as that guarantees we are in the repair task and
            // that extent_limit is set. Make sure that the first read to the
            // extent under repair has the the ExtentLiveReopen job as a
            // dependency, and that later writes have that read as their
            // dependency (which works because the read already depended on the
            // ExtentLiveReopen job). Batch up responses to send after the live
            // repair is done.

            let mut responses = vec![Vec::new(); 3];

            for io_eid in 0usize..10 {
                let mut dep_job_id = [reopen_job_id; 3];
                // read

                {
                    let guest = harness.guest.clone();
                    tokio::spawn(async move {
                        let mut buffer = Buffer::new(1, 512);
                        guest
                            .read(
                                Block::new_512(io_eid as u64 * 10),
                                &mut buffer,
                            )
                            .await
                            .unwrap();
                    })
                };

                if io_eid <= eid {
                    // IO at or below the extent under repair is sent to the
                    // downstairs under repair.
                    let m1 = harness.ds1().recv().await.unwrap();

                    match &m1 {
                        Message::ReadRequest {
                            upstairs_id,
                            session_id,
                            job_id,
                            dependencies,
                            ..
                        } => {
                            if io_eid == eid {
                                assert!(dependencies.contains(&dep_job_id[0]));
                            }

                            let (block, data) = make_blank_read_response();
                            responses[0].push(Message::ReadResponse {
                                header: ReadResponseHeader {
                                    upstairs_id: *upstairs_id,
                                    session_id: *session_id,
                                    job_id: *job_id,
                                    blocks: Ok(vec![block.clone()]),
                                },
                                data,
                            });

                            // At this point, the next operation is going to be
                            // a write.  This write will depend on the three
                            // reads that are already enqueued (but not the
                            // repair close, because the reads already
                            // implicitly depend on the repair close id).  We'll
                            // update our target dep_job_id to match this read.
                            dep_job_id[0] = *job_id;
                        }

                        _ => bail!("saw {:?}", m1),
                    }
                } else {
                    // All IO above this is skipped for the downstairs under
                    // repair.
                    assert!(matches!(
                        harness.ds1().try_recv(),
                        Err(TryRecvError::Empty)
                    ));
                }

                let m2 = harness.ds2.recv().await.unwrap();
                let m3 = harness.ds3.recv().await.unwrap();

                match &m2 {
                    Message::ReadRequest {
                        upstairs_id,
                        session_id,
                        job_id,
                        dependencies,
                        ..
                    } => {
                        if io_eid == eid {
                            assert!(dependencies.contains(&dep_job_id[1]));
                        }

                        let (block, data) = make_blank_read_response();
                        responses[1].push(Message::ReadResponse {
                            header: ReadResponseHeader {
                                upstairs_id: *upstairs_id,
                                session_id: *session_id,
                                job_id: *job_id,
                                blocks: Ok(vec![block.clone()]),
                            },
                            data,
                        });
                        dep_job_id[1] = *job_id;
                    }

                    _ => bail!("saw {:?}", m2),
                }

                match &m3 {
                    Message::ReadRequest {
                        upstairs_id,
                        session_id,
                        job_id,
                        dependencies,
                        ..
                    } => {
                        if io_eid == eid {
                            assert!(dependencies.contains(&dep_job_id[2]));
                        }

                        let (block, data) = make_blank_read_response();
                        responses[2].push(Message::ReadResponse {
                            header: ReadResponseHeader {
                                upstairs_id: *upstairs_id,
                                session_id: *session_id,
                                job_id: *job_id,
                                blocks: Ok(vec![block.clone()]),
                            },
                            data,
                        });
                        dep_job_id[2] = *job_id;
                    }

                    _ => bail!("saw {:?}", m3),
                }

                // write

                {
                    let guest = harness.guest.clone();
                    tokio::spawn(async move {
                        let bytes = BytesMut::from(vec![1u8; 512].as_slice());
                        guest
                            .write(Block::new_512(io_eid as u64 * 10), bytes)
                            .await
                            .unwrap();
                    })
                };

                if io_eid <= eid {
                    // IO at or below the extent under repair is sent to the
                    // downstairs under repair.
                    let m1 = harness.ds1().recv().await.unwrap();

                    match &m1 {
                        Message::Write {
                            header:
                                WriteHeader {
                                    upstairs_id,
                                    session_id,
                                    job_id,
                                    dependencies,
                                    ..
                                },
                            ..
                        } => {
                            if io_eid == eid {
                                assert!(dependencies.contains(&dep_job_id[0]));
                            }

                            responses[0].push(Message::WriteAck {
                                upstairs_id: *upstairs_id,
                                session_id: *session_id,
                                job_id: *job_id,
                                result: Ok(()),
                            });
                            // Writes are blocking, so we need to update
                            // dep_job_id right away:
                            dep_job_id[0] = *job_id;
                        }

                        _ => bail!("saw {:?}", m1),
                    }
                } else {
                    // All IO above this is skipped for the downstairs under
                    // repair.
                    assert!(matches!(
                        harness.ds1().try_recv(),
                        Err(TryRecvError::Empty)
                    ));
                }

                let m2 = harness.ds2.recv().await.unwrap();
                let m3 = harness.ds3.recv().await.unwrap();

                match &m2 {
                    Message::Write {
                        header:
                            WriteHeader {
                                upstairs_id,
                                session_id,
                                job_id,
                                dependencies,
                                ..
                            },
                        ..
                    } => {
                        if io_eid == eid {
                            assert!(dependencies.contains(&dep_job_id[1]));
                        }

                        responses[1].push(Message::WriteAck {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        });
                        dep_job_id[1] = *job_id;
                    }

                    _ => bail!("saw {:?}", m2),
                }

                match &m3 {
                    Message::Write {
                        header:
                            WriteHeader {
                                upstairs_id,
                                session_id,
                                job_id,
                                dependencies,
                                ..
                            },
                        ..
                    } => {
                        if io_eid == eid {
                            assert!(dependencies.contains(&dep_job_id[2]));
                        }

                        responses[2].push(Message::WriteAck {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        });
                        dep_job_id[2] = *job_id;
                    }

                    _ => bail!("saw {:?}", m3),
                }
            }

            // The repair task then waits for the close responses.

            let m1 = filter_out(&mut ds1_buffered_messages, |x| {
                matches!(x, Message::ExtentLiveClose { .. })
            })
            .unwrap();
            let m2 = filter_out(&mut ds2_buffered_messages, |x| {
                matches!(x, Message::ExtentLiveFlushClose { .. })
            })
            .unwrap();
            let m3 = filter_out(&mut ds3_buffered_messages, |x| {
                matches!(x, Message::ExtentLiveFlushClose { .. })
            })
            .unwrap();

            match &m1 {
                Message::ExtentLiveClose {
                    upstairs_id,
                    session_id,
                    job_id,
                    extent_id,
                    ..
                } => {
                    assert!(*extent_id == eid);

                    // ds1 didn't get the flush, it was set to faulted
                    let gen = 1;
                    let flush = 0;
                    let dirty = false;

                    harness
                        .ds1()
                        .send(Message::ExtentLiveCloseAck {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok((gen, flush, dirty)),
                        })
                        .unwrap();
                }

                _ => bail!("saw {:?}", m1),
            }

            match &m2 {
                Message::ExtentLiveFlushClose {
                    upstairs_id,
                    session_id,
                    job_id,
                    extent_id,
                    ..
                } => {
                    assert!(*extent_id == eid);

                    // ds2 and ds3 did get a flush
                    let gen = 0;
                    let flush = 2;
                    let dirty = false;

                    harness
                        .ds2
                        .send(Message::ExtentLiveCloseAck {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok((gen, flush, dirty)),
                        })
                        .unwrap()
                }

                _ => bail!("saw {:?}", m2),
            }

            match &m3 {
                Message::ExtentLiveFlushClose {
                    upstairs_id,
                    session_id,
                    job_id,
                    extent_id,
                    ..
                } => {
                    assert!(*extent_id == eid);

                    // ds2 and ds3 did get a flush
                    let gen = 0;
                    let flush = 2;
                    let dirty = false;

                    harness
                        .ds3
                        .send(Message::ExtentLiveCloseAck {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok((gen, flush, dirty)),
                        })
                        .unwrap()
                }

                _ => bail!("saw {:?}", m3),
            }

            // Based on those gen, flush, and dirty values, ds1 should get the
            // ExtentLiveRepair message, while ds2 and ds3 should get
            // ExtentLiveNoOp.

            let m1 = harness.ds1().recv().await.unwrap();
            let m2 = harness.ds2.recv().await.unwrap();
            let m3 = harness.ds3.recv().await.unwrap();

            match &m1 {
                Message::ExtentLiveRepair {
                    upstairs_id,
                    session_id,
                    job_id,
                    extent_id,
                    source_client_id,
                    ..
                } => {
                    assert!(*source_client_id != ClientId::new(0));
                    assert!(*extent_id == eid);

                    harness
                        .ds1()
                        .send(Message::ExtentLiveRepairAckId {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        })
                        .unwrap();
                }

                _ => bail!("saw {:?}", m3),
            }

            match &m2 {
                Message::ExtentLiveNoOp {
                    upstairs_id,
                    session_id,
                    job_id,
                    ..
                } => harness
                    .ds2
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .unwrap(),

                _ => bail!("saw {:?}", m2),
            }

            match &m3 {
                Message::ExtentLiveNoOp {
                    upstairs_id,
                    session_id,
                    job_id,
                    ..
                } => harness
                    .ds3
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .unwrap(),

                _ => bail!("saw {:?}", m2),
            }

            // Now, all downstairs will see ExtentLiveNoop

            let m1 = harness.ds1().recv().await.unwrap();
            let m2 = harness.ds2.recv().await.unwrap();
            let m3 = harness.ds3.recv().await.unwrap();

            match &m1 {
                Message::ExtentLiveNoOp {
                    upstairs_id,
                    session_id,
                    job_id,
                    ..
                } => harness
                    .ds1()
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .unwrap(),

                _ => bail!("saw {:?}", m2),
            }

            match &m2 {
                Message::ExtentLiveNoOp {
                    upstairs_id,
                    session_id,
                    job_id,
                    ..
                } => harness
                    .ds2
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .unwrap(),

                _ => bail!("saw {:?}", m2),
            }

            match &m3 {
                Message::ExtentLiveNoOp {
                    upstairs_id,
                    session_id,
                    job_id,
                    ..
                } => harness
                    .ds3
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .unwrap(),

                _ => bail!("saw {:?}", m2),
            }

            // Finally, processing the ExtentLiveNoOp above means that the
            // dependencies for the final Reopen are all completed.

            let m1 = filter_out(&mut ds1_buffered_messages, |x| {
                matches!(x, Message::ExtentLiveReopen { .. })
            })
            .unwrap();
            let m2 = filter_out(&mut ds2_buffered_messages, |x| {
                matches!(x, Message::ExtentLiveReopen { .. })
            })
            .unwrap();
            let m3 = filter_out(&mut ds3_buffered_messages, |x| {
                matches!(x, Message::ExtentLiveReopen { .. })
            })
            .unwrap();

            match &m1 {
                Message::ExtentLiveReopen {
                    upstairs_id,
                    session_id,
                    job_id,
                    extent_id,
                    ..
                } => {
                    assert!(*extent_id == eid);

                    harness
                        .ds1()
                        .send(Message::ExtentLiveAckId {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        })
                        .unwrap()
                }

                _ => bail!("saw {:?}", m2),
            }

            match &m2 {
                Message::ExtentLiveReopen {
                    upstairs_id,
                    session_id,
                    job_id,
                    extent_id,
                    ..
                } => {
                    assert!(*extent_id == eid);

                    harness
                        .ds2
                        .send(Message::ExtentLiveAckId {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        })
                        .unwrap()
                }

                _ => bail!("saw {:?}", m2),
            }

            match &m3 {
                Message::ExtentLiveReopen {
                    upstairs_id,
                    session_id,
                    job_id,
                    extent_id,
                    ..
                } => {
                    assert!(*extent_id == eid);

                    harness
                        .ds3
                        .send(Message::ExtentLiveAckId {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        })
                        .unwrap()
                }

                _ => bail!("saw {:?}", m2),
            }

            // After those are done, send out the read and write job responses
            for m in &responses[0] {
                harness.ds1().send(m.clone()).unwrap();
            }
            for m in &responses[1] {
                harness.ds2.send(m.clone()).unwrap();
            }
            for m in &responses[2] {
                harness.ds3.send(m.clone()).unwrap();
            }
        }

        // Expect the live repair to send a final flush
        {
            let flush_job_id = match harness.ds1().recv().await.unwrap() {
                Message::Flush {
                    job_id,
                    flush_number: 12,
                    ..
                } => job_id,

                _ => bail!("saw non flush!"),
            };

            assert!(matches!(
                harness.ds2.recv().await.unwrap(),
                Message::Flush {
                    flush_number: 12,
                    ..
                },
            ));

            assert!(matches!(
                harness.ds3.recv().await.unwrap(),
                Message::Flush {
                    flush_number: 12,
                    ..
                },
            ));

            let upstairs_id = harness.guest.get_uuid().await.unwrap();
            let session_id = harness.ds1().upstairs_session_id.unwrap();
            harness
                .ds1()
                .send(Message::FlushAck {
                    upstairs_id,
                    session_id,
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .unwrap();

            harness
                .ds2
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness.ds2.upstairs_session_id.unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .unwrap();

            harness
                .ds3
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness.ds3.upstairs_session_id.unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .unwrap();
        }

        // Try another read
        {
            {
                let guest = harness.guest.clone();

                // We must tokio::spawn here because `read` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    let mut buffer = Buffer::new(1, 512);
                    guest.read(Block::new_512(0), &mut buffer).await.unwrap();
                });
            }

            // All downstairs should see it

            assert!(matches!(
                harness.ds1().recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));

            assert!(matches!(
                harness.ds2.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));

            assert!(matches!(
                harness.ds3.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));
        }

        Ok(())
    }

    /// Test that we will mark a Downstairs as failed if we hit the byte limit
    #[tokio::test]
    async fn test_byte_fault_condition() -> Result<()> {
        let mut harness = TestHarness::new().await?;

        // Send enough bytes to hit the IO_OUTSTANDING_MAX_BYTES condition on
        // downstairs 1, which should mark it as faulted and kick it out.
        const WRITE_SIZE: usize = 50 * 1024; // 50 KiB
        let write_buf = BytesMut::from(vec![1; WRITE_SIZE].as_slice()); // 50 KiB
        let num_jobs = IO_OUTSTANDING_MAX_BYTES as usize / write_buf.len() + 10;
        let mut job_ids = Vec::with_capacity(num_jobs);
        assert!(num_jobs < IO_OUTSTANDING_MAX_JOBS);

        for i in 0..num_jobs {
            {
                let guest = harness.guest.clone();

                // We must tokio::spawn here because `write` will wait for the
                // response to come back before returning
                let write_buf = write_buf.clone();
                tokio::spawn(async move {
                    guest.write(Block::new_512(0), write_buf).await.unwrap();
                });
            }

            if (i + 1) * WRITE_SIZE < IO_OUTSTANDING_MAX_BYTES as usize {
                // Before we're kicked out, assert we're seeing the read
                // requests
                assert!(matches!(
                    harness.ds1().recv().await.unwrap(),
                    Message::Write { .. },
                ));
            } else {
                // After ds1 is kicked out, we shouldn't see any more messages
                match harness.ds1().try_recv() {
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {}
                    x => {
                        info!(
                            harness.log,
                            "Read {i} should return EMPTY, but we got:{:?}", x
                        );

                        bail!(
                            "Read {i} should return EMPTY, but we got:{:?}",
                            x
                        );
                    }
                }
            }

            match harness.ds2.recv().await.unwrap() {
                Message::Write {
                    header: WriteHeader { job_id, .. },
                    ..
                } => {
                    // Record the job ids of the write requests
                    job_ids.push(job_id);
                }

                _ => bail!("saw non write request!"),
            }

            assert!(matches!(
                harness.ds3.recv().await.unwrap(),
                Message::Write { .. },
            ));

            // Respond with write responses for downstairs 2 and 3
            harness
                .ds2
                .send(Message::WriteAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness.ds2.upstairs_session_id.unwrap(),
                    job_id: job_ids[i],
                    result: Ok(()),
                })
                .unwrap();

            harness
                .ds3
                .send(Message::WriteAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness.ds3.upstairs_session_id.unwrap(),
                    job_id: job_ids[i],

                    result: Ok(()),
                })
                .unwrap();
        }

        // Confirm that's all the Upstairs sent us (only ds2 and ds3) - with the
        // flush_timeout set to 24 hours, we shouldn't see anything else
        assert!(matches!(harness.ds2.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(harness.ds3.try_recv(), Err(TryRecvError::Empty)));

        // Assert the Upstairs isn't sending ds1 more work, because it is
        // Faulted
        let v = harness.ds1().try_recv();
        assert_eq!(
            v,
            Err(TryRecvError::Disconnected),
            "ds1 message queue must be disconnected"
        );
        Ok(())
    }

    /// Test that an error during the live repair doesn't halt indefinitely
    #[tokio::test]
    async fn test_error_during_live_repair_no_halt() -> Result<()> {
        let mut harness = TestHarness::new().await?;

        // Send 200 more than IO_OUTSTANDING_MAX_JOBS jobs, sending read
        // responses from two of the three downstairs. After we have sent
        // IO_OUTSTANDING_MAX_JOBS jobs, the Upstairs will set ds1 to faulted,
        // and send it no more work.
        const NUM_JOBS: usize = IO_OUTSTANDING_MAX_JOBS + 200;
        let mut job_ids = Vec::with_capacity(NUM_JOBS);

        for i in 0..NUM_JOBS {
            {
                let guest = harness.guest.clone();

                // We must tokio::spawn here because `read` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    let mut buffer = Buffer::new(1, 512);
                    guest.read(Block::new_512(0), &mut buffer).await.unwrap();
                });
            }

            if i < IO_OUTSTANDING_MAX_JOBS {
                // Before we're kicked out, assert we're seeing the read
                // requests
                assert!(matches!(
                    harness.ds1().recv().await.unwrap(),
                    Message::ReadRequest { .. },
                ));
            } else {
                // After ds1 is kicked out, we shouldn't see any more messages
                match harness.ds1().try_recv() {
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {}
                    x => {
                        info!(
                            harness.log,
                            "Read {i} should return EMPTY, but we got:{:?}", x
                        );

                        bail!(
                            "Read {i} should return EMPTY, but we got:{:?}",
                            x
                        );
                    }
                }
            }

            match harness.ds2.recv().await.unwrap() {
                Message::ReadRequest { job_id, .. } => {
                    // Record the job ids of the read requests
                    job_ids.push(job_id);
                }

                _ => bail!("saw non read request!"),
            }

            assert!(matches!(
                harness.ds3.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));

            // Respond with read responses for downstairs 2 and 3
            let (block, data) = make_blank_read_response();
            harness
                .ds2
                .send(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: harness.guest.get_uuid().await.unwrap(),
                        session_id: harness.ds2.upstairs_session_id.unwrap(),
                        job_id: job_ids[i],
                        blocks: Ok(vec![block.clone()]),
                    },
                    data: data.clone(),
                })
                .unwrap();

            harness
                .ds3
                .send(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: harness.guest.get_uuid().await.unwrap(),
                        session_id: harness.ds3.upstairs_session_id.unwrap(),
                        job_id: job_ids[i],
                        blocks: Ok(vec![block.clone()]),
                    },
                    data: data.clone(),
                })
                .unwrap();
        }

        // Confirm that's all the Upstairs sent us (only ds2 and ds3) - with the
        // flush_timeout set to 24 hours, we shouldn't see anything else
        assert!(matches!(harness.ds2.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(harness.ds3.try_recv(), Err(TryRecvError::Empty)));

        // Flush to clean out skipped jobs
        {
            let jh = {
                let guest = harness.guest.clone();

                // We must tokio::spawn here because `flush` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    guest.flush(None).await.unwrap();
                })
            };

            let flush_job_id = match harness.ds2.recv().await.unwrap() {
                Message::Flush { job_id, .. } => job_id,

                _ => bail!("saw non flush ack!"),
            };

            assert!(matches!(
                harness.ds3.recv().await.unwrap(),
                Message::Flush { .. },
            ));

            harness
                .ds2
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness.ds2.upstairs_session_id.unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .unwrap();

            harness
                .ds3
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness.ds3.upstairs_session_id.unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .unwrap();

            // Wait for the flush to come back
            jh.await.unwrap();
        }

        // Send ds1 responses for the jobs it saw
        for (i, job_id) in job_ids.iter().enumerate() {
            let (block, data) = make_blank_read_response();
            let upstairs_id = harness.guest.get_uuid().await.unwrap();
            let session_id = harness.ds1().upstairs_session_id.unwrap();
            match harness.ds1().send(Message::ReadResponse {
                header: ReadResponseHeader {
                    upstairs_id,
                    session_id,
                    job_id: *job_id,
                    blocks: Ok(vec![block.clone()]),
                },
                data: data.clone(),
            }) {
                Ok(()) => {
                    info!(
                        harness.log,
                        "sent read response for job {} = {}", i, job_id,
                    );
                }

                Err(e) => {
                    // We should be able to send a few, but at some point
                    // the Upstairs will disconnect us.
                    error!(
                        harness.log,
                        "could not send read response for job {} = {}: {}",
                        i,
                        job_id,
                        e
                    );
                    break;
                }
            }
        }

        // Assert the Upstairs isn't sending ds1 more work, because it is
        // Faulted
        let v = harness.ds1().try_recv();
        match v {
            // We're either disconnected, or the queue is empty.
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                // This is expected, continue on
            }

            _ => {
                // Any other error (or success!) is unexpected
                bail!("try_recv returned {:?}", v);
            }
        }

        // Reconnect ds1
        harness.restart_ds1().await;

        harness.ds1().negotiate_start().await?;
        harness
            .ds1()
            .negotiate_step_extent_versions_please()
            .await?;

        // The Upstairs will start sending LiveRepair related work, which may be
        // out of order. Buffer some here.

        let mut ds1_buffered_messages = vec![];
        let mut ds2_buffered_messages = vec![];
        let mut ds3_buffered_messages = vec![];

        // EID 0

        // The Upstairs first sends the close and reopen jobs
        for _ in 0..2 {
            ds1_buffered_messages.push(harness.ds1().recv().await.unwrap());
            ds2_buffered_messages.push(harness.ds2.recv().await.unwrap());
            ds3_buffered_messages.push(harness.ds3.recv().await.unwrap());
        }

        assert!(ds1_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveClose { .. })));
        assert!(ds2_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveFlushClose { .. })));
        assert!(ds3_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveFlushClose { .. })));

        assert!(ds1_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));
        assert!(ds2_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));
        assert!(ds3_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));

        // The repair task then waits for the close responses.

        let m1 = filter_out(&mut ds1_buffered_messages, |x| {
            matches!(x, Message::ExtentLiveClose { .. })
        })
        .unwrap();
        let m2 = filter_out(&mut ds2_buffered_messages, |x| {
            matches!(x, Message::ExtentLiveFlushClose { .. })
        })
        .unwrap();
        let m3 = filter_out(&mut ds3_buffered_messages, |x| {
            matches!(x, Message::ExtentLiveFlushClose { .. })
        })
        .unwrap();

        match &m1 {
            Message::ExtentLiveClose {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                ..
            } => {
                assert!(*extent_id == 0);

                // ds1 didn't get the flush, it was set to faulted
                let gen = 1;
                let flush = 0;
                let dirty = false;

                harness
                    .ds1()
                    .send(Message::ExtentLiveCloseAck {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok((gen, flush, dirty)),
                    })
                    .unwrap();
            }

            _ => bail!("saw {:?}", m1),
        }

        match &m2 {
            Message::ExtentLiveFlushClose {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                ..
            } => {
                assert!(*extent_id == 0);

                // ds2 and ds3 did get a flush
                let gen = 0;
                let flush = 2;
                let dirty = false;

                harness
                    .ds2
                    .send(Message::ExtentLiveCloseAck {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok((gen, flush, dirty)),
                    })
                    .unwrap()
            }

            _ => bail!("saw {:?}", m2),
        }

        match &m3 {
            Message::ExtentLiveFlushClose {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                ..
            } => {
                assert!(*extent_id == 0);

                // ds2 and ds3 did get a flush
                let gen = 0;
                let flush = 2;
                let dirty = false;

                harness
                    .ds3
                    .send(Message::ExtentLiveCloseAck {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok((gen, flush, dirty)),
                    })
                    .unwrap()
            }

            _ => bail!("saw {:?}", m3),
        }

        // Based on those gen, flush, and dirty values, ds1 should get the
        // ExtentLiveRepair message, while ds2 and ds3 should get
        // ExtentLiveNoOp.

        let m1 = harness.ds1().recv().await.unwrap();
        let m2 = harness.ds2.recv().await.unwrap();
        let m3 = harness.ds3.recv().await.unwrap();

        match &m1 {
            Message::ExtentLiveRepair {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                source_client_id,
                ..
            } => {
                assert!(*source_client_id != ClientId::new(0));
                assert!(*extent_id == 0);

                // send back error report here!
                harness
                    .ds1()
                    .send(Message::ErrorReport {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        error: CrucibleError::GenericError(String::from(
                            "bad news, networks are tricky",
                        )),
                    })
                    .unwrap();
            }

            _ => bail!("saw {:?}", m3),
        }

        match &m2 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds2
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .unwrap(),

            _ => bail!("saw {:?}", m2),
        }

        match &m3 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds3
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .unwrap(),

            _ => bail!("saw {:?}", m2),
        }

        error!(harness.log, "dropping ds1 now!");

        // Now, all downstairs will see ExtentLiveNoop, except ds1, which will
        // abort itself due to an ErrorReport during an extent repair.

        // If the Upstairs doesn't disconnect and try to reconnect to the
        // downstairs, this test will get stuck here, and will not progress to
        // the negotate_start function below.
        error!(harness.log, "reconnecting ds1 now!");
        harness.restart_ds1().await;

        error!(harness.log, "ds1 negotiate start now!");
        harness.ds1().negotiate_start().await?;
        error!(harness.log, "ds1 negotiate extent versions please now!");
        harness
            .ds1()
            .negotiate_step_extent_versions_please()
            .await?;

        // Continue faking for downstairs 2 and 3 - the work that was occuring
        // for extent 0 should finish before the Upstairs aborts the repair
        // task.

        let m2 = harness.ds2.recv().await.unwrap();
        let m3 = harness.ds3.recv().await.unwrap();

        match &m2 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds2
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .unwrap(),

            _ => bail!("saw {:?}", m2),
        }

        match &m3 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds3
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .unwrap(),

            _ => bail!("saw {:?}", m2),
        }

        let m2 = filter_out(&mut ds2_buffered_messages, |x| {
            matches!(x, Message::ExtentLiveReopen { .. })
        })
        .unwrap();
        let m3 = filter_out(&mut ds3_buffered_messages, |x| {
            matches!(x, Message::ExtentLiveReopen { .. })
        })
        .unwrap();

        match &m2 {
            Message::ExtentLiveReopen {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds2
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .unwrap(),

            _ => bail!("saw {:?}", m2),
        }

        match &m3 {
            Message::ExtentLiveReopen {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds3
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .unwrap(),

            _ => bail!("saw {:?}", m2),
        }

        // The Upstairs will abort the live repair task, and will send a final
        // flush to ds2 and ds3. The flush number will not be incremented as it
        // would have been for each extent.

        let flush_job_id = match harness.ds2.recv().await.unwrap() {
            Message::Flush {
                job_id,
                flush_number: 3,
                ..
            } => job_id,

            _ => bail!("saw non flush!"),
        };

        assert!(matches!(
            harness.ds3.recv().await.unwrap(),
            Message::Flush {
                flush_number: 3,
                ..
            },
        ));

        harness
            .ds2
            .send(Message::FlushAck {
                upstairs_id: harness.guest.get_uuid().await.unwrap(),
                session_id: harness.ds2.upstairs_session_id.unwrap(),
                job_id: flush_job_id,
                result: Ok(()),
            })
            .unwrap();

        harness
            .ds3
            .send(Message::FlushAck {
                upstairs_id: harness.guest.get_uuid().await.unwrap(),
                session_id: harness.ds3.upstairs_session_id.unwrap(),
                job_id: flush_job_id,
                result: Ok(()),
            })
            .unwrap();

        // After this, another repair task will start from the beginning, and
        // send a bunch of work to ds1 again.

        assert!(matches!(
            harness.ds1().recv().await.unwrap(),
            Message::ExtentLiveClose { extent_id: 0, .. },
        ));

        assert!(matches!(
            harness.ds1().recv().await.unwrap(),
            Message::ExtentLiveReopen { extent_id: 0, .. },
        ));

        Ok(())
    }

    /// Test that after giving up on a downstairs, setting it to faulted, and
    /// letting it reconnect, live repair does *not* occur if the upstairs is
    /// configured read-only.
    #[tokio::test]
    async fn test_no_read_only_live_repair() -> Result<()> {
        let mut harness = TestHarness::new_ro().await?;

        // Send 200 more than IO_OUTSTANDING_MAX_JOBS jobs, sending read
        // responses from two of the three downstairs. After we have sent
        // IO_OUTSTANDING_MAX_JOBS jobs, the Upstairs will set ds1 to faulted,
        // and send it no more work.
        const NUM_JOBS: usize = IO_OUTSTANDING_MAX_JOBS + 200;
        let mut job_ids = Vec::with_capacity(NUM_JOBS);

        for i in 0..NUM_JOBS {
            {
                let guest = harness.guest.clone();

                // We must tokio::spawn here because `read` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    let mut buffer = Buffer::new(1, 512);
                    guest.read(Block::new_512(0), &mut buffer).await.unwrap();
                });
            }

            if i < IO_OUTSTANDING_MAX_JOBS {
                // Before we're kicked out, assert we're seeing the read
                // requests
                assert!(matches!(
                    harness.ds1().recv().await.unwrap(),
                    Message::ReadRequest { .. },
                ));
            } else {
                // After ds1 is kicked out, we shouldn't see any more messages
                match harness.ds1().try_recv() {
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {}
                    x => {
                        info!(
                            harness.log,
                            "Read {i} should return EMPTY, but we got:{:?}", x
                        );

                        bail!(
                            "Read {i} should return EMPTY, but we got:{:?}",
                            x
                        );
                    }
                }
            }

            match harness.ds2.recv().await.unwrap() {
                Message::ReadRequest { job_id, .. } => {
                    // Record the job ids of the read requests
                    job_ids.push(job_id);
                }

                _ => bail!("saw non read request!"),
            }

            assert!(matches!(
                harness.ds3.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));

            // Respond with read responses for downstairs 2 and 3
            let (block, data) = make_blank_read_response();
            harness
                .ds2
                .send(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: harness.guest.get_uuid().await.unwrap(),
                        session_id: harness.ds2.upstairs_session_id.unwrap(),
                        job_id: job_ids[i],
                        blocks: Ok(vec![block.clone()]),
                    },
                    data: data.clone(),
                })
                .unwrap();

            harness
                .ds3
                .send(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: harness.guest.get_uuid().await.unwrap(),
                        session_id: harness.ds3.upstairs_session_id.unwrap(),
                        job_id: job_ids[i],
                        blocks: Ok(vec![block.clone()]),
                    },
                    data: data.clone(),
                })
                .unwrap();
        }

        // Confirm that's all the Upstairs sent us (only ds2 and ds3) - with the
        // flush_timeout set to 24 hours, we shouldn't see anything else
        assert!(matches!(harness.ds2.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(harness.ds3.try_recv(), Err(TryRecvError::Empty)));

        // Flush to clean out skipped jobs
        {
            let jh = {
                let guest = harness.guest.clone();

                // We must tokio::spawn here because `flush` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    guest.flush(None).await.unwrap();
                })
            };

            let flush_job_id = match harness.ds2.recv().await.unwrap() {
                Message::Flush { job_id, .. } => job_id,

                _ => bail!("saw non flush ack!"),
            };

            assert!(matches!(
                harness.ds3.recv().await.unwrap(),
                Message::Flush { .. },
            ));

            harness
                .ds2
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness.ds2.upstairs_session_id.unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .unwrap();

            harness
                .ds3
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness.ds3.upstairs_session_id.unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .unwrap();

            // Wait for the flush to come back
            jh.await.unwrap();
        }

        // Send ds1 responses for the jobs it saw
        for (i, job_id) in job_ids.iter().enumerate() {
            let (block, data) = make_blank_read_response();
            let upstairs_id = harness.guest.get_uuid().await.unwrap();
            let session_id = harness.ds1().upstairs_session_id.unwrap();
            match harness.ds1().send(Message::ReadResponse {
                header: ReadResponseHeader {
                    upstairs_id,
                    session_id,
                    job_id: *job_id,
                    blocks: Ok(vec![block.clone()]),
                },
                data: data.clone(),
            }) {
                Ok(()) => {}
                Err(e) => {
                    // We should be able to send a few, but at some point
                    // the Upstairs will disconnect us.
                    error!(
                        harness.log,
                        "could not send read response for job {} = {}: {}",
                        i,
                        job_id,
                        e
                    );
                    break;
                }
            }
        }

        // Assert the Upstairs isn't sending ds1 more work, because it is
        // Faulted
        let v = harness.ds1().try_recv();
        match v {
            // We're either disconnected, or the queue is empty.
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                // This is expected, continue on
            }

            _ => {
                // Any other error (or success!) is unexpected
                bail!("try_recv returned {:?}", v);
            }
        }

        // Reconnect ds1
        harness.restart_ds1().await;

        harness.ds1().negotiate_start().await?;
        harness
            .ds1()
            .negotiate_step_extent_versions_please()
            .await?;

        // Wait for all three downstairs to be online before we send
        // our final read.
        loop {
            let qwq = harness.guest.query_work_queue().await.unwrap();
            if qwq.active_count == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        info!(harness.log, "submitting final read!");

        // The read should be served as normal
        let guest = harness.guest.clone();

        // We must tokio::spawn here because `read` will wait for the
        // response to come back before returning
        tokio::spawn(async move {
            let mut buffer = Buffer::new(1, 512);
            guest.read(Block::new_512(0), &mut buffer).await.unwrap();
        });

        // All downstairs should see it
        assert!(matches!(
            harness.ds1().recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));
        assert!(matches!(
            harness.ds2.recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        assert!(matches!(
            harness.ds3.recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        Ok(())
    }

    /// Test that deactivation doesn't fail if one client is slower than others
    #[tokio::test]
    async fn test_deactivate_slow() -> Result<()> {
        let mut harness = TestHarness::new().await?;

        // Queue up a read, so that the deactivate requires a flush
        {
            let guest = harness.guest.clone();

            // We must tokio::spawn here because `read` will wait for the
            // response to come back before returning
            tokio::spawn(async move {
                let mut buffer = Buffer::new(1, 512);
                guest.read(Block::new_512(0), &mut buffer).await.unwrap();
            });
        }

        // Ensure that all three clients got the read request
        let job_id = match harness.ds1().recv().await.unwrap() {
            Message::ReadRequest { job_id, .. } => job_id,
            _ => panic!("invalid request"),
        };
        assert!(matches!(
            harness.ds2.recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));
        assert!(matches!(
            harness.ds3.recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        let (block, data) = make_blank_read_response();
        let response = Message::ReadResponse {
            header: ReadResponseHeader {
                upstairs_id: harness.guest.get_uuid().await.unwrap(),
                session_id: harness.ds1().upstairs_session_id.unwrap(),
                job_id,
                blocks: Ok(vec![block.clone()]),
            },
            data,
        };
        harness.ds1().send(response.clone()).unwrap();
        harness.ds2.send(response.clone()).unwrap();
        harness.ds3.send(response.clone()).unwrap();

        let deactivate_handle = {
            // Send a deactivate request.
            let guest = harness.guest.clone();
            tokio::spawn(async move { guest.deactivate().await })
        };

        let job_id = match harness.ds1().recv().await.unwrap() {
            Message::Flush { job_id, .. } => job_id,
            _ => panic!("invalid request"),
        };
        assert!(matches!(
            harness.ds2.recv().await.unwrap(),
            Message::Flush { .. },
        ));
        assert!(matches!(
            harness.ds3.recv().await.unwrap(),
            Message::Flush { .. },
        ));

        // Reply on ds1, which concludes the deactivation of this downstairs
        let response = Message::FlushAck {
            upstairs_id: harness.guest.get_uuid().await.unwrap(),
            session_id: harness.ds1().upstairs_session_id.unwrap(),
            job_id,
            result: Ok(()),
        };
        harness.ds1().send(response.clone()).unwrap();

        // At this point, the upstairs sends YouAreNoLongerActive, but then
        // drops the sender end, so we can't actually see it.
        assert!(harness.ds1().recv().await.is_none());

        // Reconnect ds1, since the Downstairs always tries to reconnect
        let ds1 = harness.take_ds1();

        // Restart ds1, in a task because this won't actually connect right away
        const RECONNECT_NONE: u8 = 0;
        const RECONNECT_TRYING: u8 = 1;
        const RECONNECT_DONE: u8 = 2;
        let reconnected = Arc::new(AtomicU8::new(RECONNECT_NONE));
        let ds1_restart_handle = {
            let reconnected = reconnected.clone();
            tokio::spawn(async move {
                let cfg = ds1.cfg.clone();
                let log = ds1.log.clone();
                let listener = ds1.halt().await;
                reconnected.store(RECONNECT_TRYING, Ordering::Release);
                let mut ds1 = cfg.start_with_listener(log, listener).await;

                ds1.negotiate_start().await.unwrap();
                reconnected.store(RECONNECT_DONE, Ordering::Release);
                ds1.negotiate_step_extent_versions_please().await.unwrap();
            })
        };

        // Give that task some time to try reconnecting.  It won't get anywhere,
        // because we don't automatically reconnect in this case.  Activating
        // will also fail, because we're still in deactivating.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        assert_eq!(reconnected.load(Ordering::Acquire), RECONNECT_TRYING);
        assert!(harness.guest.activate().await.is_err());

        // Finish deactivation on ds2 / ds3
        harness.ds2.send(response.clone()).unwrap();
        assert_eq!(reconnected.load(Ordering::Acquire), RECONNECT_TRYING);
        assert!(harness.guest.activate().await.is_err());
        harness.ds3.send(response.clone()).unwrap();

        // At this point, the upstairs should be Initializing, but we still
        // won't connect because we don't automatically connect after being
        // Deactivated.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        assert_eq!(reconnected.load(Ordering::Acquire), RECONNECT_TRYING);

        // At this point, deactivation is done and we can join that handle
        deactivate_handle.await.unwrap().unwrap();

        // Now, we can try to reactivate the guest.
        //
        // This won't work, because we haven't restarted ds2 and ds3, but it
        // will allow ds1 to reconnect.
        {
            let guest = harness.guest.clone();
            tokio::spawn(async move { guest.activate().await.unwrap() });
        }

        // At this point, ds1 should reconnect
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        ds1_restart_handle.await.unwrap();
        assert_eq!(reconnected.load(Ordering::Acquire), RECONNECT_DONE);

        Ok(())
    }

    /// Test that replaying writes works
    #[tokio::test]
    async fn test_write_replay() -> Result<()> {
        let mut harness = TestHarness::new().await?;

        // Send a write, which will succeed
        let write_handle = {
            let guest = harness.guest.clone();
            tokio::spawn(async move {
                let mut data = BytesMut::new();
                data.resize(512, 1u8);
                guest.write(Block::new_512(0), data).await.unwrap();
            })
        };

        // Ensure that all three clients got the read request
        let job_id = match harness.ds1().recv().await.unwrap() {
            Message::Write { header, .. } => header.job_id,
            _ => panic!("invalid request"),
        };
        assert!(matches!(
            harness.ds2.recv().await.unwrap(),
            Message::Write { .. },
        ));
        assert!(matches!(
            harness.ds3.recv().await.unwrap(),
            Message::Write { .. },
        ));

        let response = Message::WriteAck {
            upstairs_id: harness.guest.get_uuid().await.unwrap(),
            session_id: harness.ds1().upstairs_session_id.unwrap(),
            job_id,
            result: Ok(()),
        };
        harness.ds1().send(response.clone()).unwrap();
        harness.ds2.send(response.clone()).unwrap();
        harness.ds3.send(response.clone()).unwrap();

        // Check that the write worked
        write_handle.await.unwrap();

        // If downstairs 1 disconnects and reconnects, it should get the exact
        // same message replayed to it.
        harness.restart_ds1().await;

        harness.ds1().negotiate_start().await?;
        harness.ds1().negotiate_step_last_flush(JobId(0)).await?;

        // Ensure that we get the same Write
        match harness.ds1().recv().await.unwrap() {
            Message::Write { header, .. } => assert_eq!(header.job_id, job_id),
            _ => panic!("invalid request"),
        };

        // Send a reply, which is the second time this Write operation completes
        harness.ds1().send(response.clone()).unwrap();

        // Give it a second to think about it
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Check that the guest hasn't panicked by sending it a message that
        // requires going to the worker thread.
        harness.guest.get_uuid().await.unwrap();

        Ok(())
    }
}
