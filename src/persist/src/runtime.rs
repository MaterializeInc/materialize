// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Runtime for concurrent, asynchronous use of [Indexed], and the public API
//! used by the rest of the crate to connect to it.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use build_info::BuildInfo;
use differential_dataflow::trace::Description;
use ore::metrics::MetricsRegistry;
use timely::progress::Antichain;
use tokio::runtime::Runtime as AsyncRuntime;
use tokio::time;

use crate::client::RuntimeClient;
use crate::error::Error;
use crate::indexed::arrangement::ArrangementSnapshot;
use crate::indexed::background::Maintainer;
use crate::indexed::cache::BlobCache;
use crate::indexed::columnar::ColumnarRecords;
use crate::indexed::encoding::Id;
use crate::indexed::metrics::Metrics;
use crate::indexed::{Indexed, ListenEvent};
use crate::pfuture::{PFuture, PFutureHandle};
use crate::storage::{Blob, Log, SeqNo};
use futures_executor::block_on;

#[derive(Debug)]
pub(crate) enum Cmd {
    Register(String, (String, String), PFutureHandle<Id>),
    Destroy(String, PFutureHandle<bool>),
    Write(Vec<(Id, ColumnarRecords)>, PFutureHandle<SeqNo>),
    Seal(Vec<Id>, u64, PFutureHandle<SeqNo>),
    GetDescription(String, PFutureHandle<Description<u64>>),
    AllowCompaction(Vec<(Id, Antichain<u64>)>, PFutureHandle<SeqNo>),
    Snapshot(Id, PFutureHandle<ArrangementSnapshot>),
    Listen(
        Id,
        crossbeam_channel::Sender<ListenEvent>,
        PFutureHandle<ArrangementSnapshot>,
    ),
    Stop(PFutureHandle<()>),
    /// A no-op command sent on a regular interval so the runtime has an
    /// opportunity to do periodic maintenance work.
    Tick,
}

/// Starts the runtime in a [std::thread].
///
/// This returns a clone-able client handle. The runtime is stopped when any
/// client calls [RuntimeClient::stop] or when all clients have been dropped.
///
/// If Some, the given [tokio::runtime::Runtime] is used for IO and cpu heavy
/// operations. If None, a new Runtime is constructed for this. The latter
/// requires that we are not in the context of an existing Runtime, so if this
/// is the case, the caller must use the Some form.
//
// TODO: The rust doc above is still a bit of a lie. Actually use this runtime
// for all IO and cpu heavy operations.
//
// TODO: The whole story around Runtime usage in persist is pretty awkward and
// still pretty unprincipled. I think when we do the TODO to make the Log and
// Blob storage traits async, this will clear up a bit.
pub fn start<L, B>(
    config: RuntimeConfig,
    log: L,
    blob: B,
    build: BuildInfo,
    reg: &MetricsRegistry,
    async_runtime: Option<Arc<AsyncRuntime>>,
) -> Result<RuntimeClient, Error>
where
    L: Log + Send + 'static,
    B: Blob + Send + 'static,
{
    // TODO: Is an unbounded channel the right thing to do here?
    let (tx, rx) = crossbeam_channel::unbounded();
    let metrics = Arc::new(Metrics::register_with(reg));

    let async_runtime = match async_runtime {
        Some(pool) => pool,
        None => Arc::new(AsyncRuntime::new()?),
    };

    // Start up the runtime.
    let blob = BlobCache::new(build, metrics.clone(), async_runtime.clone(), blob);
    let maintainer = Maintainer::new(blob.clone(), async_runtime.clone());
    let indexed = Indexed::new(log, blob, maintainer, metrics.clone())?;
    let mut runtime = RuntimeImpl::new(config.clone(), indexed, rx, metrics.clone());
    let id = RuntimeId::new();
    let impl_handle = thread::Builder::new()
        .name("persist:runtime".into())
        .spawn(move || while runtime.work() {})?;

    // Start up the ticker thread.
    let ticker_tx = tx.clone();
    let ticker_handle = async_runtime.spawn(async move {
        // Try to keep worst case command response times to roughly `110% of
        // min_step_interval` by ensuring there's a tick relatively shortly
        // after a step becomes eligible. We could just as easily make this
        // 2 if we decide 150% is okay.
        let mut interval = time::interval(config.min_step_interval / 10);
        loop {
            interval.tick().await;
            match ticker_tx.send(Cmd::Tick) {
                Ok(_) => {}
                Err(_) => {
                    // Runtime has shut down, we can stop ticking.
                    return;
                }
            }
        }
    });

    // Construct the client.
    let handles = Mutex::new(Some(RuntimeHandles {
        impl_handle,
        ticker_handle,
        ticker_async_runtime: async_runtime,
    }));
    let core = RuntimeCore {
        handles,
        tx,
        metrics,
    };
    let client = RuntimeClient::new(id, Arc::new(core));

    Ok(client)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RuntimeId(u64);

impl RuntimeId {
    pub fn new() -> Self {
        let mut h = DefaultHasher::new();
        Instant::now().hash(&mut h);
        RuntimeId(h.finish())
    }
}

#[derive(Debug)]
struct RuntimeHandles {
    impl_handle: JoinHandle<()>,
    ticker_handle: tokio::task::JoinHandle<()>,
    ticker_async_runtime: Arc<AsyncRuntime>,
}

#[derive(Debug)]
pub(crate) struct RuntimeCore {
    handles: Mutex<Option<RuntimeHandles>>,
    tx: crossbeam_channel::Sender<Cmd>,
    metrics: Arc<Metrics>,
}

impl RuntimeCore {
    pub(crate) fn send(&self, cmd: Cmd) {
        self.metrics.cmd_queue_in.inc();
        if let Err(crossbeam_channel::SendError(cmd)) = self.tx.send(cmd) {
            // According to the docs, a SendError can only happen if the
            // receiver has hung up, which in this case only happens if the
            // thread has exited. The thread only exits if we send it a
            // Cmd::Stop (or it panics).
            match cmd {
                Cmd::Stop(res) => {
                    // Already stopped: no-op.
                    res.fill(Ok(()))
                }
                Cmd::Register(_, _, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::Destroy(_, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::Write(_, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::Seal(_, _, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::GetDescription(_, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::AllowCompaction(_, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::Snapshot(_, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::Listen(_, _, res) => res.fill(Err(Error::RuntimeShutdown)),
                Cmd::Tick => {}
            }
        }
    }

    pub(crate) fn stop(&self) -> Result<(), Error> {
        if let Some(handles) = self.handles.lock()?.take() {
            let (tx, rx) = PFuture::new();
            self.send(Cmd::Stop(tx));
            // NB: Make sure there are no early returns before this `join`,
            // otherwise the runtime thread might still be cleaning up when this
            // returns (flushing out final writes, cleaning up LOCK files, etc).
            //
            // TODO: Regression test for this.
            if let Err(_) = handles.impl_handle.join() {
                // If the thread panic'd, then by definition it has been
                // stopped, so we can return an Ok. This is surprising, though,
                // so log a message. Unfortunately, there isn't really a way to
                // put the panic message in this log.
                tracing::error!("persist runtime thread panic'd");
            }
            if let Err(err) = block_on(handles.ticker_handle) {
                tracing::error!("persist ticker thread error'd: {:?}", err);
            }
            // Thread a copy of the Arc<AsyncRuntime> being used to drive
            // ticker_handle to make sure the runtime doesn't shut down before
            // ticker_handle has a chance to finish cleanly.
            drop(handles.ticker_async_runtime);
            rx.recv()
        } else {
            Ok(())
        }
    }
}

impl Drop for RuntimeCore {
    fn drop(&mut self) {
        if let Err(err) = self.stop() {
            tracing::error!("error while stopping dropped persist runtime: {}", err);
        }
    }
}

struct RuntimeImpl<L: Log, B: Blob> {
    indexed: Indexed<L, B>,
    rx: crossbeam_channel::Receiver<Cmd>,
    metrics: Arc<Metrics>,
    prev_step: Instant,
    min_step_interval: Duration,
}

/// Configuration for [start]ing a [RuntimeClient].
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Minimum step interval to use
    min_step_interval: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            min_step_interval: Self::DEFAULT_MIN_STEP_INTERVAL,
        }
    }
}

impl RuntimeConfig {
    const DEFAULT_MIN_STEP_INTERVAL: Duration = Duration::from_millis(1000);

    /// An alternate configuration that minimizes latency at the cost of
    /// increased storage traffic.
    pub(crate) fn for_tests() -> Self {
        RuntimeConfig {
            min_step_interval: Duration::from_millis(1),
        }
    }

    /// A configuration with a configurable min_step_interval
    pub fn with_min_step_interval(min_step_interval: Duration) -> Self {
        RuntimeConfig { min_step_interval }
    }
}

impl<L: Log, B: Blob> RuntimeImpl<L, B> {
    fn new(
        config: RuntimeConfig,
        indexed: Indexed<L, B>,
        rx: crossbeam_channel::Receiver<Cmd>,
        metrics: Arc<Metrics>,
    ) -> Self {
        RuntimeImpl {
            indexed,
            rx,
            metrics,
            // Initialize this so it's ready to trigger immediately.
            prev_step: Instant::now() - config.min_step_interval,
            min_step_interval: config.min_step_interval,
        }
    }

    /// Synchronously waits for the next command, executes it, and responds.
    ///
    /// Returns false to indicate a graceful shutdown, true otherwise.
    fn work(&mut self) -> bool {
        let mut cmds = vec![];
        match self.rx.recv() {
            Ok(cmd) => cmds.push(cmd),
            Err(crossbeam_channel::RecvError) => {
                // All Runtime handles hung up. Drop should have shut things down
                // nicely, so this is unexpected.
                return false;
            }
        };

        let mut more_work = true;

        // Grab as many commands as we can out of the channel and execute them
        // all before calling `step` again to amortise the cost of trace
        // maintenance between them.
        loop {
            match self.rx.try_recv() {
                Ok(cmd) => cmds.push(cmd),
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    break;
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    // All Runtime handles hung up. Drop should have shut things down
                    // nicely, so this is unexpected.
                    more_work = false;
                }
            }
        }

        let run_start = Instant::now();
        for cmd in cmds {
            match cmd {
                Cmd::Stop(res) => {
                    // Finish up any pending work that we can before closing.
                    if let Err(e) = self.indexed.step() {
                        self.metrics.cmd_step_error_count.inc();
                        tracing::warn!("error running step: {:?}", e);
                    }
                    res.fill(self.indexed.close());
                    return false;
                }
                Cmd::Register(id, (key_codec_name, val_codec_name), res) => {
                    self.indexed
                        .register(&id, &key_codec_name, &val_codec_name, res);
                }
                Cmd::Destroy(id, res) => {
                    self.indexed.destroy(&id, res);
                }
                Cmd::Write(updates, res) => {
                    self.metrics.cmd_write_count.inc();
                    self.indexed.write(updates, res);
                }
                Cmd::Seal(ids, ts, res) => {
                    self.indexed.seal(ids, ts, res);
                }
                Cmd::GetDescription(id_str, res) => {
                    self.indexed.get_description(&id_str, res);
                }
                Cmd::AllowCompaction(id_sinces, res) => {
                    self.indexed.allow_compaction(id_sinces, res);
                }
                Cmd::Snapshot(id, res) => {
                    self.indexed.snapshot(id, res);
                }
                Cmd::Listen(id, sender, res) => {
                    self.indexed.listen(id, sender, res);
                }
                Cmd::Tick => {
                    // This is a no-op. It's only here to give us the
                    // opportunity to hit the step logic below on some minimum
                    // interval, even when no other commands are coming in.
                }
            }
            self.metrics.cmd_run_count.inc()
        }
        let step_start = Instant::now();
        self.metrics
            .cmd_run_seconds
            .inc_by(step_start.duration_since(run_start).as_secs_f64());

        // HACK: This rate limits how much we call step in response to workloads
        // consisting entirely of write, seal, and allow_compaction (which is
        // exactly what we expect in the common/steady state). At the moment,
        // step is too aggressive about compaction if called in a tight loop,
        // causing excess disk usage. This is exactly what happens if coord is
        // getting steady read traffic over indexes derived directly or
        // indirectly from tables (it continually seals).
        //
        // The downside to this rate limiting is that it introduces artificial
        // latency into filling the response futures we returned to the client
        // as well as to updating listeners. The former blocks literally no
        // critical paths in the initial persisted system tables test, which is
        // considered an acceptable tradeoff for the short-term. In the
        // long-term, we'll do something better (probably by using Buffer).
        //
        // For context, in the persistent system tables test, we get writes
        // every ~30s (how often we write to mz_metrics and
        // mz_metric_histograms). In a non-loaded system as well as anything
        // that's only selecting from views purely derived from sources, we also
        // get seals every ~30s. In a system that's selecting from mz_metrics in
        // a loop, the selects all take ~1s and we get seals every ~1s. In a
        // system that's selecting from a user table in a tight loop, the
        // selects take ~10ms (the same amount of time they would with
        // persistence disabled) and we get seals every ~10ms.
        //
        // TODO: It would almost certainly be better to separate the rate limit
        // of drain_pending vs the rest of step. The other parts (drain_unsealed
        // and compact) can and should be called even less frequently than this.
        // However, this change is going into a release at the last minute and
        // I'm less confidant about unknown ramifications of a two interval
        // strategy.
        let need_step = step_start.duration_since(self.prev_step) > self.min_step_interval;

        // BONUS HACK: If pending_responses is empty, then step would be a no-op
        // from a user's perspective. Unfortunately, step would still write out
        // meta (three times, in fact). So, we manually skip over it here as an
        // optimization. In a system that's not selecting from tables, this
        // reduces the frequency of step calls from every ~1s
        // (DEFAULT_MIN_STEP_INTERVAL) to ~30s (the frequency of writing to
        // mz_metrics). Otherwise, it has no effect.
        //
        // TODO: Make step smarter and remove this hack.
        let need_step = need_step && self.indexed.has_pending_responses();

        if need_step {
            self.prev_step = step_start;
            if let Err(e) = self.indexed.step() {
                self.metrics.cmd_step_error_count.inc();
                // TODO: revisit whether we need to move this to a different log level
                // depending on how spammy it ends up being. Alternatively, we
                // may want to rate-limit our logging here.
                tracing::warn!("error running step: {:?}", e);
            }

            self.metrics
                .cmd_step_seconds
                .inc_by(step_start.elapsed().as_secs_f64());
        }

        return more_work;
    }
}

#[cfg(test)]
mod tests {
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, OkErr, Probe};
    use timely::dataflow::ProbeHandle;

    use crate::client::MultiWriteHandle;
    use crate::indexed::SnapshotExt;
    use crate::mem::{MemMultiRegistry, MemRegistry};
    use crate::operators::source::PersistedSource;
    use crate::operators::split_ok_err;

    use super::*;

    #[test]
    fn runtime() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), "val1".to_string()), 1, 1),
            (("key2".to_string(), "val2".to_string()), 1, 1),
        ];

        let mut runtime = MemRegistry::new().runtime_no_reentrance()?;

        let (write, meta) = runtime.create_or_load("0");
        write.write(&data).recv()?;
        assert_eq!(meta.snapshot()?.read_to_end()?, data);

        // Commands sent after stop return an error, but calling stop again is
        // fine.
        runtime.stop()?;
        assert!(runtime.create_or_load::<(), ()>("0").0.stream_id().is_err());
        runtime.stop()?;

        Ok(())
    }

    #[test]
    fn concurrent() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), "val1".to_string()), 1, 1),
            (("key2".to_string(), "val2".to_string()), 1, 1),
        ];

        let client1 = MemRegistry::new().runtime_no_reentrance()?;
        let _ = client1.create_or_load::<String, String>("0");

        // Everything is still running after client1 is dropped.
        let mut client2 = client1.clone();
        drop(client1);
        let (write, meta) = client2.create_or_load("0");
        write.write(&data).recv()?;
        assert_eq!(meta.snapshot()?.read_to_end()?, data);
        client2.stop()?;

        Ok(())
    }

    #[test]
    fn restart() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), "val1".to_string()), 1, 1),
            (("key2".to_string(), "val2".to_string()), 1, 1),
        ];

        let mut registry = MemRegistry::new();

        // Shutdown happens if we explicitly call stop, unlocking the log and
        // blob and allowing them to be reused in the next Indexed.
        let mut persister = registry.runtime_no_reentrance()?;
        let (write, _) = persister.create_or_load("0");
        write.write(&data[0..1]).recv()?;
        assert_eq!(persister.stop(), Ok(()));

        // Shutdown happens if all handles are dropped, even if we don't call
        // stop.
        let persister = registry.runtime_no_reentrance()?;
        let (write, _) = persister.create_or_load("0");
        write.write(&data[1..2]).recv()?;
        drop(write);
        drop(persister);

        // We can read back what we previously wrote.
        {
            let persister = registry.runtime_no_reentrance()?;
            let (_, meta) = persister.create_or_load("0");
            assert_eq!(meta.snapshot()?.read_to_end()?, data);
        }

        Ok(())
    }

    #[test]
    fn seal_get_description_roundtrip() -> Result<(), Error> {
        let id = "test";

        let mut registry = MemRegistry::new();
        let persister = registry.runtime_no_reentrance()?;
        let (write, _) = persister.create_or_load::<(), ()>(id);

        // Initial seal frontier should be `0`.
        let upper = persister.get_description(id)?.upper().clone();
        assert_eq!(upper, Antichain::from_elem(0));

        write.seal(42);

        let upper = persister.get_description(id)?.upper().clone();
        assert_eq!(upper, Antichain::from_elem(42));

        Ok(())
    }

    #[test]
    fn allow_compaction_get_description_roundtrip() -> Result<(), Error> {
        let id = "test";

        let mut registry = MemRegistry::new();
        let persister = registry.runtime_no_reentrance()?;
        let (write, _) = persister.create_or_load::<(), ()>(id);

        // Initial compaction/since frontier should be `0`.
        let since = persister.get_description(id)?.since().clone();
        assert_eq!(since, Antichain::from_elem(0));

        write.seal(100).recv()?;
        write.allow_compaction(Antichain::from_elem(42)).recv()?;

        let since = persister.get_description(id)?.since().clone();
        assert_eq!(since, Antichain::from_elem(42));

        Ok(())
    }

    #[test]
    fn get_description_lower() -> Result<(), Error> {
        let id = "test";

        let mut registry = MemRegistry::new();
        let persister = registry.runtime_no_reentrance()?;
        let (_write, _) = persister.create_or_load::<(), ()>(id);

        // `lower` will always be `Antichain::from_elem(Timestamp::minimum())`.
        let since = persister.get_description(id)?.lower().clone();
        assert_eq!(since, Antichain::from_elem(0));

        Ok(())
    }

    #[test]
    fn multi_write_handle() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), ()), 1, 1),
            (("key2".to_string(), ()), 1, 1),
        ];

        let mut registry = MemMultiRegistry::new();
        let client1 = registry.open("1", "multi")?;
        let client2 = registry.open("2", "multi")?;

        let (c1s1, c1s1_read) = client1.create_or_load("1");
        let (c1s2, c1s2_read) = client1.create_or_load("2");
        let (c2s1, _) = client2.create_or_load("1");

        // Cannot construct with no streams.
        assert!(MultiWriteHandle::<(), ()>::new(&[]).is_err());

        // Cannot construct with streams from different runtimes.
        assert!(MultiWriteHandle::new(&[&c1s2, &c2s1]).is_err());

        // Normal write
        let multi = MultiWriteHandle::new(&[&c1s1, &c1s2])?;
        let updates = vec![
            (c1s1.stream_id()?, data[..1].to_vec()),
            (c1s2.stream_id()?, data[1..].to_vec()),
        ];
        multi.write_atomic(updates).recv()?;
        assert_eq!(c1s1_read.snapshot()?.read_to_end()?, data[..1].to_vec());
        assert_eq!(c1s2_read.snapshot()?.read_to_end()?, data[1..].to_vec());

        // Normal seal
        let ids = &[c1s1.stream_id()?, c1s2.stream_id()?];
        multi.seal(ids, 2).recv()?;
        // We don't expose reading the seal directly, so hack it a bit here by
        // verifying that we can't re-seal at a prior timestamp (which is
        // disallowed).
        assert_eq!(c1s1.seal(1).recv().map_err(|err| err.to_string()), Err("invalid seal for Id(0): 1 not at or in advance of current seal frontier Antichain { elements: [2] }".into()));
        assert_eq!(c1s2.seal(1).recv().map_err(|err| err.to_string()), Err("invalid seal for Id(1): 1 not at or in advance of current seal frontier Antichain { elements: [2] }".into()));

        // Cannot write to streams not specified during construction.
        let (c1s3, _) = client1.create_or_load::<(), ()>("3");
        assert!(multi
            .write_atomic(vec![(c1s3.stream_id()?, data)])
            .recv()
            .is_err());

        // Cannot seal streams not specified during construction.
        assert!(multi.seal(&[c1s3.stream_id()?], 3).recv().is_err());

        Ok(())
    }

    #[test]
    fn codec_mismatch() -> Result<(), Error> {
        let client = MemRegistry::new().runtime_no_reentrance()?;

        let _ = client.create_or_load::<(), String>("stream");

        // Normal case: registration uses same key and value codec.
        let _ = client.create_or_load::<(), String>("stream");

        // Different key codec
        assert_eq!(
            client
                .create_or_load::<Vec<u8>, String>("stream")
                .0
                .stream_id(),
            Err(Error::from(
                "invalid registration: key codec mismatch Vec<u8> vs previous ()"
            ))
        );

        // Different val codec
        assert_eq!(
            client.create_or_load::<(), Vec<u8>>("stream").0.stream_id(),
            Err(Error::from(
                "invalid registration: val codec mismatch Vec<u8> vs previous String"
            ))
        );

        Ok(())
    }

    /// Previously, the persisted source would first register a listener, and
    /// then read a snapshot in two separate commands. This approach had the
    /// problem that there could be some data duplication between the snapshot
    /// and the listener. We attempted to solve that problem by filtering out all
    /// records <= the snapshot's sealed frontier from the listener, and allowed the
    /// listener to send us records > the snapshot's sealed frontier.
    ///
    /// Unfortunately, we also allowed the snapshot to send us records > the
    /// snapshot's sealed frontier so this didn't fix the data duplication issue.
    /// #8606 manifested as an issue with the persistent system tables test where
    /// some records had negative multiplicity. Basically, something like the
    /// following sequence of events happened:
    ///
    /// 1. Register a listener
    /// 2. Insert (foo, t1, +1)
    /// 3. Insert (foo, t100, -1)
    /// 4. Seal t2
    /// 5. Take a snapshot - which has sealed frontier t2
    /// 6. Start reading from the listener and snapshot.
    ///
    /// Now when we did step 6 - we received from the snapshot:
    ///
    /// (foo, t1, +1)
    /// (foo, t100, -1)
    ///
    /// because we didn't filter anything from the snapshot.
    ///
    /// From the listener, we received:
    ///
    /// (foo, t100, -1) because we filtered out all records at times < t2
    ///
    /// at t100, we now have a negative multiplicity.
    ///
    /// This test attempts to replicate that scenario by interleaving writes
    /// and seals with persisted source creation to catch any regressions where
    /// taking a snapshot and registering a listener are not properly atomic.
    #[test]
    fn regression_8606_snapshot_listener_atomicity() -> Result<(), Error> {
        let data = vec![(("foo".into(), ()), 1, 1), (("foo".into(), ()), 1000, -1)];

        let mut p = MemRegistry::new().runtime_no_reentrance()?;
        let (write, read) = p.create_or_load::<String, ()>("1");

        let ok = timely::execute_directly(move |worker| {
            let writes = std::thread::spawn(move || {
                write.write(&data).recv().expect("write was successful");
                write.seal(2).recv().expect("seal was successful");
            });

            let mut probe = ProbeHandle::new();
            let ok_stream = worker.dataflow(|scope| {
                let (ok_stream, _err_stream) = scope
                    .persisted_source(read, &Antichain::from_elem(0))
                    .ok_err(split_ok_err);
                ok_stream.probe_with(&mut probe).capture()
            });

            writes.join().expect("write thread succeeds");

            while probe.less_than(&2) {
                worker.step();
            }
            p.stop().expect("stop was successful");

            ok_stream
        });

        let diff_sum: isize = ok
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter().map(|(_, _, diff)| diff))
            .sum();
        assert_eq!(diff_sum, 0);

        Ok(())
    }
}
