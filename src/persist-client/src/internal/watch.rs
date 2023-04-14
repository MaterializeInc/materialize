// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Notifications for state changes.

use std::sync::Arc;

use mz_persist::location::SeqNo;
use tokio::sync::broadcast;
use tracing::debug;

use crate::cache::LockingTypedState;
use crate::internal::metrics::Metrics;

#[derive(Debug)]
pub struct StateWatchNotifier {
    tx: broadcast::Sender<SeqNo>,
}

impl Default for StateWatchNotifier {
    fn default() -> Self {
        let (tx, _rx) = broadcast::channel(1);
        StateWatchNotifier { tx }
    }
}

impl StateWatchNotifier {
    /// Wake up any watchers of this state.
    ///
    /// This must be called while under the same lock that modified the state to
    /// avoid any potential for out of order SeqNos in the broadcast channel.
    ///
    /// This restriction can be lifted (i.e. we could notify after releasing the
    /// write lock), but we'd have to reason about out of order SeqNos in the
    /// broadcast channel. In particular, if we see `RecvError::Lagged` then
    /// it's possible we lost X+1 and got X, so if X isn't sufficient to return,
    /// we'd need to grab the read lock and verify the real SeqNo.
    pub(crate) fn notify(&self, seqno: SeqNo) {
        match self.tx.send(seqno) {
            // Someone got woken up.
            Ok(_) => {}
            // No one is listening, that's also fine.
            Err(_) => {}
        }
    }
}

/// A reactive subscription to changes in [LockingTypedState].
///
/// Invariants:
/// - The `state.seqno` only advances (never regresses). This is guaranteed by
///   LockingTypedState.
/// - `seqno_high_water` is always <= `state.seqno`.
/// - If `seqno_high_water` is < `state.seqno`, then we'll get a notification on
///   `rx`. This is maintained by notifying new seqnos under the same lock which
///   adds them.
/// - `seqno_high_water` always holds the highest value received in the channel
///   This is maintained by `wait_for_seqno_gt` taking an exclusive reference to
///   self.
#[derive(Debug)]
pub struct StateWatch<K, V, T, D> {
    state: Arc<LockingTypedState<K, V, T, D>>,
    seqno_high_water: SeqNo,
    rx: broadcast::Receiver<SeqNo>,
}

impl<K, V, T, D> StateWatch<K, V, T, D> {
    pub(crate) fn new(state: Arc<LockingTypedState<K, V, T, D>>, metrics: &Metrics) -> Self {
        // Important! We have to subscribe to the broadcast channel _before_ we
        // grab the current seqno. Otherwise, we could race with a write to
        // state and miss a notification. Tokio guarantees that "the returned
        // Receiver will receive values sent after the call to subscribe", and
        // the read_lock linearizes the subscribe to be _before_ whatever
        // seqno_high_water we get here.
        let rx = state.notifier().tx.subscribe();
        let seqno_high_water = state.read_lock(&metrics.locks.watch, |x| x.seqno);
        StateWatch {
            state,
            seqno_high_water,
            rx,
        }
    }

    /// Blocks until the State has a SeqNo >= the requested one.
    ///
    /// This method is cancel-safe.
    pub async fn wait_for_seqno_ge(&mut self, requested: SeqNo) {
        debug!("wait_for_seqno_ge {} {}", self.state.shard_id(), requested);
        loop {
            if self.seqno_high_water >= requested {
                break;
            }
            match self.rx.recv().await {
                Ok(x) => {
                    assert!(x >= self.seqno_high_water);
                    self.seqno_high_water = x;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    unreachable!("we're holding on to a reference to the sender")
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // This is just a hint that our buffer (of size 1) filled
                    // up, which is totally fine. The broadcast channel
                    // guarantees that the most recent N (again, =1 here) are
                    // kept, so just loop around. This branch means we should be
                    // able to read a new value immediately.
                    continue;
                }
            }
        }
        debug!(
            "wait_for_seqno_ge {} {} returning",
            self.state.shard_id(),
            requested
        );
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Context;
    use std::time::Duration;

    use futures::FutureExt;
    use futures_task::noop_waker;
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::cast::CastFrom;
    use mz_ore::metrics::MetricsRegistry;
    use timely::progress::Antichain;

    use crate::cache::StateCache;
    use crate::cfg::{PersistConfig, PersistParameters, RetryParameters};
    use crate::internal::state::TypedState;
    use crate::tests::new_test_client;
    use crate::ShardId;

    use super::*;

    #[tokio::test]
    async fn state_watch() {
        mz_ore::test::init_logging();
        let metrics = Metrics::new(&PersistConfig::new_for_tests(), &MetricsRegistry::new());
        let cache = StateCache::default();
        let shard_id = ShardId::new();
        let state = cache
            .get::<(), (), u64, i64, _, _>(shard_id, || async {
                Ok(TypedState::new(
                    DUMMY_BUILD_INFO.semver_version(),
                    shard_id,
                    "host".to_owned(),
                    0u64,
                ))
            })
            .await
            .unwrap();
        assert_eq!(state.read_lock(&metrics.locks.watch, |x| x.seqno), SeqNo(0));

        // A watch for 0 resolves immediately.
        let mut w0 = StateWatch::new(Arc::clone(&state), &metrics);
        let () = w0.wait_for_seqno_ge(SeqNo(0)).await;

        // A watch for 1 does not yet resolve.
        let w0s1 = w0.wait_for_seqno_ge(SeqNo(1)).shared();
        assert_eq!(w0s1.clone().now_or_never(), None);

        // After mutating state, the watch for 1 does resolve.
        state.write_lock(&metrics.locks.applier_write, |state| {
            state.seqno = state.seqno.next()
        });
        let () = w0s1.await;

        // A watch for an old seqno immediately resolves.
        let () = w0.wait_for_seqno_ge(SeqNo(0)).await;

        // We can create a new watch and it also behaves.
        let mut w1 = StateWatch::new(Arc::clone(&state), &metrics);
        let () = w1.wait_for_seqno_ge(SeqNo(0)).await;
        let () = w1.wait_for_seqno_ge(SeqNo(1)).await;
        assert_eq!(w1.wait_for_seqno_ge(SeqNo(2)).now_or_never(), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn state_watch_concurrency() {
        mz_ore::test::init_logging();
        let metrics = Arc::new(Metrics::new(
            &PersistConfig::new_for_tests(),
            &MetricsRegistry::new(),
        ));
        let cache = StateCache::default();
        let shard_id = ShardId::new();
        let state = cache
            .get::<(), (), u64, i64, _, _>(shard_id, || async {
                Ok(TypedState::new(
                    DUMMY_BUILD_INFO.semver_version(),
                    shard_id,
                    "host".to_owned(),
                    0u64,
                ))
            })
            .await
            .unwrap();
        assert_eq!(state.read_lock(&metrics.locks.watch, |x| x.seqno), SeqNo(0));

        const NUM_WATCHES: usize = 100;
        const NUM_WRITES: usize = 20;

        let watches = (0..NUM_WATCHES)
            .map(|idx| {
                let state = Arc::clone(&state);
                let metrics = Arc::clone(&metrics);
                mz_ore::task::spawn(|| "watch", async move {
                    let mut watch = StateWatch::new(Arc::clone(&state), &metrics);
                    // We stared at 0, so N writes means N+1 seqnos.
                    let wait_seqno = SeqNo(u64::cast_from(idx % NUM_WRITES + 1));
                    let () = watch.wait_for_seqno_ge(wait_seqno).await;
                    let observed_seqno =
                        state.read_lock(&metrics.locks.applier_read_noncacheable, |x| x.seqno);
                    tracing::info!("{} vs {}", wait_seqno, observed_seqno);
                    assert!(
                        wait_seqno <= observed_seqno,
                        "{} vs {}",
                        wait_seqno,
                        observed_seqno
                    );
                })
            })
            .collect::<Vec<_>>();
        let writes = (0..NUM_WRITES)
            .map(|_| {
                let state = Arc::clone(&state);
                let metrics = Arc::clone(&metrics);
                mz_ore::task::spawn(|| "write", async move {
                    state.write_lock(&metrics.locks.applier_write, |x| {
                        x.seqno = x.seqno.next();
                        eprintln!("wrote {}", x.seqno);
                    });
                })
            })
            .collect::<Vec<_>>();
        for watch in watches {
            assert!(watch.await.is_ok());
        }
        for write in writes {
            assert!(write.await.is_ok());
        }
    }

    #[tokio::test]
    async fn state_watch_listen_snapshot() {
        mz_ore::test::init_logging();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let client = new_test_client().await;
        // Override the listen poll so that it's useless.
        PersistParameters {
            next_listen_batch_retryer: Some(RetryParameters {
                initial_backoff: Duration::from_secs(1_000_000),
                multiplier: 1,
                clamp: Duration::from_secs(1_000_000),
            }),
            ..PersistParameters::default()
        }
        .apply(&client.cfg);

        let (mut write, mut read) = client.expect_open::<(), (), u64, i64>(ShardId::new()).await;

        // Grab a snapshot for 1, which doesn't resolve yet. Also grab a listen
        // for 0, which resolves but doesn't yet resolve the next batch.
        let mut listen = read
            .clone("test")
            .await
            .listen(Antichain::from_elem(0))
            .await
            .unwrap();
        let mut snapshot = Box::pin(read.snapshot(Antichain::from_elem(0)));
        assert!(Pin::new(&mut snapshot).poll(&mut cx).is_pending());
        let mut listen_next_batch = Box::pin(listen.next());
        assert!(Pin::new(&mut listen_next_batch).poll(&mut cx).is_pending());

        // Now update the frontier, which should allow the snapshot to resolve
        // and the listen to resolve its next batch. Because we disabled the
        // polling, the listen_next_batch future will block forever and timeout
        // the test if the watch doesn't work.
        write.expect_compare_and_append(&[], 0, 1).await;
        let _ = listen_next_batch.await;

        // For good measure, also resolve the snapshot, though we haven't broken
        // the polling on this.
        let _ = snapshot.await;
    }
}
