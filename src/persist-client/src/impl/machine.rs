// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of the persist state machine.

use std::convert::Infallible;
use std::fmt::Debug;
use std::ops::{ControlFlow, ControlFlow::Break, ControlFlow::Continue};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::location::{Consensus, ExternalError, Indeterminate, SeqNo, VersionedData};
use mz_persist::retry::Retry;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use tracing::{debug, info, trace};

use crate::error::InvalidUsage;
use crate::r#impl::state::{ReadCapability, Since, State, StateCollections, Upper};
use crate::read::ReaderId;
use crate::ShardId;

#[derive(Debug)]
pub struct Machine<K, V, T, D> {
    consensus: Arc<dyn Consensus + Send + Sync>,

    state: State<K, V, T, D>,
}

// Impl Clone regardless of the type params.
impl<K, V, T: Clone, D> Clone for Machine<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            consensus: Arc::clone(&self.consensus),
            state: self.state.clone(),
        }
    }
}

impl<K, V, T, D> Machine<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    pub async fn new(
        shard_id: ShardId,
        consensus: Arc<dyn Consensus + Send + Sync>,
    ) -> Result<Self, InvalidUsage<T>> {
        let state = Self::maybe_init_state(consensus.as_ref(), shard_id).await?;
        Ok(Machine { consensus, state })
    }

    pub fn shard_id(&self) -> ShardId {
        self.state.shard_id()
    }

    pub async fn fetch_upper(&mut self) -> Antichain<T> {
        self.fetch_and_update_state().await;
        self.state.upper()
    }

    pub fn upper(&self) -> Antichain<T> {
        self.state.upper()
    }

    pub async fn register(&mut self, reader_id: &ReaderId) -> (Upper<T>, ReadCapability<T>) {
        let (seqno, (shard_upper, read_cap)) = self
            .apply_unbatched_idempotent_cmd(|seqno, state| state.register(seqno, reader_id))
            .await;
        debug_assert_eq!(seqno, read_cap.seqno);
        (shard_upper, read_cap)
    }

    pub async fn clone_reader(&mut self, new_reader_id: &ReaderId) -> ReadCapability<T> {
        let (seqno, read_cap) = self
            .apply_unbatched_idempotent_cmd(|seqno, state| state.clone_reader(seqno, new_reader_id))
            .await;
        debug_assert_eq!(seqno, read_cap.seqno);
        read_cap
    }

    pub async fn compare_and_append(
        &mut self,
        keys: &[String],
        desc: &Description<T>,
    ) -> Result<Result<Result<SeqNo, Upper<T>>, InvalidUsage<T>>, Indeterminate> {
        let (seqno, res) = self
            .apply_unbatched_cmd(|_, state| state.compare_and_append(keys, desc))
            .await?;
        match res {
            Ok(()) => Ok(Ok(Ok(seqno))),
            Err(Ok(err)) => return Ok(Ok(Err(err))),
            Err(Err(current_upper)) => return Ok(Err(current_upper)),
        }
    }

    pub async fn downgrade_since(
        &mut self,
        reader_id: &ReaderId,
        new_since: &Antichain<T>,
    ) -> (SeqNo, Since<T>) {
        self.apply_unbatched_idempotent_cmd(|_, state| state.downgrade_since(reader_id, new_since))
            .await
    }

    pub async fn expire_reader(&mut self, reader_id: &ReaderId) -> SeqNo {
        let (seqno, _existed) = self
            .apply_unbatched_idempotent_cmd(|_, state| state.expire_reader(reader_id))
            .await;
        seqno
    }

    pub async fn snapshot(
        &mut self,
        as_of: &Antichain<T>,
    ) -> Result<Vec<(String, Description<T>)>, Since<T>> {
        let mut fetches = 0;
        let mut retry = Retry::persist_defaults(SystemTime::now()).into_retry_stream();
        loop {
            let upper = match self.state.snapshot(as_of) {
                Ok(Ok(x)) => return Ok(x),
                Ok(Err(Upper(upper))) => {
                    // The upper isn't ready yet, fall through and try again.
                    upper
                }
                Err(Since(since)) => return Err(Since(since)),
            };
            // Only sleep after the first fetch, because the first time through
            // maybe our state was just out of date.
            if fetches > 0 {
                info!(
                    "snapshot as of {:?} not available for upper {:?} retrying in {:?}",
                    as_of,
                    upper,
                    retry.next_sleep()
                );
                retry = retry.sleep().await;
            }
            self.fetch_and_update_state().await;
            fetches += 1;
        }
    }

    pub async fn next_listen_batch(
        &mut self,
        frontier: &Antichain<T>,
    ) -> (Vec<String>, Description<T>) {
        // This unconditionally fetches the latest state and uses that to
        // determine if we can serve `as_of`. TODO: We could instead check first
        // and only fetch if necessary.
        let mut retry = Retry::persist_defaults(SystemTime::now()).into_retry_stream();
        loop {
            self.fetch_and_update_state().await;
            if let Some((keys, desc)) = self.state.next_listen_batch(frontier) {
                return (keys.to_owned(), desc.clone());
            }
            // Wait a bit and try again.
            //
            // TODO: See if we can watch for changes in Consensus to be more
            // reactive here.
            debug!(
                "next_listen_batch didn't find new data, retrying in {:?}",
                retry.next_sleep()
            );
            retry = retry.sleep().await;
        }
    }

    async fn apply_unbatched_idempotent_cmd<
        R,
        WorkFn: FnMut(SeqNo, &mut StateCollections<T>) -> ControlFlow<Infallible, R>,
    >(
        &mut self,
        mut work_fn: WorkFn,
    ) -> (SeqNo, R) {
        let mut retry = Retry::persist_defaults(SystemTime::now()).into_retry_stream();
        loop {
            match self.apply_unbatched_cmd(&mut work_fn).await {
                Ok((seqno, x)) => match x {
                    Ok(x) => return (seqno, x),
                    Err(infallible) => match infallible {},
                },
                Err(err) => {
                    debug!(
                        "apply_unbatched_idempotent_cmd received an indeterminate error, retrying in {:?}: {}", retry.next_sleep(), err);
                    retry = retry.sleep().await;
                    continue;
                }
            }
        }
    }

    async fn apply_unbatched_cmd<
        R,
        E,
        WorkFn: FnMut(SeqNo, &mut StateCollections<T>) -> ControlFlow<E, R>,
    >(
        &mut self,
        mut work_fn: WorkFn,
    ) -> Result<(SeqNo, Result<R, E>), Indeterminate> {
        let path = self.shard_id().to_string();

        loop {
            let (work_ret, new_state) = match self.state.clone_apply(&mut work_fn) {
                Continue(x) => x,
                Break(err) => return Ok((self.state.seqno(), Err(err))),
            };
            trace!(
                "apply_unbatched_cmd attempting {}\n  new_state={:?}",
                self.state.seqno(),
                new_state
            );

            let new = VersionedData::from((new_state.seqno(), &new_state));
            // SUBTLE! Unlike the other consensus and blob uses, we can't
            // automatically retry indeterminate ExternalErrors here. However,
            // if the state change itself is _idempotent_, then we're free to
            // retry even indeterminate errors. See
            // [Self::apply_unbatched_idempotent_cmd].
            let cas_res = retry_determinate("apply_unbatched_cmd::cas", || async {
                // If Consensus::compare_and_set took new as a ref, then we
                // wouldn't have to clone here.
                self.consensus
                    .compare_and_set(
                        Instant::now() + FOREVER,
                        &path,
                        Some(self.state.seqno()),
                        new.clone(),
                    )
                    .await
            })
            .await
            .map_err(|err| {
                debug!("apply_unbatched_cmd errored: {}", err);
                err
            })?;
            match cas_res {
                Ok(()) => {
                    trace!(
                        "apply_unbatched_cmd succeeded {}\n  new_state={:?}",
                        new_state.seqno(),
                        new_state
                    );
                    self.state = new_state;

                    // Bound the number of entries in consensus.
                    let () = retry_external("apply_unbatched_cmd::truncate", || async {
                        self.consensus
                            .truncate(Instant::now() + FOREVER, &path, self.state.seqno())
                            .await
                    })
                    .await;

                    return Ok((self.state.seqno(), Ok(work_ret)));
                }
                Err(current) => {
                    debug!(
                        "apply_unbatched_cmd lost the CaS race, retrying: {} vs {:?}",
                        self.state.seqno(),
                        current.as_ref().map(|x| x.seqno)
                    );
                    self.update_state(current).await;

                    // TODO: Some sort of exponential backoff here? We have
                    // Retry for this but using it here seems to hang our unit
                    // tests. We should look into that, but also maybe it
                    // doesn't make sense here anyway because it would just make
                    // starvation worse.
                    continue;
                }
            }
        }
    }

    // TODO: This is fairly duplicative of apply_unbatched_cmd. Unclear if
    // there's anything to do here...
    async fn maybe_init_state(
        consensus: &(dyn Consensus + Send + Sync),
        shard_id: ShardId,
    ) -> Result<State<K, V, T, D>, InvalidUsage<T>> {
        debug!("Machine::maybe_init_state shard_id={}", shard_id);

        let path = shard_id.to_string();
        let mut current = retry_external("maybe_init_state::head", || async {
            consensus.head(Instant::now() + FOREVER, &path).await
        })
        .await;

        loop {
            // First, check if the shard has already been initialized.
            if let Some(current) = current.as_ref() {
                let current_state = match State::decode(&current.data) {
                    Ok(x) => x,
                    Err(err) => return Err(err),
                };
                debug_assert_eq!(current.seqno, current_state.seqno());
                return Ok(current_state);
            }

            // It hasn't been initialized, try initializing it.
            let state = State::new(shard_id);
            let new = VersionedData::from((state.seqno(), &state));
            trace!(
                "maybe_init_state attempting {}\n  state={:?}",
                new.seqno,
                state
            );
            let cas_res = retry_external("maybe_init_state::cas", || async {
                // If Consensus::compare_and_set took new as a ref, then we
                // wouldn't have to clone here.
                consensus
                    .compare_and_set(Instant::now() + FOREVER, &path, None, new.clone())
                    .await
            })
            .await;
            match cas_res {
                Ok(()) => {
                    trace!(
                        "maybe_init_state succeeded {}\n  state={:?}",
                        state.seqno(),
                        state
                    );
                    return Ok(state);
                }
                Err(x) => {
                    // We lost a CaS race, use the value included in the CaS
                    // expectation error. Because we used None for expected,
                    // this should never be None.
                    debug!(
                        "maybe_init_state lost the CaS race, using current value: {:?}",
                        x.as_ref().map(|x| x.seqno)
                    );
                    debug_assert!(x.is_some());
                    current = x
                }
            }
        }
    }

    pub async fn fetch_and_update_state(&mut self) {
        let shard_id = self.shard_id();
        let current = retry_external("fetch_and_update_state::head", || async {
            self.consensus
                .head(Instant::now() + FOREVER, &shard_id.to_string())
                .await
        })
        .await;
        self.update_state(current).await;
    }

    async fn update_state(&mut self, current: Option<VersionedData>) {
        let current = match current {
            Some(x) => x,
            None => {
                // Machine is only constructed once, we've successfully
                // retrieved state from durable storage, but now it's gone? In
                // the future, maybe this means the shard was deleted or
                // something, but for now it's entirely unexpected.
                panic!("internal error: missing state {}", self.state.shard_id());
            }
        };
        let current_state = State::decode(&current.data)
            // We received a State with different declared codecs than a
            // previous SeqNo of the same State. Fail loudly.
            .expect("internal error: new durable state disagreed with old durable state");
        debug_assert_eq!(current.seqno, current_state.seqno());
        debug_assert!(self.state.seqno() <= current.seqno);
        self.state = current_state;
    }
}

pub const FOREVER: Duration = Duration::from_secs(1_000_000_000);

pub async fn retry_external<R, F, WorkFn>(name: &str, mut work_fn: WorkFn) -> R
where
    F: std::future::Future<Output = Result<R, ExternalError>>,
    WorkFn: FnMut() -> F,
{
    let mut retry = Retry::persist_defaults(SystemTime::now()).into_retry_stream();
    loop {
        match work_fn().await {
            Ok(x) => return x,
            Err(err) => {
                info!(
                    "external operation {} failed, retrying in {:?}: {}",
                    name,
                    retry.next_sleep(),
                    err
                );
                retry = retry.sleep().await;
            }
        }
    }
}

pub async fn retry_determinate<R, F, WorkFn>(
    name: &str,
    mut work_fn: WorkFn,
) -> Result<R, Indeterminate>
where
    F: std::future::Future<Output = Result<R, ExternalError>>,
    WorkFn: FnMut() -> F,
{
    let mut retry = Retry::persist_defaults(SystemTime::now()).into_retry_stream();
    loop {
        match work_fn().await {
            Ok(x) => return Ok(x),
            Err(ExternalError::Determinate(err)) => {
                info!(
                    "external operation {} failed, retrying in {:?}: {}",
                    name,
                    retry.next_sleep(),
                    err
                );
                retry = retry.sleep().await;
                continue;
            }
            Err(ExternalError::Indeterminate(x)) => return Err(x),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::new_test_client;
    use crate::ShardId;

    use super::*;

    #[tokio::test]
    async fn apply_unbatched_cmd_truncate() {
        mz_ore::test::init_logging();

        let (mut write, _) = new_test_client()
            .await
            .expect_open::<String, (), u64, i64>(ShardId::new())
            .await;
        let consensus = Arc::clone(&write.machine.consensus);

        // Write a bunch of batches. This should result in a bounded number of
        // live entries in consensus.
        const NUM_BATCHES: u64 = 100;
        for idx in 0..NUM_BATCHES {
            write
                .expect_compare_and_append(&[((idx.to_string(), ()), idx, 1)], idx, idx + 1)
                .await;
        }
        let key = write.machine.shard_id().to_string();
        let consensus_entries = consensus
            .scan(Instant::now() + FOREVER, &key, SeqNo::minimum())
            .await
            .expect("scan failed");
        // Make sure we constructed the key correctly.
        assert!(consensus_entries.len() > 0);
        // Make sure the number of entries is bounded.
        //
        // TODO: When we implement incremental state, this will be something
        // like log(NUM_BATCHES).
        let max_entries = 1;
        assert!(
            consensus_entries.len() <= max_entries,
            "expected at most {} entries got {}",
            max_entries,
            consensus_entries.len()
        );
    }
}
