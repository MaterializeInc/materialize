// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of the persist state machine.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::location::{Consensus, ExternalError, SeqNo, VersionedData};
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use tracing::{debug, info, trace};

use crate::error::{InvalidUsage, NoOp};
use crate::r#impl::state::{
    ReadCapability, Since, State, StateCollections, Upper, WriteCapability,
};
use crate::read::ReaderId;
use crate::write::WriterId;
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
        deadline: Instant,
        shard_id: ShardId,
        consensus: Arc<dyn Consensus + Send + Sync>,
    ) -> Result<Self, ExternalError> {
        let state = Self::maybe_init_state(deadline, consensus.as_ref(), shard_id).await?;
        Ok(Machine { consensus, state })
    }

    pub fn shard_id(&self) -> ShardId {
        self.state.shard_id()
    }

    pub async fn upper(&mut self, deadline: Instant) -> Result<Antichain<T>, ExternalError> {
        self.fetch_and_update_state(deadline).await?;
        Ok(self.state.upper())
    }

    pub async fn register(
        &mut self,
        deadline: Instant,
        writer_id: &WriterId,
        reader_id: &ReaderId,
    ) -> Result<(WriteCapability<T>, ReadCapability<T>), ExternalError> {
        let (seqno, (write_cap, read_cap)) = self
            .apply_unbatched_cmd::<_, (), _>(deadline, |seqno, state| {
                Ok(state.register(seqno, writer_id, reader_id))
            })
            .await?
            // TODO: Once the rust (!) Never type is stabilized, use it for
            // apply_unbatched_cmd's Err type to avoid this expect.
            .expect("register is infallible");
        debug_assert_eq!(seqno, read_cap.seqno);
        Ok((write_cap, read_cap))
    }

    pub async fn clone_reader(
        &mut self,
        deadline: Instant,
        new_reader_id: &ReaderId,
    ) -> Result<ReadCapability<T>, ExternalError> {
        let (seqno, read_cap) = self
            .apply_unbatched_cmd::<_, (), _>(deadline, |seqno, state| {
                Ok(state.clone_reader(seqno, new_reader_id))
            })
            .await?
            // TODO: Once the rust (!) Never type is stabilized, use it for
            // apply_unbatched_cmd's Err type to avoid this expect.
            .expect("clone_reader is infallible");
        debug_assert_eq!(seqno, read_cap.seqno);
        Ok(read_cap)
    }

    pub async fn append(
        &mut self,
        deadline: Instant,
        writer_id: &WriterId,
        keys: &[String],
        desc: &Description<T>,
    ) -> Result<Result<Result<SeqNo, Antichain<T>>, InvalidUsage>, ExternalError> {
        let res = self
            .apply_unbatched_cmd(deadline, |_, state| state.append(writer_id, keys, desc))
            .await?;
        let (seqno, res) = match res {
            Ok(x) => x,
            Err(err) => return Ok(Err(err)),
        };
        match res {
            Ok(()) => (),
            Err(current_upper) => return Ok(Ok(Err(current_upper))),
        };
        Ok(Ok(Ok(seqno)))
    }

    pub async fn compare_and_append(
        &mut self,
        deadline: Instant,
        writer_id: &WriterId,
        keys: &[String],
        desc: &Description<T>,
    ) -> Result<Result<Result<SeqNo, Antichain<T>>, InvalidUsage>, ExternalError> {
        let res = self
            .apply_unbatched_cmd(deadline, |_, state| {
                state.compare_and_append(writer_id, keys, desc)
            })
            .await?;
        let (seqno, res) = match res {
            Ok(x) => x,
            Err(err) => return Ok(Err(err)),
        };
        match res {
            Ok(()) => (),
            Err(current_upper) => return Ok(Ok(Err(current_upper))),
        };
        Ok(Ok(Ok(seqno)))
    }

    pub async fn downgrade_since(
        &mut self,
        deadline: Instant,
        reader_id: &ReaderId,
        new_since: &Antichain<T>,
    ) -> Result<Result<SeqNo, InvalidUsage>, ExternalError> {
        let res = self
            .apply_unbatched_cmd(deadline, |_, state| {
                state.downgrade_since(reader_id, new_since)
            })
            .await?;
        let (seqno, _) = match res {
            Ok(x) => x,
            Err(err) => return Ok(Err(err)),
        };
        Ok(Ok(seqno))
    }

    pub async fn expire_writer(
        &mut self,
        deadline: Instant,
        writer_id: &WriterId,
    ) -> Result<SeqNo, ExternalError> {
        let res = self
            .apply_unbatched_cmd(deadline, |seqno, state| {
                state.expire_writer(seqno, writer_id)
            })
            .await?;
        let seqno = match res {
            Ok((seqno, ())) => seqno,
            Err(NoOp { seqno }) => seqno,
        };
        Ok(seqno)
    }

    pub async fn expire_reader(
        &mut self,
        deadline: Instant,
        reader_id: &ReaderId,
    ) -> Result<SeqNo, ExternalError> {
        let res = self
            .apply_unbatched_cmd(deadline, |seqno, state| {
                state.expire_reader(seqno, reader_id)
            })
            .await?;
        let seqno = match res {
            Ok((seqno, ())) => seqno,
            Err(NoOp { seqno }) => seqno,
        };
        Ok(seqno)
    }

    pub async fn snapshot(
        &mut self,
        deadline: Instant,
        as_of: &Antichain<T>,
    ) -> Result<Result<Vec<(String, Description<T>)>, InvalidUsage>, ExternalError> {
        let mut fetches = 0;
        loop {
            let upper = match self.state.snapshot(as_of) {
                Ok(Ok(x)) => return Ok(Ok(x)),
                Ok(Err(Upper(upper))) => {
                    // The upper isn't ready yet, fall through and try again.
                    upper
                }
                Err(Since(since)) => {
                    return Ok(Err(InvalidUsage(anyhow!(
                        "snapshot with as_of {:?} cannot be served by shard with since: {:?}",
                        as_of,
                        since
                    ))))
                }
            };
            // Only sleep after the first fetch, because the first time through
            // maybe our state was just out of date.
            //
            // TODO: Some sort of backoff instead of a fixed sleep.
            if fetches > 0 {
                let sleep = if cfg!(test) {
                    Duration::from_nanos(1)
                } else {
                    Duration::from_secs(1)
                };
                if Instant::now() + sleep > deadline {
                    return Err(ExternalError::new_timeout(deadline));
                }
                info!(
                    "snapshot as of {:?} not available for upper {:?} retrying in {:?}",
                    as_of, upper, sleep
                );
                tokio::time::sleep(sleep).await;
            }
            self.fetch_and_update_state(deadline).await?;
            fetches += 1;
        }
    }

    pub async fn next_listen_batch(
        &mut self,
        deadline: Instant,
        frontier: &Antichain<T>,
    ) -> Result<(Vec<String>, Description<T>), ExternalError> {
        // This unconditionally fetches the latest state and uses that to
        // determine if we can serve `as_of`. TODO: We could instead check first
        // and only fetch if necessary.
        loop {
            self.fetch_and_update_state(deadline).await?;
            if let Some((keys, desc)) = self.state.next_listen_batch(frontier) {
                return Ok((keys.to_owned(), desc.clone()));
            }
            let sleep = if cfg!(test) {
                Duration::from_nanos(1)
            } else {
                Duration::from_secs(1)
            };
            if Instant::now() + sleep > deadline {
                return Err(ExternalError::new_timeout(deadline));
            }
            // Wait a bit and try again.
            //
            // TODO: See if we can watch for changes in Consensus to be more
            // reactive here.
            tokio::time::sleep(sleep).await;
        }
    }

    async fn apply_unbatched_cmd<
        R,
        E,
        WorkFn: FnMut(SeqNo, &mut StateCollections<T>) -> Result<R, E>,
    >(
        &mut self,
        deadline: Instant,
        mut work_fn: WorkFn,
    ) -> Result<Result<(SeqNo, R), E>, ExternalError> {
        let path = self.shard_id().to_string();

        loop {
            let (work_ret, new_state) = match self.state.clone_apply(&mut work_fn) {
                Ok(x) => x,
                Err(err) => return Ok(Err(err)),
            };
            trace!(
                "apply_unbatched_cmd attempting {}\n  new_state={:?}",
                self.state.seqno(),
                new_state
            );

            let new = VersionedData::from((new_state.seqno(), &new_state));
            let cas_res = self
                .consensus
                .compare_and_set(deadline, &path, Some(self.state.seqno()), new)
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
                    //
                    // It's weird that this will return an error for the whole
                    // request after we know it's successful, but this goes away
                    // in #12223.
                    let () = self
                        .consensus
                        .truncate(deadline, &path, self.state.seqno())
                        .await?;

                    return Ok(Ok((self.state.seqno(), work_ret)));
                }
                Err(current) => {
                    debug!(
                        "apply_unbatched_cmd lost the CaS race, retrying: {} vs {:?}",
                        self.state.seqno(),
                        current.as_ref().map(|x| x.seqno)
                    );
                    self.update_state(deadline, current).await?;

                    // TODO: Some sort of exponential backoff here?
                    let sleep = Duration::from_secs(0);
                    if Instant::now() + sleep > deadline {
                        return Err(ExternalError::new_timeout(deadline));
                    }
                    continue;
                }
            }
        }
    }

    // TODO: This is fairly duplicative of apply_unbatched_cmd. Unclear if
    // there's anything to do here...
    async fn maybe_init_state(
        deadline: Instant,
        consensus: &(dyn Consensus + Send + Sync),
        shard_id: ShardId,
    ) -> Result<State<K, V, T, D>, ExternalError> {
        debug!(
            "Machine::maybe_init_state deadline={:?} shard_id={}",
            deadline, shard_id
        );

        let path = shard_id.to_string();
        let mut current = consensus.head(deadline, &path).await?;

        loop {
            // First, check if the shard has already been initialized.
            if let Some(current) = current.as_ref() {
                let (current_seqno, current_state) =
                    <(SeqNo, State<K, V, T, D>)>::try_from(current)?;
                debug_assert_eq!(current_seqno, current_state.seqno());
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
            let cas_res = consensus
                .compare_and_set(deadline, &path, None, new)
                .await
                .map_err(|err| {
                    debug!("maybe_init_state errored: {}", err);
                    err
                })?;
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

    async fn fetch_and_update_state(&mut self, deadline: Instant) -> Result<(), ExternalError> {
        let shard_id = self.shard_id();
        let current = self.consensus.head(deadline, &shard_id.to_string()).await?;
        self.update_state(deadline, current).await
    }

    async fn update_state(
        &mut self,
        deadline: Instant,
        current: Option<VersionedData>,
    ) -> Result<(), ExternalError> {
        let current = match current {
            Some(x) => x,
            None => {
                // This seems a little wonky...
                self.state =
                    Self::maybe_init_state(deadline, self.consensus.as_ref(), self.shard_id())
                        .await?;
                return Ok(());
            }
        };
        let (current_seqno, current_state) = <(SeqNo, State<K, V, T, D>)>::try_from(&current)?;
        debug_assert_eq!(current_seqno, current_state.seqno());
        debug_assert!(self.state.seqno() <= current.seqno);
        self.state = current_state;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::new_test_client;
    use crate::{ShardId, NO_TIMEOUT};

    use super::*;

    #[tokio::test]
    async fn apply_unbatched_cmd_truncate() -> Result<(), Box<dyn std::error::Error>> {
        mz_ore::test::init_logging();

        let (mut write, _) = new_test_client()
            .await?
            .open::<String, (), u64, i64>(NO_TIMEOUT, ShardId::new())
            .await?;
        let consensus = Arc::clone(&write.machine.consensus);

        // Write a bunch of batches. This should result in a bounded number of
        // live entries in consensus.
        const NUM_BATCHES: u64 = 100;
        for idx in 0..NUM_BATCHES {
            write
                .compare_and_append(
                    NO_TIMEOUT,
                    [((idx.to_string(), ()), idx, 1)],
                    Antichain::from_elem(idx),
                    Antichain::from_elem(idx + 1),
                )
                .await??
                .expect("invalid current upper");
        }
        let key = write.machine.shard_id().to_string();
        let consensus_entries = consensus
            .scan(Instant::now() + NO_TIMEOUT, &key, SeqNo::minimum())
            .await?;
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

        Ok(())
    }
}
