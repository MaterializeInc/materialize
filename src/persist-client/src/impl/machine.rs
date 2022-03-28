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
use mz_persist::location::{Consensus, LocationError, SeqNo, VersionedData};
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use tracing::debug;

use crate::error::InvalidUsage;
use crate::r#impl::state::{ReadCapability, State, WriteCapability};
use crate::read::ReaderId;
use crate::write::WriterId;
use crate::Id;

#[derive(Debug)]
pub struct Machine<K, V, T, D> {
    consensus: Arc<dyn Consensus>,

    seqno: Option<SeqNo>,
    state: State<K, V, T, D>,
}

// Impl Clone regardless of the type params.
impl<K, V, T: Clone, D> Clone for Machine<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            consensus: Arc::clone(&self.consensus),
            seqno: self.seqno.clone(),
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
    pub fn new(id: Id, consensus: Arc<dyn Consensus>) -> Self {
        Machine {
            consensus,
            seqno: None,
            state: State::new(id),
        }
    }

    pub fn id(&self) -> Id {
        self.state.id()
    }

    pub async fn register(
        &mut self,
        deadline: Instant,
        writer_id: &WriterId,
        reader_id: &ReaderId,
    ) -> Result<(WriteCapability<T>, ReadCapability<T>), LocationError> {
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
        reader_id: &ReaderId,
    ) -> Result<ReadCapability<T>, LocationError> {
        let (seqno, read_cap) = self
            .apply_unbatched_cmd::<_, (), _>(deadline, |seqno, state| {
                Ok(state.clone_reader(seqno, reader_id))
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
    ) -> Result<Result<SeqNo, InvalidUsage>, LocationError> {
        let res = self
            .apply_unbatched_cmd(deadline, |_, state| state.append(writer_id, keys, desc))
            .await?;
        let (seqno, _) = match res {
            Ok(x) => x,
            Err(err) => return Ok(Err(err)),
        };
        Ok(Ok(seqno))
    }

    pub async fn downgrade_since(
        &mut self,
        deadline: Instant,
        reader_id: &ReaderId,
        new_since: &Antichain<T>,
    ) -> Result<Result<SeqNo, InvalidUsage>, LocationError> {
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
    ) -> Result<SeqNo, LocationError> {
        let (seqno, ()) = self
            .apply_unbatched_cmd(deadline, |_, state| state.expire_writer(writer_id))
            .await?
            .expect("WIP");
        Ok(seqno)
    }

    pub async fn expire_reader(
        &mut self,
        deadline: Instant,
        reader_id: &ReaderId,
    ) -> Result<SeqNo, LocationError> {
        let (seqno, ()) = self
            .apply_unbatched_cmd(deadline, |_, state| state.expire_reader(reader_id))
            .await?
            .expect("WIP");
        Ok(seqno)
    }

    pub async fn snapshot(
        &mut self,
        deadline: Instant,
        as_of: &Antichain<T>,
    ) -> Result<Result<Vec<String>, InvalidUsage>, LocationError> {
        // TODO: This fetches the latest state and uses that to determine if we
        // can serve `as_of`. It's possible that this `as_of` could be served by
        // the version we happen to have cached, but not by this one we fetch.
        // Do we care?
        self.fetch_and_update_state(deadline).await?;
        Ok(self.state.snapshot(as_of))
    }

    pub async fn next_listen_batch(
        &mut self,
        deadline: Instant,
        frontier: &Antichain<T>,
    ) -> Result<(Vec<String>, Description<T>), LocationError> {
        loop {
            self.fetch_and_update_state(deadline).await?;
            if let Some((keys, desc)) = self.state.next_listen_batch(frontier) {
                return Ok((keys.to_owned(), desc.clone()));
            }
            // Wait a bit and try again.
            //
            // TODO: See if we can watch for changes in Consensus to be more
            // reactive here.
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn apply_unbatched_cmd<
        R,
        E,
        WorkFn: FnMut(SeqNo, &mut State<K, V, T, D>) -> Result<R, E>,
    >(
        &mut self,
        deadline: Instant,
        mut work_fn: WorkFn,
    ) -> Result<Result<(SeqNo, R), E>, LocationError> {
        loop {
            let new_seqno = self.seqno.unwrap_or_default().next();
            let mut new_state = self.state.clone();
            let work_ret = match work_fn(new_seqno, &mut new_state) {
                Ok(x) => x,
                Err(err) => return Ok(Err(err)),
            };

            let mut value = Vec::new();
            new_state.encode(&mut value);
            let cas_res = self
                .consensus
                .compare_and_swap(
                    self.seqno,
                    Some(VersionedData {
                        seqno: new_seqno,
                        data: value,
                    }),
                    deadline,
                )
                .await?;
            match cas_res {
                Ok(()) => {
                    self.seqno = Some(new_seqno);
                    self.state = new_state;
                    return Ok(Ok((new_seqno, work_ret)));
                }
                Err(current) => {
                    debug!(
                        "apply_unbatched_cmd lost the CaS race, retrying: {:?} vs {:?}",
                        self.seqno,
                        current.as_ref().map(|x| x.seqno)
                    );
                    self.update_state(current).await?;
                    // TODO: Some sort of exponential backoff here?
                    continue;
                }
            }
        }
    }

    pub(crate) async fn fetch_and_update_state(
        &mut self,
        deadline: Instant,
    ) -> Result<(), LocationError> {
        let current = self.consensus.head(deadline).await?;
        self.update_state(current).await
    }

    async fn update_state(&mut self, current: Option<VersionedData>) -> Result<(), LocationError> {
        let current = match current {
            Some(x) => x,
            None => {
                self.seqno = None;
                self.state = State::new(self.state.id());
                return Ok(());
            }
        };
        let current_state = State::decode(&current.data)
            .map_err(|err| LocationError::from(anyhow!("unable to decode state: {}", err)))?;
        self.seqno = Some(current.seqno);
        self.state = current_state;
        Ok(())
    }
}
