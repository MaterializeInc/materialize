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
use tracing::debug;

use crate::error::{InvalidUsage, NoOp};
use crate::r#impl::state::{ReadCapability, State, WriteCapability};
use crate::read::ReaderId;
use crate::write::WriterId;
use crate::Id;

#[derive(Debug)]
pub struct Machine<K, V, T, D> {
    consensus: Arc<dyn Consensus + Send + Sync>,

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
    pub fn new(id: Id, consensus: Arc<dyn Consensus + Send + Sync>) -> Self {
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
    ) -> Result<Result<SeqNo, InvalidUsage>, ExternalError> {
        let res = self
            .apply_unbatched_cmd(deadline, |_, state| state.append(writer_id, keys, desc))
            .await?;
        let (seqno, _) = match res {
            Ok(x) => x,
            Err(err) => return Ok(Err(err)),
        };
        Ok(Ok(seqno))
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
    ) -> Result<Result<Vec<String>, InvalidUsage>, ExternalError> {
        // This unconditionally fetches the latest state and uses that to
        // determine if we can serve `as_of`. TODO: We could instead check first
        // and only fetch if necessary.
        self.fetch_and_update_state(deadline).await?;
        Ok(self.state.snapshot(as_of))
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
            let sleep = Duration::from_secs(1);
            if Instant::now() + sleep > deadline {
                return Err(ExternalError::from(anyhow!("timeout at {:?}", deadline)));
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
        WorkFn: FnMut(SeqNo, &mut State<K, V, T, D>) -> Result<R, E>,
    >(
        &mut self,
        deadline: Instant,
        mut work_fn: WorkFn,
    ) -> Result<Result<(SeqNo, R), E>, ExternalError> {
        loop {
            let id = self.state.id();

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
                .compare_and_set(
                    &id.to_string(),
                    deadline,
                    self.seqno,
                    VersionedData {
                        seqno: new_seqno,
                        data: value,
                    },
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
                    let sleep = Duration::from_secs(0);
                    if Instant::now() + sleep > deadline {
                        return Err(ExternalError::from(anyhow!("timeout at {:?}", deadline)));
                    }
                    continue;
                }
            }
        }
    }

    async fn fetch_and_update_state(&mut self, deadline: Instant) -> Result<(), ExternalError> {
        let id = self.id();
        let current = self.consensus.head(&id.to_string(), deadline).await?;
        self.update_state(current).await
    }

    async fn update_state(&mut self, current: Option<VersionedData>) -> Result<(), ExternalError> {
        let current = match current {
            Some(x) => x,
            None => {
                self.seqno = None;
                self.state = State::new(self.state.id());
                return Ok(());
            }
        };
        let current_state = State::decode(&current.data)
            .map_err(|err| ExternalError::from(anyhow!("unable to decode state: {}", err)))?;
        debug_assert!(self.seqno.unwrap_or_default() <= current.seqno);
        self.seqno = Some(current.seqno);
        self.state = current_state;
        Ok(())
    }
}
