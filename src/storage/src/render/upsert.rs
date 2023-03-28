// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use differential_dataflow::consolidation;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::{AsCollection, Collection};
use itertools::Either;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::Scope;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::{Antichain, Timestamp};

use mz_ore::collections::{CollectionExt, HashSet};
use mz_repr::{Datum, Diff, Row};
use mz_storage_client::types::errors::{DataflowError, EnvelopeError, UpsertError};
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum UpsertCommand {
    Insert(Result<Row, UpsertError>),
    Remove(Result<Row, UpsertError>),
}

impl UpsertCommand {
    fn keyed<'a>(&'a self, key_indices: &'a [usize]) -> KeyedCommand<'a, &Self> {
        KeyedCommand {
            command: self,
            key_indices,
        }
    }

    fn into_keyed(self, key_indices: &[usize]) -> KeyedCommand<'_, Self> {
        KeyedCommand {
            command: self,
            key_indices,
        }
    }
}

/// An UpsertCommand wrapper whose implementations of Hash,PartialEq, and Eq are such that any
/// variant with the same key is considered the same.
struct KeyedCommand<'a, C: Borrow<UpsertCommand>> {
    command: C,
    key_indices: &'a [usize],
}

impl<'a, C: Borrow<UpsertCommand>> KeyedCommand<'a, C> {
    /// Produces the key of this upsert command. If the command has a well-formed key then an
    /// `Ok(_)` variant is returned with an iterator over the datum keys. If the command is about a
    /// key error then an `Err(_)` variant is returned.
    fn key(&self) -> Result<impl Iterator<Item = Datum<'_>> + '_, Datum<'_>> {
        match self.command.borrow() {
            UpsertCommand::Insert(Ok(row)) => {
                let mut key_indices = &*self.key_indices;
                let iter = row.iter().enumerate().flat_map(move |(idx, datum)| {
                    let key_idx = key_indices.get(0)?;
                    if idx == *key_idx {
                        key_indices = &key_indices[1..];
                        Some(datum)
                    } else {
                        None
                    }
                });
                Ok(Either::Left(Either::Left(iter)))
            }
            UpsertCommand::Remove(Ok(key)) => Ok(Either::Left(Either::Right(key.iter()))),
            UpsertCommand::Insert(Err(err)) | UpsertCommand::Remove(Err(err)) => match err {
                UpsertError::Value(err) => Ok(Either::Right(err.for_key.iter())),
                UpsertError::KeyDecode(err) => Err(Datum::Bytes(&err.raw)),
                UpsertError::NullKey(_) => Err(Datum::Null),
            },
        }
    }
}

impl<C: Borrow<UpsertCommand>> Hash for KeyedCommand<'_, C> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.key() {
            Ok(iter) => {
                for datum in iter {
                    datum.hash(state);
                }
                state.write_u8(0x01);
            }
            Err(datum) => {
                datum.hash(state);
                state.write_u8(0x02);
            }
        }
    }
}

impl<C: Borrow<UpsertCommand>> PartialEq for KeyedCommand<'_, C> {
    fn eq(&self, other: &Self) -> bool {
        match (self.key(), other.key()) {
            (Ok(this), Ok(other)) => this.eq(other),
            (Err(this), Err(other)) => this.eq(&other),
            _ => false,
        }
    }
}

impl<C: Borrow<UpsertCommand>> Eq for KeyedCommand<'_, C> {}

/// Resumes an upsert computation at `resume_upper` given as inputs a collection of upsert commands
/// and the collection of the previous output of this operator.
pub(crate) fn upsert<G: Scope, O: timely::ExchangeData + Ord>(
    input: &Collection<G, (UpsertCommand, O), Diff>,
    mut key_indices: Vec<usize>,
    resume_upper: Antichain<G::Timestamp>,
    previous: Collection<G, Result<Row, DataflowError>, Diff>,
    previous_token: Option<Rc<dyn Any>>,
) -> Collection<G, Result<Row, DataflowError>, Diff>
where
    G::Timestamp: TotalOrder,
{
    // Sort key indices to ensure we can construct the key by iterating over the datums of the row
    key_indices.sort_unstable();

    let mut builder = AsyncOperatorBuilder::new("Upsert".to_string(), input.scope());

    let mut input = {
        let key_indices = key_indices.clone();
        builder.new_input(
            &input.inner,
            Exchange::new(move |((cmd, _), _, _)| UpsertCommand::keyed(cmd, &key_indices).hashed()),
        )
    };

    // We only care about UpsertValueError since this is the only error that we can retract
    let previous = previous.flat_map(|result| match result {
        Ok(ok) => Some(UpsertCommand::Insert(Ok(ok))),
        Err(DataflowError::EnvelopeError(err)) => match *err {
            EnvelopeError::Upsert(err) => Some(UpsertCommand::Insert(Err(err))),
            _ => None,
        },
        Err(_) => None,
    });
    let mut previous = {
        let key_indices = key_indices.clone();
        builder.new_input(
            &previous.inner,
            Exchange::new(move |(cmd, _, _)| UpsertCommand::keyed(cmd, &key_indices).hashed()),
        )
    };
    let (mut output_handle, output) = builder.new_output();

    builder.build(move |caps| async move {
        let mut output_cap = caps.into_element();

        let mut snapshot = vec![];

        // In the first phase we will collect all the updates from our output that are not beyond
        // resume_upper. This will be the seed state for the command processing below.
        while let Some(event) = previous.next_mut().await {
            match event {
                AsyncEvent::Data(_cap, data) => {
                    snapshot.extend(
                        data.drain(..)
                            .filter(|(_row, ts, _diff)| !resume_upper.less_equal(ts))
                            .map(|(row, _ts, diff)| (row, diff)),
                    );
                }
                AsyncEvent::Progress(upper) => {
                    if PartialOrder::less_equal(&resume_upper, &upper) {
                        break;
                    }
                }
            }
        }
        drop(previous_token);
        while let Some(_event) = previous.next().await {
            // Exchaust the previous input. It is expected to immediately reach the empty
            // antichain since we have dropped its token.
        }

        consolidation::consolidate(&mut snapshot);

        let mut state = HashSet::new();

        for (cmd, diff) in snapshot {
            assert_eq!(diff, 1, "invalid upsert state");
            state.insert(cmd.into_keyed(&key_indices));
        }

        // Now can can resume consuming the collection
        let mut pending_batches = vec![];
        let mut output_updates = vec![];
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());
        while let Some(event) = input.next_mut().await {
            match event {
                AsyncEvent::Data(_cap, data) => {
                    if PartialOrder::less_equal(&input_upper, &resume_upper) {
                        data.retain(|(_, ts, _)| resume_upper.less_equal(ts));
                    }

                    // This could be done with a BTree but since we don't need indexed access merge
                    // sorting sorted batches is simpler.
                    data.sort_unstable_by(|((_, a_order), a_ts, _), ((_, b_order), b_ts, _)| {
                        a_ts.cmp(b_ts).then_with(|| a_order.cmp(b_order))
                    });

                    pending_batches.push(std::mem::take(data).into_iter().peekable());
                }
                AsyncEvent::Progress(upper) => {
                    // From all the pending batches take the prefix that is not beyond upper and
                    // produce the overall sorted iterator by merge sorting them.
                    let mut commands = pending_batches
                        .iter_mut()
                        .map(|batch| batch.peeking_take_while(|(_, ts, _)| !upper.less_equal(ts)))
                        .kmerge_by(|((_, a_order), a_ts, _), ((_, b_order), b_ts, _)| {
                            a_ts.cmp(b_ts).then_with(|| a_order.cmp(b_order)).is_lt()
                        })
                        .peekable();

                    while let Some(((cmd, _order), ts, diff)) = commands.next() {
                        assert!(diff > 0, "invalid upsert input");

                        if let Some(((p, _), p_ts, _)) = commands.peek() {
                            // Skip this command if the next one is for the same (time, key) pair.
                            // This skips to the command with the latest offset, as sorted above.
                            if p_ts == &ts && p.keyed(&key_indices) == cmd.keyed(&key_indices) {
                                continue;
                            }
                        }

                        match cmd {
                            UpsertCommand::Insert(result) => {
                                let new_cmd =
                                    UpsertCommand::Insert(result.clone()).into_keyed(&key_indices);
                                if let Some(old_cmd) = state.replace(new_cmd) {
                                    let UpsertCommand::Insert(old_result) = old_cmd.command else {
                                        // We only store insertions in the state set
                                        unreachable!()
                                    };
                                    output_updates.push((old_result, ts.clone(), -1));
                                }
                                output_updates.push((result, ts, 1));
                            }
                            UpsertCommand::Remove(result) => {
                                let cmd = UpsertCommand::Remove(result).into_keyed(&key_indices);
                                if let Some(old_cmd) = state.take(&cmd) {
                                    let UpsertCommand::Insert(old_result) = old_cmd.command else {
                                        // We only store insertions in the state set
                                        unreachable!()
                                    };
                                    output_updates.push((old_result, ts, -1));
                                }
                            }
                        }
                    }

                    // Retain batches that still have data
                    pending_batches.retain_mut(|batch| batch.peek().is_some());

                    output_handle
                        .give_container(&output_cap, &mut output_updates)
                        .await;
                    if let Some(ts) = upper.as_option() {
                        output_cap.downgrade(ts);
                    }
                    input_upper = upper;
                }
            }
        }
    });

    output.as_collection().map(|result| match result {
        Ok(ok) => Ok(ok),
        Err(err) => Err(DataflowError::from(EnvelopeError::Upsert(err))),
    })
}
