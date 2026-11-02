// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Kafka transactions, with the server as its own transaction coordinator.
//!
//! A transactional ID has one producer ID at a time, and `InitProducerId` bumps its epoch, which
//! aborts the open transaction and fences every earlier producer with that ID. Records produced
//! in a transaction are buffered here until `EndTxn`. A commit hands them to the transaction's
//! claimed topic as one [`ProduceBatch`] and completes once the topic's consumer acknowledges
//! it, so a committed transaction is durable.
//!
//! The commit runs in its own task. A producer whose connection drops while `EndTxn` waits
//! retries it, and the retry must find the transaction's outcome. Until the commit completes,
//! `EndTxn` and `InitProducerId` for the transactional ID get the retriable
//! `CONCURRENT_TRANSACTIONS`, so a producer that restarts sees the effects of the commit.

use std::collections::{BTreeMap, BTreeSet};

use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::oneshot;
use tracing::debug;

use crate::{
    Admission, CompactedRecord, Holder, Home, KafkaServer, ProduceAck, ProduceBatch,
    ProducedRecord, ProducerBatchId, ProducerStates, ResponseError, SaslPlainCredentials,
    TopicSender,
};

#[derive(Debug)]
pub(crate) struct TransactionState {
    producer_id: i64,
    epoch: i16,
    /// The recent batches of the producer per topic, for deduplicating resent batches.
    sequences: BTreeMap<String, ProducerStates>,
    open: Option<OpenTransaction>,
    end: End,
}

#[derive(Debug, Default)]
struct OpenTransaction {
    /// The claimed topic the transaction writes, if any.
    claimed: Option<String>,
    compacted_topics: BTreeSet<String>,
    records: Vec<ProducedRecord>,
    bytes: usize,
    /// In the order produced.
    compacted: Vec<CompactedRecord>,
}

#[derive(Debug, Clone)]
enum End {
    None,
    /// A commit is in progress.
    Committing,
    /// The last transaction ended, which a retried `EndTxn` reports again.
    Ended {
        committed: bool,
        result: Result<(), ResponseError>,
    },
}

/// Where a committing transaction's records go.
struct Target {
    sender: TopicSender,
    home: Home,
}

impl KafkaServer {
    /// Assigns the producer ID and the next epoch to the producer of `transactional_id`.
    ///
    /// `producer` is the ID and epoch that a producer passes to bump its own epoch.
    pub(crate) fn init_transactional_producer(
        &self,
        transactional_id: &str,
        producer: Option<(i64, i16)>,
    ) -> Result<(i64, i16), ResponseError> {
        let mut transactions = self.inner.transactions.lock().expect("lock poisoned");
        let Some(state) = transactions.get_mut(transactional_id) else {
            // NOTE: a producer that passes its ID after the server restarted gets a new one,
            // like a new producer. Kafka brokers keep transaction state durably and reject it.
            let producer_id = self.allocate_producer_id();
            transactions.insert(
                transactional_id.into(),
                TransactionState {
                    producer_id,
                    epoch: 0,
                    sequences: BTreeMap::new(),
                    open: None,
                    end: End::None,
                },
            );
            return Ok((producer_id, 0));
        };
        if let Some((producer_id, epoch)) = producer {
            if producer_id != state.producer_id || epoch != state.epoch {
                return Err(ResponseError::ProducerFenced);
            }
        }
        if matches!(state.end, End::Committing) {
            return Err(ResponseError::ConcurrentTransactions);
        }
        if state.epoch == i16::MAX - 1 {
            state.producer_id = self.allocate_producer_id();
            state.epoch = 0;
        } else {
            state.epoch += 1;
        }
        if state.open.take().is_some() {
            debug!("aborted the open transaction of {transactional_id}");
        }
        state.sequences.clear();
        state.end = End::None;
        Ok((state.producer_id, state.epoch))
    }

    /// Adds `topics` to the open transaction of `transactional_id`, opening one if needed.
    ///
    /// Returns an error for the whole request, or one result per topic. If any topic fails,
    /// none is added.
    pub(crate) fn add_to_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        epoch: i16,
        topics: &[String],
        credentials: Option<&SaslPlainCredentials>,
    ) -> Result<Vec<Result<(), ResponseError>>, ResponseError> {
        let mut transactions = self.inner.transactions.lock().expect("lock poisoned");
        let state = producer_state(&mut transactions, transactional_id, producer_id, epoch)?;
        if matches!(state.end, End::Committing) {
            return Err(ResponseError::ConcurrentTransactions);
        }
        let open = state.open.get_or_insert_with(OpenTransaction::default);
        let server_state = self.inner.state.lock().expect("lock poisoned");
        let mut claimed = open.claimed.clone();
        let mut compacted = Vec::new();
        let mut results = Vec::new();
        for topic in topics {
            let result = if server_state.compacted.contains_key(topic) {
                compacted.push(topic.clone());
                Ok(())
            } else {
                match server_state.claimed(topic, credentials) {
                    // A commit is atomic only within one claimed topic.
                    Ok(_) if claimed.as_ref().is_some_and(|c| c != topic) => {
                        Err(ResponseError::InvalidTxnState)
                    }
                    Ok(_) => {
                        claimed = Some(topic.clone());
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            };
            results.push(result);
        }
        if results.iter().any(|r| r.is_err()) {
            return Ok(results
                .into_iter()
                .map(|r| r.and(Err(ResponseError::OperationNotAttempted)))
                .collect());
        }
        open.claimed = claimed;
        open.compacted_topics.extend(compacted);
        state.end = End::None;
        Ok(results)
    }

    /// Buffers a batch of a transactional producer until its transaction ends.
    pub(crate) fn produce_transactional(
        &self,
        transactional_id: &str,
        topic: &str,
        records: Vec<ProducedRecord>,
        bytes: usize,
        id: &ProducerBatchId,
    ) -> Result<(), ResponseError> {
        let mut transactions = self.inner.transactions.lock().expect("lock poisoned");
        let state = producer_state(
            &mut transactions,
            transactional_id,
            id.producer_id,
            id.epoch,
        )?;
        let open = match (&mut state.open, &state.end) {
            (Some(open), End::None) => open,
            _ => return Err(ResponseError::InvalidTxnState),
        };
        let is_compacted = open.compacted_topics.contains(topic);
        if !is_compacted && open.claimed.as_deref() != Some(topic) {
            return Err(ResponseError::InvalidTxnState);
        }
        if is_compacted && records.iter().any(|r| r.key.is_none()) {
            return Err(ResponseError::InvalidRecord);
        }
        let sequences = state.sequences.entry(topic.into()).or_default();
        match sequences.admit(id, records.len()) {
            Admission::Append => {}
            Admission::Duplicate { .. } => return Ok(()),
            Admission::Reject(e) => return Err(e),
        }
        sequences.record(id, records.len(), -1, 0);
        if is_compacted {
            open.compacted
                .extend(records.into_iter().map(|record| CompactedRecord {
                    topic: topic.into(),
                    key: record.key.expect("checked above"),
                    value: record.value,
                }));
        } else {
            open.records.extend(records);
            open.bytes += bytes;
        }
        Ok(())
    }

    /// Ends the open transaction of `transactional_id`. The returned future completes once a
    /// commit is durable.
    pub(crate) fn end_transaction(
        &self,
        transactional_id: String,
        producer_id: i64,
        epoch: i16,
        committed: bool,
        credentials: Option<&SaslPlainCredentials>,
    ) -> BoxFuture<'static, Result<(), ResponseError>> {
        let ready = |result| futures::future::ready(result).boxed();
        let mut transactions = self.inner.transactions.lock().expect("lock poisoned");
        let state = match producer_state(&mut transactions, &transactional_id, producer_id, epoch) {
            Ok(state) => state,
            Err(e) => return ready(Err(e)),
        };
        match &state.end {
            End::Committing => return ready(Err(ResponseError::ConcurrentTransactions)),
            End::Ended {
                committed: ended_committed,
                result,
            } if state.open.is_none() => {
                return ready(if *ended_committed == committed {
                    result.clone()
                } else {
                    Err(ResponseError::InvalidTxnState)
                });
            }
            _ => {}
        }
        let Some(open) = &state.open else {
            // librdkafka ends a transaction that wrote nothing without telling the broker, but
            // other clients may not.
            return ready(Ok(()));
        };
        if !committed {
            state.open = None;
            state.end = End::Ended {
                committed,
                result: Ok(()),
            };
            return ready(Ok(()));
        }

        // Keep the latest record of each key.
        let mut compacted: BTreeMap<(&str, &bytes::Bytes), &Option<bytes::Bytes>> = BTreeMap::new();
        for record in &open.compacted {
            compacted.insert((&record.topic, &record.key), &record.value);
        }
        let compacted: Vec<_> = compacted
            .into_iter()
            .map(|((topic, key), value)| CompactedRecord {
                topic: topic.into(),
                key: key.clone(),
                value: value.clone(),
            })
            .collect();

        // On failure the transaction stays open, for the producer to retry or abort.
        let target = {
            let server_state = self.inner.state.lock().expect("lock poisoned");
            commit_target(
                &server_state,
                open.claimed.as_deref(),
                &compacted,
                credentials,
            )
        };
        let target = match target {
            Ok(target) => target,
            Err(e) => return ready(Err(e)),
        };
        let open = state.open.take().expect("checked above");
        state.end = End::Committing;
        drop(transactions);

        let server = self.clone();
        let task_name = format!("kafka_txn_commit:{transactional_id}");
        let commit = mz_ore::task::spawn(|| task_name, {
            async move {
                let home = target.as_ref().map(|t| t.home.clone());
                let result = match target {
                    None => Ok(()),
                    Some(Target { sender, home: _ }) => {
                        let (tx, rx) = oneshot::channel();
                        let batch = ProduceBatch {
                            records: open.records,
                            bytes: open.bytes,
                            producer: None,
                            compacted: compacted.clone(),
                            ack: ProduceAck { tx: Some(tx) },
                        };
                        match sender.send(batch).await {
                            Ok(()) => match rx.await {
                                Ok(Ok(_)) => Ok(()),
                                Ok(Err(e)) => Err(e),
                                // The consumer dropped the batch, and may or may not have
                                // stored it.
                                Err(_) => Err(ResponseError::UnknownServerError),
                            },
                            Err(_) => Err(ResponseError::UnknownServerError),
                        }
                    }
                };
                if result.is_ok() {
                    let holder = match home {
                        Some(home) => Holder::Topic(home),
                        None => Holder::Memory,
                    };
                    let mut server_state = server.inner.state.lock().expect("lock poisoned");
                    server_state.apply_compacted(compacted, &holder);
                }
                let mut transactions = server.inner.transactions.lock().expect("lock poisoned");
                if let Some(state) = transactions.get_mut(&transactional_id) {
                    if state.producer_id == producer_id && state.epoch == epoch {
                        state.end = End::Ended { committed, result };
                    }
                }
                result
            }
        });
        commit.boxed()
    }
}

/// Looks up the state of `transactional_id` for a producer with `producer_id` and `epoch`.
fn producer_state<'a>(
    transactions: &'a mut BTreeMap<String, TransactionState>,
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
) -> Result<&'a mut TransactionState, ResponseError> {
    let state = transactions
        .get_mut(transactional_id)
        .ok_or(ResponseError::InvalidProducerIdMapping)?;
    if producer_id != state.producer_id {
        return Err(ResponseError::InvalidProducerIdMapping);
    }
    if epoch < state.epoch {
        return Err(ResponseError::ProducerFenced);
    }
    if epoch > state.epoch {
        return Err(ResponseError::InvalidProducerEpoch);
    }
    Ok(state)
}

/// Picks the claimed topic that stores a committing transaction's records: the one it wrote,
/// or else the one that stores the keys of its compacted records, or none if none does.
fn commit_target(
    state: &crate::State,
    claimed: Option<&str>,
    compacted: &[CompactedRecord],
    credentials: Option<&SaslPlainCredentials>,
) -> Result<Option<Target>, ResponseError> {
    let mut homes = BTreeSet::new();
    for record in compacted {
        let home = state.compacted_home(&record.topic, &record.key)?;
        if let Some(home) = home {
            homes.insert(home.topic);
        }
    }
    let topic = match claimed {
        Some(topic) => {
            // A key's records stay with the claimed topic that stores them, so that a restarted
            // topic restores the latest one.
            if homes.iter().any(|home| home != topic) {
                return Err(ResponseError::InvalidTxnState);
            }
            topic.to_string()
        }
        None => {
            if homes.len() > 1 {
                return Err(ResponseError::InvalidTxnState);
            }
            match homes.into_iter().next() {
                Some(topic) => topic,
                None => return Ok(None),
            }
        }
    };
    let (sender, registration_id) = state.claimed(&topic, credentials)?;
    Ok(Some(Target {
        sender,
        home: Home {
            topic,
            registration_id,
        },
    }))
}
