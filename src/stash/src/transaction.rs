// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    cmp,
    collections::{btree_map::Entry, BTreeMap},
    sync::{Arc, Mutex},
};

use futures::{
    future::{self, try_join, try_join3, try_join_all, BoxFuture},
    TryFutureExt,
};
use mz_ore::collections::CollectionExt;
use serde_json::Value;
use timely::{progress::Antichain, PartialOrder};
use tokio::sync::mpsc;
use tokio_postgres::{types::ToSql, Client};
use tracing::info;

use crate::{
    consolidate_kv, consolidate_updates_kv,
    postgres::{ConsolidateRequest, CountedStatements},
    AntichainFormatter, AppendBatch, Data, Diff, Id, InternalStashError, Stash, StashCollection,
    StashError, Timestamp,
};

// The limit AFTER which to split an update batch (that is, we will ship an update that
// exceeds this number, but then start another batch).
//
// Cockroach's default limit for sql.conn.max_read_buffer_message_size is 16MiB
// (https://github.com/cockroachdb/cockroach/blob/7e4e0b195cd61da6cd7a719a5b9aa2e84f68d475/pkg/sql/pgwire/pgwirebase/encoding.go#L50).
// Use a number well under that but still big ish that most things won't ever need to
// batch. Because we are only estimating the value size and ignoring various other
// things that contribute to the total pgwire message size, having a 14MiB headspace
// seems safe here.
pub const INSERT_BATCH_SPLIT_SIZE: usize = 2 * 1024 * 1024;

impl Stash {
    pub async fn with_transaction<F, T>(&mut self, f: F) -> Result<T, StashError>
    where
        F: FnOnce(Transaction) -> BoxFuture<Result<T, StashError>> + Clone + Sync + Send + 'static,
    {
        let (res, mut cons_rx, txn_collections) = self
            .transact(|stmts, client, collections| {
                let f = f.clone();
                let (cons_tx, cons_rx) = mpsc::unbounded_channel();
                let txn_collections = Arc::new(Mutex::new(BTreeMap::new()));
                let tx = Transaction {
                    stmts,
                    client,
                    consolidations: cons_tx,
                    savepoint: Arc::new(Mutex::new(false)),
                    sinces: Arc::new(Mutex::new(BTreeMap::new())),
                    uppers: Arc::new(Mutex::new(BTreeMap::new())),
                    stash_collections: collections,
                    txn_collections: Arc::clone(&txn_collections),
                };
                Box::pin(async move {
                    let res = f(tx).await?;
                    Ok((res, cons_rx, txn_collections))
                })
            })
            .await?;
        while let Some(cons) = cons_rx.recv().await {
            self.sinces_tx
                .send(cons)
                .expect("consolidator unexpectedly gone");
        }
        self.collections
            .extend(std::mem::take(&mut *txn_collections.lock().unwrap()));
        Ok(res)
    }
}

pub struct Transaction<'a> {
    stmts: &'a CountedStatements<'a>,
    client: &'a Client,
    // The set of consolidations that need to be performed if the transaction
    // succeeds.
    consolidations: mpsc::UnboundedSender<ConsolidateRequest>,
    // Savepoint state to enforce the invariant that only one SAVEPOINT is
    // active at once.
    savepoint: Arc<Mutex<bool>>,

    // Cached sinces and uppers for this transaction. These are set on first
    // query and updated on seal/compact.
    sinces: Arc<Mutex<BTreeMap<Id, Antichain<Timestamp>>>>,
    uppers: Arc<Mutex<BTreeMap<Id, Antichain<Timestamp>>>>,

    // Collections cached by the outer Stash.
    stash_collections: &'a BTreeMap<String, Id>,
    // Collections discovered by this transaction.
    txn_collections: Arc<Mutex<BTreeMap<String, Id>>>,
}

impl<'a> Transaction<'a> {
    /// Executes f in a SAVEPOINT. RELEASE if f returns Ok, ROLLBACK if f
    /// returns Err. This must be used for any fn that performs any falliable
    /// operation after its first write. This includes multiple write operations
    /// in a row (any function with multpile writes must use this function).
    async fn in_savepoint<'res, F, T>(&self, f: F) -> Result<T, StashError>
    where
        F: FnOnce() -> BoxFuture<'res, Result<T, StashError>>,
    {
        // Savepoints cannot be used recursively, so panic if that happened.
        {
            // Put this in a block to convince the rust compiler that savepoint
            // won't be used across the await.
            let mut savepoint = self.savepoint.lock().unwrap();
            if *savepoint {
                panic!("cannot call savepoint recursively");
            }
            *savepoint = true;
        };

        self.client.batch_execute("SAVEPOINT txn_savepoint").await?;
        let res = match f().await {
            Ok(t) => {
                self.client
                    .batch_execute("RELEASE SAVEPOINT txn_savepoint")
                    .await?;
                Ok(t)
            }
            Err(err) => {
                self.client
                    .batch_execute("ROLLBACK TO SAVEPOINT txn_savepoint")
                    .await?;
                Err(err)
            }
        };

        let mut savepoint = self.savepoint.lock().unwrap();
        assert!(*savepoint);
        *savepoint = false;

        res
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn collection<K, V>(&self, name: &str) -> Result<StashCollection<K, V>, StashError>
    where
        K: Data,
        V: Data,
    {
        if let Some(id) = self.stash_collections.get(name) {
            return Ok(StashCollection::new(*id));
        }

        let collection_id_opt: Option<_> = self
            .client
            .query_one(self.stmts.collection(), &[&name])
            .await
            .map(|row| row.get("collection_id"))
            .ok();

        let collection_id = match collection_id_opt {
            Some(id) => id,
            None => {
                let collection_id = self
                    .client
                    .query_one(
                        "INSERT INTO collections (name) VALUES ($1) RETURNING collection_id",
                        &[&name],
                    )
                    .await?
                    .get("collection_id");
                self.client
                    .execute(
                        "INSERT INTO sinces (collection_id, since) VALUES ($1, $2)",
                        &[&collection_id, &Timestamp::MIN],
                    )
                    .await?;
                self.client
                    .execute(
                        "INSERT INTO uppers (collection_id, upper) VALUES ($1, $2)",
                        &[&collection_id, &Timestamp::MIN],
                    )
                    .await?;
                collection_id
            }
        };

        self.txn_collections
            .lock()
            .unwrap()
            .insert(name.to_string(), collection_id);
        Ok(StashCollection::new(collection_id))
    }

    /// Returns the ids and names of all collections.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn collections(&self) -> Result<BTreeMap<Id, String>, StashError> {
        let rows = self
            .client
            .query("SELECT name, collection_id FROM collections", &[])
            .await?;
        let names = rows
            .into_iter()
            .map(|row| (row.get("collection_id"), row.get("name")));
        Ok(BTreeMap::from_iter(names))
    }

    /// Returns the raw sealed rows as JSON, not consolidated.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn peek_raw(
        &self,
        id: Id,
    ) -> Result<impl Iterator<Item = ((Value, Value), Diff)>, StashError> {
        let peek_timestamp = self.peek_timestamp_id(id).await?;
        Ok(self
            .iter_raw(id)
            .await?
            .filter_map(move |((k, v), time, diff)| {
                if time.less_equal(&peek_timestamp) {
                    Some(((k, v), diff))
                } else {
                    None
                }
            }))
    }

    /// Fixes collections that have retractions that don't have exactly equal additions (from a JSON
    /// perspective), but for which Rust thinks there are equivalents. Specifically, a Rust Option
    /// is equal for a JSON object without that property (which gets deserialized into None) or a
    /// JSON object with that property whose value is JSON null. This function will notice the
    /// attempted retraction and "consolidate" it with the thing it was trying to retract.
    ///
    /// Implement by fetching the raw rows (unconsolidated) as untype JSON, making a copy of those
    /// into a Rust struct, consolidating the Rust structs, subtracting those from the original
    /// untyped JSON rows. The leftover rows are the failed retraction attempts which we need to
    /// remove from on disk.
    ///
    /// Returns whether rows were removed.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn collection_fix_unconsolidated_rows<K, V>(
        &self,
        collection: StashCollection<K, V>,
    ) -> Result<bool, StashError>
    where
        K: Data,
        V: Data,
    {
        let raw_rows = self.peek_raw(collection.id).await?.collect::<Vec<_>>();
        if raw_rows.iter().all(|((_k, _v), diff)| *diff == 1) {
            // No retractions to even worry about.
            return Ok(false);
        }

        // Generate the set of consolidated rows AFTER converting to a non-JSON value (i.e., a real Rust
        // struct). This is required because it WILL consolidate things that have different JSON values, but
        // equal Rust values (None Options).
        let consolidated = consolidate_kv::<K, V, _>(&raw_rows);
        // Convert to stringified-JSON (because we need an Ord impl) but with negative diffs for
        // removal from raw_rows.
        let consolidated = consolidated.map(|((k, v), diff)| {
            (
                (
                    serde_json::to_string(&serde_json::to_value(k).expect("must serialize"))
                        .expect("must string"),
                    serde_json::to_string(&serde_json::to_value(v).expect("must serialize"))
                        .expect("must string"),
                ),
                -diff,
            )
        });
        // Convert to a comparable data structure so we can consolidate.
        let mut to_retract = raw_rows
            .iter()
            .map(|((k, v), diff)| {
                (
                    (
                        serde_json::to_string(k).expect("must string"),
                        serde_json::to_string(v).expect("must string"),
                    ),
                    *diff,
                )
            })
            .chain(consolidated)
            .collect::<Vec<_>>();
        // Consolidate away the expected rows. The rows leftover are the superfluous ones that we
        // need to remove from on disk.
        differential_dataflow::consolidation::consolidate(&mut to_retract);
        if to_retract.is_empty() {
            return Ok(false);
        }
        let to_retract = to_retract.into_iter().map(|((k, v), diff)| {
            let k: Value = serde_json::from_str(&k).expect("must deserialize");
            let v: Value = serde_json::from_str(&v).expect("must deserialize");
            ((k, v), diff)
        });
        let mut batch = collection.make_batch_tx(self).await?;
        for ((key, value), diff) in to_retract {
            info!(
                "fixing unmatched but consolidated entry from collection {}: {:?}",
                collection.id,
                ((&key, &value), diff)
            );
            batch.entries.push(((key, value), batch.timestamp, -diff));
        }
        // Remove the on-disk data.
        self.append(vec![batch]).await?;
        Ok(true)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn consolidate(&self, id: Id) -> Result<(), StashError> {
        let since = self.since(id).await?;
        self.consolidations
            .send(ConsolidateRequest {
                id,
                since,
                done: None,
            })
            .unwrap();
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn upper(&self, collection_id: Id) -> Result<Antichain<Timestamp>, StashError> {
        // We can't use .entry here because that would require holding the
        // MutexGuard across the .await.
        if let Some(entry) = self.uppers.lock().unwrap().get(&collection_id) {
            return Ok(entry.clone());
        }
        let upper: Option<Timestamp> = self
            .client
            .query_one(self.stmts.upper(), &[&collection_id])
            .await?
            .get("upper");
        let upper = Antichain::from_iter(upper);
        maybe_update_antichain(&self.uppers, collection_id, upper.clone());
        Ok(upper)
    }

    pub async fn since(&self, collection_id: Id) -> Result<Antichain<Timestamp>, StashError> {
        // We can't use .entry here because that would require holding the
        // MutexGuard across the .await.
        if let Some(entry) = self.sinces.lock().unwrap().get(&collection_id) {
            return Ok(entry.clone());
        }
        let since: Option<Timestamp> = self
            .client
            .query_one(self.stmts.since(), &[&collection_id])
            .await?
            .get("since");
        let since = Antichain::from_iter(since);
        maybe_update_antichain(&self.sinces, collection_id, since.clone());
        Ok(since)
    }

    /// Returns sinces for the requested collections.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn sinces_batch(
        &self,
        collections: &[Id],
    ) -> Result<BTreeMap<Id, Antichain<Timestamp>>, StashError> {
        let mut futures = Vec::with_capacity(collections.len());
        for collection_id in collections {
            futures.push(async move {
                let since = self.since(*collection_id).await?;
                // Without this type assertion, we get a "type inside `async fn` body must be
                // known in this context" error.
                Result::<_, StashError>::Ok((*collection_id, since))
            });
        }
        let sinces = BTreeMap::from_iter(try_join_all(futures).await?);
        Ok(sinces)
    }

    /// Iterates over a collection.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn iter<K, V>(
        &self,
        collection: StashCollection<K, V>,
    ) -> Result<Vec<((K, V), Timestamp, Diff)>, StashError>
    where
        K: Data,
        V: Data,
    {
        let rows = self.iter_raw(collection.id).await?;
        let rows = consolidate_updates_kv(rows).collect();
        Ok(rows)
    }

    /// Iterates over a collection, returning the raw data on disk, unconsolidated.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn iter_raw(
        &self,
        id: Id,
    ) -> Result<impl Iterator<Item = ((Value, Value), Timestamp, Diff)>, StashError> {
        let since = match self.since(id).await?.into_option() {
            Some(since) => since,
            None => {
                return Err(StashError::from(
                    "cannot iterate collection with empty since frontier",
                ));
            }
        };
        let rows = self
            .client
            .query(self.stmts.iter(), &[&id])
            .await?
            .into_iter()
            .map(move |row| {
                let key: Value = row.get("key");
                let value: Value = row.get("value");
                let time = row.get("time");
                let diff: Diff = row.get("diff");
                ((key, value), cmp::max(time, since), diff)
            });
        Ok(rows)
    }

    /// Iterates over the values of a key.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn iter_key<K, V>(
        &self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Vec<(V, Timestamp, Diff)>, StashError>
    where
        K: Data,
        V: Data,
    {
        let key = serde_json::to_vec(key).expect("must serialize");
        let key: Value = serde_json::from_slice(&key)?;
        let (since, rows) = future::try_join(
            self.since(collection.id),
            self.client
                .query(self.stmts.iter_key(), &[&collection.id, &key])
                .map_err(|err| err.into()),
        )
        .await?;
        let since = match since.into_option() {
            Some(since) => since,
            None => {
                return Err(StashError::from(
                    "cannot iterate collection with empty since frontier",
                ));
            }
        };
        let mut rows = rows
            .into_iter()
            .map(|row| {
                let value: Value = row.try_get("value")?;
                let value: V = serde_json::from_value(value)?;
                let time = row.try_get("time")?;
                let diff = row.try_get("diff")?;
                Ok::<_, StashError>((value, cmp::max(time, since), diff))
            })
            .collect::<Result<Vec<_>, _>>()?;
        differential_dataflow::consolidation::consolidate_updates(&mut rows);
        Ok(rows)
    }

    /// Returns the most recent timestamp at which sealed entries can be read.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn peek_timestamp_id(&self, id: Id) -> Result<Timestamp, StashError> {
        let (since, upper) = try_join(self.since(id), self.upper(id)).await?;
        if PartialOrder::less_equal(&upper, &since) {
            return Err(StashError {
                inner: InternalStashError::PeekSinceUpper(format!(
                    "collection {} since {} is not less than upper {}",
                    id,
                    AntichainFormatter(&since),
                    AntichainFormatter(&upper)
                )),
            });
        }
        match upper.as_option() {
            Some(ts) => match ts.checked_sub(1) {
                Some(ts) => Ok(ts),
                None => Err("could not determine peek timestamp".into()),
            },
            None => Ok(Timestamp::MAX),
        }
    }

    /// Returns the most recent timestamp at which sealed entries can be read.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn peek_timestamp<K, V>(
        &self,
        collection: StashCollection<K, V>,
    ) -> Result<Timestamp, StashError>
    where
        K: Data,
        V: Data,
    {
        self.peek_timestamp_id(collection.id).await
    }

    /// Returns the current value of sealed entries.
    ///
    /// Entries are iterated in `(key, value)` order and are guaranteed to be
    /// consolidated.
    ///
    /// Sealed entries are those with timestamps less than the collection's upper
    /// frontier.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn peek<K, V>(
        &self,
        collection: StashCollection<K, V>,
    ) -> Result<Vec<(K, V, Diff)>, StashError>
    where
        K: Data,
        V: Data,
    {
        let timestamp = self.peek_timestamp(collection).await?;
        let mut rows: Vec<_> = self
            .iter(collection)
            .await?
            .into_iter()
            .filter_map(|((k, v), data_ts, diff)| {
                if data_ts.less_equal(&timestamp) {
                    Some((k, v, diff))
                } else {
                    None
                }
            })
            .collect();
        differential_dataflow::consolidation::consolidate_updates(&mut rows);
        Ok(rows)
    }

    /// Returns the current k,v pairs of sealed entries, erroring if there is more
    /// than one entry for a given key or the multiplicity is not 1 for each key.
    ///
    /// Sealed entries are those with timestamps less than the collection's upper
    /// frontier.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn peek_one<K, V>(
        &self,
        collection: StashCollection<K, V>,
    ) -> Result<BTreeMap<K, V>, StashError>
    where
        K: Data + std::hash::Hash,
        V: Data,
    {
        let rows = self.peek(collection).await?;
        let mut res = BTreeMap::new();
        for (k, v, diff) in rows {
            if diff != 1 {
                return Err("unexpected peek multiplicity".into());
            }
            if res.insert(k, v).is_some() {
                return Err(format!("duplicate peek keys for collection {}", collection.id).into());
            }
        }
        Ok(res)
    }

    /// Returns the current sealed value for the given key, erroring if there is
    /// more than one entry for the key or its multiplicity is not 1.
    ///
    /// Sealed entries are those with timestamps less than the collection's upper
    /// frontier.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn peek_key_one<K, V>(
        &self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Option<V>, StashError>
    where
        K: Data,
        V: Data,
    {
        let timestamp = self.peek_timestamp(collection).await?;
        let mut rows: Vec<_> = self
            .iter_key(collection, key)
            .await?
            .into_iter()
            .filter_map(|(v, data_ts, diff)| {
                if data_ts.less_equal(&timestamp) {
                    Some((v, diff))
                } else {
                    None
                }
            })
            .collect();
        differential_dataflow::consolidation::consolidate(&mut rows);
        let v = match rows.len() {
            1 => {
                let (v, diff) = rows.into_element();
                match diff {
                    1 => Some(v),
                    0 => None,
                    _ => return Err("multiple values unexpected".into()),
                }
            }
            0 => None,
            _ => return Err("multiple values unexpected".into()),
        };
        Ok(v)
    }

    /// Applies batches to the current transaction. If any batch fails and in
    /// error returned, all other applications are rolled back.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn append(&self, batches: Vec<AppendBatch>) -> Result<(), StashError> {
        if batches.is_empty() {
            return Ok(());
        }

        let consolidations = self
            .in_savepoint(|| {
                Box::pin(async move {
                    let futures = batches.into_iter().map(
                        |AppendBatch {
                             collection_id,
                             lower,
                             upper,
                             entries,
                             ..
                         }| async move {
                            // Clone to appease rust async.
                            let compact = lower.clone();
                            let lower1 = lower.clone();
                            let lower2 = lower.clone();
                            let upper1 = upper.clone();
                            // The new upper must be validated before being sealed.
                            // Compaction can be done in any order because none of
                            // update/seal care about what the since is. Update, although it
                            // cares about the upper, can also be done in any order because
                            // we pass in the old upper, so it's not a problem if the seal
                            // has executed already since update won't query the current
                            // upper.
                            try_join3(
                                    async move {
                                        let current_upper = self.upper(collection_id).await?;
                                        if current_upper != lower1 {
                                            return Err(StashError::from(format!(
                                                "unexpected lower, got {:?}, expected {:?}",
                                                current_upper, lower1
                                            )));
                                        }

                                        self.seal(collection_id, upper1, Some(lower)).await
                                    },
                                    async move {
                                        self.update(collection_id, &entries, Some(lower2)).await
                                    },
                                    async move {
                                        self.compact(collection_id, &compact, Some(upper)).await
                                    },
                                )
                                .await?;
                            Ok::<_, StashError>(ConsolidateRequest {
                                id: collection_id,
                                since: self.since(collection_id).await?,
                                done: None,
                            })
                        },
                    );
                    try_join_all(futures).await
                })
            })
            .await?;

        for cons in consolidations {
            self.consolidations.send(cons).unwrap();
        }
        Ok(())
    }

    /// Like update, but starts a savepoint.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn update_savepoint(
        &self,
        collection_id: Id,
        entries: &[((Value, Value), Timestamp, Diff)],
        upper: Option<Antichain<Timestamp>>,
    ) -> Result<(), StashError> {
        self.in_savepoint(|| Box::pin(async { self.update(collection_id, entries, upper).await }))
            .await
    }

    /// Directly add k, v, ts, diff tuples to a collection.`upper` can be `Some`
    /// if the collection's upper is already known. Caller must have already
    /// called in_savepoint.
    ///
    /// This function should not be called outside of the stash crate since it
    /// allows for arbitrary bytes, non-unit diffs in collections, and doesn't
    /// support transaction safety. Use `TypedCollection`'s methods instead.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn update(
        &self,
        collection_id: Id,
        entries: &[((Value, Value), Timestamp, Diff)],
        upper: Option<Antichain<Timestamp>>,
    ) -> Result<(), StashError> {
        {
            // Panic if the caller didn't initiate the savepoint.
            let savepoint = self.savepoint.lock().unwrap();
            assert!(*savepoint);
        }

        if entries.is_empty() {
            return Ok(());
        }

        // Check the upper in a separate future so we can issue the updates without
        // waiting for it first.
        let upper_fut = async {
            let upper = match upper {
                Some(upper) => upper,
                None => self.upper(collection_id).await?,
            };
            for ((_key, _value), time, _diff) in entries {
                if !upper.less_equal(time) {
                    return Err(StashError::from(format!(
                        "entry time {} is less than the current upper frontier {}",
                        time,
                        AntichainFormatter(&upper)
                    )));
                }
            }
            Ok(upper)
        };

        /// Returns the estimated number of bytes v would take when encoded using ToSql. This is
        /// meant to be a fast estimate that accounts for things that could be possibly large, and
        /// isn't too worried about missing various single bytes.
        fn estimate_json_value_size(v: &Value) -> usize {
            match v {
                Value::Null => 4,    // "null"
                Value::Bool(_) => 5, // "false"
                Value::Number(_) => 8,
                Value::String(v) => v.len() + 2, // string bytes + double quotes; will be incorrect for strings needing escaping
                Value::Array(v) => {
                    let mut s = 2; // "[]"
                    for element in v {
                        s += estimate_json_value_size(element);
                        s += 2; // ", "
                    }
                    s
                }
                Value::Object(v) => {
                    let mut s = 2; // "{}"
                    for (key, val) in v {
                        s += key.len() + 2;
                        s += 2; // ": "
                        s += estimate_json_value_size(val);
                        s += 2; // ", "
                    }
                    s
                }
            }
        }

        let insert_fut = async {
            let mut entries = entries.iter();
            loop {
                let mut args: Vec<&'_ (dyn ToSql + Sync)> = Vec::new();
                // All rows use the collection id, so hard code it as the first.
                args.push(&collection_id);
                let mut batch_size = 0;
                let mut estimated_json_size = 0;
                // Accumulate into a batch until the size limit is exceeded or there are no more
                // entries.
                while let Some(((key, value), time, diff)) = entries.next() {
                    estimated_json_size += estimate_json_value_size(key);
                    estimated_json_size += estimate_json_value_size(value);
                    args.push(key);
                    args.push(value);
                    args.push(time);
                    args.push(diff);
                    batch_size += 1;
                    if estimated_json_size > INSERT_BATCH_SPLIT_SIZE {
                        break;
                    }
                }
                if batch_size == 0 {
                    return Ok(());
                }
                let stmt = self.stmts.update(self.client, batch_size).await?;
                self.client.execute(&stmt, &args).await?;
            }
        };
        try_join(upper_fut, insert_fut).await?;
        Ok(())
    }

    /// Sets the since of a collection. The current upper can be `Some` if it is
    /// already known.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn compact<'ts>(
        &self,
        id: Id,
        new_since: &'ts Antichain<Timestamp>,
        upper: Option<Antichain<Timestamp>>,
    ) -> Result<(), StashError> {
        // Do all validation first.
        try_join(
            async move {
                let since = self.since(id).await?;
                if PartialOrder::less_than(new_since, &since) {
                    return Err(StashError::from(format!(
                        "compact request {} is less than the current since frontier {}",
                        AntichainFormatter(new_since),
                        AntichainFormatter(&since)
                    )));
                }
                Ok(())
            },
            async move {
                let upper = match upper {
                    Some(upper) => upper,
                    None => self.upper(id).await?,
                };
                if PartialOrder::less_than(&upper, new_since) {
                    return Err(StashError::from(format!(
                        "compact request {} is greater than the current upper frontier {}",
                        AntichainFormatter(new_since),
                        AntichainFormatter(&upper)
                    )));
                }
                Ok(())
            },
        )
        .await?;

        // If successful, execute the change in the txn.
        self.client
            .execute(self.stmts.compact(), &[&new_since.as_option(), &id])
            .map_err(StashError::from)
            .await?;
        maybe_update_antichain(&self.sinces, id, new_since.clone());
        Ok(())
    }

    /// Sets the upper of a collection. The current upper can be `Some` if it is
    /// already known.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn seal(
        &self,
        id: Id,
        new_upper: Antichain<Timestamp>,
        upper: Option<Antichain<Timestamp>>,
    ) -> Result<(), StashError> {
        let upper = match upper {
            Some(upper) => upper,
            None => self.upper(id).await?,
        };
        if PartialOrder::less_than(&new_upper, &upper) {
            return Err(StashError::from(format!(
                "seal request {} is less than the current upper frontier {}",
                AntichainFormatter(&new_upper),
                AntichainFormatter(&upper),
            )));
        }

        self.client
            .execute(self.stmts.seal(), &[&new_upper.as_option(), &id])
            .map_err(StashError::from)
            .await?;
        maybe_update_antichain(&self.uppers, id, new_upper);
        Ok(())
    }
}

// Updates an antichain cache if the new value has advanced. Needed because the
// functions here are often called in try_joins where the futures execute in
// unknown order and we want to prevent a race condition poisioning the cache by
// going backward.
fn maybe_update_antichain(
    map: &Arc<Mutex<BTreeMap<Id, Antichain<Timestamp>>>>,
    id: Id,
    updated: Antichain<Timestamp>,
) {
    match map.lock().unwrap().entry(id) {
        Entry::Occupied(mut entry) => {
            if PartialOrder::less_than(entry.get(), &updated) {
                entry.insert(updated);
            }
        }
        Entry::Vacant(entry) => {
            entry.insert(updated);
        }
    };
}
