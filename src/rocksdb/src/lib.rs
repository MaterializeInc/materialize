// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An async wrapper around RocksDB, that does IO on a separate thread.
//!
//! This crate offers a limited API to communicate with RocksDB, to get
//! the best performance possible (most importantly, by batching operations).
//! Currently this API is only `upsert`, which replaces (or deletes) values for
//! a set of keys, and returns the previous values.

#![warn(missing_docs)]

use std::convert::AsRef;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::time::Instant;

use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::metrics::{DeleteOnDropCounter, DeleteOnDropHistogram};
use mz_ore::retry::{Retry, RetryResult};
use prometheus::core::AtomicU64;
use rocksdb::{Env, Error as RocksDBError, ErrorKind, Options as RocksDBOptions, WriteOptions, DB};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

pub mod config;
pub use config::{defaults, RocksDBConfig, RocksDBTuningParameters};

use crate::config::WriteBufferManagerHandle;

/// An error using this RocksDB wrapper.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error from the underlying Kafka library.
    #[error(transparent)]
    RocksDB(#[from] RocksDBError),

    /// Error when using the instance after RocksDB as errored
    /// or been shutdown.
    #[error("RocksDB thread has been shut down or errored")]
    RocksDBThreadGoneAway,

    /// Error decoding a value previously written.
    #[error("failed to decode value")]
    DecodeError(#[from] bincode::Error),

    /// A tokio thread used by the implementation panicked.
    #[error("tokio thread panicked")]
    TokioPanic(#[from] tokio::task::JoinError),
}

/// Fixed options to configure a [`RocksDBInstance`]. These are not tuning parameters,
/// see the `config` modules for tuning. These are generally fixed within the binary.
pub struct InstanceOptions {
    /// Whether or not to clear state at the instance
    /// path before starting.
    pub cleanup_on_new: bool,

    /// Whether or not to write writes
    /// to the wal. This is not in `RocksDBTuningParameters` because it
    /// applies to `WriteOptions` when creating `WriteBatch`es.
    pub use_wal: bool,

    /// A possibly shared RocksDB `Env`.
    pub env: Env,
}

impl InstanceOptions {
    /// A new `Options` object with reasonable defaults.
    pub fn defaults_with_env(env: rocksdb::Env) -> Self {
        InstanceOptions {
            cleanup_on_new: true,
            use_wal: false,
            env,
        }
    }

    fn as_rocksdb_options(
        &self,
        tuning_config: &RocksDBConfig,
    ) -> (RocksDBOptions, Option<WriteBufferManagerHandle>) {
        // Defaults + `create_if_missing`
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);

        // Set the env first so tuning applies to the shared `Env`.
        options.set_env(&self.env);

        let write_buffer_handle = config::apply_to_options(tuning_config, &mut options);
        // Returns the rocksdb options and an optional `WriteBufferManagerHandle`
        // if write buffer manager was enabled in the configs.
        (options, write_buffer_handle)
    }

    fn as_rocksdb_write_options(&self) -> WriteOptions {
        let mut wo = rocksdb::WriteOptions::new();
        wo.disable_wal(!self.use_wal);
        wo
    }
}

/// Shared metrics about an instances usage of RocksDB. User-provided
/// so the user can choose the labels.
pub struct RocksDBSharedMetrics {
    /// Latency of multi_gets, in fractional seconds.
    pub multi_get_latency: DeleteOnDropHistogram<'static, Vec<String>>,
    /// Latency of write batch writes, in fractional seconds.
    pub multi_put_latency: DeleteOnDropHistogram<'static, Vec<String>>,
}

/// Worker metrics about an instances usage of RocksDB. User-provided
/// so the user can choose the labels.
pub struct RocksDBInstanceMetrics {
    /// Size of multi_get batches.
    pub multi_get_size: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    /// Size of multi_get non-empty results.
    pub multi_get_result_count: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    /// Total size of bytes returned in the result
    pub multi_get_result_bytes: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    /// The number of calls to rocksdb multi_get
    pub multi_get_count: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    /// The number of calls to rocksdb multi_put
    pub multi_put_count: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    /// Size of write batches.
    pub multi_put_size: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
}

/// The result type for `multi_get`.
#[derive(Default, Debug)]
pub struct MultiGetResult {
    /// The number of keys we fetched.
    pub processed_gets: u64,
    /// The total size of values fetched.
    pub processed_gets_size: u64,
    /// The number of records returns.
    pub returned_gets: u64,
}

/// The result type for individual gets.
#[derive(Debug, Default, Clone)]
pub struct GetResult<V> {
    /// The previous value, if there was one.
    pub value: V,
    /// The size of `value` as persisted, if there was one.
    /// Useful for users keeping track of statistics.
    pub size: u64,
}

/// The result type for `multi_put`.
#[derive(Default, Debug)]
pub struct MultiPutResult {
    /// The number of keys we put or deleted.
    pub processed_puts: u64,
    /// The total size of values we put into the database.
    /// Does not contain any information about deletes.
    pub size_written: u64,
}

#[derive(Debug)]
enum Command<K, V> {
    MultiGet {
        batch: Vec<K>,
        // Scratch vector to return results in.
        results_scratch: Vec<Option<GetResult<V>>>,
        response_sender: oneshot::Sender<
            Result<
                (
                    MultiGetResult,
                    // The batch scratch vector being given back.
                    Vec<K>,
                    Vec<Option<GetResult<V>>>,
                ),
                Error,
            >,
        >,
    },
    MultiPut {
        batch: Vec<(K, Option<V>)>,
        // Scratch vector to return results in.
        response_sender: oneshot::Sender<Result<(MultiPutResult, Vec<(K, Option<V>)>), Error>>,
    },
    Shutdown {
        done_sender: oneshot::Sender<()>,
    },
}

/// An async wrapper around RocksDB.
pub struct RocksDBInstance<K, V> {
    tx: mpsc::Sender<Command<K, V>>,

    // Scratch vector to send keys to the RocksDB thread
    // during `MultiGet`.
    multi_get_scratch: Vec<K>,

    // Scratch vector to return results from the RocksDB thread
    // during `MultiGet`.
    multi_get_results_scratch: Vec<Option<GetResult<V>>>,

    // Scratch vector to send updates to the RocksDB thread
    // during `MultiPut`.
    multi_put_scratch: Vec<(K, Option<V>)>,

    // Configuration that can change dynamically.
    dynamic_config: config::RocksDBDynamicConfig,
}

impl<K, V> RocksDBInstance<K, V>
where
    K: AsRef<[u8]> + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Start a new RocksDB instance at the path, using
    /// the `Options` and `RocksDBTuningParameters` to
    /// configure the instance.
    ///
    /// `metrics` is a set of metric types that this type will keep
    /// up to date. `enc_opts` is the `bincode` options used to
    /// serialize and deserialize the keys and values.
    pub async fn new<M, O, IM>(
        instance_path: &Path,
        // An old path to the db that we should cleanup. Used to migrate paths.
        // TODO(guswynn): remove this once a release with it goes out.
        legacy_instance_path: &Path,
        options: InstanceOptions,
        tuning_config: RocksDBConfig,
        shared_metrics: M,
        instance_metrics: IM,
        enc_opts: O,
    ) -> Result<Self, Error>
    where
        O: bincode::Options + Copy + Send + Sync + 'static,
        M: Deref<Target = RocksDBSharedMetrics> + Send + 'static,
        IM: Deref<Target = RocksDBInstanceMetrics> + Send + 'static,
    {
        let dynamic_config = tuning_config.dynamic.clone();
        if options.cleanup_on_new && instance_path.exists() {
            let instance_path_owned = instance_path.to_owned();
            let legacy_instance_path_owned = legacy_instance_path.to_owned();
            mz_ore::task::spawn_blocking(
                || {
                    format!(
                        "RocksDB instance at {}: cleanup on creation",
                        instance_path.display()
                    )
                },
                move || {
                    if let Err(e) = DB::destroy(&RocksDBOptions::default(), &*instance_path_owned) {
                        tracing::warn!(
                            "failed to cleanup rocksdb dir on creation {}: {}",
                            instance_path_owned.display(),
                            e.display_with_causes(),
                        );
                    }
                    if let Err(e) =
                        DB::destroy(&RocksDBOptions::default(), &*legacy_instance_path_owned)
                    {
                        tracing::warn!(
                            "failed to cleanup legacy rocksdb dir on creation {}: {}",
                            legacy_instance_path_owned.display(),
                            e.display_with_causes(),
                        );
                    }
                },
            )
            .await?;
        }

        // The buffer can be small here, as all interactions with it take `&mut self`.
        let (tx, rx): (mpsc::Sender<Command<K, V>>, _) = mpsc::channel(10);

        let instance_path = instance_path.to_owned();
        let legacy_instance_path = legacy_instance_path.to_owned();
        let (creation_error_tx, creation_error_rx) = oneshot::channel();
        std::thread::spawn(move || {
            rocksdb_core_loop(
                options,
                tuning_config,
                instance_path,
                legacy_instance_path,
                rx,
                shared_metrics,
                instance_metrics,
                creation_error_tx,
                enc_opts,
            )
        });

        if let Ok(creation_error) = creation_error_rx.await {
            return Err(creation_error);
        }

        Ok(Self {
            tx,
            multi_get_scratch: Vec::new(),
            multi_get_results_scratch: Vec::new(),
            multi_put_scratch: Vec::new(),
            dynamic_config,
        })
    }

    /// For each _unique_ key in `gets`, place the stored value (if any) in `results_out`.
    ///
    /// Panics if `gets` and `results_out` are not the same length.
    pub async fn multi_get<'r, G, R, Ret, Placement>(
        &mut self,
        gets: G,
        results_out: R,
        placement: Placement,
    ) -> Result<MultiGetResult, Error>
    where
        G: IntoIterator<Item = K>,
        R: IntoIterator<Item = &'r mut Ret>,
        Ret: 'r,
        Placement: Fn(Option<GetResult<V>>) -> Ret,
    {
        let batch_size = self.dynamic_config.batch_size();
        let mut stats = MultiGetResult::default();

        let mut gets = gets.into_iter().peekable();
        if gets.peek().is_some() {
            let gets = gets.chunks(batch_size);
            let results_out = results_out.into_iter().chunks(batch_size);

            for (gets, results_out) in gets.into_iter().zip_eq(results_out.into_iter()) {
                let ret = self.multi_get_inner(gets, results_out, &placement).await?;
                stats.processed_gets += ret.processed_gets;
            }
        }

        Ok(stats)
    }

    async fn multi_get_inner<'r, G, R, Ret, Placement>(
        &mut self,
        gets: G,
        results_out: R,
        placement: &Placement,
    ) -> Result<MultiGetResult, Error>
    where
        G: IntoIterator<Item = K>,
        R: IntoIterator<Item = &'r mut Ret>,
        Ret: 'r,
        Placement: Fn(Option<GetResult<V>>) -> Ret,
    {
        let mut multi_get_vec = std::mem::take(&mut self.multi_get_scratch);
        let mut results_vec = std::mem::take(&mut self.multi_get_results_scratch);
        multi_get_vec.clear();
        results_vec.clear();

        multi_get_vec.extend(gets);
        if multi_get_vec.is_empty() {
            self.multi_get_scratch = multi_get_vec;
            self.multi_get_results_scratch = results_vec;
            return Ok(MultiGetResult {
                processed_gets: 0,
                processed_gets_size: 0,
                returned_gets: 0,
            });
        }

        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::MultiGet {
                batch: multi_get_vec,
                results_scratch: results_vec,
                response_sender: tx,
            })
            .await
            .map_err(|_| Error::RocksDBThreadGoneAway)?;

        // We also unwrap all rocksdb errors here.
        match rx.await.map_err(|_| Error::RocksDBThreadGoneAway)? {
            Ok((ret, get_scratch, mut results_scratch)) => {
                for (place, get) in results_out.into_iter().zip_eq(results_scratch.drain(..)) {
                    *place = placement(get);
                }
                self.multi_get_scratch = get_scratch;
                self.multi_get_results_scratch = results_scratch;
                Ok(ret)
            }
            Err(e) => {
                // Note we don't attempt to preserve the scratch allocations here.
                Err(e)
            }
        }
    }

    /// For each key in puts, store the given value, or delete it if
    /// the value is `None`. If the same `key` appears multiple times,
    /// the last value for the key wins.
    pub async fn multi_put<P>(&mut self, puts: P) -> Result<MultiPutResult, Error>
    where
        P: IntoIterator<Item = (K, Option<V>)>,
    {
        let batch_size = self.dynamic_config.batch_size();
        let mut stats = MultiPutResult::default();

        let mut puts = puts.into_iter().peekable();
        if puts.peek().is_some() {
            let puts = puts.chunks(batch_size);

            for puts in puts.into_iter() {
                let ret = self.multi_put_inner(puts).await?;
                stats.processed_puts += ret.processed_puts;
                stats.size_written += ret.size_written;
            }
        }

        Ok(stats)
    }

    async fn multi_put_inner<P>(&mut self, puts: P) -> Result<MultiPutResult, Error>
    where
        P: IntoIterator<Item = (K, Option<V>)>,
    {
        let mut multi_put_vec = std::mem::take(&mut self.multi_put_scratch);
        multi_put_vec.clear();

        multi_put_vec.extend(puts);
        if multi_put_vec.is_empty() {
            self.multi_put_scratch = multi_put_vec;
            return Ok(MultiPutResult {
                processed_puts: 0,
                size_written: 0,
            });
        }

        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::MultiPut {
                batch: multi_put_vec,
                response_sender: tx,
            })
            .await
            .map_err(|_| Error::RocksDBThreadGoneAway)?;

        // We also unwrap all rocksdb errors here.
        match rx.await.map_err(|_| Error::RocksDBThreadGoneAway)? {
            Ok((ret, scratch)) => {
                self.multi_put_scratch = scratch;
                Ok(ret)
            }
            Err(e) => {
                // Note we don't attempt to preserve the allocation here.
                Err(e)
            }
        }
    }

    /// Gracefully shut down RocksDB. Can error if the instance
    /// is already shut down or errored.
    pub async fn close(self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Shutdown { done_sender: tx })
            .await
            .map_err(|_| Error::RocksDBThreadGoneAway)?;

        let _ = rx.await;

        Ok(())
    }
}

fn rocksdb_core_loop<K, V, M, O, IM>(
    options: InstanceOptions,
    tuning_config: RocksDBConfig,
    instance_path: PathBuf,
    legacy_instance_path: PathBuf,
    mut cmd_rx: mpsc::Receiver<Command<K, V>>,
    shared_metrics: M,
    instance_metrics: IM,
    creation_error_tx: oneshot::Sender<Error>,
    enc_opts: O,
) where
    K: AsRef<[u8]> + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    M: Deref<Target = RocksDBSharedMetrics> + Send + 'static,
    O: bincode::Options + Copy + Send + Sync + 'static,
    IM: Deref<Target = RocksDBInstanceMetrics> + Send + 'static,
{
    let retry_max_duration = tuning_config.retry_max_duration;

    // Handle to an optional reference of a write buffer manager which
    // should be valid till the rocksdb thread is running.
    // The shared write buffer manager will be cleaned up if all
    // the handles are dropped across all the rocksdb instances.
    let (rocksdb_options, write_buffer_handle) = options.as_rocksdb_options(&tuning_config);
    tracing::info!(
        "Starting rocksdb at {:?} with write_buffer_manager: {:?}",
        instance_path,
        write_buffer_handle
    );

    let retry_result = Retry::default()
        .max_duration(retry_max_duration)
        .retry(|_| match DB::open(&rocksdb_options, &instance_path) {
            Ok(db) => RetryResult::Ok(db),
            Err(e) => match e.kind() {
                ErrorKind::TryAgain => RetryResult::RetryableErr(Error::RocksDB(e)),
                _ => RetryResult::FatalErr(Error::RocksDB(e)),
            },
        });

    let db: DB = match retry_result {
        Ok(db) => {
            drop(creation_error_tx);
            db
        }
        Err(e) => {
            // Communicate the error back to `new`.
            let _ = creation_error_tx.send(e);
            return;
        }
    };

    let mut encoded_batch_buffers: Vec<Option<Vec<u8>>> = Vec::new();
    let mut encoded_batch: Vec<(K, Option<Vec<u8>>)> = Vec::new();

    let wo = options.as_rocksdb_write_options();

    while let Some(cmd) = cmd_rx.blocking_recv() {
        match cmd {
            Command::Shutdown { done_sender } => {
                db.cancel_all_background_work(true);
                drop(db);
                drop(write_buffer_handle);
                let _ = done_sender.send(());
                return;
            }
            Command::MultiGet {
                mut batch,
                mut results_scratch,
                response_sender,
            } => {
                let batch_size = batch.len();

                // Perform the multi_get and record metrics, if there wasn't an error.
                let now = Instant::now();
                let retry_result = Retry::default()
                    .max_duration(retry_max_duration)
                    .retry(|_| {
                        let gets = db.multi_get(batch.iter());
                        let latency = now.elapsed();

                        let gets: Result<Vec<_>, _> = gets.into_iter().collect();
                        match gets {
                            Ok(gets) => {
                                shared_metrics
                                    .multi_get_latency
                                    .observe(latency.as_secs_f64());
                                instance_metrics
                                    .multi_get_size
                                    .inc_by(batch_size.try_into().unwrap());
                                instance_metrics.multi_get_count.inc();

                                RetryResult::Ok(gets)
                            }
                            Err(e) => match e.kind() {
                                ErrorKind::TryAgain => RetryResult::RetryableErr(Error::RocksDB(e)),
                                _ => RetryResult::FatalErr(Error::RocksDB(e)),
                            },
                        }
                    });

                let _ = match retry_result {
                    Ok(gets) => {
                        let processed_gets: u64 = gets.len().try_into().unwrap();
                        let mut processed_gets_size = 0;
                        let mut returned_gets: u64 = 0;
                        for previous_value in gets {
                            let get_result = match previous_value {
                                Some(previous_value) => {
                                    match enc_opts.deserialize(&previous_value) {
                                        Ok(value) => {
                                            let size = u64::cast_from(previous_value.len());
                                            processed_gets_size += size;
                                            returned_gets += 1;
                                            Some(GetResult { value, size })
                                        }
                                        Err(e) => {
                                            let _ =
                                                response_sender.send(Err(Error::DecodeError(e)));
                                            return;
                                        }
                                    }
                                }
                                None => None,
                            };
                            results_scratch.push(get_result);
                        }

                        instance_metrics
                            .multi_get_result_count
                            .inc_by(returned_gets);
                        instance_metrics
                            .multi_get_result_bytes
                            .inc_by(processed_gets_size);
                        batch.clear();
                        response_sender.send(Ok((
                            MultiGetResult {
                                processed_gets,
                                processed_gets_size,
                                returned_gets,
                            },
                            batch,
                            results_scratch,
                        )))
                    }
                    Err(e) => response_sender.send(Err(e)),
                };
            }
            Command::MultiPut {
                mut batch,
                response_sender,
            } => {
                let batch_size = batch.len();

                let mut ret = MultiPutResult {
                    processed_puts: 0,
                    size_written: 0,
                };

                // initialize and push values into the buffer to match the batch size
                let buf_size = encoded_batch_buffers.len();
                for _ in buf_size..batch_size {
                    encoded_batch_buffers.push(Some(Vec::new()));
                }
                // shrinking the buffers in case the scratch buffer's capacity is significantly
                // more than the size of batch
                if tuning_config.shrink_buffers_by_ratio > 0 {
                    let reduced_capacity =
                        encoded_batch_buffers.capacity() / tuning_config.shrink_buffers_by_ratio;
                    if reduced_capacity > batch_size {
                        encoded_batch_buffers.truncate(reduced_capacity);
                        encoded_batch_buffers.shrink_to(reduced_capacity);

                        encoded_batch.truncate(reduced_capacity);
                        encoded_batch.shrink_to(reduced_capacity);
                    }
                }
                assert!(encoded_batch_buffers.len() >= batch_size);

                // TODO(guswynn): sort by key before writing.
                for ((key, value), encode_buf) in
                    batch.drain(..).zip(encoded_batch_buffers.iter_mut())
                {
                    ret.processed_puts += 1;

                    match value {
                        Some(update) => {
                            let mut encode_buf =
                                encode_buf.take().expect("encode_buf should not be empty");
                            encode_buf.clear();
                            match enc_opts
                                .serialize_into::<&mut Vec<u8>, _>(&mut encode_buf, &update)
                            {
                                Ok(()) => ret.size_written += u64::cast_from(encode_buf.len()),
                                Err(e) => {
                                    let _ = response_sender.send(Err(Error::DecodeError(e)));
                                    return;
                                }
                            };
                            encoded_batch.push((key, Some(encode_buf)));
                        }
                        None => encoded_batch.push((key, None)),
                    }
                }
                // Perform the multi_put and record metrics, if there wasn't an error.
                let now = Instant::now();
                let retry_result = Retry::default()
                    .max_duration(retry_max_duration)
                    .retry(|_| {
                        let mut writes = rocksdb::WriteBatch::default();

                        for (key, value) in encoded_batch.iter() {
                            match value {
                                Some(update) => writes.put(key, update),
                                None => writes.delete(key),
                            }
                        }

                        match db.write_opt(writes, &wo) {
                            Ok(()) => {
                                let latency = now.elapsed();
                                shared_metrics
                                    .multi_put_latency
                                    .observe(latency.as_secs_f64());
                                instance_metrics
                                    .multi_put_size
                                    .inc_by(batch_size.try_into().unwrap());
                                instance_metrics.multi_put_count.inc();
                                RetryResult::Ok(())
                            }
                            Err(e) => match e.kind() {
                                ErrorKind::TryAgain => RetryResult::RetryableErr(Error::RocksDB(e)),
                                _ => RetryResult::FatalErr(Error::RocksDB(e)),
                            },
                        }
                    });

                // put back the values in the buffer so we don't lose allocation
                for (i, (_, encoded_buffer)) in encoded_batch.drain(..).enumerate() {
                    if let Some(encoded_buffer) = encoded_buffer {
                        encoded_batch_buffers[i] = Some(encoded_buffer);
                    }
                }

                let _ = match retry_result {
                    Ok(()) => {
                        batch.clear();
                        response_sender.send(Ok((ret, batch)))
                    }
                    Err(e) => response_sender.send(Err(e)),
                };
            }
        }
    }
    // Gracefully cleanup if the `RocksDBInstance` has gone away.
    db.cancel_all_background_work(true);
    drop(db);

    // Note that we don't retry, as we already may race here with a source being restarted.
    if let Err(e) = DB::destroy(&RocksDBOptions::default(), &*instance_path) {
        tracing::warn!(
            "failed to cleanup rocksdb dir at {}: {}",
            instance_path.display(),
            e.display_with_causes(),
        );
    }
    if let Err(e) = DB::destroy(&RocksDBOptions::default(), &*legacy_instance_path) {
        tracing::warn!(
            "failed to cleanup legacy rocksdb dir at {}: {}",
            legacy_instance_path.display(),
            e.display_with_causes(),
        );
    }
}
