// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

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
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::DeleteOnDropHistogram;
use mz_ore::retry::{Retry, RetryResult};
use rocksdb::{Env, Error as RocksDBError, ErrorKind, Options as RocksDBOptions, WriteOptions, DB};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

pub mod config;
pub use config::{defaults, RocksDBConfig, RocksDBTuningParameters};

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

    fn as_rocksdb_options(&self, tuning_config: &RocksDBConfig) -> RocksDBOptions {
        // Defaults + `create_if_missing`
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);

        // Set the env first so tuning applies to the shared `Env`.
        options.set_env(&self.env);

        config::apply_to_options(tuning_config, &mut options);

        options
    }

    fn as_rocksdb_write_options(&self) -> WriteOptions {
        let mut wo = rocksdb::WriteOptions::new();
        wo.disable_wal(!self.use_wal);
        wo
    }
}

/// Metrics about an instances usage of RocksDB. User-provided
/// so the user can choose the labels.
pub struct RocksDBMetrics {
    /// Latency of multi_gets, in fractional seconds.
    pub multi_get_latency: DeleteOnDropHistogram<'static, Vec<String>>,
    /// Size of multi_get batches.
    pub multi_get_size: DeleteOnDropHistogram<'static, Vec<String>>,
    /// Size of multi_get non-empty results.
    pub multi_get_result_size: DeleteOnDropHistogram<'static, Vec<String>>,
    /// Latency of write batch writes, in fractional seconds.
    pub multi_put_latency: DeleteOnDropHistogram<'static, Vec<String>>,
    /// Size of write batches.
    pub multi_put_size: DeleteOnDropHistogram<'static, Vec<String>>,
}

/// The result type for `multi_get`.
#[derive(Default, Debug)]
pub struct MultiGetResult {
    /// The number of keys we fetched.
    pub processed_gets: u64,
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
    pub async fn new<M, O>(
        instance_path: &Path,
        options: InstanceOptions,
        tuning_config: RocksDBConfig,
        metrics: M,
        enc_opts: O,
    ) -> Result<Self, Error>
    where
        O: bincode::Options + Copy + Send + Sync + 'static,
        M: Deref<Target = RocksDBMetrics> + Send + 'static,
    {
        let dynamic_config = tuning_config.dynamic.clone();
        if options.cleanup_on_new && instance_path.exists() {
            let instance_path_owned = instance_path.to_owned();
            mz_ore::task::spawn_blocking(
                || {
                    format!(
                        "RocksDB instance at {}: cleanup on creation",
                        instance_path.display()
                    )
                },
                move || {
                    DB::destroy(&RocksDBOptions::default(), instance_path_owned).unwrap();
                },
            )
            .await?;
        }

        // The buffer can be small here, as all interactions with it take `&mut self`.
        let (tx, rx): (mpsc::Sender<Command<K, V>>, _) = mpsc::channel(10);

        let instance_path = instance_path.to_owned();

        let (creation_error_tx, creation_error_rx) = oneshot::channel();
        std::thread::spawn(move || {
            rocksdb_core_loop(
                options,
                tuning_config,
                instance_path,
                rx,
                metrics,
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
            return Ok(MultiGetResult { processed_gets: 0 });
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

fn rocksdb_core_loop<K, V, M, O>(
    options: InstanceOptions,
    tuning_config: RocksDBConfig,
    instance_path: PathBuf,
    mut cmd_rx: mpsc::Receiver<Command<K, V>>,
    metrics: M,
    creation_error_tx: oneshot::Sender<Error>,
    enc_opts: O,
) where
    K: AsRef<[u8]> + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    M: Deref<Target = RocksDBMetrics> + Send + 'static,
    O: bincode::Options + Copy + Send + Sync + 'static,
{
    let retry_max_duration = tuning_config.retry_max_duration;
    let rocksdb_options = options.as_rocksdb_options(&tuning_config);

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
                                metrics.multi_get_latency.observe(latency.as_secs_f64());
                                metrics.multi_get_size.observe(f64::cast_lossy(batch_size));
                                let multi_get_result_size =
                                    gets.iter().filter(|r| r.is_some()).count();
                                metrics
                                    .multi_get_result_size
                                    .observe(f64::cast_lossy(multi_get_result_size));
                                let result = MultiGetResult {
                                    processed_gets: gets.len().try_into().unwrap(),
                                };

                                RetryResult::Ok((result, gets))
                            }
                            Err(e) => match e.kind() {
                                ErrorKind::TryAgain => RetryResult::RetryableErr(Error::RocksDB(e)),
                                _ => RetryResult::FatalErr(Error::RocksDB(e)),
                            },
                        }
                    });

                let _ = match retry_result {
                    Ok((result, gets)) => {
                        for previous_value in gets {
                            let get_result = match previous_value {
                                Some(previous_value) => {
                                    match enc_opts.deserialize(&previous_value) {
                                        Ok(value) => Some(GetResult {
                                            value,
                                            size: u64::cast_from(previous_value.len()),
                                        }),
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
                        batch.clear();
                        response_sender.send(Ok((result, batch, results_scratch)))
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
                                metrics.multi_put_latency.observe(latency.as_secs_f64());
                                metrics.multi_put_size.observe(f64::cast_lossy(batch_size));
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
}
