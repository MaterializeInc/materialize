// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Consensus] backed by FoundationDB.
//!
//! We're storing the consensus data in a subspace at `/mz/consensus`. Each key maps to a subspace
//! with the following structure:
//! ./seqno/<key> -> <seq_no>
//! ./data/<key>/<seq_no> -> <data>

use crate::error::Error;
use crate::location::{
    CaSResult, Consensus, Determinate, ExternalError, Indeterminate, ResultStream, SeqNo,
    VersionedData,
};
use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use foundationdb::api::NetworkAutoStop;
use foundationdb::directory::{Directory, DirectoryLayer, DirectoryOutput};
use foundationdb::options::StreamingMode;
use foundationdb::tuple::{
    PackResult, TupleDepth, TuplePack, TupleUnpack, VersionstampOffset, pack, unpack,
};
use foundationdb::{
    Database, FdbBindingError, FdbError, KeySelector, RangeOption, TransactError, TransactOption,
    Transaction,
};
use futures_util::future::FutureExt;
use std::io::Write;
use std::sync::{Arc, OnceLock};

impl From<FdbError> for ExternalError {
    fn from(x: FdbError) -> Self {
        if x.is_retryable() {
            ExternalError::Indeterminate(Indeterminate::new(x.into()))
        } else {
            ExternalError::Determinate(Determinate::new(x.into()))
        }
    }
}

impl From<FdbBindingError> for ExternalError {
    fn from(x: FdbBindingError) -> Self {
        ExternalError::Determinate(Determinate::new(x.into()))
    }
}

// impl From<TransactionCommitError> for ExternalError {
//     fn from(x: TransactionCommitError) -> Self {
//         match x {
//             TransactionCommitError::Retryable(e) => {
//                 ExternalError::Indeterminate(Indeterminate::new(e.into()))
//             }
//             TransactionCommitError::NonRetryable(e) => {
//                 ExternalError::Determinate(Determinate::new(e.into()))
//             }
//         }

// impl From<DirectoryError> for ExternalError {
//     fn from(x: DirectoryError) -> Self {
//         ExternalError::Determinate(Determinate {
//             inner: anyhow::Error::new(x),
//         })
//     }
// }

static CELL: OnceLock<Arc<NetworkAutoStop>> = OnceLock::new();

pub fn get_network() -> Arc<NetworkAutoStop> {
    CELL.get_or_init(|| unsafe { foundationdb::boot() }.into())
        .clone()
}

/// Configuration to connect to a Postgres backed implementation of [Consensus].
#[derive(Clone)]
pub struct FdbConsensusConfig {
    network: Arc<NetworkAutoStop>,
}

impl std::fmt::Debug for FdbConsensusConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FdbConsensusConfig").finish_non_exhaustive()
    }
}

// impl From<FdbConsensusConfig> for PostgresClientConfig {
//     fn from(config: FdbConsensusConfig) -> Self {
//         let network = unsafe { foundationdb::boot() };
//         PostgresClientConfig::new(config.url, config.knobs, config.metrics)
//     }
// }

impl FdbConsensusConfig {
    /// Returns a new [FdbConsensusConfig] for use in production.
    pub fn new(network: Arc<NetworkAutoStop>) -> Result<Self, Error> {
        Ok(FdbConsensusConfig { network })
    }

    pub fn new_for_test() -> Result<Self, Error> {
        let network = unsafe { foundationdb::boot() };
        Self::new(network.into())
    }

    pub fn get_network() -> Arc<NetworkAutoStop> {
        get_network()
    }
}

/// Implementation of [Consensus] over a Postgres database.
pub struct FdbConsensus {
    // content_subspace: DirectoryOutput,
    seqno: DirectoryOutput,
    data: DirectoryOutput,
    db: Database,
    _network: Arc<NetworkAutoStop>,
}

enum FdbTransactError {
    FdbError(FdbError),
    ExternalError(ExternalError),
}

impl From<FdbError> for FdbTransactError {
    fn from(value: FdbError) -> Self {
        Self::FdbError(value)
    }
}

impl From<ExternalError> for FdbTransactError {
    fn from(value: ExternalError) -> Self {
        Self::ExternalError(value)
    }
}

impl From<FdbTransactError> for ExternalError {
    fn from(value: FdbTransactError) -> Self {
        match value {
            FdbTransactError::FdbError(e) => e.into(),
            FdbTransactError::ExternalError(e) => e,
        }
    }
}

impl TransactError for FdbTransactError {
    fn try_into_fdb_error(self) -> Result<FdbError, Self> {
        match self {
            Self::FdbError(e) => Ok(e),
            other => Err(other),
        }
    }
}

impl TuplePack for SeqNo {
    fn pack<W: Write>(
        &self,
        w: &mut W,
        tuple_depth: TupleDepth,
    ) -> std::io::Result<VersionstampOffset> {
        self.0.pack(w, tuple_depth)
    }
}

impl<'de> TupleUnpack<'de> for SeqNo {
    fn unpack(input: &'de [u8], tuple_depth: TupleDepth) -> PackResult<(&'de [u8], Self)> {
        u64::unpack(input, tuple_depth).map(|(rem, v)| (rem, SeqNo(v)))
    }
}

impl std::fmt::Debug for FdbConsensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FdbConsensus").finish_non_exhaustive()
    }
}

impl FdbConsensus {
    /// Open a Postgres [Consensus] instance with `config`, for the collection
    /// named `shard`.
    pub async fn open(config: FdbConsensusConfig) -> Result<Self, ExternalError> {
        let db = Database::new(None)?;
        let directory = DirectoryLayer::default();
        let path = vec!["seqno".to_owned()];
        let seqno = db
            .run(async |trx, _maybe_commited| {
                Ok(directory.create_or_open(&trx, &path, None, None).await)
            })
            .await?
            .expect("valid directory");
        let path = vec!["data".to_owned()];
        let data = db
            .run(async |trx, _maybe_commited| {
                Ok(directory.create_or_open(&trx, &path, None, None).await)
            })
            .await?
            .expect("valid directory");
        Ok(FdbConsensus {
            seqno,
            data,
            db,
            _network: config.network,
        })
    }

    /// Drops and recreates the `consensus` table in Postgres
    ///
    /// ONLY FOR TESTING
    pub async fn drop_and_recreate(&self) -> Result<(), ExternalError> {
        self.db
            .run(async |trx, _maybe_commited| {
                self.seqno.remove(&trx, &[]).await?;
                self.data.remove(&trx, &[]).await?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn head_trx(
        &self,
        trx: &Transaction,
        key: &str,
    ) -> Result<Option<VersionedData>, FdbTransactError> {
        let seqno_key = self.seqno.subspace(&key).expect("valid directory");
        let data_key = self.data.subspace(&key).expect("valid directory");

        let seq_no = trx.get(seqno_key.bytes(), false).await?;
        if let Some(seq_no) = &seq_no {
            // println!("key: {key}, seqno bytes: {:?}", seq_no);
            let seqno: SeqNo = unpack(seq_no).expect("valid data");

            let seq_no_space = data_key.pack(&seqno);
            let data = trx.get(&seq_no_space, false).await?.expect("valid data");
            let data = unpack::<Vec<u8>>(&data).expect("valid data");
            // println!(
            //     "key: {key}, key_value bytes: {} seqno: {:?}",
            //     data.len(),
            //     seqno
            // );
            Ok(Some(VersionedData {
                seqno,
                data: Bytes::from(data),
            }))
        } else {
            Ok(None)
        }
    }
    async fn compare_and_set_trx(
        &self,
        trx: &Transaction,
        key: &str,
        expected: &Option<SeqNo>,
        new: &VersionedData,
    ) -> Result<CaSResult, FdbTransactError> {
        let seqno_key = self.seqno.subspace(&key).expect("valid directory");
        let data_key = self.data.subspace(&key).expect("valid directory");

        let seq_no = trx
            .get(seqno_key.bytes(), false)
            .await?
            .map(|data| unpack(&data).expect("valid data"));

        if expected.is_some() && (expected != &seq_no) {
            return Ok(CaSResult::ExpectationMismatch);
        }

        trx.set(seqno_key.bytes(), &pack(&new.seqno));
        // println!(
        //     "cas seqno_key:      {:?}",
        //     foundationdb::tuple::Bytes::from(seqno_key.bytes())
        // );

        let data_seqno_key = data_key.pack(&new.seqno);
        trx.set(&data_seqno_key, &pack(&new.data.as_ref()));
        // println!(
        //     "cas data_seqno_key: {:?}",
        //     foundationdb::tuple::Bytes::from(data_seqno_key.bytes())
        // );
        let written = trx.get(&data_seqno_key, false).await?;
        let unpacked: Vec<u8> = unpack(&written.unwrap()).expect("valid data");
        assert_eq!(&*unpacked, &*new.data);
        Ok(CaSResult::Committed)
    }
    async fn scan_trx(
        &self,
        trx: &Transaction,
        key: &str,
        from: &SeqNo,
        limit: &usize,
    ) -> Result<Vec<VersionedData>, FdbTransactError> {
        let mut limit = *limit;
        let data_key = self.data.subspace(&key).expect("valid directory");
        let seqno_start = data_key.pack(&from);
        let seqno_end = data_key.pack(&0xff);

        // let output = trx.get_range(&data_key.range().into(), 1, false).await?;
        // for key_value in &output {
        //     println!("entry: all                              {:?}", key_value);
        // }

        let mut range = RangeOption::from((seqno_start, seqno_end));
        range.limit = Some(limit);

        let mut entries = Vec::new();

        // println!("Scanning range begin: {:?}", range.begin);
        // println!("Scanning range   end: {:?}", range.end);

        loop {
            let output = trx.get_range(&range, 1, false).await?;
            for key_value in &output {
                // println!("entry:                                  {:?}", key_value);
                let seqno = data_key.unpack(key_value.key()).expect("valid data");
                let value: Vec<u8> = unpack(key_value.value()).expect("valid data");
                // println!(
                //     "key: {key}, key_value bytes: {} seqno: {:?}",
                //     value.len(),
                //     seqno
                // );
                entries.push(VersionedData {
                    seqno: seqno,
                    data: Bytes::from(value),
                });
            }

            limit = limit.saturating_sub(output.len());

            if let Some(last) = output.last()
                && limit > 0
            {
                range.begin = KeySelector::first_greater_than(last.key().to_vec());
                range.limit = Some(limit);
            } else {
                break;
            }
        }

        entries.sort_by_key(|e| e.seqno);
        Ok(entries)
    }
    async fn truncate_trx(
        &self,
        trx: &Transaction,
        key: &str,
        seqno: &SeqNo,
    ) -> Result<(), FdbTransactError> {
        let seqno_key = self.seqno.subspace(&key).expect("valid directory");

        let seq_no = trx.get(seqno_key.bytes(), false).await?;
        if let Some(seq_no) = &seq_no {
            let current_seqno: SeqNo = unpack(seq_no).expect("valid data");
            if current_seqno < *seqno {
                return Err(ExternalError::Determinate(
                    anyhow!("upper bound too high for truncate: {:?}", seqno).into(),
                )
                .into());
            }
        } else {
            return Err(
                ExternalError::Determinate(anyhow!("no entries for key: {}", key).into()).into(),
            );
        }
        let data_key = self.data.subspace(&key).expect("valid directory");
        let key_space_start = data_key.pack(&SeqNo::minimum());
        let key_space_end = data_key.pack(&seqno);

        trx.clear_range(&key_space_start, &key_space_end);
        Ok(())
    }
}

#[async_trait]
impl Consensus for FdbConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        Box::pin(try_stream! {
            let keys: Vec<String> = self
                .db
                .run(async |trx, _maybe_commited| {
                    let mut range = RangeOption::from(self.seqno.range().expect("valid directory"));
                    let mut keys = Vec::new();
                    loop {
                        let values = trx.get_range(&range, 1, false).await?;
                        for value in &values {
                            // println!("entry: {:?}", value);
                            let key: String = self.seqno.unpack(value.key()).expect("valid directory").expect("valid data");
                            keys.push(key);
                        }
                        if let Some(last) = values.last() {
                            range.begin = KeySelector::first_greater_than(last.key().to_vec());
                        } else {
                            break;
                        }
                    }
                    Ok(keys)
                }).await?;

            for shard in keys {
                yield shard;
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        // info!("FdbConsensus::head({})", key);
        let ok = self
            .db
            .transact_boxed(
                key,
                |trx, key| self.head_trx(trx, key).boxed(),
                TransactOption::default(),
            )
            .await?;
        // info!(
        //     "FdbConsensus::head({}) -> {:?}",
        //     key,
        //     ok.as_ref().map(|ok| ok.seqno)
        // );
        Ok(ok)
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        // info!(
        //     "FdbConsensus::compare_and_set({}, {:?}, <{} bytes at seqno {}>)",
        //     key,
        //     expected,
        //     new.data.len(),
        //     new.seqno
        // );
        if let Some(expected) = expected {
            if new.seqno <= expected {
                return Err(Error::from(
                    format!("new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                            new.seqno, expected)).into());
            }
        }
        if new.seqno.0 > i64::MAX.try_into().expect("i64::MAX known to fit in u64") {
            return Err(ExternalError::from(anyhow!(
                "sequence numbers must fit within [0, i64::MAX], received: {:?}",
                new.seqno
            )));
        }

        let ok = self
            .db
            .transact_boxed(
                (key, expected, &new),
                |trx, (key, expected, new)| {
                    self.compare_and_set_trx(trx, key, expected, new).boxed()
                },
                TransactOption::default(),
            )
            .await?;
        Ok(ok)
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        // info!("FdbConsensus::scan({}, {:?}, {})", key, from, limit);
        let ok = self
            .db
            .transact_boxed(
                (key, from, limit),
                |trx, (key, from, limit)| self.scan_trx(trx, key, from, limit).boxed(),
                TransactOption::default(),
            )
            .await?;
        Ok(ok)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
        // info!("FdbConsensus::truncate({}, {:?})", key, seqno);
        self.db
            .transact_boxed(
                (key, seqno),
                |trx, (key, seqno)| self.truncate_trx(trx, key, seqno).boxed(),
                TransactOption::default(),
            )
            .await?;
        Ok(0)
    }

    fn truncate_counts(&self) -> bool {
        false
    }
}

#[derive(Debug)]
pub struct ValidatingConsensus<A, B> {
    pub inner: A,
    pub validator: B,
}

#[async_trait]
impl<A: Consensus, B: Consensus> Consensus for ValidatingConsensus<A, B> {
    fn list_keys(&self) -> ResultStream<'_, String> {
        self.inner.list_keys()
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let inner = self.inner.head(key).await?;
        let valid = self.validator.head(key).await?;
        assert_eq!(inner, valid, "mismatched head for key {}", key);
        Ok(inner)
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let inner = self
            .inner
            .compare_and_set(key, expected.clone(), new.clone())
            .await?;
        let valid = self.validator.compare_and_set(key, expected, new).await?;
        assert_eq!(inner, valid, "mismatched cas for key {}", key);
        Ok(inner)
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let inner = self.inner.scan(key, from, limit).await?;
        let valid = self.validator.scan(key, from, limit).await?;
        for inner in &inner {
            println!(
                "inner scan: seqno: {:?}, {} bytes",
                inner.seqno,
                inner.data.len()
            );
        }
        for valid in &valid {
            println!(
                "valid scan: seqno: {:?}, {} bytes",
                valid.seqno,
                valid.data.len()
            );
        }
        for (a, b) in inner.iter().zip(valid.iter()) {
            assert_eq!(a.seqno, b.seqno);
            assert_eq!(&*a.data, &*b.data);
        }
        assert_eq!(inner.len(), valid.len());
        Ok(inner)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
        let inner = self.inner.truncate(key, seqno).await?;
        let valid = self.validator.truncate(key, seqno).await?;
        self.scan(key, SeqNo::minimum(), 100000000).await?;
        if self.truncate_counts() {
            assert_eq!(inner, valid, "mismatched truncate counts for key {}", key);
        }
        Ok(inner)
    }

    fn truncate_counts(&self) -> bool {
        self.inner.truncate_counts() && self.validator.truncate_counts()
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::location::tests::consensus_impl_test;

    use super::*;

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn fdb_consensus() -> Result<(), ExternalError> {
        let config = FdbConsensusConfig::new_for_test()?;

        {
            let fdb = FdbConsensus::open(config.clone()).await?;
            fdb.drop_and_recreate().await?;
        }

        consensus_impl_test(|| FdbConsensus::open(config.clone())).await?;

        // and now verify the implementation-specific `drop_and_recreate` works as intended
        let consensus = FdbConsensus::open(config.clone()).await?;
        let key = Uuid::new_v4().to_string();
        let mut state = VersionedData {
            seqno: SeqNo(5),
            data: Bytes::from("abc"),
        };

        assert_eq!(
            consensus.compare_and_set(&key, None, state.clone()).await,
            Ok(CaSResult::Committed),
        );
        state.seqno = SeqNo(6);
        assert_eq!(
            consensus
                .compare_and_set(&key, Some(SeqNo(5)), state.clone())
                .await,
            Ok(CaSResult::Committed),
        );
        state.seqno = SeqNo(129 + 5);
        assert_eq!(
            consensus
                .compare_and_set(&key, Some(SeqNo(6)), state.clone())
                .await,
            Ok(CaSResult::Committed),
        );

        assert_eq!(consensus.head(&key).await, Ok(Some(state.clone())));

        println!("--- SCANNING ---");

        for data in consensus.scan(&key, SeqNo(129), 10).await? {
            println!(
                "scan data: seqno: {:?}, {} bytes",
                data.seqno,
                data.data.len()
            );
        }

        consensus.drop_and_recreate().await?;

        assert_eq!(consensus.head(&key).await, Ok(None));

        Ok(())
    }
}
