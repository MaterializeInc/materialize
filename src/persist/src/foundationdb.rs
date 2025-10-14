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
//! ./seqno/<key> -> <seqno>
//! ./data/<key>/<seqno> -> <data>

use std::io::Write;
use std::sync::OnceLock;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use foundationdb::api::NetworkAutoStop;
use foundationdb::directory::{
    Directory, DirectoryError, DirectoryLayer, DirectoryOutput, DirectorySubspace,
};
use foundationdb::tuple::{
    PackError, PackResult, Subspace, TupleDepth, TuplePack, TupleUnpack, VersionstampOffset, pack,
    unpack,
};
use foundationdb::{
    Database, FdbBindingError, FdbError, KeySelector, RangeOption, TransactError, TransactOption,
    Transaction,
};
use futures_util::future::FutureExt;
use mz_ore::url::SensitiveUrl;
use url::Url;

use crate::error::Error;
use crate::location::{
    CaSResult, Consensus, Determinate, ExternalError, Indeterminate, ResultStream, SeqNo,
    VersionedData,
};

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

/// FoundationDB network singleton.
///
/// Normally, we'd need to drop this to clean up the network, but since we
/// never expect to exit normally, it's fine to leak it.
static FDB_NETWORK: OnceLock<NetworkAutoStop> = OnceLock::new();

fn init_network() -> &'static NetworkAutoStop {
    FDB_NETWORK.get_or_init(|| unsafe { foundationdb::boot() })
}

/// Configuration to connect to a FoundationDB backed implementation of [Consensus].
#[derive(Clone, Debug)]
pub struct FdbConsensusConfig {
    url: SensitiveUrl,
}

impl FdbConsensusConfig {
    /// Returns a new [FdbConsensusConfig] for use in production.
    pub fn new(url: SensitiveUrl) -> Result<Self, Error> {
        Ok(FdbConsensusConfig { url })
    }

    pub fn new_for_test() -> Result<Self, Error> {
        Self::new(SensitiveUrl(Url::parse("foundationdb:").unwrap()))
    }
}

/// Implementation of [Consensus] over a Foundation database.
pub struct FdbConsensus {
    seqno: DirectorySubspace,
    data: DirectorySubspace,
    db: Database,
}

/// An error that can occur during a FoundationDB transaction.
/// This is either a FoundationDB error or an external error.
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

impl From<PackError> for FdbTransactError {
    fn from(value: PackError) -> Self {
        ExternalError::Determinate(anyhow::Error::new(value).into()).into()
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

impl From<DirectoryError> for ExternalError {
    fn from(e: DirectoryError) -> Self {
        ExternalError::Determinate(anyhow!("directory error: {e:?}").into())
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
        f.debug_struct("FdbConsensus")
            .field("seqno", &self.seqno)
            .field("data", &self.data)
            .finish_non_exhaustive()
    }
}

impl FdbConsensus {
    /// Open a FoundationDB [Consensus] instance with `config`.
    pub async fn open(config: FdbConsensusConfig) -> Result<Self, ExternalError> {
        let mut prefix = Vec::new();
        for (key, value) in config.url.query_pairs() {
            match &*key {
                "options" => {
                    if let Some(path) = value.strip_prefix("--search_path=") {
                        prefix = path.split('/').map(|s| s.to_owned()).collect();
                    } else {
                        return Err(ExternalError::from(anyhow!(
                            "unrecognized FoundationDB URL options parameter: {value}",
                        )));
                    }
                }
                key => {
                    return Err(ExternalError::from(anyhow!(
                        "unrecognized FoundationDB URL query parameter: {key}: {value}",
                    )));
                }
            }
        }
        let path = if config.url.0.cannot_be_a_base() {
            None
        } else {
            Some(config.url.0.path())
        };

        let _ = init_network();

        let db = Database::new(path)?;
        let directory = DirectoryLayer::default();
        let path: Vec<_> = prefix
            .iter()
            .cloned()
            .chain(std::iter::once("seqno".to_owned()))
            .collect();
        let seqno = db
            .run(async |trx, _maybe_commited| {
                Ok(directory.create_or_open(&trx, &path, None, None).await)
            })
            .await??;
        let seqno = match seqno {
            DirectoryOutput::DirectorySubspace(subspace) => subspace,
            DirectoryOutput::DirectoryPartition(_partition) => {
                return Err(ExternalError::from(anyhow!(
                    "consensus seqno cannot be a partition"
                )));
            }
        };
        let path: Vec<_> = prefix
            .into_iter()
            .chain(std::iter::once("data".to_owned()))
            .collect();
        let data = db
            .run(async |trx, _maybe_commited| {
                Ok(directory.create_or_open(&trx, &path, None, None).await)
            })
            .await??;
        let data = match data {
            DirectoryOutput::DirectorySubspace(subspace) => subspace,
            DirectoryOutput::DirectoryPartition(_partition) => {
                return Err(ExternalError::from(anyhow!(
                    "consensus data cannot be a partition"
                )));
            }
        };
        Ok(FdbConsensus { seqno, data, db })
    }

    /// Drops and recreates the `consensus` data in FoundationDB.
    ///
    /// ONLY FOR TESTING
    #[cfg(test)]
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
        seqno_key: &Subspace,
        data_key: &Subspace,
    ) -> Result<Option<VersionedData>, FdbTransactError> {
        let seqno = trx.get(seqno_key.bytes(), false).await?;
        if let Some(seqno) = &seqno {
            let seqno: SeqNo = unpack(seqno)?;

            let seqno_space = data_key.pack(&seqno);
            let data = trx.get(&seqno_space, false).await?;
            if let Some(data) = data {
                let data = unpack::<Vec<u8>>(&data)?;
                Ok(Some(VersionedData {
                    seqno,
                    data: Bytes::from(data),
                }))
            } else {
                Err(ExternalError::Determinate(
                    anyhow!("inconsistent state: seqno present without data").into(),
                )
                .into())
            }
        } else {
            Ok(None)
        }
    }
    async fn compare_and_set_trx(
        &self,
        trx: &Transaction,
        seqno_key: &Subspace,
        data_key: &Subspace,
        expected: &Option<SeqNo>,
        new: &VersionedData,
    ) -> Result<CaSResult, FdbTransactError> {
        let seqno = trx
            .get(seqno_key.bytes(), false)
            .await?
            .map(|data| unpack(&data))
            .transpose()?;

        if expected != &seqno {
            return Ok(CaSResult::ExpectationMismatch);
        }

        trx.set(seqno_key.bytes(), &pack(&new.seqno));

        let data_seqno_key = data_key.pack(&new.seqno);
        trx.set(&data_seqno_key, &pack(&new.data.as_ref()));
        Ok(CaSResult::Committed)
    }
    async fn scan_trx(
        &self,
        trx: &Transaction,
        data_key: &Subspace,
        from: &SeqNo,
        limit: &usize,
    ) -> Result<Vec<VersionedData>, FdbTransactError> {
        let mut limit = *limit;
        let seqno_start = data_key.pack(&from);
        let seqno_end = data_key.pack(&SeqNo::maximum());

        let mut range = RangeOption::from(seqno_start..=seqno_end);
        range.limit = Some(limit);

        let mut entries = Vec::new();

        loop {
            let output = trx.get_range(&range, 1, false).await?;
            for key_value in &output {
                let seqno = data_key.unpack(key_value.key())?;
                let value: Vec<u8> = unpack(key_value.value())?;
                entries.push(VersionedData {
                    seqno,
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
        seqno_key: &Subspace,
        data_key: &Subspace,
        until: &SeqNo,
    ) -> Result<(), FdbTransactError> {
        let seqno = trx.get(seqno_key.bytes(), false).await?;
        if let Some(seqno) = &seqno {
            let current_seqno: SeqNo = unpack(seqno)?;
            if current_seqno < *until {
                return Err(ExternalError::Determinate(
                    anyhow!("upper bound too high for truncate: {until}").into(),
                )
                .into());
            }
        } else {
            return Err(ExternalError::Determinate(anyhow!("no entries for key").into()).into());
        }
        let key_space_start = data_key.pack(&SeqNo::minimum());
        let key_space_end = data_key.pack(&until);

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
                    let mut range = RangeOption::from(self.seqno.range());
                    let mut keys = Vec::new();
                    loop {
                        let values = trx.get_range(&range, 1, false).await?;
                        for value in &values {
                            let key: String = self.seqno.unpack(value.key()).map_err(FdbBindingError::PackError)?;
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
        let seqno_key = self.seqno.subspace(&key);
        let data_key = self.data.subspace(&key);

        let ok = self
            .db
            .transact_boxed(
                (&seqno_key, &data_key),
                |trx, (seqno_key, data_key)| self.head_trx(trx, seqno_key, data_key).boxed(),
                TransactOption::default(),
            )
            .await?;
        Ok(ok)
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
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

        let seqno_key = self.seqno.subspace(&key);
        let data_key = self.data.subspace(&key);

        let ok = self
            .db
            .transact_boxed(
                (expected, &new),
                |trx, (expected, new)| {
                    self.compare_and_set_trx(trx, &seqno_key, &data_key, expected, new)
                        .boxed()
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
        let data_key = self.data.subspace(&key);
        let ok = self
            .db
            .transact_boxed(
                (&data_key, from, limit),
                |trx, (data_key, from, limit)| self.scan_trx(trx, data_key, from, limit).boxed(),
                TransactOption::default(),
            )
            .await?;
        Ok(ok)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
        let seqno_key = self.seqno.subspace(&key);
        let data_key = self.data.subspace(&key);

        self.db
            .transact_boxed(
                (&seqno_key, &data_key, seqno),
                |trx, (seqno_key, data_key, seqno)| {
                    self.truncate_trx(trx, seqno_key, data_key, seqno).boxed()
                },
                TransactOption::idempotent(),
            )
            .await?;
        Ok(0)
    }

    fn truncate_counts(&self) -> bool {
        false
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
