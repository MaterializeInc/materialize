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
//! We're storing the consensus data in a subspace. Each key maps to a subspace
//! with the following structure:
//! * `./keys/<key> -> ()` to track existing keys.
//! * `./data/<key> -> <seqno>` mapping to the latest seqno for the key.
//! * `./data/<key>/<seqno> -> <data>` mapping seqnos to data blobs.

use std::io::Write;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
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
use mz_foundationdb::{FdbConfig, init_network};
use mz_ore::url::SensitiveUrl;

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
}

/// Implementation of [Consensus] over a Foundation database.
pub struct FdbConsensus {
    /// Subspace for data.
    keys: DirectorySubspace,
    /// Subspace for data.
    data: DirectorySubspace,
    /// The FoundationDB database handle.
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
            .field("keys", &self.keys)
            .field("data", &self.data)
            .finish_non_exhaustive()
    }
}

impl FdbConsensus {
    /// Open a FoundationDB [Consensus] instance with `config`.
    pub async fn open(config: FdbConsensusConfig) -> Result<Self, ExternalError> {
        let fdb_config =
            FdbConfig::parse(&config.url).map_err(|e| ExternalError::Determinate(e.into()))?;

        let _ = init_network();

        let db = Database::new(None)?;
        let directory = DirectoryLayer::default();
        let keys_path: Vec<_> = fdb_config
            .prefix
            .iter()
            .cloned()
            .chain(std::iter::once("keys".to_owned()))
            .collect();
        let keys = Self::open_directory(&db, &directory, &keys_path).await?;
        let data_path: Vec<_> = fdb_config
            .prefix
            .into_iter()
            .chain(std::iter::once("data".to_owned()))
            .collect();
        let data = Self::open_directory(&db, &directory, &data_path).await?;
        Ok(FdbConsensus { keys, data, db })
    }

    /// Opens (or creates) a directory at the specified path. Errors if the
    /// directory is a partition, or cannot be opened for another reason..
    async fn open_directory(
        db: &Database,
        directory: &DirectoryLayer,
        path: &[String],
    ) -> Result<DirectorySubspace, ExternalError> {
        let directory = db
            .run(async |trx, _maybe_commited| {
                Ok(directory.create_or_open(&trx, path, None, None).await)
            })
            .await??;
        match directory {
            DirectoryOutput::DirectorySubspace(subspace) => Ok(subspace),
            DirectoryOutput::DirectoryPartition(_partition) => Err(ExternalError::from(anyhow!(
                "consensus data cannot be a partition"
            ))),
        }
    }

    async fn head_trx(
        &self,
        trx: &Transaction,
        data_key: &Subspace,
    ) -> Result<Option<VersionedData>, FdbTransactError> {
        let mut range = RangeOption::from(data_key).rev();
        range.limit = Some(1);
        range.mode = foundationdb::options::StreamingMode::Exact;
        // Allow snapshot reads as we don't need the latest data for head, just some recent data.
        let values = trx.get_range(&range, 1, true).await?;
        if let Some(kv) = values.first() {
            let seqno = data_key.unpack(kv.key())?;
            Ok(Some(VersionedData {
                seqno,
                data: Bytes::from(kv.value().to_vec()),
            }))
        } else {
            Ok(None)
        }
    }
    async fn compare_and_set_trx(
        &self,
        trx: &Transaction,
        data_key: &Subspace,
        expected: &Option<SeqNo>,
        new: &VersionedData,
        key: &str,
    ) -> Result<CaSResult, FdbTransactError> {
        let seqno = trx
            .get(data_key.bytes(), false)
            .await?
            .map(|data| unpack(&data))
            .transpose()?;

        if expected != &seqno {
            return Ok(CaSResult::ExpectationMismatch);
        }

        trx.set(data_key.bytes(), &pack(&new.seqno));

        if expected.is_none() {
            // If expected is `None`, it's a new key which we need to register in the keys directory.
            let key = self.keys.pack(&key);
            trx.set(&key, &[]);
        }

        let data_seqno_key = data_key.pack(&new.seqno);
        trx.set(&data_seqno_key, new.data.as_ref());
        Ok(CaSResult::Committed)
    }

    async fn scan_trx(
        &self,
        trx: &Transaction,
        data_key: &Subspace,
        from: &SeqNo,
        limit: &usize,
        entries: &mut Vec<VersionedData>,
    ) -> Result<(), FdbTransactError> {
        let seqno_start = data_key.pack(&from);
        let seqno_end = data_key.pack(&SeqNo::maximum());

        let mut range = RangeOption::from(seqno_start..=seqno_end);
        range.limit = Some(*limit);

        entries.clear();

        loop {
            let output = trx.get_range(&range, 1, false).await?;
            entries.reserve(output.len());
            for key_value in &output {
                let seqno = data_key.unpack(key_value.key())?;
                entries.push(VersionedData {
                    seqno,
                    data: Bytes::from(key_value.value().to_vec()),
                });
            }

            if let Some(next_range) = range.next_range(&output) {
                range = next_range;
            } else {
                break;
            }
        }
        Ok(())
    }

    // TODO: The current implementation doesn't clean up `keys` when removing the last seqno.
    async fn truncate_trx(
        &self,
        trx: &Transaction,
        data_key: &Subspace,
        until: &SeqNo,
    ) -> Result<(), FdbTransactError> {
        let seqno = trx.get(data_key.bytes(), false).await?;
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
                    let mut range = RangeOption::from(self.keys.range());
                    let mut keys = Vec::new();
                    loop {
                        let values = trx.get_range(&range, 1, false).await?;
                        for value in &values {
                            let key: String = self.keys.unpack(value.key()).map_err(FdbBindingError::PackError)?;
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
        let data_key = self.data.subspace(&key);

        let ok = self
            .db
            .transact_boxed(
                &data_key,
                |trx, data_key| self.head_trx(trx, data_key).boxed(),
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

        let data_key = self.data.subspace(&key);

        let ok = self
            .db
            .transact_boxed(
                (expected, &new, &*key),
                |trx, (expected, new, key)| {
                    self.compare_and_set_trx(trx, &data_key, expected, new, key)
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
        let mut entries = Vec::new();
        self.db
            .transact_boxed(
                (&data_key, from, limit, &mut entries),
                |trx, (data_key, from, limit, entries)| {
                    self.scan_trx(trx, data_key, from, limit, entries).boxed()
                },
                TransactOption::default(),
            )
            .await?;

        entries.sort_by_key(|e| e.seqno);
        Ok(entries)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        let data_key = self.data.subspace(&key);

        self.db
            .transact_boxed(
                (&data_key, seqno),
                |trx, (data_key, seqno)| self.truncate_trx(trx, data_key, seqno).boxed(),
                TransactOption::idempotent(),
            )
            .await?;
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use foundationdb::directory::Directory;
    use uuid::Uuid;

    use crate::location::tests::consensus_impl_test;

    /// Drops and recreates the `consensus` data in FoundationDB.
    ///
    /// ONLY FOR TESTING
    async fn drop_and_recreate(consensus: &FdbConsensus) -> Result<(), ExternalError> {
        consensus
            .db
            .run(async |trx, _maybe_commited| {
                consensus.keys.remove(&trx, &[]).await?;
                consensus.data.remove(&trx, &[]).await?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn fdb_consensus() -> Result<(), ExternalError> {
        let config = FdbConsensusConfig::new(
            std::str::FromStr::from_str("foundationdb:?prefix=test/consensus").unwrap(),
        )?;

        {
            let fdb = FdbConsensus::open(config.clone()).await?;
            drop_and_recreate(&fdb).await?;
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

        drop_and_recreate(&consensus).await?;

        assert_eq!(consensus.head(&key).await, Ok(None));
        Ok(())
    }
}
