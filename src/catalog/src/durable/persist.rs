// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(test)]
mod tests;

use std::cmp::max;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use mz_audit_log::VersionedEvent;
use mz_ore::metrics::MetricsFutureExt;
use mz_ore::now::EpochMillis;
use mz_ore::{
    soft_assert_eq_no_log, soft_assert_eq_or_log, soft_assert_ne_or_log, soft_assert_no_log,
    soft_assert_or_log, soft_panic_or_log,
};
use mz_persist_client::cfg::USE_CRITICAL_SINCE_CATALOG;
use mz_persist_client::cli::admin::{CATALOG_FORCE_COMPACTION_FUEL, CATALOG_FORCE_COMPACTION_WAIT};
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::read::{Listen, ListenEvent, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_proto::{RustType, TryFromProtoError};
use mz_repr::{Diff, RelationDesc, SqlScalarType};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use sha2::Digest;
use timely::Container;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::durable::debug::{Collection, CollectionType, DebugCatalogState, Trace};
use crate::durable::error::FenceError;
use crate::durable::initialize::{
    ENABLE_0DT_DEPLOYMENT, ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT, SYSTEM_CONFIG_SYNCED_KEY,
    USER_VERSION_KEY, WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL, WITH_0DT_DEPLOYMENT_MAX_WAIT,
};
use crate::durable::metrics::Metrics;
use crate::durable::objects::state_update::{
    IntoStateUpdateKindJson, StateUpdate, StateUpdateKind, StateUpdateKindJson,
    TryIntoStateUpdateKind,
};
use crate::durable::objects::{AuditLogKey, FenceToken, Snapshot};
use crate::durable::transaction::TransactionBatch;
use crate::durable::upgrade::upgrade;
use crate::durable::{
    AuditLogIterator, BootstrapArgs, CATALOG_CONTENT_VERSION_KEY, CatalogError,
    DurableCatalogError, DurableCatalogState, Epoch, OpenableDurableCatalogState,
    ReadOnlyDurableCatalogState, Transaction, initialize,
};
use crate::memory;

/// New-type used to represent timestamps in persist.
pub(crate) type Timestamp = mz_repr::Timestamp;

/// The minimum value of an epoch.
///
/// # Safety
/// `new_unchecked` is safe to call with a non-zero value.
const MIN_EPOCH: Epoch = unsafe { Epoch::new_unchecked(1) };

/// Human readable catalog shard name.
const CATALOG_SHARD_NAME: &str = "catalog";
/// Human readable catalog upgrade shard name.
const UPGRADE_SHARD_NAME: &str = "catalog_upgrade";

/// Seed used to generate the persist shard ID for the catalog.
const CATALOG_SEED: usize = 1;
/// Seed used to generate the catalog upgrade shard ID.
///
/// All state that gets written to persist is tagged with the version of the code that wrote that
/// state. Persist has limited forward compatibility in how many versions in the future a reader can
/// read. Reading from persist updates state and the version that the state is tagged with. As a
/// consequence, reading from persist may unintentionally fence out other readers and writers with
/// a lower version. We use the catalog upgrade shard to track what database version is actively
/// deployed so readers from the future, such as the upgrade checker tool, don't accidentally fence out the
/// database from persist. Only writable opened catalogs can increment the version in the upgrade
/// shard.
///
/// One specific example that we are trying to avoid with the catalog upgrade shard is the
/// following:
///
///   1. Database is running on version 0.X.0.
///   2. Upgrade checker is run on version 0.X+1.0.
///   3. Upgrade checker is run on version 0.X+2.0.
///
/// With the catalog upgrade shard, the upgrade checker in step (3) can see that the database is
/// currently running on v0.X.0 and reading the catalog would cause the database to get fenced out.
/// So instead of reading the catalog it errors out. Without the catalog upgrade shard, the upgrade
/// checker could read the version in the catalog shard, and see that it is v0.X+1.0, but it would
/// be impossible to differentiate between the following two scenarios:
///
///   - The database is running on v0.X+1.0 and it's safe to run the upgrade checker at v0.X+2.0.
///   - Some other upgrade checker incremented the version to v0.X+1.0, the database is running on
///   version v0.X.0, and it is not safe to run the upgrade checker.
///
/// Persist guarantees that the shard versions are non-decreasing, so we don't need to worry about
/// race conditions where the shard version decreases after reading it.
const UPGRADE_SEED: usize = 2;
/// Legacy seed used to generate the persist shard ID for builtin table migrations. DO NOT REUSE.
pub const _BUILTIN_MIGRATION_SEED: usize = 3;
/// Legacy seed used to generate the persist shard ID for the expression cache. DO NOT REUSE.
pub const _EXPRESSION_CACHE_SEED: usize = 4;

/// Durable catalog mode that dictates the effect of mutable operations.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum Mode {
    /// Mutable operations are prohibited.
    Readonly,
    /// Mutable operations have an effect in-memory, but aren't persisted durably.
    Savepoint,
    /// Mutable operations have an effect in-memory and durably.
    Writable,
}

/// Enum representing the fenced state of the catalog.
#[derive(Debug)]
pub(crate) enum FenceableToken {
    /// The catalog is still initializing and learning about previously written fence tokens. This
    /// state can be fenced if it encounters a larger deploy generation.
    Initializing {
        /// The largest fence token durably written to the catalog, if any.
        durable_token: Option<FenceToken>,
        /// This process's deploy generation.
        current_deploy_generation: Option<u64>,
    },
    /// The current token has not been fenced.
    Unfenced { current_token: FenceToken },
    /// The current token has been fenced.
    Fenced {
        current_token: FenceToken,
        fence_token: FenceToken,
    },
}

impl FenceableToken {
    /// Returns a new token.
    fn new(current_deploy_generation: Option<u64>) -> Self {
        Self::Initializing {
            durable_token: None,
            current_deploy_generation,
        }
    }

    /// Returns the current token if it is not fenced, otherwise returns an error.
    fn validate(&self) -> Result<Option<FenceToken>, FenceError> {
        match self {
            FenceableToken::Initializing { durable_token, .. } => Ok(durable_token.clone()),
            FenceableToken::Unfenced { current_token, .. } => Ok(Some(current_token.clone())),
            FenceableToken::Fenced {
                current_token,
                fence_token,
            } => {
                assert!(
                    fence_token > current_token,
                    "must be fenced by higher token; current={current_token:?}, fence={fence_token:?}"
                );
                if fence_token.deploy_generation > current_token.deploy_generation {
                    Err(FenceError::DeployGeneration {
                        current_generation: current_token.deploy_generation,
                        fence_generation: fence_token.deploy_generation,
                    })
                } else {
                    assert!(
                        fence_token.epoch > current_token.epoch,
                        "must be fenced by higher token; current={current_token:?}, fence={fence_token:?}"
                    );
                    Err(FenceError::Epoch {
                        current_epoch: current_token.epoch,
                        fence_epoch: fence_token.epoch,
                    })
                }
            }
        }
    }

    /// Returns the current token.
    fn token(&self) -> Option<FenceToken> {
        match self {
            FenceableToken::Initializing { durable_token, .. } => durable_token.clone(),
            FenceableToken::Unfenced { current_token, .. } => Some(current_token.clone()),
            FenceableToken::Fenced { current_token, .. } => Some(current_token.clone()),
        }
    }

    /// Returns `Err` if `token` fences out `self`, `Ok` otherwise.
    fn maybe_fence(&mut self, token: FenceToken) -> Result<(), FenceError> {
        match self {
            FenceableToken::Initializing {
                durable_token,
                current_deploy_generation,
                ..
            } => {
                match durable_token {
                    Some(durable_token) => {
                        *durable_token = max(durable_token.clone(), token.clone());
                    }
                    None => {
                        *durable_token = Some(token.clone());
                    }
                }
                if let Some(current_deploy_generation) = current_deploy_generation {
                    if *current_deploy_generation < token.deploy_generation {
                        *self = FenceableToken::Fenced {
                            current_token: FenceToken {
                                deploy_generation: *current_deploy_generation,
                                epoch: token.epoch,
                            },
                            fence_token: token,
                        };
                        self.validate()?;
                    }
                }
            }
            FenceableToken::Unfenced { current_token } => {
                if *current_token < token {
                    *self = FenceableToken::Fenced {
                        current_token: current_token.clone(),
                        fence_token: token,
                    };
                    self.validate()?;
                }
            }
            FenceableToken::Fenced { .. } => {
                self.validate()?;
            }
        }

        Ok(())
    }

    /// Returns a [`FenceableToken::Unfenced`] token and the updates to the catalog required to
    /// transition to the `Unfenced` state if `self` is [`FenceableToken::Initializing`], otherwise
    /// returns `None`.
    fn generate_unfenced_token(
        &self,
        mode: Mode,
    ) -> Result<Option<(Vec<(StateUpdateKind, Diff)>, FenceableToken)>, DurableCatalogError> {
        let (durable_token, current_deploy_generation) = match self {
            FenceableToken::Initializing {
                durable_token,
                current_deploy_generation,
            } => (durable_token.clone(), current_deploy_generation.clone()),
            FenceableToken::Unfenced { .. } | FenceableToken::Fenced { .. } => return Ok(None),
        };

        let mut fence_updates = Vec::with_capacity(2);

        if let Some(durable_token) = &durable_token {
            fence_updates.push((
                StateUpdateKind::FenceToken(durable_token.clone()),
                Diff::MINUS_ONE,
            ));
        }

        let current_deploy_generation = current_deploy_generation
            .or_else(|| durable_token.as_ref().map(|token| token.deploy_generation))
            // We cannot initialize a catalog without a deploy generation.
            .ok_or(DurableCatalogError::Uninitialized)?;
        let mut current_epoch = durable_token
            .map(|token| token.epoch)
            .unwrap_or(MIN_EPOCH)
            .get();
        // Only writable catalogs attempt to increment the epoch.
        if matches!(mode, Mode::Writable) {
            current_epoch = current_epoch + 1;
        }
        let current_epoch = Epoch::new(current_epoch).expect("known to be non-zero");
        let current_token = FenceToken {
            deploy_generation: current_deploy_generation,
            epoch: current_epoch,
        };

        fence_updates.push((
            StateUpdateKind::FenceToken(current_token.clone()),
            Diff::ONE,
        ));

        let current_fenceable_token = FenceableToken::Unfenced { current_token };

        Ok(Some((fence_updates, current_fenceable_token)))
    }
}

/// An error that can occur while executing [`PersistHandle::compare_and_append`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum CompareAndAppendError {
    #[error(transparent)]
    Fence(#[from] FenceError),
    /// Catalog encountered an upper mismatch when trying to write to the catalog. This should only
    /// happen while trying to fence out other catalogs.
    #[error(
        "expected catalog upper {expected_upper:?} did not match actual catalog upper {actual_upper:?}"
    )]
    UpperMismatch {
        expected_upper: Timestamp,
        actual_upper: Timestamp,
    },
}

impl CompareAndAppendError {
    pub(crate) fn unwrap_fence_error(self) -> FenceError {
        match self {
            CompareAndAppendError::Fence(e) => e,
            e @ CompareAndAppendError::UpperMismatch { .. } => {
                panic!("unexpected upper mismatch: {e:?}")
            }
        }
    }
}

impl From<UpperMismatch<Timestamp>> for CompareAndAppendError {
    fn from(upper_mismatch: UpperMismatch<Timestamp>) -> Self {
        Self::UpperMismatch {
            expected_upper: antichain_to_timestamp(upper_mismatch.expected),
            actual_upper: antichain_to_timestamp(upper_mismatch.current),
        }
    }
}

pub(crate) trait ApplyUpdate<T: IntoStateUpdateKindJson> {
    /// Process and apply `update`.
    ///
    /// Returns `Some` if `update` should be cached in memory and `None` otherwise.
    fn apply_update(
        &mut self,
        update: StateUpdate<T>,
        current_fence_token: &mut FenceableToken,
        metrics: &Arc<Metrics>,
    ) -> Result<Option<StateUpdate<T>>, FenceError>;
}

/// A handle for interacting with the persist catalog shard.
///
/// The catalog shard is used in multiple different contexts, for example pre-open and post-open,
/// but for all contexts the majority of the durable catalog's behavior is identical. This struct
/// implements those behaviors that are identical while allowing the user to specify the different
/// behaviors via generic parameters.
///
/// The behavior of the durable catalog can be different along one of two axes. The first is the
/// format of each individual update, i.e. raw binary, the current protobuf version, previous
/// protobuf versions, etc. The second axis is what to do with each individual update, for example
/// before opening we cache all config updates but don't cache them after opening. These behaviors
/// are customizable via the `T: TryIntoStateUpdateKind` and `U: ApplyUpdate<T>` generic parameters
/// respectively.
#[derive(Debug)]
pub(crate) struct PersistHandle<T: TryIntoStateUpdateKind, U: ApplyUpdate<T>> {
    /// The [`Mode`] that this catalog was opened in.
    pub(crate) mode: Mode,
    /// Since handle to control compaction.
    since_handle: SinceHandle<SourceData, (), Timestamp, StorageDiff, i64>,
    /// Write handle to persist.
    write_handle: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    /// Listener to catalog changes.
    listen: Listen<SourceData, (), Timestamp, StorageDiff>,
    /// Handle for connecting to persist.
    persist_client: PersistClient,
    /// Catalog shard ID.
    shard_id: ShardId,
    /// Cache of the most recent catalog snapshot.
    ///
    /// We use a tuple instead of [`StateUpdate`] to make consolidation easier.
    pub(crate) snapshot: Vec<(T, Timestamp, Diff)>,
    /// Applies custom processing, filtering, and fencing for each individual update.
    update_applier: U,
    /// The current upper of the persist shard.
    pub(crate) upper: Timestamp,
    /// The fence token of the catalog, if one exists.
    fenceable_token: FenceableToken,
    /// The semantic version of the current binary.
    catalog_content_version: semver::Version,
    /// Flag to indicate if bootstrap is complete.
    bootstrap_complete: bool,
    /// Metrics for the persist catalog.
    metrics: Arc<Metrics>,
}

impl<T: TryIntoStateUpdateKind, U: ApplyUpdate<T>> PersistHandle<T, U> {
    /// Increment the version in the catalog upgrade shard to the code's current version.
    async fn increment_catalog_upgrade_shard_version(&self, organization_id: Uuid) {
        let upgrade_shard_id = shard_id(organization_id, UPGRADE_SEED);
        let mut write_handle: WriteHandle<(), (), Timestamp, StorageDiff> = self
            .persist_client
            .open_writer(
                upgrade_shard_id,
                Arc::new(UnitSchema::default()),
                Arc::new(UnitSchema::default()),
                Diagnostics {
                    shard_name: UPGRADE_SHARD_NAME.to_string(),
                    handle_purpose: "increment durable catalog upgrade shard version".to_string(),
                },
            )
            .await
            .expect("invalid usage");
        const EMPTY_UPDATES: &[(((), ()), Timestamp, StorageDiff)] = &[];
        let mut upper = write_handle.fetch_recent_upper().await.clone();
        loop {
            let next_upper = upper
                .iter()
                .map(|timestamp| timestamp.step_forward())
                .collect();
            match write_handle
                .compare_and_append(EMPTY_UPDATES, upper, next_upper)
                .await
                .expect("invalid usage")
            {
                Ok(()) => break,
                Err(upper_mismatch) => {
                    upper = upper_mismatch.current;
                }
            }
        }
    }

    /// Fetch the current upper of the catalog state.
    #[mz_ore::instrument]
    async fn current_upper(&mut self) -> Timestamp {
        match self.mode {
            Mode::Writable | Mode::Readonly => {
                let upper = self.write_handle.fetch_recent_upper().await;
                antichain_to_timestamp(upper.clone())
            }
            Mode::Savepoint => self.upper,
        }
    }

    /// Appends `updates` iff the current global upper of the catalog is `self.upper`.
    ///
    /// Returns the next upper used to commit the transaction.
    #[mz_ore::instrument]
    pub(crate) async fn compare_and_append<S: IntoStateUpdateKindJson>(
        &mut self,
        updates: Vec<(S, Diff)>,
        commit_ts: Timestamp,
    ) -> Result<Timestamp, CompareAndAppendError> {
        assert_eq!(self.mode, Mode::Writable);
        assert!(
            commit_ts >= self.upper,
            "expected commit ts, {}, to be greater than or equal to upper, {}",
            commit_ts,
            self.upper
        );

        // This awkward code allows us to perform an expensive soft assert that requires cloning
        // `updates` twice, after `updates` has been consumed.
        let contains_fence = if mz_ore::assert::soft_assertions_enabled() {
            let updates: Vec<_> = updates.clone();
            let parsed_updates: Vec<_> = updates
                .clone()
                .into_iter()
                .map(|(update, diff)| {
                    let update: StateUpdateKindJson = update.into();
                    (update, diff)
                })
                .filter_map(|(update, diff)| {
                    <StateUpdateKindJson as TryIntoStateUpdateKind>::try_into(update)
                        .ok()
                        .map(|update| (update, diff))
                })
                .collect();
            let contains_retraction = parsed_updates.iter().any(|(update, diff)| {
                matches!(update, StateUpdateKind::FenceToken(..)) && *diff == Diff::MINUS_ONE
            });
            let contains_addition = parsed_updates.iter().any(|(update, diff)| {
                matches!(update, StateUpdateKind::FenceToken(..)) && *diff == Diff::ONE
            });
            let contains_fence = contains_retraction && contains_addition;
            Some((contains_fence, updates))
        } else {
            None
        };

        let updates = updates.into_iter().map(|(kind, diff)| {
            let kind: StateUpdateKindJson = kind.into();
            (
                (Into::<SourceData>::into(kind), ()),
                commit_ts,
                diff.into_inner(),
            )
        });
        let next_upper = commit_ts.step_forward();
        let res = self
            .write_handle
            .compare_and_append(
                updates,
                Antichain::from_elem(self.upper),
                Antichain::from_elem(next_upper),
            )
            .await
            .expect("invalid usage");

        // There was an upper mismatch which means something else must have written to the catalog.
        // Syncing to the current upper should result in a fence error since writing to the catalog
        // without fencing other catalogs should be impossible. The one exception is if we are
        // trying to fence other catalogs with this write, in which case we won't see a fence error.
        if let Err(e @ UpperMismatch { .. }) = res {
            self.sync_to_current_upper().await?;
            if let Some((contains_fence, updates)) = contains_fence {
                assert!(
                    contains_fence,
                    "updates were neither fenced nor fencing and encountered an upper mismatch: {updates:#?}"
                )
            }
            return Err(e.into());
        }

        // Lag the shard's upper by 1 to keep it readable.
        let downgrade_to = Antichain::from_elem(next_upper.saturating_sub(1));

        // The since handle gives us the ability to fence out other downgraders using an opaque token.
        // (See the method documentation for details.)
        // That's not needed here, so we use the since handle's opaque token to avoid any comparison
        // failures.
        let opaque = *self.since_handle.opaque();
        let downgrade = self
            .since_handle
            .maybe_compare_and_downgrade_since(&opaque, (&opaque, &downgrade_to))
            .await;

        match downgrade {
            None => {}
            Some(Err(e)) => soft_panic_or_log!("found opaque value {e}, but expected {opaque}"),
            Some(Ok(updated)) => soft_assert_or_log!(
                updated == downgrade_to,
                "updated bound should match expected"
            ),
        }
        self.sync(next_upper).await?;
        Ok(next_upper)
    }

    /// Generates an iterator of [`StateUpdate`] that contain all unconsolidated updates to the
    /// catalog state up to, and including, `as_of`.
    #[mz_ore::instrument]
    async fn snapshot_unconsolidated(&mut self) -> Vec<StateUpdate<StateUpdateKind>> {
        let current_upper = self.current_upper().await;

        let mut snapshot = Vec::new();
        let mut read_handle = self.read_handle().await;
        let as_of = as_of(&read_handle, current_upper);
        let mut stream = Box::pin(
            // We use `snapshot_and_stream` because it guarantees unconsolidated output.
            read_handle
                .snapshot_and_stream(Antichain::from_elem(as_of))
                .await
                .expect("we have advanced the restart_as_of by the since"),
        );
        while let Some(update) = stream.next().await {
            snapshot.push(update)
        }
        read_handle.expire().await;
        snapshot
            .into_iter()
            .map(Into::<StateUpdate<StateUpdateKindJson>>::into)
            .map(|state_update| state_update.try_into().expect("kind decoding error"))
            .collect()
    }

    /// Listen and apply all updates that are currently in persist.
    ///
    /// Returns an error if this instance has been fenced out.
    #[mz_ore::instrument]
    pub(crate) async fn sync_to_current_upper(&mut self) -> Result<(), FenceError> {
        let upper = self.current_upper().await;
        self.sync(upper).await
    }

    /// Listen and apply all updates up to `target_upper`.
    ///
    /// Returns an error if this instance has been fenced out.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn sync(&mut self, target_upper: Timestamp) -> Result<(), FenceError> {
        self.metrics.syncs.inc();
        let counter = self.metrics.sync_latency_seconds.clone();
        self.sync_inner(target_upper)
            .wall_time()
            .inc_by(counter)
            .await
    }

    #[mz_ore::instrument(level = "debug")]
    async fn sync_inner(&mut self, target_upper: Timestamp) -> Result<(), FenceError> {
        self.fenceable_token.validate()?;

        // Savepoint catalogs do not yet know how to update themselves in response to concurrent
        // writes from writer catalogs.
        if self.mode == Mode::Savepoint {
            self.upper = max(self.upper, target_upper);
            return Ok(());
        }

        let mut updates: BTreeMap<_, Vec<_>> = BTreeMap::new();

        while self.upper < target_upper {
            let listen_events = self.listen.fetch_next().await;
            for listen_event in listen_events {
                match listen_event {
                    ListenEvent::Progress(upper) => {
                        debug!("synced up to {upper:?}");
                        self.upper = antichain_to_timestamp(upper);
                        // Attempt to apply updates in batches of a single timestamp. If another
                        // catalog wrote a fence token at one timestamp and then updates in a new
                        // format at a later timestamp, then we want to apply the fence token
                        // before attempting to deserialize the new updates.
                        while let Some((ts, updates)) = updates.pop_first() {
                            assert!(ts < self.upper, "expected {} < {}", ts, self.upper);
                            let updates = updates.into_iter().map(
                                |update: StateUpdate<StateUpdateKindJson>| {
                                    let kind =
                                        T::try_from(update.kind).expect("kind decoding error");
                                    StateUpdate {
                                        kind,
                                        ts: update.ts,
                                        diff: update.diff,
                                    }
                                },
                            );
                            self.apply_updates(updates)?;
                        }
                    }
                    ListenEvent::Updates(batch_updates) => {
                        for update in batch_updates {
                            let update: StateUpdate<StateUpdateKindJson> = update.into();
                            updates.entry(update.ts).or_default().push(update);
                        }
                    }
                }
            }
        }
        assert_eq!(updates, BTreeMap::new(), "all updates should be applied");
        Ok(())
    }

    #[mz_ore::instrument(level = "debug")]
    pub(crate) fn apply_updates(
        &mut self,
        updates: impl IntoIterator<Item = StateUpdate<T>>,
    ) -> Result<(), FenceError> {
        let mut updates: Vec<_> = updates
            .into_iter()
            .map(|StateUpdate { kind, ts, diff }| (kind, ts, diff))
            .collect();

        // This helps guarantee that for a single key, there is at most a single retraction and a
        // single insertion per timestamp. Otherwise, we would need to match the retractions and
        // insertions up by value and manually figure out what the end value should be.
        differential_dataflow::consolidation::consolidate_updates(&mut updates);

        // Updates must be applied in timestamp order. Within a timestamp retractions must be
        // applied before insertions, or we might end up retracting the wrong value.
        updates.sort_by(|(_, ts1, diff1), (_, ts2, diff2)| ts1.cmp(ts2).then(diff1.cmp(diff2)));

        let mut errors = Vec::new();

        for (kind, ts, diff) in updates {
            if diff != Diff::ONE && diff != Diff::MINUS_ONE {
                panic!("invalid update in consolidated trace: ({kind:?}, {ts:?}, {diff:?})");
            }

            match self.update_applier.apply_update(
                StateUpdate { kind, ts, diff },
                &mut self.fenceable_token,
                &self.metrics,
            ) {
                Ok(Some(StateUpdate { kind, ts, diff })) => self.snapshot.push((kind, ts, diff)),
                Ok(None) => {}
                // Instead of returning immediately, we accumulate all the errors and return the one
                // with the most information.
                Err(err) => errors.push(err),
            }
        }

        errors.sort();
        if let Some(err) = errors.into_iter().next() {
            return Err(err);
        }

        self.consolidate();

        Ok(())
    }

    #[mz_ore::instrument]
    pub(crate) fn consolidate(&mut self) {
        soft_assert_no_log!(
            self.snapshot
                .windows(2)
                .all(|updates| updates[0].1 <= updates[1].1),
            "snapshot should be sorted by timestamp, {:#?}",
            self.snapshot
        );

        let new_ts = self
            .snapshot
            .last()
            .map(|(_, ts, _)| *ts)
            .unwrap_or_else(Timestamp::minimum);
        for (_, ts, _) in &mut self.snapshot {
            *ts = new_ts;
        }
        differential_dataflow::consolidation::consolidate_updates(&mut self.snapshot);
    }

    /// Execute and return the results of `f` on the current catalog trace.
    ///
    /// Will return an error if the catalog has been fenced out.
    async fn with_trace<R>(
        &mut self,
        f: impl FnOnce(&Vec<(T, Timestamp, Diff)>) -> Result<R, CatalogError>,
    ) -> Result<R, CatalogError> {
        self.sync_to_current_upper().await?;
        f(&self.snapshot)
    }

    /// Open a read handle to the catalog.
    async fn read_handle(&self) -> ReadHandle<SourceData, (), Timestamp, StorageDiff> {
        self.persist_client
            .open_leased_reader(
                self.shard_id,
                Arc::new(desc()),
                Arc::new(UnitSchema::default()),
                Diagnostics {
                    shard_name: CATALOG_SHARD_NAME.to_string(),
                    handle_purpose: "openable durable catalog state temporary reader".to_string(),
                },
                USE_CRITICAL_SINCE_CATALOG.get(self.persist_client.dyncfgs()),
            )
            .await
            .expect("invalid usage")
    }

    /// Politely releases all external resources that can only be released in an async context.
    async fn expire(self: Box<Self>) {
        self.write_handle.expire().await;
        self.listen.expire().await;
    }
}

impl<U: ApplyUpdate<StateUpdateKind>> PersistHandle<StateUpdateKind, U> {
    /// Execute and return the results of `f` on the current catalog snapshot.
    ///
    /// Will return an error if the catalog has been fenced out.
    async fn with_snapshot<T>(
        &mut self,
        f: impl FnOnce(Snapshot) -> Result<T, CatalogError>,
    ) -> Result<T, CatalogError> {
        fn apply<K, V>(map: &mut BTreeMap<K, V>, key: &K, value: &V, diff: Diff)
        where
            K: Ord + Clone,
            V: Ord + Clone + Debug,
        {
            let key = key.clone();
            let value = value.clone();
            if diff == Diff::ONE {
                let prev = map.insert(key, value);
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value"
                );
            } else if diff == Diff::MINUS_ONE {
                let prev = map.remove(&key);
                assert_eq!(
                    prev,
                    Some(value),
                    "retraction does not match existing value"
                );
            }
        }

        self.with_trace(|trace| {
            let mut snapshot = Snapshot::empty();
            for (kind, ts, diff) in trace {
                let diff = *diff;
                if diff != Diff::ONE && diff != Diff::MINUS_ONE {
                    panic!("invalid update in consolidated trace: ({kind:?}, {ts:?}, {diff:?})");
                }

                match kind {
                    StateUpdateKind::AuditLog(_key, ()) => {
                        // Ignore for snapshots.
                    }
                    StateUpdateKind::Cluster(key, value) => {
                        apply(&mut snapshot.clusters, key, value, diff);
                    }
                    StateUpdateKind::ClusterReplica(key, value) => {
                        apply(&mut snapshot.cluster_replicas, key, value, diff);
                    }
                    StateUpdateKind::Comment(key, value) => {
                        apply(&mut snapshot.comments, key, value, diff);
                    }
                    StateUpdateKind::Config(key, value) => {
                        apply(&mut snapshot.configs, key, value, diff);
                    }
                    StateUpdateKind::Database(key, value) => {
                        apply(&mut snapshot.databases, key, value, diff);
                    }
                    StateUpdateKind::DefaultPrivilege(key, value) => {
                        apply(&mut snapshot.default_privileges, key, value, diff);
                    }
                    StateUpdateKind::FenceToken(_token) => {
                        // Ignore for snapshots.
                    }
                    StateUpdateKind::IdAllocator(key, value) => {
                        apply(&mut snapshot.id_allocator, key, value, diff);
                    }
                    StateUpdateKind::IntrospectionSourceIndex(key, value) => {
                        apply(&mut snapshot.introspection_sources, key, value, diff);
                    }
                    StateUpdateKind::Item(key, value) => {
                        apply(&mut snapshot.items, key, value, diff);
                    }
                    StateUpdateKind::NetworkPolicy(key, value) => {
                        apply(&mut snapshot.network_policies, key, value, diff);
                    }
                    StateUpdateKind::Role(key, value) => {
                        apply(&mut snapshot.roles, key, value, diff);
                    }
                    StateUpdateKind::Schema(key, value) => {
                        apply(&mut snapshot.schemas, key, value, diff);
                    }
                    StateUpdateKind::Setting(key, value) => {
                        apply(&mut snapshot.settings, key, value, diff);
                    }
                    StateUpdateKind::SourceReferences(key, value) => {
                        apply(&mut snapshot.source_references, key, value, diff);
                    }
                    StateUpdateKind::SystemConfiguration(key, value) => {
                        apply(&mut snapshot.system_configurations, key, value, diff);
                    }
                    StateUpdateKind::SystemObjectMapping(key, value) => {
                        apply(&mut snapshot.system_object_mappings, key, value, diff);
                    }
                    StateUpdateKind::SystemPrivilege(key, value) => {
                        apply(&mut snapshot.system_privileges, key, value, diff);
                    }
                    StateUpdateKind::StorageCollectionMetadata(key, value) => {
                        apply(&mut snapshot.storage_collection_metadata, key, value, diff);
                    }
                    StateUpdateKind::UnfinalizedShard(key, ()) => {
                        apply(&mut snapshot.unfinalized_shards, key, &(), diff);
                    }
                    StateUpdateKind::TxnWalShard((), value) => {
                        apply(&mut snapshot.txn_wal_shard, &(), value, diff);
                    }
                    StateUpdateKind::RoleAuth(key, value) => {
                        apply(&mut snapshot.role_auth, key, value, diff);
                    }
                }
            }
            f(snapshot)
        })
        .await
    }

    /// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
    /// state.
    ///
    /// The output is fetched directly from persist instead of the in-memory cache.
    ///
    /// The output is consolidated and sorted by timestamp in ascending order.
    #[mz_ore::instrument(level = "debug")]
    async fn persist_snapshot(
        &mut self,
    ) -> impl Iterator<Item = StateUpdate> + DoubleEndedIterator {
        let mut read_handle = self.read_handle().await;
        let as_of = as_of(&read_handle, self.upper);
        let snapshot = snapshot_binary(&mut read_handle, as_of, &self.metrics)
            .await
            .map(|update| update.try_into().expect("kind decoding error"));
        read_handle.expire().await;
        snapshot
    }
}

/// Applies updates for an unopened catalog.
#[derive(Debug)]
pub(crate) struct UnopenedCatalogStateInner {
    /// The organization ID of the environment.
    organization_id: Uuid,
    /// A cache of the config collection of the catalog.
    configs: BTreeMap<String, u64>,
    /// A cache of the settings collection of the catalog.
    settings: BTreeMap<String, String>,
}

impl UnopenedCatalogStateInner {
    fn new(organization_id: Uuid) -> UnopenedCatalogStateInner {
        UnopenedCatalogStateInner {
            organization_id,
            configs: BTreeMap::new(),
            settings: BTreeMap::new(),
        }
    }
}

impl ApplyUpdate<StateUpdateKindJson> for UnopenedCatalogStateInner {
    fn apply_update(
        &mut self,
        update: StateUpdate<StateUpdateKindJson>,
        current_fence_token: &mut FenceableToken,
        _metrics: &Arc<Metrics>,
    ) -> Result<Option<StateUpdate<StateUpdateKindJson>>, FenceError> {
        if !update.kind.is_audit_log() && update.kind.is_always_deserializable() {
            let kind = TryInto::try_into(&update.kind).expect("kind is known to be deserializable");
            match (kind, update.diff) {
                (StateUpdateKind::Config(key, value), Diff::ONE) => {
                    let prev = self.configs.insert(key.key, value.value);
                    assert_eq!(
                        prev, None,
                        "values must be explicitly retracted before inserting a new value"
                    );
                }
                (StateUpdateKind::Config(key, value), Diff::MINUS_ONE) => {
                    let prev = self.configs.remove(&key.key);
                    assert_eq!(
                        prev,
                        Some(value.value),
                        "retraction does not match existing value"
                    );
                }
                (StateUpdateKind::Setting(key, value), Diff::ONE) => {
                    let prev = self.settings.insert(key.name, value.value);
                    assert_eq!(
                        prev, None,
                        "values must be explicitly retracted before inserting a new value"
                    );
                }
                (StateUpdateKind::Setting(key, value), Diff::MINUS_ONE) => {
                    let prev = self.settings.remove(&key.name);
                    assert_eq!(
                        prev,
                        Some(value.value),
                        "retraction does not match existing value"
                    );
                }
                (StateUpdateKind::FenceToken(fence_token), Diff::ONE) => {
                    current_fence_token.maybe_fence(fence_token)?;
                }
                _ => {}
            }
        }

        Ok(Some(update))
    }
}

/// A Handle to an unopened catalog stored in persist. The unopened catalog can serve `Config` data,
/// `Setting` data, or the current epoch. All other catalog data may be un-migrated and should not
/// be read until the catalog has been opened. The [`UnopenedPersistCatalogState`] is responsible
/// for opening the catalog, see [`OpenableDurableCatalogState::open`] for more details.
///
/// Production users should call [`Self::expire`] before dropping an [`UnopenedPersistCatalogState`]
/// so that it can expire its leases. If/when rust gets AsyncDrop, this will be done automatically.
pub(crate) type UnopenedPersistCatalogState =
    PersistHandle<StateUpdateKindJson, UnopenedCatalogStateInner>;

impl UnopenedPersistCatalogState {
    /// Create a new [`UnopenedPersistCatalogState`] to the catalog state associated with
    /// `organization_id`.
    ///
    /// All usages of the persist catalog must go through this function. That includes the
    /// catalog-debug tool, the adapter's catalog, etc.
    #[mz_ore::instrument]
    pub(crate) async fn new(
        persist_client: PersistClient,
        organization_id: Uuid,
        version: semver::Version,
        deploy_generation: Option<u64>,
        metrics: Arc<Metrics>,
    ) -> Result<UnopenedPersistCatalogState, DurableCatalogError> {
        let catalog_shard_id = shard_id(organization_id, CATALOG_SEED);
        let upgrade_shard_id = shard_id(organization_id, UPGRADE_SEED);
        debug!(
            ?catalog_shard_id,
            ?upgrade_shard_id,
            "new persist backed catalog state"
        );

        // Check the catalog upgrade shard to see ensure that we don't fence anyone out of persist.
        let version_in_upgrade_shard =
            fetch_catalog_upgrade_shard_version(&persist_client, upgrade_shard_id).await;
        // If this is `None`, no version was found in the upgrade shard. This is a brand-new
        // environment, and we don't need to worry about fencing existing users.
        if let Some(version_in_upgrade_shard) = version_in_upgrade_shard {
            // IMPORTANT: We swap the order of arguments here! Normally it's
            // `code_version, data_version`, and we check whether a given code
            // version, which is usually _older_, is allowed to touch a shard
            // that has been touched by a _future_ version.
            //
            // By inverting argument order, we check if our version is too far
            // ahead of the version in the shard.
            if mz_persist_client::cfg::check_data_version(&version_in_upgrade_shard, &version)
                .is_err()
            {
                return Err(DurableCatalogError::IncompatiblePersistVersion {
                    found_version: version_in_upgrade_shard,
                    catalog_version: version,
                });
            }
        }

        let open_handles_start = Instant::now();
        info!("startup: envd serve: catalog init: open handles beginning");
        let since_handle = persist_client
            .open_critical_since(
                catalog_shard_id,
                // TODO: We may need to use a different critical reader
                // id for this if we want to be able to introspect it via SQL.
                PersistClient::CONTROLLER_CRITICAL_SINCE,
                Diagnostics {
                    shard_name: CATALOG_SHARD_NAME.to_string(),
                    handle_purpose: "durable catalog state critical since".to_string(),
                },
            )
            .await
            .expect("invalid usage");
        let (mut write_handle, mut read_handle) = persist_client
            .open(
                catalog_shard_id,
                Arc::new(desc()),
                Arc::new(UnitSchema::default()),
                Diagnostics {
                    shard_name: CATALOG_SHARD_NAME.to_string(),
                    handle_purpose: "durable catalog state handles".to_string(),
                },
                USE_CRITICAL_SINCE_CATALOG.get(persist_client.dyncfgs()),
            )
            .await
            .expect("invalid usage");
        info!(
            "startup: envd serve: catalog init: open handles complete in {:?}",
            open_handles_start.elapsed()
        );

        // Commit an empty write at the minimum timestamp so the catalog is always readable.
        let upper = {
            const EMPTY_UPDATES: &[((SourceData, ()), Timestamp, StorageDiff)] = &[];
            let upper = Antichain::from_elem(Timestamp::minimum());
            let next_upper = Timestamp::minimum().step_forward();
            match write_handle
                .compare_and_append(EMPTY_UPDATES, upper, Antichain::from_elem(next_upper))
                .await
                .expect("invalid usage")
            {
                Ok(()) => next_upper,
                Err(mismatch) => antichain_to_timestamp(mismatch.current),
            }
        };

        let snapshot_start = Instant::now();
        info!("startup: envd serve: catalog init: snapshot beginning");
        let as_of = as_of(&read_handle, upper);
        let snapshot: Vec<_> = snapshot_binary(&mut read_handle, as_of, &metrics)
            .await
            .map(|StateUpdate { kind, ts, diff }| (kind, ts, diff))
            .collect();
        let listen = read_handle
            .listen(Antichain::from_elem(as_of))
            .await
            .expect("invalid usage");
        info!(
            "startup: envd serve: catalog init: snapshot complete in {:?}",
            snapshot_start.elapsed()
        );

        let mut handle = UnopenedPersistCatalogState {
            // Unopened catalogs are always writeable until they're opened in an explicit mode.
            mode: Mode::Writable,
            since_handle,
            write_handle,
            listen,
            persist_client,
            shard_id: catalog_shard_id,
            // Initialize empty in-memory state.
            snapshot: Vec::new(),
            update_applier: UnopenedCatalogStateInner::new(organization_id),
            upper,
            fenceable_token: FenceableToken::new(deploy_generation),
            catalog_content_version: version,
            bootstrap_complete: false,
            metrics,
        };
        // If the snapshot is not consolidated, and we see multiple epoch values while applying the
        // updates, then we might accidentally fence ourselves out.
        soft_assert_no_log!(
            snapshot.iter().all(|(_, _, diff)| *diff == Diff::ONE),
            "snapshot should be consolidated: {snapshot:#?}"
        );

        let apply_start = Instant::now();
        info!("startup: envd serve: catalog init: apply updates beginning");
        let updates = snapshot
            .into_iter()
            .map(|(kind, ts, diff)| StateUpdate { kind, ts, diff });
        handle.apply_updates(updates)?;
        info!(
            "startup: envd serve: catalog init: apply updates complete in {:?}",
            apply_start.elapsed()
        );

        // Validate that the binary version of the current process is not less than any binary
        // version that has written to the catalog.
        // This condition is only checked once, right here. If a new process comes along with a
        // higher version, it must fence this process out with one of the existing fencing
        // mechanisms.
        if let Some(found_version) = handle.get_catalog_content_version().await? {
            if handle.catalog_content_version < found_version {
                return Err(DurableCatalogError::IncompatiblePersistVersion {
                    found_version,
                    catalog_version: handle.catalog_content_version,
                });
            }
        }

        Ok(handle)
    }

    #[mz_ore::instrument]
    async fn open_inner(
        mut self,
        mode: Mode,
        initial_ts: Timestamp,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<(Box<dyn DurableCatalogState>, AuditLogIterator), CatalogError> {
        // It would be nice to use `initial_ts` here, but it comes from the system clock, not the
        // timestamp oracle.
        let mut commit_ts = self.upper;
        self.mode = mode;

        // Validate the current deploy generation.
        match (&self.mode, &self.fenceable_token) {
            (_, FenceableToken::Unfenced { .. } | FenceableToken::Fenced { .. }) => {
                return Err(DurableCatalogError::Internal(
                    "catalog should not have fenced before opening".to_string(),
                )
                .into());
            }
            (
                Mode::Writable | Mode::Savepoint,
                FenceableToken::Initializing {
                    current_deploy_generation: None,
                    ..
                },
            ) => {
                return Err(DurableCatalogError::Internal(format!(
                    "cannot open in mode '{:?}' without a deploy generation",
                    self.mode,
                ))
                .into());
            }
            _ => {}
        }

        let read_only = matches!(self.mode, Mode::Readonly);

        // Fence out previous catalogs.
        loop {
            self.sync_to_current_upper().await?;
            commit_ts = max(commit_ts, self.upper);
            let (fence_updates, current_fenceable_token) = self
                .fenceable_token
                .generate_unfenced_token(self.mode)?
                .ok_or_else(|| {
                    DurableCatalogError::Internal(
                        "catalog should not have fenced before opening".to_string(),
                    )
                })?;
            debug!(
                ?self.upper,
                ?self.fenceable_token,
                ?current_fenceable_token,
                "fencing previous catalogs"
            );
            if matches!(self.mode, Mode::Writable) {
                match self
                    .compare_and_append(fence_updates.clone(), commit_ts)
                    .await
                {
                    Ok(upper) => {
                        commit_ts = upper;
                    }
                    Err(CompareAndAppendError::Fence(e)) => return Err(e.into()),
                    Err(e @ CompareAndAppendError::UpperMismatch { .. }) => {
                        warn!("catalog write failed due to upper mismatch, retrying: {e:?}");
                        continue;
                    }
                }
            }
            self.fenceable_token = current_fenceable_token;
            break;
        }

        let is_initialized = self.is_initialized_inner();
        if !matches!(self.mode, Mode::Writable) && !is_initialized {
            return Err(CatalogError::Durable(DurableCatalogError::NotWritable(
                format!(
                    "catalog tables do not exist; will not create in {:?} mode",
                    self.mode
                ),
            )));
        }
        soft_assert_ne_or_log!(self.upper, Timestamp::minimum());

        // Remove all audit log entries.
        let (audit_logs, snapshot): (Vec<_>, Vec<_>) = self
            .snapshot
            .into_iter()
            .partition(|(update, _, _)| update.is_audit_log());
        self.snapshot = snapshot;
        let audit_log_count = audit_logs.iter().map(|(_, _, diff)| diff).sum::<Diff>();
        let audit_log_handle = AuditLogIterator::new(audit_logs);

        // Perform data migrations.
        if is_initialized && !read_only {
            commit_ts = upgrade(&mut self, commit_ts).await?;
        }

        debug!(
            ?is_initialized,
            ?self.upper,
            "initializing catalog state"
        );
        let mut catalog = PersistCatalogState {
            mode: self.mode,
            since_handle: self.since_handle,
            write_handle: self.write_handle,
            listen: self.listen,
            persist_client: self.persist_client,
            shard_id: self.shard_id,
            upper: self.upper,
            fenceable_token: self.fenceable_token,
            // Initialize empty in-memory state.
            snapshot: Vec::new(),
            update_applier: CatalogStateInner::new(),
            catalog_content_version: self.catalog_content_version,
            bootstrap_complete: false,
            metrics: self.metrics,
        };
        catalog.metrics.collection_entries.reset();
        // Normally, `collection_entries` is updated in `apply_updates`. The audit log updates skip
        // over that function so we manually update it here.
        catalog
            .metrics
            .collection_entries
            .with_label_values(&[&CollectionType::AuditLog.to_string()])
            .add(audit_log_count.into_inner());
        let updates = self.snapshot.into_iter().map(|(kind, ts, diff)| {
            let kind = TryIntoStateUpdateKind::try_into(kind).expect("kind decoding error");
            StateUpdate { kind, ts, diff }
        });
        catalog.apply_updates(updates)?;

        let catalog_content_version = catalog.catalog_content_version.to_string();
        let txn = if is_initialized {
            let mut txn = catalog.transaction().await?;
            txn.set_catalog_content_version(catalog_content_version)?;
            txn
        } else {
            soft_assert_eq_no_log!(
                catalog
                    .snapshot
                    .iter()
                    .filter(|(kind, _, _)| !matches!(kind, StateUpdateKind::FenceToken(_)))
                    .count(),
                0,
                "trace should not contain any updates for an uninitialized catalog: {:#?}",
                catalog.snapshot
            );

            let mut txn = catalog.transaction().await?;
            initialize::initialize(
                &mut txn,
                bootstrap_args,
                initial_ts.into(),
                catalog_content_version,
            )
            .await?;
            txn
        };

        if read_only {
            let (txn_batch, _) = txn.into_parts();
            // The upper here doesn't matter because we are only applying the updates in memory.
            let updates = StateUpdate::from_txn_batch_ts(txn_batch, catalog.upper);
            catalog.apply_updates(updates)?;
        } else {
            txn.commit_internal(commit_ts).await?;
        }

        // Now that we've fully opened the catalog at the current version, we can increment the
        // version in the catalog upgrade shard to signal to readers that the allowable versions
        // have increased.
        if matches!(catalog.mode, Mode::Writable) {
            catalog
                .increment_catalog_upgrade_shard_version(self.update_applier.organization_id)
                .await;
            let write_handle = catalog
                .persist_client
                .open_writer::<SourceData, (), Timestamp, i64>(
                    catalog.write_handle.shard_id(),
                    Arc::new(desc()),
                    Arc::new(UnitSchema::default()),
                    Diagnostics {
                        shard_name: CATALOG_SHARD_NAME.to_string(),
                        handle_purpose: "compact catalog".to_string(),
                    },
                )
                .await
                .expect("invalid usage");
            let fuel = CATALOG_FORCE_COMPACTION_FUEL.handle(catalog.persist_client.dyncfgs());
            let wait = CATALOG_FORCE_COMPACTION_WAIT.handle(catalog.persist_client.dyncfgs());
            // We're going to gradually turn this on via dyncfgs. Run it in a task so that it
            // doesn't block startup.
            let _task = mz_ore::task::spawn(|| "catalog::force_shard_compaction", async move {
                let () =
                    mz_persist_client::cli::admin::dangerous_force_compaction_and_break_pushdown(
                        &write_handle,
                        || fuel.get(),
                        || wait.get(),
                    )
                    .await;
            });
        }

        Ok((Box::new(catalog), audit_log_handle))
    }

    /// Reports if the catalog state has been initialized.
    ///
    /// NOTE: This is the answer as of the last call to [`PersistHandle::sync`] or [`PersistHandle::sync_to_current_upper`],
    /// not necessarily what is currently in persist.
    #[mz_ore::instrument]
    fn is_initialized_inner(&self) -> bool {
        !self.update_applier.configs.is_empty()
    }

    /// Get the current value of config `key`.
    ///
    /// Some configs need to be read before the catalog is opened for bootstrapping.
    #[mz_ore::instrument]
    async fn get_current_config(&mut self, key: &str) -> Result<Option<u64>, DurableCatalogError> {
        self.sync_to_current_upper().await?;
        Ok(self.update_applier.configs.get(key).cloned())
    }

    /// Get the user version of this instance.
    ///
    /// The user version is used to determine if a migration is needed.
    #[mz_ore::instrument]
    pub(crate) async fn get_user_version(&mut self) -> Result<Option<u64>, DurableCatalogError> {
        self.get_current_config(USER_VERSION_KEY).await
    }

    /// Get the current value of setting `name`.
    ///
    /// Some settings need to be read before the catalog is opened for bootstrapping.
    #[mz_ore::instrument]
    async fn get_current_setting(
        &mut self,
        name: &str,
    ) -> Result<Option<String>, DurableCatalogError> {
        self.sync_to_current_upper().await?;
        Ok(self.update_applier.settings.get(name).cloned())
    }

    /// Get the catalog content version.
    ///
    /// The catalog content version is the semantic version of the most recent binary that wrote to
    /// the catalog.
    #[mz_ore::instrument]
    async fn get_catalog_content_version(
        &mut self,
    ) -> Result<Option<semver::Version>, DurableCatalogError> {
        let version = self
            .get_current_setting(CATALOG_CONTENT_VERSION_KEY)
            .await?;
        let version = version.map(|version| version.parse().expect("invalid version persisted"));
        Ok(version)
    }
}

#[async_trait]
impl OpenableDurableCatalogState for UnopenedPersistCatalogState {
    #[mz_ore::instrument]
    async fn open_savepoint(
        mut self: Box<Self>,
        initial_ts: Timestamp,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<(Box<dyn DurableCatalogState>, AuditLogIterator), CatalogError> {
        self.open_inner(Mode::Savepoint, initial_ts, bootstrap_args)
            .boxed()
            .await
    }

    #[mz_ore::instrument]
    async fn open_read_only(
        mut self: Box<Self>,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        self.open_inner(Mode::Readonly, EpochMillis::MIN.into(), bootstrap_args)
            .boxed()
            .await
            .map(|(catalog, _)| catalog)
    }

    #[mz_ore::instrument]
    async fn open(
        mut self: Box<Self>,
        initial_ts: Timestamp,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<(Box<dyn DurableCatalogState>, AuditLogIterator), CatalogError> {
        self.open_inner(Mode::Writable, initial_ts, bootstrap_args)
            .boxed()
            .await
    }

    #[mz_ore::instrument(level = "debug")]
    async fn open_debug(mut self: Box<Self>) -> Result<DebugCatalogState, CatalogError> {
        Ok(DebugCatalogState(*self))
    }

    #[mz_ore::instrument]
    async fn is_initialized(&mut self) -> Result<bool, CatalogError> {
        self.sync_to_current_upper().await?;
        Ok(self.is_initialized_inner())
    }

    #[mz_ore::instrument]
    async fn epoch(&mut self) -> Result<Epoch, CatalogError> {
        self.sync_to_current_upper().await?;
        self.fenceable_token
            .validate()?
            .map(|token| token.epoch)
            .ok_or(CatalogError::Durable(DurableCatalogError::Uninitialized))
    }

    #[mz_ore::instrument]
    async fn get_deployment_generation(&mut self) -> Result<u64, CatalogError> {
        self.sync_to_current_upper().await?;
        self.fenceable_token
            .token()
            .map(|token| token.deploy_generation)
            .ok_or(CatalogError::Durable(DurableCatalogError::Uninitialized))
    }

    #[mz_ore::instrument(level = "debug")]
    async fn get_enable_0dt_deployment(&mut self) -> Result<Option<bool>, CatalogError> {
        let value = self.get_current_config(ENABLE_0DT_DEPLOYMENT).await?;
        match value {
            None => Ok(None),
            Some(0) => Ok(Some(false)),
            Some(1) => Ok(Some(true)),
            Some(v) => Err(
                DurableCatalogError::from(TryFromProtoError::UnknownEnumVariant(format!(
                    "{v} is not a valid boolean value"
                )))
                .into(),
            ),
        }
    }

    #[mz_ore::instrument(level = "debug")]
    async fn get_0dt_deployment_max_wait(&mut self) -> Result<Option<Duration>, CatalogError> {
        let value = self
            .get_current_config(WITH_0DT_DEPLOYMENT_MAX_WAIT)
            .await?;
        match value {
            None => Ok(None),
            Some(millis) => Ok(Some(Duration::from_millis(millis))),
        }
    }

    #[mz_ore::instrument(level = "debug")]
    async fn get_0dt_deployment_ddl_check_interval(
        &mut self,
    ) -> Result<Option<Duration>, CatalogError> {
        let value = self
            .get_current_config(WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL)
            .await?;
        match value {
            None => Ok(None),
            Some(millis) => Ok(Some(Duration::from_millis(millis))),
        }
    }

    #[mz_ore::instrument(level = "debug")]
    async fn get_enable_0dt_deployment_panic_after_timeout(
        &mut self,
    ) -> Result<Option<bool>, CatalogError> {
        let value = self
            .get_current_config(ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT)
            .await?;
        match value {
            None => Ok(None),
            Some(0) => Ok(Some(false)),
            Some(1) => Ok(Some(true)),
            Some(v) => Err(
                DurableCatalogError::from(TryFromProtoError::UnknownEnumVariant(format!(
                    "{v} is not a valid boolean value"
                )))
                .into(),
            ),
        }
    }

    #[mz_ore::instrument]
    async fn has_system_config_synced_once(&mut self) -> Result<bool, DurableCatalogError> {
        self.get_current_config(SYSTEM_CONFIG_SYNCED_KEY)
            .await
            .map(|value| value.map(|value| value > 0).unwrap_or(false))
    }

    #[mz_ore::instrument]
    async fn trace_unconsolidated(&mut self) -> Result<Trace, CatalogError> {
        self.sync_to_current_upper().await?;
        if self.is_initialized_inner() {
            let snapshot = self.snapshot_unconsolidated().await;
            Ok(Trace::from_snapshot(snapshot))
        } else {
            Err(CatalogError::Durable(DurableCatalogError::Uninitialized))
        }
    }

    #[mz_ore::instrument]
    async fn trace_consolidated(&mut self) -> Result<Trace, CatalogError> {
        self.sync_to_current_upper().await?;
        if self.is_initialized_inner() {
            let snapshot = self.current_snapshot().await?;
            Ok(Trace::from_snapshot(snapshot))
        } else {
            Err(CatalogError::Durable(DurableCatalogError::Uninitialized))
        }
    }

    #[mz_ore::instrument(level = "debug")]
    async fn expire(self: Box<Self>) {
        self.expire().await
    }
}

/// Applies updates for an opened catalog.
#[derive(Debug)]
struct CatalogStateInner {
    /// A trace of all catalog updates that can be consumed by some higher layer.
    updates: VecDeque<memory::objects::StateUpdate>,
}

impl CatalogStateInner {
    fn new() -> CatalogStateInner {
        CatalogStateInner {
            updates: VecDeque::new(),
        }
    }
}

impl ApplyUpdate<StateUpdateKind> for CatalogStateInner {
    fn apply_update(
        &mut self,
        update: StateUpdate<StateUpdateKind>,
        current_fence_token: &mut FenceableToken,
        metrics: &Arc<Metrics>,
    ) -> Result<Option<StateUpdate<StateUpdateKind>>, FenceError> {
        if let Some(collection_type) = update.kind.collection_type() {
            metrics
                .collection_entries
                .with_label_values(&[&collection_type.to_string()])
                .add(update.diff.into_inner());
        }

        {
            let update: Option<memory::objects::StateUpdate> = (&update)
                .try_into()
                .expect("invalid persisted update: {update:#?}");
            if let Some(update) = update {
                self.updates.push_back(update);
            }
        }

        match (update.kind, update.diff) {
            (StateUpdateKind::AuditLog(_, ()), _) => Ok(None),
            // Nothing to due for fence token retractions but wait for the next insertion.
            (StateUpdateKind::FenceToken(_), Diff::MINUS_ONE) => Ok(None),
            (StateUpdateKind::FenceToken(token), Diff::ONE) => {
                current_fence_token.maybe_fence(token)?;
                Ok(None)
            }
            (kind, diff) => Ok(Some(StateUpdate {
                kind,
                ts: update.ts,
                diff,
            })),
        }
    }
}

/// A durable store of the catalog state using Persist as an implementation. The durable store can
/// serve any catalog data and transactionally modify catalog data.
///
/// Production users should call [`Self::expire`] before dropping a [`PersistCatalogState`]
/// so that it can expire its leases. If/when rust gets AsyncDrop, this will be done automatically.
type PersistCatalogState = PersistHandle<StateUpdateKind, CatalogStateInner>;

#[async_trait]
impl ReadOnlyDurableCatalogState for PersistCatalogState {
    fn epoch(&self) -> Epoch {
        self.fenceable_token
            .token()
            .expect("opened catalog state must have an epoch")
            .epoch
    }

    #[mz_ore::instrument(level = "debug")]
    async fn expire(self: Box<Self>) {
        self.expire().await
    }

    fn is_bootstrap_complete(&self) -> bool {
        self.bootstrap_complete
    }

    async fn get_audit_logs(&mut self) -> Result<Vec<VersionedEvent>, CatalogError> {
        self.sync_to_current_upper().await?;
        let audit_logs: Vec<_> = self
            .persist_snapshot()
            .await
            .filter_map(
                |StateUpdate {
                     kind,
                     ts: _,
                     diff: _,
                 }| match kind {
                    StateUpdateKind::AuditLog(key, ()) => Some(key),
                    _ => None,
                },
            )
            .collect();
        let mut audit_logs: Vec<_> = audit_logs
            .into_iter()
            .map(RustType::from_proto)
            .map_ok(|key: AuditLogKey| key.event)
            .collect::<Result<_, _>>()?;
        audit_logs.sort_by(|a, b| a.sortable_id().cmp(&b.sortable_id()));
        Ok(audit_logs)
    }

    #[mz_ore::instrument(level = "debug")]
    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, CatalogError> {
        self.with_trace(|trace| {
            Ok(trace
                .into_iter()
                .rev()
                .filter_map(|(kind, _, _)| match kind {
                    StateUpdateKind::IdAllocator(key, value) if key.name == id_type => {
                        Some(value.next_id)
                    }
                    _ => None,
                })
                .next()
                .expect("must exist"))
        })
        .await
    }

    #[mz_ore::instrument(level = "debug")]
    async fn get_deployment_generation(&mut self) -> Result<u64, CatalogError> {
        self.sync_to_current_upper().await?;
        Ok(self
            .fenceable_token
            .token()
            .expect("opened catalogs must have a token")
            .deploy_generation)
    }

    #[mz_ore::instrument(level = "debug")]
    async fn snapshot(&mut self) -> Result<Snapshot, CatalogError> {
        self.with_snapshot(Ok).await
    }

    #[mz_ore::instrument(level = "debug")]
    async fn sync_to_current_updates(
        &mut self,
    ) -> Result<Vec<memory::objects::StateUpdate>, CatalogError> {
        let upper = self.current_upper().await;
        self.sync_updates(upper).await
    }

    #[mz_ore::instrument(level = "debug")]
    async fn sync_updates(
        &mut self,
        target_upper: mz_repr::Timestamp,
    ) -> Result<Vec<memory::objects::StateUpdate>, CatalogError> {
        self.sync(target_upper).await?;
        let mut updates = Vec::new();
        while let Some(update) = self.update_applier.updates.front() {
            if update.ts >= target_upper {
                break;
            }

            let update = self
                .update_applier
                .updates
                .pop_front()
                .expect("peeked above");
            updates.push(update);
        }
        Ok(updates)
    }

    async fn current_upper(&mut self) -> Timestamp {
        self.current_upper().await
    }
}

#[async_trait]
#[allow(mismatched_lifetime_syntaxes)]
impl DurableCatalogState for PersistCatalogState {
    fn is_read_only(&self) -> bool {
        matches!(self.mode, Mode::Readonly)
    }

    fn is_savepoint(&self) -> bool {
        matches!(self.mode, Mode::Savepoint)
    }

    fn mark_bootstrap_complete(&mut self) {
        self.bootstrap_complete = true;
    }

    #[mz_ore::instrument(level = "debug")]
    async fn transaction(&mut self) -> Result<Transaction, CatalogError> {
        self.metrics.transactions_started.inc();
        let snapshot = self.snapshot().await?;
        let commit_ts = self.upper.clone();
        Transaction::new(self, snapshot, commit_ts)
    }

    #[mz_ore::instrument(level = "debug")]
    async fn commit_transaction(
        &mut self,
        txn_batch: TransactionBatch,
        commit_ts: Timestamp,
    ) -> Result<Timestamp, CatalogError> {
        async fn commit_transaction_inner(
            catalog: &mut PersistCatalogState,
            txn_batch: TransactionBatch,
            commit_ts: Timestamp,
        ) -> Result<Timestamp, CatalogError> {
            // If the current upper does not match the transaction's commit timestamp, then the
            // catalog must have changed since the transaction was started, making the transaction
            // invalid. When/if we want a multi-writer catalog, this will likely have to change
            // from an assert to a retry.
            assert_eq!(
                catalog.upper, txn_batch.upper,
                "only one transaction at a time is supported"
            );

            assert!(
                commit_ts >= catalog.upper,
                "expected commit ts, {}, to be greater than or equal to upper, {}",
                commit_ts,
                catalog.upper
            );

            let updates = StateUpdate::from_txn_batch(txn_batch).collect();
            debug!("committing updates: {updates:?}");

            let next_upper = match catalog.mode {
                Mode::Writable => catalog
                    .compare_and_append(updates, commit_ts)
                    .await
                    .map_err(|e| e.unwrap_fence_error())?,
                Mode::Savepoint => {
                    let updates = updates.into_iter().map(|(kind, diff)| StateUpdate {
                        kind,
                        ts: commit_ts,
                        diff,
                    });
                    catalog.apply_updates(updates)?;
                    catalog.upper = commit_ts.step_forward();
                    catalog.upper
                }
                Mode::Readonly => {
                    // If the transaction is empty then we don't error, even in read-only mode.
                    // This is mostly for legacy reasons (i.e. with enough elbow grease this
                    // behavior can be changed without breaking any fundamental assumptions).
                    if !updates.is_empty() {
                        return Err(DurableCatalogError::NotWritable(format!(
                            "cannot commit a transaction in a read-only catalog: {updates:#?}"
                        ))
                        .into());
                    }
                    catalog.upper
                }
            };

            Ok(next_upper)
        }
        self.metrics.transaction_commits.inc();
        let counter = self.metrics.transaction_commit_latency_seconds.clone();
        commit_transaction_inner(self, txn_batch, commit_ts)
            .wall_time()
            .inc_by(counter)
            .await
    }

    #[mz_ore::instrument(level = "debug")]
    async fn confirm_leadership(&mut self) -> Result<(), CatalogError> {
        // Read only catalog does not care about leadership.
        if self.is_read_only() {
            return Ok(());
        }
        self.sync_to_current_upper().await?;
        Ok(())
    }
}

/// Deterministically generate a shard ID for the given `organization_id` and `seed`.
pub fn shard_id(organization_id: Uuid, seed: usize) -> ShardId {
    let hash = sha2::Sha256::digest(format!("{organization_id}{seed}")).to_vec();
    soft_assert_eq_or_log!(hash.len(), 32, "SHA256 returns 32 bytes (256 bits)");
    let uuid = Uuid::from_slice(&hash[0..16]).expect("from_slice accepts exactly 16 bytes");
    ShardId::from_str(&format!("s{uuid}")).expect("known to be valid")
}

/// Returns the schema of the `Row`s/`SourceData`s stored in the persist
/// shard backing the catalog.
fn desc() -> RelationDesc {
    RelationDesc::builder()
        .with_column("data", SqlScalarType::Jsonb.nullable(false))
        .finish()
}

/// Generates a timestamp for reading from `read_handle` that is as fresh as possible, given
/// `upper`.
fn as_of(
    read_handle: &ReadHandle<SourceData, (), Timestamp, StorageDiff>,
    upper: Timestamp,
) -> Timestamp {
    let since = read_handle.since().clone();
    let mut as_of = upper.checked_sub(1).unwrap_or_else(|| {
        panic!("catalog persist shard should be initialize, found upper: {upper:?}")
    });
    // We only downgrade the since after writing, and we always set the since to one less than the
    // upper.
    soft_assert_or_log!(
        since.less_equal(&as_of),
        "since={since:?}, as_of={as_of:?}; since must be less than or equal to as_of"
    );
    // This should be a no-op if the assert above passes, however if it doesn't then we'd like to
    // continue with a correct timestamp instead of entering a panic loop.
    as_of.advance_by(since.borrow());
    as_of
}

/// Fetch the persist version of the catalog upgrade shard, if one exists. A version will not
/// exist if we are creating a brand-new environment.
async fn fetch_catalog_upgrade_shard_version(
    persist_client: &PersistClient,
    upgrade_shard_id: ShardId,
) -> Option<semver::Version> {
    let shard_state = persist_client
        .inspect_shard::<Timestamp>(&upgrade_shard_id)
        .await
        .ok()?;
    let json_state = serde_json::to_value(shard_state).expect("state serialization error");
    let upgrade_version = json_state
        .get("applier_version")
        .cloned()
        .expect("missing applier_version");
    let upgrade_version =
        serde_json::from_value(upgrade_version).expect("version deserialization error");
    Some(upgrade_version)
}

/// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
/// state up to, and including, `as_of`.
///
/// The output is consolidated and sorted by timestamp in ascending order.
#[mz_ore::instrument(level = "debug")]
async fn snapshot_binary(
    read_handle: &mut ReadHandle<SourceData, (), Timestamp, StorageDiff>,
    as_of: Timestamp,
    metrics: &Arc<Metrics>,
) -> impl Iterator<Item = StateUpdate<StateUpdateKindJson>> + DoubleEndedIterator + use<> {
    metrics.snapshots_taken.inc();
    let counter = metrics.snapshot_latency_seconds.clone();
    snapshot_binary_inner(read_handle, as_of)
        .wall_time()
        .inc_by(counter)
        .await
}

/// Generates an iterator of [`StateUpdate`] that contain all updates to the catalog
/// state up to, and including, `as_of`.
///
/// The output is consolidated and sorted by timestamp in ascending order.
#[mz_ore::instrument(level = "debug")]
async fn snapshot_binary_inner(
    read_handle: &mut ReadHandle<SourceData, (), Timestamp, StorageDiff>,
    as_of: Timestamp,
) -> impl Iterator<Item = StateUpdate<StateUpdateKindJson>> + DoubleEndedIterator + use<> {
    let snapshot = read_handle
        .snapshot_and_fetch(Antichain::from_elem(as_of))
        .await
        .expect("we have advanced the restart_as_of by the since");
    soft_assert_no_log!(
        snapshot.iter().all(|(_, _, diff)| *diff == 1),
        "snapshot_and_fetch guarantees a consolidated result: {snapshot:#?}"
    );
    snapshot
        .into_iter()
        .map(Into::<StateUpdate<StateUpdateKindJson>>::into)
        .sorted_by(|a, b| Ord::cmp(&b.ts, &a.ts))
}

/// Convert an [`Antichain<Timestamp>`] to a [`Timestamp`].
///
/// The correctness of this function relies on [`Timestamp`] being totally ordered and never
/// finalizing the catalog shard.
pub(crate) fn antichain_to_timestamp(antichain: Antichain<Timestamp>) -> Timestamp {
    antichain
        .into_option()
        .expect("we use a totally ordered time and never finalize the shard")
}

// Debug methods used by the catalog-debug tool.

impl Trace {
    /// Generates a [`Trace`] from snapshot.
    fn from_snapshot(snapshot: impl IntoIterator<Item = StateUpdate>) -> Trace {
        let mut trace = Trace::new();
        for StateUpdate { kind, ts, diff } in snapshot {
            match kind {
                StateUpdateKind::AuditLog(k, v) => trace.audit_log.values.push(((k, v), ts, diff)),
                StateUpdateKind::Cluster(k, v) => trace.clusters.values.push(((k, v), ts, diff)),
                StateUpdateKind::ClusterReplica(k, v) => {
                    trace.cluster_replicas.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::Comment(k, v) => trace.comments.values.push(((k, v), ts, diff)),
                StateUpdateKind::Config(k, v) => trace.configs.values.push(((k, v), ts, diff)),
                StateUpdateKind::Database(k, v) => trace.databases.values.push(((k, v), ts, diff)),
                StateUpdateKind::DefaultPrivilege(k, v) => {
                    trace.default_privileges.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::FenceToken(_) => {
                    // Fence token not included in trace.
                }
                StateUpdateKind::IdAllocator(k, v) => {
                    trace.id_allocator.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::IntrospectionSourceIndex(k, v) => {
                    trace.introspection_sources.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::Item(k, v) => trace.items.values.push(((k, v), ts, diff)),
                StateUpdateKind::NetworkPolicy(k, v) => {
                    trace.network_policies.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::Role(k, v) => trace.roles.values.push(((k, v), ts, diff)),
                StateUpdateKind::Schema(k, v) => trace.schemas.values.push(((k, v), ts, diff)),
                StateUpdateKind::Setting(k, v) => trace.settings.values.push(((k, v), ts, diff)),
                StateUpdateKind::SourceReferences(k, v) => {
                    trace.source_references.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::SystemConfiguration(k, v) => {
                    trace.system_configurations.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::SystemObjectMapping(k, v) => {
                    trace.system_object_mappings.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::SystemPrivilege(k, v) => {
                    trace.system_privileges.values.push(((k, v), ts, diff))
                }
                StateUpdateKind::StorageCollectionMetadata(k, v) => trace
                    .storage_collection_metadata
                    .values
                    .push(((k, v), ts, diff)),
                StateUpdateKind::UnfinalizedShard(k, ()) => {
                    trace.unfinalized_shards.values.push(((k, ()), ts, diff))
                }
                StateUpdateKind::TxnWalShard((), v) => {
                    trace.txn_wal_shard.values.push((((), v), ts, diff))
                }
                StateUpdateKind::RoleAuth(k, v) => trace.role_auth.values.push(((k, v), ts, diff)),
            }
        }
        trace
    }
}

impl UnopenedPersistCatalogState {
    /// Manually update value of `key` in collection `T` to `value`.
    #[mz_ore::instrument]
    pub(crate) async fn debug_edit<T: Collection>(
        &mut self,
        key: T::Key,
        value: T::Value,
    ) -> Result<Option<T::Value>, CatalogError>
    where
        T::Key: PartialEq + Eq + Debug + Clone,
        T::Value: Debug + Clone,
    {
        let prev_value = loop {
            let key = key.clone();
            let value = value.clone();
            let snapshot = self.current_snapshot().await?;
            let trace = Trace::from_snapshot(snapshot);
            let collection_trace = T::collection_trace(trace);
            let prev_values: Vec<_> = collection_trace
                .values
                .into_iter()
                .filter(|((k, _), _, diff)| {
                    soft_assert_eq_or_log!(*diff, Diff::ONE, "trace is consolidated");
                    &key == k
                })
                .collect();

            let prev_value = match &prev_values[..] {
                [] => None,
                [((_, v), _, _)] => Some(v.clone()),
                prev_values => panic!("multiple values found for key {key:?}: {prev_values:?}"),
            };

            let mut updates: Vec<_> = prev_values
                .into_iter()
                .map(|((k, v), _, _)| (T::update(k, v), Diff::MINUS_ONE))
                .collect();
            updates.push((T::update(key, value), Diff::ONE));
            // We must fence out all other catalogs, if we haven't already, since we are writing.
            match self.fenceable_token.generate_unfenced_token(self.mode)? {
                Some((fence_updates, current_fenceable_token)) => {
                    updates.extend(fence_updates.clone());
                    match self.compare_and_append(updates, self.upper).await {
                        Ok(_) => {
                            self.fenceable_token = current_fenceable_token;
                            break prev_value;
                        }
                        Err(CompareAndAppendError::Fence(e)) => return Err(e.into()),
                        Err(e @ CompareAndAppendError::UpperMismatch { .. }) => {
                            warn!("catalog write failed due to upper mismatch, retrying: {e:?}");
                            continue;
                        }
                    }
                }
                None => {
                    self.compare_and_append(updates, self.upper)
                        .await
                        .map_err(|e| e.unwrap_fence_error())?;
                    break prev_value;
                }
            }
        };
        Ok(prev_value)
    }

    /// Manually delete `key` from collection `T`.
    #[mz_ore::instrument]
    pub(crate) async fn debug_delete<T: Collection>(
        &mut self,
        key: T::Key,
    ) -> Result<(), CatalogError>
    where
        T::Key: PartialEq + Eq + Debug + Clone,
        T::Value: Debug,
    {
        loop {
            let key = key.clone();
            let snapshot = self.current_snapshot().await?;
            let trace = Trace::from_snapshot(snapshot);
            let collection_trace = T::collection_trace(trace);
            let mut retractions: Vec<_> = collection_trace
                .values
                .into_iter()
                .filter(|((k, _), _, diff)| {
                    soft_assert_eq_or_log!(*diff, Diff::ONE, "trace is consolidated");
                    &key == k
                })
                .map(|((k, v), _, _)| (T::update(k, v), Diff::MINUS_ONE))
                .collect();

            // We must fence out all other catalogs, if we haven't already, since we are writing.
            match self.fenceable_token.generate_unfenced_token(self.mode)? {
                Some((fence_updates, current_fenceable_token)) => {
                    retractions.extend(fence_updates.clone());
                    match self.compare_and_append(retractions, self.upper).await {
                        Ok(_) => {
                            self.fenceable_token = current_fenceable_token;
                            break;
                        }
                        Err(CompareAndAppendError::Fence(e)) => return Err(e.into()),
                        Err(e @ CompareAndAppendError::UpperMismatch { .. }) => {
                            warn!("catalog write failed due to upper mismatch, retrying: {e:?}");
                            continue;
                        }
                    }
                }
                None => {
                    self.compare_and_append(retractions, self.upper)
                        .await
                        .map_err(|e| e.unwrap_fence_error())?;
                    break;
                }
            }
        }
        Ok(())
    }

    /// Generates a [`Vec<StateUpdate>`] that contain all updates to the catalog
    /// state.
    ///
    /// The output is consolidated and sorted by timestamp in ascending order and the current upper.
    async fn current_snapshot(
        &mut self,
    ) -> Result<impl IntoIterator<Item = StateUpdate> + '_, CatalogError> {
        self.sync_to_current_upper().await?;
        self.consolidate();
        Ok(self.snapshot.iter().cloned().map(|(kind, ts, diff)| {
            let kind = TryIntoStateUpdateKind::try_into(kind).expect("kind decoding error");
            StateUpdate { kind, ts, diff }
        }))
    }
}
