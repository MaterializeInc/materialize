// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to deriving controller commands from [catalog
//! changes](ParsedStateUpdate) and applying them to the controller(s).
//!
//! The flow from "raw" catalog changes to [ControllerCommand] works like this:
//!
//! StateUpdateKind -> ParsedStateUpdate -> ControllerCommand
//!
//! [ParsedStateUpdate] adds context to a "raw" catalog change
//! ([StateUpdateKind](mz_catalog::memory::objects::StateUpdateKind)). It
//! includes an in-memory representation of the updated object, which can in
//! theory be derived from the raw change but only when we have access to all
//! the other raw changes or to an in-memory Catalog, which represents a
//! "rollup" of all the raw changes.
//!
//! [ControllerCommand] is both the state machine that we use for absorbing
//! multiple state updates for the same object and the final command that has to
//! be applied to the controller(s) after absorbing all the state updates in a
//! given batch of updates.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::consolidation::consolidate_updates;
use fail::fail_point;
use itertools::Itertools;
use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::builtin;
use mz_catalog::memory::objects::{
    CatalogItem, Cluster, ClusterReplica, Connection, ContinualTask, DataSourceDesc, Index,
    MaterializedView, Secret, Sink, Source, StateDiff, Table, TableDataSource, View,
};
use mz_compute_client::protocol::response::PeekResponse;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::collections::CollectionExt;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_ore::instrument;
use mz_ore::retry::Retry;
use mz_ore::str::StrExt;
use mz_ore::task;
use mz_repr::{CatalogItemId, GlobalId, RelationVersion, RelationVersionSelector, Timestamp};
use mz_sql::plan::ConnectionDetails;
use mz_storage_client::controller::{CollectionDescription, DataSource};
use mz_storage_types::connections::PostgresConnection;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::sources::GenericSourceConnection;
use tracing::{Instrument, info_span, warn};

use crate::active_compute_sink::ActiveComputeSinkRetireReason;
use crate::coord::Coordinator;
use crate::coord::controller_commands::parsed_state_updates::{
    ParsedStateUpdate, ParsedStateUpdateKind,
};
use crate::coord::statement_logging::StatementLoggingId;
use crate::coord::timeline::TimelineState;
use crate::statement_logging::StatementEndedExecutionReason;
use crate::{AdapterError, CollectionIdBundle, ExecuteContext, ResultExt};

pub mod parsed_state_updates;

impl Coordinator {
    #[instrument(level = "debug")]
    pub async fn controller_apply_catalog_updates(
        &mut self,
        ctx: Option<&mut ExecuteContext>,
        catalog_updates: Vec<ParsedStateUpdate>,
    ) -> Result<(), AdapterError> {
        let mut controller_commands: BTreeMap<CatalogItemId, ControllerCommand> = BTreeMap::new();
        let mut cluster_commands: BTreeMap<ClusterId, ControllerCommand> = BTreeMap::new();
        let mut cluster_replica_commands: BTreeMap<(ClusterId, ReplicaId), ControllerCommand> =
            BTreeMap::new();

        let catalog_updates = Self::consolidate_updates(catalog_updates);

        for update in catalog_updates {
            tracing::trace!(?update, "got parsed state update");
            match &update.kind {
                ParsedStateUpdateKind::Item {
                    durable_item,
                    parsed_item: _,
                    connection: _,
                    parsed_full_name: _,
                } => {
                    let entry = controller_commands
                        .entry(durable_item.id.clone())
                        .or_insert_with(|| ControllerCommand::None);
                    entry.absorb(update);
                }
                ParsedStateUpdateKind::TemporaryItem {
                    parsed_item,
                    connection: _,
                    parsed_full_name: _,
                } => {
                    let entry = controller_commands
                        .entry(parsed_item.id.clone())
                        .or_insert_with(|| ControllerCommand::None);
                    entry.absorb(update);
                }
                ParsedStateUpdateKind::Cluster {
                    durable_cluster,
                    parsed_cluster: _,
                } => {
                    let entry = cluster_commands
                        .entry(durable_cluster.id)
                        .or_insert_with(|| ControllerCommand::None);
                    entry.absorb(update.clone());
                }
                ParsedStateUpdateKind::ClusterReplica {
                    durable_cluster_replica,
                    parsed_cluster_replica: _,
                } => {
                    let entry = cluster_replica_commands
                        .entry((
                            durable_cluster_replica.cluster_id,
                            durable_cluster_replica.replica_id,
                        ))
                        .or_insert_with(|| ControllerCommand::None);
                    entry.absorb(update.clone());
                }
            }
        }

        Ok(())
    }

    /// It can happen that the sequencing logic creates "fluctuating" updates
    /// for a given catalog ID. For example, when doing a `DROP OWNED BY ...`,
    /// for a table, there will be a retraction of the original table state,
    /// then an addition for the same table but stripped of some of the roles
    /// and access things, and then a retraction for that intermediate table
    /// state. By consolidating, the intermediate state addition/retraction will
    /// cancel out and we'll only see the retraction for the original state.
    fn consolidate_updates(catalog_updates: Vec<ParsedStateUpdate>) -> Vec<ParsedStateUpdate> {
        let mut updates: Vec<(ParsedStateUpdateKind, Timestamp, mz_repr::Diff)> = catalog_updates
            .into_iter()
            .map(|update| (update.kind, update.ts, update.diff.into()))
            .collect_vec();

        consolidate_updates(&mut updates);

        updates
            .into_iter()
            .filter(|(_kind, _ts, diff)| *diff != 0.into())
            .map(|(kind, ts, diff)| ParsedStateUpdate {
                kind,
                ts,
                diff: diff
                    .try_into()
                    .expect("catalog state cannot have diff other than -1 or 1"),
            })
            .collect_vec()
    }
}

/// A state machine for building controller commands from catalog updates.
///
/// Once all [ParsedStateUpdate] of a timestamp are ingested these are a command
/// that has to be applied to the controller(s).
#[derive(Debug, Clone)]
enum ControllerCommand {
    None,
    Table(ControllerCommandKind<Table>),
    Source(ControllerCommandKind<(Source, Option<GenericSourceConnection>)>),
    Sink(ControllerCommandKind<Sink>),
    Index(ControllerCommandKind<Index>),
    MaterializedView(ControllerCommandKind<MaterializedView>),
    View(ControllerCommandKind<View>),
    ContinualTask(ControllerCommandKind<ContinualTask>),
    Secret(ControllerCommandKind<Secret>),
    Connection(ControllerCommandKind<Connection>),
    Cluster(ControllerCommandKind<Cluster>),
    ClusterReplica(ControllerCommandKind<ClusterReplica>),
}

#[derive(Debug, Clone)]
enum ControllerCommandKind<T> {
    /// No operations seen yet.
    None,
    /// Item was added.
    Added(T),
    /// Item was dropped (with its name retained for error messages).
    Dropped(T, String),
    /// Item is being altered from one state to another.
    Altered { prev: T, new: T },
}

impl<T: Clone> ControllerCommandKind<T> {
    /// Apply a state transition based on a diff. Returns an error message if
    /// the transition is invalid.
    fn transition(&mut self, item: T, name: Option<String>, diff: StateDiff) -> Result<(), String> {
        use ControllerCommandKind::*;
        use StateDiff::*;

        let new_state = match (&*self, diff) {
            // Initial state transitions
            (None, Addition) => Added(item),
            (None, Retraction) => Dropped(item, name.unwrap_or_else(|| "<unknown>".to_string())),

            // From Added state
            (Added(existing), Retraction) => {
                // Add -> Drop means the item is being altered
                Altered {
                    prev: item,
                    new: existing.clone(),
                }
            }
            (Added(_), Addition) => {
                return Err("Cannot add an already added object".to_string());
            }

            // From Dropped state
            (Dropped(existing, _), Addition) => {
                // Drop -> Add means the item is being altered
                Altered {
                    prev: existing.clone(),
                    new: item,
                }
            }
            (Dropped(_, _), Retraction) => {
                return Err("Cannot drop an already dropped object".to_string());
            }

            // From Altered state
            (Altered { .. }, _) => {
                return Err(format!(
                    "Cannot apply {:?} to an object in Altered state",
                    diff
                ));
            }
        };

        *self = new_state;
        Ok(())
    }
}

/// Macro to generate absorb methods for each item type.
macro_rules! impl_absorb_method {
    (
        $method_name:ident,
        $variant:ident,
        $item_type:ty
    ) => {
        fn $method_name(
            &mut self,
            item: $item_type,
            parsed_full_name: Option<String>,
            _ts: Timestamp,
            diff: StateDiff,
        ) {
            let state = match self {
                ControllerCommand::$variant(state) => state,
                ControllerCommand::None => {
                    *self = ControllerCommand::$variant(ControllerCommandKind::None);
                    match self {
                        ControllerCommand::$variant(state) => state,
                        _ => unreachable!(),
                    }
                }
                _ => {
                    panic!(
                        "Unexpected command type for {:?}: {} {:?}",
                        self,
                        stringify!($variant),
                        diff,
                    );
                }
            };

            if let Err(e) = state.transition(item, parsed_full_name, diff) {
                panic!(
                    "Invalid state transition for {}: {}",
                    stringify!($variant),
                    e
                );
            }
        }
    };
}

impl ControllerCommand {
    /// Absorbs the given catalog update into this [ControllerCommand], causing
    /// a state transition or error.
    fn absorb(&mut self, catalog_update: ParsedStateUpdate) {
        match catalog_update.kind {
            ParsedStateUpdateKind::Item {
                durable_item: _,
                parsed_item,
                connection,
                parsed_full_name,
            } => match parsed_item {
                CatalogItem::Table(table) => self.absorb_table(
                    table,
                    Some(parsed_full_name),
                    catalog_update.ts,
                    catalog_update.diff,
                ),
                CatalogItem::Source(source) => {
                    self.absorb_source(
                        (source, connection),
                        Some(parsed_full_name),
                        catalog_update.ts,
                        catalog_update.diff,
                    );
                }
                CatalogItem::Sink(sink) => {
                    self.absorb_sink(
                        sink,
                        Some(parsed_full_name),
                        catalog_update.ts,
                        catalog_update.diff,
                    );
                }
                CatalogItem::Index(index) => {
                    self.absorb_index(
                        index,
                        Some(parsed_full_name),
                        catalog_update.ts,
                        catalog_update.diff,
                    );
                }
                CatalogItem::MaterializedView(mv) => {
                    self.absorb_materialized_view(
                        mv,
                        Some(parsed_full_name),
                        catalog_update.ts,
                        catalog_update.diff,
                    );
                }
                CatalogItem::View(view) => {
                    self.absorb_view(
                        view,
                        Some(parsed_full_name),
                        catalog_update.ts,
                        catalog_update.diff,
                    );
                }
                CatalogItem::ContinualTask(ct) => {
                    self.absorb_continual_task(ct, None, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Secret(secret) => {
                    self.absorb_secret(secret, None, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Connection(connection) => {
                    self.absorb_connection(
                        connection,
                        None,
                        catalog_update.ts,
                        catalog_update.diff,
                    );
                }
                CatalogItem::Log(_) => {}
                CatalogItem::Type(_) => {}
                CatalogItem::Func(_) => {}
            },
            ParsedStateUpdateKind::TemporaryItem {
                parsed_item,
                connection,
                parsed_full_name,
            } => match parsed_item.item {
                CatalogItem::Table(table) => self.absorb_table(
                    table,
                    Some(parsed_full_name),
                    catalog_update.ts,
                    catalog_update.diff,
                ),
                CatalogItem::Source(source) => {
                    self.absorb_source(
                        (source, connection),
                        Some(parsed_full_name),
                        catalog_update.ts,
                        catalog_update.diff,
                    );
                }
                CatalogItem::Sink(sink) => {
                    self.absorb_sink(
                        sink,
                        Some(parsed_full_name),
                        catalog_update.ts,
                        catalog_update.diff,
                    );
                }
                CatalogItem::Index(index) => {
                    self.absorb_index(
                        index,
                        Some(parsed_full_name),
                        catalog_update.ts,
                        catalog_update.diff,
                    );
                }
                CatalogItem::MaterializedView(mv) => {
                    self.absorb_materialized_view(
                        mv,
                        Some(parsed_full_name),
                        catalog_update.ts,
                        catalog_update.diff,
                    );
                }
                CatalogItem::View(view) => {
                    self.absorb_view(
                        view,
                        Some(parsed_full_name),
                        catalog_update.ts,
                        catalog_update.diff,
                    );
                }
                CatalogItem::ContinualTask(ct) => {
                    self.absorb_continual_task(ct, None, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Secret(secret) => {
                    self.absorb_secret(secret, None, catalog_update.ts, catalog_update.diff);
                }
                CatalogItem::Connection(connection) => {
                    self.absorb_connection(
                        connection,
                        None,
                        catalog_update.ts,
                        catalog_update.diff,
                    );
                }
                CatalogItem::Log(_) => {}
                CatalogItem::Type(_) => {}
                CatalogItem::Func(_) => {}
            },
            ParsedStateUpdateKind::Cluster {
                durable_cluster: _,
                parsed_cluster,
            } => {
                self.absorb_cluster(parsed_cluster, catalog_update.ts, catalog_update.diff);
            }
            ParsedStateUpdateKind::ClusterReplica {
                durable_cluster_replica: _,
                parsed_cluster_replica,
            } => {
                self.absorb_cluster_replica(
                    parsed_cluster_replica,
                    catalog_update.ts,
                    catalog_update.diff,
                );
            }
        }
    }

    impl_absorb_method!(absorb_table, Table, Table);
    impl_absorb_method!(
        absorb_source,
        Source,
        (Source, Option<GenericSourceConnection>)
    );
    impl_absorb_method!(absorb_sink, Sink, Sink);
    impl_absorb_method!(absorb_index, Index, Index);
    impl_absorb_method!(absorb_materialized_view, MaterializedView, MaterializedView);
    impl_absorb_method!(absorb_view, View, View);

    impl_absorb_method!(absorb_continual_task, ContinualTask, ContinualTask);
    impl_absorb_method!(absorb_secret, Secret, Secret);
    impl_absorb_method!(absorb_connection, Connection, Connection);

    // Special case for cluster which uses the cluster's name field.
    fn absorb_cluster(&mut self, cluster: Cluster, _ts: Timestamp, diff: StateDiff) {
        let state = match self {
            ControllerCommand::Cluster(state) => state,
            ControllerCommand::None => {
                *self = ControllerCommand::Cluster(ControllerCommandKind::None);
                match self {
                    ControllerCommand::Cluster(state) => state,
                    _ => unreachable!(),
                }
            }
            _ => {
                panic!("Unexpected command type for {:?}: Cluster {:?}", self, diff);
            }
        };

        if let Err(e) = state.transition(cluster.clone(), Some(cluster.name), diff) {
            panic!("invalid state transition for cluster: {}", e);
        }
    }

    // Special case for cluster replica which uses the cluster replica's name field.
    fn absorb_cluster_replica(
        &mut self,
        cluster_replica: ClusterReplica,
        _ts: Timestamp,
        diff: StateDiff,
    ) {
        let state = match self {
            ControllerCommand::ClusterReplica(state) => state,
            ControllerCommand::None => {
                *self = ControllerCommand::ClusterReplica(ControllerCommandKind::None);
                match self {
                    ControllerCommand::ClusterReplica(state) => state,
                    _ => unreachable!(),
                }
            }
            _ => {
                panic!(
                    "Unexpected command type for {:?}: ClusterReplica {:?}",
                    self, diff
                );
            }
        };

        if let Err(e) = state.transition(cluster_replica.clone(), Some(cluster_replica.name), diff)
        {
            panic!("invalid state transition for cluster replica: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::{GlobalId, RelationDesc, RelationVersion, VersionedRelationDesc};
    use mz_sql::names::ResolvedIds;
    use std::collections::BTreeMap;

    fn create_test_table(name: &str) -> Table {
        Table {
            desc: VersionedRelationDesc::new(
                RelationDesc::builder()
                    .with_column(name, mz_repr::SqlScalarType::String.nullable(false))
                    .finish(),
            ),
            create_sql: None,
            collections: BTreeMap::from([(RelationVersion::root(), GlobalId::System(1))]),
            conn_id: None,
            resolved_ids: ResolvedIds::empty(),
            custom_logical_compaction_window: None,
            is_retained_metrics_object: false,
            data_source: TableDataSource::TableWrites { defaults: vec![] },
        }
    }

    #[mz_ore::test]
    fn test_item_state_transitions() {
        // Test None -> Added
        let mut state = ControllerCommandKind::None;
        assert!(
            state
                .transition("item1".to_string(), None, StateDiff::Addition)
                .is_ok()
        );
        assert!(matches!(state, ControllerCommandKind::Added(_)));

        // Test Added -> Altered (via retraction)
        let mut state = ControllerCommandKind::Added("new_item".to_string());
        assert!(
            state
                .transition("old_item".to_string(), None, StateDiff::Retraction)
                .is_ok()
        );
        match &state {
            ControllerCommandKind::Altered { prev, new } => {
                // The retracted item is the OLD state
                assert_eq!(prev, "old_item");
                // The existing Added item is the NEW state
                assert_eq!(new, "new_item");
            }
            _ => panic!("Expected Altered state"),
        }

        // Test None -> Dropped
        let mut state = ControllerCommandKind::None;
        assert!(
            state
                .transition(
                    "item1".to_string(),
                    Some("test_name".to_string()),
                    StateDiff::Retraction
                )
                .is_ok()
        );
        assert!(matches!(state, ControllerCommandKind::Dropped(_, _)));

        // Test Dropped -> Altered (via addition)
        let mut state = ControllerCommandKind::Dropped("old_item".to_string(), "name".to_string());
        assert!(
            state
                .transition("new_item".to_string(), None, StateDiff::Addition)
                .is_ok()
        );
        match &state {
            ControllerCommandKind::Altered { prev, new } => {
                // The existing Dropped item is the OLD state
                assert_eq!(prev, "old_item");
                // The added item is the NEW state
                assert_eq!(new, "new_item");
            }
            _ => panic!("Expected Altered state"),
        }

        // Test invalid transitions
        let mut state = ControllerCommandKind::Added("item".to_string());
        assert!(
            state
                .transition("item2".to_string(), None, StateDiff::Addition)
                .is_err()
        );

        let mut state = ControllerCommandKind::Dropped("item".to_string(), "name".to_string());
        assert!(
            state
                .transition("item2".to_string(), None, StateDiff::Retraction)
                .is_err()
        );
    }

    #[mz_ore::test]
    fn test_table_absorb_state_machine() {
        let table1 = create_test_table("table1");
        let table2 = create_test_table("table2");

        // Test None -> AddTable
        let mut cmd = ControllerCommand::None;
        cmd.absorb_table(
            table1.clone(),
            Some("schema.table1".to_string()),
            Timestamp::MIN,
            StateDiff::Addition,
        );
        // Check that we have an Added state
        match &cmd {
            ControllerCommand::Table(state) => match state {
                ControllerCommandKind::Added(t) => {
                    assert_eq!(t.desc.latest().arity(), table1.desc.latest().arity())
                }
                _ => panic!("Expected Added state"),
            },
            _ => panic!("Expected Table command"),
        }

        // Test AddTable -> AlterTable (via retraction)
        // This tests the bug fix: when we have AddTable(table1) and receive Retraction(table2),
        // table2 is the old state being removed, table1 is the new state
        cmd.absorb_table(
            table2.clone(),
            Some("schema.table2".to_string()),
            Timestamp::MIN,
            StateDiff::Retraction,
        );
        match &cmd {
            ControllerCommand::Table(state) => match state {
                ControllerCommandKind::Altered { prev, new } => {
                    // Verify the fix: prev should be the retracted table, new should be the added table
                    assert_eq!(prev.desc.latest().arity(), table2.desc.latest().arity());
                    assert_eq!(new.desc.latest().arity(), table1.desc.latest().arity());
                }
                _ => panic!("Expected Altered state"),
            },
            _ => panic!("Expected Table command"),
        }

        // Test None -> DropTable
        let mut cmd = ControllerCommand::None;
        cmd.absorb_table(
            table1.clone(),
            Some("schema.table1".to_string()),
            Timestamp::MIN,
            StateDiff::Retraction,
        );
        match &cmd {
            ControllerCommand::Table(state) => match state {
                ControllerCommandKind::Dropped(t, name) => {
                    assert_eq!(t.desc.latest().arity(), table1.desc.latest().arity());
                    assert_eq!(name, "schema.table1");
                }
                _ => panic!("Expected Dropped state"),
            },
            _ => panic!("Expected Table command"),
        }

        // Test DropTable -> AlterTable (via addition)
        cmd.absorb_table(
            table2.clone(),
            Some("schema.table2".to_string()),
            Timestamp::MIN,
            StateDiff::Addition,
        );
        match &cmd {
            ControllerCommand::Table(state) => match state {
                ControllerCommandKind::Altered { prev, new } => {
                    // prev should be the dropped table, new should be the added table
                    assert_eq!(prev.desc.latest().arity(), table1.desc.latest().arity());
                    assert_eq!(new.desc.latest().arity(), table2.desc.latest().arity());
                }
                _ => panic!("Expected Altered state"),
            },
            _ => panic!("Expected Table command"),
        }
    }

    #[mz_ore::test]
    #[should_panic(expected = "Cannot add an already added object")]
    fn test_invalid_double_add() {
        let table = create_test_table("table");
        let mut cmd = ControllerCommand::None;

        // First addition
        cmd.absorb_table(
            table.clone(),
            Some("schema.table".to_string()),
            Timestamp::MIN,
            StateDiff::Addition,
        );

        // Second addition should panic
        cmd.absorb_table(
            table.clone(),
            Some("schema.table".to_string()),
            Timestamp::MIN,
            StateDiff::Addition,
        );
    }

    #[mz_ore::test]
    #[should_panic(expected = "Cannot drop an already dropped object")]
    fn test_invalid_double_drop() {
        let table = create_test_table("table");
        let mut cmd = ControllerCommand::None;

        // First drop
        cmd.absorb_table(
            table.clone(),
            Some("schema.table".to_string()),
            Timestamp::MIN,
            StateDiff::Retraction,
        );

        // Second drop should panic
        cmd.absorb_table(
            table.clone(),
            Some("schema.table".to_string()),
            Timestamp::MIN,
            StateDiff::Retraction,
        );
    }
}
