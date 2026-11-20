// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The durable form of an LIR dataflow.
//!
//! A [`PinnedDataflow`] holds the durable plan for a dataflow. It is the
//! root of the stable LIR schema traced in `tests/lir_schema.rs`, so every
//! type reachable from it is part of the frozen serialization surface.
//!
//! We try to avoid pinning things that can be re-derived from the catalog
//! when loading the plan. This means keeping track of assumptions about
//! inputs but only the wiring for outputs. The rest can be recomputed.
//!
//! Loading a pinned plan is called _instantiation_, and it is done by means
//! of an [`InstantiationContext`].
//!
//! Only dataflows behind catalog items can be pinned: indexes, materialized
//! views, and metric sinks. Subscribes and one-shot copies have no durable
//! identity to pin under, and the [`TryFrom`] conversion rejects them, along
//! with any export under a transient id (a peek's index, a curated metric
//! sink) and a dataflow that exports nothing.
//!
//! [`TryFrom`] also rejects a dataflow that imports or exports a builtin
//! collection. Builtins keep their ids across releases while their schemas
//! and definitions change, and the plan inlines the bodies of builtin views,
//! which instantiation cannot check.
//!
//! Index keys are stored as [`LirScalarExpr`] and converted back to
//! `MirScalarExpr` on instantiation. LIR scalars are a subset of MIR scalars,
//! so that direction is total, and `DataflowDescription` keeps its MIR-typed
//! index keys for the optimizer's benefit. Source operators are already an
//! [`MfpPlan`] over LIR expressions in a lowered description and are stored
//! as they are.
//!
//! The stored form is JSON, whose map keys must be strings. Maps keyed by
//! `GlobalId` serialize their keys through [`GlobalIdKey`], the id's string
//! form.

use std::collections::BTreeMap;
use std::fmt;

use itertools::Itertools;
use mz_expr::{Id, MfpPlan, MirScalarExpr};
use mz_ore::soft_assert_or_log;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{GlobalId, RelationDesc, ReprRelationType, SqlRelationType};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use timely::progress::Antichain;

use crate::dataflows::{
    BuildDesc, DataflowDescription, IndexDesc, IndexImport, LirDataflowDescription, SourceImport,
};
use crate::plan::scalar::LirScalarExpr;
use crate::plan::{LirRelationExpr, LirRelationNode};
use crate::sinks::{
    ComputeSinkConnection, ComputeSinkDesc, MaterializedViewSinkConnection, MetricSinkConnection,
};
use crate::sources::{SourceInstanceArguments, SourceInstanceDesc};

/// A pinned LIR plan: the optimizer's decisions for one dataflow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedDataflow {
    /// Sources the plan reads, with the operators pushed down onto each.
    #[serde(with = "global_id_keys")]
    pub source_imports: BTreeMap<GlobalId, PinnedSourceImport>,
    /// Indexes the plan reads, keyed by index id.
    #[serde(with = "global_id_keys")]
    pub index_imports: BTreeMap<GlobalId, PinnedIndexImport>,
    /// Objects to build, in dependency order.
    pub objects_to_build: Vec<BuildDesc<LirRelationExpr>>,
    /// Indexes the plan exports, mapping each index id to the id of the
    /// object it arranges.
    #[serde(with = "global_id_keys")]
    pub index_exports: BTreeMap<GlobalId, GlobalId>,
    /// Sinks the plan exports, keyed by sink id.
    #[serde(with = "global_id_keys")]
    pub sink_exports: BTreeMap<GlobalId, PinnedSink>,
}

/// A `GlobalId` in its string form (`u1`, `s2`, `t3`), for use as a map key.
///
/// `GlobalId` itself serializes as an enum, which JSON cannot use as a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GlobalIdKey(pub GlobalId);

impl Serialize for GlobalIdKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct("GlobalIdKey", &self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for GlobalIdKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor;

        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = GlobalIdKey;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a global id such as u1")
            }

            fn visit_newtype_struct<D: Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> Result<Self::Value, D::Error> {
                let id = String::deserialize(deserializer)?;
                id.parse().map(GlobalIdKey).map_err(de::Error::custom)
            }

            fn visit_str<E: de::Error>(self, id: &str) -> Result<Self::Value, E> {
                id.parse().map(GlobalIdKey).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_newtype_struct("GlobalIdKey", KeyVisitor)
    }
}

/// Serializes a `GlobalId`-keyed map with [`GlobalIdKey`] keys.
mod global_id_keys {
    use super::*;

    pub fn serialize<V: Serialize, S: Serializer>(
        map: &BTreeMap<GlobalId, V>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_map(map.iter().map(|(id, value)| (GlobalIdKey(*id), value)))
    }

    pub fn deserialize<'de, V: Deserialize<'de>, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<GlobalId, V>, D::Error> {
        let map = BTreeMap::<GlobalIdKey, V>::deserialize(deserializer)?;
        Ok(map.into_iter().map(|(key, value)| (key.0, value)).collect())
    }
}

/// The plan's assumptions about an imported source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedSourceImport {
    /// Operators pushed down onto the source read, with any temporal filter
    /// held as the plan's bounds.
    pub operators: Option<MfpPlan<LirScalarExpr>>,
    /// The relation type the plan was compiled against.
    pub typ: SqlRelationType,
    /// Whether the plan relies on the source being monotonic.
    pub monotonic: bool,
    /// Whether the plan needs the source's snapshot.
    pub with_snapshot: bool,
}

/// The plan's assumptions about an imported index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedIndexImport {
    /// The object the index arranges and the key it arranges by.
    pub desc: IndexDesc<LirScalarExpr>,
    /// The relation type of the arranged object.
    pub typ: ReprRelationType,
    /// Whether the plan relies on the index being monotonic.
    pub monotonic: bool,
    /// Whether the plan needs the index's snapshot.
    pub with_snapshot: bool,
}

/// The wiring of an exported sink. Its descriptor is rebuilt from the catalog.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedSink {
    /// The built object the sink reads.
    pub from: GlobalId,
    /// The kind of catalog item the sink was compiled for.
    pub kind: PinnedSinkKind,
    /// Which form of `from` the sink consumes, as lowering chose it. See
    /// `ComputeSinkDesc::from_arrangement`.
    pub from_arrangement: Option<(Vec<LirScalarExpr>, MfpPlan<LirScalarExpr>)>,
}

/// The kinds of sink a pinned dataflow may export.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PinnedSinkKind {
    /// A materialized view. Its column names, non-null assertions, and
    /// refresh schedule come from the catalog item.
    MaterializedView {
        /// The sink's relation type as the optimizer built it: the plan's
        /// inferred nullability and keys, with the non-null assertions
        /// applied.
        typ: SqlRelationType,
    },
    /// A metric sink. Its label and the shaped description of its input come
    /// from the catalog item.
    MetricSink,
}

///////////////////////////////////////////////////////////////////////////////
// PINNING
///////////////////////////////////////////////////////////////////////////////

/// Why a `DataflowDescription` could not be pinned.
///
/// Lowering scalar expressions to LIR is not an error case: expression
/// preparation rejects unmaterializable functions before lowering, and
/// temporal extraction moves `mz_now()` into `MfpPlan` bounds, so a failure
/// there is a lowering bug and panics like the other LIR conversions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PinError {
    /// The dataflow exports a sink kind that has no catalog identity.
    UnpinnableSink(GlobalId),
    /// The dataflow exports an index under a transient id, as a peek does.
    TransientIndexExport(GlobalId),
    /// The dataflow exports a sink under a transient id, as a curated
    /// per-replica metric sink does.
    TransientSinkExport(GlobalId),
    /// The dataflow exports nothing, so no catalog item anchors it.
    NoExports,
    /// The dataflow imports or exports the builtin collection under this id.
    BuiltinCollection(GlobalId),
}

impl fmt::Display for PinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PinError::UnpinnableSink(id) => {
                write!(f, "sink {id} is a one-shot sink and cannot be pinned")
            }
            PinError::TransientIndexExport(id) => {
                write!(
                    f,
                    "index {id} is a peek's transient result and cannot be pinned"
                )
            }
            PinError::TransientSinkExport(id) => {
                write!(
                    f,
                    "sink {id} has a transient id and no catalog item, so it cannot be pinned"
                )
            }
            PinError::NoExports => write!(f, "a dataflow with no exports cannot be pinned"),
            PinError::BuiltinCollection(id) => {
                write!(
                    f,
                    "a dataflow that reads or writes builtin collection {id} cannot be pinned"
                )
            }
        }
    }
}

impl std::error::Error for PinError {}

impl TryFrom<LirDataflowDescription> for PinnedDataflow {
    type Error = PinError;

    fn try_from(desc: LirDataflowDescription) -> Result<Self, PinError> {
        // Destructured without `..` on purpose, so a new field on any of
        // these types fails to compile here until it is either pinned or
        // listed among the supplied fields.
        let DataflowDescription {
            source_imports,
            index_imports,
            objects_to_build,
            index_exports,
            sink_exports,
            as_of: _,
            until: _,
            initial_storage_as_of: _,
            refresh_schedule: _,
            debug_name: _,
            time_dependence: _,
        } = desc;

        // Every dataflow we run should have exports; missing exports indicate
        // a serious error somewhere.
        soft_assert_or_log!(
            !(index_exports.is_empty() && sink_exports.is_empty()),
            "pinning a dataflow with no exports"
        );
        if index_exports.is_empty() && sink_exports.is_empty() {
            return Err(PinError::NoExports);
        }
        if let Some(id) = source_imports
            .keys()
            .chain(index_imports.keys())
            .chain(index_exports.keys())
            .chain(sink_exports.keys())
            .find(|id| id.is_system())
        {
            return Err(PinError::BuiltinCollection(*id));
        }

        let source_imports = source_imports
            .into_iter()
            .map(|(id, import)| {
                let SourceImport {
                    desc:
                        SourceInstanceDesc {
                            arguments: SourceInstanceArguments { operators },
                            storage_metadata: (),
                            typ,
                        },
                    monotonic,
                    with_snapshot,
                    upper: _,
                } = import;
                (
                    id,
                    PinnedSourceImport {
                        operators,
                        typ,
                        monotonic,
                        with_snapshot,
                    },
                )
            })
            .collect();

        let index_imports = index_imports
            .into_iter()
            .map(|(id, import)| {
                let IndexImport {
                    desc,
                    typ,
                    monotonic,
                    with_snapshot,
                } = import;
                (
                    id,
                    PinnedIndexImport {
                        desc: desc.as_lir(),
                        typ,
                        monotonic,
                        with_snapshot,
                    },
                )
            })
            .collect();

        let index_exports = index_exports
            .into_iter()
            .map(|(id, (IndexDesc { on_id, key: _ }, _typ))| {
                if id.is_transient() {
                    return Err(PinError::TransientIndexExport(id));
                }
                Ok((id, on_id))
            })
            .collect::<Result<_, _>>()?;

        let sink_exports = sink_exports
            .into_iter()
            .map(|(id, sink)| {
                let ComputeSinkDesc {
                    from,
                    from_desc,
                    connection,
                    with_snapshot: _,
                    up_to: _,
                    non_null_assertions: _,
                    refresh_schedule: _,
                    from_arrangement,
                } = sink;
                let kind = match connection {
                    ComputeSinkConnection::MaterializedView(_) => {
                        PinnedSinkKind::MaterializedView {
                            typ: from_desc.into_typ(),
                        }
                    }
                    ComputeSinkConnection::MetricSink(_) => PinnedSinkKind::MetricSink,
                    ComputeSinkConnection::Subscribe(_)
                    | ComputeSinkConnection::CopyToS3Oneshot(_) => {
                        return Err(PinError::UnpinnableSink(id));
                    }
                };
                if id.is_transient() {
                    return Err(PinError::TransientSinkExport(id));
                }
                Ok((
                    id,
                    PinnedSink {
                        from,
                        kind,
                        from_arrangement,
                    },
                ))
            })
            .collect::<Result<_, _>>()?;

        Ok(PinnedDataflow {
            source_imports,
            index_imports,
            objects_to_build,
            index_exports,
            sink_exports,
        })
    }
}

///////////////////////////////////////////////////////////////////////////////
// INSTANTIATION
///////////////////////////////////////////////////////////////////////////////

/// What [`PinnedDataflow::instantiate`] needs from its environment: catalog
/// facts about the plan's imports and exports, looked up by id, and fresh
/// transient ids. Each lookup returns `None` when no item of that kind exists.
pub trait InstantiationContext {
    /// The relation type of the storage collection `id`, as the optimizer
    /// imports it.
    fn collection_type(&self, id: GlobalId) -> Option<SqlRelationType>;
    /// The index under `id`.
    fn index(&self, id: GlobalId) -> Option<IndexInfo>;
    /// The materialized view whose sink is exported under `id`.
    fn materialized_view(&self, id: GlobalId) -> Option<MaterializedViewInfo>;
    /// The metric sink exported under `id`.
    fn metric_sink(&self, id: GlobalId) -> Option<MetricSinkInfo>;
    /// A transient id no other dataflow in this process uses.
    fn allocate_transient_id(&self) -> GlobalId;
}

/// The catalog facts an index import is checked against and an index export
/// is rebuilt from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexInfo {
    /// The object the index is on.
    pub on: GlobalId,
    /// The key the index arranges by.
    pub keys: Vec<MirScalarExpr>,
    /// The relation type of the object the index is on.
    pub typ: ReprRelationType,
}

/// The catalog facts an exported materialized view sink is rebuilt from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializedViewInfo {
    /// The view's description. Instantiation takes its column names from it
    /// and checks its type against the pinned type.
    pub desc: RelationDesc,
    /// Columns asserted non-null.
    pub non_null_assertions: Vec<usize>,
    /// The view's refresh schedule.
    pub refresh_schedule: Option<RefreshSchedule>,
}

/// The catalog facts an exported metric sink is rebuilt from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricSinkInfo {
    /// The `sink` label on the sink's health gauges.
    pub label: String,
    /// The description of the shaped object the sink reads. This is what the
    /// metric sink optimizer derives from the source description and prefix,
    /// not the source description itself.
    pub from_desc: RelationDesc,
}

/// Why a [`PinnedDataflow`] could not be instantiated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InstantiateError {
    /// The catalog no longer has an item of the expected kind under this id.
    MissingCatalogItem {
        /// The import's or export's id.
        id: GlobalId,
        /// The kind of item the plan was compiled for.
        kind: &'static str,
    },
    /// The catalog item and the pinned plan disagree about the import.
    ImportDrift {
        /// The import's id.
        id: GlobalId,
        /// What disagrees.
        detail: String,
    },
    /// The catalog item and the pinned plan disagree about the export.
    ExportDrift {
        /// The export's id.
        id: GlobalId,
        /// What disagrees.
        detail: String,
    },
}

impl fmt::Display for InstantiateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstantiateError::MissingCatalogItem { id, kind } => {
                write!(f, "no {kind} catalog item for {id}")
            }
            InstantiateError::ImportDrift { id, detail } => {
                write!(f, "import {id} drifted from its pinned plan: {detail}")
            }
            InstantiateError::ExportDrift { id, detail } => {
                write!(f, "export {id} drifted from its pinned plan: {detail}")
            }
        }
    }
}
impl std::error::Error for InstantiateError {}

impl PinnedDataflow {
    /// Rebuilds the description the compute controller expects.
    ///
    /// The result has no `as_of`, `until`, `initial_storage_as_of`, or
    /// `time_dependence`, and its source imports carry no upper frontier or
    /// storage metadata. The caller supplies those the same way it does for a
    /// freshly optimized dataflow. The dataflow-level refresh schedule is the
    /// exported materialized view's, if there is one.
    ///
    /// The result equals the description that was pinned, up to transient
    /// build ids, which are reallocated from `ctx`.
    ///
    /// Fails if a catalog item an import or export was compiled for is gone,
    /// or if the catalog disagrees with the plan about what it is. Only the
    /// first problem found is reported.
    ///
    /// NOTE: The plan also contains the bodies of the views it inlines, and
    /// of SQL-defined builtin functions, and nothing here verifies those.
    /// Pinning refuses builtin collections, which keeps out builtin views
    /// that read one, but not a builtin view that reads no collection or a
    /// SQL-defined function.
    // TODO(mgree): the import and export checks are independent, so
    // collecting every problem into a list instead of returning at the first
    // one would be easy.
    pub fn instantiate(
        self,
        ctx: &dyn InstantiationContext,
        debug_name: String,
    ) -> Result<LirDataflowDescription, InstantiateError> {
        let PinnedDataflow {
            source_imports,
            index_imports,
            mut objects_to_build,
            index_exports,
            sink_exports,
        } = self;

        // Transient ids come from a per-process counter, so the internal
        // build ids of a plan pinned elsewhere can collide with a dataflow
        // built in this process. Compute keys its LIR introspection by build
        // id, and a collision merges two dataflows' operators there.
        let mut renamed = BTreeMap::new();
        for object in &mut objects_to_build {
            if object.id.is_transient() {
                let fresh = ctx.allocate_transient_id();
                renamed.insert(object.id, fresh);
                object.id = fresh;
            }
        }
        let rename = |id: GlobalId| renamed.get(&id).copied().unwrap_or(id);
        for object in &mut objects_to_build {
            replace_global_ids(&mut object.plan, &rename);
        }

        // An import's id resolving is not evidence that the plan's assumptions
        // about it hold: a materialized view's inferred nullability and keys
        // can change between releases. So each import is checked against what
        // the catalog says now.
        let mut rebuilt_source_imports = BTreeMap::new();
        for (id, import) in source_imports {
            let PinnedSourceImport {
                operators,
                typ,
                monotonic,
                with_snapshot,
            } = import;
            let catalog_typ =
                ctx.collection_type(id)
                    .ok_or(InstantiateError::MissingCatalogItem {
                        id,
                        kind: "collection",
                    })?;
            if catalog_typ != typ {
                return Err(InstantiateError::ImportDrift {
                    id,
                    detail: format!("pinned type {typ:?}, catalog says {catalog_typ:?}"),
                });
            }
            let import = SourceImport {
                desc: SourceInstanceDesc {
                    arguments: SourceInstanceArguments { operators },
                    storage_metadata: (),
                    typ,
                },
                monotonic,
                with_snapshot,
                upper: Antichain::new(),
            };
            rebuilt_source_imports.insert(id, import);
        }

        let mut rebuilt_index_imports = BTreeMap::new();
        for (id, import) in index_imports {
            let PinnedIndexImport {
                desc: IndexDesc { on_id, key },
                typ,
                monotonic,
                with_snapshot,
            } = import;
            let info = ctx
                .index(id)
                .ok_or(InstantiateError::MissingCatalogItem { id, kind: "index" })?;
            let key: Vec<MirScalarExpr> = key.iter().map(MirScalarExpr::from).collect();
            if info.on != on_id {
                return Err(InstantiateError::ImportDrift {
                    id,
                    detail: format!("pinned on {on_id}, catalog says {}", info.on),
                });
            }
            if info.keys != key {
                return Err(InstantiateError::ImportDrift {
                    id,
                    detail: format!("pinned key {key:?}, catalog says {:?}", info.keys),
                });
            }
            if info.typ != typ {
                return Err(InstantiateError::ImportDrift {
                    id,
                    detail: format!("pinned type {typ:?}, catalog says {:?}", info.typ),
                });
            }
            let import = IndexImport {
                desc: IndexDesc { on_id, key },
                typ,
                monotonic,
                with_snapshot,
            };
            rebuilt_index_imports.insert(id, import);
        }

        let is_built = |id: GlobalId| objects_to_build.iter().any(|object| object.id == id);

        let mut rebuilt_index_exports = BTreeMap::new();
        for (id, on) in index_exports {
            let info = ctx
                .index(id)
                .ok_or(InstantiateError::MissingCatalogItem { id, kind: "index" })?;
            if info.on != on {
                return Err(InstantiateError::ExportDrift {
                    id,
                    detail: format!("pinned on {on}, catalog says {}", info.on),
                });
            }
            let Some(build) = objects_to_build.iter().find(|object| object.id == id) else {
                return Err(InstantiateError::ExportDrift {
                    id,
                    detail: "the plan builds no object for the index".to_string(),
                });
            };
            // The index's build arranges by the catalog key, either with an
            // explicit `ArrangeBy` or, when lowering found the key already
            // available on the object it reads, with a bare `Get` whose
            // `keys` lists it.
            let arranged = match &build.plan.node {
                LirRelationNode::ArrangeBy { forms, .. } => &forms.arranged,
                LirRelationNode::Get { keys, .. } => &keys.arranged,
                node => {
                    return Err(InstantiateError::ExportDrift {
                        id,
                        detail: format!(
                            "the index's build is neither an arrangement nor a read: {node:?}"
                        ),
                    });
                }
            };
            let lir_key: Vec<LirScalarExpr> = info
                .keys
                .iter()
                .map(|expr| {
                    LirScalarExpr::try_from(expr).expect("catalog index keys are materializable")
                })
                .collect();
            if !arranged.iter().any(|(key, _, _)| *key == lir_key) {
                return Err(InstantiateError::ExportDrift {
                    id,
                    detail: format!("catalog key {lir_key:?} is not among the plan's arrangements"),
                });
            }
            rebuilt_index_exports.insert(
                id,
                (
                    IndexDesc {
                        on_id: on,
                        key: info.keys,
                    },
                    info.typ,
                ),
            );
        }

        let mut refresh_schedule = None;
        let mut rebuilt_sink_exports = BTreeMap::new();
        for (
            id,
            PinnedSink {
                from,
                kind,
                from_arrangement,
            },
        ) in sink_exports
        {
            let from = rename(from);
            if !is_built(from) {
                return Err(InstantiateError::ExportDrift {
                    id,
                    detail: format!("the plan builds no object {from} for the sink to read"),
                });
            }
            let desc = match kind {
                PinnedSinkKind::MaterializedView { typ } => {
                    let info =
                        ctx.materialized_view(id)
                            .ok_or(InstantiateError::MissingCatalogItem {
                                id,
                                kind: "materialized view",
                            })?;

                    // TODO(mgree): once we're loading pinned plans, this check compares the
                    // pinned type against itself.
                    let catalog_typ = info.desc.typ();
                    if typ.column_types.len() != catalog_typ.column_types.len() {
                        return Err(InstantiateError::ExportDrift {
                            id,
                            detail: format!(
                                "pinned {} columns, catalog says {}",
                                typ.column_types.len(),
                                catalog_typ.column_types.len()
                            ),
                        });
                    }
                    for (column, (pinned, catalog)) in typ
                        .column_types
                        .iter()
                        .zip_eq(&catalog_typ.column_types)
                        .enumerate()
                    {
                        if pinned.scalar_type != catalog.scalar_type {
                            return Err(InstantiateError::ExportDrift {
                                id,
                                detail: format!(
                                    "column {column}: pinned type {:?}, catalog says {:?}",
                                    pinned.scalar_type, catalog.scalar_type
                                ),
                            });
                        }
                    }
                    // Arity and scalar types are checked above, so this
                    // compares nullability and keys.
                    if typ != *catalog_typ {
                        return Err(InstantiateError::ExportDrift {
                            id,
                            detail: format!("pinned type {typ:?}, catalog says {catalog_typ:?}"),
                        });
                    }
                    let rel_desc = RelationDesc::new(typ, info.desc.iter_names().cloned());
                    refresh_schedule.clone_from(&info.refresh_schedule);
                    ComputeSinkDesc {
                        from,
                        from_desc: rel_desc.clone(),
                        connection: ComputeSinkConnection::MaterializedView(
                            MaterializedViewSinkConnection {
                                value_desc: rel_desc,
                                storage_metadata: (),
                            },
                        ),
                        with_snapshot: true,
                        up_to: Antichain::new(),
                        non_null_assertions: info.non_null_assertions,
                        refresh_schedule: info.refresh_schedule,
                        from_arrangement,
                    }
                }
                PinnedSinkKind::MetricSink => {
                    let info = ctx
                        .metric_sink(id)
                        .ok_or(InstantiateError::MissingCatalogItem {
                            id,
                            kind: "metric sink",
                        })?;
                    ComputeSinkDesc {
                        from,
                        from_desc: info.from_desc,
                        connection: ComputeSinkConnection::MetricSink(MetricSinkConnection {
                            label: info.label,
                        }),
                        with_snapshot: true,
                        up_to: Antichain::new(),
                        non_null_assertions: Vec::new(),
                        refresh_schedule: None,
                        from_arrangement,
                    }
                }
            };
            rebuilt_sink_exports.insert(id, desc);
        }

        Ok(DataflowDescription {
            source_imports: rebuilt_source_imports,
            index_imports: rebuilt_index_imports,
            objects_to_build,
            index_exports: rebuilt_index_exports,
            sink_exports: rebuilt_sink_exports,
            as_of: None,
            until: Antichain::new(),
            initial_storage_as_of: None,
            refresh_schedule,
            debug_name,
            time_dependence: None,
        })
    }
}

fn replace_global_ids(expr: &mut LirRelationExpr, rename: &impl Fn(GlobalId) -> GlobalId) {
    let mut todo = vec![expr];
    while let Some(expr) = todo.pop() {
        if let LirRelationNode::Get {
            id: Id::Global(id), ..
        } = &mut expr.node
        {
            *id = rename(*id);
        }
        todo.extend(expr.node.children_mut());
    }
}

#[cfg(test)]
#[path = "pinned_tests.rs"]
mod tests;
