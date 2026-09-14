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
//! inputs but only the wiring for outputs---the rest can be recomputed.
//!
//! Loading a pinned plan is called _instantiation_, and it is done by means
//! of an [`InstantiationContext`].
//!
//! Only dataflows behind catalog items can be pinned: indexes, materialized
//! views, and metric sinks. Subscribes and one-shot copies have no durable
//! identity to pin under, and the [`TryFrom`] conversion rejects them.
//!
//! Scalar expressions are stored as [`LirScalarExpr`] and converted back to
//! `MirScalarExpr` on instantiation. LIR scalars are a subset of MIR scalars,
//! so that direction is total, and `DataflowDescription` keeps its MIR-typed
//! index keys and source operators for the optimizer's benefit.

use std::collections::BTreeMap;
use std::fmt;

use mz_expr::{MapFilterProject, MirScalarExpr, UnmaterializableFunc};
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{GlobalId, RelationDesc, ReprRelationType, SqlRelationType};
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

use crate::dataflows::{BuildDesc, DataflowDescription, IndexDesc, IndexImport, SourceImport};
use crate::plan::scalar::{LirScalarExpr, mfp_lir_to_mir, try_mfp_mir_to_lir};
use crate::plan::{LirRelationExpr, LirRelationNode};
use crate::sinks::{
    ComputeSinkConnection, ComputeSinkDesc, MaterializedViewSinkConnection, MetricSinkConnection,
};
use crate::sources::{SourceInstanceArguments, SourceInstanceDesc};

/// A pinned LIR plan: the optimizer's decisions for one dataflow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedDataflow {
    /// Sources the plan reads, with the operators pushed down onto each.
    pub source_imports: BTreeMap<GlobalId, PinnedSourceImport>,
    /// Indexes the plan reads, keyed by index id.
    pub index_imports: BTreeMap<GlobalId, PinnedIndexImport>,
    /// Objects to build, in dependency order.
    pub objects_to_build: Vec<BuildDesc<LirRelationExpr>>,
    /// Indexes the plan exports, mapping each index id to the id of the
    /// object it arranges.
    pub index_exports: BTreeMap<GlobalId, GlobalId>,
    /// Sinks the plan exports, keyed by sink id.
    pub sink_exports: BTreeMap<GlobalId, PinnedSink>,
}

/// The plan's assumptions about an imported source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedSourceImport {
    /// Operators pushed down onto the source read.
    pub operators: Option<MapFilterProject<LirScalarExpr>>,
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
}

/// The kinds of sink a pinned dataflow may export.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PinnedSinkKind {
    /// A materialized view. Its description, non-null assertions, and refresh
    /// schedule come from the catalog item.
    MaterializedView,
    /// A metric sink. Its label and the shaped description of its input come
    /// from the catalog item.
    MetricSink,
}

///////////////////////////////////////////////////////////////////////////////
// PINNING
///////////////////////////////////////////////////////////////////////////////

/// Why a `DataflowDescription` could not be pinned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PinError {
    /// An import or key still contains unmaterializable functions, which
    /// lowering should have resolved.
    Unmaterializable {
        /// The import the expressions belong to.
        id: GlobalId,
        /// The offending functions.
        funcs: Vec<UnmaterializableFunc>,
    },
    /// The dataflow exports a sink kind that has no catalog identity.
    UnpinnableSink(GlobalId),
}

impl fmt::Display for PinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PinError::Unmaterializable { id, funcs } => {
                write!(
                    f,
                    "import {id} contains unmaterializable functions {funcs:?}"
                )
            }
            PinError::UnpinnableSink(id) => {
                write!(f, "sink {id} is a one-shot sink and cannot be pinned")
            }
        }
    }
}

impl std::error::Error for PinError {}

impl TryFrom<DataflowDescription<LirRelationExpr>> for PinnedDataflow {
    type Error = PinError;

    fn try_from(desc: DataflowDescription<LirRelationExpr>) -> Result<Self, PinError> {
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
                let operators = operators
                    .map(try_mfp_mir_to_lir)
                    .transpose()
                    .map_err(|funcs| PinError::Unmaterializable { id, funcs })?;
                Ok((
                    id,
                    PinnedSourceImport {
                        operators,
                        typ,
                        monotonic,
                        with_snapshot,
                    },
                ))
            })
            .collect::<Result<_, _>>()?;

        let index_imports = index_imports
            .into_iter()
            .map(|(id, import)| {
                let IndexImport {
                    desc: IndexDesc { on_id, key },
                    typ,
                    monotonic,
                    with_snapshot,
                } = import;
                let key = key
                    .iter()
                    .map(LirScalarExpr::try_from)
                    .collect::<Result<_, _>>()
                    .map_err(|funcs| PinError::Unmaterializable { id, funcs })?;
                Ok((
                    id,
                    PinnedIndexImport {
                        desc: IndexDesc { on_id, key },
                        typ,
                        monotonic,
                        with_snapshot,
                    },
                ))
            })
            .collect::<Result<_, _>>()?;

        let index_exports = index_exports
            .into_iter()
            .map(|(id, (IndexDesc { on_id, key: _ }, _typ))| (id, on_id))
            .collect();

        let sink_exports = sink_exports
            .into_iter()
            .map(|(id, sink)| {
                let ComputeSinkDesc {
                    from,
                    from_desc: _,
                    connection,
                    with_snapshot: _,
                    up_to: _,
                    non_null_assertions: _,
                    refresh_schedule: _,
                } = sink;
                let kind = match connection {
                    ComputeSinkConnection::MaterializedView(_) => PinnedSinkKind::MaterializedView,
                    ComputeSinkConnection::MetricSink(_) => PinnedSinkKind::MetricSink,
                    ComputeSinkConnection::Subscribe(_)
                    | ComputeSinkConnection::CopyToS3Oneshot(_) => {
                        return Err(PinError::UnpinnableSink(id));
                    }
                };
                Ok((id, PinnedSink { from, kind }))
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

/// The catalog facts [`PinnedDataflow::instantiate`] needs, looked up by
/// export id. Each lookup returns `None` when no item of that kind exists.
pub trait InstantiationContext {
    /// The index exported under `id`.
    fn index(&self, id: GlobalId) -> Option<IndexInfo>;
    /// The materialized view whose sink is exported under `id`.
    fn materialized_view(&self, id: GlobalId) -> Option<MaterializedViewInfo>;
    /// The metric sink exported under `id`.
    fn metric_sink(&self, id: GlobalId) -> Option<MetricSinkInfo>;
}

/// The catalog facts an exported index descriptor is rebuilt from.
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
    /// The view's description, before non-null assertions are applied.
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
        /// The export's id.
        id: GlobalId,
        /// The kind of item the plan was compiled for.
        kind: &'static str,
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
                write!(f, "no {kind} catalog item for export {id}")
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
    /// Fails if a catalog item an export was compiled for is gone, or if the
    /// catalog disagrees with the plan about what the export is.
    pub fn instantiate(
        self,
        ctx: &dyn InstantiationContext,
        debug_name: String,
    ) -> Result<DataflowDescription<LirRelationExpr>, InstantiateError> {
        let PinnedDataflow {
            source_imports,
            index_imports,
            objects_to_build,
            index_exports,
            sink_exports,
        } = self;

        let source_imports = source_imports
            .into_iter()
            .map(|(id, import)| {
                let PinnedSourceImport {
                    operators,
                    typ,
                    monotonic,
                    with_snapshot,
                } = import;
                let import = SourceImport {
                    desc: SourceInstanceDesc {
                        arguments: SourceInstanceArguments {
                            operators: operators.map(mfp_lir_to_mir),
                        },
                        storage_metadata: (),
                        typ,
                    },
                    monotonic,
                    with_snapshot,
                    upper: Antichain::new(),
                };
                (id, import)
            })
            .collect();

        let index_imports = index_imports
            .into_iter()
            .map(|(id, import)| {
                let PinnedIndexImport {
                    desc: IndexDesc { on_id, key },
                    typ,
                    monotonic,
                    with_snapshot,
                } = import;
                let import = IndexImport {
                    desc: IndexDesc {
                        on_id,
                        key: key.iter().map(MirScalarExpr::from).collect(),
                    },
                    typ,
                    monotonic,
                    with_snapshot,
                };
                (id, import)
            })
            .collect();

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
            if !is_built(id) {
                return Err(InstantiateError::ExportDrift {
                    id,
                    detail: "the plan builds no object for the index".to_string(),
                });
            }
            // Lowering elides the export's `ArrangeBy` when its input already
            // carries the key, so only an explicit arrangement can be checked
            // against the catalog key.
            let lir_key: Vec<LirScalarExpr> = info
                .keys
                .iter()
                .map(|expr| {
                    LirScalarExpr::try_from(expr).expect("catalog index keys are materializable")
                })
                .collect();
            let explicit_keys = objects_to_build
                .iter()
                .find(|object| object.id == id)
                .and_then(|object| match &object.plan.node {
                    LirRelationNode::ArrangeBy { forms, .. } => Some(&forms.arranged),
                    _ => None,
                });
            if let Some(arranged) = explicit_keys {
                if !arranged.iter().any(|(key, _, _)| *key == lir_key) {
                    return Err(InstantiateError::ExportDrift {
                        id,
                        detail: format!(
                            "catalog key {lir_key:?} is not among the plan's arrangements"
                        ),
                    });
                }
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
        for (id, PinnedSink { from, kind }) in sink_exports {
            if !is_built(from) {
                return Err(InstantiateError::ExportDrift {
                    id,
                    detail: format!("the plan builds no object {from} for the sink to read"),
                });
            }
            let desc = match kind {
                PinnedSinkKind::MaterializedView => {
                    let info =
                        ctx.materialized_view(id)
                            .ok_or(InstantiateError::MissingCatalogItem {
                                id,
                                kind: "materialized view",
                            })?;
                    // Mirrors the materialized view optimizer: the sink's
                    // description is the catalog description with the
                    // asserted columns made non-nullable.
                    let mut typ = info.desc.typ().clone();
                    for &column in &info.non_null_assertions {
                        typ.column_types[column].nullable = false;
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
                    }
                }
            };
            rebuilt_sink_exports.insert(id, desc);
        }

        Ok(DataflowDescription {
            source_imports,
            index_imports,
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

#[cfg(test)]
mod tests {
    use mz_expr::{Id, MfpPlan};
    use mz_repr::{ReprScalarType, SqlScalarType};

    use super::*;
    use crate::plan::{ArrangementStrategy, AvailableCollections, GetPlan, LirId};
    use crate::sinks::SubscribeSinkConnection;

    const SOURCE: GlobalId = GlobalId::User(1);
    const IMPORTED_INDEX: GlobalId = GlobalId::User(2);
    const ON: GlobalId = GlobalId::User(3);
    const INDEX: GlobalId = GlobalId::User(4);
    const MV_SINK: GlobalId = GlobalId::User(5);
    const VIEW: GlobalId = GlobalId::Transient(1);

    ///////////////////////////////////////////////////////////////////////////
    // FIXTURES
    ///////////////////////////////////////////////////////////////////////////

    fn repr_typ() -> ReprRelationType {
        ReprRelationType::new(vec![ReprScalarType::Int64.nullable(true)])
    }

    fn sql_typ() -> SqlRelationType {
        SqlRelationType::new(vec![SqlScalarType::Int64.nullable(true)])
    }

    fn key() -> Vec<MirScalarExpr> {
        vec![MirScalarExpr::column(0)]
    }

    fn lir_key() -> Vec<LirScalarExpr> {
        key()
            .iter()
            .map(|e| LirScalarExpr::try_from(e).unwrap())
            .collect()
    }

    fn identity_mfp() -> MfpPlan<LirScalarExpr> {
        MfpPlan::create_from(MapFilterProject::<LirScalarExpr>::new(1)).unwrap()
    }

    fn get(lir_id: u64, id: GlobalId) -> LirRelationExpr {
        LirRelationExpr {
            lir_id: LirId(lir_id),
            node: LirRelationNode::Get {
                id: Id::Global(id),
                keys: AvailableCollections::new_raw(),
                plan: GetPlan::Collection(identity_mfp()),
            },
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // TESTS
    ///////////////////////////////////////////////////////////////////////////

    /// A dataflow reading `SOURCE` (with a pushed-down filter) and
    /// `IMPORTED_INDEX`, building `VIEW`, with no exports.
    fn imports_only() -> DataflowDescription<LirRelationExpr> {
        let mut mfp = MapFilterProject::new(1);
        mfp = mfp.filter([MirScalarExpr::column(0).call_is_null().not()]);
        let mut df = DataflowDescription::new("test".to_string());
        df.source_imports.insert(
            SOURCE,
            SourceImport {
                desc: SourceInstanceDesc {
                    arguments: SourceInstanceArguments {
                        operators: Some(mfp),
                    },
                    storage_metadata: (),
                    typ: sql_typ(),
                },
                monotonic: true,
                with_snapshot: true,
                upper: Antichain::new(),
            },
        );
        df.index_imports.insert(
            IMPORTED_INDEX,
            IndexImport {
                desc: IndexDesc {
                    on_id: ON,
                    key: key(),
                },
                typ: repr_typ(),
                monotonic: false,
                with_snapshot: true,
            },
        );
        df.objects_to_build.push(BuildDesc {
            id: VIEW,
            plan: get(1, SOURCE),
        });
        df
    }

    fn mv_desc() -> RelationDesc {
        RelationDesc::new(sql_typ(), ["c"])
    }

    struct Ctx {
        index: Option<IndexInfo>,
        mv: Option<MaterializedViewInfo>,
    }

    impl InstantiationContext for Ctx {
        fn index(&self, _id: GlobalId) -> Option<IndexInfo> {
            self.index.clone()
        }
        fn materialized_view(&self, _id: GlobalId) -> Option<MaterializedViewInfo> {
            self.mv.clone()
        }
        fn metric_sink(&self, _id: GlobalId) -> Option<MetricSinkInfo> {
            None
        }
    }

    #[mz_ore::test]
    fn rejects_one_shot_sinks() {
        let mut df = imports_only();
        df.sink_exports.insert(
            MV_SINK,
            ComputeSinkDesc {
                from: VIEW,
                from_desc: mv_desc(),
                connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection {
                    output: Vec::new(),
                }),
                with_snapshot: true,
                up_to: Antichain::new(),
                non_null_assertions: Vec::new(),
                refresh_schedule: None,
            },
        );
        assert_eq!(
            PinnedDataflow::try_from(df),
            Err(PinError::UnpinnableSink(MV_SINK))
        );
    }

    #[mz_ore::test]
    fn materialized_view_round_trips() {
        let mut df = imports_only();
        let mut asserted = mv_desc();
        asserted = RelationDesc::new(
            SqlRelationType::new(vec![SqlScalarType::Int64.nullable(false)]),
            asserted.iter_names().cloned(),
        );
        df.sink_exports.insert(
            MV_SINK,
            ComputeSinkDesc {
                from: VIEW,
                from_desc: asserted.clone(),
                connection: ComputeSinkConnection::MaterializedView(
                    MaterializedViewSinkConnection {
                        value_desc: asserted,
                        storage_metadata: (),
                    },
                ),
                with_snapshot: true,
                up_to: Antichain::new(),
                non_null_assertions: vec![0],
                refresh_schedule: None,
            },
        );

        let pinned = PinnedDataflow::try_from(df.clone()).unwrap();
        let ctx = Ctx {
            index: None,
            mv: Some(MaterializedViewInfo {
                desc: mv_desc(),
                non_null_assertions: vec![0],
                refresh_schedule: None,
            }),
        };
        let rebuilt = pinned.instantiate(&ctx, "test".to_string()).unwrap();
        assert_eq!(rebuilt, df);
    }

    fn index_dataflow() -> DataflowDescription<LirRelationExpr> {
        let mut df = imports_only();
        df.objects_to_build.push(BuildDesc {
            id: ON,
            plan: get(2, SOURCE),
        });
        df.objects_to_build.push(BuildDesc {
            id: INDEX,
            plan: LirRelationExpr {
                lir_id: LirId(3),
                node: LirRelationNode::ArrangeBy {
                    input_key: None,
                    input: Box::new(get(4, ON)),
                    input_mfp: identity_mfp(),
                    forms: AvailableCollections::new_arranged(vec![(lir_key(), vec![0], vec![])]),
                    strategy: ArrangementStrategy::Direct,
                },
            },
        });
        df.index_exports.insert(
            INDEX,
            (
                IndexDesc {
                    on_id: ON,
                    key: key(),
                },
                repr_typ(),
            ),
        );
        df
    }

    #[mz_ore::test]
    fn index_round_trips() {
        let df = index_dataflow();
        let pinned = PinnedDataflow::try_from(df.clone()).unwrap();
        assert_eq!(pinned.index_exports, BTreeMap::from([(INDEX, ON)]));
        let ctx = Ctx {
            index: Some(IndexInfo {
                on: ON,
                keys: key(),
                typ: repr_typ(),
            }),
            mv: None,
        };
        let rebuilt = pinned.instantiate(&ctx, "test".to_string()).unwrap();
        assert_eq!(rebuilt, df);
    }

    #[mz_ore::test]
    fn index_key_drift_is_detected() {
        let pinned = PinnedDataflow::try_from(index_dataflow()).unwrap();
        let ctx = Ctx {
            index: Some(IndexInfo {
                on: ON,
                keys: vec![MirScalarExpr::column(0).call_is_null()],
                typ: repr_typ(),
            }),
            mv: None,
        };
        assert!(matches!(
            pinned.instantiate(&ctx, "test".to_string()),
            Err(InstantiateError::ExportDrift { id: INDEX, .. })
        ));
    }

    #[mz_ore::test]
    fn missing_catalog_item_is_detected() {
        let pinned = PinnedDataflow::try_from(index_dataflow()).unwrap();
        let ctx = Ctx {
            index: None,
            mv: None,
        };
        assert_eq!(
            pinned.instantiate(&ctx, "test".to_string()),
            Err(InstantiateError::MissingCatalogItem {
                id: INDEX,
                kind: "index"
            })
        );
    }
}
