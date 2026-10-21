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
//! identity to pin under, and the [`TryFrom`] conversion rejects them.
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

use mz_expr::MfpPlan;
use mz_repr::{GlobalId, ReprRelationType, SqlRelationType};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::dataflows::{
    BuildDesc, DataflowDescription, IndexDesc, IndexImport, LirDataflowDescription, SourceImport,
};
use crate::plan::LirRelationExpr;
use crate::plan::scalar::LirScalarExpr;
use crate::sinks::{ComputeSinkConnection, ComputeSinkDesc};
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
///
/// Lowering scalar expressions to LIR is not an error case: expression
/// preparation rejects unmaterializable functions before lowering, and
/// temporal extraction moves `mz_now()` into `MfpPlan` bounds, so a failure
/// there is a lowering bug and panics like the other LIR conversions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PinError {
    /// The dataflow exports a sink kind that has no catalog identity.
    UnpinnableSink(GlobalId),
}

impl fmt::Display for PinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PinError::UnpinnableSink(id) => {
                write!(f, "sink {id} is a one-shot sink and cannot be pinned")
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
                    from_arrangement,
                } = sink;
                let kind = match connection {
                    ComputeSinkConnection::MaterializedView(_) => PinnedSinkKind::MaterializedView,
                    ComputeSinkConnection::MetricSink(_) => PinnedSinkKind::MetricSink,
                    ComputeSinkConnection::Subscribe(_)
                    | ComputeSinkConnection::CopyToS3Oneshot(_) => {
                        return Err(PinError::UnpinnableSink(id));
                    }
                };
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mz_expr::{Id, MapFilterProject, MirScalarExpr, UnmaterializableFunc, func};
    use mz_repr::RelationDesc;
    use mz_repr::refresh_schedule::RefreshEvery;
    use mz_repr::refresh_schedule::RefreshSchedule;
    use mz_repr::{Datum, ReprScalarType, SqlScalarType, Timestamp};
    use timely::progress::Antichain;

    use super::*;
    use crate::plan::LirRelationNode;
    use crate::plan::scalar::mfp_plan_mir_to_lir;
    use crate::plan::{ArrangementStrategy, AvailableCollections, GetPlan, LirId};
    use crate::sinks::{MaterializedViewSinkConnection, SubscribeSinkConnection};

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

    /// A dataflow reading `SOURCE` (with a pushed-down filter, including a
    /// temporal bound) and `IMPORTED_INDEX`, building `VIEW`, with no exports.
    fn imports_only() -> LirDataflowDescription {
        let mz_now = MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow);
        let bound = MirScalarExpr::literal_ok(
            Datum::MzTimestamp(Timestamp::from(5u64)),
            ReprScalarType::MzTimestamp,
        );
        let mfp = MapFilterProject::new(1).filter([
            MirScalarExpr::column(0).call_is_null().not(),
            mz_now.call_binary(bound, func::Gte),
        ]);
        let mfp =
            mfp_plan_mir_to_lir(MfpPlan::create_from(mfp).expect("temporal bound is plannable"));
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

    fn schedule() -> RefreshSchedule {
        RefreshSchedule {
            everies: vec![RefreshEvery {
                interval: Duration::from_secs(60),
                aligned_to: Timestamp::from(0u64),
            }],
            ats: vec![Timestamp::from(10u64)],
        }
    }

    /// The sink the materialized view optimizer emits for `mv_info()`.
    fn mv_sink(from: GlobalId) -> ComputeSinkDesc {
        let asserted = RelationDesc::new(
            SqlRelationType::new(vec![SqlScalarType::Int64.nullable(false)]),
            mv_desc().iter_names().cloned(),
        );
        ComputeSinkDesc {
            from,
            from_desc: asserted.clone(),
            connection: ComputeSinkConnection::MaterializedView(MaterializedViewSinkConnection {
                value_desc: asserted,
                storage_metadata: (),
            }),
            with_snapshot: true,
            up_to: Antichain::new(),
            non_null_assertions: vec![0],
            refresh_schedule: Some(schedule()),
            from_arrangement: Some((lir_key(), identity_mfp())),
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
                from_arrangement: None,
            },
        );
        assert_eq!(
            PinnedDataflow::try_from(df),
            Err(PinError::UnpinnableSink(MV_SINK))
        );
    }

    fn materialized_view_dataflow() -> LirDataflowDescription {
        let mut df = imports_only();
        df.sink_exports.insert(MV_SINK, mv_sink(VIEW));
        df.refresh_schedule = Some(schedule());
        df
    }

    #[mz_ore::test]
    fn pinned_dataflow_round_trips_through_json() {
        let pinned = PinnedDataflow::try_from(materialized_view_dataflow()).unwrap();
        let json = serde_json::to_string(&pinned).expect("serializes as JSON");
        assert!(json.contains("\"u1\":"), "map keys are id strings: {json}");
        let parsed: PinnedDataflow = serde_json::from_str(&json).expect("parses back");
        assert_eq!(parsed, pinned);
    }
}
