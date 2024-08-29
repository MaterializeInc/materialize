// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)] // For generated protos.

//! Aggregate statistics about data stored in persist.

use std::fmt::Debug;

use anyhow::Context;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::{IntCounter, MetricsRegistry};
use mz_ore::{assert_none, metric};
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use proptest::prelude::*;
use proptest::strategy::{Strategy, Union};
use proptest_derive::Arbitrary;
use prost::Message;

use crate::columnar::{ColumnDecoder, Schema2};
use crate::part::Part2;
use crate::stats::bytes::any_bytes_stats;
use crate::stats::primitive::any_primitive_stats;

pub mod bytes;
pub mod json;
pub mod primitive;
pub mod structured;

pub use bytes::{AtomicBytesStats, BytesStats, FixedSizeBytesStats, FixedSizeBytesStatsKind};
pub use json::{JsonMapElementStats, JsonStats};
pub use primitive::{
    truncate_bytes, PrimitiveStats, PrimitiveStatsVariants, TruncateBound, TRUNCATE_LEN,
};
pub use structured::StructStats;

include!(concat!(env!("OUT_DIR"), "/mz_persist_types.stats.rs"));

/// Statistics for a single column of data.
#[derive(Debug, Clone)]
pub struct ColumnarStats {
    /// Expected to be `None` if the associated column is non-nullable.
    pub nulls: Option<ColumnNullStats>,
    /// Statistics on the values of the column.
    pub values: ColumnStatKinds,
}

impl ColumnarStats {
    /// Returns a `[StructStats]` with a single non-nullable column.
    pub fn one_column_struct(len: usize, col: ColumnStatKinds) -> StructStats {
        let col = ColumnarStats {
            nulls: None,
            values: col,
        };
        StructStats {
            len,
            cols: [("".to_owned(), col)].into_iter().collect(),
        }
    }

    /// Returns the inner [`ColumnStatKinds`] if `nulls` is [`None`].
    pub fn as_non_null_values(&self) -> Option<&ColumnStatKinds> {
        match self.nulls {
            None => Some(&self.values),
            Some(_) => None,
        }
    }

    /// Returns the inner [`ColumnStatKinds`] if `nulls` is [`None`].
    pub fn into_non_null_values(self) -> Option<ColumnStatKinds> {
        match self.nulls {
            None => Some(self.values),
            Some(_) => None,
        }
    }

    /// Returns the inner [`StructStats`] if `nulls` is [`None`] and `values`
    /// is [`ColumnStatKinds::Struct`].
    pub fn into_struct_stats(self) -> Option<StructStats> {
        match self.into_non_null_values()? {
            ColumnStatKinds::Struct(stats) => Some(stats),
            _ => None,
        }
    }

    /// Helper method to "downcast" to stats of type `T`.
    fn try_as_stats<'a, T, F>(&'a self, map: F) -> Result<T, anyhow::Error>
    where
        F: FnOnce(&'a ColumnStatKinds) -> Result<T, anyhow::Error>,
    {
        let inner = map(&self.values)?;
        match self.nulls {
            Some(nulls) => Err(anyhow::anyhow!(
                "expected non-nullable stats, found nullable {nulls:?}"
            )),
            None => Ok(inner),
        }
    }

    /// Helper method to "downcast" [`OptionStats<T>`].
    fn try_as_option_stats<'a, T, F>(&'a self, map: F) -> Result<OptionStats<T>, anyhow::Error>
    where
        F: FnOnce(&'a ColumnStatKinds) -> Result<T, anyhow::Error>,
    {
        let inner = map(&self.values)?;
        match self.nulls {
            Some(nulls) => Ok(OptionStats {
                none: nulls.count,
                some: inner,
            }),
            None => Err(anyhow::anyhow!(
                "expected nullable stats, found non-nullable"
            )),
        }
    }

    /// Tries to "downcast" this instance of [`ColumnarStats`] to an [`OptionStats<StructStats>`]
    /// if the inner statistics are nullable and for a structured column.
    pub fn try_as_optional_struct(&self) -> Result<OptionStats<&StructStats>, anyhow::Error> {
        self.try_as_option_stats(|values| match values {
            ColumnStatKinds::Struct(inner) => Ok(inner),
            other => anyhow::bail!("expected StructStats found {other:?}"),
        })
    }

    /// Tries to "downcast" this instance of [`ColumnarStats`] to an [`OptionStats<BytesStats>`]
    /// if the inner statistics are nullable and for a column of bytes.
    pub fn try_as_optional_bytes(&self) -> Result<OptionStats<&BytesStats>, anyhow::Error> {
        self.try_as_option_stats(|values| match values {
            ColumnStatKinds::Bytes(inner) => Ok(inner),
            other => anyhow::bail!("expected BytesStats found {other:?}"),
        })
    }

    /// Tries to "downcast" this instance of [`ColumnarStats`] to a [`PrimitiveStats<String>`]
    /// if the inner statistics are nullable and for a structured column.
    pub fn try_as_string(&self) -> Result<&PrimitiveStats<String>, anyhow::Error> {
        self.try_as_stats(|values| match values {
            ColumnStatKinds::Primitive(PrimitiveStatsVariants::String(inner)) => Ok(inner),
            other => anyhow::bail!("expected PrimitiveStats<String> found {other:?}"),
        })
    }
}

impl DynStats for ColumnarStats {
    fn debug_json(&self) -> serde_json::Value {
        let value_json = self.values.debug_json();

        match (&self.nulls, value_json) {
            (Some(nulls), serde_json::Value::Object(mut x)) => {
                if nulls.count > 0 {
                    x.insert("nulls".to_owned(), nulls.count.into());
                }
                serde_json::Value::Object(x)
            }
            (Some(nulls), x) => {
                serde_json::json!({"nulls": nulls.count, "not nulls": x})
            }
            (None, x) => x,
        }
    }

    fn into_columnar_stats(self) -> ColumnarStats {
        self
    }
}

/// Statistics about the null values in a column.
#[derive(Debug, Copy, Clone)]
pub struct ColumnNullStats {
    /// Number of nulls in the column.
    pub count: usize,
}

impl RustType<ProtoOptionStats> for ColumnNullStats {
    fn into_proto(&self) -> ProtoOptionStats {
        ProtoOptionStats {
            none: u64::cast_from(self.count),
        }
    }

    fn from_proto(proto: ProtoOptionStats) -> Result<Self, TryFromProtoError> {
        Ok(ColumnNullStats {
            count: usize::cast_from(proto.none),
        })
    }
}

/// All of the kinds of statistics that we support.
#[derive(Debug, Clone)]
pub enum ColumnStatKinds {
    /// Primitive stats that maintin just an upper and lower bound.
    Primitive(PrimitiveStatsVariants),
    /// Statistics for objects with multiple fields.
    Struct(StructStats),
    /// Statistics about a column of binary data.
    Bytes(BytesStats),
    /// Maintain no statistics for a given column.
    None,
}

impl DynStats for ColumnStatKinds {
    fn debug_json(&self) -> serde_json::Value {
        match self {
            ColumnStatKinds::Primitive(prim) => prim.debug_json(),
            ColumnStatKinds::Struct(x) => x.debug_json(),
            ColumnStatKinds::Bytes(bytes) => bytes.debug_json(),
            ColumnStatKinds::None => NoneStats.debug_json(),
        }
    }

    fn into_columnar_stats(self) -> ColumnarStats {
        ColumnarStats {
            nulls: None,
            values: self,
        }
    }
}

impl RustType<proto_dyn_stats::Kind> for ColumnStatKinds {
    fn into_proto(&self) -> proto_dyn_stats::Kind {
        match self {
            ColumnStatKinds::Primitive(prim) => {
                proto_dyn_stats::Kind::Primitive(RustType::into_proto(prim))
            }
            ColumnStatKinds::Struct(x) => proto_dyn_stats::Kind::Struct(RustType::into_proto(x)),
            ColumnStatKinds::Bytes(bytes) => {
                proto_dyn_stats::Kind::Bytes(RustType::into_proto(bytes))
            }
            ColumnStatKinds::None => proto_dyn_stats::Kind::None(()),
        }
    }

    fn from_proto(proto: proto_dyn_stats::Kind) -> Result<Self, TryFromProtoError> {
        let stats = match proto {
            proto_dyn_stats::Kind::Primitive(prim) => ColumnStatKinds::Primitive(prim.into_rust()?),
            proto_dyn_stats::Kind::Struct(x) => ColumnStatKinds::Struct(x.into_rust()?),
            proto_dyn_stats::Kind::Bytes(bytes) => ColumnStatKinds::Bytes(bytes.into_rust()?),
            proto_dyn_stats::Kind::None(_) => ColumnStatKinds::None,
        };
        Ok(stats)
    }
}

impl<T: Into<PrimitiveStatsVariants>> From<T> for ColumnStatKinds {
    fn from(value: T) -> Self {
        ColumnStatKinds::Primitive(value.into())
    }
}

impl From<PrimitiveStats<Vec<u8>>> for ColumnStatKinds {
    fn from(value: PrimitiveStats<Vec<u8>>) -> Self {
        ColumnStatKinds::Bytes(BytesStats::Primitive(value))
    }
}

impl From<StructStats> for ColumnStatKinds {
    fn from(value: StructStats) -> Self {
        ColumnStatKinds::Struct(value)
    }
}

impl From<BytesStats> for ColumnStatKinds {
    fn from(value: BytesStats) -> Self {
        ColumnStatKinds::Bytes(value)
    }
}

/// Metrics for [PartStats].
#[derive(Debug)]
pub struct PartStatsMetrics {
    pub mismatched_count: IntCounter,
}

impl PartStatsMetrics {
    pub fn new(registry: &MetricsRegistry) -> Self {
        PartStatsMetrics {
            mismatched_count: registry.register(metric!(
                name: "mz_persist_pushdown_parts_mismatched_stats_count",
                help: "number of parts read with unexpectedly the incorrect type of stats",
            )),
        }
    }
}

/// Aggregate statistics about a column of type `T`.
pub trait ColumnStats: DynStats {
    /// Type returned as the stat bounds.
    type Ref<'a>
    where
        Self: 'a;

    /// An inclusive lower bound on the data contained in the column, if known.
    ///
    /// This will often be a tight bound, but it's not guaranteed. Persist
    /// reserves the right to (for example) invent smaller bounds for long byte
    /// strings. SUBTLE: This means that this exact value may not be present in
    /// the column.
    ///
    /// Similarly, if the column is empty, this will contain `T: Default`.
    /// Emptiness will be indicated in statistics higher up (i.e.
    /// [StructStats]).
    fn lower<'a>(&'a self) -> Option<Self::Ref<'a>>;
    /// Same as [Self::lower] but an (also inclusive) upper bound.
    fn upper<'a>(&'a self) -> Option<Self::Ref<'a>>;
    /// The number of `None`s if this column is optional or 0 if it isn't.
    fn none_count(&self) -> usize;
}

/// Type that can be used to represent some [`ColumnStats`].
///
/// This is a separate trait than [`ColumnStats`] because its implementations
/// generally don't care about what kind of stats they contain, whereas
/// [`ColumnStats`] is generic over the inner type of statistics.
pub trait DynStats: Debug + Send + Sync + 'static {
    /// Returns the name of the erased type for use in error messages.
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Formats these statistics for use in `INSPECT SHARD` and debugging.
    fn debug_json(&self) -> serde_json::Value;

    /// Return `self` as [`ColumnarStats`].
    fn into_columnar_stats(self) -> ColumnarStats;
}

/// Trim, possibly in a lossy way, statistics to reduce the serialization costs.
pub trait TrimStats: Message {
    /// Attempts to reduce the serialization costs of these stats.
    ///
    /// This is lossy (might increase the false positive rate) and so should
    /// be avoided if the full fidelity stats are within an acceptable cost
    /// threshold.
    fn trim(&mut self);
}

/// Aggregate statistics about data contained in a [Part2].
#[derive(Arbitrary, Debug)]
pub struct PartStats {
    /// Aggregate statistics about key data contained in a [Part2].
    pub key: StructStats,
}

impl serde::Serialize for PartStats {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let PartStats { key } = self;
        key.serialize(s)
    }
}

impl PartStats {
    /// Calculates and returns stats for the given [`Part2`].
    pub fn new<T, K>(part: &Part2, desc: &K) -> Result<Self, anyhow::Error>
    where
        K: Schema2<T, Statistics = StructStats>,
    {
        let decoder = K::decoder_any(desc, &part.key).context("decoder_any")?;
        let stats = decoder.stats();
        Ok(PartStats { key: stats })
    }
}

/// Statistics about a column of some optional type.
pub struct OptionStats<T> {
    /// Statistics about the `Some` values in the column.
    pub some: T,
    /// The count of `None` values in the column.
    pub none: usize,
}

impl<T: DynStats> Debug for OptionStats<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.debug_json(), f)
    }
}

impl<T: DynStats> DynStats for OptionStats<T> {
    fn debug_json(&self) -> serde_json::Value {
        match self.some.debug_json() {
            serde_json::Value::Object(mut x) => {
                if self.none > 0 {
                    x.insert("nulls".to_owned(), self.none.into());
                }
                serde_json::Value::Object(x)
            }
            s => {
                serde_json::json!({"nulls": self.none, "not nulls": s})
            }
        }
    }

    fn into_columnar_stats(self) -> ColumnarStats {
        let inner = self.some.into_columnar_stats();
        assert_none!(inner.nulls, "we don't support nested OptionStats");

        ColumnarStats {
            nulls: Some(ColumnNullStats { count: self.none }),
            values: inner.values,
        }
    }
}

/// Empty set of statistics.
#[derive(Debug)]
#[cfg_attr(any(test), derive(Clone))]
pub struct NoneStats;

impl DynStats for NoneStats {
    fn debug_json(&self) -> serde_json::Value {
        serde_json::Value::String(format!("{self:?}"))
    }

    fn into_columnar_stats(self) -> ColumnarStats {
        ColumnarStats {
            nulls: None,
            values: ColumnStatKinds::None,
        }
    }
}

impl ColumnStats for NoneStats {
    type Ref<'a> = ();

    fn lower<'a>(&'a self) -> Option<Self::Ref<'a>> {
        None
    }

    fn upper<'a>(&'a self) -> Option<Self::Ref<'a>> {
        None
    }

    fn none_count(&self) -> usize {
        0
    }
}

impl ColumnStats for OptionStats<NoneStats> {
    type Ref<'a> = Option<()>;

    fn lower<'a>(&'a self) -> Option<Self::Ref<'a>> {
        None
    }

    fn upper<'a>(&'a self) -> Option<Self::Ref<'a>> {
        None
    }

    fn none_count(&self) -> usize {
        self.none
    }
}

impl RustType<()> for NoneStats {
    fn into_proto(&self) -> () {
        ()
    }

    fn from_proto(_proto: ()) -> Result<Self, TryFromProtoError> {
        Ok(NoneStats)
    }
}

/// Trims the included column status until they fit within a budget.
///
/// This might remove stats for a column entirely, unless `force_keep_col`
/// returns true for that column. The resulting StructStats object is
/// guaranteed to fit within the passed budget, except when the columns that
/// are force-kept are collectively larger than the budget.
///
/// The number of bytes trimmed is returned.
pub fn trim_to_budget(
    stats: &mut ProtoStructStats,
    budget: usize,
    force_keep_col: impl Fn(&str) -> bool,
) -> usize {
    // No trimming necessary should be the overwhelming common case in practice.
    let original_cost = stats.encoded_len();
    if original_cost <= budget {
        return 0;
    }

    // First try any lossy trimming that doesn't lose an entire column.
    stats.trim();
    let new_cost = stats.encoded_len();
    if new_cost <= budget {
        return original_cost.saturating_sub(new_cost);
    }

    // That wasn't enough. Try recursively trimming out entire cols.
    //
    // TODO: There are certainly more elegant things we could do here. One idea
    // would be to call `trim_to_budget_struct` but with a closure for
    // force_keep_col that always returns false. That would potentially at least
    // keep _something_. Another possibility would be to replace this whole bit
    // with some sort of recursive max-cost search with force_keep_col things
    // weighted after everything else.
    let mut budget_shortfall = new_cost.saturating_sub(budget);
    trim_to_budget_struct(stats, &mut budget_shortfall, &force_keep_col);
    original_cost.saturating_sub(stats.encoded_len())
}

/// Recursively trims cols in the struct, greatest-size first, keeping force
/// kept cols and ancestors of force kept cols.
fn trim_to_budget_struct(
    stats: &mut ProtoStructStats,
    budget_shortfall: &mut usize,
    force_keep_col: &impl Fn(&str) -> bool,
) {
    // Sort the columns in order of ascending size and keep however many fit
    // within the budget. This strategy both keeps the largest total number of
    // columns and also optimizes for the sort of columns we expect to need
    // stats in practice (timestamps are numbers or small strings).
    //
    // Note: even though we sort in ascending order, we use `.pop()` to iterate
    // over the elements, which takes from the back of the Vec.
    let mut col_costs: Vec<_> = stats
        .cols
        .iter()
        .map(|(name, stats)| (name.to_owned(), stats.encoded_len()))
        .collect();
    col_costs.sort_unstable_by_key(|(_, c)| *c);

    while *budget_shortfall > 0 {
        let Some((name, cost)) = col_costs.pop() else {
            break;
        };

        if force_keep_col(&name) {
            continue;
        }

        // Otherwise, if the col is a struct, recurse into it.
        //
        // TODO: Do this same recursion for json stats.
        let col_stats = stats.cols.get_mut(&name).expect("col exists");
        match &mut col_stats.kind {
            Some(proto_dyn_stats::Kind::Struct(col_struct)) => {
                trim_to_budget_struct(col_struct, budget_shortfall, force_keep_col);
                // This recursion might have gotten us under budget.
                if *budget_shortfall == 0 {
                    break;
                }
                // Otherwise, if any columns are left, they must have been force
                // kept, which means we need to force keep this struct as well.
                if !col_struct.cols.is_empty() {
                    continue;
                }
                // We have to recompute the cost because trim_to_budget_struct might
                // have already accounted for some of the shortfall.
                *budget_shortfall = budget_shortfall.saturating_sub(col_struct.encoded_len() + 1);
                stats.cols.remove(&name);
            }
            Some(proto_dyn_stats::Kind::Bytes(ProtoBytesStats {
                kind:
                    Some(proto_bytes_stats::Kind::Json(ProtoJsonStats {
                        kind: Some(proto_json_stats::Kind::Maps(col_jsonb)),
                    })),
            })) => {
                trim_to_budget_jsonb(col_jsonb, budget_shortfall, force_keep_col);
                // This recursion might have gotten us under budget.
                if *budget_shortfall == 0 {
                    break;
                }
                // Otherwise, if any columns are left, they must have been force
                // kept, which means we need to force keep this struct as well.
                if !col_jsonb.elements.is_empty() {
                    continue;
                }
                // We have to recompute the cost because trim_to_budget_jsonb might
                // have already accounted for some of the shortfall.
                *budget_shortfall = budget_shortfall.saturating_sub(col_jsonb.encoded_len() + 1);
                stats.cols.remove(&name);
            }
            _ => {
                stats.cols.remove(&name);
                // Each field costs at least the cost of serializing the value
                // and a byte for the tag. (Though a tag may be more than one
                // byte in extreme cases.)
                *budget_shortfall = budget_shortfall.saturating_sub(cost + 1);
            }
        }
    }
}

fn trim_to_budget_jsonb(
    stats: &mut ProtoJsonMapStats,
    budget_shortfall: &mut usize,
    force_keep_col: &impl Fn(&str) -> bool,
) {
    // Sort the columns in order of ascending size and keep however many fit
    // within the budget. This strategy both keeps the largest total number of
    // columns and also optimizes for the sort of columns we expect to need
    // stats in practice (timestamps are numbers or small strings).
    //
    // Note: even though we sort in ascending order, we use `.pop()` to iterate
    // over the elements, which takes from the back of the Vec.
    stats
        .elements
        .sort_unstable_by_key(|element| element.encoded_len());

    // Our strategy is to pop of stats until there are no more, or we're under
    // budget. As we trim anything we want to keep, e.g. with force_keep_col,
    // we stash it here, and later re-append.
    let mut stats_to_keep = Vec::with_capacity(stats.elements.len());

    while *budget_shortfall > 0 {
        let Some(mut column) = stats.elements.pop() else {
            break;
        };

        // We're force keeping this column.
        if force_keep_col(&column.name) {
            stats_to_keep.push(column);
            continue;
        }

        // If the col is another JSON map, recurse into it and trim its stats.
        if let Some(ProtoJsonStats {
            kind: Some(proto_json_stats::Kind::Maps(ref mut col_jsonb)),
        }) = column.stats
        {
            trim_to_budget_jsonb(col_jsonb, budget_shortfall, force_keep_col);

            // We still have some columns left after trimming, so we want to keep these stats.
            if !col_jsonb.elements.is_empty() {
                stats_to_keep.push(column);
            }

            // We've trimmed enough, so we can stop recursing!
            if *budget_shortfall == 0 {
                break;
            }
        } else {
            // Each field costs at least the cost of serializing the value
            // and a byte for the tag. (Though a tag may be more than one
            // byte in extreme cases.)
            *budget_shortfall = budget_shortfall.saturating_sub(column.encoded_len() + 1);
        }
    }

    // Re-add all of the stats we want to keep.
    stats.elements.extend(stats_to_keep);
}

impl RustType<ProtoDynStats> for ColumnarStats {
    fn into_proto(&self) -> ProtoDynStats {
        let option = self.nulls.as_ref().map(|n| n.into_proto());
        let kind = RustType::into_proto(&self.values);

        ProtoDynStats {
            option,
            kind: Some(kind),
        }
    }

    fn from_proto(proto: ProtoDynStats) -> Result<Self, TryFromProtoError> {
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoDynStats::kind"))?;

        Ok(ColumnarStats {
            nulls: proto.option.into_rust()?,
            values: kind.into_rust()?,
        })
    }
}

/// Returns a [`Strategy`] that generates arbitrary [`ColumnarStats`].
pub(crate) fn any_columnar_stats() -> impl Strategy<Value = ColumnarStats> {
    let leaf = Union::new(vec![
        any_primitive_stats::<bool>()
            .prop_map(|s| ColumnStatKinds::Primitive(s.into()))
            .boxed(),
        any_primitive_stats::<i64>()
            .prop_map(|s| ColumnStatKinds::Primitive(s.into()))
            .boxed(),
        any_primitive_stats::<String>()
            .prop_map(|s| ColumnStatKinds::Primitive(s.into()))
            .boxed(),
        any_bytes_stats().prop_map(ColumnStatKinds::Bytes).boxed(),
    ])
    .prop_map(|values| ColumnarStats {
        nulls: None,
        values,
    });

    leaf.prop_recursive(2, 10, 3, |inner| {
        (
            any::<usize>(),
            proptest::collection::btree_map(any::<String>(), inner, 0..3),
        )
            .prop_map(|(len, cols)| {
                let values = ColumnStatKinds::Struct(StructStats { len, cols });
                ColumnarStats {
                    nulls: None,
                    values,
                }
            })
    })
}
