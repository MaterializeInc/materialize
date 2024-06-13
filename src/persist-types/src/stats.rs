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

use std::any::Any;
use std::fmt::Debug;

use arrow::array::Array;
use mz_ore::metric;
use mz_ore::metrics::{IntCounter, MetricsRegistry};
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use proptest::prelude::*;
use proptest::strategy::{Strategy, Union};
use proptest_derive::Arbitrary;
use prost::Message;

use crate::columnar::Data;
use crate::dyn_col::DynColumnRef;
use crate::dyn_struct::ValidityRef;
use crate::part::Part;
use crate::stats::bytes::any_bytes_stats;
use crate::stats::primitive::any_primitive_stats;

pub mod bytes;
pub mod json;
pub mod primitive;
pub mod structured;

pub use bytes::{AtomicBytesStats, BytesStats};
pub use json::{JsonMapElementStats, JsonStats};
pub use primitive::{PrimitiveStats, PrimitiveStatsVariants};
pub use structured::StructStats;

include!(concat!(env!("OUT_DIR"), "/mz_persist_types.stats.rs"));

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

/// The logic to use when computing stats for a column of `T: Data`.
///
/// If Custom is used, the DynStats returned must be a`<T as Data>::Stats`.
pub enum StatsFn {
    Default,
    Custom(fn(&DynColumnRef, ValidityRef) -> Result<Box<dyn DynStats>, String>),
}

#[cfg(debug_assertions)]
impl PartialEq for StatsFn {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StatsFn::Default, StatsFn::Default) => true,
            (StatsFn::Custom(s), StatsFn::Custom(o)) => {
                let s: fn(&'static DynColumnRef, ValidityRef) -> Result<Box<dyn DynStats>, String> =
                    *s;
                let o: fn(&'static DynColumnRef, ValidityRef) -> Result<Box<dyn DynStats>, String> =
                    *o;
                // I think this is not always correct, but it's only used in
                // debug_assertions so as long as CI is happy with it, probably
                // good enough.
                s == o
            }
            (StatsFn::Default, StatsFn::Custom(_)) | (StatsFn::Custom(_), StatsFn::Default) => {
                false
            }
        }
    }
}

impl std::fmt::Debug for StatsFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Default => write!(f, "Default"),
            Self::Custom(_) => f.debug_struct("Custom").finish_non_exhaustive(),
        }
    }
}

/// Aggregate statistics about a column of type `T`.
pub trait ColumnStats<T: Data>: DynStats {
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
    fn lower<'a>(&'a self) -> Option<T::Ref<'a>>;
    /// Same as [Self::lower] but an (also inclusive) upper bound.
    fn upper<'a>(&'a self) -> Option<T::Ref<'a>>;
    /// The number of `None`s if this column is optional or 0 if it isn't.
    fn none_count(&self) -> usize;
}

/// A source of aggregate statistics about a column of data.
pub trait StatsFrom<T> {
    /// Computes statistics from a column of data.
    ///
    /// The validity, if given, indicates which values in the columns are and
    /// are not used for stats. This allows us to model non-nullable columns in
    /// a nullable struct. For optional columns (i.e. ones with their own
    /// validity) it _must be a subset_ of the column's validity, otherwise this
    /// panics.
    fn stats_from(col: &T, validity: ValidityRef) -> Self;
}

/// Type-erased aggregate statistics about a column of data.
pub trait DynStats: Debug + Send + Sync + 'static {
    /// Returns self as a `dyn Any` for downcasting.
    fn as_any(&self) -> &dyn Any;
    /// Returns the name of the erased type for use in error messages.
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
    /// See [mz_proto::RustType::into_proto].
    fn into_proto(&self) -> ProtoDynStats;
    /// Formats these statistics for use in `INSPECT SHARD` and debugging.
    fn debug_json(&self) -> serde_json::Value;
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

/// Aggregate statistics about data contained in a [Part].
#[derive(Arbitrary, Debug)]
pub struct PartStats {
    /// Aggregate statistics about key data contained in a [Part].
    pub key: StructStats,
}

impl serde::Serialize for PartStats {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let PartStats { key } = self;
        key.serialize(s)
    }
}

impl PartStats {
    /// Calculates and returns stats for the given [Part].
    pub fn new(part: &Part) -> Result<Self, String> {
        let key = part.key_stats()?;
        Ok(PartStats { key })
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn into_proto(&self) -> ProtoDynStats {
        let mut ret = self.some.into_proto();
        // This prevents us from serializing `OptionStats<OptionStats<T>>`, but
        // that's intentionally out of scope. See the comment on ProtoDynStats.
        assert!(ret.option.is_none());
        ret.option = Some(ProtoOptionStats {
            none: self.none.into_proto(),
        });
        ret
    }
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
}

/// Empty set of statistics.
#[derive(Debug)]
#[cfg_attr(any(test), derive(Clone))]
pub struct NoneStats;

impl DynStats for NoneStats {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_proto(&self) -> ProtoDynStats {
        ProtoDynStats {
            option: None,
            kind: Some(proto_dyn_stats::Kind::None(RustType::into_proto(self))),
        }
    }

    fn debug_json(&self) -> serde_json::Value {
        serde_json::Value::String(format!("{self:?}"))
    }
}

impl<T: Data> ColumnStats<T> for NoneStats {
    fn lower<'a>(&'a self) -> Option<<T as Data>::Ref<'a>> {
        None
    }

    fn upper<'a>(&'a self) -> Option<<T as Data>::Ref<'a>> {
        None
    }

    fn none_count(&self) -> usize {
        0
    }
}

impl<T> ColumnStats<Option<T>> for OptionStats<NoneStats>
where
    Option<T>: Data,
{
    fn lower<'a>(&'a self) -> Option<<Option<T> as Data>::Ref<'a>> {
        None
    }

    fn upper<'a>(&'a self) -> Option<<Option<T> as Data>::Ref<'a>> {
        None
    }

    fn none_count(&self) -> usize {
        self.none
    }
}

impl<T: Array> StatsFrom<T> for NoneStats {
    fn stats_from(col: &T, _validity: ValidityRef) -> Self {
        assert!(col.logical_nulls().is_none());
        NoneStats
    }
}

impl<T: Array> StatsFrom<T> for OptionStats<NoneStats> {
    fn stats_from(col: &T, validity: ValidityRef) -> Self {
        debug_assert!(validity.is_superset(col.logical_nulls().as_ref()));
        let none = col
            .logical_nulls()
            .as_ref()
            .map_or(0, |nulls| nulls.null_count());

        OptionStats {
            none,
            some: NoneStats,
        }
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

impl RustType<ProtoDynStats> for Box<dyn DynStats> {
    fn into_proto(&self) -> ProtoDynStats {
        DynStats::into_proto(self.as_ref())
    }

    fn from_proto(mut proto: ProtoDynStats) -> Result<Self, TryFromProtoError> {
        struct BoxFn;
        impl DynStatsFn<Box<dyn DynStats>> for BoxFn {
            fn call<T: DynStats>(self, t: T) -> Result<Box<dyn DynStats>, TryFromProtoError> {
                Ok(Box::new(t))
            }
        }
        struct OptionStatsFn<F>(usize, F);
        impl<R, F: DynStatsFn<R>> DynStatsFn<R> for OptionStatsFn<F> {
            fn call<T: DynStats>(self, some: T) -> Result<R, TryFromProtoError> {
                let OptionStatsFn(none, f) = self;
                f.call(OptionStats { none, some })
            }
        }

        match proto.option.take() {
            Some(option) => {
                let none = option.none.into_rust()?;
                dyn_from_proto(proto, OptionStatsFn(none, BoxFn))
            }
            None => dyn_from_proto(proto, BoxFn),
        }
    }
}

/// Basically `FnOnce<T: DynStats>(self, t: T) -> R`, if rust would let us
/// type that.
///
/// We use this in `dyn_from_proto` so that OptionStats can hold a `some: T`
/// instead of a `Box<dyn DynStats>`.
trait DynStatsFn<R> {
    fn call<T: DynStats>(self, t: T) -> Result<R, TryFromProtoError>;
}

fn dyn_from_proto<R, F: DynStatsFn<R>>(proto: ProtoDynStats, f: F) -> Result<R, TryFromProtoError> {
    assert!(proto.option.is_none());
    let kind = proto
        .kind
        .ok_or_else(|| TryFromProtoError::missing_field("ProtoDynStats::kind"))?;
    match kind {
        // Sniff the type of x.lower and use that to determine which type of
        // PrimitiveStats to decode it as.
        proto_dyn_stats::Kind::Primitive(x) => match x.lower {
            Some(proto_primitive_stats::Lower::LowerBool(_)) => {
                f.call(PrimitiveStats::<bool>::from_proto(x)?)
            }
            Some(proto_primitive_stats::Lower::LowerU8(_)) => {
                f.call(PrimitiveStats::<u8>::from_proto(x)?)
            }
            Some(proto_primitive_stats::Lower::LowerU16(_)) => {
                f.call(PrimitiveStats::<u16>::from_proto(x)?)
            }
            Some(proto_primitive_stats::Lower::LowerU32(_)) => {
                f.call(PrimitiveStats::<u32>::from_proto(x)?)
            }
            Some(proto_primitive_stats::Lower::LowerU64(_)) => {
                f.call(PrimitiveStats::<u64>::from_proto(x)?)
            }
            Some(proto_primitive_stats::Lower::LowerI8(_)) => {
                f.call(PrimitiveStats::<i8>::from_proto(x)?)
            }
            Some(proto_primitive_stats::Lower::LowerI16(_)) => {
                f.call(PrimitiveStats::<i16>::from_proto(x)?)
            }
            Some(proto_primitive_stats::Lower::LowerI32(_)) => {
                f.call(PrimitiveStats::<i32>::from_proto(x)?)
            }
            Some(proto_primitive_stats::Lower::LowerI64(_)) => {
                f.call(PrimitiveStats::<i64>::from_proto(x)?)
            }
            Some(proto_primitive_stats::Lower::LowerF32(_)) => {
                f.call(PrimitiveStats::<f32>::from_proto(x)?)
            }
            Some(proto_primitive_stats::Lower::LowerF64(_)) => {
                f.call(PrimitiveStats::<f64>::from_proto(x)?)
            }
            Some(proto_primitive_stats::Lower::LowerString(_)) => {
                f.call(PrimitiveStats::<String>::from_proto(x)?)
            }
            None => Err(TryFromProtoError::missing_field("ProtoPrimitiveStats::min")),
        },
        proto_dyn_stats::Kind::Struct(x) => f.call(StructStats::from_proto(x)?),
        proto_dyn_stats::Kind::Bytes(x) => f.call(BytesStats::from_proto(x)?),
        proto_dyn_stats::Kind::None(x) => f.call(NoneStats::from_proto(x)?),
    }
}

/// Returns a [`Strategy`] that generates arbitrary `Box<dyn DynStats>`.
pub(crate) fn any_box_dyn_stats() -> impl Strategy<Value = Box<dyn DynStats>> {
    fn into_box_dyn_stats<T: DynStats>(x: T) -> Box<dyn DynStats> {
        let x: Box<dyn DynStats> = Box::new(x);
        x
    }
    let leaf = Union::new(vec![
        any_primitive_stats::<bool>()
            .prop_map(into_box_dyn_stats)
            .boxed(),
        any_primitive_stats::<i64>()
            .prop_map(into_box_dyn_stats)
            .boxed(),
        any_primitive_stats::<String>()
            .prop_map(into_box_dyn_stats)
            .boxed(),
        any_bytes_stats().prop_map(into_box_dyn_stats).boxed(),
    ]);
    leaf.prop_recursive(2, 10, 3, |inner| {
        (
            any::<usize>(),
            proptest::collection::btree_map(any::<String>(), inner, 0..3),
        )
            .prop_map(|(len, cols)| into_box_dyn_stats(StructStats { len, cols }))
    })
}
