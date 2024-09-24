// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_proto::{ProtoType, RustType, TryFromProtoError};
use proptest::prelude::*;
use proptest::strategy::Strategy;
use proptest_derive::Arbitrary;
use serde::ser::{SerializeMap, SerializeStruct};

use crate::stats::{
    any_columnar_stats, proto_dyn_stats, ColumnStatKinds, ColumnStats, ColumnarStats, DynStats,
    OptionStats, ProtoStructStats, TrimStats,
};

/// Statistics about a column of a struct type with a uniform schema (the same
/// columns and associated `T: Data` types in each instance of the struct).
#[derive(Arbitrary, Default, Clone)]
pub struct StructStats {
    /// The count of structs in the column.
    pub len: usize,
    /// Statistics about each of the columns in the struct.
    ///
    /// This will often be all of the columns, but it's not guaranteed. Persist
    /// reserves the right to prune statistics about some or all of the columns.
    #[proptest(strategy = "any_struct_stats_cols()")]
    pub cols: BTreeMap<String, ColumnarStats>,
}

impl std::fmt::Debug for StructStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.debug_json(), f)
    }
}

impl serde::Serialize for StructStats {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let StructStats { len, cols } = self;
        let mut s = s.serialize_struct("StructStats", 2)?;
        let () = s.serialize_field("len", len)?;
        let () = s.serialize_field("cols", &DynStatsCols(cols))?;
        s.end()
    }
}

impl DynStats for StructStats {
    fn debug_json(&self) -> serde_json::Value {
        let mut cols = serde_json::Map::new();
        cols.insert("len".to_owned(), self.len.into());
        for (name, stats) in self.cols.iter() {
            cols.insert(name.clone(), stats.debug_json());
        }
        cols.into()
    }
    fn into_columnar_stats(self) -> ColumnarStats {
        ColumnarStats {
            nulls: None,
            values: ColumnStatKinds::Struct(self),
        }
    }
}

impl StructStats {
    /// Returns the statistics for the specified column in the struct, if they exist.
    ///
    /// This will often be all of the columns, but it's not guaranteed. Persist
    /// reserves the right to prune statistics about some or all of the columns.
    pub fn col(&self, name: &str) -> Option<&ColumnarStats> {
        self.cols.get(name)
    }
}

impl ColumnStats for StructStats {
    type Ref<'a> = ();

    fn lower<'a>(&'a self) -> Option<Self::Ref<'a>> {
        // Not meaningful for structs
        None
    }
    fn upper<'a>(&'a self) -> Option<Self::Ref<'a>> {
        // Not meaningful for structs
        None
    }
    fn none_count(&self) -> usize {
        0
    }
}

impl ColumnStats for OptionStats<StructStats> {
    type Ref<'a> = Option<()>;

    fn lower<'a>(&'a self) -> Option<Self::Ref<'a>> {
        self.some.lower().map(Some)
    }
    fn upper<'a>(&'a self) -> Option<Self::Ref<'a>> {
        self.some.upper().map(Some)
    }
    fn none_count(&self) -> usize {
        self.none
    }
}

impl RustType<ProtoStructStats> for StructStats {
    fn into_proto(&self) -> ProtoStructStats {
        ProtoStructStats {
            len: self.len.into_proto(),
            cols: self
                .cols
                .iter()
                .map(|(k, v)| (k.into_proto(), RustType::into_proto(v)))
                .collect(),
        }
    }

    fn from_proto(proto: ProtoStructStats) -> Result<Self, TryFromProtoError> {
        let mut cols = BTreeMap::new();
        for (k, v) in proto.cols {
            cols.insert(k.into_rust()?, v.into_rust()?);
        }
        Ok(StructStats {
            len: proto.len.into_rust()?,
            cols,
        })
    }
}

impl TrimStats for ProtoStructStats {
    fn trim(&mut self) {
        use proto_dyn_stats::*;

        for value in self.cols.values_mut() {
            match &mut value.kind {
                Some(Kind::Primitive(stats)) => stats.trim(),
                Some(Kind::Bytes(stats)) => stats.trim(),
                Some(Kind::Struct(stats)) => stats.trim(),
                Some(Kind::None(())) => (),
                None => {}
            }
        }
    }
}

struct DynStatsCols<'a>(&'a BTreeMap<String, ColumnarStats>);

impl serde::Serialize for DynStatsCols<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut s = s.serialize_map(Some(self.0.len()))?;
        for (k, v) in self.0.iter() {
            let v = v.debug_json();
            let () = s.serialize_entry(k, &v)?;
        }
        s.end()
    }
}

/// Returns a [`Strategy`] for generating arbitrary [`StructStats`].
pub(crate) fn any_struct_stats_cols() -> impl Strategy<Value = BTreeMap<String, ColumnarStats>> {
    proptest::collection::btree_map(any::<String>(), any_columnar_stats(), 1..5)
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::stats::primitive::PrimitiveStats;
    use crate::stats::{trim_to_budget, BytesStats, ColumnNullStats, ColumnStatKinds};

    #[mz_ore::test]
    fn struct_trim_to_budget() {
        #[track_caller]
        fn testcase(cols: &[(&str, usize)], required: Option<&str>) {
            let cols = cols
                .iter()
                .map(|(key, cost)| {
                    let stats = BytesStats::Primitive(PrimitiveStats {
                        lower: vec![],
                        upper: vec![0u8; *cost],
                    });
                    let stats = ColumnarStats {
                        nulls: None,
                        values: ColumnStatKinds::Bytes(stats),
                    };
                    ((*key).to_owned(), stats)
                })
                .collect();
            let mut stats: ProtoStructStats = RustType::into_proto(&StructStats { len: 0, cols });
            let mut budget = stats.encoded_len().next_power_of_two();
            while budget > 0 {
                let cost_before = stats.encoded_len();
                let trimmed = trim_to_budget(&mut stats, budget, |col| Some(col) == required);
                let cost_after = stats.encoded_len();
                assert!(cost_before >= cost_after);
                assert_eq!(trimmed, cost_before - cost_after);
                if let Some(required) = required {
                    assert!(stats.cols.contains_key(required));
                } else {
                    assert!(cost_after <= budget);
                }
                budget = budget / 2;
            }
        }

        testcase(&[], None);
        testcase(&[("a", 100)], None);
        testcase(&[("a", 1), ("b", 2), ("c", 4)], None);
        testcase(&[("a", 1), ("b", 2), ("c", 4)], Some("b"));
    }

    // Confirm that fields are being trimmed from largest to smallest.
    #[mz_ore::test]
    fn trim_order_regression() {
        fn column_stats(lower: &'static str, upper: &'static str) -> ColumnarStats {
            let stats = PrimitiveStats {
                lower: lower.to_owned(),
                upper: upper.to_owned(),
            };
            ColumnarStats {
                nulls: None,
                values: ColumnStatKinds::Primitive(stats.into()),
            }
        }
        let stats = StructStats {
            len: 2,
            cols: BTreeMap::from([
                ("foo".to_owned(), column_stats("a", "b")),
                (
                    "bar".to_owned(),
                    column_stats("aaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaab"),
                ),
            ]),
        };

        // The threshold here is arbitrary... we just care that there's some budget where
        // we'll discard the large field before the small one.
        let mut proto_stats = RustType::into_proto(&stats);
        trim_to_budget(&mut proto_stats, 30, |_| false);
        assert!(proto_stats.cols.contains_key("foo"));
        assert!(!proto_stats.cols.contains_key("bar"));
    }

    // Regression test for a bug found by a customer: trim_to_budget method only
    // operates on the top level struct columns. This (sorta) worked before
    // materialize#19309, but now there are always two columns at the top level, "ok" and
    // "err", and the real columns are all nested under "ok".
    #[mz_ore::test]
    fn stats_trim_to_budget_regression_recursion() {
        fn str_stats(n: usize, l: &str, u: &str) -> ColumnarStats {
            ColumnarStats {
                nulls: Some(ColumnNullStats { count: n }),
                values: ColumnStatKinds::Primitive(
                    PrimitiveStats {
                        lower: l.to_owned(),
                        upper: u.to_owned(),
                    }
                    .into(),
                ),
            }
        }

        const BIG: usize = 100;

        // Model our ok/err structure for SourceData stats for a RelationDesc
        // with wide columns.
        let mut cols = BTreeMap::new();
        for col in 'a'..='z' {
            let col = col.to_string();
            let stats = str_stats(2, "", &col.repeat(BIG));
            cols.insert(col, stats);
        }
        cols.insert("foo_timestamp".to_string(), str_stats(2, "foo", "foo"));
        let source_data_stats = StructStats {
            len: 2,
            cols: BTreeMap::from([
                ("err".to_owned(), str_stats(2, "", "")),
                (
                    "ok".to_owned(),
                    ColumnarStats {
                        nulls: None,
                        values: ColumnStatKinds::Struct(StructStats { len: 2, cols }),
                    },
                ),
            ]),
        };
        let mut proto_stats = RustType::into_proto(&source_data_stats);
        let trimmed = trim_to_budget(&mut proto_stats, BIG, |x| {
            x.ends_with("timestamp") || x == "err"
        });
        // Sanity-check that the test is trimming something.
        assert!(trimmed > 0);
        // We don't want to trim either "ok" or "err".
        assert!(proto_stats.cols.contains_key("ok"));
        assert!(proto_stats.cols.contains_key("err"));
        // Assert that we kept the timestamp column.
        let ok = proto_stats.cols.get("ok").unwrap();
        let proto_dyn_stats::Kind::Struct(ok_struct) = ok.kind.as_ref().unwrap() else {
            panic!("ok was of unexpected type {:?}", ok);
        };
        assert!(ok_struct.cols.contains_key("foo_timestamp"));
    }
}
