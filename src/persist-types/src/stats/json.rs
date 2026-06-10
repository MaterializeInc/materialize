// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::Debug;

use mz_ore::str::redact;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use proptest::prelude::*;
use proptest::strategy::{Strategy, Union};
use serde_json::json;

use crate::stats::primitive::{PrimitiveStats, any_primitive_stats};
use crate::stats::{
    DynStats, ProtoJsonMapElementStats, ProtoJsonMapStats, ProtoJsonStats, TrimStats,
    proto_json_stats,
};

// Aggregate statistics about a column of Json elements.
//
// Each element could be any of a JsonNull, a bool, a string, a numeric, a list,
// or a map/object. The column might be a single type but could also be a
// mixture of any subset of these types.
#[derive(Clone)]
pub enum JsonStats {
    /// A sentinel that indicates there were no elements.
    None,
    /// There were elements from more than one category of: bools, strings,
    /// numerics, lists, maps.
    Mixed,
    /// A sentinel that indicates all elements were `Datum::JsonNull`s.
    JsonNulls,
    /// The min and max bools, or None if there were none.
    Bools(PrimitiveStats<bool>),
    /// The min and max strings, or None if there were none.
    Strings(PrimitiveStats<String>),
    /// The min and max numerics, or None if there were none.
    /// Since we don't have a decimal type here yet, this is stored in serialized
    /// form.
    Numerics(PrimitiveStats<Vec<u8>>),
    /// A sentinel that indicates all elements were `Datum::List`s.
    ///
    /// TODO: We could also do something for list indexes analogous to what we
    /// do for map keys, but it initially seems much less likely that a user
    /// would expect that to work with pushdown, so don't bother keeping the
    /// stats until someone asks for it.
    Lists,
    /// Recursive statistics about the set of keys present in any maps/objects
    /// in the column, or None if there were no maps/objects.
    Maps(BTreeMap<String, JsonMapElementStats>),
}

#[derive(Default, Clone)]
pub struct JsonMapElementStats {
    pub len: usize,
    pub stats: JsonStats,
}

impl Default for JsonStats {
    fn default() -> Self {
        JsonStats::None
    }
}

impl Debug for JsonStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonStats::None => f.write_str("None"),
            JsonStats::Mixed => f.write_str("Mixed"),
            JsonStats::JsonNulls => f.write_str("JsonNulls"),
            JsonStats::Bools(stats) => f.debug_tuple("Bools").field(stats).finish(),
            JsonStats::Strings(stats) => f.debug_tuple("Strings").field(stats).finish(),
            JsonStats::Numerics(stats) => f.debug_tuple("Numerics").field(stats).finish(),
            JsonStats::Lists => f.write_str("Lists"),
            JsonStats::Maps(stats) => {
                let mut f = f.debug_tuple("Maps");
                for (k, v) in stats.iter() {
                    f.field(&(redact(k), v.len, &v.stats));
                }
                f.finish()
            }
        }
    }
}

impl JsonStats {
    pub fn debug_json(&self) -> serde_json::Value {
        match self {
            JsonStats::None => json!({}),
            JsonStats::Mixed => "json_mixed".into(),
            JsonStats::JsonNulls => "json_nulls".into(),
            JsonStats::Bools(x) => x.debug_json(),
            JsonStats::Strings(x) => x.debug_json(),
            JsonStats::Numerics(x) => x.debug_json(),
            JsonStats::Lists => "json_lists".into(),
            JsonStats::Maps(x) => x
                .iter()
                .map(|(k, v)| (k.clone(), v.debug_json()))
                .collect::<serde_json::Map<_, _>>()
                .into(),
        }
    }
}

impl JsonMapElementStats {
    pub fn debug_json(&self) -> serde_json::Value {
        json!({"len": self.len, "stats": self.stats.debug_json()})
    }
}

impl RustType<ProtoJsonStats> for JsonStats {
    fn into_proto(&self) -> ProtoJsonStats {
        ProtoJsonStats {
            kind: Some(match self {
                JsonStats::None => proto_json_stats::Kind::None(()),
                JsonStats::Mixed => proto_json_stats::Kind::Mixed(()),
                JsonStats::JsonNulls => proto_json_stats::Kind::JsonNulls(()),
                JsonStats::Bools(x) => proto_json_stats::Kind::Bools(RustType::into_proto(x)),
                JsonStats::Strings(x) => proto_json_stats::Kind::Strings(RustType::into_proto(x)),
                JsonStats::Numerics(x) => proto_json_stats::Kind::Numerics(RustType::into_proto(x)),
                JsonStats::Lists => proto_json_stats::Kind::Lists(()),
                JsonStats::Maps(x) => proto_json_stats::Kind::Maps(ProtoJsonMapStats {
                    elements: x
                        .iter()
                        .map(|(k, v)| ProtoJsonMapElementStats {
                            name: k.into_proto(),
                            len: v.len.into_proto(),
                            stats: Some(RustType::into_proto(&v.stats)),
                        })
                        .collect(),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoJsonStats) -> Result<Self, TryFromProtoError> {
        Ok(match proto.kind {
            Some(proto_json_stats::Kind::None(())) => JsonStats::None,
            Some(proto_json_stats::Kind::Mixed(())) => JsonStats::Mixed,
            Some(proto_json_stats::Kind::JsonNulls(())) => JsonStats::JsonNulls,
            Some(proto_json_stats::Kind::Bools(x)) => JsonStats::Bools(x.into_rust()?),
            Some(proto_json_stats::Kind::Strings(x)) => JsonStats::Strings(x.into_rust()?),
            Some(proto_json_stats::Kind::Numerics(x)) => JsonStats::Numerics(x.into_rust()?),
            Some(proto_json_stats::Kind::Lists(())) => JsonStats::Lists,
            Some(proto_json_stats::Kind::Maps(x)) => {
                let mut elements = BTreeMap::new();
                for x in x.elements {
                    let stats = JsonMapElementStats {
                        len: x.len.into_rust()?,
                        stats: x.stats.into_rust_if_some("JsonMapElementStats::stats")?,
                    };
                    elements.insert(x.name.into_rust()?, stats);
                }
                JsonStats::Maps(elements)
            }
            // Unknown JSON stats type: assume this might have any value.
            None => JsonStats::Mixed,
        })
    }
}

impl TrimStats for ProtoJsonStats {
    fn trim(&mut self) {
        use proto_json_stats::*;
        match &mut self.kind {
            Some(Kind::Strings(stats)) => {
                stats.trim();
            }
            Some(Kind::Maps(stats)) => {
                for value in &mut stats.elements {
                    if let Some(stats) = &mut value.stats {
                        stats.trim();
                    }
                }
            }
            Some(
                Kind::None(_)
                | Kind::Mixed(_)
                | Kind::JsonNulls(_)
                | Kind::Bools(_)
                | Kind::Numerics(_)
                | Kind::Lists(_),
            ) => {}
            None => {}
        }
    }
}

/// Returns a [`Strategy`] for generating abritrary [`JsonStats`].
pub(crate) fn any_json_stats() -> impl Strategy<Value = JsonStats> {
    let leaf = Union::new(vec![
        any::<()>().prop_map(|_| JsonStats::None).boxed(),
        any::<()>().prop_map(|_| JsonStats::Mixed).boxed(),
        any::<()>().prop_map(|_| JsonStats::JsonNulls).boxed(),
        any_primitive_stats::<bool>()
            .prop_map(JsonStats::Bools)
            .boxed(),
        any_primitive_stats::<String>()
            .prop_map(JsonStats::Strings)
            .boxed(),
        any::<()>().prop_map(|_| JsonStats::Lists).boxed(),
    ]);
    leaf.prop_recursive(2, 5, 3, |inner| {
        (proptest::collection::btree_map(any::<String>(), inner, 0..3)).prop_map(|cols| {
            let cols = cols
                .into_iter()
                .map(|(k, stats)| (k, JsonMapElementStats { len: 1, stats }))
                .collect();
            JsonStats::Maps(cols)
        })
    })
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::stats::trim_to_budget_jsonb;

    #[mz_ore::test]
    fn jsonb_trim_to_budget() {
        #[track_caller]
        fn testcase(cols: &[(&str, usize)], required: Option<&str>) {
            let cols = cols
                .iter()
                .map(|(key, cost)| {
                    let stats = JsonStats::Numerics(PrimitiveStats {
                        lower: vec![],
                        upper: vec![0u8; *cost],
                    });
                    let len = stats.debug_json().to_string().len();
                    ((*key).to_owned(), JsonMapElementStats { len, stats })
                })
                .collect();

            // Serialize into proto and extract the necessary type.
            let stats: ProtoJsonStats = RustType::into_proto(&JsonStats::Maps(cols));
            let ProtoJsonStats {
                kind: Some(proto_json_stats::Kind::Maps(mut stats)),
            } = stats
            else {
                panic!("serialized produced wrong type!");
            };

            let mut budget = stats.encoded_len().next_power_of_two();
            while budget > 0 {
                let cost_before = stats.encoded_len();
                trim_to_budget_jsonb(&mut stats, &mut budget, &|col| Some(col) == required);
                let cost_after = stats.encoded_len();
                assert!(cost_before >= cost_after);

                // Assert force keep columns were kept.
                if let Some(required) = required {
                    assert!(
                        stats
                            .elements
                            .iter()
                            .any(|element| element.name == required)
                    );
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

    #[mz_ore::test]
    fn jsonb_trim_to_budget_smoke() {
        let og_stats = JsonStats::Maps(
            [
                (
                    "a".to_string(),
                    JsonMapElementStats {
                        len: 1,
                        stats: JsonStats::Strings(PrimitiveStats {
                            lower: "foobar".to_string(),
                            upper: "foobaz".to_string(),
                        }),
                    },
                ),
                (
                    "context".to_string(),
                    JsonMapElementStats {
                        len: 100,
                        stats: JsonStats::Maps(
                            [
                                (
                                    "b".to_string(),
                                    JsonMapElementStats {
                                        len: 99,
                                        stats: JsonStats::Numerics(PrimitiveStats {
                                            lower: vec![],
                                            upper: vec![42u8; 99],
                                        }),
                                    },
                                ),
                                (
                                    "c".to_string(),
                                    JsonMapElementStats {
                                        len: 1,
                                        stats: JsonStats::Bools(PrimitiveStats {
                                            lower: false,
                                            upper: true,
                                        }),
                                    },
                                ),
                            ]
                            .into(),
                        ),
                    },
                ),
            ]
            .into(),
        );

        // Serialize into proto and extract the necessary type.
        let stats: ProtoJsonStats = RustType::into_proto(&og_stats);
        let ProtoJsonStats {
            kind: Some(proto_json_stats::Kind::Maps(mut stats)),
        } = stats
        else {
            panic!("serialized produced wrong type!");
        };

        let mut budget_shortfall = 50;
        // We should recurse into the "context" message and only drop the "b" column.
        trim_to_budget_jsonb(&mut stats, &mut budget_shortfall, &|_name| false);

        let mut elements = stats
            .elements
            .into_iter()
            .map(|element| (element.name.clone(), element))
            .collect::<BTreeMap<String, _>>();
        assert!(elements.remove("a").is_some());

        let context = elements.remove("context").expect("trimmed too much");
        let Some(ProtoJsonStats {
            kind: Some(proto_json_stats::Kind::Maps(context)),
        }) = context.stats
        else {
            panic!("serialized produced wrong type!")
        };

        // We should only have one element in "context" because we trimmed "b".
        assert_eq!(context.elements.len(), 1);
        assert_eq!(context.elements[0].name, "c");

        // Redo the triming, force keeping the largest column.

        // Serialize into proto and extract the necessary type.
        let stats: ProtoJsonStats = RustType::into_proto(&og_stats);
        let ProtoJsonStats {
            kind: Some(proto_json_stats::Kind::Maps(mut stats)),
        } = stats
        else {
            panic!("serialized produced wrong type!");
        };

        let mut budget_shortfall = 50;
        // We're force keeping "b" which is larger than our budgets_shortfall, so we should drop
        // everything else.
        trim_to_budget_jsonb(&mut stats, &mut budget_shortfall, &|name| name == "b");

        assert_eq!(stats.elements.len(), 1);
        assert_eq!(stats.elements[0].name, "context");

        let Some(ProtoJsonStats {
            kind: Some(proto_json_stats::Kind::Maps(context)),
        }) = &stats.elements[0].stats
        else {
            panic!("serialized produced wrong type!")
        };

        assert_eq!(context.elements.len(), 1);
        assert_eq!(context.elements[0].name, "b");
    }

    // Regression test for a bug found during code review of initial stats
    // trimming PR.
    #[mz_ore::test]
    fn stats_trim_regression_json() {
        // Make sure we recursively trim json string and map stats by asserting
        // that the goes down after trimming.
        #[track_caller]
        fn testcase(stats: JsonStats) {
            let mut stats = stats.into_proto();
            let before = stats.encoded_len();
            stats.trim();
            let after = stats.encoded_len();
            assert!(after < before, "{} vs {}: {:?}", after, before, stats);
        }

        let col = JsonStats::Strings(PrimitiveStats {
            lower: "foobar".into(),
            upper: "foobaz".into(),
        });
        testcase(col.clone());
        let mut cols = BTreeMap::new();
        cols.insert("col".into(), JsonMapElementStats { len: 1, stats: col });
        testcase(JsonStats::Maps(cols));
    }
}
