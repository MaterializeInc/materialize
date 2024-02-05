// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Test determine_timestamp.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use futures::executor::block_on;
use mz_adapter::catalog::CatalogState;
use mz_adapter::session::Session;
use mz_adapter::{CollectionIdBundle, TimelineContext, TimestampProvider};
use mz_compute_types::ComputeInstanceId;
use mz_expr::MirScalarExpr;
use mz_repr::{Datum, GlobalId, ScalarType, Timestamp};
use mz_sql::plan::QueryWhen;
use mz_sql::session::vars::IsolationLevel;
use mz_sql_parser::ast::TransactionIsolationLevel;
use mz_storage_types::sources::Timeline;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(transparent)]
struct Set {
    ids: BTreeMap<String, SetFrontier>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SetFrontier {
    read: Timestamp,
    write: Timestamp,
}

impl Set {
    fn to_compute_frontiers(self) -> BTreeMap<(ComputeInstanceId, GlobalId), Frontier> {
        let mut m = BTreeMap::new();
        for (id, v) in self.ids {
            let (instance, id) = id.split_once(',').unwrap();
            let instance: ComputeInstanceId = instance.parse().unwrap();
            let id: GlobalId = id.parse().unwrap();
            m.insert((instance, id), v.into());
        }
        m
    }
    fn to_storage_frontiers(self) -> BTreeMap<GlobalId, Frontier> {
        let mut m = BTreeMap::new();
        for (id, v) in self.ids {
            let id: GlobalId = id.parse().unwrap();
            m.insert(id, v.into());
        }
        m
    }
}

struct Frontiers {
    compute: BTreeMap<(ComputeInstanceId, GlobalId), Frontier>,
    storage: BTreeMap<GlobalId, Frontier>,
    oracle: Timestamp,
}

struct Frontier {
    read: Antichain<Timestamp>,
    write: Antichain<Timestamp>,
}

impl From<SetFrontier> for Frontier {
    fn from(s: SetFrontier) -> Self {
        Frontier {
            read: Antichain::from_elem(s.read),
            write: Antichain::from_elem(s.write),
        }
    }
}

#[async_trait(?Send)]
impl TimestampProvider for Frontiers {
    fn compute_read_frontier<'a>(
        &'a self,
        instance: ComputeInstanceId,
        id: GlobalId,
    ) -> timely::progress::frontier::AntichainRef<'a, Timestamp> {
        self.compute.get(&(instance, id)).unwrap().read.borrow()
    }

    fn compute_read_capability<'a>(
        &'a self,
        instance: ComputeInstanceId,
        id: GlobalId,
    ) -> &'a timely::progress::Antichain<Timestamp> {
        &self.compute.get(&(instance, id)).unwrap().read
    }

    fn compute_write_frontier<'a>(
        &'a self,
        instance: ComputeInstanceId,
        id: GlobalId,
    ) -> timely::progress::frontier::AntichainRef<'a, Timestamp> {
        self.compute.get(&(instance, id)).unwrap().write.borrow()
    }

    fn storage_read_capabilities<'a>(
        &'a self,
        id: GlobalId,
    ) -> timely::progress::frontier::AntichainRef<'a, Timestamp> {
        self.storage.get(&id).unwrap().read.borrow()
    }

    fn storage_implied_capability<'a>(
        &'a self,
        id: GlobalId,
    ) -> &'a timely::progress::Antichain<Timestamp> {
        &self.storage.get(&id).unwrap().read
    }

    fn storage_write_frontier<'a>(
        &'a self,
        id: GlobalId,
    ) -> &'a timely::progress::Antichain<Timestamp> {
        &self.storage.get(&id).unwrap().write
    }
}

#[derive(Deserialize, Debug, Clone)]
struct Determine {
    id_bundle: IdBundle,
    when: String,
    instance: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct IdBundle {
    #[serde(default)]
    storage_ids: BTreeSet<String>,
    #[serde(default)]
    compute_ids: BTreeMap<String, BTreeSet<String>>,
}

impl From<IdBundle> for CollectionIdBundle {
    fn from(val: IdBundle) -> CollectionIdBundle {
        CollectionIdBundle {
            storage_ids: BTreeSet::from_iter(val.storage_ids.iter().map(|id| id.parse().unwrap())),
            compute_ids: BTreeMap::from_iter(val.compute_ids.iter().map(|(id, set)| {
                let set = BTreeSet::from_iter(set.iter().map(|s| s.parse().unwrap()));
                (id.parse().unwrap(), set)
            })),
        }
    }
}

fn parse_query_when(s: &str) -> QueryWhen {
    let s = s.to_lowercase();
    match s.split_once(':') {
        Some((when, ts)) => {
            let ts: i64 = ts.parse().unwrap();
            let expr = MirScalarExpr::literal_ok(Datum::Int64(ts), ScalarType::Int64);
            match when {
                "attimestamp" => QueryWhen::AtTimestamp(expr),
                "atleasttimestamp" => QueryWhen::AtLeastTimestamp(expr),
                _ => panic!("bad when {s}"),
            }
        }
        None => match s.as_str() {
            "freshesttablewrite" => QueryWhen::FreshestTableWrite,
            "immediately" => QueryWhen::Immediately,
            _ => panic!("bad when {s}"),
        },
    }
}

/// Tests determine_timestamp.
///
/// This works by mocking out the compute and storage controllers and timestamp oracle. Then we can
/// call determine_timestamp for specified sources and QueryWhens. The testdrive language supports
/// various set directives that can be used to set the state of the fake controllers or timestamp
/// oracle. The tuple of two timestamps for those specifies the `(read frontier, write frontier)`.
/// Transaction isolation can also be set. The `determine` directive runs determine_timestamp and
/// returns the chosen timestamp. Append `full` as an argument to it to see the entire
/// TimestampDetermination.
// TODO(aljoscha): We allow `futures::block_on` for testing because
// `determine_timestamp_for()` is now async. We will remove async here again
// once we have sufficiently evolved the TimestampOracle API and are done with
// adding the new Durable TimestampOracle based on Postgres/CRDB.
#[allow(clippy::disallowed_methods)]
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_timestamp_selection() {
    datadriven::walk("tests/testdata/timestamp_selection", |tf| {
        let mut f = Frontiers {
            compute: BTreeMap::new(),
            storage: BTreeMap::new(),
            oracle: Timestamp::MIN,
        };
        let catalog = CatalogState::empty();
        let mut isolation = TransactionIsolationLevel::StrictSerializable;
        tf.run(move |tc| -> String {
            match tc.directive.as_str() {
                "set-compute" => {
                    let set: Set = serde_json::from_str(&tc.input).unwrap();
                    f.compute = set.to_compute_frontiers();
                    "".into()
                }
                "set-storage" => {
                    let set: Set = serde_json::from_str(&tc.input).unwrap();
                    f.storage = set.to_storage_frontiers();
                    "".into()
                }
                "set-oracle" => {
                    let set: Timestamp = serde_json::from_str(&tc.input).unwrap();
                    f.oracle = set;
                    "".into()
                }
                "set-isolation" => {
                    let level = tc.input.trim().to_uppercase();
                    isolation =
                        if level == TransactionIsolationLevel::StrictSerializable.to_string() {
                            TransactionIsolationLevel::StrictSerializable
                        } else if level
                            == TransactionIsolationLevel::StrongSessionSerializable.to_string()
                        {
                            TransactionIsolationLevel::StrongSessionSerializable
                        } else if level == TransactionIsolationLevel::Serializable.to_string() {
                            TransactionIsolationLevel::Serializable
                        } else {
                            panic!("unknown level {}", tc.input);
                        };
                    "".into()
                }
                "determine" => {
                    let det: Determine = serde_json::from_str(&tc.input).unwrap();
                    let mut session = Session::dummy();
                    let _ = session.start_transaction(
                        mz_ore::now::to_datetime(0),
                        None,
                        Some(isolation),
                    );

                    // TODO: Factor out into method, or somesuch!
                    let timeline_ctx = TimelineContext::TimestampDependent;
                    let isolation_level = IsolationLevel::from(isolation);
                    let when = parse_query_when(&det.when);
                    let linearized_timeline =
                        Frontiers::get_linearized_timeline(&isolation_level, &when, &timeline_ctx);

                    let oracle_read_ts = if let Some(timeline) = linearized_timeline {
                        match timeline {
                            Timeline::EpochMilliseconds => Some(f.oracle),
                            timeline => {
                                unreachable!(
                                    "only EpochMillis is used in tests but we got {:?}",
                                    timeline
                                )
                            }
                        }
                    } else {
                        None
                    };

                    let ts = block_on(f.determine_timestamp_for(
                        &catalog,
                        &session,
                        &det.id_bundle.into(),
                        &parse_query_when(&det.when),
                        det.instance.parse().unwrap(),
                        &TimelineContext::TimestampDependent,
                        oracle_read_ts,
                        None, /* real_time_recency_ts */
                        &IsolationLevel::from(isolation),
                    ))
                    .unwrap();

                    if tc.args.contains_key("full") {
                        format!("{}\n", serde_json::to_string_pretty(&ts).unwrap())
                    } else {
                        format!("{}\n", ts.timestamp_context.timestamp_or_default())
                    }
                }
                _ => panic!("unknown directive {}", tc.directive),
            }
        })
    })
}
