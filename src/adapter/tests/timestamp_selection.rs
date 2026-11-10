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

use mz_adapter::ReadHolds;
use mz_adapter::session::Session;
use mz_adapter::{CollectionIdBundle, TimelineContext, TimestampProvider};
use mz_compute_types::ComputeInstanceId;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::plan::QueryWhen;
use mz_sql::session::vars::IsolationLevel;
use mz_sql_parser::ast::TransactionIsolationLevel;
use mz_storage_types::read_holds::ReadHold;
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

impl TimestampProvider for Frontiers {}

impl Frontiers {
    fn least_valid_write(&self, id_bundle: &CollectionIdBundle) -> Antichain<Timestamp> {
        let mut upper = Antichain::new();
        for id in &id_bundle.storage_ids {
            upper.extend(self.storage.get(id).unwrap().write.clone());
        }
        for (instance, ids) in &id_bundle.compute_ids {
            for id in ids {
                upper.extend(self.compute.get(&(*instance, *id)).unwrap().write.clone());
            }
        }
        upper
    }

    fn acquire_read_holds(&self, id_bundle: &CollectionIdBundle) -> ReadHolds {
        let mut read_holds = ReadHolds::new();

        let mock_read_hold = |id, frontier| {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            ReadHold::with_channel(id, frontier, tx)
        };

        for (instance_id, ids) in id_bundle.compute_ids.iter() {
            for id in ids.iter() {
                let frontiers = self.compute.get(&(*instance_id, *id)).unwrap();
                read_holds.compute_holds.insert(
                    (*instance_id, *id),
                    mock_read_hold(*id, frontiers.read.clone()),
                );
            }
        }
        for id in id_bundle.storage_ids.iter() {
            let frontiers = self.storage.get(id).unwrap();
            read_holds
                .storage_holds
                .insert(*id, mock_read_hold(*id, frontiers.read.clone()));
        }

        read_holds
    }
}

#[derive(Deserialize, Debug, Clone)]
struct Determine {
    id_bundle: IdBundle,
    when: String,
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
            let ts = ts.parse().unwrap();
            match when {
                "attimestamp" => QueryWhen::AtTimestamp(ts),
                "atleasttimestamp" => QueryWhen::AtLeastTimestamp(ts),
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

/// Tests timestamp selection with explicit mock read holds, write frontiers, and oracle timestamps.
/// The datadriven language supports directives that set these inputs and transaction isolation.
/// The tuple of two timestamps specifies `(read frontier, write frontier)`.
/// The `determine` directive runs the pure solver for the specified collections and `QueryWhen`.
/// It returns the chosen timestamp, or the full determination if `full` is specified.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_timestamp_selection() {
    datadriven::walk("tests/testdata/timestamp_selection", |tf| {
        let mut f = Frontiers {
            compute: BTreeMap::new(),
            storage: BTreeMap::new(),
            oracle: Timestamp::MIN,
        };
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
                    let timeline = Frontiers::get_timeline(&timeline_ctx);
                    let needs_linearized_timeline =
                        Frontiers::needs_linearized_read_ts(&isolation_level, &when);

                    let oracle_read_ts = match timeline {
                        Some(timeline) if needs_linearized_timeline => match timeline {
                            Timeline::EpochMilliseconds => Some(f.oracle),
                            timeline => {
                                unreachable!(
                                    "only EpochMillis is used in tests but we got {:?}",
                                    timeline
                                )
                            }
                        },
                        Some(_) | None => None,
                    };

                    let id_bundle = det.id_bundle.into();
                    let read_holds = f.acquire_read_holds(&id_bundle);
                    let upper = f.least_valid_write(&id_bundle);
                    let (ts, _read_holds) = Frontiers::determine_timestamp_for_inner(
                        &session,
                        &id_bundle,
                        &when,
                        &timeline_ctx,
                        oracle_read_ts,
                        None, /* real_time_recency_ts */
                        &isolation_level,
                        read_holds,
                        upper,
                    )
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
