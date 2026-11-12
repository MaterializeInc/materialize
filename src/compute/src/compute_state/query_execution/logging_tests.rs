// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use differential_dataflow::trace::{Cursor, TraceReader};
use itertools::Itertools;
use mz_compute_client::logging::{ComputeLog, LogVariant, LoggingConfig};
use mz_repr::{Datum, Diff, Row};

const CURRENT: GlobalId = GlobalId::System(1);
const HYDRATION: GlobalId = GlobalId::System(2);

/// Read the actual logging arrangements, undoing their key/value permutation.
fn rows(h: &mut Harness, id: GlobalId, log: ComputeLog) -> BTreeMap<Row, Diff> {
    let variant = LogVariant::Compute(log);
    let key = variant.index_by();
    let value: Vec<_> = (0..variant.desc().arity())
        .filter(|column| !key.contains(column))
        .collect();
    let trace = h.state.traces.get_mut(&id).unwrap().oks_mut();
    let (mut cursor, storage) = trace.cursor();
    let mut rows = BTreeMap::new();
    while cursor.key_valid(&storage) {
        while cursor.val_valid(&storage) {
            let mut datums = vec![Datum::Null; variant.desc().arity()];
            for (column, datum) in key.iter().zip_eq(cursor.key(&storage)) {
                datums[*column] = datum;
            }
            for (column, datum) in value.iter().zip_eq(cursor.val(&storage)) {
                datums[*column] = datum;
            }
            if matches!(datums[0], Datum::String(id) if id == "t1" || id.starts_with("t1@a") || id.starts_with("t2@a"))
            {
                let mut diff = Diff::ZERO;
                cursor.map_times(&storage, |_, d| diff += d);
                if diff != Diff::ZERO {
                    rows.insert(Row::pack_slice(&datums), diff);
                }
            }
            cursor.step_val(&storage);
        }
        cursor.step_key(&storage);
    }
    rows
}

async fn await_logs(h: &mut Harness, mut check: impl FnMut(&mut Harness) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        h.state.compute_logger.as_ref().unwrap().flush();
        h.worker.step();
        h.poll();
        ActiveComputeState {
            timely_worker: &mut h.worker,
            compute_state: &mut h.state,
            response_tx: &mut h.sender,
        }
        .report_frontiers();
        h.drain();
        if check(h) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "logging did not converge: current={:?}, hydration={:?}",
            rows(h, CURRENT, ComputeLog::DataflowCurrent),
            rows(h, HYDRATION, ComputeLog::HydrationTime),
        );
        tokio::task::yield_now().await;
    }
}

fn hydrated(rows: &BTreeMap<Row, Diff>, count: i64) -> bool {
    rows.values().map(|diff| diff.into_inner()).sum::<i64>() == count
        && rows.iter().all(|(row, diff)| {
            *diff > Diff::ZERO && row.iter().skip(2).all(|datum| datum != Datum::Null)
        })
}

/// Global IDs belong to scopes, while logging belongs to the worker. Both drop
/// orders must retract only the owning instance, even when both exports are t1.
#[mz_ore::test(tokio::test)]
async fn colliding_export_instances_retract_only_their_own_logging_rows() {
    for maintained in [false, true] {
        for drop_first in [true, false] {
            let mut h = Harness::new();
            ActiveComputeState {
                timely_worker: &mut h.worker,
                compute_state: &mut h.state,
                response_tx: &mut h.sender,
            }
            .initialize_logging(LoggingConfig {
                interval: Duration::from_millis(1),
                enable_logging: true,
                log_logging: false,
                index_logs: BTreeMap::from([
                    (ComputeLog::DataflowCurrent.into(), CURRENT),
                    (ComputeLog::HydrationTime.into(), HYDRATION),
                ]),
            });
            h.state
                .traces
                .set(CATALOG, trace_bundle(&wide_ok_rows(1), vec![]));

            let first_index = h.worker.next_dataflow_index();
            if maintained {
                let mut active = ActiveComputeState {
                    timely_worker: &mut h.worker,
                    compute_state: &mut h.state,
                    response_tx: &mut h.sender,
                };
                active.handle_create_dataflow(alias(Timestamp::MIN), None);
                active.handle_schedule(EXPORT);
            } else {
                h.open(A);
                h.create(A, Timestamp::MIN);
                h.command(A, ComputeCommand::Schedule(EXPORT));
            }
            await_logs(&mut h, |h| {
                hydrated(&rows(h, HYDRATION, ComputeLog::HydrationTime), 1)
            })
            .await;
            let first_hydration = rows(&mut h, HYDRATION, ComputeLog::HydrationTime);

            let second_index = h.worker.next_dataflow_index();
            assert_ne!(first_index, second_index);
            h.open(B);
            h.create(B, Timestamp::MIN);
            h.command(B, ComputeCommand::Schedule(EXPORT));
            let current_row = |index: usize| {
                Row::pack_slice(&[
                    Datum::String("t1"),
                    Datum::UInt64(0),
                    Datum::UInt64(index.try_into().unwrap()),
                ])
            };
            let both_current = BTreeMap::from([
                (current_row(first_index), Diff::ONE),
                (current_row(second_index), Diff::ONE),
            ]);
            await_logs(&mut h, |h| {
                rows(h, CURRENT, ComputeLog::DataflowCurrent) == both_current
                    && hydrated(&rows(h, HYDRATION, ComputeLog::HydrationTime), 2)
            })
            .await;
            let mut second_hydration = rows(&mut h, HYDRATION, ComputeLog::HydrationTime);
            for (row, diff) in &first_hydration {
                *second_hydration
                    .get_mut(row)
                    .expect("first instance retained") -= diff;
            }
            second_hydration.retain(|_, diff| *diff != Diff::ZERO);
            assert!(hydrated(&second_hydration, 1));

            let close = |h: &mut Harness, first: bool| {
                if first && maintained {
                    ActiveComputeState {
                        timely_worker: &mut h.worker,
                        compute_state: &mut h.state,
                        response_tx: &mut h.sender,
                    }
                    .drop_collection(EXPORT);
                } else {
                    h.state.handle_query_command(
                        &mut h.worker,
                        None,
                        if first { A } else { B },
                        &mut h.sender,
                    );
                }
            };
            close(&mut h, drop_first);
            let survivor = if drop_first {
                second_index
            } else {
                first_index
            };
            let surviving_hydration = if drop_first {
                second_hydration
            } else {
                first_hydration
            };
            await_logs(&mut h, |h| {
                rows(h, CURRENT, ComputeLog::DataflowCurrent)
                    == BTreeMap::from([(current_row(survivor), Diff::ONE)])
                    && rows(h, HYDRATION, ComputeLog::HydrationTime) == surviving_hydration
            })
            .await;
            close(&mut h, !drop_first);
            await_logs(&mut h, |h| {
                rows(h, CURRENT, ComputeLog::DataflowCurrent).is_empty()
                    && rows(h, HYDRATION, ComputeLog::HydrationTime).is_empty()
            })
            .await;
        }
    }
}

fn subscription() -> DataflowDescription<RenderPlan, CollectionMetadata> {
    use mz_compute_types::sinks::{
        ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection,
    };
    let mut dataflow = alias(Timestamp::MIN);
    dataflow.index_exports.clear();
    dataflow.sink_exports.insert(
        EXPORT,
        ComputeSinkDesc {
            from: EXPORT,
            from_desc: source_metadata().relation_desc,
            connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection {
                output: vec![],
            }),
            with_snapshot: true,
            up_to: Antichain::new(),
            non_null_assertions: vec![],
            refresh_schedule: None,
        },
    );
    dataflow
}

// Both connections use execution IDs t1 and t2. Every public logging relation must join
// on the qualified identity, and dropping either connection must leave the other.
#[mz_ore::test(tokio::test)]
async fn qualified_subscriptions_join_and_retract_independently() {
    const FRONTIER: GlobalId = GlobalId::System(3);
    const IMPORT: GlobalId = GlobalId::System(4);
    let logs = [
        (CURRENT, ComputeLog::DataflowCurrent),
        (HYDRATION, ComputeLog::HydrationTime),
        (FRONTIER, ComputeLog::FrontierCurrent),
        (IMPORT, ComputeLog::ImportFrontierCurrent),
    ];
    for schedule in [false, true] {
        for drop_first in [A, B] {
            let mut h = Harness::new();
            ActiveComputeState {
                timely_worker: &mut h.worker,
                compute_state: &mut h.state,
                response_tx: &mut h.sender,
            }
            .initialize_logging(LoggingConfig {
                interval: Duration::from_millis(1),
                enable_logging: true,
                log_logging: false,
                index_logs: logs.iter().map(|(id, log)| ((*log).into(), *id)).collect(),
            });
            h.state
                .traces
                .set(CATALOG, trace_bundle(&wide_ok_rows(1), vec![]));
            for (nonce, client_incarnation) in [(A, 1), (B, 2)] {
                let mut dataflow = subscription();
                dataflow.sink_exports.insert(
                    GlobalId::Transient(2),
                    dataflow.sink_exports[&EXPORT].clone(),
                );
                h.open(nonce);
                h.command(
                    nonce,
                    ComputeCommand::CreateQueryDataflow {
                        request_id: A,
                        catalog_position: None,
                        subscription_incarnation: Some(client_incarnation),
                        dataflow: Box::new(dataflow),
                    },
                );
            }
            let identities = |rows: BTreeMap<Row, Diff>| -> BTreeSet<String> {
                rows.into_iter()
                    .map(|(row, diff)| {
                        assert_eq!(diff, Diff::ONE);
                        row.iter().next().unwrap().unwrap_str().to_owned()
                    })
                    .collect()
            };
            let both: BTreeSet<_> = ["t1@a1", "t2@a1", "t1@a2", "t2@a2"]
                .map(str::to_owned)
                .into_iter()
                .collect();
            await_logs(&mut h, |h| {
                logs.iter()
                    .all(|(id, log)| identities(rows(h, *id, *log)) == both)
            })
            .await;
            let frontier_rows = rows(&mut h, FRONTIER, ComputeLog::FrontierCurrent);
            assert_eq!(frontier_rows.len(), 4);
            for row in frontier_rows.keys() {
                assert_eq!(
                    row.iter().collect::<Vec<_>>()[1..],
                    [Datum::UInt64(0), Datum::MzTimestamp(Timestamp::MIN)]
                );
            }
            let import_rows = rows(&mut h, IMPORT, ComputeLog::ImportFrontierCurrent);
            assert_eq!(import_rows.len(), 4);
            for row in import_rows.keys() {
                assert_eq!(
                    row.iter().collect::<Vec<_>>()[1..],
                    [
                        Datum::String("u1"),
                        Datum::UInt64(0),
                        Datum::MzTimestamp(Timestamp::MIN),
                    ]
                );
            }
            if schedule {
                for nonce in [A, B] {
                    for id in [EXPORT, GlobalId::Transient(2)] {
                        h.command(nonce, ComputeCommand::Schedule(id));
                    }
                }
                await_logs(&mut h, |h| {
                    hydrated(&rows(h, HYDRATION, ComputeLog::HydrationTime), 4)
                        && rows(h, FRONTIER, ComputeLog::FrontierCurrent).is_empty()
                        && rows(h, IMPORT, ComputeLog::ImportFrontierCurrent).is_empty()
                })
                .await;
            }
            let survivor = if drop_first == A { "@a2" } else { "@a1" };
            // Capture exact surviving rows, including timestamps, before either drop.
            let expected: Vec<_> = logs
                .iter()
                .map(|(id, log)| {
                    rows(&mut h, *id, *log)
                        .into_iter()
                        .filter(|(row, _)| {
                            row.iter().next().unwrap().unwrap_str().ends_with(survivor)
                        })
                        .collect::<BTreeMap<_, _>>()
                })
                .collect();
            h.state
                .handle_query_command(&mut h.worker, None, drop_first, &mut h.sender);
            await_logs(&mut h, |h| {
                logs.iter()
                    .zip(&expected)
                    .all(|((id, log), expected)| rows(h, *id, *log) == *expected)
            })
            .await;
            let other = if drop_first == A { B } else { A };
            h.state
                .handle_query_command(&mut h.worker, None, other, &mut h.sender);
            await_logs(&mut h, |h| {
                logs.iter().all(|(id, log)| rows(h, *id, *log).is_empty())
            })
            .await;
        }
    }
}

#[mz_ore::test(tokio::test)]
async fn subscription_incarnation_does_not_qualify_index_exports() {
    let mut h = Harness::new();
    ActiveComputeState {
        timely_worker: &mut h.worker,
        compute_state: &mut h.state,
        response_tx: &mut h.sender,
    }
    .initialize_logging(LoggingConfig {
        interval: Duration::from_millis(1),
        enable_logging: true,
        log_logging: false,
        index_logs: BTreeMap::from([
            (ComputeLog::DataflowCurrent.into(), CURRENT),
            (ComputeLog::HydrationTime.into(), HYDRATION),
        ]),
    });
    h.state
        .traces
        .set(CATALOG, trace_bundle(&wide_ok_rows(1), vec![]));
    h.open(A);
    h.command(
        A,
        ComputeCommand::CreateQueryDataflow {
            request_id: B,
            catalog_position: None,
            subscription_incarnation: Some(1),
            dataflow: Box::new(alias(Timestamp::MIN)),
        },
    );
    await_logs(&mut h, |h| {
        let current = rows(h, CURRENT, ComputeLog::DataflowCurrent);
        current.len() == 1
            && current.iter().all(|(row, diff)| {
                row.iter().next() == Some(Datum::String("t1")) && *diff == Diff::ONE
            })
    })
    .await;
}
