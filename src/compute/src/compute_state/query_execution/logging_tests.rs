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
            if datums[0] == Datum::String("t1") {
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
            .initialize_logging(
                LoggingConfig {
                    interval: Duration::from_millis(1),
                    enable_logging: true,
                    log_logging: false,
                    index_logs: BTreeMap::from([
                        (ComputeLog::DataflowCurrent.into(), CURRENT),
                        (ComputeLog::HydrationTime.into(), HYDRATION),
                    ]),
                },
                None,
            );
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
                active.handle_create_dataflow(alias(Timestamp::MIN));
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
