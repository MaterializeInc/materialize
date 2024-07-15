// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::StreamExt;
use mz_repr::GlobalId;
use mz_storage_types::sources::SourceTimestamp;
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, Stream};

use crate::source::types::ProgressStatisticsUpdate;
use crate::statistics::SourceStatistics;

pub fn process_statistics<G, FromTime>(
    scope: G,
    source_id: GlobalId,
    worker_id: usize,
    stats_stream: Stream<G, ProgressStatisticsUpdate>,
    source_statistics: SourceStatistics,
) where
    G: Scope<Timestamp = FromTime>,
    FromTime: SourceTimestamp,
{
    let name = format!("SourceProgressStats({})", source_id);
    let mut builder = AsyncOperatorBuilder::new(name, scope.clone());

    let mut input = builder.new_disconnected_input(&stats_stream, Pipeline);

    builder.build(move |caps| async move {
        drop(caps);

        while let Some(event) = input.next().await {
            let AsyncEvent::Data(_, data) = event else {
                continue;
            };
            tracing::debug!(
                ?data,
                %source_id,
                %worker_id,
                "timely-{worker_id} received \
                    source progress statistics update"
            );

            for d in data {
                match d {
                    ProgressStatisticsUpdate::Snapshot {
                        records_known,
                        records_staged,
                    } => {
                        source_statistics.set_snapshot_records_known(records_known);
                        source_statistics.set_snapshot_records_staged(records_staged);
                    }
                    ProgressStatisticsUpdate::SteadyState {
                        mut offset_known,
                        offset_committed,
                    } => {
                        // There are two reasons `offset_known` could be below
                        // `offset_committed`:
                        // - A source implementation only periodically fetches `offset_known`,
                        // but drives offset_committed based on the data its received. This is
                        // the case for sources like Kafka.
                        // - Some irrecoverable restore has regressed `offset_known`. This is
                        // possible for sources like Postgres, and the these metrics are NOT
                        // the intended signal for this failure (source status is).
                        offset_known = std::cmp::max(offset_known, offset_committed);

                        source_statistics.set_offset_known(offset_known);
                        source_statistics.set_offset_committed(offset_committed);
                    }
                }
            }
        }
    });
}
