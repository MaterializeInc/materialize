// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders the statistics collection side of the [`PostgresSourceConnection`] ingestion dataflow.

use std::cell::{Cell, RefCell};

use mz_ore::future::InTask;
use mz_postgres_util::tunnel::PostgresFlavor;
use mz_storage_types::dyncfgs::PG_OFFSET_KNOWN_INTERVAL;
use mz_storage_types::sources::{MzOffset, PostgresSourceConnection};
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio_stream::StreamExt;

use crate::source::postgres::{ReplicationError, TransientError};
use crate::source::types::{Probe, ProgressStatisticsUpdate};
use crate::source::{probe, RawSourceCreationConfig};

pub(crate) fn render<G: Scope<Timestamp = MzOffset>>(
    scope: G,
    config: RawSourceCreationConfig,
    connection: PostgresSourceConnection,
    resume_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
) -> (
    Stream<G, ProgressStatisticsUpdate>,
    Stream<G, ReplicationError>,
    Option<Stream<G, Probe<MzOffset>>>,
    PressOnDropButton,
) {
    let op_name = format!("PostgresStatistics({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    let (stats_output, stats_stream) = builder.new_output();
    let (probe_output, probe_stream) = builder.new_output();

    // Yugabyte doesn't support LSN probing currently.
    let probe_stream = match connection.connection.flavor {
        PostgresFlavor::Vanilla => Some(probe_stream),
        PostgresFlavor::Yugabyte => None,
    };

    let (button, transient_errors) = builder.build_fallible::<TransientError, _>(move |caps| {
        Box::pin(async move {
            let [stats_cap, probe_cap]: &mut [_; 2] = caps.try_into().unwrap();

            // Only run the statistics collector on a single worker.
            if !config.responsible_for("statistics") {
                // Emit 0, to mark this worker as having started up correctly.
                stats_output.give(
                    &stats_cap[0],
                    ProgressStatisticsUpdate::SteadyState {
                        offset_known: 0,
                        offset_committed: 0,
                    },
                );
                return Ok(());
            }

            let connection_config = connection
                .connection
                .config(
                    &config.config.connection_context.secrets_reader,
                    &config.config,
                    InTask::Yes,
                )
                .await?;

            let pg_client = connection_config
                .connect(
                    "Postgres statistics",
                    &config.config.connection_context.ssh_tunnel_manager,
                )
                .await?;

            tokio::pin!(resume_uppers);

            let prev_offset_known = Cell::new(None);
            let prev_offset_committed = Cell::new(None);
            let stats_output = RefCell::new(stats_output);

            let mut probe_ticker = probe::Ticker::new(
                || PG_OFFSET_KNOWN_INTERVAL.get(config.config.config_set()),
                config.now_fn,
            );
            let probe_loop = async {
                loop {
                    let probe_ts = probe_ticker.tick().await;

                    let lsn = super::fetch_max_lsn(&pg_client).await?;
                    let offset_known = lsn.offset;
                    let upstream_frontier = Antichain::from_elem(lsn);

                    probe_output.give(
                        &probe_cap[0],
                        Probe {
                            probe_ts,
                            upstream_frontier,
                        },
                    );

                    if let Some(offset_committed) = prev_offset_committed.get() {
                        stats_output.borrow_mut().give(
                            &stats_cap[0],
                            ProgressStatisticsUpdate::SteadyState {
                                // Similar to the kafka source, we don't subtract 1 from the
                                // upper as we want to report the _number of bytes_ we have
                                // processed/in upstream.
                                offset_known,
                                offset_committed,
                            },
                        );
                    }

                    prev_offset_known.set(Some(offset_known));
                }
            };

            let commit_loop = async {
                while let Some(upper) = resume_uppers.next().await {
                    let offset_committed = match upper.into_option() {
                        Some(lsn) => lsn.offset,
                        None => continue,
                    };

                    if let Some(offset_known) = prev_offset_known.get() {
                        stats_output.borrow_mut().give(
                            &stats_cap[0],
                            ProgressStatisticsUpdate::SteadyState {
                                offset_known,
                                offset_committed,
                            },
                        );
                    }

                    prev_offset_committed.set(Some(offset_committed));
                }
            };

            futures::future::join(probe_loop, commit_loop).await.0
        })
    });

    (
        stats_stream,
        transient_errors.map(ReplicationError::from),
        probe_stream,
        button.press_on_drop(),
    )
}
