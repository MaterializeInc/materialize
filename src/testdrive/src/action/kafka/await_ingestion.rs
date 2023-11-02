// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::bail;
use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use mz_pgrepr::{Numeric, UInt8};
use mz_repr::adt::range::Range;
use mz_storage_types::sources::MzOffset;
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::order::Partitioned;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use timely::progress::Antichain;
use timely::PartialOrder;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_await_ingestion(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let topic = cmd.args.string("topic")?;
    let source = cmd.args.opt_string("source");
    let progress_subsource = cmd.args.opt_string("progress-subsource");
    cmd.args.done()?;

    let progress_subsource = match (source, progress_subsource) {
        (None, Some(progress)) => progress,
        (Some(source), None) => format!("{source}_progress"),
        (None, None) => bail!("Missing argument. Specify one of `source` or `progress-subsource`"),
        (Some(_), Some(_)) => {
            bail!("Conflicting arguments. Specify one of `source` or `progress-subsource`")
        }
    };

    let topic = format!("testdrive-{}-{}", topic, state.seed);
    let config = state.kafka_config.clone();

    // First let's get the upper frontier of the kafka topic
    let current_upper = mz_ore::task::spawn_blocking(
        || "kakfa_watermark".to_string(),
        move || {
            let client: AdminClient<DefaultClientContext> = config.create()?;
            let meta = client
                .inner()
                .fetch_metadata(Some(&topic), Duration::from_secs(10))?;

            let pids = meta
                .topics()
                .into_element()
                .partitions()
                .iter()
                .map(|p| p.id());

            let mut current_upper = Antichain::new();
            let mut max_pid = None;
            for pid in pids {
                let (_, high) =
                    client
                        .inner()
                        .fetch_watermarks(&topic, pid, Duration::from_secs(10))?;
                max_pid = std::cmp::max(Some(pid), max_pid);
                current_upper.insert(Partitioned::with_partition(
                    pid,
                    MzOffset::from(u64::try_from(high).unwrap()),
                ));
            }
            current_upper.insert(Partitioned::with_range(max_pid, None, MzOffset::from(0)));

            println!(
                "Awaiting ingestion of Kafka topic {topic:?} until {}...",
                current_upper.pretty(),
            );

            Ok::<_, anyhow::Error>(current_upper)
        },
    )
    .await
    .unwrap()?;

    // And now wait until the ingested frontier is beyond the upper we observed
    let query = format!(r#"SELECT "partition", "offset" FROM {progress_subsource}"#);
    Retry::default()
        .max_duration(state.default_timeout)
        .retry_async_canceling(|_| async {
            let row_times = state.pgclient.query(&query, &[]).await?;

            let mut ingested_upper = Antichain::new();
            for row_time in row_times {
                let range = row_time.get::<_, Range<Numeric>>("partition");
                let offset = row_time.get::<_, UInt8>("offset").0;

                let range = range.inner.expect("empty range");

                let lower = match range.lower.bound {
                    Some(n) => Some(i32::try_from(n.0 .0).unwrap()),
                    None => None,
                };
                let upper = match range.upper.bound {
                    Some(n) => Some(i32::try_from(n.0 .0).unwrap()),
                    None => None,
                };

                let ts = match (range.lower.inclusive, range.upper.inclusive) {
                    (true, true) => {
                        assert_eq!(lower, upper);
                        Partitioned::with_partition(lower.unwrap(), MzOffset::from(offset))
                    }
                    (false, false) => Partitioned::with_range(lower, upper, MzOffset::from(offset)),
                    _ => panic!("invalid timestamp"),
                };
                ingested_upper.insert(ts);
            }

            if !PartialOrder::less_equal(&current_upper, &ingested_upper) {
                bail!(
                    "expected ingestion to proceed until {} but only reached {}",
                    current_upper.pretty(),
                    ingested_upper.pretty(),
                );
            }
            Ok(())
        })
        .await?;

    Ok(ControlFlow::Continue)
}
