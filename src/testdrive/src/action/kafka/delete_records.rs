// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{bail, Context};
use mz_ore::collections::CollectionExt;
use rdkafka::{Offset, TopicPartitionList};

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_delete_records(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partition = cmd.args.parse("partition")?;
    let offset = cmd.args.parse("offset")?;
    cmd.args.done()?;

    let topic_name = format!("{}-{}", topic_prefix, state.seed);
    println!(
        "Deleting records up to offset {offset} from partition {partition} of topic {topic_name}",
    );

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic_name, partition, Offset::Offset(offset))
        .context("internal error: adding partition to delete records topic partition list")?;
    let res = state
        .kafka_admin
        .delete_records(&tpl, &state.kafka_admin_opts)
        .await
        .context("deleting records")?;
    if res.count() != 1 {
        bail!(
            "kafka record deletion returned {} results, but exactly one result was expected",
            res.count()
        );
    }
    res.elements().into_element().error()?;

    Ok(ControlFlow::Continue)
}
