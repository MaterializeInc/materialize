// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::server::{TimestampChanges, TimestampHistories};
use dataflow_types::{Consistency, ExternalSourceConnector, KinesisSourceConnector, Timestamp};
use timely::dataflow::{Scope, Stream};

use super::util::source;
use super::{SourceStatus, SourceToken};
use expr::SourceInstanceId;

#[allow(clippy::too_many_arguments)]
pub fn kinesis<G>(
    scope: &G,
    name: String,
    connector: KinesisSourceConnector,
    id: SourceInstanceId,
    _advance_timestamp: bool,
    timestamp_histories: TimestampHistories,
    timestamp_tx: TimestampChanges,
    consistency: Consistency,
    read_kinesis: bool,
) -> (Stream<G, (Vec<u8>, Option<i64>)>, Option<SourceToken>)
where
    G: Scope<Timestamp = Timestamp>,
{
    // This Dataflow worker will communicate it has created a source
    // to the coordinator by putting the source information on the
    // Timestamp channel.
    let ts = if read_kinesis {
        let prev = timestamp_histories.borrow_mut().insert(id.clone(), vec![]);
        assert!(prev.is_none());
        timestamp_tx.as_ref().borrow_mut().push((
            id,
            Some((ExternalSourceConnector::Kinesis(connector), consistency)),
        ));
        Some(timestamp_tx)
    } else {
        None
    };

    // todo@jldlaughlin: Actually read from the Kinesis stream!
    // Right now, this code will create a Kinesis source and immediately
    // give up all its capabilities -- rendering it "Done".
    let (stream, _capability) = source(id, ts, scope, &name.clone(), move |_info| {
        move |_cap, _output| SourceStatus::Done
    });
    (stream, None)
}
