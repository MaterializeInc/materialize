// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::progress::{Antichain, Timestamp as _};

use mz_repr::GlobalId;
use mz_repr::{Diff, Row, Timestamp};
use mz_storage::controller::CollectionMetadata;

use crate::compute_state::ComputeState;

pub(crate) fn persist_sink<G>(
    target_id: GlobalId,
    target: &CollectionMetadata,
    compute_state: &mut ComputeState,
    log_collection: Collection<G, Row, Diff>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let desired_collection = log_collection.map(Ok);
    let as_of = Antichain::from_elem(Timestamp::minimum());

    let token =
        crate::sink::persist_sink(target_id, target, desired_collection, as_of, compute_state);

    compute_state.sink_tokens.insert(
        target_id,
        crate::compute_state::SinkToken {
            token: Box::new(token),
            is_subscribe: false,
        },
    );
}
