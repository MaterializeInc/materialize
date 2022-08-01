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

use mz_repr::GlobalId;
use mz_repr::{Diff, Row, Timestamp};
use mz_storage::controller::CollectionMetadata;

use crate::compute_state::ComputeState;

pub(crate) fn persist_sink<G>(
    target_id: GlobalId,
    target: &CollectionMetadata,
    compute_state: &mut ComputeState,
    collection: Collection<G, Row, Diff>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let collection = collection.map(Ok);

    let token = crate::sink::persist_sink(target_id, target, collection, compute_state);

    // We don't allow these dataflows to be dropped, so the tokens could
    // be stored anywhere.
    compute_state.sink_tokens.insert(
        target_id,
        crate::compute_state::SinkToken {
            token: Box::new(token),
            is_tail: false,
        },
    );
}
