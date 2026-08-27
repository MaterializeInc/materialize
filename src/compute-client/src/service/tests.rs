// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of how a peek's responses from several replicas are merged.

use super::*;
use std::num::NonZeroUsize;

use mz_repr::Row;

#[mz_ore::test]
fn pending_peek_response_precedence() {
    let rows = PeekResponse::Rows(vec![RowCollection::default()]);
    let error = PeekResponse::Error(PeekError::unstructured("dataflow error"));
    let row_limit = |limit| PeekResponse::Error(PeekError::RowIterationLimitExceeded { limit });

    let mut pending = PendingPeek::new();
    pending.absorb(0, rows.clone(), u64::MAX);
    pending.absorb(1, row_limit(1000), u64::MAX);
    pending.absorb(2, row_limit(500), u64::MAX);
    assert_eq!(pending.response, row_limit(500));

    let mut pending = PendingPeek::new();
    pending.absorb(0, rows, u64::MAX);
    pending.absorb(1, row_limit(1000), u64::MAX);
    pending.absorb(2, error.clone(), u64::MAX);
    assert_eq!(pending.response, error);

    let mut pending = PendingPeek::new();
    pending.absorb(
        0,
        PeekResponse::Error(PeekError::unstructured("dataflow error")),
        u64::MAX,
    );
    pending.absorb(3, PeekResponse::Canceled, u64::MAX);
    assert_eq!(pending.response, PeekResponse::Canceled);
}

#[mz_ore::test]
fn peek_max_size_wins_over_row_iteration_limit_in_every_order() {
    let row = RowCollection::new(vec![(Row::default(), NonZeroUsize::new(1).unwrap())], &[]);
    let rows = PeekResponse::Rows(vec![row]);
    let max_result_size = u64::try_from(rows.inline_byte_len()).unwrap();
    let responses = [
        rows.clone(),
        PeekResponse::Error(PeekError::RowIterationLimitExceeded { limit: 1000 }),
        rows,
    ];
    let permutations = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    let expected = PeekResponse::Error(PeekError::unstructured(format!(
        "total result exceeds max size of {}",
        ByteSize::b(max_result_size)
    )));

    for permutation in permutations {
        let mut pending = PendingPeek::new();
        for (shard_id, response_index) in permutation.into_iter().enumerate() {
            pending.absorb(shard_id, responses[response_index].clone(), max_result_size);
        }

        assert_eq!(pending.response, expected, "{permutation:?}");
    }
}
