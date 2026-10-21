// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of how the responses of a peek or a batched subscribe from several parts are merged.

use super::*;
use std::num::NonZeroUsize;

use mz_repr::{Datum, Diff, Row};

use crate::protocol::response::{BatchedSubscribeBatch, BatchedSubscribeResponse, Share};

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
    let expected = PeekResponse::Error(PeekError::ResultExceedsMaxSize {
        max_result_size: max_result_size.cast_into(),
    });

    for permutation in permutations {
        let mut pending = PendingPeek::new();
        for (shard_id, response_index) in permutation.into_iter().enumerate() {
            pending.absorb(shard_id, responses[response_index].clone(), max_result_size);
        }

        assert_eq!(pending.response, expected, "{permutation:?}");
    }
}

fn frontier(time: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(Timestamp::from(time))
}

fn inline_share(value: i64) -> Share {
    let row = Row::pack_slice(&[Datum::Int64(value)]);
    Share::Inline(std::iter::once((&*row, &Timestamp::MIN, Diff::ONE)).collect())
}

fn batch(
    lower: Antichain<Timestamp>,
    upper: Antichain<Timestamp>,
    shares: Vec<Share>,
) -> BatchedSubscribeResponse {
    BatchedSubscribeResponse::Batch(BatchedSubscribeBatch {
        lower,
        upper,
        shares,
        error: None,
    })
}

fn dropped_at(lower: u64, orphans: Vec<Share>) -> BatchedSubscribeResponse {
    BatchedSubscribeResponse::DroppedAt {
        frontier: frontier(lower),
        orphans,
    }
}

fn partitioned_state(parts: usize) -> PartitionedComputeState {
    <(ComputeCommand, ComputeResponse) as Partitionable<_, _>>::new(parts)
}

/// Feeds `response` from `part` through `state` and unwraps the batched subscribe response it
/// emits, if any.
fn absorb(
    state: &mut PartitionedComputeState,
    part: usize,
    response: BatchedSubscribeResponse,
) -> Option<BatchedSubscribeResponse> {
    let id = GlobalId::User(1);
    let response = ComputeResponse::BatchedSubscribeResponse(id, Box::new(response));
    match state.absorb_response(part, response)? {
        Ok(ComputeResponse::BatchedSubscribeResponse(emitted_id, response)) => {
            assert_eq!(emitted_id, id);
            Some(*response)
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

#[mz_ore::test]
fn a_description_is_emitted_once_every_part_reported_it() {
    let mut state = partitioned_state(2);
    let d0 = || (frontier(0), frontier(5));
    let d1 = || (frontier(5), frontier(10));

    assert_eq!(
        absorb(&mut state, 0, batch(d0().0, d0().1, vec![inline_share(1)])),
        None
    );
    assert_eq!(
        absorb(&mut state, 0, batch(d1().0, d1().1, vec![inline_share(2)])),
        None
    );
    assert_eq!(
        absorb(&mut state, 1, batch(d0().0, d0().1, vec![inline_share(3)])),
        Some(batch(
            d0().0,
            d0().1,
            vec![inline_share(1), inline_share(3)]
        )),
    );
    assert_eq!(
        absorb(&mut state, 1, batch(d1().0, d1().1, vec![inline_share(4)])),
        Some(batch(
            d1().0,
            d1().1,
            vec![inline_share(2), inline_share(4)]
        )),
    );
}

#[mz_ore::test]
fn descriptions_are_emitted_in_order() {
    let mut state = partitioned_state(2);
    let bounds = [(0, 5), (5, 10), (10, 15)];

    for (lower, upper) in bounds {
        let response = batch(frontier(lower), frontier(upper), vec![inline_share(0)]);
        assert_eq!(absorb(&mut state, 0, response), None);
    }
    let mut emitted = Vec::new();
    for (lower, upper) in bounds {
        let response = batch(frontier(lower), frontier(upper), vec![inline_share(1)]);
        match absorb(&mut state, 1, response) {
            Some(BatchedSubscribeResponse::Batch(batch)) => {
                assert_eq!(batch.shares, vec![inline_share(0), inline_share(1)]);
                emitted.push((batch.lower, batch.upper));
            }
            other => panic!("expected a batch, got {other:?}"),
        }
    }

    let expected: Vec<_> = bounds
        .into_iter()
        .map(|(lower, upper)| (frontier(lower), frontier(upper)))
        .collect();
    assert_eq!(emitted, expected);
}

#[mz_ore::test]
fn disagreeing_descriptions_poison_the_batch() {
    let mut state = partitioned_state(2);

    let first = batch(frontier(0), frontier(5), vec![inline_share(1)]);
    assert_eq!(absorb(&mut state, 0, first), None);
    let disagreeing = batch(frontier(0), frontier(7), vec![inline_share(2)]);
    assert_eq!(
        absorb(&mut state, 1, disagreeing),
        Some(BatchedSubscribeResponse::Batch(BatchedSubscribeBatch {
            lower: frontier(0),
            upper: frontier(5),
            shares: vec![inline_share(1), inline_share(2)],
            error: Some("replica processes disagree on batch descriptions".into()),
        })),
    );
}

#[mz_ore::test]
fn completion_is_emitted_once_after_every_part_ended() {
    let mut state = partitioned_state(2);
    let completion = || batch(frontier(10), Antichain::new(), vec![]);

    assert_eq!(absorb(&mut state, 0, completion()), None);
    assert!(!state.pending_batched_subscribes.is_empty());
    assert_eq!(absorb(&mut state, 1, completion()), Some(completion()));
    assert!(
        state.pending_batched_subscribes.is_empty(),
        "a completed subscribe leaves no state behind"
    );
}

#[mz_ore::test]
fn dropped_at_carries_the_shares_of_incomplete_descriptions() {
    let mut state = partitioned_state(2);

    // Part 0 reports two descriptions, part 1 only the first, so only the first is emitted.
    let d0 = |share| batch(frontier(0), frontier(5), vec![inline_share(share)]);
    assert_eq!(absorb(&mut state, 0, d0(1)), None);
    let d1 = batch(frontier(5), frontier(10), vec![inline_share(2)]);
    assert_eq!(absorb(&mut state, 0, d1), None);
    assert_eq!(
        absorb(&mut state, 1, d0(3)),
        Some(batch(
            frontier(0),
            frontier(5),
            vec![inline_share(1), inline_share(3)]
        )),
    );

    // Part 1 forwards an orphan of its own, as a lower merge would.
    assert_eq!(
        absorb(&mut state, 1, dropped_at(5, vec![inline_share(4)])),
        None
    );
    assert_eq!(
        absorb(&mut state, 0, dropped_at(10, vec![])),
        Some(dropped_at(5, vec![inline_share(2), inline_share(4)])),
    );
    assert!(
        state.pending_batched_subscribes.is_empty(),
        "a dropped subscribe leaves no state behind"
    );
}

#[mz_ore::test]
fn held_inline_bytes_past_the_limit_poison_the_batch() {
    // Rows pack small integers compactly, so the shares used below all have the same length.
    let share_len = inline_share(1).inline_byte_len();
    assert!((1..=3).all(|value| inline_share(value).inline_byte_len() == share_len));
    let d0 = |share| batch(frontier(0), frontier(5), vec![inline_share(share)]);
    let d1 = |share| batch(frontier(5), frontier(10), vec![inline_share(share)]);

    // Holding exactly the limit does not poison.
    let max_result_size = u64::cast_from(2 * share_len);
    let mut pending = PendingBatchedSubscribe::new(2);
    assert_eq!(pending.absorb(0, d0(1), max_result_size), None);
    assert_eq!(
        pending.absorb(1, d0(2), max_result_size),
        Some(batch(
            frontier(0),
            frontier(5),
            vec![inline_share(1), inline_share(2)]
        )),
    );

    // Holding a description beyond the one emitted passes the limit.
    let max_result_size = u64::cast_from(share_len);
    let mut pending = PendingBatchedSubscribe::new(2);
    assert_eq!(pending.absorb(0, d0(1), max_result_size), None);
    assert_eq!(pending.absorb(0, d1(2), max_result_size), None);
    let error = format!(
        "total result exceeds max size of {}",
        ByteSize::b(max_result_size)
    );
    assert_eq!(
        pending.absorb(1, d0(3), max_result_size),
        Some(BatchedSubscribeResponse::Batch(BatchedSubscribeBatch {
            lower: frontier(0),
            upper: frontier(5),
            shares: vec![inline_share(1), inline_share(3)],
            error: Some(error),
        })),
    );
    assert_eq!(
        pending.inline_bytes, share_len,
        "emitting releases the bytes"
    );
}
