// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_ore::now::SYSTEM_TIME;
use mz_repr::{Diff, Row};
use mz_storage_types::sources::MzOffset;
use mz_storage_types::sources::load_generator::{
    Event, Generator, GeneratorUpdate as Update, LoadGeneratorOutput,
};
use sha2::{Digest, Sha256};

use super::Tpch;

/// Scale factor 0.01, with the given tick.
fn tpch(tick: Duration) -> Tpch {
    Tpch {
        count_supplier: 100,
        count_part: 2_000,
        count_customer: 1_500,
        count_orders: 15_000,
        count_clerk: 10,
        tick,
    }
}

/// Collects updates until the first progress event at or beyond `up_to`.
fn collect(iter: impl Iterator<Item = Update>, up_to: u64) -> Vec<Update> {
    let mut updates = Vec::new();
    for update in iter {
        let done = matches!(&update.1, Event::Progress(Some(o)) if o.offset >= up_to);
        updates.push(update);
        if done {
            break;
        }
    }
    updates
}

fn digest(updates: &[Update]) -> String {
    let mut hasher = Sha256::new();
    for (output, event) in updates {
        hasher.update(format!("{output:?}").as_bytes());
        match event {
            Event::Message(offset, (row, diff)) => {
                hasher.update(b"m");
                hasher.update(offset.offset.to_le_bytes());
                hasher.update(diff.into_inner().to_le_bytes());
                hasher.update(u64::try_from(row.data().len()).unwrap().to_le_bytes());
                hasher.update(row.data());
            }
            Event::Progress(offset) => {
                hasher.update(b"p");
                hasher.update(offset.map_or(u64::MAX, |o| o.offset).to_le_bytes());
            }
        }
    }
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The digests pin the output of the single-threaded generator. Sources
/// replay the generator after a restart and retract rows they regenerate, so
/// any change to the output breaks existing sources.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn snapshot_matches_golden() {
    let updates: Vec<_> = tpch(Duration::ZERO)
        .by_seed(SYSTEM_TIME.clone(), None, MzOffset::from(0))
        .collect();
    assert_eq!(updates.len(), 86857);
    assert_eq!(
        digest(&updates),
        "c7ffe8ba3b9ef7dba651ec6b8e0a1d14f14a8532b59c0b5b95aa17170c8c4020"
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn ticks_match_golden() {
    let updates = collect(
        tpch(Duration::from_secs(1)).by_seed(SYSTEM_TIME.clone(), None, MzOffset::from(0)),
        500,
    );
    assert_eq!(updates.len(), 92406);
    assert_eq!(
        digest(&updates),
        "f3e9a44157c5e4bd1a5215d4c10d4df4677f9b8d02a033510414d4715b92b85f"
    );
}

/// Splits updates into the sorted messages and the progress events in order.
fn split(updates: Vec<Update>) -> (Vec<(u64, LoadGeneratorOutput, Row, Diff)>, Vec<u64>) {
    let mut messages = Vec::new();
    let mut progress = Vec::new();
    for (output, event) in updates {
        match event {
            Event::Message(offset, (row, diff)) => {
                messages.push((offset.offset, output, row, diff))
            }
            Event::Progress(offset) => progress.push(offset.expect("finite").offset),
        }
    }
    messages.sort();
    (messages, progress)
}

fn assert_partitions_match(tick: Duration, up_to: u64) {
    let tpch = tpch(tick);
    let single = collect(
        tpch.by_seed(SYSTEM_TIME.clone(), None, MzOffset::from(0)),
        up_to,
    );
    let (expected_messages, expected_progress) = split(single);
    for worker_count in [1, 3, 4] {
        let mut messages = Vec::new();
        let mut owned_counts = Vec::new();
        for worker_index in 0..worker_count {
            let updates = collect(
                tpch.by_seed_for_worker(
                    SYSTEM_TIME.clone(),
                    None,
                    MzOffset::from(0),
                    worker_index,
                    worker_count,
                ),
                up_to,
            );
            let (worker_messages, progress) = split(updates);
            if worker_index == 0 || tick.is_zero() {
                assert_eq!(
                    progress, expected_progress,
                    "worker {worker_index}/{worker_count} progress"
                );
            } else {
                assert_eq!(
                    progress,
                    [1],
                    "worker {worker_index}/{worker_count} ends after the snapshot"
                );
            }
            owned_counts.push(worker_messages.len());
            messages.extend(worker_messages);
        }
        messages.sort();
        assert_eq!(
            messages, expected_messages,
            "union of {worker_count} workers"
        );
        if worker_count > 1 {
            assert!(
                owned_counts.iter().all(|n| *n > 0),
                "every worker owns rows: {owned_counts:?}"
            );
        }
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn snapshot_partitions_match() {
    assert_partitions_match(Duration::ZERO, u64::MAX);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn ticks_partitions_match() {
    assert_partitions_match(Duration::from_secs(1), 500);
}
