// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_persist::client::{StreamReadHandle, StreamWriteHandle};
use timely::dataflow::operators::{Concat, Map, OkErr};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use mz_dataflow_types::{DataflowError, DecodeError, SourceError, SourceErrorDetails};
use mz_expr::SourceInstanceId;
use mz_persist::operators::replay::Replay;
use mz_persist::operators::stream::{Persist, RetractUnsealed};
use mz_persist_types::Codec;
use mz_repr::{Diff, Row, Timestamp};

/// Persist configuration for `ENVELOPE NONE` sources.
#[derive(Debug, Clone)]
pub struct PersistentEnvelopeNoneConfig<V: Codec> {
    /// The timestamp up to which which data should be read when restoring.
    pub upper_seal_ts: u64,

    /// [`StreamReadHandle`] for the collection that we should persist to.
    pub read_handle: StreamReadHandle<V, ()>,

    /// [`StreamWriteHandle`] for the collection that we should persist to.
    pub write_handle: StreamWriteHandle<V, ()>,
}

impl<V: Codec> PersistentEnvelopeNoneConfig<V> {
    /// Creates a new [`PersistentEnvelopeNoneConfig`] from the given parts.
    pub fn new(
        upper_seal_ts: u64,
        read_handle: StreamReadHandle<V, ()>,
        write_handle: StreamWriteHandle<V, ()>,
    ) -> Self {
        PersistentEnvelopeNoneConfig {
            upper_seal_ts,
            read_handle,
            write_handle,
        }
    }
}

/// Persists the given input stream, passes through the data it carries and replays previously
/// persisted updates when starting up.
///
/// This will filter out and retract replayed updates that are not beyond the `upper_seal_ts` given
/// in `persist_config`.
///
pub(crate) fn persist_and_replay<G>(
    source_name: &str,
    source_id: SourceInstanceId,
    stream: &Stream<G, (Result<Row, DecodeError>, Timestamp, Diff)>,
    as_of_frontier: &Antichain<Timestamp>,
    persist_config: PersistentEnvelopeNoneConfig<Result<Row, DecodeError>>,
) -> (
    Stream<G, (Result<Row, DecodeError>, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = stream.scope();

    let (restored_oks, restored_errs) = {
        let snapshot = persist_config.read_handle.snapshot();

        let (restored_oks, restored_errs) =
            scope.replay(snapshot, as_of_frontier).ok_err(split_ok_err);

        let (restored_oks, retract_errs) = restored_oks.retract_unsealed(
            source_name,
            persist_config.write_handle.clone(),
            persist_config.upper_seal_ts,
        );

        let combined_errs = restored_errs.concat(&retract_errs);

        (restored_oks, combined_errs)
    };

    // Persist can only deal with (key, value) streams, so promote value to key.
    let flattened_stream = stream.map(|(row, ts, diff)| ((row, ()), ts, diff));

    let (flattened_stream, persist_errs) =
        flattened_stream.persist(source_name, persist_config.write_handle);

    let persist_errs = persist_errs.concat(&restored_errs);

    let persist_errs = persist_errs.map(move |(err, ts, diff)| {
        let source_error = SourceError::new(source_id, SourceErrorDetails::Persistence(err));
        (source_error.into(), ts, diff)
    });

    let combined_stream = flattened_stream.concat(&restored_oks);

    // Strip away the key/value separation that we needed for persistence.
    let combined_stream = combined_stream.map(|((k, ()), ts, diff)| (k, ts, diff));

    (combined_stream, persist_errs)
}

// TODO: Maybe we should finally move this to some central place and re-use. There seem to be
// enough instances of this by now.
fn split_ok_err<K, V>(
    x: (Result<(K, V), String>, u64, Diff),
) -> Result<((K, V), u64, Diff), (String, u64, Diff)> {
    match x {
        (Ok(kv), ts, diff) => Ok((kv, ts, diff)),
        (Err(err), ts, diff) => Err((err, ts, diff)),
    }
}
