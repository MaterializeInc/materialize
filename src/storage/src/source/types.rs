// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to the source ingestion pipeline/framework.

// https://github.com/tokio-rs/prost/issues/237
// #![allow(missing_docs)]

use std::convert::Infallible;
use std::fmt::Debug;

use differential_dataflow::Collection;
use mz_repr::{Diff, Row};
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::errors::{DecodeError, SourceErrorDetails};
use mz_storage_types::sources::{MzOffset, SourceTimestamp};
use mz_timely_util::builder_async::PressOnDropButton;
use serde::{Deserialize, Serialize};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use crate::healthcheck::{HealthStatusMessage, StatusNamespace};
use crate::source::RawSourceCreationConfig;

/// Describes a source that can render itself in a timely scope.
pub trait SourceRender {
    type Key: timely::Data + MaybeLength;
    type Value: timely::Data + MaybeLength;
    type Time: SourceTimestamp;
    const STATUS_NAMESPACE: StatusNamespace;

    /// Renders the source in the provided timely scope.
    ///
    /// The `resume_uppers` stream can be used by the source to observe the overall progress of the
    /// ingestion. When a frontier appears in this stream the source implementation can be certain
    /// that future ingestion instances will request to read the external data only at times beyond
    /// that frontier. Therefore, the source implementation can react to this stream by e.g
    /// committing offsets upstream or advancing the LSN of a replication slot. It is safe to
    /// ignore this argument.
    ///
    /// Rendering a source is expected to return four things.
    ///
    /// First, a source must produce a collection that is produced by the rendered dataflow and
    /// must contain *definite*[^1] data for all times beyond the resumption frontier.
    ///
    /// Second, a source may produce an optional progress stream that will be used to drive
    /// reclocking. This is useful for sources that can query the highest offsets of the external
    /// source before reading the data for those offsets. In those cases it is preferable to
    /// produce this additional stream.
    ///
    /// Third, a source must produce a stream of health status updates.
    ///
    /// Finally, the source is expected to return an opaque token that when dropped will cause the
    /// source to immediately drop all capabilities and advance its frontier to the empty antichain.
    ///
    /// [^1] <https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210831_correctness.md#describing-definite-data>
    fn render<G: Scope<Timestamp = Self::Time>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        connection_context: ConnectionContext,
        resume_uppers: impl futures::Stream<Item = Antichain<Self::Time>> + 'static,
        start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        Collection<
            G,
            (
                usize,
                Result<SourceMessage<Self::Key, Self::Value>, SourceReaderError>,
            ),
            Diff,
        >,
        Option<Stream<G, Infallible>>,
        Stream<G, HealthStatusMessage>,
        Vec<PressOnDropButton>,
    );
}

/// Source-agnostic wrapper for messages. Each source must implement a
/// conversion to Message.
#[derive(Debug, Clone)]
pub struct SourceMessage<Key, Value> {
    /// The message key
    pub key: Key,
    /// The message value
    pub value: Value,
    /// Additional metadata columns requested by the user
    pub metadata: Row,
}

/// A record produced by a source
#[derive(Clone, Serialize, Debug, Deserialize)]
pub struct SourceOutput<K, V> {
    /// The record's key (or some empty/default value for sources without the concept of key)
    pub key: K,
    /// The record's value
    pub value: V,
    /// Additional metadata columns requested by the user
    pub metadata: Row,
    /// The offset position in the partition of a kafka source. This is field is on its way out and
    /// its only valid use is in the upsert operator. Do NOT use it in any new place!
    // TODO(petrosagg): remove this field
    pub position_for_upsert: MzOffset,
}

impl<K, V> SourceOutput<K, V> {
    /// Build a new SourceOutput
    pub fn new(
        key: K,
        value: V,
        metadata: Row,
        position_for_upsert: MzOffset,
    ) -> SourceOutput<K, V> {
        SourceOutput {
            key,
            value,
            metadata,
            position_for_upsert,
        }
    }
}

/// The output of the decoding operator
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct DecodeResult {
    /// The decoded key
    pub key: Option<Result<Row, DecodeError>>,
    /// The decoded value, as well as the the
    /// differential `diff` value for this value, if the value
    /// is present and not and error.
    pub value: Option<Result<Row, DecodeError>>,
    /// Additional metadata requested by the user
    pub metadata: Row,
    /// The offset position in the partition of a kafka source. This is field is on its way out and
    /// its only valid use is in the upsert operator. Do NOT use it in any new place!
    // TODO(petrosagg): remove this field
    pub position_for_upsert: MzOffset,
}

/// A structured error for `SourceReader::get_next_message` implementors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceReaderError {
    pub inner: SourceErrorDetails,
}

impl SourceReaderError {
    /// This is an unclassified but definite error. This is typically only appropriate
    /// when the error is permanently fatal for the source... some critical invariant
    /// is violated or data is corrupted, for example.
    pub fn other_definite(e: anyhow::Error) -> SourceReaderError {
        SourceReaderError {
            inner: SourceErrorDetails::Other(format!("{}", e)),
        }
    }
}

/// Types that implement this trait expose a length function
pub trait MaybeLength {
    /// Returns the size of the object
    fn len(&self) -> Option<usize>;
}

impl MaybeLength for () {
    fn len(&self) -> Option<usize> {
        None
    }
}

impl MaybeLength for Vec<u8> {
    fn len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl MaybeLength for mz_repr::Row {
    fn len(&self) -> Option<usize> {
        Some(self.data().len())
    }
}

impl<T: MaybeLength> MaybeLength for Option<T> {
    fn len(&self) -> Option<usize> {
        self.as_ref().and_then(|v| v.len())
    }
}
