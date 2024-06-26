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
use mz_storage_types::errors::{DataflowError, DecodeError};
use mz_storage_types::sources::SourceTimestamp;
use mz_timely_util::builder_async::PressOnDropButton;
use serde::{Deserialize, Serialize};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use crate::healthcheck::{HealthStatusMessage, StatusNamespace};
use crate::source::RawSourceCreationConfig;

/// An update produced by implementors of `SourceRender` that presents an _aggregated_
/// description of the number of _offset_committed_ and _offset_known_ for the given
/// source.
///
/// The aggregate is required to be a 64 bit unsigned integer, whose units are
/// implementation-defined.
#[derive(Clone, Debug)]
pub enum ProgressStatisticsUpdate {
    SteadyState {
        offset_known: u64,
        offset_committed: u64,
    },
    Snapshot {
        records_known: u64,
        records_staged: u64,
    },
}

/// Describes a source that can render itself in a timely scope.
pub trait SourceRender {
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
        resume_uppers: impl futures::Stream<Item = Antichain<Self::Time>> + 'static,
        start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        Collection<G, (usize, Result<SourceMessage, DataflowError>), Diff>,
        Option<Stream<G, Infallible>>,
        Stream<G, HealthStatusMessage>,
        Stream<G, ProgressStatisticsUpdate>,
        Vec<PressOnDropButton>,
    );
}

/// Source-agnostic wrapper for messages. Each source must implement a
/// conversion to Message.
#[derive(Debug, Clone)]
pub struct SourceMessage {
    /// The message key
    pub key: Row,
    /// The message value
    pub value: Row,
    /// Additional metadata columns requested by the user
    pub metadata: Row,
}

mod columnation {
    use columnation::{Columnation, Region};
    use mz_repr::Row;

    use super::SourceMessage;

    impl Columnation for SourceMessage {
        type InnerRegion = SourceMessageRegion;
    }

    #[derive(Default)]
    pub struct SourceMessageRegion {
        inner: <Row as Columnation>::InnerRegion,
    }

    impl Region for SourceMessageRegion {
        type Item = SourceMessage;

        unsafe fn copy(&mut self, item: &Self::Item) -> Self::Item {
            SourceMessage {
                key: self.inner.copy(&item.key),
                value: self.inner.copy(&item.value),
                metadata: self.inner.copy(&item.metadata),
            }
        }

        fn clear(&mut self) {
            self.inner.clear()
        }

        fn reserve_items<'a, I>(&mut self, items: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self::Item> + Clone,
        {
            self.inner.reserve_items(
                items
                    .map(|item| [&item.key, &item.value, &item.metadata])
                    .flatten(),
            )
        }

        fn reserve_regions<'a, I>(&mut self, regions: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self> + Clone,
        {
            self.inner.reserve_regions(regions.map(|r| &r.inner))
        }

        fn heap_size(&self, callback: impl FnMut(usize, usize)) {
            self.inner.heap_size(callback)
        }
    }
}

/// A record produced by a source
#[derive(Clone, Serialize, Debug, Deserialize)]
pub struct SourceOutput<FromTime> {
    /// The record's key (or some empty/default value for sources without the concept of key)
    pub key: Row,
    /// The record's value
    pub value: Row,
    /// Additional metadata columns requested by the user
    pub metadata: Row,
    /// The original timestamp of this message
    pub from_time: FromTime,
}

/// The output of the decoding operator
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct DecodeResult<FromTime> {
    /// The decoded key
    pub key: Option<Result<Row, DecodeError>>,
    /// The decoded value, as well as the the
    /// differential `diff` value for this value, if the value
    /// is present and not and error.
    pub value: Option<Result<Row, DecodeError>>,
    /// Additional metadata requested by the user
    pub metadata: Row,
    /// The original timestamp of this message
    pub from_time: FromTime,
}
