// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the ingestion dataflow of a [`MySqlSourceConnection`].

use std::convert::Infallible;

use differential_dataflow::{AsCollection, Collection};
use mz_repr::{Diff, Row};
use mz_storage_types::sources::MySqlSourceConnection;
use mz_timely_util::builder_async::PressOnDropButton;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use crate::healthcheck::{HealthStatusMessage, StatusNamespace};
use crate::source::types::SourceRender;
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

mod timestamp;

use timestamp::TransactionId;

impl SourceRender for MySqlSourceConnection {
    type Key = ();
    type Value = Row;
    // TODO: Eventually replace with a Partitioned<Uuid, TransactionId> timestamp
    type Time = TransactionId;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::Postgres;

    /// Render the ingestion dataflow. This function only connects things together and contains no
    /// actual processing logic.
    fn render<G: Scope<Timestamp = TransactionId>>(
        self,
        scope: &mut G,
        _config: RawSourceCreationConfig,
        _resume_uppers: impl futures::Stream<Item = Antichain<TransactionId>> + 'static,
        _start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        Collection<G, (usize, Result<SourceMessage<(), Row>, SourceReaderError>), Diff>,
        Option<Stream<G, Infallible>>,
        Stream<G, HealthStatusMessage>,
        Vec<PressOnDropButton>,
    ) {
        let data = timely::dataflow::operators::generic::operator::empty(scope);
        let health = timely::dataflow::operators::generic::operator::empty(scope);

        (data.as_collection(), None, health, vec![])
    }
}
