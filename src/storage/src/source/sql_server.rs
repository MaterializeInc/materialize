// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the ingestion dataflow of a [`SqlServerSourceConnection`].

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;

use mz_repr::GlobalId;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::{sql_server, MzOffset, SqlServerSource};
use mz_timely_util::builder_async::PressOnDropButton;
use timely::dataflow::{Scope, Stream as TimelyStream};
use timely::progress::Antichain;

use crate::healthcheck::{HealthStatusMessage, StatusNamespace};
use crate::source::types::{
    Probe, ProgressStatisticsUpdate, SourceMessage, SourceRender, StackedCollection,
};
use crate::source::RawSourceCreationConfig;

impl SourceRender for SqlServerSource {
    type Time = sql_server::Lsn;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::SqlServer;

    fn render<G: Scope<Timestamp = Self::Time>>(
        self,
        scope: &mut G,
        config: &RawSourceCreationConfig,
        resume_uppers: impl futures::Stream<Item = Antichain<Self::Time>> + 'static,
        start_signal: impl Future<Output = ()> + 'static,
    ) -> (
        // Timely Collection for each Source Export defined in the provided `config`.
        BTreeMap<GlobalId, StackedCollection<G, Result<SourceMessage, DataflowError>>>,
        TimelyStream<G, Infallible>,
        TimelyStream<G, HealthStatusMessage>,
        TimelyStream<G, ProgressStatisticsUpdate>,
        Option<TimelyStream<G, Probe<Self::Time>>>,
        Vec<PressOnDropButton>,
    ) {
        tracing::error!(source_exports = ?config.source_exports, ?self, "SQL SERVER SOURCE");
        todo!()
    }
}
