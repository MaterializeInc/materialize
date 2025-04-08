// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::convert::Infallible;

use futures::{Future, Stream};
use mz_repr::GlobalId;
use mz_sql_server_util::cdc::Lsn;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SqlServerSource;
use mz_timely_util::builder_async::PressOnDropButton;
use timely::dataflow::{Scope, Stream as TimelyStream};
use timely::progress::Antichain;

use crate::healthcheck::{HealthStatusMessage, StatusNamespace};
use crate::source::types::{Probe, ProgressStatisticsUpdate, SourceRender, StackedCollection};
use crate::source::{RawSourceCreationConfig, SourceMessage};

impl SourceRender for SqlServerSource {
    type Time = Lsn;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::SqlServer;

    fn render<G: Scope<Timestamp = Self::Time>>(
        self,
        _scope: &mut G,
        _config: &RawSourceCreationConfig,
        _resume_uppers: impl Stream<Item = Antichain<Self::Time>> + 'static,
        _start_signal: impl Future<Output = ()> + 'static,
    ) -> (
        // Timely Collection for each Source Export defined in the provided `config`.
        BTreeMap<GlobalId, StackedCollection<G, Result<SourceMessage, DataflowError>>>,
        TimelyStream<G, Infallible>,
        TimelyStream<G, HealthStatusMessage>,
        TimelyStream<G, ProgressStatisticsUpdate>,
        Option<TimelyStream<G, Probe<Self::Time>>>,
        Vec<PressOnDropButton>,
    ) {
        unreachable!("SQL Server source not yet implemented")
    }
}
