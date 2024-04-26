// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::lattice::Lattice;
use maplit::btreemap;
use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::memory::objects::{CatalogItem, MaterializedView};
use mz_expr::refresh_schedule::RefreshSchedule;
use mz_expr::CollectionPlan;
use mz_ore::collections::CollectionExt;
use mz_ore::instrument;
use mz_ore::soft_panic_or_log;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::optimize::OverrideFrom;
use mz_repr::Datum;
use mz_repr::Row;
use mz_sql::ast::ExplainStage;
use mz_sql::catalog::CatalogError;
use mz_sql::names::{ObjectId, ResolvedIds};
use mz_sql::plan;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast;
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_client::controller::{CollectionDescription, DataSource, DataSourceOther};
use timely::progress::Antichain;
use tracing::Span;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateMaterializedViewExplain, CreateMaterializedViewFinish,
    CreateMaterializedViewOptimize, CreateMaterializedViewStage, ExplainContext,
    ExplainPlanContext, Message, PlanValidity, StageResult, Staged,
};
use crate::error::AdapterError;
use crate::explain::explain_dataflow;
use crate::explain::explain_plan;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::util::ResultExt;
use crate::ReadHolds;
use crate::{catalog, AdapterNotice, CollectionIdBundle, ExecuteContext, TimestampProvider};

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_create_continually_insert(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateContinuallyInsertPlan,
        resolved_ids: ResolvedIds,
    ) {
        todo!(
            "create continually insert: {:?}, resolved ids: {:?}",
            plan,
            resolved_ids
        );
    }
}
