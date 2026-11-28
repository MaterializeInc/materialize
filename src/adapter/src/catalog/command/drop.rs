// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Relation drops resolve names, cascade expansion, and physical readers at commit.

use std::collections::BTreeSet;
use std::sync::Arc;

use mz_catalog::memory::objects::Index;
use mz_compute_client::controller::ComputeController;
use mz_repr::explain::ExprHumanizer;
use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::ast::{Raw, Statement};
use mz_sql::catalog::{ErrorMessageObjectDescription, SessionCatalog};
use mz_sql::names::ObjectId;
use mz_sql::plan::{DropObjectsPlan, Params, Plan, PlanNotice};

use crate::catalog::{CatalogState, ConnCatalog, DropObjectInfo, Op};
use crate::notice::DroppedInUseIndex;
use crate::session::SessionMeta;
use crate::{AdapterError, AdapterNotice};

use super::CommandExecution;

#[derive(Clone, Debug, Default)]
pub(crate) struct IndexDropNotices {
    pub orphaned: Vec<DroppedInUseIndex>,
    pub in_flight: Vec<(String, Vec<GlobalId>)>,
}

#[derive(Clone, Debug)]
pub struct DropStatement {
    statement: Arc<Statement<Raw>>,
    params: Params,
    session: Arc<SessionMeta>,
    notices: Vec<PlanNotice>,
    adapter_notices: Vec<AdapterNotice>,
    index_notices: IndexDropNotices,
}

impl DropStatement {
    pub(crate) fn new(
        statement: Arc<Statement<Raw>>,
        params: Params,
        session: SessionMeta,
    ) -> Self {
        Self {
            statement,
            params,
            session: Arc::new(session),
            notices: Vec::new(),
            adapter_notices: Vec::new(),
            index_notices: IndexDropNotices::default(),
        }
    }

    pub(crate) fn plan(&mut self, state: &CatalogState) -> Result<DropObjectsPlan, AdapterError> {
        self.adapter_notices.clear();
        self.index_notices = IndexDropNotices::default();
        let (result, notices) =
            super::plan_command_with_notices(state, &self.statement, &self.params, &self.session);
        self.notices = notices;
        let (plan, _) = result?;
        let Plan::DropObjects(plan) = plan else {
            return Err(AdapterError::Unsupported("non-DROP relation intent"));
        };
        Ok(plan)
    }

    pub(super) fn compile(
        &mut self,
        state: &CatalogState,
        compute: Option<&ComputeController>,
    ) -> Result<CommandExecution<'_>, AdapterError> {
        let plan = self.plan(state)?;
        // Item dependency expansion cannot drop roles or namespaces. Their cleanup
        // requires a different compiler, not an unconditional DropObjects operation.
        if plan
            .drop_ids
            .iter()
            .any(|id| !matches!(id, ObjectId::Item(_)))
        {
            return Err(AdapterError::Internal(
                "relation DROP expanded to a non-item".into(),
            ));
        }
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let humanizer = state.for_session_metadata(&*self.session, tx);
        self.index_notices = check_index_dependents(state, &humanizer, compute, &plan.drop_ids)?;
        let referenced: BTreeSet<_> = plan.referenced_ids.iter().collect();
        let objects: Vec<_> = plan
            .drop_ids
            .iter()
            .filter(|id| !referenced.contains(id))
            .map(|id| ErrorMessageObjectDescription::from_id(id, &humanizer).to_string())
            .collect();
        self.adapter_notices.clear();
        if !objects.is_empty() {
            self.adapter_notices
                .push(AdapterNotice::CascadeDroppedObject { objects });
        }
        Ok(CommandExecution::raw(vec![Op::DropObjects {
            objects: plan
                .drop_ids
                .into_iter()
                .map(DropObjectInfo::manual_drop_from_object_id)
                .collect(),
            cascade: plan.cascade,
        }]))
    }

    pub(crate) fn notices(&self) -> &[PlanNotice] {
        &self.notices
    }
    pub(crate) fn adapter_notices(&self) -> &[AdapterNotice] {
        &self.adapter_notices
    }
    pub(crate) fn index_notices(&self) -> &IndexDropNotices {
        &self.index_notices
    }
}

pub(crate) fn check_index_dependents(
    state: &CatalogState,
    humanizer: &ConnCatalog<'_>,
    compute: Option<&ComputeController>,
    drop_ids: &[ObjectId],
) -> Result<IndexDropNotices, AdapterError> {
    let drop_in_use_allowed = state.system_config().enable_drop_in_use_index();
    let humanize_index = |index: &Index, item_id: &CatalogItemId| {
        humanizer
            .humanize_id(index.global_id())
            .unwrap_or_else(|| item_id.to_string())
    };

    let drop_set: BTreeSet<ObjectId> = drop_ids.iter().cloned().collect();
    let mut orphaned = Vec::new();
    let mut blocked = Vec::new();
    let mut transient_by_index = Vec::new();
    for id in drop_ids {
        let ObjectId::Item(item_id) = id else {
            continue;
        };
        let Some(index) = state.get_entry(item_id).index() else {
            continue;
        };
        let (durable, transient) = index_readers(
            state,
            compute.ok_or_else(|| {
                AdapterError::Internal("index drop requires live compute readers".into())
            })?,
            index,
            &drop_set,
        );
        if !transient.is_empty() {
            transient_by_index.push((index, item_id, transient));
        }
        if durable.is_empty() {
            continue;
        }
        if !drop_in_use_allowed {
            let dependents = durable
                .into_iter()
                .map(|dependent_id| {
                    let entry = state.get_entry(&dependent_id);
                    (
                        entry.item_type().to_string(),
                        humanizer.minimal_qualification(entry.name()).to_string(),
                    )
                })
                .collect();
            // Minimal qualification, like the planner's RESTRICT error.
            let index_name = humanizer
                .minimal_qualification(state.get_entry(item_id).name())
                .to_string();
            blocked.push((index_name, dependents));
            continue;
        }
        let dependant_objects = durable
            .into_iter()
            .filter_map(|dependent_id| {
                let entry = state.get_entry(&dependent_id);
                humanizer.humanize_id(entry.latest_global_id())
            })
            .collect();
        orphaned.push(DroppedInUseIndex {
            index_name: humanize_index(index, item_id),
            dependant_objects,
        });
    }

    if !blocked.is_empty() {
        return Err(AdapterError::IndexInUse { blocked });
    }

    Ok(IndexDropNotices {
        orphaned,
        in_flight: transient_by_index
            .into_iter()
            .map(|(index, item_id, readers)| (humanize_index(index, item_id), readers))
            .collect(),
    })
}

fn index_readers(
    state: &CatalogState,
    compute: &ComputeController,
    index: &Index,
    drop_set: &BTreeSet<ObjectId>,
) -> (Vec<CatalogItemId>, Vec<GlobalId>) {
    let mut durable = Vec::new();
    let mut transient = Vec::new();
    let readers = compute
        .collection_reverse_dependencies(index.cluster_id, index.global_id())
        .ok()
        .into_iter()
        .flatten();
    for gid in readers {
        if gid.is_transient() {
            transient.push(gid);
            continue;
        }
        // A collection whose item is gone from the catalog is already being torn down.
        let Some(entry) = state.try_get_entry_by_global_id(&gid) else {
            continue;
        };
        if !drop_set.contains(&ObjectId::Item(entry.id())) {
            durable.push(entry.id());
        }
    }
    (durable, transient)
}
