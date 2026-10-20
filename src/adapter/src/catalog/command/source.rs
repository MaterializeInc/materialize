// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_ore::assert_none;
use mz_repr::{CatalogItemId, GlobalId, RelationVersionSelector};
use mz_sql::catalog::SessionCatalog;
use mz_sql::names::ResolvedItemName;
use mz_sql::plan::{self, CreateSourcePlanBundle, Params, Plan, PlanContext, StatementContext};
use mz_sql::pure::{
    PurifiedStatement, generate_progress_subsource_statement, generate_subsource_statements,
};
use mz_sql_parser::ast::{
    CreateSourceOptionName, CreateSubsourceOption, CreateSubsourceOptionName, DeferredItemName,
    Statement, UnresolvedItemName,
};

use crate::AdapterError;
use crate::catalog::ConnCatalog;

/// Stable allocation keys, independent of metadata enumeration order.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SourceObject {
    Progress,
    Primary,
    Export(UnresolvedItemName),
}

#[derive(Debug)]
pub(crate) enum SourcePlanning {
    NeedsIds(Vec<SourceObject>),
    Ready(Vec<CreateSourcePlanBundle>),
}

pub(crate) fn plan_create_source(
    catalog: &ConnCatalog<'_>,
    pcx: &PlanContext,
    params: &Params,
    purified: PurifiedStatement,
    ids: &BTreeMap<SourceObject, (CatalogItemId, GlobalId)>,
) -> Result<SourcePlanning, AdapterError> {
    let PurifiedStatement::PurifiedCreateSource {
        create_progress_subsource,
        mut create_source_stmt,
        subsources,
        available_source_references,
    } = purified
    else {
        return Err(AdapterError::Unsupported(
            "non-CREATE SOURCE source planning",
        ));
    };
    let mut create_source_plans = Vec::with_capacity(subsources.len() + 2);

    if create_progress_subsource {
        let progress_stmt = {
            let scx = StatementContext::new(None, catalog);
            generate_progress_subsource_statement(&scx, &create_source_stmt)?
        };
        // The primary source references the progress collection, so emit it first.
        assert_none!(progress_stmt.of_source);
        let Some(&(item_id, global_id)) = ids.get(&SourceObject::Progress) else {
            return Ok(SourcePlanning::NeedsIds(vec![SourceObject::Progress]));
        };
        let progress_plan =
            super::plan_subsource(catalog, pcx, params, progress_stmt, item_id, global_id)?;
        let progress_full_name = catalog.resolve_full_name(&progress_plan.plan.name);
        let progress_subsource = ResolvedItemName::Item {
            id: progress_plan.item_id,
            qualifiers: progress_plan.plan.name.qualifiers.clone(),
            full_name: progress_full_name,
            print_id: true,
            version: RelationVersionSelector::Latest,
        };

        create_source_plans.push(progress_plan);

        create_source_stmt.progress_subsource = Some(DeferredItemName::Named(progress_subsource));
    }

    let resolved_ids = mz_sql::names::visit_dependencies(catalog, &create_source_stmt);

    let propagated_with_options: Vec<_> = create_source_stmt
        .with_options
        .iter()
        .filter_map(|opt| match opt.name {
            CreateSourceOptionName::TimestampInterval => None,
            CreateSourceOptionName::RetainHistory => Some(CreateSubsourceOption {
                name: CreateSubsourceOptionName::RetainHistory,
                value: opt.value.clone(),
            }),
        })
        .collect();

    let source_plan = match mz_sql::plan::plan(
        Some(pcx),
        catalog,
        Statement::CreateSource(create_source_stmt),
        params,
        &resolved_ids,
    )? {
        (Plan::CreateSource(plan), _sql_impl_ids) => plan,
        (p, _) => unreachable!("s must be CreateSourcePlan but got {:?}", p),
    };

    let Some(&(item_id, global_id)) = ids.get(&SourceObject::Primary) else {
        return Ok(SourcePlanning::NeedsIds(vec![SourceObject::Primary]));
    };

    let source_full_name = catalog.resolve_full_name(&source_plan.name);
    let of_source = ResolvedItemName::Item {
        id: item_id,
        qualifiers: source_plan.name.qualifiers.clone(),
        full_name: source_full_name,
        print_id: true,
        version: RelationVersionSelector::Latest,
    };

    let conn_catalog = catalog.for_system_session();
    let generated_pcx = plan::PlanContext::zero();
    let scx = StatementContext::new(Some(&generated_pcx), &conn_catalog);

    let mut subsource_stmts = generate_subsource_statements(&scx, of_source, subsources)?;

    for subsource_stmt in subsource_stmts.iter_mut() {
        subsource_stmt
            .with_options
            .extend(propagated_with_options.iter().cloned())
    }

    create_source_plans.push(CreateSourcePlanBundle {
        item_id,
        global_id,
        plan: source_plan,
        resolved_ids: resolved_ids.clone(),
        available_source_references: Some(available_source_references),
    });

    let missing: Vec<_> = subsource_stmts
        .iter()
        .map(|stmt| SourceObject::Export(stmt.name.clone()))
        .filter(|key| !ids.contains_key(key))
        .collect();
    if !missing.is_empty() {
        return Ok(SourcePlanning::NeedsIds(missing));
    }
    for stmt in subsource_stmts {
        let (item_id, global_id) = ids[&SourceObject::Export(stmt.name.clone())];
        let plan = super::plan_subsource(catalog, pcx, params, stmt, item_id, global_id)?;
        create_source_plans.push(plan);
    }

    Ok(SourcePlanning::Ready(create_source_plans))
}
