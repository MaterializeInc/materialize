// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! View commands re-plan against the commit candidate. Only the
//! catalog-independent optimization is reusable, and only when its complete
//! inputs still match. Neither stale successes nor stale errors can authorize
//! publication.

use std::sync::Arc;

use mz_catalog::expr_cache::LocalExpressions;
use mz_catalog::memory::objects::{CatalogItem, View};
use mz_expr::{CollectionPlan, OptimizedMirRelationExpr};
use mz_repr::{CatalogItemId, GlobalId, RelationDesc, RelationVersion};
use mz_sql::ast::{Raw, Statement};
use mz_sql::names::ResolvedIds;
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::{CreateViewPlan, HirRelationExpr, Params, Plan, PlanNotice};
use mz_sql::session::metadata::SessionMetadata;

use crate::AdapterError;
use crate::catalog::{Catalog, CatalogState, DropObjectInfo, Op};
use crate::coord::infer_sql_type_for_catalog;
use crate::optimize::{Optimize, OptimizerConfig};
use crate::session::SessionMeta;

#[derive(Clone, Debug)]
pub struct ViewStatement {
    statement: Arc<Statement<Raw>>,
    params: Params,
    session: Arc<SessionMeta>,
    create_id: Option<(CatalogItemId, GlobalId)>,
    prepared: Option<(ViewPreparation, OptimizedMirRelationExpr)>,
    notices: Vec<PlanNotice>,
    skipped_name: Option<String>,
}

pub(crate) struct ViewCompilation {
    pub plan: CreateViewPlan,
    resolved_ids: ResolvedIds,
    notices: Vec<PlanNotice>,
}

pub(crate) enum ViewResolution {
    Prepare(ViewPreparation),
    Ready {
        ops: Vec<Op>,
        name: String,
        if_not_exists: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ViewPreparation {
    global_id: GlobalId,
    temporary: bool,
    expr: HirRelationExpr,
    config: OptimizerConfig,
}

#[derive(Debug)]
#[must_use = "consume optimization through ViewStatement::resolve"]
pub(crate) struct ViewPreparationResponse {
    request: ViewPreparation,
    result: Result<OptimizedMirRelationExpr, AdapterError>,
}

impl ViewStatement {
    pub(crate) fn new(
        statement: Arc<Statement<Raw>>,
        params: Params,
        session: SessionMeta,
    ) -> Self {
        Self {
            statement,
            params,
            session: Arc::new(session),
            create_id: None,
            prepared: None,
            notices: Vec::new(),
            skipped_name: None,
        }
    }

    pub(crate) fn allocate_create(&mut self, id: (CatalogItemId, GlobalId)) {
        assert!(
            self.create_id.is_none(),
            "view IDs must not change across retries"
        );
        self.create_id = Some(id);
    }

    pub(crate) fn compile_create(
        &self,
        state: &CatalogState,
    ) -> Result<ViewCompilation, AdapterError> {
        let (plan, resolved_ids, notices) =
            super::plan_command(state, &self.statement, &self.params, &self.session)?;
        let Plan::CreateView(plan) = plan else {
            return Err(AdapterError::Unsupported("non-CREATE VIEW intent"));
        };
        let dependencies = plan.view.expr.depends_on();
        state.validate_timeline_context(dependencies.iter().copied())?;
        state.validate_system_column_references(plan.ambiguous_columns, &dependencies)?;
        Ok(ViewCompilation {
            plan,
            resolved_ids,
            notices,
        })
    }

    pub(crate) fn preparation(
        &self,
        state: &CatalogState,
        compiled: &ViewCompilation,
    ) -> ViewPreparation {
        ViewPreparation {
            global_id: self.create_id.expect("view IDs allocated at admission").1,
            temporary: compiled.plan.view.temporary,
            expr: compiled.plan.view.expr.clone(),
            config: OptimizerConfig::from(state.system_config()),
        }
    }

    pub(crate) fn resolve(
        &mut self,
        state: &CatalogState,
        response: Option<ViewPreparationResponse>,
    ) -> Result<ViewResolution, AdapterError> {
        let compiled = self.compile_create(state)?;
        let request = self.preparation(state, &compiled);
        if let Some(response) = response {
            if request == response.request {
                self.prepared = Some((response.request, response.result?));
            }
        }
        let Some((prepared, optimized)) = &self.prepared else {
            return Ok(ViewResolution::Prepare(request));
        };
        if *prepared != request {
            return Ok(ViewResolution::Prepare(request));
        }
        let CreateViewPlan {
            name,
            view,
            drop_ids,
            if_not_exists,
            ..
        } = compiled.plan;
        let typ = infer_sql_type_for_catalog(&view.expr, optimized);
        let (id, global_id) = self.create_id.expect("view IDs allocated at admission");
        let ops = vec![
            Op::DropObjects {
                objects: drop_ids.into_iter().map(DropObjectInfo::Item).collect(),
                cascade: false,
            },
            Op::CreateItem {
                id,
                name: name.clone(),
                item: CatalogItem::View(View {
                    create_sql: view.create_sql,
                    global_id,
                    raw_expr: view.expr.into(),
                    desc: RelationDesc::new(typ, view.column_names),
                    locally_optimized_expr: optimized.clone().into(),
                    conn_id: view.temporary.then(|| self.session.conn_id().clone()),
                    resolved_ids: compiled.resolved_ids,
                    dependencies: view.dependencies,
                }),
                owner_id: *self.session.current_role_id(),
            },
        ];
        state.validate_resource_limits(&ops, self.session.conn_id())?;
        self.notices = compiled.notices;
        self.skipped_name = None;
        Ok(ViewResolution::Ready {
            ops,
            name: name.item,
            if_not_exists,
        })
    }

    pub(crate) fn notices(&self) -> &[PlanNotice] {
        &self.notices
    }

    pub(super) fn is_temporary(&self) -> bool {
        matches!(self.statement.as_ref(), Statement::CreateView(view) if view.temporary)
    }

    pub(crate) fn skipped_name(&self) -> Option<&str> {
        self.skipped_name.as_deref()
    }

    pub(in crate::catalog) fn skipped_name_mut(&mut self) -> &mut Option<String> {
        &mut self.skipped_name
    }

    pub(in crate::catalog) fn item_id(&self) -> CatalogItemId {
        self.create_id.expect("view IDs allocated at admission").0
    }
}

impl ViewPreparation {
    pub(crate) fn execute(
        self,
        catalog: &Catalog,
        metrics: OptimizerMetrics,
    ) -> impl std::future::Future<Output = ViewPreparationResponse> + Send + use<> {
        // Retain the cache sender, not a catalog snapshot across optimization and I/O.
        let cache = if self.temporary {
            None
        } else {
            catalog.expr_cache_handle.clone()
        };
        async move {
            let request = self.clone();
            let span = tracing::Span::current();
            let result = mz_ore::task::spawn_blocking(
                || "optimize view command",
                move || {
                    span.in_scope(|| {
                        let result =
                            crate::optimize::view::Optimizer::new(request.config, Some(metrics))
                                .catch_unwind_optimize(request.expr)
                                .map_err(AdapterError::from);
                        fail::fail_point!("catalog_view_optimize_ready");
                        result
                    })
                },
            )
            .await;
            if let (Some(cache), Ok(optimized)) = (cache, &result) {
                cache
                    .update(
                        vec![(
                            self.global_id,
                            LocalExpressions {
                                local_mir: optimized.clone(),
                                optimizer_features: self.config.features.clone(),
                                item_version: RelationVersion::root(),
                            },
                        )],
                        Vec::new(),
                        Default::default(),
                    )
                    .await;
            }
            ViewPreparationResponse {
                request: self,
                result,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_sql::session::vars::VarInput;

    use super::*;
    use crate::session::Session;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn view_preparation_checks_configuration_and_error_freshness() {
        Catalog::with_debug(|catalog| async move {
            let mut state = catalog.state().clone();
            let mut session = Session::dummy();
            session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
            session
                .start_transaction(mz_ore::now::to_datetime(1000), None, None)
                .expect("start invoking transaction");
            let raw = mz_sql_parser::parser::parse_statements(
                "CREATE VIEW materialize.public.prepared_view AS SELECT 1 AS a",
            )
            .expect("parse view statement")
            .remove(0)
            .ast;
            let mut command = ViewStatement::new(Arc::new(raw), Params::empty(), session.meta());
            let ts = catalog.current_upper().await;
            let ids = catalog
                .allocate_user_ids(1, ts)
                .await
                .expect("allocate view IDs")
                .remove(0);
            command.allocate_create(ids);
            let ViewResolution::Prepare(original) = command
                .resolve(&state, None)
                .expect("resolve initial preparation")
            else {
                panic!("first execution must request preparation");
            };
            let original_flag = state.system_config().enable_new_outer_join_lowering();
            state
                .system_config_mut()
                .set(
                    "enable_new_outer_join_lowering",
                    VarInput::Flat(if original_flag { "false" } else { "true" }),
                )
                .expect("change optimizer configuration");
            let ViewResolution::Prepare(current) = command
                .resolve(
                    &state,
                    Some(ViewPreparationResponse {
                        request: original,
                        result: Err(AdapterError::Unsupported("obsolete optimizer error")),
                    }),
                )
                .expect("obsolete errors must not decide the current command")
            else {
                panic!("changed configuration must require new preparation");
            };
            assert!(matches!(
                command.resolve(
                    &state,
                    Some(ViewPreparationResponse {
                        request: current.clone(),
                        result: Err(AdapterError::Unsupported("current optimizer error")),
                    })
                ),
                Err(AdapterError::Unsupported("current optimizer error"))
            ));

            let optimized = crate::optimize::view::Optimizer::new(current.config.clone(), None)
                .optimize(current.expr.clone())
                .expect("optimize current view");
            assert!(matches!(
                command
                    .resolve(
                        &state,
                        Some(ViewPreparationResponse {
                            request: current.clone(),
                            result: Ok(optimized.clone()),
                        })
                    )
                    .expect("accept current preparation"),
                ViewResolution::Ready { .. }
            ));

            state
                .system_config_mut()
                .set("max_tables", VarInput::Flat("999"))
                .expect("change unrelated limit");
            assert!(
                matches!(
                    command
                        .resolve(&state, None)
                        .expect("reuse current preparation"),
                    ViewResolution::Ready { .. }
                ),
                "unrelated configuration changes must reuse preparation"
            );

            state
                .system_config_mut()
                .set(
                    "enable_new_outer_join_lowering",
                    VarInput::Flat(if original_flag { "true" } else { "false" }),
                )
                .expect("restore optimizer configuration");
            assert!(
                matches!(
                    command
                        .resolve(
                            &state,
                            Some(ViewPreparationResponse {
                                request: current,
                                result: Ok(optimized),
                            })
                        )
                        .expect("reject obsolete success"),
                    ViewResolution::Prepare(_)
                ),
                "neither a late success nor retained preparation may bypass current inputs"
            );
            catalog.expire().await;
        })
        .await;
    }
}
