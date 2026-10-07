// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeSet, HashSet};

use mz_ore::instrument;
use mz_sql::catalog::ErrorMessageObjectDescription;
use mz_sql::names::ObjectId;
use mz_sql::plan;
use mz_sql::session::metadata::SessionMetadata;
use tracing::Span;

use super::{DropOps, return_if_err};
use crate::command::ExecuteResponse;
use crate::coord::{Coordinator, DropObjectsStage, Message, PlanValidity, StageResult, Staged};
use crate::{AdapterError, AdapterNotice, ExecuteContext};

impl Staged for DropObjectsStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut PlanValidity {
        &mut self.validity
    }

    async fn stage(
        mut self,
        coord: &mut Coordinator,
        ctx: &mut ExecuteContext,
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        if coord.catalog().transient_revision() != self.planning_revision {
            // Replanning this statement enters ordinary DDL serialization again.
            // Do not retain its previous guard or lend it to another statement.
            coord.release_ddl_lock(ctx.session().conn_id());
            return Err(AdapterError::CatalogSnapshotChanged);
        }
        let committed = match coord
            .try_catalog_transact_with_context(ctx, &self.ops, &mut self.prepared)
            .await
        {
            Err(AdapterError::CatalogSnapshotChanged) => {
                coord.release_ddl_lock(ctx.session().conn_id());
                return Err(AdapterError::CatalogSnapshotChanged);
            }
            result => result?,
        };
        if !committed {
            let delay = coord.read_protection_conflict_delay();
            return Ok(StageResult::Await(Box::pin(async move {
                tokio::time::sleep(delay).await;
                Ok(Box::new(self))
            })));
        }

        if !self.expr_cache_invalidate_ids.is_empty() {
            let _fut = coord.catalog().update_expression_cache(
                Default::default(),
                Default::default(),
                self.expr_cache_invalidate_ids,
            );
        }
        fail::fail_point!("after_sequencer_drop_replica");
        if self.dropped_active_db {
            ctx.session()
                .add_notice(AdapterNotice::DroppedActiveDatabase {
                    name: ctx.session().vars().database().to_string(),
                });
        }
        if self.dropped_active_cluster {
            ctx.session()
                .add_notice(AdapterNotice::DroppedActiveCluster {
                    name: ctx.session().vars().cluster().to_string(),
                });
        }
        Ok(StageResult::Response(ExecuteResponse::DroppedObject(
            self.object_type,
        )))
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::DropObjectsStageReady {
            ctx,
            span,
            stage: self,
        }
    }

    fn cancel_enabled(&self) -> bool {
        true
    }
}

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_drop_objects(
        &mut self,
        ctx: ExecuteContext,
        plan::DropObjectsPlan {
            drop_ids,
            object_type,
            referenced_ids,
        }: plan::DropObjectsPlan,
    ) {
        let referenced_ids = referenced_ids.iter().collect::<HashSet<_>>();
        let mut objects = Vec::new();
        for obj_id in &drop_ids {
            if !referenced_ids.contains(obj_id) {
                objects.push(
                    ErrorMessageObjectDescription::from_id(
                        obj_id,
                        &self.catalog().for_session(ctx.session()),
                    )
                    .to_string(),
                );
            }
        }
        if !objects.is_empty() {
            ctx.session()
                .add_notice(AdapterNotice::CascadeDroppedObject { objects });
        }
        let expr_cache_invalidate_ids = drop_ids
            .iter()
            .filter_map(|id| match id {
                ObjectId::Item(item_id) => Some(self.catalog().get_entry(item_id).global_ids()),
                _ => None,
            })
            .flatten()
            .collect();
        let DropOps {
            ops,
            dropped_active_db,
            dropped_active_cluster,
        } = return_if_err!(self.sequence_drop_common(ctx.session(), drop_ids), ctx);
        let stage = DropObjectsStage {
            validity: PlanValidity::new(
                self.catalog(),
                BTreeSet::new(),
                None,
                None,
                ctx.session().role_metadata().clone(),
            ),
            planning_revision: self.catalog().transient_revision(),
            ops,
            prepared: None,
            object_type,
            dropped_active_db,
            dropped_active_cluster,
            expr_cache_invalidate_ids,
        };
        self.sequence_staged(ctx, Span::current(), stage).await;
    }
}
