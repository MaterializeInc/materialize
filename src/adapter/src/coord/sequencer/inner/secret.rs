// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::sync::Arc;

use mz_audit_log::{EventDetails, EventType, RotateKeysV1};
use mz_catalog::memory::objects::{CatalogItem, Secret};
use mz_expr::MirScalarExpr;
use mz_ore::instrument;
use mz_repr::{Datum, GlobalId, RowArena};
use mz_sql::catalog::{CatalogError, ObjectType};
use mz_sql::plan::{self, CreateSecretPlan};
use mz_sql::session::metadata::SessionMetadata;
use mz_ssh_util::keys::SshKeyPairSet;
use tracing::{warn, Instrument, Span};

use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    AlterSecret, Coordinator, CreateSecretEnsure, CreateSecretFinish, Message, PlanValidity,
    RotateKeysSecretEnsure, RotateKeysSecretFinish, SecretStage, StageResult, Staged,
};
use crate::optimize::dataflows::{prep_scalar_expr, EvalTime, ExprPrepStyle};
use crate::session::Session;
use crate::{catalog, AdapterError, AdapterNotice, ExecuteContext, ExecuteResponse};

impl Staged for SecretStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut crate::coord::PlanValidity {
        match self {
            SecretStage::CreateFinish(stage) => &mut stage.validity,
            SecretStage::RotateKeysFinish(stage) => &mut stage.validity,
            SecretStage::RotateKeysEnsure(stage) => &mut stage.validity,
            SecretStage::CreateEnsure(stage) => &mut stage.validity,
            SecretStage::Alter(stage) => &mut stage.validity,
        }
    }

    async fn stage(
        self,
        coord: &mut Coordinator,
        ctx: &mut ExecuteContext,
    ) -> Result<crate::coord::StageResult<Box<Self>>, AdapterError> {
        match self {
            SecretStage::CreateEnsure(stage) => {
                coord.create_secret_ensure(ctx.session(), stage).await
            }
            SecretStage::CreateFinish(stage) => {
                coord.create_secret_finish(ctx.session(), stage).await
            }
            SecretStage::RotateKeysEnsure(stage) => coord.rotate_keys_ensure(ctx.session(), stage),
            SecretStage::RotateKeysFinish(stage) => {
                coord.rotate_keys_finish(ctx.session(), stage).await
            }
            SecretStage::Alter(stage) => coord.alter_secret(ctx.session(), stage.plan),
        }
    }

    fn message(self, ctx: ExecuteContext, span: tracing::Span) -> Message {
        Message::SecretStageReady {
            ctx,
            span,
            stage: self,
        }
    }

    fn cancel_enabled(&self) -> bool {
        // Because secrets operations call out to external services and transact the catalog
        // separately, disable cancellation.
        false
    }
}

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_create_secret(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateSecretPlan,
    ) {
        let stage = return_if_err!(self.create_secret_validate(ctx.session(), plan).await, ctx);
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    async fn create_secret_validate(
        &mut self,
        session: &Session,
        plan: plan::CreateSecretPlan,
    ) -> Result<SecretStage, AdapterError> {
        // No dependencies.
        let validity = PlanValidity::new(
            self.catalog().transient_revision(),
            BTreeSet::new(),
            None,
            None,
            session.role_metadata().clone(),
        );
        Ok(SecretStage::CreateEnsure(CreateSecretEnsure {
            validity,
            plan,
        }))
    }

    #[instrument]
    async fn create_secret_ensure(
        &mut self,
        session: &Session,
        CreateSecretEnsure { validity, mut plan }: CreateSecretEnsure,
    ) -> Result<StageResult<Box<SecretStage>>, AdapterError> {
        let id = self.catalog_mut().allocate_user_id().await?;
        let secrets_controller = Arc::clone(&self.secrets_controller);
        let payload = self.extract_secret(session, &mut plan.secret.secret_as)?;
        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn(
            || "create secret ensure",
            async move {
                secrets_controller.ensure(id, &payload).await?;
                let stage = SecretStage::CreateFinish(CreateSecretFinish { validity, id, plan });
                Ok(Box::new(stage))
            }
            .instrument(span),
        )))
    }

    fn extract_secret(
        &self,
        session: &Session,
        secret_as: &mut MirScalarExpr,
    ) -> Result<Vec<u8>, AdapterError> {
        let temp_storage = RowArena::new();
        prep_scalar_expr(
            secret_as,
            ExprPrepStyle::OneShot {
                logical_time: EvalTime::NotAvailable,
                session,
                catalog_state: self.catalog().state(),
            },
        )?;
        let evaled = secret_as.eval(&[], &temp_storage)?;

        if evaled == Datum::Null {
            coord_bail!("secret value can not be null");
        }

        let payload = evaled.unwrap_bytes();

        // Limit the size of a secret to 512 KiB
        // This is the largest size of a single secret in Consul/Kubernetes
        // We are enforcing this limit across all types of Secrets Controllers
        // Most secrets are expected to be roughly 75B
        if payload.len() > 1024 * 512 {
            coord_bail!("secrets can not be bigger than 512KiB")
        }

        // Enforce that all secrets are valid UTF-8 for now. We expect to lift
        // this restriction in the future, when we discover a connection type
        // that requires binary secrets, but for now it is convenient to ensure
        // here that `SecretsReader::read_string` can never fail due to invalid
        // UTF-8.
        //
        // If you want to remove this line, verify that no caller of
        // `SecretsReader::read_string` will panic if the secret contains
        // invalid UTF-8.
        if std::str::from_utf8(payload).is_err() {
            // Intentionally produce a vague error message (rather than
            // including the invalid bytes, for example), to avoid including
            // secret material in the error message, which might end up in a log
            // file somewhere.
            coord_bail!("secret value must be valid UTF-8");
        }

        Ok(Vec::from(payload))
    }

    #[instrument]
    async fn create_secret_finish(
        &mut self,
        session: &Session,
        CreateSecretFinish {
            id,
            plan,
            validity: _,
        }: CreateSecretFinish,
    ) -> Result<StageResult<Box<SecretStage>>, AdapterError> {
        let CreateSecretPlan {
            name,
            secret,
            if_not_exists,
        } = plan;
        let secret = Secret {
            create_sql: secret.create_sql,
        };

        let ops = vec![catalog::Op::CreateItem {
            id,
            name: name.clone(),
            item: CatalogItem::Secret(secret),
            owner_id: *session.current_role_id(),
        }];

        let res = match self.catalog_transact(Some(session), ops).await {
            Ok(()) => Ok(ExecuteResponse::CreatedSecret),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: name.item,
                    ty: "secret",
                });
                Ok(ExecuteResponse::CreatedSecret)
            }
            Err(err) => {
                if let Err(e) = self.secrets_controller.delete(id).await {
                    warn!(
                        "Dropping newly created secrets has encountered an error: {}",
                        e
                    );
                }
                Err(err)
            }
        };
        res.map(StageResult::Response)
    }

    #[instrument]
    pub(crate) async fn sequence_alter_secret(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::AlterSecretPlan,
    ) {
        // Notably this does not include `plan.id` in `dependency_ids` because `alter_secret()` just
        // calls `ensure()` and returns a success result to the client. If there's a concurrent
        // delete of the secret, the persisted secret is in an unknown state (but will be cleaned up
        // if needed at next envd boot), but we will still return success.
        let validity = PlanValidity::new(
            self.catalog().transient_revision(),
            BTreeSet::new(),
            None,
            None,
            ctx.session().role_metadata().clone(),
        );
        let stage = SecretStage::Alter(AlterSecret { validity, plan });
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    fn alter_secret(
        &mut self,
        session: &Session,
        plan: plan::AlterSecretPlan,
    ) -> Result<StageResult<Box<SecretStage>>, AdapterError> {
        let plan::AlterSecretPlan { id, mut secret_as } = plan;
        let secrets_controller = Arc::clone(&self.secrets_controller);
        let payload = self.extract_secret(session, &mut secret_as)?;
        let span = Span::current();
        Ok(StageResult::HandleRetire(mz_ore::task::spawn(
            || "alter secret ensure",
            async move {
                secrets_controller.ensure(id, &payload).await?;
                Ok(ExecuteResponse::AlteredObject(ObjectType::Secret))
            }
            .instrument(span),
        )))
    }

    #[instrument]
    pub(crate) async fn sequence_rotate_keys(&mut self, ctx: ExecuteContext, id: GlobalId) {
        // If the secret is deleted from the catalog during `rotate_keys_ensure()`, this will
        // prevent `rotate_keys_finish()` from issuing the `WeirdBuiltinTableUpdates` for the
        // change. The state of the persisted secret is unknown, and if the rotate ensure'd
        // after the delete (i.e., the secret is persisted to the secret store but not the
        // catalog), the secret will be cleaned up during next envd boot.
        let validity = PlanValidity::new(
            self.catalog().transient_revision(),
            BTreeSet::from_iter(std::iter::once(id)),
            None,
            None,
            ctx.session().role_metadata().clone(),
        );
        let stage = SecretStage::RotateKeysEnsure(RotateKeysSecretEnsure { validity, id });
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    fn rotate_keys_ensure(
        &mut self,
        session: &Session,
        RotateKeysSecretEnsure { validity, id }: RotateKeysSecretEnsure,
    ) -> Result<StageResult<Box<SecretStage>>, AdapterError> {
        let secrets_controller = Arc::clone(&self.secrets_controller);
        let catalog = self.owned_catalog();
        let entry = catalog.get_entry(&id);
        let name = catalog.resolve_full_name(&entry.name, Some(session.conn_id()));
        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn(
            || "rotate keys ensure",
            async move {
                let secret = secrets_controller.reader().read(id).await?;
                let previous_key_set = SshKeyPairSet::from_bytes(&secret)?;
                let new_key_set = previous_key_set.rotate()?;
                secrets_controller
                    .ensure(id, &new_key_set.to_bytes())
                    .await?;

                let builtin_table_retraction = catalog.state().pack_ssh_tunnel_connection_update(
                    id,
                    &previous_key_set.public_keys(),
                    -1,
                );
                let builtin_table_retraction = catalog
                    .state()
                    .resolve_builtin_table_update(builtin_table_retraction);
                let builtin_table_addition = catalog.state().pack_ssh_tunnel_connection_update(
                    id,
                    &new_key_set.public_keys(),
                    1,
                );
                let builtin_table_addition = catalog
                    .state()
                    .resolve_builtin_table_update(builtin_table_addition);
                let ops = vec![
                    catalog::Op::WeirdBuiltinTableUpdates {
                        builtin_table_update: builtin_table_retraction,
                        audit_log: Vec::new(),
                    },
                    catalog::Op::WeirdBuiltinTableUpdates {
                        builtin_table_update: builtin_table_addition,
                        audit_log: vec![(
                            EventType::Alter,
                            mz_audit_log::ObjectType::Connection,
                            EventDetails::RotateKeysV1(RotateKeysV1 {
                                id: id.to_string(),
                                name: name.to_string(),
                            }),
                        )],
                    },
                ];
                let stage = SecretStage::RotateKeysFinish(RotateKeysSecretFinish { validity, ops });
                Ok(Box::new(stage))
            }
            .instrument(span),
        )))
    }

    #[instrument]
    async fn rotate_keys_finish(
        &mut self,
        session: &Session,
        RotateKeysSecretFinish { ops, validity: _ }: RotateKeysSecretFinish,
    ) -> Result<StageResult<Box<SecretStage>>, AdapterError> {
        self.catalog_transact(Some(session), ops).await?;
        Ok(StageResult::Response(ExecuteResponse::AlteredObject(
            ObjectType::Connection,
        )))
    }
}
