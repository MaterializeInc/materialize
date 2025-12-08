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

use mz_catalog::memory::objects::{CatalogItem, Secret};
use mz_expr::MirScalarExpr;
use mz_ore::collections::CollectionExt;
use mz_ore::instrument;
use mz_repr::{CatalogItemId, Datum, RowArena};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{ConnectionOption, ConnectionOptionName, Statement, Value, WithOptionValue};
use mz_sql::catalog::{CatalogError, ObjectType};
use mz_sql::plan::{self, CreateSecretPlan};
use mz_sql::session::metadata::SessionMetadata;
use mz_ssh_util::keys::SshKeyPairSet;
use tracing::{Instrument, Span, warn};

use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    AlterSecret, Coordinator, CreateSecretEnsure, CreateSecretFinish, Message, PlanValidity,
    RotateKeysSecretEnsure, RotateKeysSecretFinish, SecretStage, StageResult, Staged,
};
use crate::optimize::dataflows::{EvalTime, ExprPrepStyle, prep_scalar_expr};
use crate::session::Session;
use crate::{AdapterError, AdapterNotice, ExecuteContext, ExecuteResponse, catalog};

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
            SecretStage::RotateKeysEnsure(stage) => coord.rotate_keys_ensure(stage),
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
        &self,
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
        let id_ts = self.get_catalog_write_ts().await;
        let (item_id, global_id) = self.catalog().allocate_user_id(id_ts).await?;

        let secrets_controller = Arc::clone(&self.secrets_controller);
        let payload = self.extract_secret(session, &mut plan.secret.secret_as)?;
        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn(
            || "create secret ensure",
            async move {
                secrets_controller.ensure(item_id, &payload).await?;
                let stage = SecretStage::CreateFinish(CreateSecretFinish {
                    validity,
                    item_id,
                    global_id,
                    plan,
                });
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
            item_id,
            global_id,
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
            global_id,
        };

        let ops = vec![catalog::Op::CreateItem {
            id: item_id,
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
                if let Err(e) = self.secrets_controller.delete(item_id).await {
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
        &self,
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
    pub(crate) async fn sequence_rotate_keys(&mut self, ctx: ExecuteContext, id: CatalogItemId) {
        // If the secret is deleted from the catalog during
        // `rotate_keys_ensure()`, this will prevent `rotate_keys_finish()` from
        // issuing the catalog update for the change. The state of the persisted
        // secret is unknown, and if the rotate ensure'd after the delete (i.e.,
        // the secret is persisted to the secret store but not the catalog), the
        // secret will be cleaned up during next envd boot.
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
        &self,
        RotateKeysSecretEnsure { validity, id }: RotateKeysSecretEnsure,
    ) -> Result<StageResult<Box<SecretStage>>, AdapterError> {
        let secrets_controller = Arc::clone(&self.secrets_controller);
        let entry = self.catalog().get_entry(&id).clone();
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

                let mut to_item = entry.item;
                match &mut to_item {
                    CatalogItem::Connection(c) => {
                        let mut stmt = match mz_sql::parse::parse(&c.create_sql)
                            .expect("invalid create sql persisted to catalog")
                            .into_element()
                            .ast
                        {
                            Statement::CreateConnection(stmt) => stmt,
                            _ => coord_bail!("internal error: persisted SQL for {id} is invalid"),
                        };

                        stmt.values.retain(|v| {
                            v.name != ConnectionOptionName::PublicKey1
                                && v.name != ConnectionOptionName::PublicKey2
                        });
                        stmt.values.push(ConnectionOption {
                            name: ConnectionOptionName::PublicKey1,
                            value: Some(WithOptionValue::Value(Value::String(
                                new_key_set.primary().ssh_public_key(),
                            ))),
                        });
                        stmt.values.push(ConnectionOption {
                            name: ConnectionOptionName::PublicKey2,
                            value: Some(WithOptionValue::Value(Value::String(
                                new_key_set.secondary().ssh_public_key(),
                            ))),
                        });

                        c.create_sql = stmt.to_ast_string_stable();
                    }
                    _ => coord_bail!(
                        "internal error: rotate keys called on non-connection object {id}"
                    ),
                }

                let ops = vec![catalog::Op::UpdateItem {
                    id,
                    name: entry.name,
                    to_item,
                }];
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
