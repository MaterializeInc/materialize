// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use mz_catalog::memory::objects::{CatalogItem, Connection};
use mz_ore::collections::CollectionExt;
use mz_ore::future::OreFutureExt;
use mz_repr::CatalogItemId;
use mz_secrets::SecretsReader;
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{ConnectionOption, ConnectionOptionName, Statement, Value, WithOptionValue};
use mz_sql::names::Aug;
use mz_sql::plan::{ConnectionDetails, Params, Plan, PlanError};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_types::connections::AwsPrivatelinkConnection;
use mz_storage_types::connections::inline::IntoInlineConnection;

use crate::AdapterError;
use crate::catalog::{CatalogState, ConnCatalog, Op};

#[derive(Clone, Debug)]
pub struct ConnectionStatement {
    statement: std::sync::Arc<Statement<mz_sql::ast::Raw>>,
    params: Params,
    session: std::sync::Arc<crate::session::SessionMeta>,
    prepared: Option<ConnectionPreparation>,
    notices: Vec<mz_sql::plan::PlanNotice>,
}

#[derive(Debug)]
pub(crate) struct AlterConnectionCompilation {
    pub plan: Plan,
    pub connection: Option<(CatalogItemId, Connection, bool)>,
    pub notices: Vec<mz_sql::plan::PlanNotice>,
}

impl ConnectionStatement {
    pub(crate) fn new(
        statement: std::sync::Arc<Statement<mz_sql::ast::Raw>>,
        params: Params,
        session: crate::session::SessionMeta,
    ) -> Self {
        Self {
            statement,
            params,
            session: std::sync::Arc::new(session),
            prepared: None,
            notices: Vec::new(),
        }
    }

    pub(crate) fn notices(&self) -> &[mz_sql::plan::PlanNotice] {
        &self.notices
    }

    pub(in crate::catalog) fn applied(&mut self, notices: Vec<mz_sql::plan::PlanNotice>) {
        self.notices = notices;
    }

    pub(crate) fn resolve_alter(
        &mut self,
        state: &CatalogState,
        response: Option<ConnectionPreparationResponse>,
    ) -> Result<ConnectionResolution, AdapterError> {
        let compiled = self.compile_alter(state)?;
        let Some((id, connection, validate)) = &compiled.connection else {
            return Ok(ConnectionResolution::Ready(compiled));
        };
        let request = ConnectionPreparation {
            id: *id,
            definition: connection.create_sql.clone(),
            connection: connection
                .details
                .to_connection()
                .into_inline_connection(state),
            guards: connection.details.secret_content_guards(),
            validate: *validate,
        };
        if let Some(response) = response {
            if request.same_inputs(&response.request) {
                response.result?;
                self.prepared = Some(response.request);
            }
        }
        if !request.validate && request.guards.is_empty()
            || self
                .prepared
                .as_ref()
                .is_some_and(|prepared| request.same_inputs(prepared))
        {
            Ok(ConnectionResolution::Ready(compiled))
        } else {
            Ok(ConnectionResolution::Prepare(request))
        }
    }

    pub(crate) fn compile_alter(
        &self,
        state: &CatalogState,
    ) -> Result<AlterConnectionCompilation, AdapterError> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let catalog = state.for_session_metadata(self.session.as_ref(), tx);
        let (statement, resolved_ids) =
            mz_sql::names::resolve(&catalog, (*self.statement).clone())?;
        let (plan, sql_impl_ids) = mz_sql::plan::plan(
            Some(self.session.pcx()),
            &catalog,
            statement,
            &self.params,
            &resolved_ids,
        )?;
        mz_sql::rbac::check_plan(
            &catalog,
            None,
            self.session.as_ref(),
            &plan,
            None,
            &resolved_ids,
            &sql_impl_ids,
        )?;
        let connection = match &plan {
            Plan::AlterConnection(mz_sql::plan::AlterConnectionPlan {
                id,
                action:
                    mz_sql::plan::AlterConnectionAction::AlterOptions {
                        set_options,
                        drop_options,
                        validate,
                    },
            }) => {
                let rebuilt = rebuild_connection(
                    &catalog,
                    self.session.as_ref(),
                    *id,
                    set_options.clone(),
                    drop_options.clone(),
                )?;
                Some((*id, rebuilt, *validate))
            }
            Plan::AlterNoop(_) => None,
            _ => return Err(AdapterError::Unsupported("non-option connection command")),
        };
        let mut notices = Vec::new();
        while let Ok(notice) = rx.try_recv() {
            match notice {
                crate::AdapterNotice::PlanNotice(notice) => notices.push(notice),
                _ => unreachable!("planning only emits plan notices"),
            }
        }
        Ok(AlterConnectionCompilation {
            plan,
            connection,
            notices,
        })
    }
}

#[derive(Debug)]
pub(crate) enum ConnectionResolution {
    Prepare(ConnectionPreparation),
    Ready(AlterConnectionCompilation),
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionPreparation {
    id: CatalogItemId,
    definition: String,
    connection: mz_storage_types::connections::Connection,
    guards: Vec<(CatalogItemId, fn(&str) -> Result<(), anyhow::Error>)>,
    validate: bool,
}

#[derive(Debug)]
#[must_use = "consume completions through ConnectionStatement::resolve_alter"]
pub(crate) struct ConnectionPreparationResponse {
    request: ConnectionPreparation,
    result: Result<(), AdapterError>,
}

impl ConnectionPreparation {
    fn same_inputs(&self, other: &Self) -> bool {
        self.id == other.id
            && self.definition == other.definition
            && self.connection == other.connection
            && self.validate == other.validate
            && self
                .guards
                .iter()
                .map(|(id, _)| id)
                .eq(other.guards.iter().map(|(id, _)| id))
    }

    pub(crate) async fn execute(
        self,
        config: &mz_storage_types::configuration::StorageConfiguration,
        secrets: &mz_secrets::cache::CachingSecretsReader,
    ) -> ConnectionPreparationResponse {
        let result = async {
            for (id, guard) in &self.guards {
                let contents = secrets.read_string(*id).await?;
                guard(&contents)?;
            }
            if self.validate {
                match std::panic::AssertUnwindSafe(self.connection.validate(self.id, config))
                    .ore_catch_unwind()
                    .await
                {
                    Ok(result) => result.map_err(AdapterError::from)?,
                    Err(_) => {
                        return Err(AdapterError::Internal(
                            "alter connection validation panicked".into(),
                        ));
                    }
                }
            }
            Ok(())
        }
        .await;
        ConnectionPreparationResponse {
            request: self,
            result,
        }
    }
}

/// Reconstructs a connection and authorizes its final dependencies for the invoking session.
pub(crate) fn rebuild_connection(
    catalog: &ConnCatalog<'_>,
    session: &dyn SessionMetadata,
    id: CatalogItemId,
    set_options: BTreeMap<ConnectionOptionName, Option<WithOptionValue<Aug>>>,
    drop_options: BTreeSet<ConnectionOptionName>,
) -> Result<Connection, AdapterError> {
    let cur_entry = catalog.state().get_entry(&id);
    let cur_conn = cur_entry.connection().expect("known to be connection");
    let create_conn_stmt = match mz_sql::parse::parse(&cur_conn.create_sql)
        .expect("invalid create sql persisted to catalog")
        .into_element()
        .ast
    {
        Statement::CreateConnection(stmt) => stmt,
        _ => unreachable!("proved type is connection"),
    };

    let system_catalog = catalog.for_system_session();

    let (mut create_conn_stmt, resolved_ids) =
        mz_sql::names::resolve(&system_catalog, create_conn_stmt)
            .map_err(|e| AdapterError::internal("ALTER CONNECTION", e))?;

    create_conn_stmt
        .values
        .retain(|o| !set_options.contains_key(&o.name) && !drop_options.contains(&o.name));

    create_conn_stmt.values.extend(
        set_options
            .into_iter()
            .map(|(name, value)| ConnectionOption { name, value }),
    );

    let mut system_catalog = catalog.for_system_session();
    system_catalog.mark_id_unresolvable_for_replanning(id);

    let plan = match mz_sql::plan::plan(
        None,
        &system_catalog,
        Statement::CreateConnection(create_conn_stmt),
        &Params::empty(),
        &resolved_ids,
    )
    .map_err(|e| AdapterError::InvalidAlter("CONNECTION", e))?
    {
        (Plan::CreateConnection(plan), _sql_impl_ids) => plan,
        (p, _) => {
            unreachable!("create connection plan is only valid response, got {:?}", p)
        }
    };

    let create_conn_stmt = match mz_sql::parse::parse(&plan.connection.create_sql)
        .expect("invalid create sql persisted to catalog")
        .into_element()
        .ast
    {
        Statement::CreateConnection(stmt) => stmt,
        _ => unreachable!("proved type is connection"),
    };

    let system_catalog = catalog.for_system_session();

    let (_, new_deps) = mz_sql::names::resolve(&system_catalog, create_conn_stmt)
        .map_err(|e| AdapterError::internal("ALTER CONNECTION", e))?;

    mz_sql::rbac::check_usage(
        catalog,
        session,
        &new_deps,
        &mz_sql::rbac::CREATE_ITEM_USAGE,
    )?;
    check_connection_details(&plan.connection.details)
        .map_err(|error| AdapterError::InvalidAlter("CONNECTION", error))?;
    Ok(Connection {
        create_sql: plan.connection.create_sql,
        global_id: cur_conn.global_id,
        details: plan.connection.details,
        resolved_ids: new_deps,
    })
}

/// Rejects connection options whose values cannot work, independent of any
/// external system.
pub(crate) fn check_connection_details(details: &ConnectionDetails) -> Result<(), PlanError> {
    match details {
        ConnectionDetails::AwsPrivatelink(privatelink) => {
            AwsPrivatelinkConnection::check_service_name(&privatelink.service_name)
                .map_err(PlanError::InvalidPrivatelinkServiceName)
        }
        _ => Ok(()),
    }
}

/// Publication of a completed secret-store rotation, pinned to its original target definition.
#[derive(Clone, Debug)]
pub struct RotatedKeys {
    id: CatalogItemId,
    expected_create_sql: String,
    public_keys: [String; 2],
}

impl RotatedKeys {
    pub(crate) fn new(
        id: CatalogItemId,
        expected_create_sql: String,
        public_keys: [String; 2],
    ) -> Self {
        Self {
            id,
            expected_create_sql,
            public_keys,
        }
    }

    pub(in crate::catalog) fn execute(
        &self,
        state: &CatalogState,
    ) -> Result<Vec<Op>, AdapterError> {
        let unchanged = state.try_get_entry(&self.id).and_then(|entry| {
            entry
                .connection()
                .ok()
                .filter(|connection| connection.create_sql == self.expected_create_sql)
                .map(|connection| (entry, connection))
        });
        let Some((entry, connection)) = unchanged else {
            return Err(AdapterError::ConcurrentDependencyMutation {
                dependency_id: self.id.to_string(),
            });
        };
        let mut replacement = connection.clone();
        let mut statement = match mz_sql::parse::parse(&replacement.create_sql)
            .expect("invalid create sql persisted to catalog")
            .into_element()
            .ast
        {
            Statement::CreateConnection(statement) => statement,
            _ => unreachable!("connection definition"),
        };
        statement.values.retain(|option| {
            !matches!(
                option.name,
                ConnectionOptionName::PublicKey1 | ConnectionOptionName::PublicKey2,
            )
        });
        for (name, key) in [
            (ConnectionOptionName::PublicKey1, &self.public_keys[0]),
            (ConnectionOptionName::PublicKey2, &self.public_keys[1]),
        ] {
            statement.values.push(ConnectionOption {
                name,
                value: Some(WithOptionValue::Value(Value::String(key.clone()))),
            });
        }
        replacement.create_sql = statement.to_ast_string_stable();
        Ok(vec![Op::UpdateItem {
            id: self.id,
            name: entry.name().clone(),
            to_item: CatalogItem::Connection(replacement),
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;

    async fn install_ssh(catalog: &mut Catalog) -> CatalogItemId {
        let raw = mz_sql::parse::parse(
                "CREATE CONNECTION materialize.public.rotation_target TO SSH TUNNEL (HOST 'localhost', USER 'mz')",
            ).expect("parse connection").into_element().ast;
        let conn_catalog = catalog.for_system_session();
        let (statement, resolved_ids) =
            mz_sql::names::resolve(&conn_catalog, raw).expect("resolve");
        let (plan, _) = mz_sql::plan::plan(
            None,
            &conn_catalog,
            statement,
            &Params::empty(),
            &resolved_ids,
        )
        .expect("plan connection");
        let Plan::CreateConnection(plan) = plan else {
            unreachable!()
        };
        let ts = catalog.current_upper().await;
        let (id, global_id) = catalog
            .allocate_user_ids(1, ts)
            .await
            .expect("allocate")
            .into_element();
        let connection = Connection {
            create_sql: plan.connection.create_sql,
            global_id,
            details: plan.connection.details,
            resolved_ids,
        };
        let ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                ts,
                None,
                vec![Op::CreateItem {
                    id,
                    name: plan.name.clone(),
                    item: CatalogItem::Connection(connection),
                    owner_id: mz_sql::session::user::MZ_SYSTEM_ROLE_ID,
                }],
            )
            .await
            .expect("install connection");
        id
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn rotation_checks_the_candidate_definition_and_rolls_back() {
        Catalog::with_debug(|mut catalog| async move {
            let id = install_ssh(&mut catalog).await;
            let entry = catalog.get_entry(&id);
            let name = entry.name().clone();
            let connection = entry.connection().expect("connection");
            let ConnectionDetails::Ssh { key_1, key_2, .. } = &connection.details else {
                unreachable!()
            };
            let keys = [key_2.public_key(), key_1.public_key()];
            let publication = std::sync::Arc::new(RotatedKeys::new(
                id,
                connection.create_sql.clone(),
                keys.clone(),
            ));
            let (_, before) = catalog.ddl_transaction_snapshot().await;

            let mut session = crate::session::Session::dummy();
            session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
            let changed = rebuild_connection(
                &catalog.for_system_session(),
                &session,
                id,
                BTreeMap::from([(
                    ConnectionOptionName::Host,
                    Some(WithOptionValue::Value(Value::String(
                        "changed.example".into(),
                    ))),
                )]),
                BTreeSet::new(),
            )
            .expect("rebuild changed connection");
            let ts = catalog.current_upper().await;
            let error = catalog
                .transact(
                    None,
                    ts,
                    None,
                    vec![
                        Op::UpdateItem {
                            id,
                            name,
                            to_item: CatalogItem::Connection(changed),
                        },
                        Op::RotatedKeys(std::sync::Arc::clone(&publication)),
                    ],
                )
                .await;
            assert!(matches!(error,
                Err(AdapterError::ConcurrentDependencyMutation { dependency_id })
                if dependency_id == id.to_string()
            ));
            assert_eq!(catalog.ddl_transaction_snapshot().await.1, before);

            let ts = catalog.current_upper().await;
            catalog
                .transact(None, ts, None, vec![Op::RotatedKeys(publication)])
                .await
                .expect("publish against unchanged target");
            let connection = catalog.get_entry(&id).connection().expect("connection");
            let parsed = mz_sql::parse::parse(&connection.create_sql)
                .expect("parse published connection")
                .into_element()
                .ast;
            let Statement::CreateConnection(statement) = parsed else {
                unreachable!()
            };
            let published: BTreeMap<_, _> = statement
                .values
                .into_iter()
                .filter(|option| {
                    matches!(
                        option.name,
                        ConnectionOptionName::PublicKey1 | ConnectionOptionName::PublicKey2
                    )
                })
                .map(|option| (option.name, option.value))
                .collect();
            assert_eq!(
                published,
                BTreeMap::from([
                    (
                        ConnectionOptionName::PublicKey1,
                        Some(WithOptionValue::Value(Value::String(keys[0].clone()),))
                    ),
                    (
                        ConnectionOptionName::PublicKey2,
                        Some(WithOptionValue::Value(Value::String(keys[1].clone()),))
                    ),
                ])
            );
            assert_ne!(catalog.ddl_transaction_snapshot().await.1, before);
            catalog.expire().await;
        })
        .await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn connection_validation_rechecks_retained_inputs() {
        use mz_sql::session::vars::VarInput;
        Catalog::with_debug(|mut catalog| async move {
            let id = install_ssh(&mut catalog).await;
            catalog.state.system_config_mut()
                .set("enable_connection_validation_syntax", VarInput::Flat("true"))
                .expect("allow validation syntax");
            let mut session = crate::session::Session::dummy();
            session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
            session.start_transaction(mz_ore::now::to_datetime(1000), None, None)
                .expect("start invoking transaction");
            let statement = mz_sql::parse::parse(
                "ALTER CONNECTION materialize.public.rotation_target SET (PORT 2222) WITH (VALIDATE = true)",
            ).expect("parse alter").into_element().ast;
            let mut command = ConnectionStatement::new(
                std::sync::Arc::new(statement), Params::empty(), session.meta(),
            );
            let ConnectionResolution::Prepare(old_request) =
                command.resolve_alter(catalog.state(), None).expect("request validation")
            else { panic!("validation required") };
            let changed = rebuild_connection(
                &catalog.for_system_session(), &session, id,
                BTreeMap::from([(ConnectionOptionName::Host, Some(
                    WithOptionValue::Value(Value::String("changed.example".into())),
                ))]), BTreeSet::new(),
            ).expect("change retained host");
            let name = catalog.get_entry(&id).name().clone();
            let ts = catalog.current_upper().await;
            catalog.transact(None, ts, None, vec![Op::UpdateItem {
                id, name, to_item: CatalogItem::Connection(changed),
            }]).await.expect("commit retained input change");
            let failure = ConnectionPreparationResponse {
                request: old_request.clone(),
                result: Err(AdapterError::Internal("obsolete validation failure".into())),
            };
            let ConnectionResolution::Prepare(current_request) =
                command.resolve_alter(catalog.state(), Some(failure))
                    .expect("ignore obsolete failure")
            else { panic!("changed retained host requires validation") };
            assert!(!old_request.same_inputs(&current_request));
            let success = ConnectionPreparationResponse { request: old_request, result: Ok(()) };
            assert!(matches!(
                command.resolve_alter(catalog.state(), Some(success)).expect("reject stale success"),
                ConnectionResolution::Prepare(_)
            ));
            let failure = ConnectionPreparationResponse {
                request: current_request,
                result: Err(AdapterError::Internal("current validation failure".into())),
            };
            assert!(matches!(
                command.resolve_alter(catalog.state(), Some(failure)),
                Err(AdapterError::Internal(message)) if message == "current validation failure"
            ));
            catalog.expire().await;
        }).await;
    }
}
