// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_adapter_types::connection::ConnectionId;
use mz_auth::AuthenticatorKind;
use mz_ore::metrics::MetricsRegistry;
use mz_sql::plan::{PlanError, QueryPlanFeature, QueryPolicyMode, QueryPolicyRule};
use mz_sql::session::vars::{OwnedVarInput, VarInput};
use uuid::Uuid;

use super::*;
use crate::catalog::command::CatalogCommand;
use crate::catalog::{Catalog, CommandExecutionError};
use crate::metrics::Metrics;
use crate::session::{Session, SessionConfig};

fn intent(sql: &str, session: &Session) -> MetadataStatement {
    let statement = mz_sql_parser::parser::parse_statements(sql)
        .expect("parse policy fixture")
        .remove(0)
        .ast;
    MetadataStatement::new(Arc::new(statement), Params::empty(), session.meta())
}

fn command(sql: &str, session: &Session) -> CatalogCommand {
    CatalogCommand::Metadata(Arc::new(intent(sql, session)))
}

async fn commit(catalog: &mut Catalog, commands: &mut [CatalogCommand]) {
    let ts = catalog.current_upper().await;
    catalog
        .transact_commands(None, None, ts, None, commands, &mut BTreeMap::new())
        .await
        .expect("commit policy commands");
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn partial_query_policy_alter_preserves_candidate_rules() {
    Catalog::with_debug(|mut catalog| async move {
        for flag in ["enable_query_policies", "enable_query_policy_enforcement"] {
            catalog
                .system_config_mut()
                .set(flag, VarInput::Flat("true"))
                .expect("enable query policies");
        }
        let mut session = Session::dummy();
        session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
        session
            .start_transaction(mz_ore::now::to_datetime(1000), None, None)
            .expect("start transaction");
        commit(
            &mut catalog,
            &mut [command(
                "CREATE QUERY POLICY qp (MODE = 'warn', RULES ())",
                &session,
            )],
        )
        .await;

        let mut alter = intent("ALTER QUERY POLICY qp SET (MODE = 'enforce')", &session);
        alter.compile(catalog.state()).expect("initial compile");
        commit(
            &mut catalog,
            &mut [command(
                "ALTER QUERY POLICY qp SET (RULES (concurrent_rule \
                 (action = 'reject', metric = 'query_plan_includes', value = 'storage_read')))",
                &session,
            )],
        )
        .await;
        let concurrent = catalog
            .state()
            .try_get_query_policy_by_name("qp")
            .expect("query policy")
            .clone();
        assert_eq!(concurrent.mode, QueryPolicyMode::Warn);
        assert_eq!(
            concurrent.rules,
            vec![QueryPolicyRule {
                name: "concurrent_rule".into(),
                value: QueryPlanFeature::StorageRead,
            }]
        );

        commit(
            &mut catalog,
            &mut [CatalogCommand::Metadata(Arc::new(alter))],
        )
        .await;
        let policy = catalog.get_query_policy(concurrent.id);
        assert_eq!(policy.mode, QueryPolicyMode::Enforce);
        assert_eq!(policy.rules, concurrent.rules);
        assert_eq!(policy.owner_id, concurrent.owner_id);
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn network_policy_lockout_uses_candidate_default() {
    Catalog::with_debug(|mut catalog| async move {
        catalog
            .system_config_mut()
            .set("enable_network_policies", VarInput::Flat("true"))
            .expect("enable network policies");
        catalog
            .system_config_mut()
            .set("enable_rbac_checks", VarInput::Flat("false"))
            .expect("isolate lockout validation from privileges");
        let ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                ts,
                None,
                vec![Op::CreateRole {
                    name: "policy_user".into(),
                    attributes: RoleAttributesRaw::new(),
                }],
            )
            .await
            .expect("create session role");
        let role_id = catalog
            .try_get_role_by_name("policy_user")
            .expect("fixture role exists")
            .id;
        let metrics = Metrics::register_into(&MetricsRegistry::new());
        let mut session = Session::new(
            &mz_build_info::DUMMY_BUILD_INFO,
            SessionConfig {
                conn_id: ConnectionId::Static(1),
                uuid: Uuid::new_v4(),
                user: "policy_user".into(),
                client_ip: Some("192.0.2.1".parse().expect("valid test address")),
                external_metadata_rx: None,
                helm_chart_version: None,
                authenticator_kind: AuthenticatorKind::None,
                groups: None,
            },
            metrics.session_metrics(),
        );
        session.initialize_role_metadata(role_id);
        session
            .start_transaction(mz_ore::now::to_datetime(1000), None, None)
            .expect("start transaction");
        assert!(!session.user().is_internal());
        commit(
            &mut catalog,
            &mut [command(
                "CREATE NETWORK POLICY np (RULES (allow_client \
                 (address = '192.0.2.1/32', action = 'allow', direction = 'ingress')))",
                &session,
            )],
        )
        .await;
        let original = catalog
            .get_network_policy_by_name("np")
            .expect("fixture policy exists")
            .clone();
        let initial_default = catalog.system_config().default_network_policy_name();
        assert_ne!(initial_default, "np");
        let mut alter = intent(
            "ALTER NETWORK POLICY np SET (RULES (other_client \
             (address = '192.0.2.2/32', action = 'allow', direction = 'ingress')))",
            &session,
        );
        alter
            .compile(catalog.state())
            .expect("inactive policy may exclude the client");

        let ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                ts,
                None,
                vec![Op::UpdateSystemConfiguration {
                    name: "network_policy".into(),
                    value: OwnedVarInput::Flat("np".into()),
                }],
            )
            .await
            .expect("activate policy after initial compile");
        assert_eq!(catalog.system_config().default_network_policy_name(), "np");
        let revision = catalog.transient_revision();
        let mut commands = [CatalogCommand::Metadata(Arc::new(alter))];
        let ts = catalog.current_upper().await;
        let result = catalog
            .transact_commands(None, None, ts, None, &mut commands, &mut BTreeMap::new())
            .await;
        assert!(matches!(
            result,
            Err(CommandExecutionError::Error(AdapterError::PlanError(
                PlanError::NetworkPolicyLockoutError
            )))
        ));
        assert_eq!(catalog.get_network_policy_by_name("np"), Some(&original));
        assert_eq!(catalog.transient_revision(), revision);
        assert_eq!(catalog.system_config().default_network_policy_name(), "np");
        catalog.expire().await;
    })
    .await;
}
