// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use mz_repr::CatalogItemId;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_sql::ast::{Raw, Statement};
use mz_sql::catalog::{
    DefaultPrivilegeAclItem, DefaultPrivilegeObject, ObjectType, RoleAttributesRaw,
};
use mz_sql::names::{ObjectId, ResolvedDatabaseSpecifier, SystemObjectId};
use mz_sql::plan::Params;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;

use super::CatalogCommand;
use super::drop::DropStatement;
use super::table::TableStatement;
use crate::AdapterError;
use crate::catalog::{Catalog, CommandExecutionError, Op, UpdatePrivilegeVariant};
use crate::session::Session;

fn session() -> Session {
    let mut session = Session::dummy();
    session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
    session
        .start_transaction(mz_ore::now::to_datetime(1000), None, None)
        .expect("start invoking transaction");
    session
}

fn statement(sql: &str) -> Arc<Statement<Raw>> {
    Arc::new(
        mz_sql_parser::parser::parse_statements(sql)
            .expect("parse test statement")
            .remove(0)
            .ast,
    )
}

fn drop_statement(session: &Session, name: &str) -> DropStatement {
    DropStatement::new(
        statement(&format!("DROP TABLE materialize.public.{name}")),
        Params::empty(),
        session.meta(),
    )
}

async fn table_command(
    catalog: &Catalog,
    session: &Session,
    name: &str,
) -> (CatalogCommand, CatalogItemId) {
    let ts = catalog.current_upper().await;
    let ids = catalog
        .allocate_user_ids(1, ts)
        .await
        .expect("allocate table IDs")
        .remove(0);
    let mut command = TableStatement::new(
        statement(&format!("CREATE TABLE materialize.public.{name} (a int)")),
        Params::empty(),
        session.meta(),
    );
    command.allocate_create(ids);
    (CatalogCommand::CreateTable(Arc::new(command)), ids.0)
}

async fn commit(catalog: &mut Catalog, commands: &mut [CatalogCommand]) {
    let ts = catalog.current_upper().await;
    catalog
        .transact_commands(None, None, ts, None, commands, &mut BTreeMap::new())
        .await
        .expect("commit test commands");
}

async fn transact_ops(catalog: &mut Catalog, ops: Vec<Op>) {
    let ts = catalog.current_upper().await;
    catalog
        .transact(None, ts, None, ops)
        .await
        .expect("commit fixture operations");
}

fn drop_sql(session: &Session, sql: &str) -> DropStatement {
    DropStatement::new(statement(sql), Params::empty(), session.meta())
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn drop_role_rechecks_dependencies_and_rolls_back_prefix() {
    for ownership in [true, false] {
        Catalog::with_debug(|mut catalog| async move {
            let session = session();
            transact_ops(
                &mut catalog,
                vec![Op::CreateRole {
                    name: "dependent_role".into(),
                    attributes: RoleAttributesRaw::new(),
                }],
            )
            .await;
            let role_id = catalog
                .try_get_role_by_name("dependent_role")
                .expect("fixture role exists")
                .id;
            let (create, dependency_id) =
                table_command(&catalog, &session, "role_dependency").await;
            commit(&mut catalog, &mut [create]).await;
            let mut drop = drop_sql(&session, "DROP ROLE dependent_role");
            drop.compile(catalog.state(), None)
                .expect("role has no dependencies when the command is captured");

            let privilege = MzAclItem {
                grantee: role_id,
                grantor: MZ_SYSTEM_ROLE_ID,
                acl_mode: AclMode::SELECT,
            };
            let dependency = if ownership {
                Op::UpdateOwner {
                    id: ObjectId::Item(dependency_id),
                    new_owner: role_id,
                }
            } else {
                Op::UpdatePrivilege {
                    target_id: SystemObjectId::Object(ObjectId::Item(dependency_id)),
                    privileges: vec![privilege],
                    variant: UpdatePrivilegeVariant::Grant,
                }
            };
            transact_ops(&mut catalog, vec![dependency]).await;
            let (prefix, prefix_id) = table_command(&catalog, &session, "aborted_prefix").await;
            let revision = catalog.transient_revision();
            let mut commands = [prefix, CatalogCommand::DropObjects(Arc::new(drop))];
            let ts = catalog.current_upper().await;
            let result = catalog
                .transact_commands(None, None, ts, None, &mut commands, &mut BTreeMap::new())
                .await;
            let Err(CommandExecutionError::Error(AdapterError::DependentObject(dependencies))) =
                result
            else {
                panic!("current role dependency must reject the entire command batch");
            };
            let descriptions = dependencies.get("dependent_role").expect("dependent role");
            let expected = if ownership {
                "owner of"
            } else {
                "privileges on"
            };
            assert!(descriptions.iter().any(|description| {
                description.contains(expected) && description.contains("role_dependency")
            }));
            assert_eq!(catalog.transient_revision(), revision);
            assert!(catalog.try_get_entry(&prefix_id).is_none());
            assert!(
                drop_statement(&session, "aborted_prefix")
                    .plan(catalog.state())
                    .is_err()
            );
            assert_eq!(
                catalog
                    .try_get_role_by_name("dependent_role")
                    .expect("fixture role exists")
                    .id,
                role_id
            );
            let entry = catalog.get_entry(&dependency_id);
            if ownership {
                assert_eq!(*entry.owner_id(), role_id);
            } else {
                assert!(entry.privileges().all_values().any(|acl| acl == &privilege));
            }
            catalog.expire().await;
        })
        .await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn drop_schema_and_database_revoke_only_scoped_default_privileges() {
    Catalog::with_debug(|mut catalog| async move {
        let session = session();
        transact_ops(
            &mut catalog,
            vec![Op::CreateDatabase {
                name: "privilege_database".into(),
                owner_id: MZ_SYSTEM_ROLE_ID,
            }],
        )
        .await;
        let database_id = catalog
            .resolve_database("privilege_database")
            .expect("fixture database exists")
            .id;
        transact_ops(
            &mut catalog,
            vec![Op::CreateSchema {
                database_id: ResolvedDatabaseSpecifier::Id(database_id),
                schema_name: "privilege_schema".into(),
                owner_id: MZ_SYSTEM_ROLE_ID,
            }],
        )
        .await;
        let database = catalog
            .resolve_database("privilege_database")
            .expect("fixture database exists");
        let schema_id = database.schemas_by_name["privilege_schema"];
        let sibling_id = database.schemas_by_name["public"];
        let objects = [
            DefaultPrivilegeObject::new(
                MZ_SYSTEM_ROLE_ID,
                Some(database_id),
                Some(schema_id),
                ObjectType::Table,
            ),
            DefaultPrivilegeObject::new(
                MZ_SYSTEM_ROLE_ID,
                Some(database_id),
                Some(sibling_id),
                ObjectType::Table,
            ),
            DefaultPrivilegeObject::new(
                MZ_SYSTEM_ROLE_ID,
                Some(database_id),
                None,
                ObjectType::Table,
            ),
            DefaultPrivilegeObject::new(MZ_SYSTEM_ROLE_ID, None, None, ObjectType::Table),
        ];
        let acl = DefaultPrivilegeAclItem::new(mz_repr::role_id::RoleId::Public, AclMode::SELECT);
        let mut schema_drop = drop_sql(&session, "DROP SCHEMA privilege_database.privilege_schema");
        let mut database_drop = drop_sql(&session, "DROP DATABASE privilege_database CASCADE");
        schema_drop
            .compile(catalog.state(), None)
            .expect("capture schema drop");
        database_drop
            .compile(catalog.state(), None)
            .expect("capture database drop");
        transact_ops(
            &mut catalog,
            objects
                .iter()
                .map(|object| Op::UpdateDefaultPrivilege {
                    privilege_object: object.clone(),
                    privilege_acl_item: acl.clone(),
                    variant: UpdatePrivilegeVariant::Grant,
                })
                .collect(),
        )
        .await;
        let has_privilege = |catalog: &Catalog, object: &DefaultPrivilegeObject| {
            catalog
                .default_privileges()
                .any(|(key, mut privileges)| key == object && privileges.any(|value| value == &acl))
        };
        assert!(objects.iter().all(|object| has_privilege(&catalog, object)));
        commit(
            &mut catalog,
            &mut [CatalogCommand::DropObjects(Arc::new(schema_drop))],
        )
        .await;
        let database = catalog
            .resolve_database("privilege_database")
            .expect("fixture database exists");
        assert!(!database.schemas_by_name.contains_key("privilege_schema"));
        assert!(database.schemas_by_name.contains_key("public"));
        assert!(!has_privilege(&catalog, &objects[0]));
        assert!(
            objects[1..]
                .iter()
                .all(|object| has_privilege(&catalog, object))
        );

        commit(
            &mut catalog,
            &mut [CatalogCommand::DropObjects(Arc::new(database_drop))],
        )
        .await;
        assert!(catalog.resolve_database("privilege_database").is_err());
        assert!(
            objects[..3]
                .iter()
                .all(|object| !has_privilege(&catalog, object))
        );
        assert!(has_privilege(&catalog, &objects[3]));
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn drop_role_cleans_current_memberships_in_both_directions() {
    Catalog::with_debug(|mut catalog| async move {
        let session = session();
        transact_ops(
            &mut catalog,
            ["parent_role", "middle_role", "child_role"]
                .into_iter()
                .map(|name| Op::CreateRole {
                    name: name.into(),
                    attributes: RoleAttributesRaw::new(),
                })
                .collect(),
        )
        .await;
        let parent = catalog
            .try_get_role_by_name("parent_role")
            .expect("fixture role exists")
            .id;
        let middle = catalog
            .try_get_role_by_name("middle_role")
            .expect("fixture role exists")
            .id;
        let child = catalog
            .try_get_role_by_name("child_role")
            .expect("fixture role exists")
            .id;
        let mut drop = drop_sql(&session, "DROP ROLE middle_role");
        drop.compile(catalog.state(), None)
            .expect("capture role drop");
        transact_ops(
            &mut catalog,
            [(parent, middle), (middle, child), (parent, child)]
                .into_iter()
                .map(|(role_id, member_id)| Op::GrantRole {
                    role_id,
                    member_id,
                    grantor_id: MZ_SYSTEM_ROLE_ID,
                })
                .collect(),
        )
        .await;
        assert_eq!(
            catalog.get_role(&middle).membership.map.get(&parent),
            Some(&MZ_SYSTEM_ROLE_ID)
        );
        assert_eq!(
            catalog.get_role(&child).membership.map.get(&middle),
            Some(&MZ_SYSTEM_ROLE_ID)
        );
        commit(
            &mut catalog,
            &mut [CatalogCommand::DropObjects(Arc::new(drop))],
        )
        .await;
        assert!(catalog.try_get_role(&middle).is_none());
        assert!(catalog.try_get_role_by_name("middle_role").is_none());
        assert!(catalog.try_get_role(&parent).is_some());
        assert_eq!(
            catalog.get_role(&child).membership.map,
            BTreeMap::from([(parent, MZ_SYSTEM_ROLE_ID)])
        );
        assert!(
            catalog
                .user_roles()
                .all(|role| !role.membership.map.contains_key(&middle))
        );
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn drop_sees_table_created_in_command_prefix() {
    Catalog::with_debug(|mut catalog| async move {
        let session = session();
        let (create, id) = table_command(&catalog, &session, "prefix_table").await;
        let mut drop = drop_statement(&session, "prefix_table");
        assert!(
            drop.plan(catalog.state()).is_err(),
            "the table must not exist in the initial catalog"
        );

        commit(
            &mut catalog,
            &mut [create, CatalogCommand::DropObjects(Arc::new(drop))],
        )
        .await;

        assert!(catalog.try_get_entry(&id).is_none());
        assert!(
            drop_statement(&session, "prefix_table")
                .plan(catalog.state())
                .is_err()
        );
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn failing_drop_rolls_back_prior_creates_and_drops() {
    Catalog::with_debug(|mut catalog| async move {
        let session = session();
        let (create, original_id) = table_command(&catalog, &session, "original_table").await;
        commit(&mut catalog, &mut [create]).await;
        let (create, new_id) = table_command(&catalog, &session, "new_table").await;
        let revision = catalog.transient_revision();
        let mut commands = [
            create,
            CatalogCommand::DropObjects(Arc::new(drop_statement(&session, "original_table"))),
            CatalogCommand::DropObjects(Arc::new(drop_statement(&session, "missing_table"))),
        ];
        let ts = catalog.current_upper().await;
        let result = catalog
            .transact_commands(None, None, ts, None, &mut commands, &mut BTreeMap::new())
            .await;
        let Err(CommandExecutionError::Error(error)) = result else {
            panic!("dropping a missing table must fail without requesting preparation");
        };
        assert!(
            error.to_string().contains("missing_table"),
            "the final DROP must cause the failure, got: {error}"
        );
        assert_eq!(catalog.transient_revision(), revision);
        assert!(catalog.try_get_entry(&new_id).is_none());
        assert!(catalog.try_get_entry(&original_id).is_some());
        let plan = drop_statement(&session, "original_table")
            .plan(catalog.state())
            .expect("rolled-back drop preserves original binding");
        assert_eq!(plan.drop_ids, vec![ObjectId::Item(original_id)]);
        assert!(
            drop_statement(&session, "new_table")
                .plan(catalog.state())
                .is_err()
        );
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn drop_replans_reused_name_and_preserves_renamed_original() {
    Catalog::with_debug(|mut catalog| async move {
        let session = session();
        let (create, original_id) = table_command(&catalog, &session, "reused_table").await;
        commit(&mut catalog, &mut [create]).await;
        let mut drop = drop_statement(&session, "reused_table");
        let plan = drop.plan(catalog.state()).expect("plan original drop");
        assert_eq!(plan.drop_ids, vec![ObjectId::Item(original_id)]);

        let current_full_name =
            catalog.resolve_full_name(catalog.get_entry(&original_id).name(), None);
        let ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                ts,
                None,
                vec![Op::RenameItem {
                    id: original_id,
                    current_full_name,
                    to_name: "renamed_table".into(),
                }],
            )
            .await
            .expect("rename original table");
        let (replacement, replacement_id) = table_command(&catalog, &session, "reused_table").await;
        commit(&mut catalog, &mut [replacement]).await;
        assert_ne!(original_id, replacement_id);

        commit(
            &mut catalog,
            &mut [CatalogCommand::DropObjects(Arc::new(drop))],
        )
        .await;

        assert!(catalog.try_get_entry(&replacement_id).is_none());
        assert_eq!(catalog.get_entry(&original_id).name().item, "renamed_table");
        let plan = drop_statement(&session, "renamed_table")
            .plan(catalog.state())
            .expect("renamed original remains addressable");
        assert_eq!(plan.drop_ids, vec![ObjectId::Item(original_id)]);
        assert!(
            drop_statement(&session, "reused_table")
                .plan(catalog.state())
                .is_err()
        );
        catalog.expire().await;
    })
    .await;
}
