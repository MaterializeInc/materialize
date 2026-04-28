// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v81 as v81;
use crate::durable::upgrade::objects_v82 as v82;

/// Verify no continual task items or comments remain in the catalog.
///
/// Continual tasks have been removed from the product. They should not be
/// present in any production environment. If they are found, we panic to
/// surface the issue — users can drop them manually before upgrading.
/// Builtin continual tasks are handled separately by the builtin item migration.
pub fn upgrade(
    snapshot: Vec<v81::StateUpdateKind>,
) -> Vec<MigrationAction<v81::StateUpdateKind, v82::StateUpdateKind>> {
    for update in &snapshot {
        match update {
            v81::StateUpdateKind::Item(item) => {
                let is_ct = match &item.value.definition {
                    v81::CatalogItem::V1(v1) => v1.create_sql.starts_with("CREATE CONTINUAL TASK"),
                };
                if is_ct {
                    panic!(
                        "found continual task {:?} in catalog; \
                         drop all continual tasks before upgrading",
                        item.value.name
                    );
                }
            }
            v81::StateUpdateKind::Comment(comment) => {
                if matches!(comment.key.object, v81::CommentObject::ContinualTask(_)) {
                    panic!(
                        "found comment on continual task in catalog; \
                         drop all continual tasks before upgrading"
                    );
                }
            }
            _ => {}
        }
    }

    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::upgrade;
    use crate::durable::upgrade::objects_v81 as v81;

    fn make_item(gid: u64, create_sql: &str) -> v81::StateUpdateKind {
        v81::StateUpdateKind::Item(v81::Item {
            key: v81::ItemKey {
                gid: v81::CatalogItemId::User(gid),
            },
            value: v81::ItemValue {
                schema_id: v81::SchemaId::User(1),
                name: "test".to_string(),
                definition: v81::CatalogItem::V1(v81::CatalogItemV1 {
                    create_sql: create_sql.to_string(),
                }),
                owner_id: v81::RoleId::User(1),
                privileges: vec![],
                oid: 20000,
                global_id: v81::GlobalId::User(gid),
                extra_versions: vec![],
            },
        })
    }

    fn make_comment_on_ct(item_id: u64) -> v81::StateUpdateKind {
        v81::StateUpdateKind::Comment(v81::Comment {
            key: v81::CommentKey {
                object: v81::CommentObject::ContinualTask(v81::CatalogItemId::User(item_id)),
                sub_component: None,
            },
            value: v81::CommentValue {
                comment: "test comment".to_string(),
            },
        })
    }

    fn make_comment_on_table(item_id: u64) -> v81::StateUpdateKind {
        v81::StateUpdateKind::Comment(v81::Comment {
            key: v81::CommentKey {
                object: v81::CommentObject::Table(v81::CatalogItemId::User(item_id)),
                sub_component: None,
            },
            value: v81::CommentValue {
                comment: "table comment".to_string(),
            },
        })
    }

    #[mz_ore::test]
    #[should_panic(expected = "found continual task")]
    fn test_panics_on_continual_task_item() {
        let snapshot = vec![
            make_item(1, "CREATE TABLE foo (a INT)"),
            make_item(2, "CREATE CONTINUAL TASK ct1 ..."),
            make_item(3, "CREATE VIEW v AS SELECT 1"),
        ];
        upgrade(snapshot);
    }

    #[mz_ore::test]
    #[should_panic(expected = "found comment on continual task")]
    fn test_panics_on_continual_task_comment() {
        let snapshot = vec![make_comment_on_ct(1), make_comment_on_table(2)];
        upgrade(snapshot);
    }

    #[mz_ore::test]
    fn test_no_ct_returns_empty() {
        let snapshot = vec![
            make_item(1, "CREATE TABLE foo (a INT)"),
            make_comment_on_table(1),
        ];
        let migrations = upgrade(snapshot);
        assert!(migrations.is_empty());
    }
}
