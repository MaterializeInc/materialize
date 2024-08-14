// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use fancy_regex::Regex;
use mysql_async::prelude::Queryable;

use crate::tunnel::MySqlConn;
use crate::{MissingPrivilege, MySqlError, QualifiedTableRef};

pub async fn validate_source_privileges(
    conn: &mut MySqlConn,
    tables: &[QualifiedTableRef<'_>],
) -> Result<(), MySqlError> {
    // MySQL doesn't have a great way to check privileges for the current user using SELECT
    // statements on information_schema tables, since privileges are set at multiple levels
    // and can be granted to users via roles, etc.
    // Instead, the SHOW GRANTS statement coalesces these privileges and shows us what we have.

    // First we need to see if we are using an account that is using any default-activated roles.
    // This can return a comma-deliminated string of roles that we can use directly in the
    // SHOW GRANTS query. This command is only possible on MySQL 8.0+, so we ignore any errors.
    let roles: Option<String> = conn
        .exec_first("SELECT CURRENT_ROLE()", ())
        .await
        .ok()
        .and_then(|val: Option<String>| match val {
            Some(inner) if inner != "NONE" => Some(inner),
            _ => None,
        });

    // Obtain the privileges for the current user using any roles that are default activated.
    let grant_query = match roles {
        None => "SHOW GRANTS FOR CURRENT_USER()".to_string(),
        Some(roles) => format!("SHOW GRANTS FOR CURRENT_USER() USING {}", roles),
    };

    // Parse and collect the grants into a map of schema -> table -> privileges
    let mut grant_map = BTreeMap::new();
    for grant in conn.exec(grant_query, ()).await? {
        let grant: String = grant;
        if let Some(object_grant) = get_object_grant(&grant) {
            grant_map
                .entry(object_grant.object_schema)
                .or_insert_with(BTreeMap::new)
                .entry(object_grant.object_name)
                .or_insert_with(BTreeSet::new)
                .extend(object_grant.privileges);
        }
    }

    // Check that we have the LOCK TABLES and SELECT privileges for each table
    let mut errors = tables
        .iter()
        .flat_map(|table| {
            ["LOCK TABLES", "SELECT"].iter().filter_map(|privilege| {
                // Check both the wildcard schema and the specific schema
                let privileged = [grant_map.get("*"), grant_map.get(table.schema_name)]
                    .iter()
                    .any(|schema_map| {
                        // Check both the wildcard table and the specific table
                        schema_map.map_or(false, |schema_map| {
                            [schema_map.get("*"), schema_map.get(table.table_name)]
                                .iter()
                                .any(|privs| {
                                    privs.map_or(false, |privs| {
                                        privs.contains(*privilege)
                                            || privs.contains("ALL PRIVILEGES")
                                    })
                                })
                        })
                    });

                if !privileged {
                    Some(MissingPrivilege {
                        privilege: privilege.to_string(),
                        qualified_table_name: format!("{}.{}", table.schema_name, table.table_name),
                    })
                } else {
                    None
                }
            })
        })
        .collect::<Vec<_>>();

    // Check that we have REPLICATION SLAVE priviliges at the global level
    if !grant_map
        .get("*")
        .and_then(|schema_map| schema_map.get("*"))
        .map(|privs| privs.contains("REPLICATION SLAVE") || privs.contains("ALL PRIVILEGES"))
        .unwrap_or(false)
    {
        errors.push(MissingPrivilege {
            privilege: "REPLICATION SLAVE".to_string(),
            qualified_table_name: "*.*".to_string(),
        });
    }

    if !errors.is_empty() {
        Err(MySqlError::MissingPrivileges(errors))
    } else {
        Ok(())
    }
}

/// Regex to parse a SHOW GRANTS line. Inspired by several stack overflow posts and
/// adjusted to account for the different quoting styles across MySQL versions.
/// If this regex matches then this is a grant on an actual object which looks like:
///     GRANT SELECT, INSERT, UPDATE ON `db1`.* TO `u1`@`localhost`
///     GRANT SELECT ON `db1`.`table1` TO `my_user`@`localhost` WITH GRANT OPTION
/// The regex needs to account for the possibility of a wildcard schema or table, and for the
/// quote-char to be part of the table/schema name too.
/// Group 1 is the list of privileges being granted
/// Group 2 is either the wildcard * or a quoted database/schema
/// Group 4 is the unquoted database/schema when the wildcard is not matched
/// Group 5 is either the wildcard * or a quoted table
/// Group 7 is the unquoted table when the wildcard is not matched
/// Group 9 is the user being granted
/// We use the `fancy_regex` crate to allow backreferences which are necessary to find the ending
/// quote of each identifier since there are different quoting types across mysql versions.
static GRANT_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"GRANT (.+) ON (\*|(['`"])(.*)\3).(\*|(['`"])(.*)\6) TO (['`"])(.*)\8@.*"#)
        .expect("valid")
});

#[derive(Debug, PartialEq, Eq)]
struct MySqlObjectGrant {
    privileges: BTreeSet<String>,
    object_schema: String,
    object_name: String,
}

/// Parses a returned row of a SHOW GRANTS statement to return a MySqlObjectGrant
/// If the grant is not on an object (e.g. a grant of a role to a user), returns None
fn get_object_grant(grant: &str) -> Option<MySqlObjectGrant> {
    match GRANT_REGEX.captures(grant) {
        Ok(None) => None,
        Err(err) => {
            tracing::warn!("Error parsing privilege grant: {}", err);
            None
        }
        Ok(Some(captures)) => {
            let object_schema = if captures.get(2).expect("valid").as_str() == "*" {
                "*".to_string()
            } else {
                captures.get(4).expect("valid").as_str().to_string()
            };
            let object_name = if captures.get(5).expect("valid").as_str() == "*" {
                "*".to_string()
            } else {
                captures.get(7).expect("valid").as_str().to_string()
            };
            let privileges = captures.get(1).expect("valid").as_str();
            Some(MySqlObjectGrant {
                privileges: privileges.split(", ").map(|s| s.to_string()).collect(),
                object_schema,
                object_name,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_get_object_grant() {
        // backticks and wildcard
        let grant = "GRANT SELECT, INSERT, UPDATE ON `db1`.* TO `u1`@`localhost`";
        let expected_grant = MySqlObjectGrant {
            privileges: ["SELECT", "INSERT", "UPDATE"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            object_schema: "db1".to_string(),
            object_name: "*".to_string(),
        };
        assert_eq!(get_object_grant(grant), Some(expected_grant));

        // single-quotes
        let grant = "GRANT SUPER, CREATE TEMPORARY TABLES, LOCK TABLES ON 'db1'.'table1' TO `u1`@`localhost`";
        let expected_grant = MySqlObjectGrant {
            privileges: ["SUPER", "CREATE TEMPORARY TABLES", "LOCK TABLES"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            object_schema: "db1".to_string(),
            object_name: "table1".to_string(),
        };
        assert_eq!(get_object_grant(grant), Some(expected_grant));

        // wildcards
        let grant = "GRANT ALL PRIVILEGES ON *.* TO `u1`@`localhost`";
        let expected_grant = MySqlObjectGrant {
            privileges: ["ALL PRIVILEGES"].iter().map(|s| s.to_string()).collect(),
            object_schema: "*".to_string(),
            object_name: "*".to_string(),
        };
        assert_eq!(get_object_grant(grant), Some(expected_grant));

        // special chars
        let grant = "GRANT SELECT, INSERT, UPDATE ON `таблица`.`mixED_CAse` TO `u1`@`localhost`";
        let expected_grant = MySqlObjectGrant {
            privileges: ["SELECT", "INSERT", "UPDATE"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            object_schema: "таблица".to_string(),
            object_name: "mixED_CAse".to_string(),
        };

        assert_eq!(get_object_grant(grant), Some(expected_grant));

        // quotes in names
        let grant = "GRANT SUPER, CREATE TEMPORARY TABLES, LOCK TABLES ON `r`w`.`'sd'` TO `u1`@`localhost` IDENTIFIED BY PASSWORD `test`";
        let expected_grant = MySqlObjectGrant {
            privileges: ["SUPER", "CREATE TEMPORARY TABLES", "LOCK TABLES"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            object_schema: "r`w".to_string(),
            object_name: "'sd'".to_string(),
        };
        assert_eq!(get_object_grant(grant), Some(expected_grant));
    }
}
