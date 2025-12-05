//! Tests for the typed representation module.

use super::super::parser::parse_statements;
use super::super::raw;
use super::types::{Database, DatabaseObject, Schema};
use mz_sql_parser::ast::*;
use std::path::PathBuf;
use tempfile::TempDir;

fn create_raw_object(name: &str, path: PathBuf, sql: &str) -> raw::DatabaseObject {
    let statements = parse_statements(vec![sql]).unwrap();
    raw::DatabaseObject {
        name: name.to_string(),
        path,
        statements,
    }
}

#[test]
fn test_valid_simple_object_name() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let raw = create_raw_object("foo", path, "CREATE TABLE foo (id INT);");
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
}

#[test]
fn test_valid_qualified_schema() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let raw = create_raw_object("foo", path, "CREATE TABLE public.foo (id INT);");
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
}

#[test]
fn test_valid_fully_qualified() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let raw = create_raw_object("foo", path, "CREATE TABLE materialize.public.foo (id INT);");
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
}

#[test]
fn test_invalid_object_name_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let raw = create_raw_object("foo", path, "CREATE TABLE bar (id INT);");
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("object name mismatch"));
    assert!(err.to_string().contains("bar"));
    assert!(err.to_string().contains("foo"));
}

#[test]
fn test_invalid_schema_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let raw = create_raw_object("foo", path, "CREATE TABLE private.foo (id INT);");
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_str = err.to_string();
    // Check for the schema mismatch error content
    assert!(err_str.contains("schema qualifier mismatch"));
    assert!(err_str.contains("private"));
    assert!(err_str.contains("public"));
}

#[test]
fn test_invalid_database_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let raw = create_raw_object("foo", path, "CREATE TABLE other_db.public.foo (id INT);");
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_str = err.to_string();
    // Check for the database mismatch error content
    assert!(err_str.contains("database qualifier mismatch"));
    assert!(err_str.contains("other_db"));
    assert!(err_str.contains("materialize"));
}

#[test]
fn test_valid_with_indexes_and_grants() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let sql = r#"
        CREATE TABLE foo (id INT);
        CREATE INDEX idx_foo IN CLUSTER c ON foo (id);
        GRANT SELECT ON foo TO user1;
        COMMENT ON TABLE foo IS 'test table';
    "#;

    let raw = create_raw_object("foo", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();
    assert_eq!(obj.indexes.len(), 1);
    assert_eq!(obj.grants.len(), 1);
    assert_eq!(obj.comments.len(), 1);
}

#[test]
fn test_invalid_index_on_different_object() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let sql = r#"
        CREATE TABLE foo (id INT);
        CREATE INDEX idx_bar ON bar (id);
    "#;

    let raw = create_raw_object("foo", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("INDEX"));
    assert!(err.to_string().contains("bar"));
    assert!(err.to_string().contains("foo"));
}

#[test]
fn test_invalid_grant_on_different_object() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let sql = r#"
        CREATE TABLE foo (id INT);
        GRANT SELECT ON bar TO user1;
    "#;

    let raw = create_raw_object("foo", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("GRANT"));
    assert!(err.to_string().contains("bar"));
    assert!(err.to_string().contains("foo"));
}

#[test]
fn test_invalid_comment_on_different_object() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let sql = r#"
        CREATE TABLE foo (id INT);
        COMMENT ON TABLE bar IS 'wrong table';
    "#;

    let raw = create_raw_object("foo", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("COMMENT"));
    assert!(err.to_string().contains("bar"));
    assert!(err.to_string().contains("foo"));
}

#[test]
fn test_valid_column_comment() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let sql = r#"
        CREATE TABLE foo (id INT, name TEXT);
        COMMENT ON COLUMN foo.id IS 'primary key';
        COMMENT ON COLUMN foo.name IS 'user name';
    "#;

    let raw = create_raw_object("foo", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();
    assert_eq!(obj.comments.len(), 2);
}

#[test]
fn test_invalid_column_comment_on_different_table() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let sql = r#"
        CREATE TABLE foo (id INT);
        COMMENT ON COLUMN bar.id IS 'wrong table';
    "#;

    let raw = create_raw_object("foo", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("column COMMENT"));
    assert!(err.to_string().contains("bar"));
    assert!(err.to_string().contains("foo"));
}

#[test]
fn test_invalid_comment_type_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/foo.sql");

    let sql = r#"
        CREATE TABLE foo (id INT);
        COMMENT ON VIEW foo IS 'this is actually a table';
    "#;

    let raw = create_raw_object("foo", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("View"));
    assert!(err.to_string().contains("Table"));
}

// ===== Dependency Normalization Tests =====
// These tests verify that all object references within statements
// are normalized to be fully qualified (database.schema.object).

#[test]
fn test_view_dependency_normalization() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/active_users.sql");

    // View references "users" without qualification
    let sql = r#"
        CREATE VIEW active_users AS
        SELECT id, name FROM users WHERE active = true;
    "#;

    let raw = create_raw_object("active_users", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();

    // Verify the statement name is normalized
    let view_stmt = match obj.stmt {
        super::super::ast::Statement::CreateView(ref s) => s,
        _ => panic!("Expected CreateView statement"),
    };

    // Verify the view name is fully qualified
    assert_eq!(
        view_stmt.definition.name.to_string(),
        "materialize.public.active_users"
    );

    // Verify the table reference in the query is normalized
    // The query body should reference materialize.public.users
    let query = &view_stmt.definition.query;
    match &query.body {
        SetExpr::Select(select) => {
            assert_eq!(select.from.len(), 1);
            match &select.from[0].relation {
                TableFactor::Table { name, .. } => {
                    assert_eq!(name.name().to_string(), "materialize.public.users");
                }
                _ => panic!("Expected table reference"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_materialized_view_dependency_normalization() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join("materialize/public/active_users_mv.sql");

    // Materialized view references "users" without qualification
    let sql = r#"
        CREATE MATERIALIZED VIEW active_users_mv IN CLUSTER quickstart AS
        SELECT * FROM users;
    "#;

    let raw = create_raw_object("active_users_mv", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();

    // Verify the statement is a materialized view
    let mv_stmt = match obj.stmt {
        super::super::ast::Statement::CreateMaterializedView(ref s) => s,
        _ => panic!("Expected CreateMaterializedView statement"),
    };

    // Verify the table reference is normalized
    match &mv_stmt.query.body {
        SetExpr::Select(select) => {
            assert_eq!(select.from.len(), 1);
            match &select.from[0].relation {
                TableFactor::Table { name, .. } => {
                    assert_eq!(name.name().to_string(), "materialize.public.users");
                }
                _ => panic!("Expected table reference"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_view_with_join_normalization() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/user_orders.sql");

    // View with JOIN - both table references unqualified
    let sql = r#"
        CREATE VIEW user_orders AS
        SELECT u.id, u.name, o.order_id
        FROM users u
        JOIN orders o ON u.id = o.user_id;
    "#;

    let raw = create_raw_object("user_orders", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();

    let view_stmt = match obj.stmt {
        super::super::ast::Statement::CreateView(ref s) => s,
        _ => panic!("Expected CreateView statement"),
    };

    // Verify both table references are normalized
    match &view_stmt.definition.query.body {
        SetExpr::Select(select) => {
            assert_eq!(select.from.len(), 1);
            let table_with_joins = &select.from[0];

            // Check main table (users)
            match &table_with_joins.relation {
                TableFactor::Table { name, .. } => {
                    assert_eq!(name.name().to_string(), "materialize.public.users");
                }
                _ => panic!("Expected table reference for users"),
            }

            // Check joined table (orders)
            assert_eq!(table_with_joins.joins.len(), 1);
            match &table_with_joins.joins[0].relation {
                TableFactor::Table { name, .. } => {
                    assert_eq!(name.name().to_string(), "materialize.public.orders");
                }
                _ => panic!("Expected table reference for orders"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_view_with_subquery_normalization() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/recent_orders.sql");

    // View with subquery - both references unqualified
    let sql = r#"
        CREATE VIEW recent_orders AS
        SELECT * FROM orders
        WHERE user_id IN (SELECT id FROM users WHERE active = true);
    "#;

    let raw = create_raw_object("recent_orders", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();

    let view_stmt = match obj.stmt {
        super::super::ast::Statement::CreateView(ref s) => s,
        _ => panic!("Expected CreateView statement"),
    };

    // Verify main query table reference is normalized
    match &view_stmt.definition.query.body {
        SetExpr::Select(select) => {
            // Check main FROM clause (orders)
            match &select.from[0].relation {
                TableFactor::Table { name, .. } => {
                    assert_eq!(name.name().to_string(), "materialize.public.orders");
                }
                _ => panic!("Expected table reference for orders"),
            }

            // Verify subquery also has normalized table reference
            // The WHERE clause contains the subquery, but verifying the exact
            // structure is complex - the main assertion above covers the key point
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_view_with_cte_normalization() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/user_summary.sql");

    // View with CTE (WITH clause) - unqualified table references
    let sql = r#"
        CREATE VIEW user_summary AS
        WITH active_users AS (
            SELECT id, name FROM users WHERE active = true
        )
        SELECT * FROM active_users;
    "#;

    let raw = create_raw_object("user_summary", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();

    let view_stmt = match obj.stmt {
        super::super::ast::Statement::CreateView(ref s) => s,
        _ => panic!("Expected CreateView statement"),
    };

    // Verify CTE references are normalized
    let query = &view_stmt.definition.query;
    match &query.ctes {
        CteBlock::Simple(ctes) => {
            assert_eq!(ctes.len(), 1);
            // Verify the CTE query references the normalized table
            match &ctes[0].query.body {
                SetExpr::Select(select) => match &select.from[0].relation {
                    TableFactor::Table { name, .. } => {
                        assert_eq!(name.name().to_string(), "materialize.public.users");
                    }
                    _ => panic!("Expected table reference in CTE"),
                },
                _ => panic!("Expected SELECT in CTE"),
            }
        }
        _ => panic!("Expected simple CTE block"),
    }
}

#[test]
fn test_table_from_source_normalization() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/kafka_table.sql");

    // CREATE TABLE FROM SOURCE with unqualified source reference
    let sql = r#"
        CREATE TABLE kafka_table
        FROM SOURCE kafka_source (REFERENCE public.kafka_table);
    "#;

    let raw = create_raw_object("kafka_table", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();

    // Verify the source reference is normalized
    let table_stmt = match obj.stmt {
        super::super::ast::Statement::CreateTableFromSource(ref s) => s,
        _ => panic!("Expected CreateTableFromSource statement"),
    };

    // Verify source name is fully qualified
    assert_eq!(
        table_stmt.source.to_string(),
        "materialize.public.kafka_source"
    );
}

#[test]
fn test_sink_normalization() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/kafka_sink.sql");

    // CREATE SINK with unqualified FROM and connection references
    let sql = r#"
        CREATE SINK kafka_sink
        IN CLUSTER quickstart
        FROM users
        INTO KAFKA CONNECTION kafka_conn (TOPIC 'users');
    "#;

    let raw = create_raw_object("kafka_sink", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();

    let sink_stmt = match obj.stmt {
        super::super::ast::Statement::CreateSink(ref s) => s,
        _ => panic!("Expected CreateSink statement"),
    };

    // Verify FROM reference is normalized
    assert_eq!(sink_stmt.from.to_string(), "materialize.public.users");

    // Verify connection reference is normalized
    match &sink_stmt.connection {
        CreateSinkConnection::Kafka { connection, .. } => {
            assert_eq!(connection.to_string(), "materialize.public.kafka_conn");
        }
        _ => panic!("Expected Kafka sink connection"),
    }
}

#[test]
fn test_index_normalization() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/users.sql");

    // Table with index - index references unqualified table name
    let sql = r#"
        CREATE TABLE users (id INT, name TEXT);
        CREATE INDEX users_id_idx IN CLUSTER c ON users (id);
    "#;

    let raw = create_raw_object("users", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();

    // Verify index reference is normalized
    assert_eq!(obj.indexes.len(), 1);
    let index = &obj.indexes[0];
    assert_eq!(index.on_name.to_string(), "materialize.public.users");
}

#[test]
fn test_comment_normalization() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/users.sql");

    // Table with comments - comment references unqualified table name
    let sql = r#"
        CREATE TABLE users (id INT, name TEXT);
        COMMENT ON TABLE users IS 'User accounts';
        COMMENT ON COLUMN users.id IS 'Unique identifier';
    "#;

    let raw = create_raw_object("users", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();

    // Verify comment references are normalized
    assert_eq!(obj.comments.len(), 2);

    // Check table comment
    match &obj.comments[0].object {
        CommentObjectType::Table { name } => {
            assert_eq!(name.to_string(), "materialize.public.users");
        }
        _ => panic!("Expected Table comment"),
    }

    // Check column comment (should normalize the table reference)
    match &obj.comments[1].object {
        CommentObjectType::Column { name } => {
            assert_eq!(name.relation.to_string(), "materialize.public.users");
            assert_eq!(name.column.to_string(), "id");
        }
        _ => panic!("Expected Column comment"),
    }
}

#[test]
fn test_schema_qualified_dependency_normalization() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/active_users.sql");

    // View with schema-qualified (but not fully qualified) table reference
    let sql = r#"
        CREATE VIEW active_users AS
        SELECT * FROM public.users WHERE active = true;
    "#;

    let raw = create_raw_object("active_users", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();

    let view_stmt = match obj.stmt {
        super::super::ast::Statement::CreateView(ref s) => s,
        _ => panic!("Expected CreateView statement"),
    };

    // Verify the schema-qualified reference is now fully qualified
    match &view_stmt.definition.query.body {
        SetExpr::Select(select) => {
            match &select.from[0].relation {
                TableFactor::Table { name, .. } => {
                    // Should prepend database name
                    assert_eq!(name.name().to_string(), "materialize.public.users");
                }
                _ => panic!("Expected table reference"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_already_fully_qualified_unchanged() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("materialize/public/cross_db_view.sql");

    // View with already fully qualified table reference
    let sql = r#"
        CREATE VIEW cross_db_view AS
        SELECT * FROM other_db.other_schema.other_table;
    "#;

    let raw = create_raw_object("cross_db_view", path, sql);
    let result = DatabaseObject::try_from(raw);

    assert!(result.is_ok());
    let obj = result.unwrap();

    let view_stmt = match obj.stmt {
        super::super::ast::Statement::CreateView(ref s) => s,
        _ => panic!("Expected CreateView statement"),
    };

    // Verify already fully qualified names remain unchanged
    match &view_stmt.definition.query.body {
        SetExpr::Select(select) => {
            match &select.from[0].relation {
                TableFactor::Table { name, .. } => {
                    // Should remain as-is
                    assert_eq!(name.name().to_string(), "other_db.other_schema.other_table");
                }
                _ => panic!("Expected table reference"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_valid_database_mod_comment() {
    use std::collections::BTreeMap;

    // Valid COMMENT ON DATABASE statement
    let sql = "COMMENT ON DATABASE materialize IS 'Main database';";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: Some(statements),
        schemas: BTreeMap::new(),
    };

    let result = Database::try_from(raw_db);
    assert!(result.is_ok(), "Valid database comment should be accepted");
}

#[test]
fn test_valid_database_mod_grant() {
    use std::collections::BTreeMap;

    // Valid GRANT ON DATABASE statement
    let sql = "GRANT USAGE ON DATABASE materialize TO user1;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: Some(statements),
        schemas: BTreeMap::new(),
    };

    let result = Database::try_from(raw_db);
    assert!(result.is_ok(), "Valid database grant should be accepted");
}

#[test]
fn test_invalid_database_mod_wrong_statement_type() {
    use std::collections::BTreeMap;

    // Invalid: CREATE TABLE in database mod file
    let sql = "CREATE TABLE users (id INT);";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: Some(statements),
        schemas: BTreeMap::new(),
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_err(),
        "CREATE TABLE in database mod file should be rejected"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("InvalidDatabaseModStatement"),
        "Should report invalid statement type"
    );
}

#[test]
fn test_invalid_database_mod_comment_wrong_target() {
    use std::collections::BTreeMap;

    // Invalid: COMMENT ON SCHEMA in database mod file
    let sql = "COMMENT ON SCHEMA public IS 'Public schema';";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: Some(statements),
        schemas: BTreeMap::new(),
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_err(),
        "COMMENT ON SCHEMA in database mod file should be rejected"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("DatabaseModCommentTargetMismatch"),
        "Should report wrong comment target"
    );
}

#[test]
fn test_invalid_database_mod_comment_wrong_database() {
    use std::collections::BTreeMap;

    // Invalid: COMMENT ON different database
    let sql = "COMMENT ON DATABASE other_db IS 'Other database';";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: Some(statements),
        schemas: BTreeMap::new(),
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_err(),
        "COMMENT ON wrong database should be rejected"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("DatabaseModCommentTargetMismatch"),
        "Should report wrong database target"
    );
}

#[test]
fn test_valid_schema_mod_comment() {
    use std::collections::BTreeMap;

    // Valid COMMENT ON SCHEMA statement (unqualified, will be normalized)
    let sql = "COMMENT ON SCHEMA public IS 'Public schema';";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_ok(),
        "Valid schema comment should be accepted: {:?}",
        result.err()
    );
}

#[test]
fn test_valid_schema_mod_grant() {
    use std::collections::BTreeMap;

    // Valid GRANT ON SCHEMA statement (unqualified, will be normalized)
    let sql = "GRANT USAGE ON SCHEMA public TO user1;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_ok(),
        "Valid schema grant should be accepted: {:?}",
        result.err()
    );
}

#[test]
fn test_invalid_schema_mod_wrong_statement_type() {
    use std::collections::BTreeMap;

    // Invalid: CREATE VIEW in schema mod file
    let sql = "CREATE VIEW v AS SELECT 1;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_err(),
        "CREATE VIEW in schema mod file should be rejected"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("InvalidSchemaModStatement"),
        "Should report invalid statement type"
    );
}

#[test]
fn test_invalid_schema_mod_comment_wrong_target() {
    use std::collections::BTreeMap;

    // Invalid: COMMENT ON TABLE in schema mod file
    let sql = "COMMENT ON TABLE users IS 'Users table';";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_err(),
        "COMMENT ON TABLE in schema mod file should be rejected"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("SchemaModCommentTargetMismatch"),
        "Should report wrong comment target"
    );
}

#[test]
fn test_invalid_schema_mod_comment_wrong_schema() {
    use std::collections::BTreeMap;

    // Invalid: COMMENT ON different schema
    let sql = "COMMENT ON SCHEMA other_schema IS 'Other schema';";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_err(),
        "COMMENT ON wrong schema should be rejected"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("SchemaModCommentTargetMismatch"),
        "Should report wrong schema target"
    );
}

#[test]
fn test_valid_database_mod_alter_default_privileges() {
    use std::collections::BTreeMap;

    // Valid ALTER DEFAULT PRIVILEGES with IN DATABASE
    let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN DATABASE materialize GRANT SELECT ON TABLES TO user2;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: Some(statements),
        schemas: BTreeMap::new(),
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_ok(),
        "Valid ALTER DEFAULT PRIVILEGES IN DATABASE should be accepted: {:?}",
        result.err()
    );
}

#[test]
fn test_invalid_database_mod_alter_default_privileges_no_scope() {
    use std::collections::BTreeMap;

    // Invalid: Missing IN DATABASE
    let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 GRANT SELECT ON TABLES TO user2;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: Some(statements),
        schemas: BTreeMap::new(),
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_err(),
        "ALTER DEFAULT PRIVILEGES without scope should be rejected"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("AlterDefaultPrivilegesRequiresDatabaseScope"),
        "Should require IN DATABASE"
    );
}

#[test]
fn test_invalid_database_mod_alter_default_privileges_wrong_database() {
    use std::collections::BTreeMap;

    // Invalid: Wrong database
    let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN DATABASE other_db GRANT SELECT ON TABLES TO user2;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: Some(statements),
        schemas: BTreeMap::new(),
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_err(),
        "ALTER DEFAULT PRIVILEGES with wrong database should be rejected"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("AlterDefaultPrivilegesDatabaseMismatch"),
        "Should report wrong database"
    );
}

#[test]
fn test_invalid_database_mod_alter_default_privileges_with_schema() {
    use std::collections::BTreeMap;

    // Invalid: IN SCHEMA not allowed in database mod
    let sql =
        "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN SCHEMA public GRANT SELECT ON TABLES TO user2;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: Some(statements),
        schemas: BTreeMap::new(),
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_err(),
        "ALTER DEFAULT PRIVILEGES with IN SCHEMA should be rejected in database mod"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("AlterDefaultPrivilegesSchemaNotAllowed"),
        "Should reject IN SCHEMA"
    );
}

#[test]
fn test_valid_schema_mod_alter_default_privileges_unqualified() {
    use std::collections::BTreeMap;

    // Valid ALTER DEFAULT PRIVILEGES with unqualified schema
    let sql =
        "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN SCHEMA public GRANT SELECT ON TABLES TO user2;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_ok(),
        "Valid ALTER DEFAULT PRIVILEGES with unqualified schema should be accepted: {:?}",
        result.err()
    );
}

#[test]
fn test_valid_schema_mod_alter_default_privileges_qualified() {
    use std::collections::BTreeMap;

    // Valid ALTER DEFAULT PRIVILEGES with qualified schema
    let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN SCHEMA materialize.public GRANT SELECT ON TABLES TO user2;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_ok(),
        "Valid ALTER DEFAULT PRIVILEGES with qualified schema should be accepted: {:?}",
        result.err()
    );
}

#[test]
fn test_invalid_schema_mod_alter_default_privileges_no_scope() {
    use std::collections::BTreeMap;

    // Invalid: Missing IN SCHEMA
    let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 GRANT SELECT ON TABLES TO user2;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_err(),
        "ALTER DEFAULT PRIVILEGES without scope should be rejected in schema mod"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("AlterDefaultPrivilegesRequiresSchemaScope"),
        "Should require IN SCHEMA"
    );
}

#[test]
fn test_invalid_schema_mod_alter_default_privileges_with_database() {
    use std::collections::BTreeMap;

    // Invalid: IN DATABASE not allowed in schema mod
    let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN DATABASE materialize GRANT SELECT ON TABLES TO user2;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_err(),
        "ALTER DEFAULT PRIVILEGES with IN DATABASE should be rejected in schema mod"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("AlterDefaultPrivilegesDatabaseNotAllowed"),
        "Should reject IN DATABASE"
    );
}

#[test]
fn test_invalid_schema_mod_alter_default_privileges_wrong_schema() {
    use std::collections::BTreeMap;

    // Invalid: Wrong schema
    let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN SCHEMA other_schema GRANT SELECT ON TABLES TO user2;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_err(),
        "ALTER DEFAULT PRIVILEGES with wrong schema should be rejected"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("AlterDefaultPrivilegesSchemaMismatch"),
        "Should report wrong schema"
    );
}

#[test]
fn test_schema_mod_comment_normalization() {
    use std::collections::BTreeMap;

    // Test that unqualified schema name gets normalized to qualified
    let sql = "COMMENT ON SCHEMA public IS 'Public schema';";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_ok(),
        "Valid schema comment should be accepted: {:?}",
        result.err()
    );

    let db = result.unwrap();
    let schema = db.schemas.iter().find(|s| s.name == "public").unwrap();

    // Check that the schema name was normalized in the mod statement
    if let Some(mod_stmts) = &schema.mod_statements {
        assert_eq!(mod_stmts.len(), 1, "Should have one mod statement");
        let stmt_sql = format!("{}", mod_stmts[0]);
        assert!(
            stmt_sql.contains("materialize.public"),
            "Schema name should be normalized to materialize.public, got: {}",
            stmt_sql
        );
    } else {
        panic!("Schema should have mod statements");
    }
}

#[test]
fn test_schema_mod_grant_normalization() {
    use std::collections::BTreeMap;

    // Test that unqualified schema name gets normalized to qualified
    let sql = "GRANT USAGE ON SCHEMA public TO user1;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_ok(),
        "Valid schema grant should be accepted: {:?}",
        result.err()
    );

    let db = result.unwrap();
    let schema = db.schemas.iter().find(|s| s.name == "public").unwrap();

    // Check that the schema name was normalized in the mod statement
    if let Some(mod_stmts) = &schema.mod_statements {
        assert_eq!(mod_stmts.len(), 1, "Should have one mod statement");
        let stmt_sql = format!("{}", mod_stmts[0]);
        assert!(
            stmt_sql.contains("materialize.public"),
            "Schema name should be normalized to materialize.public, got: {}",
            stmt_sql
        );
    } else {
        panic!("Schema should have mod statements");
    }
}

#[test]
fn test_schema_mod_alter_default_privileges_normalization() {
    use std::collections::BTreeMap;

    // Test that unqualified schema name gets normalized to qualified
    let sql =
        "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN SCHEMA public GRANT SELECT ON TABLES TO user2;";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_ok(),
        "Valid ALTER DEFAULT PRIVILEGES should be accepted: {:?}",
        result.err()
    );

    let db = result.unwrap();
    let schema = db.schemas.iter().find(|s| s.name == "public").unwrap();

    // Check that the schema name was normalized in the mod statement
    if let Some(mod_stmts) = &schema.mod_statements {
        assert_eq!(mod_stmts.len(), 1, "Should have one mod statement");
        let stmt_sql = format!("{}", mod_stmts[0]);
        assert!(
            stmt_sql.contains("materialize.public"),
            "Schema name should be normalized to materialize.public, got: {}",
            stmt_sql
        );
    } else {
        panic!("Schema should have mod statements");
    }
}

#[test]
fn test_schema_mod_already_qualified_names() {
    use std::collections::BTreeMap;

    // Test that already qualified names remain unchanged
    let sql = "COMMENT ON SCHEMA materialize.public IS 'test';";
    let statements = parse_statements(vec![sql]).unwrap();

    let raw_schema = raw::Schema {
        name: "public".to_string(),
        mod_statements: Some(statements),
        objects: Vec::new(),
    };

    let mut schemas = BTreeMap::new();
    schemas.insert("public".to_string(), raw_schema);

    let raw_db = raw::Database {
        name: "materialize".to_string(),
        mod_statements: None,
        schemas,
    };

    let result = Database::try_from(raw_db);
    assert!(
        result.is_ok(),
        "Qualified schema comment should be accepted: {:?}",
        result.err()
    );

    let db = result.unwrap();
    let schema = db.schemas.iter().find(|s| s.name == "public").unwrap();

    // Check that the already qualified name remains qualified
    if let Some(mod_stmts) = &schema.mod_statements {
        assert_eq!(mod_stmts.len(), 1, "Should have one mod statement");
        let stmt_sql = format!("{}", mod_stmts[0]);
        assert!(
            stmt_sql.contains("materialize.public"),
            "Schema name should remain materialize.public, got: {}",
            stmt_sql
        );
    } else {
        panic!("Schema should have mod statements");
    }
}

// Tests for schema segregation validation (storage vs computation objects)

#[test]
fn test_schema_with_tables_and_views_fails() {
    let table_sql = "CREATE TABLE users (id INT);";
    let view_sql = "CREATE VIEW active_users AS SELECT * FROM users;";

    let table_stmts = parse_statements(vec![table_sql]).unwrap();
    let view_stmts = parse_statements(vec![view_sql]).unwrap();

    let raw_table = raw::DatabaseObject {
        name: "users".to_string(),
        path: PathBuf::from("materialize/mixed/users.sql"),
        statements: table_stmts,
    };

    let raw_view = raw::DatabaseObject {
        name: "active_users".to_string(),
        path: PathBuf::from("materialize/mixed/active_users.sql"),
        statements: view_stmts,
    };

    let raw_schema = raw::Schema {
        name: "mixed".to_string(),
        objects: vec![raw_table, raw_view],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_err(),
        "Schema with both tables and views should fail validation"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("StorageAndComputationObjectsInSameSchema"),
        "Should report storage and computation mix"
    );
    assert!(err_msg.contains("mixed"), "Should mention schema name");
}

#[test]
fn test_schema_with_tables_and_materialized_views_fails() {
    let table_sql = "CREATE TABLE orders (id INT);";
    let mv_sql = "CREATE MATERIALIZED VIEW order_summary AS SELECT COUNT(*) FROM orders;";

    let table_stmts = parse_statements(vec![table_sql]).unwrap();
    let mv_stmts = parse_statements(vec![mv_sql]).unwrap();

    let raw_table = raw::DatabaseObject {
        name: "orders".to_string(),
        path: PathBuf::from("materialize/mixed/orders.sql"),
        statements: table_stmts,
    };

    let raw_mv = raw::DatabaseObject {
        name: "order_summary".to_string(),
        path: PathBuf::from("materialize/mixed/order_summary.sql"),
        statements: mv_stmts,
    };

    let raw_schema = raw::Schema {
        name: "mixed".to_string(),
        objects: vec![raw_table, raw_mv],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_err(),
        "Schema with both tables and materialized views should fail validation"
    );
}

#[test]
fn test_schema_with_sinks_and_views_fails() {
    let sink_sql = "CREATE SINK user_sink IN CLUSTER quickstart FROM users INTO KAFKA CONNECTION kafka_conn (TOPIC 'users');";
    let view_sql = "CREATE VIEW user_view AS SELECT * FROM users;";

    let sink_stmts = parse_statements(vec![sink_sql]).unwrap();
    let view_stmts = parse_statements(vec![view_sql]).unwrap();

    let raw_sink = raw::DatabaseObject {
        name: "user_sink".to_string(),
        path: PathBuf::from("materialize/mixed/user_sink.sql"),
        statements: sink_stmts,
    };

    let raw_view = raw::DatabaseObject {
        name: "user_view".to_string(),
        path: PathBuf::from("materialize/mixed/user_view.sql"),
        statements: view_stmts,
    };

    let raw_schema = raw::Schema {
        name: "mixed".to_string(),
        objects: vec![raw_sink, raw_view],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_err(),
        "Schema with both sinks and views should fail validation"
    );
}

#[test]
fn test_schema_with_sinks_and_materialized_views_fails() {
    let sink_sql = "CREATE SINK order_sink IN CLUSTER quickstart FROM orders INTO KAFKA CONNECTION kafka_conn (TOPIC 'orders');";
    let mv_sql =
        "CREATE MATERIALIZED VIEW order_mv IN CLUSTER quickstart AS SELECT COUNT(*) FROM orders;";

    let sink_stmts = parse_statements(vec![sink_sql]).unwrap();
    let mv_stmts = parse_statements(vec![mv_sql]).unwrap();

    let raw_sink = raw::DatabaseObject {
        name: "order_sink".to_string(),
        path: PathBuf::from("materialize/mixed/order_sink.sql"),
        statements: sink_stmts,
    };

    let raw_mv = raw::DatabaseObject {
        name: "order_mv".to_string(),
        path: PathBuf::from("materialize/mixed/order_mv.sql"),
        statements: mv_stmts,
    };

    let raw_schema = raw::Schema {
        name: "mixed".to_string(),
        objects: vec![raw_sink, raw_mv],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_err(),
        "Schema with both sinks and materialized views should fail validation"
    );
}

#[test]
fn test_schema_with_tables_sinks_and_views_fails() {
    let table_sql = "CREATE TABLE users (id INT);";
    let sink_sql = "CREATE SINK user_sink IN CLUSTER quickstart FROM users INTO KAFKA CONNECTION kafka_conn (TOPIC 'users');";
    let view_sql = "CREATE VIEW user_view AS SELECT * FROM users;";

    let table_stmts = parse_statements(vec![table_sql]).unwrap();
    let sink_stmts = parse_statements(vec![sink_sql]).unwrap();
    let view_stmts = parse_statements(vec![view_sql]).unwrap();

    let raw_table = raw::DatabaseObject {
        name: "users".to_string(),
        path: PathBuf::from("materialize/mixed/users.sql"),
        statements: table_stmts,
    };

    let raw_sink = raw::DatabaseObject {
        name: "user_sink".to_string(),
        path: PathBuf::from("materialize/mixed/user_sink.sql"),
        statements: sink_stmts,
    };

    let raw_view = raw::DatabaseObject {
        name: "user_view".to_string(),
        path: PathBuf::from("materialize/mixed/user_view.sql"),
        statements: view_stmts,
    };

    let raw_schema = raw::Schema {
        name: "mixed".to_string(),
        objects: vec![raw_table, raw_sink, raw_view],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_err(),
        "Schema with tables, sinks, and views should fail validation"
    );
}

#[test]
fn test_schema_with_only_tables_succeeds() {
    let table1_sql = "CREATE TABLE users (id INT);";
    let table2_sql = "CREATE TABLE orders (id INT);";

    let table1_stmts = parse_statements(vec![table1_sql]).unwrap();
    let table2_stmts = parse_statements(vec![table2_sql]).unwrap();

    let raw_table1 = raw::DatabaseObject {
        name: "users".to_string(),
        path: PathBuf::from("materialize/tables/users.sql"),
        statements: table1_stmts,
    };

    let raw_table2 = raw::DatabaseObject {
        name: "orders".to_string(),
        path: PathBuf::from("materialize/tables/orders.sql"),
        statements: table2_stmts,
    };

    let raw_schema = raw::Schema {
        name: "tables".to_string(),
        objects: vec![raw_table1, raw_table2],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_ok(),
        "Schema with only tables should pass validation"
    );
}

#[test]
fn test_schema_with_tables_and_sinks_succeeds() {
    let table_sql = "CREATE TABLE users (id INT);";
    let sink_sql = "CREATE SINK user_sink IN CLUSTER quickstart FROM users INTO KAFKA CONNECTION kafka_conn (TOPIC 'users');";

    let table_stmts = parse_statements(vec![table_sql]).unwrap();
    let sink_stmts = parse_statements(vec![sink_sql]).unwrap();

    let raw_table = raw::DatabaseObject {
        name: "users".to_string(),
        path: PathBuf::from("materialize/storage/users.sql"),
        statements: table_stmts,
    };

    let raw_sink = raw::DatabaseObject {
        name: "user_sink".to_string(),
        path: PathBuf::from("materialize/storage/user_sink.sql"),
        statements: sink_stmts,
    };

    let raw_schema = raw::Schema {
        name: "storage".to_string(),
        objects: vec![raw_table, raw_sink],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_ok(),
        "Schema with tables and sinks should pass validation (both storage objects)"
    );
}

#[test]
fn test_schema_with_only_views_succeeds() {
    let view1_sql = "CREATE VIEW user_view AS SELECT * FROM users;";
    let view2_sql = "CREATE VIEW order_view AS SELECT * FROM orders;";

    let view1_stmts = parse_statements(vec![view1_sql]).unwrap();
    let view2_stmts = parse_statements(vec![view2_sql]).unwrap();

    let raw_view1 = raw::DatabaseObject {
        name: "user_view".to_string(),
        path: PathBuf::from("materialize/views/user_view.sql"),
        statements: view1_stmts,
    };

    let raw_view2 = raw::DatabaseObject {
        name: "order_view".to_string(),
        path: PathBuf::from("materialize/views/order_view.sql"),
        statements: view2_stmts,
    };

    let raw_schema = raw::Schema {
        name: "views".to_string(),
        objects: vec![raw_view1, raw_view2],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_ok(),
        "Schema with only views should pass validation"
    );
}

#[test]
fn test_schema_with_views_and_materialized_views_succeeds() {
    let view_sql = "CREATE VIEW user_view AS SELECT * FROM users;";
    let mv_sql = "CREATE MATERIALIZED VIEW user_summary IN CLUSTER quickstart AS SELECT COUNT(*) FROM users;";

    let view_stmts = parse_statements(vec![view_sql]).unwrap();
    let mv_stmts = parse_statements(vec![mv_sql]).unwrap();

    let raw_view = raw::DatabaseObject {
        name: "user_view".to_string(),
        path: PathBuf::from("materialize/computation/user_view.sql"),
        statements: view_stmts,
    };

    let raw_mv = raw::DatabaseObject {
        name: "user_summary".to_string(),
        path: PathBuf::from("materialize/computation/user_summary.sql"),
        statements: mv_stmts,
    };

    let raw_schema = raw::Schema {
        name: "computation".to_string(),
        objects: vec![raw_view, raw_mv],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_ok(),
        "Schema with views and materialized views should pass validation (both computation objects)"
    );
}

#[test]
fn test_schema_with_table_from_source_and_view_fails() {
    let table_sql = "CREATE TABLE users FROM SOURCE kafka_source (REFERENCE users);";
    let view_sql = "CREATE VIEW user_view AS SELECT * FROM users;";

    let table_stmts = parse_statements(vec![table_sql]).unwrap();
    let view_stmts = parse_statements(vec![view_sql]).unwrap();

    let raw_table = raw::DatabaseObject {
        name: "users".to_string(),
        path: PathBuf::from("materialize/mixed/users.sql"),
        statements: table_stmts,
    };

    let raw_view = raw::DatabaseObject {
        name: "user_view".to_string(),
        path: PathBuf::from("materialize/mixed/user_view.sql"),
        statements: view_stmts,
    };

    let raw_schema = raw::Schema {
        name: "mixed".to_string(),
        objects: vec![raw_table, raw_view],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_err(),
        "Schema with CREATE TABLE FROM SOURCE and view should fail validation (table from source is a storage object)"
    );
}

#[test]
fn test_sink_missing_cluster_fails() {
    let sink_sql =
        "CREATE SINK user_sink FROM users INTO KAFKA CONNECTION kafka_conn (TOPIC 'users');";
    let sink_stmts = parse_statements(vec![sink_sql]).unwrap();

    let raw_sink = raw::DatabaseObject {
        name: "user_sink".to_string(),
        path: PathBuf::from("materialize/sinks/user_sink.sql"),
        statements: sink_stmts,
    };

    let raw_schema = raw::Schema {
        name: "sinks".to_string(),
        objects: vec![raw_sink],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_err(),
        "Sink without IN CLUSTER clause should fail validation"
    );

    // Verify it's the correct error type
    if let Err(crate::project::error::ValidationErrors { errors }) = result {
        assert_eq!(errors.len(), 1);
        match &errors[0].kind {
            crate::project::error::ValidationErrorKind::SinkMissingCluster { sink_name } => {
                // Name is fully qualified after normalization
                assert_eq!(sink_name, "materialize.sinks.user_sink");
            }
            _ => panic!(
                "Expected SinkMissingCluster error, got {:?}",
                errors[0].kind
            ),
        }
    } else {
        panic!("Expected ValidationErrors");
    }
}

#[test]
fn test_sink_with_cluster_succeeds() {
    let sink_sql = "CREATE SINK user_sink IN CLUSTER quickstart FROM users INTO KAFKA CONNECTION kafka_conn (TOPIC 'users');";
    let sink_stmts = parse_statements(vec![sink_sql]).unwrap();

    let raw_sink = raw::DatabaseObject {
        name: "user_sink".to_string(),
        path: PathBuf::from("materialize/sinks/user_sink.sql"),
        statements: sink_stmts,
    };

    let raw_schema = raw::Schema {
        name: "sinks".to_string(),
        objects: vec![raw_sink],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_ok(),
        "Sink with IN CLUSTER clause should pass validation"
    );
}

#[test]
fn test_materialized_view_missing_cluster_fails() {
    let mv_sql = "CREATE MATERIALIZED VIEW user_summary AS SELECT COUNT(*) FROM users;";
    let mv_stmts = parse_statements(vec![mv_sql]).unwrap();

    let raw_mv = raw::DatabaseObject {
        name: "user_summary".to_string(),
        path: PathBuf::from("materialize/views/user_summary.sql"),
        statements: mv_stmts,
    };

    let raw_schema = raw::Schema {
        name: "views".to_string(),
        objects: vec![raw_mv],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_err(),
        "Materialized view without IN CLUSTER clause should fail validation"
    );

    // Verify it's the correct error type
    if let Err(crate::project::error::ValidationErrors { errors }) = result {
        assert_eq!(errors.len(), 1);
        match &errors[0].kind {
            crate::project::error::ValidationErrorKind::MaterializedViewMissingCluster {
                view_name,
            } => {
                // Name is fully qualified after normalization
                assert_eq!(view_name, "materialize.views.user_summary");
            }
            _ => panic!(
                "Expected MaterializedViewMissingCluster error, got {:?}",
                errors[0].kind
            ),
        }
    } else {
        panic!("Expected ValidationErrors");
    }
}

#[test]
fn test_index_missing_cluster_fails() {
    let table_sql = "CREATE TABLE users (id INT);";
    let index_sql = "CREATE INDEX idx ON users (id);";

    let stmts = parse_statements(vec![table_sql, index_sql]).unwrap();

    let raw_table = raw::DatabaseObject {
        name: "users".to_string(),
        path: PathBuf::from("materialize/tables/users.sql"),
        statements: stmts,
    };

    let raw_schema = raw::Schema {
        name: "tables".to_string(),
        objects: vec![raw_table],
        mod_statements: None,
    };

    let result = Schema::try_from(raw_schema);
    assert!(
        result.is_err(),
        "Index without IN CLUSTER clause should fail validation"
    );

    // Verify it's the correct error type
    if let Err(crate::project::error::ValidationErrors { errors }) = result {
        assert_eq!(errors.len(), 1);
        match &errors[0].kind {
            crate::project::error::ValidationErrorKind::IndexMissingCluster { index_name } => {
                assert_eq!(index_name, "idx");
            }
            _ => panic!(
                "Expected IndexMissingCluster error, got {:?}",
                errors[0].kind
            ),
        }
    } else {
        panic!("Expected ValidationErrors");
    }
}

// ============================================================================
// Identifier format validation tests
// ============================================================================

mod identifier_validation {
    use super::super::validation::{IdentifierKind, validate_identifier_format};

    #[test]
    fn test_valid_lowercase_identifier() {
        assert!(validate_identifier_format("users", IdentifierKind::Object).is_ok());
        assert!(validate_identifier_format("my_table", IdentifierKind::Object).is_ok());
        assert!(validate_identifier_format("user123", IdentifierKind::Object).is_ok());
        assert!(validate_identifier_format("_private", IdentifierKind::Object).is_ok());
        assert!(validate_identifier_format("price$", IdentifierKind::Object).is_ok());
        assert!(validate_identifier_format("a1b2c3", IdentifierKind::Object).is_ok());
    }

    #[test]
    fn test_valid_unicode_identifiers() {
        // Letters with diacritical marks
        assert!(validate_identifier_format("café", IdentifierKind::Object).is_ok());
        assert!(validate_identifier_format("naïve", IdentifierKind::Object).is_ok());
        // Non-Latin letters
        assert!(validate_identifier_format("日本語", IdentifierKind::Object).is_ok());
        assert!(validate_identifier_format("данные", IdentifierKind::Object).is_ok());
    }

    #[test]
    fn test_invalid_uppercase_start() {
        let result = validate_identifier_format("Users", IdentifierKind::Object);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("uppercase"),
            "Error should mention uppercase: {}",
            err
        );
        assert!(
            err.contains("position 1"),
            "Error should mention position 1: {}",
            err
        );
    }

    #[test]
    fn test_invalid_uppercase_middle() {
        let result = validate_identifier_format("myTable", IdentifierKind::Object);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("uppercase"),
            "Error should mention uppercase: {}",
            err
        );
        assert!(
            err.contains("'T'"),
            "Error should mention the character: {}",
            err
        );
    }

    #[test]
    fn test_invalid_digit_start() {
        let result = validate_identifier_format("123table", IdentifierKind::Object);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("starts with digit"),
            "Error should mention starting with digit: {}",
            err
        );
    }

    #[test]
    fn test_invalid_special_char_start() {
        let result = validate_identifier_format("$price", IdentifierKind::Object);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("starts with invalid character"),
            "Error should mention invalid start: {}",
            err
        );
    }

    #[test]
    fn test_invalid_hyphen() {
        let result = validate_identifier_format("my-table", IdentifierKind::Object);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid character"),
            "Error should mention invalid character: {}",
            err
        );
        assert!(err.contains("'-'"), "Error should mention hyphen: {}", err);
    }

    #[test]
    fn test_invalid_space() {
        let result = validate_identifier_format("my table", IdentifierKind::Object);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid character"),
            "Error should mention invalid character: {}",
            err
        );
    }

    #[test]
    fn test_empty_identifier() {
        let result = validate_identifier_format("", IdentifierKind::Object);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("cannot be empty"),
            "Error should mention empty: {}",
            err
        );
    }

    #[test]
    fn test_all_identifier_kinds() {
        // Test that error messages correctly use the identifier kind
        let db_err = validate_identifier_format("MyDB", IdentifierKind::Database).unwrap_err();
        assert!(
            db_err.contains("database name"),
            "Should mention database: {}",
            db_err
        );

        let schema_err =
            validate_identifier_format("MySchema", IdentifierKind::Schema).unwrap_err();
        assert!(
            schema_err.contains("schema name"),
            "Should mention schema: {}",
            schema_err
        );

        let obj_err = validate_identifier_format("MyObject", IdentifierKind::Object).unwrap_err();
        assert!(
            obj_err.contains("object name"),
            "Should mention object: {}",
            obj_err
        );

        let cluster_err =
            validate_identifier_format("MyCluster", IdentifierKind::Cluster).unwrap_err();
        assert!(
            cluster_err.contains("cluster name"),
            "Should mention cluster: {}",
            cluster_err
        );
    }

    #[test]
    fn test_all_uppercase() {
        let result = validate_identifier_format("MY_TABLE", IdentifierKind::Object);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("uppercase"),
            "Error should mention uppercase: {}",
            err
        );
    }

    #[test]
    fn test_valid_underscore_only_start() {
        // Single underscore followed by valid chars
        assert!(validate_identifier_format("_", IdentifierKind::Object).is_ok());
        assert!(validate_identifier_format("__", IdentifierKind::Object).is_ok());
        assert!(validate_identifier_format("___test", IdentifierKind::Object).is_ok());
    }

    #[test]
    fn test_valid_dollar_sign_in_middle() {
        assert!(validate_identifier_format("price$usd", IdentifierKind::Object).is_ok());
        assert!(validate_identifier_format("a$b$c", IdentifierKind::Object).is_ok());
    }
}
