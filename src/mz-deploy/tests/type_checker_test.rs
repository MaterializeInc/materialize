//! Integration tests for TypeChecker trait and implementations

#[cfg(test)]
mod tests {
    use mz_deploy::types::{NoOpTypeChecker, TypeChecker};
    use mz_deploy::project;
    use tempfile::TempDir;
    use std::fs;

    #[tokio::test]
    async fn test_noop_typechecker_always_passes() {
        // Create a test project
        let temp_dir = TempDir::new().unwrap();
        let project_path = temp_dir.path();

        // Create project structure
        let materialize_dir = project_path.join("materialize");
        fs::create_dir(&materialize_dir).unwrap();
        let public_dir = materialize_dir.join("public");
        fs::create_dir(&public_dir).unwrap();

        // Create a simple SQL file
        let sql_content = r#"
CREATE VIEW test_view AS
SELECT 1 AS id, 'test' AS name;
"#;
        fs::write(public_dir.join("test_view.sql"), sql_content).unwrap();

        // Load the project
        let mir_project = project::plan(project_path).unwrap();

        // Test NoOpTypeChecker
        let checker = NoOpTypeChecker::new();
        let result = checker.typecheck(&mir_project).await;

        // NoOpTypeChecker should always pass
        assert!(result.is_ok());
    }

    #[cfg(feature = "docker-typecheck")]
    #[tokio::test]
    async fn test_docker_typechecker_with_external_dependencies() {
        use mz_deploy::types::{DockerTypeChecker, Types, ColumnType};
        use std::collections::BTreeMap;

        // Create a test project with external dependencies
        let temp_dir = TempDir::new().unwrap();
        let project_path = temp_dir.path();

        // Create project structure
        let materialize_dir = project_path.join("materialize");
        fs::create_dir(&materialize_dir).unwrap();
        let public_dir = materialize_dir.join("public");
        fs::create_dir(&public_dir).unwrap();

        // Create a view that references an external table
        let sql_content = r#"
CREATE VIEW user_summary AS
SELECT
    u.id,
    u.name,
    COUNT(*) AS total_orders
FROM external_db.external_schema.users u
JOIN external_db.external_schema.orders o ON u.id = o.user_id
GROUP BY u.id, u.name;
"#;
        fs::write(public_dir.join("user_summary.sql"), sql_content).unwrap();

        // Create types.lock with external dependencies
        let mut objects = BTreeMap::new();

        // Define external_db.external_schema.users
        let mut users_columns = BTreeMap::new();
        users_columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "INTEGER".to_string(),
                nullable: false,
            },
        );
        users_columns.insert(
            "name".to_string(),
            ColumnType {
                r#type: "TEXT".to_string(),
                nullable: false,
            },
        );
        objects.insert("external_db.external_schema.users".to_string(), users_columns);

        // Define external_db.external_schema.orders
        let mut orders_columns = BTreeMap::new();
        orders_columns.insert(
            "id".to_string(),
            ColumnType {
                r#type: "INTEGER".to_string(),
                nullable: false,
            },
        );
        orders_columns.insert(
            "user_id".to_string(),
            ColumnType {
                r#type: "INTEGER".to_string(),
                nullable: false,
            },
        );
        objects.insert("external_db.external_schema.orders".to_string(), orders_columns);

        let types = Types {
            version: 1,
            objects,
        };

        // Write types.lock
        types.write_types_lock(project_path).unwrap();

        // Load the project
        let mir_project = project::plan(project_path).unwrap();

        // Test DockerTypeChecker (will attempt to start container)
        let checker = DockerTypeChecker::new(types, project_path);
        let result = checker.typecheck(&mir_project).await;

        // This might fail if Docker is not available, which is expected
        match result {
            Ok(_) => {
                println!("Type checking passed with Docker");
            }
            Err(e) => {
                println!("Type checking with Docker failed (expected if Docker not available): {}", e);
                // If Docker is not available, this is expected behavior
                if e.to_string().contains("container startup") ||
                   e.to_string().contains("Docker daemon") {
                    // This is expected when Docker is not available
                    return;
                }
                // Re-throw other errors
                panic!("Unexpected error: {}", e);
            }
        }
    }
}