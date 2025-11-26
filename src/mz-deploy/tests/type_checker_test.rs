//! Integration tests for TypeChecker trait and implementations

#[cfg(test)]
mod tests {
    use mz_deploy::project;
    use mz_deploy::types::TypeChecker;
    use std::fs;
    use tempfile::TempDir;

    #[cfg(feature = "docker-typecheck")]
    #[tokio::test]
    async fn test_docker_typechecker_with_external_dependencies() {
        use mz_deploy::types::{ColumnType, Types, typecheck_with_client};
        use mz_deploy::utils::docker_runtime::DockerRuntime;
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
        objects.insert(
            "external_db.external_schema.users".to_string(),
            users_columns,
        );

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
        objects.insert(
            "external_db.external_schema.orders".to_string(),
            orders_columns,
        );

        let types = Types {
            version: 1,
            objects,
        };

        // Write types.lock
        types.write_types_lock(project_path).unwrap();

        // Load the project
        let mir_project = project::plan(project_path).unwrap();

        // Create Docker runtime and get client
        let runtime = DockerRuntime::new();
        let mut client = match runtime.get_client(&mir_project, &types).await {
            Ok(client) => client,
            Err(e) => {
                println!(
                    "Docker not available for testing (expected if Docker not installed): {}",
                    e
                );
                // If Docker is not available, this is expected behavior
                if e.to_string().contains("container")
                    || e.to_string().contains("Docker")
                    || e.to_string().contains("docker")
                {
                    // This is expected when Docker is not available
                    return;
                }
                // Re-throw other errors
                panic!("Unexpected error: {}", e);
            }
        };

        // Run type checking with the client
        let result = typecheck_with_client(&mut client, &mir_project, project_path).await;

        // This might fail if there are actual type errors, which is unexpected
        match result {
            Ok(_) => {
                println!("Type checking passed with Docker");
            }
            Err(e) => {
                panic!("Type checking failed: {}", e);
            }
        }
    }
}
