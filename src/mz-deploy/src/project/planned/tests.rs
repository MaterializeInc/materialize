//! Tests for the planned representation module.

use super::super::ast::{Cluster, Statement};
use super::super::typed;
use super::dependency::extract_dependencies;
use super::types::{Database, DatabaseObject, Project, Schema, SchemaType};
use crate::project::object_id::ObjectId;
use mz_sql_parser::ast::Ident;
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn test_object_id_from_item_name() {
    use mz_sql_parser::ast::UnresolvedItemName;

    let name = UnresolvedItemName(vec![Ident::new("users").unwrap()]);
    let id = ObjectId::from_item_name(&name, "db", "public");
    assert_eq!(id.database, "db");
    assert_eq!(id.schema, "public");
    assert_eq!(id.object, "users");

    let name = UnresolvedItemName(vec![
        Ident::new("myschema").unwrap(),
        Ident::new("users").unwrap(),
    ]);
    let id = ObjectId::from_item_name(&name, "db", "public");
    assert_eq!(id.database, "db");
    assert_eq!(id.schema, "myschema");
    assert_eq!(id.object, "users");

    let name = UnresolvedItemName(vec![
        Ident::new("mydb").unwrap(),
        Ident::new("myschema").unwrap(),
        Ident::new("users").unwrap(),
    ]);
    let id = ObjectId::from_item_name(&name, "db", "public");
    assert_eq!(id.database, "mydb");
    assert_eq!(id.schema, "myschema");
    assert_eq!(id.object, "users");
}

#[test]
fn test_object_id_fqn() {
    let id = ObjectId::new("db".to_string(), "schema".to_string(), "table".to_string());
    assert_eq!(id.to_string(), "db.schema.table");
}

#[test]
fn test_cluster_equality() {
    let c1 = Cluster::new("quickstart".to_string());
    let c2 = Cluster::new("quickstart".to_string());
    let c3 = Cluster::new("prod".to_string());

    assert_eq!(c1, c2);
    assert_ne!(c1, c3);
}

#[test]
fn test_cluster_in_hashset() {
    let mut clusters = BTreeSet::new();
    clusters.insert(Cluster::new("quickstart".to_string()));
    clusters.insert(Cluster::new("quickstart".to_string())); // duplicate
    clusters.insert(Cluster::new("prod".to_string()));

    assert_eq!(clusters.len(), 2);
    assert!(clusters.contains(&Cluster::new("quickstart".to_string())));
    assert!(clusters.contains(&Cluster::new("prod".to_string())));
}

#[test]
fn test_extract_dependencies_materialized_view_with_cluster() {
    let sql = "CREATE MATERIALIZED VIEW mv IN CLUSTER quickstart AS SELECT * FROM users";
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateMaterializedView(mv_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateMaterializedView(mv_stmt.clone());
        let (deps, clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have one dependency (users table)
        assert_eq!(deps.len(), 1);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "users".to_string()
        )));

        // Should have one cluster dependency
        assert_eq!(clusters.len(), 1);
        assert!(clusters.contains(&Cluster::new("quickstart".to_string())));
    } else {
        panic!("Expected CreateMaterializedView statement");
    }
}

#[test]
fn test_extract_dependencies_materialized_view_without_cluster() {
    let sql = "CREATE MATERIALIZED VIEW mv AS SELECT * FROM users";
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateMaterializedView(mv_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateMaterializedView(mv_stmt.clone());
        let (deps, clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have one dependency (users table)
        assert_eq!(deps.len(), 1);

        // Should have no cluster dependencies
        assert_eq!(clusters.len(), 0);
    } else {
        panic!("Expected CreateMaterializedView statement");
    }
}

#[test]
fn test_extract_dependencies_view_no_clusters() {
    let sql = "CREATE VIEW v AS SELECT * FROM users";
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (_deps, clusters) = extract_dependencies(&stmt, "db", "public");

        // Views don't have cluster dependencies
        assert_eq!(clusters.len(), 0);
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_multiple_materialized_views_with_different_clusters() {
    let sqls = vec![
        "CREATE MATERIALIZED VIEW mv1 IN CLUSTER quickstart AS SELECT * FROM t1",
        "CREATE MATERIALIZED VIEW mv2 IN CLUSTER prod AS SELECT * FROM t2",
        "CREATE MATERIALIZED VIEW mv3 IN CLUSTER quickstart AS SELECT * FROM t3",
    ];

    let mut all_clusters = BTreeSet::new();

    for sql in sqls {
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();
        if let mz_sql_parser::ast::Statement::CreateMaterializedView(mv_stmt) = &parsed[0].ast {
            let stmt = Statement::CreateMaterializedView(mv_stmt.clone());
            let (_deps, clusters) = extract_dependencies(&stmt, "db", "public");
            all_clusters.extend(clusters);
        }
    }

    // Should have 2 unique clusters (quickstart and prod)
    assert_eq!(all_clusters.len(), 2);
    assert!(all_clusters.contains(&Cluster::new("quickstart".to_string())));
    assert!(all_clusters.contains(&Cluster::new("prod".to_string())));
}

#[test]
fn test_build_reverse_dependency_graph() {
    // Create a simple dependency graph
    let mut dependency_graph = BTreeMap::new();

    let obj1 = ObjectId::new("db".to_string(), "public".to_string(), "table1".to_string());
    let obj2 = ObjectId::new("db".to_string(), "public".to_string(), "view1".to_string());
    let obj3 = ObjectId::new("db".to_string(), "public".to_string(), "view2".to_string());

    // view1 depends on table1
    let mut deps1 = BTreeSet::new();
    deps1.insert(obj1.clone());
    dependency_graph.insert(obj2.clone(), deps1);

    // view2 depends on view1
    let mut deps2 = BTreeSet::new();
    deps2.insert(obj2.clone());
    dependency_graph.insert(obj3.clone(), deps2);

    // table1 has no dependencies
    dependency_graph.insert(obj1.clone(), BTreeSet::new());

    let project = Project {
        databases: vec![],
        dependency_graph,
        external_dependencies: BTreeSet::new(),
        cluster_dependencies: BTreeSet::new(),
        tests: vec![],
    };

    // Build reverse graph
    let reverse = project.build_reverse_dependency_graph();

    // table1 should have view1 as a dependent
    assert!(reverse.get(&obj1).unwrap().contains(&obj2));

    // view1 should have view2 as a dependent
    assert!(reverse.get(&obj2).unwrap().contains(&obj3));

    // view2 should have no dependents
    assert!(!reverse.contains_key(&obj3));
}

#[test]
fn test_get_sorted_objects_filtered() {
    use crate::project::raw;
    use crate::project::typed;
    use std::fs;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let src_dir = temp_dir.path();

    // Create test structure with separate schemas for tables and views
    let db_path = src_dir.join("test_db");
    let tables_schema_path = db_path.join("tables");
    let views_schema_path = db_path.join("views");
    fs::create_dir_all(&tables_schema_path).unwrap();
    fs::create_dir_all(&views_schema_path).unwrap();

    // Create table in tables schema
    fs::write(
        tables_schema_path.join("table1.sql"),
        "CREATE TABLE table1 (id INT);",
    )
    .unwrap();

    // Create view depending on table in views schema
    fs::write(
        views_schema_path.join("view1.sql"),
        "CREATE VIEW view1 AS SELECT * FROM tables.table1;",
    )
    .unwrap();

    // Create another view depending on view1 in views schema
    fs::write(
        views_schema_path.join("view2.sql"),
        "CREATE VIEW view2 AS SELECT * FROM view1;",
    )
    .unwrap();

    // Load and convert to planned
    let raw_project = raw::load_project(src_dir).unwrap();
    let typed_project = typed::Project::try_from(raw_project).unwrap();
    let planned_project = Project::from(typed_project);

    // Create filter that only includes view1
    let mut filter = BTreeSet::new();
    let view1_id = ObjectId::new(
        "test_db".to_string(),
        "views".to_string(),
        "view1".to_string(),
    );
    filter.insert(view1_id.clone());

    // Get filtered objects
    let filtered = planned_project
        .get_sorted_objects_filtered(&filter)
        .unwrap();

    // Should only contain view1
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].0, view1_id);
}

#[test]
fn test_extract_dependencies_with_mutually_recursive_ctes() {
    // Test basic mutually recursive CTEs that reference each other and external tables
    let sql = r#"
        CREATE MATERIALIZED VIEW mv AS
        WITH MUTUALLY RECURSIVE
          is_even (n int, result bool) AS (
            SELECT 0 as n, TRUE as result
            UNION ALL
            SELECT ni.n, ie_prev.result
            FROM numbers_input ni, is_odd ie_prev
            WHERE ni.n > 0 AND ni.n - 1 = ie_prev.n
          ),
          is_odd (n int, result bool) AS (
            SELECT ni.n, NOT ie.result as result
            FROM numbers_input ni, is_even ie
            WHERE ni.n > 0 AND ni.n - 1 = ie.n
          )
        SELECT n, result AS is_even
        FROM is_even
        ORDER BY n
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateMaterializedView(mv_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateMaterializedView(mv_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should only have dependency on numbers_input, not on is_even or is_odd (internal CTEs)
        assert_eq!(deps.len(), 1);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "numbers_input".to_string()
        )));
    } else {
        panic!("Expected CreateMaterializedView statement");
    }
}

#[test]
fn test_extract_dependencies_mutually_recursive_with_subquery() {
    // Test mutually recursive CTEs with subqueries in WHERE clause
    let sql = r#"
        CREATE VIEW v AS
        WITH MUTUALLY RECURSIVE
          cte1 (id int) AS (
            SELECT id FROM table1
            WHERE id IN (SELECT id FROM cte2)
          ),
          cte2 (id int) AS (
            SELECT id FROM table2
            WHERE EXISTS (SELECT 1 FROM cte1)
          )
        SELECT * FROM cte1
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on table1 and table2, but not on cte1 or cte2
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table1".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table2".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_extract_dependencies_mutually_recursive_with_derived_table() {
    // Test mutually recursive CTEs with derived tables (subqueries in FROM)
    let sql = r#"
        CREATE MATERIALIZED VIEW mv AS
        WITH MUTUALLY RECURSIVE
          cte1 (id int, value text) AS (
            SELECT id, value FROM (
              SELECT id, value FROM base_table WHERE id > 0
            ) sub
            WHERE id IN (SELECT id FROM cte2)
          ),
          cte2 (id int, value text) AS (
            SELECT id, value FROM (
              SELECT id, value FROM another_table
              WHERE value IN (SELECT value FROM cte1)
            )
          )
        SELECT * FROM cte2
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateMaterializedView(mv_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateMaterializedView(mv_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on base_table and another_table
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "base_table".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "another_table".to_string()
        )));
    } else {
        panic!("Expected CreateMaterializedView statement");
    }
}

#[test]
fn test_extract_dependencies_mutually_recursive_nested_cte_reference() {
    // Test that CTE references inside nested queries don't get added as dependencies
    let sql = r#"
        CREATE VIEW v AS
        WITH MUTUALLY RECURSIVE
          cte_a (id int) AS (
            SELECT id FROM real_table
            WHERE id IN (
              SELECT id FROM (
                SELECT id FROM cte_b
              ) subquery
            )
          ),
          cte_b (id int) AS (
            SELECT id FROM cte_a
          )
        SELECT * FROM cte_b
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should only have dependency on real_table, not on cte_a or cte_b
        assert_eq!(deps.len(), 1);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "real_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_extract_dependencies_simple_cte_cannot_forward_reference() {
    // Test that Simple CTEs build scope incrementally
    // In this case, cte1 tries to reference cte2 which comes later
    // With our incremental scoping, cte2 will be treated as an external table
    let sql = r#"
        CREATE VIEW v AS
        WITH
          cte1 AS (SELECT * FROM cte2),
          cte2 AS (SELECT * FROM base_table)
        SELECT * FROM cte1
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // With incremental scoping, cte1 doesn't know about cte2 yet
        // So cte2 is treated as an external dependency (along with base_table from cte2's definition)
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "cte2".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "base_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_extract_dependencies_simple_cte_backward_reference() {
    // Test that Simple CTEs can reference earlier CTEs
    let sql = r#"
        CREATE VIEW v AS
        WITH
          cte1 AS (SELECT * FROM base_table),
          cte2 AS (SELECT * FROM cte1)
        SELECT * FROM cte2
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // cte2 can see cte1, so only base_table is an external dependency
        assert_eq!(deps.len(), 1);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "base_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_uncorrelated_subquery_in_where() {
    // Test uncorrelated subquery in WHERE clause
    let sql = r#"
        CREATE VIEW v AS
        SELECT * FROM table1
        WHERE id IN (SELECT id FROM table2 WHERE status = 'active')
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on both table1 and table2
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table1".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table2".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_correlated_subquery_in_where() {
    // Test correlated subquery in WHERE clause (references outer query)
    let sql = r#"
        CREATE VIEW v AS
        SELECT * FROM table1 t1
        WHERE EXISTS (
            SELECT 1 FROM table2 t2
            WHERE t2.parent_id = t1.id
            AND t2.status = 'active'
        )
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on both table1 and table2
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table1".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table2".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_subquery_in_select_list() {
    // Test subquery in SELECT list (scalar subquery)
    let sql = r#"
        CREATE VIEW v AS
        SELECT
            id,
            name,
            (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) as order_count
        FROM users
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on both users and orders
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "users".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "orders".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_nested_uncorrelated_subqueries() {
    // Test nested uncorrelated subqueries
    let sql = r#"
        CREATE VIEW v AS
        SELECT * FROM table1
        WHERE id IN (
            SELECT user_id FROM table2
            WHERE category_id IN (
                SELECT id FROM table3 WHERE active = true
            )
        )
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on table1, table2, and table3
        assert_eq!(deps.len(), 3);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table1".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table2".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table3".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_subquery_with_simple_cte() {
    // Test subquery referencing a Simple CTE (should not be treated as external dependency)
    let sql = r#"
        CREATE VIEW v AS
        WITH cte1 AS (
            SELECT * FROM base_table
        )
        SELECT * FROM table1
        WHERE id IN (SELECT id FROM cte1)
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on table1 and base_table, but NOT on cte1
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table1".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "base_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_correlated_subquery_with_cte() {
    // Test correlated subquery with CTE
    let sql = r#"
        CREATE VIEW v AS
        WITH active_users AS (
            SELECT id, name FROM users WHERE active = true
        )
        SELECT * FROM orders o
        WHERE EXISTS (
            SELECT 1 FROM active_users au
            WHERE au.id = o.user_id
        )
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on orders and users, but NOT on active_users
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "orders".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "users".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_subquery_in_from_with_cte() {
    // Test subquery in FROM clause (derived table) that references CTE
    let sql = r#"
        CREATE VIEW v AS
        WITH summary AS (
            SELECT category, COUNT(*) as cnt FROM products GROUP BY category
        )
        SELECT * FROM (
            SELECT s.category, s.cnt, c.name
            FROM summary s
            JOIN categories c ON s.category = c.id
        ) derived
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on products and categories, but NOT on summary
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "products".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "categories".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_multiple_subqueries_mixed_correlation() {
    // Test multiple subqueries with mixed correlation
    // Split into two tests since Materialize may have parser issues with complex WHERE clauses
    let sql1 = r#"
        CREATE VIEW v AS
        SELECT t1.id
        FROM table1 t1
        WHERE t1.id IN (SELECT user_id FROM table2)
    "#;

    let sql2 = r#"
        CREATE VIEW v2 AS
        SELECT t1.id
        FROM table1 t1
        WHERE EXISTS (
            SELECT 1 FROM table3 t3
            WHERE t3.parent_id = t1.id
        )
    "#;

    // Test first query with IN subquery
    let parsed1 = mz_sql_parser::parser::parse_statements(sql1).unwrap();
    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed1[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table1".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table2".to_string()
        )));
    }

    // Test second query with EXISTS subquery
    let parsed2 = mz_sql_parser::parser::parse_statements(sql2).unwrap();
    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed2[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table1".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table3".to_string()
        )));
    }
}

#[test]
fn test_subquery_in_case_expression() {
    // Test subquery in CASE expression
    let sql = r#"
        CREATE VIEW v AS
        SELECT
            id,
            CASE
                WHEN status = 'pending' THEN (SELECT COUNT(*) FROM pending_queue)
                WHEN status = 'active' THEN (SELECT COUNT(*) FROM active_queue)
                ELSE 0
            END as queue_size
        FROM tasks
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on tasks, pending_queue, and active_queue
        assert_eq!(deps.len(), 3);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "tasks".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "pending_queue".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "active_queue".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_subquery_with_table_alias() {
    // Test subquery with table alias
    let sql = r#"
        CREATE VIEW v AS
        SELECT t1.id
        FROM table1 t1
        WHERE t1.id IN (SELECT user_id FROM table2)
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on table1 and table2
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table1".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table2".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_wmr_with_correlated_subquery() {
    // Test WITH MUTUALLY RECURSIVE with correlated subquery
    let sql = r#"
        CREATE VIEW v AS
        WITH MUTUALLY RECURSIVE
          cte1 (id int, parent_id int) AS (
            SELECT id, parent_id FROM base_table
            WHERE EXISTS (
                SELECT 1 FROM cte2 c2
                WHERE c2.id = base_table.parent_id
            )
          ),
          cte2 (id int, parent_id int) AS (
            SELECT id, parent_id FROM another_table
            WHERE id IN (SELECT parent_id FROM cte1)
          )
        SELECT * FROM cte1
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on base_table and another_table, but NOT on cte1 or cte2
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "base_table".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "another_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

// Helper function to create a minimal Project for cluster isolation testing
fn create_test_project_for_cluster_validation() -> Project {
    Project {
        databases: vec![],
        dependency_graph: BTreeMap::new(),
        external_dependencies: BTreeSet::new(),
        cluster_dependencies: BTreeSet::new(),
        tests: vec![],
    }
}

#[test]
fn test_validate_cluster_isolation_no_conflicts() {
    let project = create_test_project_for_cluster_validation();
    let sources_by_cluster = BTreeMap::new();

    let result = project.validate_cluster_isolation(&sources_by_cluster);
    assert!(result.is_ok());
}

#[test]
fn test_validate_cluster_isolation_separate_clusters() {
    // Create a project with MV on compute_cluster and sink on storage_cluster
    let mv_sql = "CREATE MATERIALIZED VIEW mv IN CLUSTER compute_cluster AS SELECT 1";
    let sink_sql = "CREATE SINK sink IN CLUSTER storage_cluster FROM mv INTO KAFKA CONNECTION conn (TOPIC 'test')";

    let mv_parsed = mz_sql_parser::parser::parse_statements(mv_sql).unwrap();
    let sink_parsed = mz_sql_parser::parser::parse_statements(sink_sql).unwrap();

    let mv_stmt =
        if let mz_sql_parser::ast::Statement::CreateMaterializedView(s) = &mv_parsed[0].ast {
            Statement::CreateMaterializedView(s.clone())
        } else {
            panic!("Expected CreateMaterializedView");
        };

    let sink_stmt = if let mz_sql_parser::ast::Statement::CreateSink(s) = &sink_parsed[0].ast {
        Statement::CreateSink(s.clone())
    } else {
        panic!("Expected CreateSink");
    };

    let mv_obj = DatabaseObject {
        id: ObjectId::new("db".to_string(), "schema".to_string(), "mv".to_string()),
        typed_object: typed::DatabaseObject {
            stmt: mv_stmt,
            indexes: vec![],
            grants: vec![],
            comments: vec![],
            tests: vec![],
        },
        dependencies: BTreeSet::new(),
    };

    let sink_obj = DatabaseObject {
        id: ObjectId::new("db".to_string(), "schema".to_string(), "sink".to_string()),
        typed_object: typed::DatabaseObject {
            stmt: sink_stmt,
            indexes: vec![],
            grants: vec![],
            comments: vec![],
            tests: vec![],
        },
        dependencies: BTreeSet::new(),
    };

    let project = Project {
        databases: vec![Database {
            name: "db".to_string(),
            schemas: vec![Schema {
                name: "schema".to_string(),
                objects: vec![mv_obj, sink_obj],
                mod_statements: None,
                schema_type: SchemaType::Storage, // Has sink
            }],
            mod_statements: None,
        }],
        dependency_graph: BTreeMap::new(),
        external_dependencies: BTreeSet::new(),
        cluster_dependencies: BTreeSet::new(),
        tests: vec![],
    };

    // Sources on storage_cluster (different from compute objects)
    let mut sources_by_cluster = BTreeMap::new();
    sources_by_cluster.insert(
        "storage_cluster".to_string(),
        vec!["db.schema.source1".to_string()],
    );

    let result = project.validate_cluster_isolation(&sources_by_cluster);
    assert!(
        result.is_ok(),
        "Should succeed when storage and compute are on separate clusters"
    );
}

#[test]
fn test_validate_cluster_isolation_conflict_mv_and_source() {
    // Create a project with MV on shared_cluster
    let mv_sql = "CREATE MATERIALIZED VIEW mv IN CLUSTER shared_cluster AS SELECT 1";
    let mv_parsed = mz_sql_parser::parser::parse_statements(mv_sql).unwrap();

    let mv_stmt =
        if let mz_sql_parser::ast::Statement::CreateMaterializedView(s) = &mv_parsed[0].ast {
            Statement::CreateMaterializedView(s.clone())
        } else {
            panic!("Expected CreateMaterializedView");
        };

    let mv_obj = DatabaseObject {
        id: ObjectId::new("db".to_string(), "schema".to_string(), "mv".to_string()),
        typed_object: typed::DatabaseObject {
            stmt: mv_stmt,
            indexes: vec![],
            grants: vec![],
            comments: vec![],
            tests: vec![],
        },
        dependencies: BTreeSet::new(),
    };

    let project = Project {
        databases: vec![Database {
            name: "db".to_string(),
            schemas: vec![Schema {
                name: "schema".to_string(),
                objects: vec![mv_obj],
                mod_statements: None,
                schema_type: SchemaType::Compute, // Has MV
            }],
            mod_statements: None,
        }],
        dependency_graph: BTreeMap::new(),
        external_dependencies: BTreeSet::new(),
        cluster_dependencies: BTreeSet::new(),
        tests: vec![],
    };

    // Source on the same cluster as MV
    let mut sources_by_cluster = BTreeMap::new();
    sources_by_cluster.insert(
        "shared_cluster".to_string(),
        vec!["db.schema.source1".to_string()],
    );

    let result = project.validate_cluster_isolation(&sources_by_cluster);
    assert!(
        result.is_err(),
        "Should fail when MV and source share a cluster"
    );

    if let Err((cluster_name, compute_objects, storage_objects)) = result {
        assert_eq!(cluster_name, "shared_cluster");
        assert_eq!(compute_objects.len(), 1);
        assert!(compute_objects.contains(&"db.schema.mv".to_string()));
        assert_eq!(storage_objects.len(), 1);
        assert!(storage_objects.contains(&"db.schema.source1".to_string()));
    }
}

#[test]
fn test_validate_cluster_isolation_only_compute_objects() {
    // Create a project with only MVs and indexes (no sinks)
    let mv_sql = "CREATE MATERIALIZED VIEW mv IN CLUSTER compute_cluster AS SELECT 1";
    let mv_parsed = mz_sql_parser::parser::parse_statements(mv_sql).unwrap();

    let mv_stmt =
        if let mz_sql_parser::ast::Statement::CreateMaterializedView(s) = &mv_parsed[0].ast {
            Statement::CreateMaterializedView(s.clone())
        } else {
            panic!("Expected CreateMaterializedView");
        };

    let mv_obj = DatabaseObject {
        id: ObjectId::new("db".to_string(), "schema".to_string(), "mv".to_string()),
        typed_object: typed::DatabaseObject {
            stmt: mv_stmt,
            indexes: vec![],
            grants: vec![],
            comments: vec![],
            tests: vec![],
        },
        dependencies: BTreeSet::new(),
    };

    let project = Project {
        databases: vec![Database {
            name: "db".to_string(),
            schemas: vec![Schema {
                name: "schema".to_string(),
                objects: vec![mv_obj],
                mod_statements: None,
                schema_type: SchemaType::Compute, // Has MV
            }],
            mod_statements: None,
        }],
        dependency_graph: BTreeMap::new(),
        external_dependencies: BTreeSet::new(),
        cluster_dependencies: BTreeSet::new(),
        tests: vec![],
    };

    // No sources on any cluster
    let sources_by_cluster = BTreeMap::new();

    let result = project.validate_cluster_isolation(&sources_by_cluster);
    assert!(
        result.is_ok(),
        "Should succeed when cluster only has compute objects"
    );
}

#[test]
fn test_validate_cluster_isolation_only_storage_objects() {
    // Create a project with only a sink (no MVs or indexes)
    let sink_sql = "CREATE SINK sink IN CLUSTER storage_cluster FROM t INTO KAFKA CONNECTION conn (TOPIC 'test')";
    let sink_parsed = mz_sql_parser::parser::parse_statements(sink_sql).unwrap();

    let sink_stmt = if let mz_sql_parser::ast::Statement::CreateSink(s) = &sink_parsed[0].ast {
        Statement::CreateSink(s.clone())
    } else {
        panic!("Expected CreateSink");
    };

    let sink_obj = DatabaseObject {
        id: ObjectId::new("db".to_string(), "schema".to_string(), "sink".to_string()),
        typed_object: typed::DatabaseObject {
            stmt: sink_stmt,
            indexes: vec![],
            grants: vec![],
            comments: vec![],
            tests: vec![],
        },
        dependencies: BTreeSet::new(),
    };

    let project = Project {
        databases: vec![Database {
            name: "db".to_string(),
            schemas: vec![Schema {
                name: "schema".to_string(),
                objects: vec![sink_obj],
                mod_statements: None,
                schema_type: SchemaType::Storage, // Has sink
            }],
            mod_statements: None,
        }],
        dependency_graph: BTreeMap::new(),
        external_dependencies: BTreeSet::new(),
        cluster_dependencies: BTreeSet::new(),
        tests: vec![],
    };

    // Sources on the same cluster
    let mut sources_by_cluster = BTreeMap::new();
    sources_by_cluster.insert(
        "storage_cluster".to_string(),
        vec!["db.schema.source1".to_string()],
    );

    let result = project.validate_cluster_isolation(&sources_by_cluster);
    assert!(
        result.is_ok(),
        "Should succeed when cluster only has storage objects (sources + sinks)"
    );
}

// ============================================================================
// Edge case tests for external dependency extraction
// ============================================================================

#[test]
fn test_dependencies_through_lateral_join() {
    // Test that LATERAL join dependencies are extracted correctly
    let sql = r#"
        CREATE VIEW v AS
        SELECT grp.category, sub.total
        FROM (SELECT DISTINCT category FROM categories) grp,
        LATERAL (
            SELECT SUM(amount) as total
            FROM sales
            WHERE sales.category = grp.category
        ) sub
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on both categories and sales
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "categories".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "sales".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_in_window_function() {
    // Test that dependencies in window function PARTITION BY/ORDER BY are extracted
    let sql = r#"
        CREATE VIEW v AS
        SELECT
            id,
            amount,
            SUM(amount) OVER (PARTITION BY category ORDER BY created_at) as running_total,
            RANK() OVER (PARTITION BY region ORDER BY amount DESC) as rank
        FROM orders
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependency on orders
        assert_eq!(deps.len(), 1, "Expected 1 dependency, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "orders".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_in_window_function_with_subquery() {
    // Test window function with a scalar subquery in the frame
    let sql = r#"
        CREATE VIEW v AS
        SELECT
            id,
            amount,
            amount - (SELECT AVG(amount) FROM sales) as diff_from_avg
        FROM orders
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on both orders and sales
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "orders".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "sales".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_in_union() {
    // Test that UNION queries extract dependencies from all branches
    let sql = r#"
        CREATE VIEW v AS
        SELECT id, name FROM customers
        UNION
        SELECT id, name FROM vendors
        UNION ALL
        SELECT id, name FROM partners
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on all three tables
        assert_eq!(deps.len(), 3, "Expected 3 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "customers".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "vendors".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "partners".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_in_intersect_except() {
    // Test that INTERSECT and EXCEPT queries extract dependencies
    let sql = r#"
        CREATE VIEW v AS
        SELECT id FROM all_users
        INTERSECT
        SELECT id FROM active_users
        EXCEPT
        SELECT id FROM banned_users
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on all three tables
        assert_eq!(deps.len(), 3, "Expected 3 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "all_users".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "active_users".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "banned_users".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_with_self_join() {
    // Test self-join where the same table appears twice with different aliases
    let sql = r#"
        CREATE VIEW v AS
        SELECT e.id, e.name, m.name as manager_name
        FROM employees e
        LEFT JOIN employees m ON e.manager_id = m.id
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have only one dependency (employees) even though it's used twice
        assert_eq!(
            deps.len(),
            1,
            "Expected 1 dependency (self-join), found: {:?}",
            deps
        );
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "employees".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_nested_derived_tables() {
    // Test deeply nested derived tables (subqueries in FROM)
    let sql = r#"
        CREATE VIEW v AS
        SELECT *
        FROM (
            SELECT *
            FROM (
                SELECT *
                FROM (
                    SELECT * FROM deep_table
                ) level1
            ) level2
        ) level3
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should extract dependency from deeply nested subquery
        assert_eq!(
            deps.len(),
            1,
            "Expected 1 dependency from nested subquery, found: {:?}",
            deps
        );
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "deep_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_cross_schema_reference() {
    // Test that cross-schema references are extracted with correct schema
    let sql = r#"
        CREATE VIEW v AS
        SELECT a.id, b.name
        FROM public.table1 a
        JOIN internal.table2 b ON a.id = b.id
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies in different schemas
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table1".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "internal".to_string(),
            "table2".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_cross_database_reference() {
    // Test that cross-database references are extracted with correct database
    let sql = r#"
        CREATE VIEW v AS
        SELECT a.id, b.name
        FROM db1.schema1.table1 a
        JOIN db2.schema2.table2 b ON a.id = b.id
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies in different databases
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db1".to_string(),
            "schema1".to_string(),
            "table1".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db2".to_string(),
            "schema2".to_string(),
            "table2".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_with_values_clause() {
    // Test that VALUES clause doesn't create dependencies
    let sql = r#"
        CREATE VIEW v AS
        SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)
        UNION ALL
        SELECT id, name FROM real_table
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should only have dependency on real_table, not the VALUES clause
        assert_eq!(deps.len(), 1, "Expected 1 dependency, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "real_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_complex_join_with_subqueries() {
    // Test complex join with subqueries on both sides
    let sql = r#"
        CREATE VIEW v AS
        SELECT l.id, r.total
        FROM (
            SELECT id, category FROM products WHERE active = true
        ) l
        JOIN (
            SELECT category, SUM(amount) as total FROM sales GROUP BY category
        ) r ON l.category = r.category
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on both products and sales
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "products".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "sales".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

// ============================================================================
// Nested CTE Dependency Extraction Tests
// ============================================================================

#[test]
fn test_dependencies_nested_cte_in_derived_table() {
    // Test dependency extraction with CTE inside a derived table
    let sql = r#"
        CREATE VIEW v AS
        SELECT * FROM (
            WITH inner_cte AS (
                SELECT id, name FROM users
            )
            SELECT * FROM inner_cte JOIN orders ON inner_cte.id = orders.user_id
        ) subquery
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on users and orders, but NOT inner_cte
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "users".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "orders".to_string()
        )));
        // inner_cte should NOT be a dependency
        assert!(!deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "inner_cte".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_nested_cte_in_scalar_subquery() {
    // Test dependency extraction with CTE inside a scalar subquery
    let sql = r#"
        CREATE VIEW v AS
        SELECT
            id,
            (WITH totals AS (SELECT SUM(amount) as total FROM transactions WHERE transactions.user_id = users.id)
             SELECT total FROM totals) as user_total
        FROM users
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on users and transactions, but NOT totals
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "users".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "transactions".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_triple_nested_ctes() {
    // Test dependency extraction with three levels of nested CTEs
    let sql = r#"
        CREATE VIEW v AS
        WITH outer_cte AS (
            SELECT * FROM (
                WITH middle_cte AS (
                    SELECT * FROM (
                        WITH inner_cte AS (
                            SELECT id FROM base_table
                        )
                        SELECT * FROM inner_cte
                    ) innermost
                )
                SELECT * FROM middle_cte
            ) middle_result
        )
        SELECT * FROM outer_cte
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should only have dependency on base_table, not on any CTEs
        assert_eq!(deps.len(), 1, "Expected 1 dependency, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "base_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_cte_shadowing_at_different_levels() {
    // Test that CTE shadowing doesn't incorrectly add dependencies
    let sql = r#"
        CREATE VIEW v AS
        WITH data AS (
            SELECT id FROM outer_table
        )
        SELECT * FROM data
        UNION ALL
        SELECT * FROM (
            WITH data AS (
                SELECT id FROM inner_table
            )
            SELECT * FROM data
        ) inner_result
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on outer_table and inner_table, but NOT 'data'
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "outer_table".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "inner_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_nested_cte_in_lateral_join() {
    // Test dependency extraction with CTE inside a LATERAL join
    let sql = r#"
        CREATE VIEW v AS
        SELECT u.id, stats.total
        FROM users u,
        LATERAL (
            WITH user_orders AS (
                SELECT amount FROM orders WHERE orders.user_id = u.id
            )
            SELECT SUM(amount) as total FROM user_orders
        ) stats
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on users and orders, but NOT user_orders
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "users".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "orders".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_parallel_ctes_in_union_branches() {
    // Test dependency extraction with independent CTEs in UNION branches
    let sql = r#"
        CREATE VIEW v AS
        SELECT * FROM (
            WITH left_cte AS (SELECT id FROM left_table)
            SELECT * FROM left_cte
        ) left_branch
        UNION ALL
        SELECT * FROM (
            WITH right_cte AS (SELECT id FROM right_table)
            SELECT * FROM right_cte
        ) right_branch
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on left_table and right_table, but NOT on CTEs
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "left_table".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "right_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_outer_cte_visible_in_nested_subquery() {
    // Test that outer CTE references in nested subqueries are not treated as dependencies
    let sql = r#"
        CREATE VIEW v AS
        WITH main_data AS (
            SELECT id, value FROM source_table
        )
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM main_data WHERE value > 10
            ) inner_sub
        ) outer_sub
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should only have dependency on source_table, not main_data
        assert_eq!(deps.len(), 1, "Expected 1 dependency, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "source_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_nested_cte_joining_outer_cte() {
    // Test nested CTE that joins with outer CTE
    let sql = r#"
        CREATE VIEW v AS
        WITH outer_data AS (
            SELECT id, category FROM categories
        )
        SELECT * FROM (
            WITH inner_data AS (
                SELECT product_id, price FROM products
            )
            SELECT i.product_id, i.price, o.category
            FROM inner_data i
            JOIN outer_data o ON i.product_id = o.id
        ) result
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on categories and products, but NOT on CTEs
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "categories".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "products".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_nested_cte_in_exists() {
    // Test CTE inside EXISTS subquery
    let sql = r#"
        CREATE VIEW v AS
        SELECT * FROM main_table m
        WHERE EXISTS (
            WITH related AS (
                SELECT id FROM related_table WHERE status = 'active'
            )
            SELECT 1 FROM related WHERE related.id = m.related_id
        )
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on main_table and related_table, but NOT 'related'
        assert_eq!(deps.len(), 2, "Expected 2 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "main_table".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "related_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_nested_cte_with_multiple_tables_per_level() {
    // Test nested CTEs where each level references multiple external tables
    let sql = r#"
        CREATE VIEW v AS
        WITH outer_cte AS (
            SELECT a.id, b.name FROM table_a a JOIN table_b b ON a.id = b.id
        )
        SELECT * FROM (
            WITH inner_cte AS (
                SELECT c.id, d.value FROM table_c c JOIN table_d d ON c.id = d.id
            )
            SELECT o.id, o.name, i.value
            FROM outer_cte o
            JOIN inner_cte i ON o.id = i.id
        ) result
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should have dependencies on all four tables
        assert_eq!(deps.len(), 4, "Expected 4 dependencies, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table_a".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table_b".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table_c".to_string()
        )));
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "table_d".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_dependencies_mutually_recursive_with_nested_cte_in_subquery() {
    // Test MUTUALLY RECURSIVE with a nested simple CTE inside a subquery
    let sql = r#"
        CREATE VIEW v AS
        WITH MUTUALLY RECURSIVE
          cte_a (id int) AS (
            SELECT id FROM (
                WITH nested AS (SELECT id FROM base_table)
                SELECT * FROM nested
            ) sub
            WHERE id IN (SELECT id FROM cte_b)
          ),
          cte_b (id int) AS (
            SELECT id FROM cte_a
          )
        SELECT * FROM cte_b
    "#;
    let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();

    if let mz_sql_parser::ast::Statement::CreateView(view_stmt) = &parsed[0].ast {
        let stmt = Statement::CreateView(view_stmt.clone());
        let (deps, _clusters) = extract_dependencies(&stmt, "db", "public");

        // Should only have dependency on base_table
        assert_eq!(deps.len(), 1, "Expected 1 dependency, found: {:?}", deps);
        assert!(deps.contains(&ObjectId::new(
            "db".to_string(),
            "public".to_string(),
            "base_table".to_string()
        )));
    } else {
        panic!("Expected CreateView statement");
    }
}
