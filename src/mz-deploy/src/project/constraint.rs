//! Constraint lowering — compiling enforced constraints into materialized views.
//!
//! # Background
//!
//! Materialize does not natively support SQL constraints (PRIMARY KEY, UNIQUE,
//! FOREIGN KEY). However, mz-deploy allows users to declare constraints in their
//! `.sql` project files. Constraints come in two flavors:
//!
//! - **Not-enforced** (`NOT ENFORCED`): metadata-only. Recorded in the project
//!   model for documentation and catalog purposes but never executed.
//! - **Enforced**: compiled ("lowered") into a **companion materialized view**
//!   that continuously monitors for constraint violations.
//!
//! # Lowering rules
//!
//! Each enforced constraint is transformed into a materialized view whose query
//! is designed so that **any rows returned represent violations**:
//!
//! | Constraint kind | MV query pattern |
//! |-----------------|------------------|
//! | PRIMARY KEY / UNIQUE | `SELECT <cols>, count(*) FROM <table> GROUP BY <cols> HAVING count(*) > 1` |
//! | FOREIGN KEY | `SELECT <fk_cols> FROM <child> EXCEPT SELECT <pk_cols> FROM <parent>` |
//!
//! The companion MV is:
//! - Named after the constraint (or given a deterministic default name)
//! - Placed in the **same schema** as the constrained object
//! - Assigned to the **cluster specified in the constraint's IN CLUSTER clause**
//!
//! # Pipeline integration
//!
//! Lowering happens during the `typed → planned` conversion in
//! [`super::planned::dependency`]. The companion MVs are injected as first-class
//! objects in the planned project, so they automatically participate in
//! dependency tracking, topological sorting, change detection, staging
//! normalization, and blue/green deployment — no special-casing required
//! downstream.
//!
//! ```text
//!   raw → typed → planned
//!                    ↑
//!            constraint lowering
//!            (enforced constraints → companion MVs)
//! ```

use super::ast::Statement;
use super::typed;
use mz_sql_parser::ast::*;

/// Generate a deterministic name for an unnamed constraint.
///
/// Follows the convention `<object>_<col1>_<col2>_..._<kind>` where kind
/// is `pk`, `unique`, or `fk`. This mirrors how database systems generate
/// default constraint names and ensures stable, predictable names across
/// deployments.
///
/// # Examples
///
/// - `PRIMARY KEY ON foo (id)` → `foo_id_pk`
/// - `UNIQUE CONSTRAINT ON bar (a, b)` → `bar_a_b_unique`
/// - `FOREIGN KEY ON orders (customer_id) REFERENCES customers (id)` → `orders_customer_id_fk`
pub fn default_constraint_name(
    object_name: &str,
    columns: &[Ident],
    kind: &ConstraintKind,
) -> String {
    let cols: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
    let kind_suffix = match kind {
        ConstraintKind::PrimaryKey => "pk",
        ConstraintKind::Unique => "unique",
        ConstraintKind::ForeignKey => "fk",
    };
    format!("{}_{}", object_name, cols.join("_") + "_" + kind_suffix)
}

/// Lower an enforced constraint into a companion materialized view.
///
/// This is the core of constraint lowering. Given a `CreateConstraintStatement`,
/// it produces a `typed::DatabaseObject` containing a `CREATE MATERIALIZED VIEW`
/// whose query returns rows that violate the constraint.
///
/// # Arguments
///
/// * `constraint` - The constraint to lower
/// * `parent_object_name` - The simple name of the parent object (e.g., `"foo"`)
/// * `parent_on_name` - The fully-qualified `on_name` from the constraint statement
/// * `database` - The database containing the parent object
/// * `schema` - The schema containing the parent object
///
/// # Returns
///
/// - `Some(DatabaseObject)` for enforced constraints — the companion MV
/// - `None` for not-enforced constraints (they remain metadata-only)
///
/// # Generated queries
///
/// **PRIMARY KEY / UNIQUE** — duplicates indicate violations:
/// ```sql
/// SELECT id, count(*) AS count
/// FROM materialize.public.foo
/// GROUP BY id
/// HAVING count(*) > 1
/// ```
///
/// **FOREIGN KEY** — orphaned references indicate violations:
/// ```sql
/// SELECT customer_id FROM materialize.public.orders
/// EXCEPT
/// SELECT id FROM materialize.public.customers
/// ```
///
/// # Naming
///
/// If the constraint has an explicit name, that becomes the MV name.
/// Otherwise a deterministic name is generated: `<object>_<cols>_<kind>`
/// (e.g., `foo_id_pk`, `orders_customer_id_fk`).
pub fn lower_to_materialized_view(
    constraint: &CreateConstraintStatement<Raw>,
    parent_object_name: &str,
    database: &str,
    schema: &str,
) -> Option<typed::DatabaseObject> {
    if !constraint.enforced {
        return None;
    }

    // Determine the MV name
    let mv_name_str = match &constraint.name {
        Some(name) => name.to_string(),
        None => default_constraint_name(parent_object_name, &constraint.columns, &constraint.kind),
    };

    // Build the fully-qualified name for the MV
    let mv_fqn = UnresolvedItemName(vec![
        Ident::new(database).expect("valid database ident"),
        Ident::new(schema).expect("valid schema ident"),
        Ident::new(&mv_name_str).expect("valid constraint name ident"),
    ]);

    // Build the parent object's fully-qualified name string for the query
    let parent_fqn = constraint.on_name.name().to_string();

    // Build the SQL query string based on constraint kind
    let query_sql = match &constraint.kind {
        ConstraintKind::PrimaryKey | ConstraintKind::Unique => {
            let cols = constraint
                .columns
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT {cols}, count(*) AS count FROM {parent_fqn} GROUP BY {cols} HAVING count(*) > 1"
            )
        }
        ConstraintKind::ForeignKey => {
            let refs = constraint
                .references
                .as_ref()
                .expect("FK constraint must have references");
            let fk_cols = constraint
                .columns
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let ref_cols = refs
                .columns
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let ref_obj = refs.object.name().to_string();
            format!("SELECT {fk_cols} FROM {parent_fqn} EXCEPT SELECT {ref_cols} FROM {ref_obj}")
        }
    };

    // Parse the query SQL into a Query<Raw>
    let full_sql = format!(
        "CREATE MATERIALIZED VIEW {mv_fqn} IN CLUSTER {cluster} AS {query_sql}",
        mv_fqn = mv_fqn,
        cluster = constraint
            .in_cluster
            .as_ref()
            .expect("enforced constraint must have IN CLUSTER"),
    );

    let parsed = mz_sql_parser::parser::parse_statements(&full_sql).unwrap_or_else(|e| {
        panic!("failed to parse lowered constraint MV SQL: {e}\nSQL: {full_sql}")
    });

    let mv_stmt = match parsed.into_iter().next().unwrap().ast {
        mz_sql_parser::ast::Statement::CreateMaterializedView(s) => s,
        other => panic!("expected CreateMaterializedView, got: {:?}", other),
    };

    Some(typed::DatabaseObject {
        stmt: Statement::CreateMaterializedView(mv_stmt),
        indexes: vec![],
        constraints: vec![],
        grants: vec![],
        comments: vec![CommentStatement {
            object: CommentObjectType::MaterializedView {
                name: RawItemName::Name(mv_fqn),
            },
            comment: Some(constraint.to_string()),
        }],
        tests: vec![],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse a constraint from SQL text.
    fn parse_constraint(sql: &str) -> CreateConstraintStatement<Raw> {
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();
        match parsed.into_iter().next().unwrap().ast {
            mz_sql_parser::ast::Statement::CreateConstraint(c) => c,
            other => panic!("Expected CreateConstraint, got: {:?}", other),
        }
    }

    /// Extract the query text from a lowered MV.
    fn lowered_query_sql(obj: &typed::DatabaseObject) -> String {
        match &obj.stmt {
            Statement::CreateMaterializedView(mv) => mv.query.to_string(),
            other => panic!("Expected CreateMaterializedView, got: {:?}", other),
        }
    }

    /// Extract the MV name from a lowered MV.
    fn lowered_mv_name(obj: &typed::DatabaseObject) -> String {
        match &obj.stmt {
            Statement::CreateMaterializedView(mv) => mv.name.0.last().unwrap().to_string(),
            other => panic!("Expected CreateMaterializedView, got: {:?}", other),
        }
    }

    #[test]
    fn test_lower_primary_key_to_mv() {
        let constraint =
            parse_constraint("CREATE PRIMARY KEY pk IN CLUSTER c ON materialize.public.foo (id)");
        let result = lower_to_materialized_view(&constraint, "foo", "materialize", "public");
        let obj = result.expect("enforced PK should produce an MV");

        let query = lowered_query_sql(&obj);
        assert!(
            query.contains("count(*)"),
            "query should have count(*): {query}"
        );
        assert!(
            query.contains("GROUP BY"),
            "query should have GROUP BY: {query}"
        );
        assert!(
            query.contains("HAVING"),
            "query should have HAVING: {query}"
        );
        assert_eq!(lowered_mv_name(&obj), "pk");
    }

    #[test]
    fn test_lower_unique_to_mv() {
        let constraint = parse_constraint(
            "CREATE UNIQUE CONSTRAINT uq IN CLUSTER c ON materialize.public.bar (email)",
        );
        let result = lower_to_materialized_view(&constraint, "bar", "materialize", "public");
        let obj = result.expect("enforced UNIQUE should produce an MV");

        let query = lowered_query_sql(&obj);
        assert!(
            query.contains("count(*)"),
            "query should have count(*): {query}"
        );
        assert!(
            query.contains("GROUP BY"),
            "query should have GROUP BY: {query}"
        );
        assert_eq!(lowered_mv_name(&obj), "uq");
    }

    #[test]
    fn test_lower_foreign_key_to_mv() {
        let constraint = parse_constraint(
            "CREATE FOREIGN KEY fk IN CLUSTER c ON materialize.public.orders (customer_id) REFERENCES materialize.public.customers (id)",
        );
        let result = lower_to_materialized_view(&constraint, "orders", "materialize", "public");
        let obj = result.expect("enforced FK should produce an MV");

        let query = lowered_query_sql(&obj);
        assert!(
            query.contains("EXCEPT"),
            "query should have EXCEPT: {query}"
        );
        assert_eq!(lowered_mv_name(&obj), "fk");
    }

    #[test]
    fn test_lower_multi_column_pk() {
        let constraint =
            parse_constraint("CREATE PRIMARY KEY IN CLUSTER c ON materialize.public.foo (a, b)");
        let result = lower_to_materialized_view(&constraint, "foo", "materialize", "public");
        let obj = result.expect("enforced multi-col PK should produce an MV");

        let query = lowered_query_sql(&obj);
        assert!(
            query.contains("a, b"),
            "query should reference both columns: {query}"
        );
        assert!(
            query.contains("GROUP BY"),
            "query should have GROUP BY: {query}"
        );
        assert_eq!(lowered_mv_name(&obj), "foo_a_b_pk");
    }

    #[test]
    fn test_lower_multi_column_fk() {
        let constraint = parse_constraint(
            "CREATE FOREIGN KEY IN CLUSTER c ON materialize.public.orders (a, b) REFERENCES materialize.public.parents (x, y)",
        );
        let result = lower_to_materialized_view(&constraint, "orders", "materialize", "public");
        let obj = result.expect("enforced multi-col FK should produce an MV");

        let query = lowered_query_sql(&obj);
        assert!(
            query.contains("EXCEPT"),
            "query should have EXCEPT: {query}"
        );
        assert!(
            query.contains("a, b"),
            "query should reference FK columns: {query}"
        );
        assert!(
            query.contains("x, y"),
            "query should reference referenced columns: {query}"
        );
        assert_eq!(lowered_mv_name(&obj), "orders_a_b_fk");
    }

    #[test]
    fn test_lower_not_enforced_returns_none() {
        let constraint = parse_constraint(
            "CREATE PRIMARY KEY NOT ENFORCED pk IN CLUSTER c ON materialize.public.foo (id)",
        );
        let result = lower_to_materialized_view(&constraint, "foo", "materialize", "public");
        assert!(
            result.is_none(),
            "not-enforced constraint should return None"
        );
    }

    #[test]
    fn test_default_constraint_name_pk() {
        let name = default_constraint_name(
            "foo",
            &[Ident::new("id").unwrap()],
            &ConstraintKind::PrimaryKey,
        );
        assert_eq!(name, "foo_id_pk");
    }

    #[test]
    fn test_default_constraint_name_unique() {
        let name = default_constraint_name(
            "foo",
            &[Ident::new("a").unwrap(), Ident::new("b").unwrap()],
            &ConstraintKind::Unique,
        );
        assert_eq!(name, "foo_a_b_unique");
    }

    #[test]
    fn test_default_constraint_name_fk() {
        let name = default_constraint_name(
            "orders",
            &[Ident::new("customer_id").unwrap()],
            &ConstraintKind::ForeignKey,
        );
        assert_eq!(name, "orders_customer_id_fk");
    }
}
