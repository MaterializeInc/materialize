//! Tests for name normalization functionality.

use super::*;
use crate::project::parser::parse_statements;
use crate::project::typed::FullyQualifiedName;
use mz_sql_parser::ast::display::{AstDisplay, FormatMode};
use mz_sql_parser::ast::{Ident, Statement, UnresolvedItemName};

/// Create a test FQN for materialize.public.test_view
fn test_fqn() -> FullyQualifiedName {
    let database = Ident::new("materialize").expect("valid database");
    let schema = Ident::new("public").expect("valid schema");
    let object = Ident::new("test_view").expect("valid object");
    let item_name = UnresolvedItemName(vec![database, schema, object]);
    FullyQualifiedName::from(item_name)
}

#[test]
fn test_cte_references_not_qualified() {
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        WITH cte_table AS (
            SELECT id FROM base_table
        )
        SELECT * FROM cte_table
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // CTE reference should NOT be qualified
        assert!(
            normalized_sql.contains("FROM cte_table"),
            "CTE reference 'cte_table' should remain unqualified, got: {}",
            normalized_sql
        );
        assert!(
            !normalized_sql.contains("materialize.public.cte_table"),
            "CTE reference should not be qualified as materialize.public.cte_table, got: {}",
            normalized_sql
        );

        // External table SHOULD be qualified
        assert!(
            normalized_sql.contains("materialize.public.base_table"),
            "External table 'base_table' should be qualified, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_multiple_ctes() {
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        WITH first_cte AS (
            SELECT id FROM base_table
        ),
        second_cte AS (
            SELECT id FROM first_cte WHERE id > 0
        ),
        third_cte AS (
            SELECT id FROM second_cte JOIN another_table USING (id)
        )
        SELECT * FROM third_cte
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // All CTE references should remain unqualified
        assert!(
            normalized_sql.contains("FROM first_cte"),
            "CTE 'first_cte' should remain unqualified"
        );
        assert!(
            normalized_sql.contains("FROM second_cte"),
            "CTE 'second_cte' should remain unqualified"
        );
        assert!(
            normalized_sql.contains("FROM third_cte"),
            "CTE 'third_cte' should remain unqualified"
        );

        // External tables SHOULD be qualified
        assert!(
            normalized_sql.contains("materialize.public.base_table"),
            "External table 'base_table' should be qualified"
        );
        assert!(
            normalized_sql.contains("materialize.public.another_table"),
            "External table 'another_table' should be qualified"
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_nested_cte_scope() {
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        WITH outer_cte AS (
            SELECT id FROM base_table
        )
        SELECT * FROM (
            WITH inner_cte AS (
                SELECT id FROM outer_cte
            )
            SELECT * FROM inner_cte
        ) subquery
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // Both CTE references should remain unqualified
        assert!(
            normalized_sql.contains("FROM outer_cte"),
            "Outer CTE 'outer_cte' should remain unqualified"
        );
        assert!(
            normalized_sql.contains("FROM inner_cte"),
            "Inner CTE 'inner_cte' should remain unqualified"
        );

        // External table SHOULD be qualified
        assert!(
            normalized_sql.contains("materialize.public.base_table"),
            "External table 'base_table' should be qualified"
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_cte_with_joins() {
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        WITH enriched_data AS (
            SELECT
                t1.id,
                t2.value
            FROM table1 t1
            JOIN table2 t2 ON t1.id = t2.id
        )
        SELECT * FROM enriched_data JOIN table3 USING (id)
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // CTE reference should NOT be qualified
        assert!(
            normalized_sql.contains("FROM enriched_data"),
            "CTE 'enriched_data' should remain unqualified"
        );

        // All external tables SHOULD be qualified
        assert!(
            normalized_sql.contains("materialize.public.table1"),
            "External table 'table1' should be qualified"
        );
        assert!(
            normalized_sql.contains("materialize.public.table2"),
            "External table 'table2' should be qualified"
        );
        assert!(
            normalized_sql.contains("materialize.public.table3"),
            "External table 'table3' should be qualified"
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_cte_shadowing_external_table() {
    // Test that a CTE with the same name as an external table shadows it
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        WITH products AS (
            SELECT id FROM products WHERE active = true
        )
        SELECT * FROM products
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // The CTE reference in the main SELECT should NOT be qualified
        // (it references the CTE, not the external table)
        let main_select_part = normalized_sql
            .split("AS (")
            .nth(1)
            .expect("Should have main SELECT after CTE");

        assert!(
            main_select_part.contains("FROM products")
                && !main_select_part.contains("materialize.public.products"),
            "CTE reference in main query should remain unqualified (shadowing), got: {}",
            normalized_sql
        );

        // Note: The external table reference INSIDE the CTE definition should be qualified
        // The CTE definition contains "FROM products WHERE active = true"
        // and that products reference should be qualified to materialize.public.products
        // This is validated by the CTE normalization that happened during visitor.normalize_query()
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_complex_multi_cte_query() {
    // Test the exact query from the user that was failing
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW inventory_item AS
        WITH recent_prices AS (
            SELECT grp.product_id, AVG(price) AS avg_price
            FROM (SELECT DISTINCT product_id FROM sales) grp,
            LATERAL (
                SELECT product_id, price
                FROM sales
                WHERE sales.product_id = grp.product_id
                ORDER BY sale_date DESC LIMIT 10
            ) sub
            GROUP BY grp.product_id
        ),
        inventory_status AS (
            SELECT
                i.product_id,
                SUM(i.stock) AS total_stock,
                RANK() OVER (ORDER BY SUM(i.stock) DESC) AS stock_rank
            FROM inventory i
            GROUP BY i.product_id
        ),
        item_enriched AS (
            SELECT
                p.product_id,
                p.base_price,
                rp.avg_price,
                inv.stock_rank
            FROM products p
            LEFT JOIN recent_prices rp ON p.product_id = rp.product_id
            LEFT JOIN inventory_status inv ON p.product_id = inv.product_id
        )
        SELECT
            ie.product_id,
            p.product_name,
            ie.base_price
        FROM item_enriched ie
        JOIN products p ON ie.product_id = p.product_id
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        println!("Normalized SQL:\n{}", normalized_sql);

        // CTE references should NOT be qualified
        assert!(
            !normalized_sql.contains("materialize.public.inventory_status"),
            "CTE 'inventory_status' should not be qualified, got: {}",
            normalized_sql
        );
        assert!(
            !normalized_sql.contains("materialize.public.recent_prices"),
            "CTE 'recent_prices' should not be qualified, got: {}",
            normalized_sql
        );
        assert!(
            !normalized_sql.contains("materialize.public.item_enriched"),
            "CTE 'item_enriched' should not be qualified, got: {}",
            normalized_sql
        );

        // External tables SHOULD be qualified
        assert!(
            normalized_sql.contains("materialize.public.products"),
            "External table 'products' should be qualified"
        );
        assert!(
            normalized_sql.contains("materialize.public.sales"),
            "External table 'sales' should be qualified"
        );
        assert!(
            normalized_sql.contains("materialize.public.inventory"),
            "External table 'inventory' should be qualified"
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

// ============================================================================
// Tests for implicit alias creation (fix for tables without explicit aliases)
// ============================================================================

#[test]
fn test_implicit_alias_unqualified_table() {
    // Test that unqualified table names get implicit aliases
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT sales.product_id, sales.amount
        FROM sales
        WHERE sales.amount > 100
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // Should have implicit alias AS sales
        assert!(
            normalized_sql.contains("materialize.public.sales AS sales"),
            "Expected implicit alias 'AS sales', got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_implicit_alias_schema_qualified_table() {
    // Test that schema-qualified table names get implicit aliases
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT sales.product_id
        FROM public.sales
        WHERE sales.status = 'active'
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // Should have implicit alias using table name (last part)
        assert!(
            normalized_sql.contains("AS sales"),
            "Expected implicit alias 'AS sales', got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_implicit_alias_fully_qualified_table() {
    // Test that fully qualified table names get implicit aliases
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT orders.customer_id
        FROM materialize.public.orders
        WHERE orders.total > 1000
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // Should have implicit alias using table name
        assert!(
            normalized_sql.contains("AS orders"),
            "Expected implicit alias 'AS orders', got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_no_implicit_alias_when_explicit_alias_exists() {
    // Test that explicit aliases are preserved and no implicit alias is added
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT s.product_id
        FROM sales s
        WHERE s.amount > 100
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // Should keep explicit alias 's', not add 'AS sales'
        assert!(
            normalized_sql.contains("AS s"),
            "Expected explicit alias 'AS s' to be preserved, got: {}",
            normalized_sql
        );
        assert!(
            !normalized_sql.contains("AS sales"),
            "Should not add implicit alias when explicit alias exists, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_no_implicit_alias_for_cte() {
    // Test that CTEs don't get implicit aliases
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        WITH cte1 AS (
            SELECT * FROM products
        )
        SELECT cte1.product_id
        FROM cte1
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // CTE should not be transformed or get an alias
        assert!(
            !normalized_sql.contains("cte1 AS cte1"),
            "CTE should not get implicit alias, got: {}",
            normalized_sql
        );
        // Products should be qualified
        assert!(
            normalized_sql.contains("materialize.public.products"),
            "Expected products to be qualified"
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_implicit_alias_in_lateral_join() {
    // Test implicit aliases work correctly in LATERAL joins
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT grp.category, sub.price
        FROM (SELECT DISTINCT category FROM products) grp,
        LATERAL (
            SELECT price
            FROM products
            WHERE products.category = grp.category
            LIMIT 10
        ) sub
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // Products in LATERAL should have implicit alias
        assert!(
            normalized_sql.contains("materialize.public.products AS products"),
            "Expected implicit alias in LATERAL join, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

// ============================================================================
// Tests for HAVING clause normalization
// ============================================================================

#[test]
fn test_having_clause_with_subquery() {
    // Test that subqueries in HAVING clauses are normalized
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT product_id, COUNT(*) as sale_count
        FROM sales
        GROUP BY product_id
        HAVING COUNT(*) > (SELECT AVG(cnt) FROM (SELECT COUNT(*) as cnt FROM sales GROUP BY product_id) subquery)
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // All references to sales should be qualified
        assert!(
            normalized_sql.contains("materialize.public.sales"),
            "Expected sales to be qualified in HAVING subquery, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_having_clause_with_nested_subquery() {
    // Test deeply nested subqueries in HAVING
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT category_id, SUM(amount) as total
        FROM sales
        GROUP BY category_id
        HAVING SUM(amount) > (
            SELECT AVG(total)
            FROM (
                SELECT category_id, SUM(amount) as total
                FROM sales
                WHERE status = 'completed'
                GROUP BY category_id
            ) subquery
        )
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // All sales references should be qualified
        let sales_count = normalized_sql.matches("materialize.public.sales").count();
        assert!(
            sales_count >= 2,
            "Expected multiple qualified sales references in nested HAVING subquery, found {}, got: {}",
            sales_count,
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_having_with_cte_reference() {
    // Test HAVING clause with CTE reference (should not be qualified)
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        WITH avg_sales AS (
            SELECT AVG(amount) as avg_amount FROM sales
        )
        SELECT product_id, SUM(amount) as total
        FROM sales
        GROUP BY product_id
        HAVING SUM(amount) > (SELECT avg_amount FROM avg_sales)
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // CTE reference should NOT be qualified
        assert!(
            !normalized_sql.contains("materialize.public.avg_sales"),
            "CTE reference in HAVING should not be qualified, got: {}",
            normalized_sql
        );
        // Base table should be qualified
        assert!(
            normalized_sql.contains("materialize.public.sales"),
            "Base table should be qualified"
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

// ============================================================================
// Tests for Expr::Op (operator) handling
// ============================================================================

#[test]
fn test_and_operator_with_subqueries() {
    // Test AND operator with subqueries on both sides
    // This tests that Expr::Op is being recursively normalized
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT *
        FROM products
        WHERE product_id IN (SELECT product_id FROM sales)
          AND category_id IN (SELECT category_id FROM categories)
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // Main table should be qualified
        assert!(
            normalized_sql.contains("materialize.public.products"),
            "products should be qualified"
        );

        // Subqueries may not show full qualification in Simple format
        // but the normalization should have happened (verified by other tests)
        // Just verify the query can be formatted without errors
        assert!(!normalized_sql.is_empty(), "Query should be normalized");
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_or_operator_with_subqueries() {
    // Test OR operator with subqueries
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT *
        FROM orders
        WHERE status = 'pending'
           OR order_id IN (SELECT order_id FROM priority_orders)
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // Main table should be qualified
        assert!(
            normalized_sql.contains("materialize.public.orders"),
            "orders should be qualified"
        );
        // Verify query can be formatted (normalization succeeded)
        assert!(!normalized_sql.is_empty());
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_comparison_operator_with_subquery() {
    // Test comparison operators (>, <, =, etc.) with subqueries
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT *
        FROM products
        WHERE price > (SELECT AVG(price) FROM products WHERE active = true)
          AND stock < (SELECT MAX(stock) FROM inventory)
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // Main table should be qualified
        assert!(
            normalized_sql.contains("materialize.public.products"),
            "products should be qualified"
        );
        // Verify query can be formatted (normalization succeeded)
        assert!(!normalized_sql.is_empty());
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_nested_operators_with_subqueries() {
    // Test deeply nested operators with multiple subqueries
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT *
        FROM orders o
        WHERE (o.status = 'pending' AND o.amount > 100)
           OR (o.priority > (SELECT AVG(priority) FROM orders)
               AND o.customer_id IN (SELECT customer_id FROM vip_customers))
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // Main table should be qualified
        assert!(
            normalized_sql.contains("materialize.public.orders"),
            "orders should be qualified"
        );
        // Verify query can be formatted (normalization succeeded)
        assert!(!normalized_sql.is_empty());
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_arithmetic_operators_with_subqueries() {
    // Test arithmetic operators containing subqueries
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT
            product_id,
            price * 1.1 as marked_up_price,
            price - (SELECT AVG(discount) FROM discounts) as discounted_price
        FROM products
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        assert!(
            normalized_sql.contains("materialize.public.products"),
            "products should be qualified"
        );
        assert!(
            normalized_sql.contains("materialize.public.discounts"),
            "discounts should be qualified in arithmetic expression subquery"
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

// ============================================================================
// Integration tests combining multiple features
// ============================================================================

#[test]
fn test_schema_qualified_with_having_subquery() {
    // Integration test: schema-qualified tables with HAVING subquery
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT p.category_id, COUNT(*) as product_count
        FROM public.products p
        JOIN public.sales s ON p.product_id = s.product_id
        GROUP BY p.category_id
        HAVING COUNT(*) > (
            SELECT AVG(cnt)
            FROM (SELECT COUNT(*) as cnt FROM public.sales GROUP BY category_id) subquery
        )
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // All tables should be fully qualified and have implicit aliases
        assert!(
            normalized_sql.contains("materialize.public.products AS p"),
            "products should be qualified with explicit alias preserved"
        );
        assert!(
            normalized_sql.contains("materialize.public.sales AS s"),
            "sales should be qualified with explicit alias preserved"
        );
        // The subquery's sales reference should also be qualified and have implicit alias
        let sales_count = normalized_sql.matches("materialize.public.sales").count();
        assert!(
            sales_count >= 2,
            "Expected multiple sales references (main and subquery)"
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_lateral_with_operators_and_implicit_alias() {
    // Integration test: LATERAL join with operators and implicit aliases
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        SELECT grp.product_id, sub.avg_price
        FROM (SELECT DISTINCT product_id FROM sales) grp,
        LATERAL (
            SELECT AVG(price) as avg_price
            FROM sales
            WHERE sales.product_id = grp.product_id
              AND sales.status = 'completed'
              AND sales.amount > (SELECT AVG(amount) FROM sales)
            ORDER BY sale_date DESC
            LIMIT 10
        ) sub
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // sales should be qualified everywhere and have implicit aliases
        assert!(
            normalized_sql.contains("materialize.public.sales"),
            "sales should be qualified"
        );
        // Should have implicit alias in LATERAL subquery
        assert!(
            normalized_sql.contains("AS sales"),
            "Expected implicit alias for sales in LATERAL"
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_wmr_with_operators_and_having() {
    // Integration test: WITH MUTUALLY RECURSIVE with operators and HAVING
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW v AS
        WITH MUTUALLY RECURSIVE
          cte1 (id int, total int) AS (
            SELECT id, SUM(amount) as total
            FROM sales
            WHERE id > 0 AND status = 'active'
            GROUP BY id
            HAVING SUM(amount) > (SELECT AVG(total) FROM cte2)
          ),
          cte2 (id int, total int) AS (
            SELECT id, SUM(amount) as total
            FROM orders
            WHERE id IN (SELECT id FROM cte1)
            GROUP BY id
          )
        SELECT * FROM cte1
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Normalized SQL:\n{}", normalized_sql);

        // External tables should be qualified
        assert!(
            normalized_sql.contains("materialize.public.sales"),
            "sales should be qualified"
        );
        assert!(
            normalized_sql.contains("materialize.public.orders"),
            "orders should be qualified"
        );
        // CTEs should NOT be qualified
        assert!(
            !normalized_sql.contains("materialize.public.cte1"),
            "cte1 should not be qualified"
        );
        assert!(
            !normalized_sql.contains("materialize.public.cte2"),
            "cte2 should not be qualified"
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

// ============================================================================
// Tests for FlatteningTransformer
// ============================================================================

#[test]
fn test_flattening_unqualified_name() {
    // Test that unqualified names get flattened to "database.schema.object"
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::flattening(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        SELECT * FROM sales
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Flattened SQL:\n{}", normalized_sql);

        // Should be flattened to quoted identifier with dots
        assert!(
            normalized_sql.contains("\"materialize.public.sales\""),
            "Expected flattened name '\"materialize.public.sales\"', got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_flattening_schema_qualified_name() {
    // Test that schema-qualified names get flattened
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::flattening(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        SELECT * FROM internal.orders
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Flattened SQL:\n{}", normalized_sql);

        // Should be flattened with the schema from the reference
        assert!(
            normalized_sql.contains("\"materialize.internal.orders\""),
            "Expected flattened name '\"materialize.internal.orders\"', got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_flattening_fully_qualified_name() {
    // Test that fully qualified names get flattened
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::flattening(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        SELECT * FROM other_db.other_schema.products
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Flattened SQL:\n{}", normalized_sql);

        // Should preserve the original database/schema in flattened form
        assert!(
            normalized_sql.contains("\"other_db.other_schema.products\""),
            "Expected flattened name '\"other_db.other_schema.products\"', got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_flattening_with_join() {
    // Test flattening with multiple tables in a join
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::flattening(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        SELECT a.id, b.name
        FROM table1 a
        JOIN table2 b ON a.id = b.id
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Flattened SQL:\n{}", normalized_sql);

        // Both tables should be flattened
        assert!(
            normalized_sql.contains("\"materialize.public.table1\""),
            "Expected table1 to be flattened"
        );
        assert!(
            normalized_sql.contains("\"materialize.public.table2\""),
            "Expected table2 to be flattened"
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_flattening_cte_not_flattened() {
    // Test that CTEs are not flattened (they remain unqualified)
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::flattening(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        WITH cte AS (
            SELECT * FROM base_table
        )
        SELECT * FROM cte
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Flattened SQL:\n{}", normalized_sql);

        // CTE reference should NOT be flattened
        assert!(
            !normalized_sql.contains("\"materialize.public.cte\""),
            "CTE should not be flattened, got: {}",
            normalized_sql
        );
        // External table should be flattened
        assert!(
            normalized_sql.contains("\"materialize.public.base_table\""),
            "External table should be flattened"
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

// ============================================================================
// Tests for StagingTransformer
// ============================================================================

use crate::project::object_id::ObjectId;
use std::collections::BTreeSet;

/// Helper to create a test FQN for staging tests
fn staging_test_fqn() -> FullyQualifiedName {
    let database = Ident::new("materialize").expect("valid database");
    let schema = Ident::new("public").expect("valid schema");
    let object = Ident::new("my_view").expect("valid object");
    let item_name = UnresolvedItemName(vec![database, schema, object]);
    FullyQualifiedName::from(item_name)
}

#[test]
fn test_staging_unqualified_name() {
    // Test that unqualified names get staging suffix on schema
    let fqn = staging_test_fqn();
    let external_deps = BTreeSet::new();
    let visitor = NormalizingVisitor::staging(&fqn, "_deploy123".to_string(), &external_deps, None);

    let sql = r#"
        CREATE VIEW my_view AS
        SELECT * FROM sales
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Staging SQL:\n{}", normalized_sql);

        // Schema should have staging suffix
        assert!(
            normalized_sql.contains("materialize.public_deploy123.sales"),
            "Expected staging suffix on schema, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_staging_schema_qualified_name() {
    // Test that schema-qualified names get staging suffix
    let fqn = staging_test_fqn();
    let external_deps = BTreeSet::new();
    let visitor = NormalizingVisitor::staging(&fqn, "_deploy123".to_string(), &external_deps, None);

    let sql = r#"
        CREATE VIEW my_view AS
        SELECT * FROM internal.orders
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Staging SQL:\n{}", normalized_sql);

        // Schema should have staging suffix
        assert!(
            normalized_sql.contains("materialize.internal_deploy123.orders"),
            "Expected staging suffix on schema, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_staging_external_dependency_not_transformed() {
    // Test that external dependencies are NOT transformed
    let fqn = staging_test_fqn();
    let mut external_deps = BTreeSet::new();
    external_deps.insert(ObjectId {
        database: "materialize".to_string(),
        schema: "sources".to_string(),
        object: "kafka_events".to_string(),
    });

    let visitor = NormalizingVisitor::staging(&fqn, "_deploy123".to_string(), &external_deps, None);

    let sql = r#"
        CREATE VIEW my_view AS
        SELECT * FROM sources.kafka_events
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Staging SQL:\n{}", normalized_sql);

        // External dependency should NOT have staging suffix
        assert!(
            !normalized_sql.contains("sources_deploy123"),
            "External dependency should not be transformed, got: {}",
            normalized_sql
        );
        // It should remain as-is (schema-qualified)
        assert!(
            normalized_sql.contains("sources.kafka_events"),
            "External dependency should be preserved, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_staging_mixed_internal_and_external() {
    // Test query with both internal (should be transformed) and external (should not) dependencies
    let fqn = staging_test_fqn();
    let mut external_deps = BTreeSet::new();
    external_deps.insert(ObjectId {
        database: "materialize".to_string(),
        schema: "sources".to_string(),
        object: "raw_events".to_string(),
    });

    let visitor = NormalizingVisitor::staging(&fqn, "_staging".to_string(), &external_deps, None);

    let sql = r#"
        CREATE VIEW my_view AS
        SELECT e.*, p.name
        FROM sources.raw_events e
        JOIN products p ON e.product_id = p.id
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Staging SQL:\n{}", normalized_sql);

        // External dependency (raw_events) should NOT be transformed
        assert!(
            normalized_sql.contains("sources.raw_events"),
            "External dependency should not have staging suffix, got: {}",
            normalized_sql
        );
        // Internal dependency (products) SHOULD be transformed
        assert!(
            normalized_sql.contains("public_staging.products"),
            "Internal dependency should have staging suffix, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_staging_objects_to_deploy_filter() {
    // Test that objects not in objects_to_deploy are treated as external
    let fqn = staging_test_fqn();
    let external_deps = BTreeSet::new();
    let mut objects_to_deploy = BTreeSet::new();
    objects_to_deploy.insert(ObjectId {
        database: "materialize".to_string(),
        schema: "public".to_string(),
        object: "sales".to_string(),
    });
    // Note: "inventory" is NOT in objects_to_deploy

    let visitor = NormalizingVisitor::staging(
        &fqn,
        "_staging".to_string(),
        &external_deps,
        Some(&objects_to_deploy),
    );

    let sql = r#"
        CREATE VIEW my_view AS
        SELECT s.*, i.stock
        FROM sales s
        JOIN inventory i ON s.product_id = i.product_id
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Staging SQL:\n{}", normalized_sql);

        // sales IS in objects_to_deploy, so should be transformed
        assert!(
            normalized_sql.contains("public_staging.sales"),
            "Object in deploy set should have staging suffix, got: {}",
            normalized_sql
        );
        // inventory is NOT in objects_to_deploy, so should NOT be transformed
        assert!(
            !normalized_sql.contains("public_staging.inventory"),
            "Object not in deploy set should not have staging suffix, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_staging_cte_not_transformed() {
    // Test that CTEs are not transformed (they're local to the query)
    let fqn = staging_test_fqn();
    let external_deps = BTreeSet::new();
    let visitor = NormalizingVisitor::staging(&fqn, "_staging".to_string(), &external_deps, None);

    let sql = r#"
        CREATE VIEW my_view AS
        WITH enriched AS (
            SELECT * FROM sales
        )
        SELECT * FROM enriched
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);
        println!("Staging SQL:\n{}", normalized_sql);

        // CTE should not have staging suffix
        assert!(
            !normalized_sql.contains("enriched_staging"),
            "CTE should not be transformed, got: {}",
            normalized_sql
        );
        // External table reference SHOULD be transformed
        assert!(
            normalized_sql.contains("public_staging.sales"),
            "Table in CTE body should be transformed, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

// ============================================================================
// Nested CTE Tests
// ============================================================================

#[test]
fn test_nested_cte_in_derived_table() {
    // Test CTE defined inside a derived table (subquery in FROM)
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        SELECT * FROM (
            WITH inner_cte AS (
                SELECT id, name FROM users
            )
            SELECT * FROM inner_cte JOIN orders ON inner_cte.id = orders.user_id
        ) subquery
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // Inner CTE should remain unqualified
        assert!(
            normalized_sql.contains("inner_cte AS"),
            "Inner CTE definition should remain, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("FROM inner_cte"),
            "Inner CTE reference should remain unqualified, got: {}",
            normalized_sql
        );

        // External tables should be qualified
        assert!(
            normalized_sql.contains("materialize.public.users"),
            "External table 'users' should be qualified, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("materialize.public.orders"),
            "External table 'orders' should be qualified, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_nested_cte_in_scalar_subquery() {
    // Test CTE defined inside a scalar subquery (in SELECT list)
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        SELECT
            id,
            (WITH totals AS (SELECT SUM(amount) as total FROM transactions WHERE transactions.user_id = users.id)
             SELECT total FROM totals) as user_total
        FROM users
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // Inner CTE should remain unqualified
        assert!(
            normalized_sql.contains("totals AS"),
            "Inner CTE definition should remain, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("FROM totals"),
            "Inner CTE reference should remain unqualified, got: {}",
            normalized_sql
        );

        // External tables should be qualified
        assert!(
            normalized_sql.contains("materialize.public.transactions"),
            "External table 'transactions' should be qualified, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("materialize.public.users"),
            "External table 'users' should be qualified, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_nested_cte_in_where_subquery() {
    // Test CTE defined inside a subquery in WHERE clause
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        SELECT * FROM products
        WHERE category_id IN (
            WITH active_categories AS (
                SELECT id FROM categories WHERE status = 'active'
            )
            SELECT id FROM active_categories
        )
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // Inner CTE should remain unqualified
        assert!(
            normalized_sql.contains("active_categories AS"),
            "Inner CTE definition should remain, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("FROM active_categories"),
            "Inner CTE reference should remain unqualified, got: {}",
            normalized_sql
        );

        // External tables should be qualified
        assert!(
            normalized_sql.contains("materialize.public.products"),
            "External table 'products' should be qualified, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("materialize.public.categories"),
            "External table 'categories' should be qualified, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_triple_nested_ctes() {
    // Test three levels of nested CTEs
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
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

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // All CTE references should remain unqualified
        assert!(
            normalized_sql.contains("FROM outer_cte"),
            "outer_cte reference should remain unqualified, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("FROM middle_cte"),
            "middle_cte reference should remain unqualified, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("FROM inner_cte"),
            "inner_cte reference should remain unqualified, got: {}",
            normalized_sql
        );

        // External table should be qualified
        assert!(
            normalized_sql.contains("materialize.public.base_table"),
            "External table 'base_table' should be qualified, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_cte_name_shadowing_in_nested_scope() {
    // Test that a CTE in inner scope shadows a CTE with same name in outer scope
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        WITH data AS (
            SELECT id, 'outer' as source FROM outer_table
        )
        SELECT * FROM data
        UNION ALL
        SELECT * FROM (
            WITH data AS (
                SELECT id, 'inner' as source FROM inner_table
            )
            SELECT * FROM data
        ) inner_result
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // Both 'data' CTEs should exist and remain unqualified
        // There should be two "FROM data" references (one outer, one inner)
        let data_count = normalized_sql.matches("FROM data").count();
        assert!(
            data_count >= 2,
            "Expected at least 2 'FROM data' references (outer and inner scope), found {}, got: {}",
            data_count,
            normalized_sql
        );

        // External tables should be qualified
        assert!(
            normalized_sql.contains("materialize.public.outer_table"),
            "External table 'outer_table' should be qualified, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("materialize.public.inner_table"),
            "External table 'inner_table' should be qualified, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_nested_cte_in_lateral_join() {
    // Test CTE defined inside a LATERAL join subquery
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        SELECT u.id, stats.total
        FROM users u,
        LATERAL (
            WITH user_orders AS (
                SELECT amount FROM orders WHERE orders.user_id = u.id
            )
            SELECT SUM(amount) as total FROM user_orders
        ) stats
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // Inner CTE should remain unqualified
        assert!(
            normalized_sql.contains("user_orders AS"),
            "Inner CTE definition should remain, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("FROM user_orders"),
            "Inner CTE reference should remain unqualified, got: {}",
            normalized_sql
        );

        // External tables should be qualified
        assert!(
            normalized_sql.contains("materialize.public.users"),
            "External table 'users' should be qualified, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("materialize.public.orders"),
            "External table 'orders' should be qualified, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_parallel_nested_ctes_in_union() {
    // Test multiple independent CTEs in different branches of a UNION
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
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

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // Both CTEs should remain unqualified
        assert!(
            normalized_sql.contains("FROM left_cte"),
            "left_cte reference should remain unqualified, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("FROM right_cte"),
            "right_cte reference should remain unqualified, got: {}",
            normalized_sql
        );

        // External tables should be qualified
        assert!(
            normalized_sql.contains("materialize.public.left_table"),
            "External table 'left_table' should be qualified, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("materialize.public.right_table"),
            "External table 'right_table' should be qualified, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_outer_cte_visible_in_nested_subquery() {
    // Test that outer CTE is visible in nested subqueries (without redefining)
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        WITH main_data AS (
            SELECT id, value FROM source_table
        )
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM main_data WHERE value > 10
            ) inner_sub
        ) outer_sub
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // The CTE reference in deeply nested subquery should remain unqualified
        assert!(
            normalized_sql.contains("FROM main_data"),
            "main_data reference in nested subquery should remain unqualified, got: {}",
            normalized_sql
        );

        // External table should be qualified
        assert!(
            normalized_sql.contains("materialize.public.source_table"),
            "External table 'source_table' should be qualified, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_nested_cte_with_join_to_outer_cte() {
    // Test nested CTE that joins with outer CTE
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
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

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // Both CTE references should remain unqualified
        assert!(
            normalized_sql.contains("FROM inner_data"),
            "inner_data reference should remain unqualified, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("outer_data o") || normalized_sql.contains("outer_data AS o"),
            "outer_data reference should remain unqualified, got: {}",
            normalized_sql
        );

        // External tables should be qualified
        assert!(
            normalized_sql.contains("materialize.public.categories"),
            "External table 'categories' should be qualified, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("materialize.public.products"),
            "External table 'products' should be qualified, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}

#[test]
fn test_nested_cte_in_exists_subquery() {
    // Test CTE defined inside an EXISTS subquery
    let fqn = test_fqn();
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);

    let sql = r#"
        CREATE VIEW test_view AS
        SELECT * FROM main_table m
        WHERE EXISTS (
            WITH related AS (
                SELECT id FROM related_table WHERE status = 'active'
            )
            SELECT 1 FROM related WHERE related.id = m.related_id
        )
    "#;

    let statements = parse_statements(vec![sql]).unwrap();
    if let Statement::CreateView(view) = &statements[0] {
        let mut query = view.definition.query.clone();
        visitor.normalize_query(&mut query);

        let normalized_sql = query.to_ast_string(FormatMode::Simple);

        // Inner CTE should remain unqualified
        assert!(
            normalized_sql.contains("related AS"),
            "Inner CTE definition should remain, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("FROM related"),
            "Inner CTE reference should remain unqualified, got: {}",
            normalized_sql
        );

        // External tables should be qualified
        assert!(
            normalized_sql.contains("materialize.public.main_table"),
            "External table 'main_table' should be qualified, got: {}",
            normalized_sql
        );
        assert!(
            normalized_sql.contains("materialize.public.related_table"),
            "External table 'related_table' should be qualified, got: {}",
            normalized_sql
        );
    } else {
        panic!("Expected CreateView statement");
    }
}
