// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Test SQL syntax.

use std::fmt;

use repr::datetime::DateTimeField;

use matches::assert_matches;
use sql_parser::ast::*;
use sql_parser::parser::*;

#[test]
fn parse_insert_values() {
    let row = vec![
        Expr::Value(number("1")),
        Expr::Value(number("2")),
        Expr::Value(number("3")),
    ];
    let rows1 = vec![row.clone()];
    let rows2 = vec![row.clone(), row];

    let sql = "INSERT INTO customer VALUES (1, 2, 3)";
    check_one(sql, "customer", &[], &rows1);

    let sql = "INSERT INTO customer VALUES (1, 2, 3), (1, 2, 3)";
    check_one(sql, "customer", &[], &rows2);

    let sql = "INSERT INTO public.customer VALUES (1, 2, 3)";
    check_one(sql, "public.customer", &[], &rows1);

    let sql = "INSERT INTO db.public.customer VALUES (1, 2, 3)";
    check_one(sql, "db.public.customer", &[], &rows1);

    let sql = "INSERT INTO public.customer (id, name, active) VALUES (1, 2, 3)";
    check_one(
        sql,
        "public.customer",
        &["id".to_string(), "name".to_string(), "active".to_string()],
        &rows1,
    );

    fn check_one(
        sql: &str,
        expected_table_name: &str,
        expected_columns: &[String],
        expected_rows: &[Vec<Expr>],
    ) {
        match verified_stmt(sql) {
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                assert_eq!(table_name.to_string(), expected_table_name);
                assert_eq!(columns.len(), expected_columns.len());
                for (index, column) in columns.iter().enumerate() {
                    assert_eq!(column, &Ident::new(expected_columns[index].clone()));
                }
                match &source.body {
                    SetExpr::Values(Values(values)) => assert_eq!(values.as_slice(), expected_rows),
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    verified_stmt("INSERT INTO customer WITH foo AS (SELECT 1) SELECT * FROM foo UNION VALUES (1)");
}

#[test]
fn parse_insert_invalid() {
    let sql = "INSERT public.customer (id, name, active) VALUES (1, 2, 3)";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ("\
Parse error:
INSERT public.customer (id, name, active) VALUES (1, 2, 3)
       ^^^^^^
Expected INTO, found: public"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_update() {
    let sql = "UPDATE t SET a = 1, b = 2, c = 3 WHERE d";
    match verified_stmt(sql) {
        Statement::Update {
            table_name,
            assignments,
            selection,
            ..
        } => {
            assert_eq!(table_name.to_string(), "t".to_string());
            assert_eq!(
                assignments,
                vec![
                    Assignment {
                        id: "a".into(),
                        value: Expr::Value(number("1")),
                    },
                    Assignment {
                        id: "b".into(),
                        value: Expr::Value(number("2")),
                    },
                    Assignment {
                        id: "c".into(),
                        value: Expr::Value(number("3")),
                    },
                ]
            );
            assert_eq!(selection.unwrap(), Expr::Identifier("d".into()));
        }
        _ => unreachable!(),
    }

    verified_stmt("UPDATE t SET a = 1, a = 2, a = 3");

    let sql = "UPDATE t WHERE 1";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ("\
Parse error:
UPDATE t WHERE 1
         ^^^^^
Expected SET, found: WHERE"
            .to_string()),
        format!("{}", res.unwrap_err())
    );

    let sql = "UPDATE t SET a = 1 extrabadstuff";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ("\
Parse error:
UPDATE t SET a = 1 extrabadstuff
                   ^^^^^^^^^^^^^
Expected end of statement, found: extrabadstuff"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_invalid_table_name() {
    let ast = run_parser_method("db.public..customer", Parser::parse_object_name);
    assert!(ast.is_err());
}

#[test]
fn parse_no_table_name() {
    let ast = run_parser_method("", Parser::parse_object_name);
    assert!(ast.is_err());
}

#[test]
fn parse_delete_statement() {
    let sql = "DELETE FROM \"table\"";
    match verified_stmt(sql) {
        Statement::Delete { table_name, .. } => {
            assert_eq!(
                ObjectName(vec![Ident::with_quote('"', "table")]),
                table_name
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_where_delete_statement() {
    use self::BinaryOperator::*;

    let sql = "DELETE FROM foo WHERE name = 5";
    match verified_stmt(sql) {
        Statement::Delete {
            table_name,
            selection,
            ..
        } => {
            assert_eq!(ObjectName(vec![Ident::new("foo")]), table_name);

            assert_eq!(
                Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("name"))),
                    op: Eq,
                    right: Box::new(Expr::Value(number("5"))),
                },
                selection.unwrap(),
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_top_level() {
    verified_stmt("SELECT 1");
    verified_stmt("(SELECT 1)");
    verified_stmt("((SELECT 1))");
    verified_stmt("VALUES (1)");
}

#[test]
fn parse_simple_select() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT 5";
    let select = verified_only_select(sql);
    assert_eq!(false, select.distinct);
    assert_eq!(3, select.projection.len());
    let select = verified_query(sql);
    assert_eq!(Some(Expr::Value(number("5"))), select.limit);
}

#[test]
fn parse_limit_is_not_an_alias() {
    // LIMIT should not be parsed as a table alias.
    let ast = verified_query("SELECT id FROM customer LIMIT 1");
    assert_eq!(Some(Expr::Value(number("1"))), ast.limit);

    let ast = verified_query("SELECT 1 LIMIT 5");
    assert_eq!(Some(Expr::Value(number("5"))), ast.limit);
}

#[test]
fn parse_select_distinct() {
    let sql = "SELECT DISTINCT name FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(true, select.distinct);
    assert_eq!(
        &SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("name"))),
        only(&select.projection)
    );
}

#[test]
fn parse_select_all() {
    one_statement_parses_to("SELECT ALL name FROM customer", "SELECT name FROM customer");
}

#[test]
fn parse_select_all_distinct() {
    let result = parse_sql_statements("SELECT ALL DISTINCT name FROM customer");
    assert_eq!(
        "\
Parse error:
SELECT ALL DISTINCT name FROM customer
       ^^^^^^^^^^^^
Cannot specify both ALL and DISTINCT in SELECT"
            .to_string(),
        format!("{}", result.unwrap_err()),
    );
}

#[test]
fn parse_select_wildcard() {
    let sql = "SELECT * FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(&SelectItem::Wildcard, only(&select.projection));

    let sql = "SELECT foo.* FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(
        &SelectItem::QualifiedWildcard(ObjectName(vec![Ident::new("foo")])),
        only(&select.projection)
    );

    let sql = "SELECT myschema.mytable.* FROM myschema.mytable";
    let select = verified_only_select(sql);
    assert_eq!(
        &SelectItem::QualifiedWildcard(ObjectName(vec![
            Ident::new("myschema"),
            Ident::new("mytable"),
        ])),
        only(&select.projection)
    );
}

#[test]
fn parse_count_wildcard() {
    verified_only_select(
        "SELECT COUNT(Employee.*) FROM Order JOIN Employee ON Order.employee = Employee.id",
    );
}

#[test]
fn parse_column_aliases() {
    let sql = "SELECT a.col + 1 AS newname FROM foo AS a";
    let select = verified_only_select(sql);
    if let SelectItem::ExprWithAlias {
        expr: Expr::BinaryOp {
            ref op, ref right, ..
        },
        ref alias,
    } = only(&select.projection)
    {
        assert_eq!(&BinaryOperator::Plus, op);
        assert_eq!(&Expr::Value(number("1")), right.as_ref());
        assert_eq!(&Ident::new("newname"), alias);
    } else {
        panic!("Expected ExprWithAlias")
    }

    // alias without AS is parsed correctly:
    one_statement_parses_to("SELECT a.col + 1 newname FROM foo AS a", &sql);
}

#[test]
fn parse_concat_function() {
    let sql = "SELECT CONCAT('CONCAT', ' ', 'function')";
    let _select = verified_only_select(sql);
    let sql = "SELECT CONCAT(first_name, ' ', last_name) FROM customer";
    let _select = verified_only_select(sql);
    let sql = "SELECT CONCAT('Concat with ', NULL) AS result_string";
    let _select = verified_only_select(sql);
    let sql = "SELECT first_name, concat('A', 3, 'chars') FROM customer";
    let _select = verified_only_select(sql);
}

#[test]
fn test_eof_after_as() {
    let res = parse_sql_statements("SELECT foo AS");
    assert_eq!(
        ("\
Parse error:
SELECT foo AS
             ^
Expected an identifier after AS, found: EOF"
            .to_string()),
        format!("{}", res.unwrap_err())
    );

    let res = parse_sql_statements("SELECT 1 FROM foo AS");
    assert_eq!(
        ("\
Parse error:
SELECT 1 FROM foo AS
                    ^
Expected an identifier after AS, found: EOF"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_select_count_wildcard() {
    let sql = "SELECT COUNT(*) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::new("COUNT")]),
            args: vec![Expr::Wildcard],
            over: None,
            distinct: false,
        }),
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_select_count_distinct() {
    let sql = "SELECT COUNT(DISTINCT + x) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::new("COUNT")]),
            args: vec![Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr: Box::new(Expr::Identifier(Ident::new("x")))
            }],
            over: None,
            distinct: true,
        }),
        expr_from_projection(only(&select.projection))
    );

    one_statement_parses_to(
        "SELECT COUNT(ALL + x) FROM customer",
        "SELECT COUNT(+ x) FROM customer",
    );

    let sql = "SELECT COUNT(ALL DISTINCT + x) FROM customer";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ("\
Parse error:
SELECT COUNT(ALL DISTINCT + x) FROM customer
             ^^^^^^^^^^^^
Cannot specify both ALL and DISTINCT in function: COUNT"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_parameters() {
    let select = verified_only_select("SELECT $1");
    assert_eq!(
        &Expr::Parameter(1),
        expr_from_projection(only(&select.projection)),
    );

    assert_eq!(
        Expr::BinaryOp {
            left: Box::new(Expr::Parameter(91)),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Parameter(42)),
        },
        verified_expr("$91 + $42"),
    );

    let res = parse_sql_statements("SELECT $");
    assert_eq!(
        "\
Parse error:
SELECT $
       ^
parameter marker ($) was not followed by at least one digit"
            .to_string(),
        format!("{}", res.unwrap_err())
    );

    let res = parse_sql_statements("SELECT $q");
    assert_eq!(
        "\
Parse error:
SELECT $q
       ^^
parameter marker ($) was not followed by at least one digit"
            .to_string(),
        format!("{}", res.unwrap_err())
    );

    let res = parse_sql_statements("SELECT $1$2");
    assert_eq!(
        "\
Parse error:
SELECT $1$2
         ^^
Expected end of statement, found: $2"
            .to_string(),
        format!("{}", res.unwrap_err())
    );

    let res = parse_sql_statements("SELECT $18446744073709551616");
    assert_eq!(
        "\
Parse error:
SELECT $18446744073709551616
       ^^^^^^^^^^^^^^^^^^^^^
unable to parse parameter: number too large to fit in target type"
            .to_string(),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_not() {
    let sql = "SELECT id FROM customer WHERE NOT salary = ''";
    let _ast = verified_only_select(sql);
    //TODO: add assertions
}

#[test]
fn parse_invalid_infix_not() {
    let res = parse_sql_statements("SELECT c FROM t WHERE c NOT (");
    assert_eq!(
        ("\
Parse error:
SELECT c FROM t WHERE c NOT (
                        ^^^
Expected end of statement, found: NOT"
            .to_string()),
        format!("{}", res.unwrap_err(),)
    );
}

#[test]
fn parse_collate() {
    let sql = "SELECT name COLLATE \"de_DE\" FROM customer";
    assert_matches!(
        only(&verified_only_select(sql).projection),
        SelectItem::UnnamedExpr(Expr::Collate { .. })
    );
}

#[test]
fn parse_select_string_predicate() {
    let sql = "SELECT id, fname, lname FROM customer \
               WHERE salary <> 'Not Provided' AND salary <> ''";
    let _ast = verified_only_select(sql);
    //TODO: add assertions
}

#[test]
fn parse_projection_nested_type() {
    let sql = "SELECT customer.address.state FROM foo";
    let _ast = verified_only_select(sql);
    //TODO: add assertions
}

#[test]
fn parse_null_in_select() {
    let sql = "SELECT NULL";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Null),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_escaped_single_quote_string_predicate() {
    use self::BinaryOperator::*;
    let sql = "SELECT id, fname, lname FROM customer \
               WHERE salary <> 'Jim''s salary'";
    let ast = verified_only_select(sql);
    assert_eq!(
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("salary"))),
            op: NotEq,
            right: Box::new(Expr::Value(Value::SingleQuotedString(
                "Jim's salary".to_string()
            )))
        }),
        ast.selection,
    );
}

#[test]
fn parse_number() {
    let expr = verified_expr("1.0");
    assert_eq!(expr, Expr::Value(Value::Number("1.0".into())));
}

#[test]
fn parse_numeric_begin_with_decimal() {
    let expr = verified_expr(".1");
    assert_eq!(expr, Expr::Value(Value::Number(".1".into())));
}

#[test]
fn parse_approximate_numeric_literal() {
    let expr = verified_expr("1.0E2");
    assert_eq!(expr, Expr::Value(Value::Number("1.0E2".into())));
}

#[test]
fn parse_compound_expr_1() {
    use self::BinaryOperator::*;
    use self::Expr::*;
    let sql = "a + b * c";
    assert_eq!(
        BinaryOp {
            left: Box::new(Identifier(Ident::new("a"))),
            op: Plus,
            right: Box::new(BinaryOp {
                left: Box::new(Identifier(Ident::new("b"))),
                op: Multiply,
                right: Box::new(Identifier(Ident::new("c")))
            })
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_compound_expr_2() {
    use self::BinaryOperator::*;
    use self::Expr::*;
    let sql = "a * b + c";
    assert_eq!(
        BinaryOp {
            left: Box::new(BinaryOp {
                left: Box::new(Identifier(Ident::new("a"))),
                op: Multiply,
                right: Box::new(Identifier(Ident::new("b")))
            }),
            op: Plus,
            right: Box::new(Identifier(Ident::new("c")))
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_unary_math() {
    use self::Expr::*;
    let sql = "- a + - b";
    assert_eq!(
        BinaryOp {
            left: Box::new(UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Identifier(Ident::new("a"))),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Identifier(Ident::new("b"))),
            }),
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_unary_math_precedence() {
    assert_eq!(
        verified_expr("+ a / b"),
        Expr::BinaryOp {
            left: Box::new(Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr: Box::new(Expr::Identifier(Ident::new("a"))),
            }),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Identifier(Ident::new("b"))),
        }
    )
}

#[test]
fn parse_is_null() {
    use self::Expr::*;
    let sql = "a IS NULL";
    assert_eq!(
        IsNull(Box::new(Identifier(Ident::new("a")))),
        verified_expr(sql)
    );
}

#[test]
fn parse_is_not_null() {
    use self::Expr::*;
    let sql = "a IS NOT NULL";
    assert_eq!(
        IsNotNull(Box::new(Identifier(Ident::new("a")))),
        verified_expr(sql)
    );
}

#[test]
fn parse_not_precedence() {
    // NOT has higher precedence than OR/AND, so the following must parse as (NOT true) OR true
    let sql = "NOT true OR true";
    assert_matches!(verified_expr(sql), Expr::BinaryOp {
        op: BinaryOperator::Or,
        ..
    });

    // But NOT has lower precedence than comparison operators, so the following parses as NOT (a IS NULL)
    let sql = "NOT a IS NULL";
    assert_matches!(verified_expr(sql), Expr::UnaryOp {
        op: UnaryOperator::Not,
        ..
    });

    // NOT has lower precedence than BETWEEN, so the following parses as NOT (1 NOT BETWEEN 1 AND 2)
    let sql = "NOT 1 NOT BETWEEN 1 AND 2";
    assert_eq!(
        verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Between {
                expr: Box::new(Expr::Value(number("1"))),
                low: Box::new(Expr::Value(number("1"))),
                high: Box::new(Expr::Value(number("2"))),
                negated: true,
            }),
        },
    );

    // NOT has lower precedence than LIKE, so the following parses as NOT ('a' NOT LIKE 'b')
    let sql = "NOT 'a' NOT LIKE 'b'";
    assert_eq!(
        verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(Value::SingleQuotedString("a".into()))),
                op: BinaryOperator::NotLike,
                right: Box::new(Expr::Value(Value::SingleQuotedString("b".into()))),
            }),
        },
    );

    // NOT has lower precedence than IN, so the following parses as NOT (a NOT IN 'a')
    let sql = "NOT a NOT IN ('a')";
    assert_eq!(
        verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::InList {
                expr: Box::new(Expr::Identifier("a".into())),
                list: vec![Expr::Value(Value::SingleQuotedString("a".into()))],
                negated: true,
            }),
        },
    );
}

#[test]
fn parse_like() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("name"))),
                op: if negated {
                    BinaryOperator::NotLike
                } else {
                    BinaryOperator::Like
                },
                right: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
            },
            select.selection.unwrap()
        );

        // This statement tests that LIKE and NOT LIKE have the same precedence.
        // This was previously mishandled (#81).
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::IsNull(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("name"))),
                op: if negated {
                    BinaryOperator::NotLike
                } else {
                    BinaryOperator::Like
                },
                right: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
            })),
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_in_list() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE segment {}IN ('HIGH', 'MED')",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::InList {
                expr: Box::new(Expr::Identifier(Ident::new("segment"))),
                list: vec![
                    Expr::Value(Value::SingleQuotedString("HIGH".to_string())),
                    Expr::Value(Value::SingleQuotedString("MED".to_string())),
                ],
                negated,
            },
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_in_subquery() {
    let sql = "SELECT * FROM customers WHERE segment IN (SELECT segm FROM bar)";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::InSubquery {
            expr: Box::new(Expr::Identifier(Ident::new("segment"))),
            subquery: Box::new(verified_query("SELECT segm FROM bar")),
            negated: false,
        },
        select.selection.unwrap()
    );
}

#[test]
fn parse_between() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE age {}BETWEEN 25 AND 32",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::Between {
                expr: Box::new(Expr::Identifier(Ident::new("age"))),
                low: Box::new(Expr::Value(number("25"))),
                high: Box::new(Expr::Value(number("32"))),
                negated,
            },
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_between_with_expr() {
    use self::BinaryOperator::*;
    let sql = "SELECT * FROM t WHERE 1 BETWEEN 1 + 2 AND 3 + 4 IS NULL";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::IsNull(Box::new(Expr::Between {
            expr: Box::new(Expr::Value(number("1"))),
            low: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(number("1"))),
                op: Plus,
                right: Box::new(Expr::Value(number("2"))),
            }),
            high: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(number("3"))),
                op: Plus,
                right: Box::new(Expr::Value(number("4"))),
            }),
            negated: false,
        })),
        select.selection.unwrap()
    );

    let sql = "SELECT * FROM t WHERE 1 = 1 AND 1 + x BETWEEN 1 AND 2";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(number("1"))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(number("1"))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::Between {
                expr: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Value(number("1"))),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Identifier(Ident::new("x"))),
                }),
                low: Box::new(Expr::Value(number("1"))),
                high: Box::new(Expr::Value(number("2"))),
                negated: false,
            }),
        },
        select.selection.unwrap(),
    )
}

#[test]
fn parse_select_order_by() {
    fn chk(sql: &str) {
        let select = verified_query(sql);
        assert_eq!(
            vec![
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("lname")),
                    asc: Some(true),
                },
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("fname")),
                    asc: Some(false),
                },
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("id")),
                    asc: None,
                },
            ],
            select.order_by
        );
    }
    chk("SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY lname ASC, fname DESC, id");
    // make sure ORDER is not treated as an alias
    chk("SELECT id, fname, lname FROM customer ORDER BY lname ASC, fname DESC, id");
    chk("SELECT 1 AS lname, 2 AS fname, 3 AS id, 4 ORDER BY lname ASC, fname DESC, id");
}

#[test]
fn parse_select_order_by_limit() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id < 5 \
               ORDER BY lname ASC, fname DESC LIMIT 2";
    let select = verified_query(sql);
    assert_eq!(
        vec![
            OrderByExpr {
                expr: Expr::Identifier(Ident::new("lname")),
                asc: Some(true),
            },
            OrderByExpr {
                expr: Expr::Identifier(Ident::new("fname")),
                asc: Some(false),
            },
        ],
        select.order_by
    );
    assert_eq!(Some(Expr::Value(number("2"))), select.limit);
}

#[test]
fn parse_select_group_by() {
    let sql = "SELECT id, fname, lname FROM customer GROUP BY lname, fname";
    let select = verified_only_select(sql);
    assert_eq!(
        vec![
            Expr::Identifier(Ident::new("lname")),
            Expr::Identifier(Ident::new("fname")),
        ],
        select.group_by
    );
}

#[test]
fn parse_select_having() {
    let sql = "SELECT foo FROM bar GROUP BY foo HAVING COUNT(*) > 1";
    let select = verified_only_select(sql);
    assert_eq!(
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Function(Function {
                name: ObjectName(vec![Ident::new("COUNT")]),
                args: vec![Expr::Wildcard],
                over: None,
                distinct: false
            })),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Value(number("1")))
        }),
        select.having
    );

    let sql = "SELECT 'foo' HAVING 1 = 1";
    let select = verified_only_select(sql);
    assert!(select.having.is_some());
}

#[test]
fn parse_limit_accepts_all() {
    one_statement_parses_to(
        "SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT ALL",
        "SELECT id, fname, lname FROM customer WHERE id = 1",
    );
}

#[test]
fn parse_cast() {
    let sql = "SELECT CAST(id AS bigint) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            expr: Box::new(Expr::Identifier(Ident::new("id"))),
            data_type: DataType::BigInt
        },
        expr_from_projection(only(&select.projection))
    );
    one_statement_parses_to(
        "SELECT CAST(id AS BIGINT) FROM customer",
        "SELECT CAST(id AS bigint) FROM customer",
    );

    verified_stmt("SELECT CAST(id AS numeric) FROM customer");

    one_statement_parses_to(
        "SELECT CAST(id AS dec) FROM customer",
        "SELECT CAST(id AS numeric) FROM customer",
    );

    one_statement_parses_to(
        "SELECT CAST(id AS decimal) FROM customer",
        "SELECT CAST(id AS numeric) FROM customer",
    );
}

#[test]
fn parse_array_datatype() {
    let sql = "SELECT CAST('{{1,2},{3,4}}' AS int ARRAY)";
    let select = unverified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            expr: Box::new(Expr::Value(Value::SingleQuotedString(
                "{{1,2},{3,4}}".to_owned()
            ))),
            data_type: DataType::Array(Box::new(DataType::Int)),
        },
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_extract() {
    let sql = "SELECT EXTRACT(YEAR FROM d)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Extract {
            field: ExtractField::Year,
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
        },
        expr_from_projection(only(&select.projection)),
    );

    one_statement_parses_to("SELECT EXTRACT(year from d)", "SELECT EXTRACT(YEAR FROM d)");

    for extract_field in &[
        "MILLENIUM",
        "CENTURY",
        "DECADE",
        "YEAR",
        "ISOYEAR",
        "QUARTER",
        "MONTH",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "MILLISECONDS",
        "MICROSECONDS",
        "TIMEZONE",
        "TIMEZONE_HOUR",
        "TIMEZONE_MINUTE",
        "WEEK",
        "DOY",
        "DOW",
        "ISODOW",
        "EPOCH",
    ] {
        let unquoted = format!("SELECT EXTRACT({} FROM d)", extract_field);
        verified_stmt(&unquoted);
        // Quoted strings do not re-serialize to the same thing that they were
        // parsed from, while not ideal databases only supports literal
        // strings, (not column values) for `extract`, so it's not like this
        // can be confused for a column
        one_statement_parses_to(
            &format!("SELECT EXTRACT('{}' FROM d)", extract_field),
            &unquoted,
        );
    }

    let res = parse_sql_statements("SELECT EXTRACT(MILLISECOND FROM d)");
    assert_eq!(
        ("\
Parse error:
SELECT EXTRACT(MILLISECOND FROM d)
               ^^^^^^^^^^^
Expected valid extract field, found: MILLISECOND"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_create_table() {
    let sql = "CREATE TABLE uk_cities (\
               name VARCHAR(100) NOT NULL,\
               lat DOUBLE NULL,\
               lng DOUBLE,
               constrained INT NULL CONSTRAINT pkey PRIMARY KEY NOT NULL UNIQUE CHECK (constrained > 0),
               ref INT REFERENCES othertable (a, b))";
    let ast = one_statement_parses_to(
        sql,
        "CREATE TABLE uk_cities (\
         name character varying(100) NOT NULL, \
         lat double NULL, \
         lng double, \
         constrained int NULL CONSTRAINT pkey PRIMARY KEY NOT NULL UNIQUE CHECK (constrained > 0), \
         ref int REFERENCES othertable (a, b))",
    );
    match ast {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            with_options,
            if_not_exists,
        } => {
            assert_eq!("uk_cities", name.to_string());
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: "name".into(),
                        data_type: DataType::Varchar(Some(100)),
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull
                        }],
                    },
                    ColumnDef {
                        name: "lat".into(),
                        data_type: DataType::Double,
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null
                        }],
                    },
                    ColumnDef {
                        name: "lng".into(),
                        data_type: DataType::Double,
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: "constrained".into(),
                        data_type: DataType::Int,
                        collation: None,
                        options: vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Null
                            },
                            ColumnOptionDef {
                                name: Some("pkey".into()),
                                option: ColumnOption::Unique { is_primary: true }
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Unique { is_primary: false },
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Check(verified_expr("constrained > 0")),
                            }
                        ],
                    },
                    ColumnDef {
                        name: "ref".into(),
                        data_type: DataType::Int,
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::ForeignKey {
                                foreign_table: ObjectName(vec!["othertable".into()]),
                                referred_columns: vec!["a".into(), "b".into(),],
                            }
                        }]
                    }
                ]
            );
            assert!(constraints.is_empty());
            assert_eq!(with_options, vec![]);
            assert!(!if_not_exists);
        }
        _ => unreachable!(),
    }

    let res = parse_sql_statements("CREATE TABLE t (a int NOT NULL GARBAGE)");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected column option, found: GARBAGE"));
}

#[test]
fn parse_create_table_with_options() {
    let sql = "CREATE TABLE t (c int) WITH (foo = 'bar', a = 123)";
    match verified_stmt(sql) {
        Statement::CreateTable { with_options, .. } => {
            assert_eq!(
                vec![
                    SqlOption {
                        name: "foo".into(),
                        value: Value::SingleQuotedString("bar".into())
                    },
                    SqlOption {
                        name: "a".into(),
                        value: number("123")
                    },
                ],
                with_options
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_trailing_comma() {
    let sql = "CREATE TABLE foo (bar int,)";
    one_statement_parses_to(sql, "CREATE TABLE foo (bar int)");
}

#[test]
fn parse_create_table_empty() {
    // Zero-column tables are weird, but supported by at least PostgreSQL.
    let _ = verified_stmt("CREATE TABLE t ()");
}

#[test]
fn parse_alter_table_constraints() {
    check_one("CONSTRAINT address_pkey PRIMARY KEY (address_id)");
    check_one("CONSTRAINT uk_task UNIQUE (report_date, task_id)");
    check_one(
        "CONSTRAINT customer_address_id_fkey FOREIGN KEY (address_id) \
         REFERENCES public.address(address_id)",
    );
    check_one("CONSTRAINT ck CHECK (rtrim(ltrim(REF_CODE)) <> '')");

    check_one("PRIMARY KEY (foo, bar)");
    check_one("UNIQUE (id)");
    check_one("FOREIGN KEY (foo, bar) REFERENCES AnotherTable(foo, bar)");
    check_one("CHECK (end_date > start_date OR end_date IS NULL)");

    fn check_one(constraint_text: &str) {
        match verified_stmt(&format!("ALTER TABLE tab ADD {}", constraint_text)) {
            Statement::AlterTable {
                name,
                operation: AlterTableOperation::AddConstraint(constraint),
            } => {
                assert_eq!("tab", name.to_string());
                assert_eq!(constraint_text, constraint.to_string());
            }
            _ => unreachable!(),
        }
        verified_stmt(&format!("CREATE TABLE foo (id int, {})", constraint_text));
    }
}

#[test]
fn parse_bad_constraint() {
    let res = parse_sql_statements("ALTER TABLE tab ADD");
    assert_eq!(
        ("\
Parse error:
ALTER TABLE tab ADD
                   ^
Expected a constraint in ALTER TABLE .. ADD, found: EOF"
            .to_string()),
        format!("{}", res.unwrap_err())
    );

    let res = parse_sql_statements("CREATE TABLE tab (foo int,");
    assert_eq!(
        ("\
Parse error:
CREATE TABLE tab (foo int,
                          ^
Expected column name or constraint definition, found: EOF"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_scalar_function_in_projection() {
    let sql = "SELECT sqrt(id) FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::new("sqrt")]),
            args: vec![Expr::Identifier(Ident::new("id"))],
            over: None,
            distinct: false,
        }),
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_window_functions() {
    let sql = "SELECT row_number() OVER (ORDER BY dt DESC), \
               sum(foo) OVER (PARTITION BY a, b ORDER BY c, d \
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), \
               avg(bar) OVER (ORDER BY a \
               RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING), \
               max(baz) OVER (ORDER BY a \
               ROWS UNBOUNDED PRECEDING) \
               FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(4, select.projection.len());
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::new("row_number")]),
            args: vec![],
            over: Some(WindowSpec {
                partition_by: vec![],
                order_by: vec![OrderByExpr {
                    expr: Expr::Identifier(Ident::new("dt")),
                    asc: Some(false)
                }],
                window_frame: None,
            }),
            distinct: false,
        }),
        expr_from_projection(&select.projection[0])
    );
}

#[test]
fn parse_aggregate_with_group_by() {
    let sql = "SELECT a, COUNT(1), MIN(b), MAX(b) FROM foo GROUP BY a";
    let _ast = verified_only_select(sql);
    //TODO: assertions
}

#[test]
fn parse_literal_decimal() {
    // These numbers were explicitly chosen to not roundtrip if represented as
    // f64s (i.e., as 64-bit binary floating point numbers).
    let sql = "SELECT 0.300000000000000004, 9007199254740993.0";
    let select = verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Value(number("0.300000000000000004")),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::Value(number("9007199254740993.0")),
        expr_from_projection(&select.projection[1]),
    )
}

#[test]
fn parse_literal_string() {
    let sql = "SELECT 'one', X'deadBEEF'";
    let select = verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Value(Value::SingleQuotedString("one".to_string())),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value(Value::HexStringLiteral("deadBEEF".to_string())),
        expr_from_projection(&select.projection[1])
    );

    one_statement_parses_to("SELECT x'deadBEEF'", "SELECT X'deadBEEF'");
}

// Covers all of the timelike types besides interval. Note that parsing does not
// validate the contents of timelike types; that functionality is covered by
// strconv.
#[test]
fn parse_literal_timelike() {
    let sql = "SELECT DATE '1999-01-01'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Date("1999-01-01".into())),
        expr_from_projection(only(&select.projection)),
    );
    let sql = "SELECT DATE 'invalid date'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Date("invalid date".into())),
        expr_from_projection(only(&select.projection)),
    );
    let sql = "SELECT TIME '01:23:34'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Time("01:23:34".into(),)),
        expr_from_projection(only(&select.projection)),
    );
    let sql = "SELECT TIME 'invalid time'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Time("invalid time".into(),)),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT TIMESTAMP '1999-01-01 01:23:34.555'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Timestamp("1999-01-01 01:23:34.555".into(),)),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT TIMESTAMP 'invalid timestamp'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Timestamp("invalid timestamp".into(),)),
        expr_from_projection(only(&select.projection)),
    );
    let time_formats = [
        "TIMESTAMP",
        "TIMESTAMPTZ",
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP WITHOUT TIME ZONE",
    ];

    #[rustfmt::skip]
    let test_cases = vec!(
        "1999-01-01 01:23:34.555",
        "invalid timestamptx"
    );

    for test in test_cases.iter() {
        for format in time_formats.iter() {
            let sql = format!("SELECT {} '{}'", format, test);
            println!("{}", sql);
            let select = unverified_only_select(&sql);

            if *format == "TIMESTAMPTZ" || *format == "TIMESTAMP WITH TIME ZONE" {
                let value = Value::TimestampTz((*test).to_string());
                assert_eq!(
                    &Expr::Value(value),
                    expr_from_projection(only(&select.projection))
                );
            } else {
                let value = Value::Timestamp((*test).to_string());
                assert_eq!(
                    &Expr::Value(value),
                    expr_from_projection(only(&select.projection))
                );
            }
        }
    }
}

#[test]
fn parse_literal_interval_monthlike() {
    // Ambiguous parts get parsed in the PostgreSQL formatting style,
    // which sets fractional parts to 0.
    let mut iv = IntervalValue::default();
    iv.value = "1".into();
    iv.precision_low = DateTimeField::Year;
    verify_interval("SELECT INTERVAL '1' YEAR", iv.clone(), None);

    iv.value = "invalid interval".into();
    verify_interval("SELECT INTERVAL 'invalid interval' YEAR", iv.clone(), None);

    iv.value = "1 year".into();
    iv.precision_low = DateTimeField::Second;
    verify_interval("SELECT INTERVAL '1 year'", iv, None);

    let mut iv = IntervalValue::default();
    iv.value = "1".into();
    iv.precision_low = DateTimeField::Month;
    verify_interval("SELECT INTERVAL '1' MONTH", iv.clone(), None);

    iv.value = "1 month".into();
    iv.precision_low = DateTimeField::Second;
    verify_interval("SELECT INTERVAL '1 month'", iv, None);
    let mut iv = IntervalValue::default();
    iv.value = "1-1".into();
    verify_interval("SELECT INTERVAL '1-1'", iv.clone(), None);
    iv.value = "1 year 1 month".into();
    verify_interval("SELECT INTERVAL '1 year 1 month'", iv, None);
}

#[test]
fn parse_literal_interval_durationlike() {
    use DateTimeField::*;

    verify_interval(
        "SELECT INTERVAL '10' DAY",
        IntervalValue {
            value: "10".into(),
            precision_low: Day,
            ..Default::default()
        },
        None,
    );

    verify_interval(
        "SELECT INTERVAL '10' HOUR",
        IntervalValue {
            value: "10".into(),
            precision_low: Hour,
            ..Default::default()
        },
        None,
    );

    verify_interval(
        "SELECT INTERVAL '10' MINUTE",
        IntervalValue {
            value: "10".into(),
            precision_low: Minute,
            ..Default::default()
        },
        None,
    );

    verify_interval(
        "SELECT INTERVAL '10'",
        IntervalValue {
            value: "10".into(),
            ..Default::default()
        },
        None,
    );

    verify_interval(
        "SELECT INTERVAL '0.01'",
        IntervalValue {
            value: "0.01".into(),
            ..Default::default()
        },
        None,
    );

    verify_interval(
        "SELECT INTERVAL '1 1:1:1.1'",
        IntervalValue {
            value: "1 1:1:1.1".to_string(),
            ..Default::default()
        },
        None,
    );

    // Truncate Y-M + M:S.NS.
    verify_interval(
        "SELECT INTERVAL '1 4:5' DAY TO HOUR",
        IntervalValue {
            value: "1 4:5".into(),
            precision_high: Day,
            precision_low: Hour,
            ..Default::default()
        },
        None,
    );

    // Truncate Y-M + M:S.NS.
    verify_interval(
        "SELECT INTERVAL 'invalid interval' DAY TO HOUR",
        IntervalValue {
            value: "invalid interval".into(),
            precision_high: Day,
            precision_low: Hour,
            ..Default::default()
        },
        None,
    );

    verify_interval(
        "SELECT INTERVAL '01:01:01.111111111' SECOND (5)",
        IntervalValue {
            value: "01:01:01.111111111".to_string(),
            fsec_max_precision: Some(5),
            ..Default::default()
        },
        None,
    );
}

#[test]
fn parse_literal_interval_with_fsec_max_precision_errors() {
    // Cannot apply precision to non-second element.
    verify_interval(
        "SELECT INTERVAL '01:01.01' MINUTE (5) TO SECOND (5)",
        IntervalValue {
            ..Default::default()
        },
        Some(
            "\
Parse error:
SELECT INTERVAL '01:01.01' MINUTE (5) TO SECOND (5)
                                  ^
Expected end of statement, found: (",
        ),
    );

    // Only supports precision for trailing second.
    verify_interval(
        "SELECT INTERVAL '1' SECOND (5, 4)",
        IntervalValue {
            ..Default::default()
        },
        Some(
            "\
Parse error:
SELECT INTERVAL '1' SECOND (5, 4)
                             ^
Expected ), found: ,",
        ),
    );

    // Only supports precision for trailing second.
    verify_interval(
        "SELECT INTERVAL '10' SECOND (1) TO SECOND",
        IntervalValue {
            ..Default::default()
        },
        Some(
            "\
Parse error:
SELECT INTERVAL '10' SECOND (1) TO SECOND
                                   ^^^^^^
Expected end of statement, found: SECOND",
        ),
    );
}

#[test]
fn parse_simple_math_expr_plus() {
    let sql = "SELECT a + b, 2 + a, 2.5 + a, a_f + b_f, 2 + a_f, 2.5 + a_f FROM c";
    verified_only_select(sql);
}

#[test]
fn parse_simple_math_expr_minus() {
    let sql = "SELECT a - b, 2 - a, 2.5 - a, a_f - b_f, 2 - a_f, 2.5 - a_f FROM c";
    verified_only_select(sql);
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = verified_only_select(
        r#"SELECT "alias"."bar baz", "myfun"(), "simple id" AS "column alias" FROM "a table" AS "alias""#,
    );
    // check FROM
    match only(select.from).relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
        } => {
            assert_eq!(vec![Ident::with_quote('"', "a table")], name.0);
            assert_eq!(Ident::with_quote('"', "alias"), alias.unwrap().name);
            assert!(args.is_empty());
            assert!(with_hints.is_empty());
        }
        _ => panic!("Expecting TableFactor::Table"),
    }
    // check SELECT
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::CompoundIdentifier(vec![
            Ident::with_quote('"', "alias"),
            Ident::with_quote('"', "bar baz")
        ]),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::with_quote('"', "myfun")]),
            args: vec![],
            over: None,
            distinct: false,
        }),
        expr_from_projection(&select.projection[1]),
    );
    match &select.projection[2] {
        SelectItem::ExprWithAlias { expr, alias } => {
            assert_eq!(&Expr::Identifier(Ident::with_quote('"', "simple id")), expr);
            assert_eq!(&Ident::with_quote('"', "column alias"), alias);
        }
        _ => panic!("Expected ExprWithAlias"),
    }

    verified_stmt(r#"CREATE TABLE "foo" ("bar" int)"#);
    verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn parse_parens() {
    use self::BinaryOperator::*;
    use self::Expr::*;
    let sql = "(a + b) - (c + d)";
    assert_eq!(
        BinaryOp {
            left: Box::new(Nested(Box::new(BinaryOp {
                left: Box::new(Identifier(Ident::new("a"))),
                op: Plus,
                right: Box::new(Identifier(Ident::new("b")))
            }))),
            op: Minus,
            right: Box::new(Nested(Box::new(BinaryOp {
                left: Box::new(Identifier(Ident::new("c"))),
                op: Plus,
                right: Box::new(Identifier(Ident::new("d")))
            })))
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_searched_case_expr() {
    let sql = "SELECT CASE WHEN bar IS NULL THEN 'null' WHEN bar = 0 THEN '=0' WHEN bar >= 0 THEN '>=0' ELSE '<0' END FROM foo";
    use self::BinaryOperator::*;
    use self::Expr::{BinaryOp, Case, Identifier, IsNull};
    let select = verified_only_select(sql);
    assert_eq!(
        &Case {
            operand: None,
            conditions: vec![
                IsNull(Box::new(Identifier(Ident::new("bar")))),
                BinaryOp {
                    left: Box::new(Identifier(Ident::new("bar"))),
                    op: Eq,
                    right: Box::new(Expr::Value(number("0")))
                },
                BinaryOp {
                    left: Box::new(Identifier(Ident::new("bar"))),
                    op: GtEq,
                    right: Box::new(Expr::Value(number("0")))
                }
            ],
            results: vec![
                Expr::Value(Value::SingleQuotedString("null".to_string())),
                Expr::Value(Value::SingleQuotedString("=0".to_string())),
                Expr::Value(Value::SingleQuotedString(">=0".to_string()))
            ],
            else_result: Some(Box::new(Expr::Value(Value::SingleQuotedString(
                "<0".to_string()
            ))))
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_show_databases() {
    assert_eq!(
        verified_stmt("SHOW DATABASES"),
        Statement::ShowDatabases { filter: None }
    );

    assert_eq!(
        verified_stmt("SHOW DATABASES LIKE 'blah'"),
        Statement::ShowDatabases {
            filter: Some(ShowStatementFilter::Like("blah".into())),
        }
    );
}

#[test]
fn parse_show_objects() {
    let trials = [
        ("SCHEMAS", ObjectType::Schema),
        ("SOURCES", ObjectType::Source),
        ("VIEWS", ObjectType::View),
        ("TABLES", ObjectType::Table),
        ("SINKS", ObjectType::Sink),
    ];

    for &(sql, object_type) in &trials {
        assert_eq!(
            verified_stmt(&format!("SHOW {}", sql)),
            Statement::ShowObjects {
                object_type,
                extended: false,
                full: false,
                from: None,
                materialized: false,
                filter: None
            }
        );

        assert_eq!(
            verified_stmt(&format!("SHOW {} FROM foo.bar", sql)),
            Statement::ShowObjects {
                object_type,
                extended: false,
                full: false,
                materialized: false,
                from: Some(ObjectName(vec!["foo".into(), "bar".into()])),
                filter: None,
            }
        );
    }
}

#[test]
fn parse_show_objects_with_like_regex() {
    let sql = "SHOW TABLES LIKE '%foo%'";
    match verified_stmt(sql) {
        Statement::ShowObjects {
            object_type,
            extended: false,
            full: false,
            from: None,
            materialized: false,
            filter,
        } => {
            assert_eq!(filter.unwrap(), ShowStatementFilter::Like("%foo%".into()));
            assert_eq!(ObjectType::Table, object_type);
        }
        _ => panic!("invalid SHOW OBJECTS statement"),
    }
}

#[test]
fn parse_show_objects_with_full() {
    let canonical_sql = "SHOW FULL VIEWS";
    assert_eq!(
        verified_stmt(&canonical_sql),
        Statement::ShowObjects {
            object_type: ObjectType::View,
            extended: false,
            full: true,
            materialized: false,
            from: None,
            filter: None,
        }
    );
    let canonical_sql = "SHOW FULL TABLES";
    assert_eq!(
        verified_stmt(&canonical_sql),
        Statement::ShowObjects {
            object_type: ObjectType::Table,
            extended: false,
            full: true,
            materialized: false,
            from: None,
            filter: None,
        }
    );
}

#[test]
fn parse_show_sources() {
    let sql = "SHOW SOURCES";
    assert_eq!(
        verified_stmt(&sql),
        Statement::ShowObjects {
            object_type: ObjectType::Source,
            extended: false,
            full: false,
            materialized: false,
            from: None,
            filter: None,
        }
    );
    let sql = "SHOW MATERIALIZED SOURCES FROM foo";
    assert_eq!(
        verified_stmt(&sql),
        Statement::ShowObjects {
            object_type: ObjectType::Source,
            extended: false,
            full: false,
            materialized: true,
            from: Some(ObjectName(vec!["foo".into()])),
            filter: None,
        }
    );
    let sql = "SHOW FULL MATERIALIZED SOURCES FROM baz";
    assert_eq!(
        verified_stmt(&sql),
        Statement::ShowObjects {
            object_type: ObjectType::Source,
            extended: false,
            full: true,
            materialized: true,
            from: Some(ObjectName(vec!["baz".into()])),
            filter: None,
        }
    );
    let sql = "SHOW FULL SOURCES";
    assert_eq!(
        verified_stmt(&sql),
        Statement::ShowObjects {
            object_type: ObjectType::Source,
            extended: false,
            full: true,
            materialized: false,
            from: None,
            filter: None,
        }
    );
}

#[test]
fn parse_show_materialized_views() {
    let sql = "SHOW MATERIALIZED VIEWS FROM foo";
    assert_eq!(
        verified_stmt(&sql),
        Statement::ShowObjects {
            object_type: ObjectType::View,
            extended: false,
            full: false,
            materialized: true,
            from: Some(ObjectName(vec!["foo".into()])),
            filter: None,
        }
    );
    let sql = "SHOW FULL MATERIALIZED VIEWS";
    assert_eq!(
        verified_stmt(&sql),
        Statement::ShowObjects {
            object_type: ObjectType::View,
            extended: false,
            full: true,
            materialized: true,
            from: None,
            filter: None,
        }
    );
    let sql = "SHOW MATERIALIZED VIEWS FROM bar.foo LIKE '%hello%'";
    match verified_stmt(&sql) {
        Statement::ShowObjects {
            object_type,
            extended: false,
            full: false,
            from,
            materialized: true,
            filter,
        } => {
            assert_eq!(from, Some(ObjectName(vec!["bar".into(), "foo".into()])));
            assert_eq!(filter.unwrap(), ShowStatementFilter::Like("%hello%".into()));
            assert_eq!(ObjectType::View, object_type);
        }
        _ => panic!("invalid SHOW MATERIALIZED VIEWS statement"),
    }
}

#[test]
fn parse_show_indexes() {
    let canonical_sql = "SHOW INDEXES FROM foo";
    assert_eq!(
        verified_stmt(&canonical_sql),
        Statement::ShowIndexes {
            table_name: ObjectName(vec!["foo".into()]),
            extended: false,
            filter: None,
        }
    );
    one_statement_parses_to("SHOW INDEXES IN foo", &canonical_sql);
    let index_alias = ["INDEX", "KEYS"];
    let from_alias = ["FROM", "IN"];
    for i in &index_alias {
        for f in &from_alias {
            let sql = format!("SHOW {} {} foo", i, f);
            one_statement_parses_to(&sql, &canonical_sql);
        }
    }
}

#[test]
fn parse_show_indexes_with_extended() {
    let canonical_sql = "SHOW EXTENDED INDEXES FROM foo";
    assert_eq!(
        verified_stmt(&canonical_sql),
        Statement::ShowIndexes {
            table_name: ObjectName(vec!["foo".into()]),
            extended: true,
            filter: None,
        }
    );
}

#[test]
fn parse_show_indexes_with_where_expr() {
    let canonical_sql = "SHOW INDEXES FROM foo WHERE index_name = 'bar'";
    match verified_stmt(canonical_sql) {
        Statement::ShowIndexes {
            table_name,
            extended: _,
            filter,
        } => {
            assert_eq!(
                filter.unwrap(),
                ShowStatementFilter::Where(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("index_name"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("bar".to_string()))),
                })
            );
            assert_eq!(table_name, ObjectName(vec!["foo".into()]));
        }
        _ => panic!("invalid SHOW INDEXES statement"),
    }
    one_statement_parses_to(
        "SHOW INDEXES IN foo WHERE index_name = 'bar'",
        &canonical_sql,
    );
    let index_alias = ["INDEX", "KEYS"];
    let from_alias = ["FROM", "IN"];
    for i in &index_alias {
        for f in &from_alias {
            let sql = format!("SHOW {} {} foo WHERE index_name = 'bar'", i, f);
            one_statement_parses_to(&sql, &canonical_sql);
        }
    }
}

#[test]
fn parse_show_create_view() {
    assert_eq!(
        verified_stmt("SHOW CREATE VIEW foo"),
        Statement::ShowCreateView {
            view_name: ObjectName(vec!["foo".into()])
        }
    )
}

#[test]
fn parse_show_create_source() {
    assert_eq!(
        verified_stmt("SHOW CREATE SOURCE foo"),
        Statement::ShowCreateSource {
            source_name: ObjectName(vec!["foo".into()])
        }
    )
}

#[test]
fn parse_show_create_sink() {
    assert_eq!(
        verified_stmt("SHOW CREATE SINK foo"),
        Statement::ShowCreateSink {
            sink_name: ObjectName(vec!["foo".into()])
        }
    )
}

#[test]
fn parse_simple_case_expr() {
    // ANSI calls a CASE expression with an operand "<simple case>"
    let sql = "SELECT CASE foo WHEN 1 THEN 'Y' ELSE 'N' END";
    let select = verified_only_select(sql);
    use self::Expr::{Case, Identifier};
    assert_eq!(
        &Case {
            operand: Some(Box::new(Identifier(Ident::new("foo")))),
            conditions: vec![Expr::Value(number("1"))],
            results: vec![Expr::Value(Value::SingleQuotedString("Y".to_string())),],
            else_result: Some(Box::new(Expr::Value(Value::SingleQuotedString(
                "N".to_string()
            ))))
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_from_advanced() {
    let sql = "SELECT * FROM fn(1, 2) AS foo, schema.bar AS bar WITH (NOLOCK)";
    let _select = verified_only_select(sql);
}

#[test]
fn parse_implicit_join() {
    let sql = "SELECT * FROM t1, t2";
    let select = verified_only_select(sql);
    assert_eq!(
        vec![
            TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t1".into()]),
                    alias: None,
                    args: vec![],
                    with_hints: vec![],
                },
                joins: vec![],
            },
            TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t2".into()]),
                    alias: None,
                    args: vec![],
                    with_hints: vec![],
                },
                joins: vec![],
            }
        ],
        select.from,
    );

    let sql = "SELECT * FROM t1a NATURAL JOIN t1b, t2a NATURAL JOIN t2b";
    let select = verified_only_select(sql);
    assert_eq!(
        vec![
            TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t1a".into()]),
                    alias: None,
                    args: vec![],
                    with_hints: vec![],
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: ObjectName(vec!["t1b".into()]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![],
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::Natural),
                }]
            },
            TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t2a".into()]),
                    alias: None,
                    args: vec![],
                    with_hints: vec![],
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: ObjectName(vec!["t2b".into()]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![],
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::Natural),
                }]
            }
        ],
        select.from,
    );
}

#[test]
fn parse_cross_join() {
    let sql = "SELECT * FROM t1 CROSS JOIN t2";
    let select = verified_only_select(sql);
    assert_eq!(
        Join {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new("t2")]),
                alias: None,
                args: vec![],
                with_hints: vec![],
            },
            join_operator: JoinOperator::CrossJoin
        },
        only(only(select.from).joins),
    );
}

fn table_alias(name: impl Into<String>) -> Option<TableAlias> {
    Some(TableAlias {
        name: Ident::new(name),
        columns: vec![],
    })
}

#[test]
fn parse_joins_on() {
    fn join_with_constraint(
        relation: impl Into<String>,
        alias: Option<TableAlias>,
        f: impl Fn(JoinConstraint) -> JoinOperator,
    ) -> Join {
        Join {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new(relation.into())]),
                alias,
                args: vec![],
                with_hints: vec![],
            },
            join_operator: f(JoinConstraint::On(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("c1".into())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Identifier("c2".into())),
            })),
        }
    }
    // Test parsing of aliases
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2").from).joins,
        vec![join_with_constraint(
            "t2",
            table_alias("foo"),
            JoinOperator::Inner
        )]
    );
    one_statement_parses_to(
        "SELECT * FROM t1 JOIN t2 foo ON c1 = c2",
        "SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2",
    );
    // Test parsing of different join operators
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::Inner)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 FULL JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::FullOuter)]
    );
}

#[test]
fn parse_joins_using() {
    fn join_with_constraint(
        relation: impl Into<String>,
        alias: Option<TableAlias>,
        f: impl Fn(JoinConstraint) -> JoinOperator,
    ) -> Join {
        Join {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new(relation.into())]),
                alias,
                args: vec![],
                with_hints: vec![],
            },
            join_operator: f(JoinConstraint::Using(vec!["c1".into()])),
        }
    }
    // Test parsing of aliases
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 AS foo USING(c1)").from).joins,
        vec![join_with_constraint(
            "t2",
            table_alias("foo"),
            JoinOperator::Inner
        )]
    );
    one_statement_parses_to(
        "SELECT * FROM t1 JOIN t2 foo USING(c1)",
        "SELECT * FROM t1 JOIN t2 AS foo USING(c1)",
    );
    // Test parsing of different join operators
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::Inner)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 FULL JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::FullOuter)]
    );
}

#[test]
fn parse_natural_join() {
    fn natural_join(f: impl Fn(JoinConstraint) -> JoinOperator) -> Join {
        Join {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new("t2")]),
                alias: None,
                args: vec![],
                with_hints: vec![],
            },
            join_operator: f(JoinConstraint::Natural),
        }
    }
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL JOIN t2").from).joins,
        vec![natural_join(JoinOperator::Inner)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL LEFT JOIN t2").from).joins,
        vec![natural_join(JoinOperator::LeftOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL RIGHT JOIN t2").from).joins,
        vec![natural_join(JoinOperator::RightOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL FULL JOIN t2").from).joins,
        vec![natural_join(JoinOperator::FullOuter)]
    );

    let sql = "SELECT * FROM t1 natural";
    assert_eq!(
        ("\
Parse error:
SELECT * FROM t1 natural
                        ^
Expected a join type after NATURAL, found: EOF"
            .to_string()),
        parse_sql_statements(sql).unwrap_err().to_string(),
    );
}

#[test]
fn parse_complex_join() {
    let sql = "SELECT c1, c2 FROM t1, t4 JOIN t2 ON t2.c = t1.c LEFT JOIN t3 USING(q, c) WHERE t4.c = t1.c";
    verified_only_select(sql);
}

#[test]
fn parse_join_nesting() {
    fn table(name: impl Into<String>) -> TableFactor {
        TableFactor::Table {
            name: ObjectName(vec![Ident::new(name.into())]),
            alias: None,
            args: vec![],
            with_hints: vec![],
        }
    }

    fn join(relation: TableFactor) -> Join {
        Join {
            relation,
            join_operator: JoinOperator::Inner(JoinConstraint::Natural),
        }
    }

    macro_rules! nest {
        ($base:expr $(, $join:expr)*) => {
            TableFactor::NestedJoin(Box::new(TableWithJoins {
                relation: $base,
                joins: vec![$(join($join)),*]
            }))
        };
    }

    let sql = "SELECT * FROM a NATURAL JOIN (b NATURAL JOIN (c NATURAL JOIN d NATURAL JOIN e)) \
               NATURAL JOIN (f NATURAL JOIN (g NATURAL JOIN h))";
    assert_eq!(
        only(&verified_only_select(sql).from).joins,
        vec![
            join(nest!(table("b"), nest!(table("c"), table("d"), table("e")))),
            join(nest!(table("f"), nest!(table("g"), table("h"))))
        ],
    );

    let sql = "SELECT * FROM (a NATURAL JOIN b) NATURAL JOIN c";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(from.relation, nest!(table("a"), table("b")));
    assert_eq!(from.joins, vec![join(table("c"))]);

    let sql = "SELECT * FROM (((a NATURAL JOIN b)))";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(from.relation, nest!(nest!(nest!(table("a"), table("b")))));
    assert_eq!(from.joins, vec![]);

    let sql = "SELECT * FROM a NATURAL JOIN (((b NATURAL JOIN c)))";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(from.relation, table("a"));
    assert_eq!(
        from.joins,
        vec![join(nest!(nest!(nest!(table("b"), table("c")))))]
    );

    let res = parse_sql_statements("SELECT * FROM (a NATURAL JOIN (b))");
    assert_eq!(
        ("\
Parse error:
SELECT * FROM (a NATURAL JOIN (b))
                                ^
Expected joined table, found: )"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_join_syntax_variants() {
    one_statement_parses_to(
        "SELECT c1 FROM t1 INNER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 JOIN t2 USING(c1)",
    );
    one_statement_parses_to(
        "SELECT c1 FROM t1 LEFT OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 LEFT JOIN t2 USING(c1)",
    );
    one_statement_parses_to(
        "SELECT c1 FROM t1 RIGHT OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 RIGHT JOIN t2 USING(c1)",
    );
    one_statement_parses_to(
        "SELECT c1 FROM t1 FULL OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 FULL JOIN t2 USING(c1)",
    );

    let res = parse_sql_statements("SELECT * FROM a OUTER JOIN b ON 1");
    assert_eq!(
        ("\
Parse error:
SELECT * FROM a OUTER JOIN b ON 1
                ^^^^^
Expected LEFT, RIGHT, or FULL, found: OUTER"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_ctes() {
    let cte_sqls = vec!["SELECT 1 AS foo", "SELECT 2 AS bar"];
    let with = &format!(
        "WITH a AS ({}), b AS ({}) SELECT foo + bar FROM a, b",
        cte_sqls[0], cte_sqls[1]
    );

    fn assert_ctes_in_select(expected: &[&str], sel: &Query) {
        let mut i = 0;
        for exp in expected {
            let Cte { alias, query } = &sel.ctes[i];
            assert_eq!(*exp, query.to_string());
            assert_eq!(
                if i == 0 {
                    Ident::new("a")
                } else {
                    Ident::new("b")
                },
                alias.name
            );
            assert!(alias.columns.is_empty());
            i += 1;
        }
    }

    // Top-level CTE
    assert_ctes_in_select(&cte_sqls, &verified_query(with));
    // CTE in a subquery
    let sql = &format!("SELECT ({})", with);
    let select = verified_only_select(sql);
    match expr_from_projection(only(&select.projection)) {
        Expr::Subquery(ref subquery) => {
            assert_ctes_in_select(&cte_sqls, subquery.as_ref());
        }
        _ => panic!("Expected subquery"),
    }
    // CTE in a derived table
    let sql = &format!("SELECT * FROM ({})", with);
    let select = verified_only_select(sql);
    match only(select.from).relation {
        TableFactor::Derived { subquery, .. } => {
            assert_ctes_in_select(&cte_sqls, subquery.as_ref())
        }
        _ => panic!("Expected derived table"),
    }
    // CTE in a view
    let sql = &format!("CREATE VIEW v AS {}", with);
    match verified_stmt(sql) {
        Statement::CreateView { query, .. } => assert_ctes_in_select(&cte_sqls, &query),
        _ => panic!("Expected CREATE VIEW"),
    }
    // CTE in a CTE...
    let sql = &format!("WITH outer_cte AS ({}) SELECT * FROM outer_cte", with);
    let select = verified_query(sql);
    assert_ctes_in_select(&cte_sqls, &only(&select.ctes).query);
}

#[test]
fn parse_cte_renamed_columns() {
    let sql = "WITH cte (col1, col2) AS (SELECT foo, bar FROM baz) SELECT * FROM cte";
    let query = verified_query(sql);
    assert_eq!(
        vec![Ident::new("col1"), Ident::new("col2")],
        query.ctes.first().unwrap().alias.columns
    );
}

#[test]
fn parse_derived_tables() {
    let sql = "SELECT a.x, b.y FROM (SELECT x FROM foo) AS a CROSS JOIN (SELECT y FROM bar) AS b";
    let _ = verified_only_select(sql);
    //TODO: add assertions

    let sql = "SELECT a.x, b.y \
               FROM (SELECT x FROM foo) AS a (x) \
               CROSS JOIN (SELECT y FROM bar) AS b (y)";
    let _ = verified_only_select(sql);
    //TODO: add assertions

    let sql = "SELECT * FROM (((SELECT 1)))";
    let _ = verified_only_select(sql);
    // TODO: add assertions

    let sql = "SELECT * FROM t NATURAL JOIN (((SELECT 1)))";
    let _ = verified_only_select(sql);
    // TODO: add assertions

    let sql = "SELECT * FROM (((SELECT 1) UNION (SELECT 2)) AS t1 NATURAL JOIN t2)";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(
        from.relation,
        TableFactor::NestedJoin(Box::new(TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(verified_query("(SELECT 1) UNION (SELECT 2)")),
                alias: Some(TableAlias {
                    name: "t1".into(),
                    columns: vec![],
                })
            },
            joins: vec![Join {
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t2".into()]),
                    alias: None,
                    args: vec![],
                    with_hints: vec![],
                },
                join_operator: JoinOperator::Inner(JoinConstraint::Natural),
            }],
        }))
    );

    let res = parse_sql_statements("SELECT * FROM ((SELECT 1) AS t)");
    assert_eq!(
        ("\
Parse error:
SELECT * FROM ((SELECT 1) AS t)
                              ^
Expected joined table, found: )"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_union() {
    // TODO: add assertions
    verified_stmt("SELECT 1 UNION SELECT 2");
    verified_stmt("SELECT 1 UNION ALL SELECT 2");
    verified_stmt("SELECT 1 EXCEPT SELECT 2");
    verified_stmt("SELECT 1 EXCEPT ALL SELECT 2");
    verified_stmt("SELECT 1 INTERSECT SELECT 2");
    verified_stmt("SELECT 1 INTERSECT ALL SELECT 2");
    verified_stmt("SELECT 1 UNION SELECT 2 UNION SELECT 3");
    verified_stmt("SELECT 1 EXCEPT SELECT 2 UNION SELECT 3"); // Union[Except[1,2], 3]
    verified_stmt("SELECT 1 INTERSECT (SELECT 2 EXCEPT SELECT 3)");
    verified_stmt("WITH cte AS (SELECT 1 AS foo) (SELECT foo FROM cte ORDER BY 1 LIMIT 1)");
    verified_stmt("SELECT 1 UNION (SELECT 2 ORDER BY 1 LIMIT 1)");
    verified_stmt("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3"); // Union[1, Intersect[2,3]]
    verified_stmt("SELECT foo FROM tab UNION SELECT bar FROM TAB");
    verified_stmt("(SELECT * FROM new EXCEPT SELECT * FROM old) UNION ALL (SELECT * FROM old EXCEPT SELECT * FROM new) ORDER BY 1");
}

#[test]
fn parse_values() {
    verified_stmt("SELECT * FROM (VALUES (1), (2), (3))");
    verified_stmt("SELECT * FROM (VALUES (1), (2), (3)), (VALUES (1, 2, 3))");
    verified_stmt("SELECT * FROM (VALUES (1)) UNION VALUES (1)");
}

#[test]
fn parse_multiple_statements() {
    fn test_with(sql1: &str, sql2_kw: &str, sql2_rest: &str) {
        // Check that a string consisting of two statements delimited by a semicolon
        // parses the same as both statements individually:
        let res = parse_sql_statements(&(sql1.to_owned() + ";" + sql2_kw + sql2_rest));
        assert_eq!(
            vec![
                one_statement_parses_to(&sql1, ""),
                one_statement_parses_to(&(sql2_kw.to_owned() + sql2_rest), ""),
            ],
            res.unwrap()
        );
        // Check that extra semicolon at the end is stripped by normalization:
        one_statement_parses_to(&(sql1.to_owned() + ";"), sql1);
        // Check that forgetting the semicolon results in an error:
        let res = parse_sql_statements(&(sql1.to_owned() + " " + sql2_kw + sql2_rest));
        assert!(format!("{}", res.unwrap_err())
            .contains(&format!("Expected end of statement, found: {}", sql2_kw)));
    }
    test_with("SELECT foo", "SELECT", " bar");
    // ensure that SELECT/WITH is not parsed as a table or column alias if ';'
    // separating the statements is omitted:
    test_with("SELECT foo FROM baz", "SELECT", " bar");
    test_with("SELECT foo", "WITH", " cte AS (SELECT 1 AS s) SELECT bar");
    test_with(
        "SELECT foo FROM baz",
        "WITH",
        " cte AS (SELECT 1 AS s) SELECT bar",
    );
    test_with("DELETE FROM foo", "SELECT", " bar");
    test_with("INSERT INTO foo VALUES (1)", "SELECT", " bar");
    test_with("CREATE TABLE foo (baz int)", "SELECT", " bar");
    // Make sure that empty statements do not cause an error:
    let res = parse_sql_statements(";;");
    assert_eq!(0, res.unwrap().len());
}

#[test]
fn parse_scalar_subqueries() {
    let sql = "(SELECT 1) + (SELECT 2)";
    assert_matches!(verified_expr(sql), Expr::BinaryOp {
        op: BinaryOperator::Plus, ..
        //left: box Subquery { .. },
        //right: box Subquery { .. },
    });
}

#[test]
fn parse_any_some_all() {
    let sql = "1 < ANY (SELECT 2)";
    assert_matches!(verified_expr(sql), Expr::Any {
        op: BinaryOperator::Lt, ..
        //left: box Expr,
        //right: box Query { .. },
    });

    let sql = "1 < SOME (SELECT 2)";
    assert_matches!(verified_expr(sql), Expr::Any {
        op: BinaryOperator::Lt, ..
        //left: box Expr,
        //right: box Query { .. },
    });

    let sql = "1 < ALL (SELECT 2)";
    assert_matches!(verified_expr(sql), Expr::All {
        op: BinaryOperator::Lt, ..
        //left: box Expr,
        //right: box Query { .. },
    });

    let res = parse_sql_statements("SELECT 1 WHERE 1 < ANY SELECT 2");
    assert_eq!(
        ("\
Parse error:
SELECT 1 WHERE 1 < ANY SELECT 2
                       ^^^^^^
Expected (, found: SELECT"
            .to_string()),
        format!("{}", res.unwrap_err())
    );

    let res = parse_sql_statements("SELECT 1 WHERE 1 < NONE (SELECT 2)");
    assert_eq!(
        // TODO this is a pretty unhelpful error - it started parsing "NONE (SELECT" as applying the function NONE to the argument SELECT
        ("\
Parse error:
SELECT 1 WHERE 1 < NONE (SELECT 2)
                                ^
Expected ), found: 2"
            .to_string()),
        format!("{}", res.unwrap_err())
    );

    let res = parse_sql_statements("SELECT 1 WHERE 1 < ANY (SELECT 2");
    assert_eq!(
        ("\
Parse error:
SELECT 1 WHERE 1 < ANY (SELECT 2
                                ^
Expected ), found: EOF"
            .to_string()),
        format!("{}", res.unwrap_err())
    );

    let res = parse_sql_statements("SELECT 1 WHERE 1 + ANY (SELECT 2)");
    assert_eq!(
        ("\
Parse error:
SELECT 1 WHERE 1 + ANY (SELECT 2)
                 ^
Expected comparison operator, found: +"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_exists_subquery() {
    let expected_inner = verified_query("SELECT 1");
    let sql = "SELECT * FROM t WHERE EXISTS (SELECT 1)";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::Exists(Box::new(expected_inner.clone())),
        select.selection.unwrap(),
    );

    let sql = "SELECT * FROM t WHERE NOT EXISTS (SELECT 1)";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Exists(Box::new(expected_inner))),
        },
        select.selection.unwrap(),
    );

    verified_stmt("SELECT * FROM t WHERE EXISTS (WITH u AS (SELECT 1) SELECT * FROM u)");
    verified_stmt("SELECT EXISTS (SELECT 1)");

    let res = parse_sql_statements("SELECT EXISTS (");
    assert_eq!(
        ("\
Parse error:
SELECT EXISTS (
               ^
Expected SELECT, VALUES, or a subquery in the query body, found: EOF"
            .to_string()),
        format!("{}", res.unwrap_err(),)
    );

    let res = parse_sql_statements("SELECT EXISTS (NULL)");
    assert_eq!(
        ("\
Parse error:
SELECT EXISTS (NULL)
               ^^^^
Expected SELECT, VALUES, or a subquery in the query body, found: NULL"
            .to_string()),
        format!("{}", res.unwrap_err(),)
    );
}

#[test]
fn parse_create_database() {
    match verified_stmt("CREATE DATABASE foo") {
        Statement::CreateDatabase {
            name,
            if_not_exists,
        } => {
            assert_eq!(name.to_string(), "foo");
            assert!(!if_not_exists);
        }
        _ => unreachable!(),
    }

    match verified_stmt("CREATE DATABASE IF NOT EXISTS foo") {
        Statement::CreateDatabase {
            name,
            if_not_exists,
        } => {
            assert_eq!(name.to_string(), "foo");
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    }

    let res = parse_sql_statements("CREATE DATABASE IF EXISTS foo");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected NOT, found: EXISTS"));

    let res = parse_sql_statements("CREATE DATABASE foo.bar");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected end of statement, found: ."));
}

#[test]
fn parse_create_schema() {
    match verified_stmt("CREATE SCHEMA foo.bar") {
        Statement::CreateSchema {
            name,
            if_not_exists,
        } => {
            assert_eq!(name.to_string(), "foo.bar");
            assert!(!if_not_exists);
        }
        _ => unreachable!(),
    }

    match verified_stmt("CREATE SCHEMA IF NOT EXISTS foo") {
        Statement::CreateSchema {
            name,
            if_not_exists,
        } => {
            assert_eq!(name.to_string(), "foo");
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    }

    let res = parse_sql_statements("CREATE SCHEMA IF EXISTS foo");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected NOT, found: EXISTS"));
}

#[test]
fn parse_create_view() {
    let sql = "CREATE VIEW myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            if_exists,
            with_options,
        } => {
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(Vec::<Ident>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(!materialized);
            assert_eq!(if_exists, IfExistsBehavior::Error);
            assert_eq!(with_options, vec![]);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_or_replace_view() {
    let sql = "CREATE OR REPLACE VIEW v AS SELECT 1";
    match verified_stmt(sql) {
        Statement::CreateView { if_exists, .. } => assert_eq!(if_exists, IfExistsBehavior::Replace),
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view_if_not_exists() {
    let sql = "CREATE VIEW IF NOT EXISTS v AS SELECT 1";
    match verified_stmt(sql) {
        Statement::CreateView { if_exists, .. } => assert_eq!(if_exists, IfExistsBehavior::Skip),
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_or_replace_view_if_not_exists() {
    let res = parse_sql_statements("CREATE OR REPLACE VIEW IF NOT EXISTS v AS SELECT 1");
    assert_eq!(
        res.unwrap_err().to_string(),
        "Parse error:
CREATE OR REPLACE VIEW IF NOT EXISTS v AS SELECT 1
                          ^^^
Expected AS, found: NOT"
    );
}

#[test]
fn parse_create_view_with_options() {
    let sql = "CREATE VIEW v WITH (foo = 'bar', a = 123) AS SELECT 1";
    match verified_stmt(sql) {
        Statement::CreateView { with_options, .. } => {
            assert_eq!(
                vec![
                    SqlOption {
                        name: "foo".into(),
                        value: Value::SingleQuotedString("bar".into())
                    },
                    SqlOption {
                        name: "a".into(),
                        value: number("123")
                    },
                ],
                with_options
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view_with_columns() {
    let sql = "CREATE VIEW v (has, cols) AS SELECT 1, 2";
    match verified_stmt(sql) {
        Statement::CreateView {
            name,
            columns,
            with_options,
            query,
            materialized,
            if_exists,
        } => {
            assert_eq!("v", name.to_string());
            assert_eq!(columns, vec![Ident::new("has"), Ident::new("cols")]);
            assert_eq!(with_options, vec![]);
            assert_eq!("SELECT 1, 2", query.to_string());
            assert!(!materialized);
            assert_eq!(if_exists, IfExistsBehavior::Error);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_materialized_view() {
    let sql = "CREATE MATERIALIZED VIEW myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            if_exists,
            with_options,
        } => {
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(Vec::<Ident>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(materialized);
            assert_eq!(if_exists, IfExistsBehavior::Error);
            assert_eq!(with_options, vec![]);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_materialized_view_if_not_exists() {
    let sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateView {
            name, if_exists, ..
        } => {
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(if_exists, IfExistsBehavior::Skip);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_source_inline_schema() {
    let sql = "CREATE SOURCE foo FROM FILE 'bar' FORMAT AVRO USING SCHEMA 'baz'";
    match verified_stmt(sql) {
        Statement::CreateSource {
            name,
            connector,
            with_options,
            format,
            envelope,
            if_not_exists,
            materialized,
        } => {
            assert_eq!("foo", name.to_string());
            assert_eq!(Connector::File { path: "bar".into() }, connector);
            assert!(with_options.is_empty());
            assert_eq!(
                Format::Avro(AvroSchema::Schema(Schema::Inline("baz".into()))),
                format.unwrap()
            );
            assert_eq!(Envelope::None, envelope);
            assert!(!if_not_exists);
            assert!(!materialized);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_source_kafka() {
    let sql = "CREATE SOURCE foo \
               FROM KAFKA BROKER 'bar' TOPIC 'baz' WITH (consistency = 'lug', ssl_certificate_file = '/Path/to/file') \
               FORMAT BYTES";
    match verified_stmt(sql) {
        Statement::CreateSource {
            name,
            connector,
            with_options,
            format,
            envelope,
            if_not_exists,
            materialized,
        } => {
            assert_eq!("foo", name.to_string());
            assert_eq!(
                Connector::Kafka {
                    broker: "bar".into(),
                    topic: "baz".into(),
                },
                connector
            );
            assert_eq!(
                vec![
                    SqlOption {
                        name: "consistency".into(),
                        value: Value::SingleQuotedString("lug".into()),
                    },
                    SqlOption {
                        name: "ssl_certificate_file".into(),
                        value: Value::SingleQuotedString("/Path/to/file".into()),
                    },
                ],
                with_options
            );
            assert_eq!(Format::Bytes, format.unwrap());
            assert_eq!(Envelope::None, envelope);
            assert!(!if_not_exists);
            assert!(!materialized);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_source_file_schema_protobuf_multiple_args() {
    let sql = "CREATE MATERIALIZED SOURCE foo FROM FILE 'bar' \
               FORMAT PROTOBUF MESSAGE 'somemessage' USING SCHEMA FILE 'path'";
    match verified_stmt(sql) {
        Statement::CreateSource {
            name,
            connector,
            with_options,
            format,
            envelope,
            if_not_exists,
            materialized,
        } => {
            assert_eq!("foo", name.to_string());
            assert_eq!(Connector::File { path: "bar".into() }, connector);
            assert!(with_options.is_empty());
            assert_eq!(
                Format::Protobuf {
                    message_name: "somemessage".into(),
                    schema: Schema::File("path".into())
                },
                format.unwrap()
            );
            assert_eq!(Envelope::None, envelope);
            assert!(!if_not_exists);
            assert!(materialized);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_source_regex() {
    let sql = "CREATE SOURCE IF NOT EXISTS foo \
               FROM FILE 'bar' WITH (tail = true) \
               FORMAT REGEX '(asdf)|(jkl)'";
    match verified_stmt(sql) {
        Statement::CreateSource {
            name,
            connector,
            with_options,
            format,
            envelope,
            if_not_exists,
            materialized,
        } => {
            assert_eq!("foo", name.to_string());
            assert_eq!(Connector::File { path: "bar".into() }, connector);
            assert_eq!(
                vec![SqlOption {
                    name: "tail".into(),
                    value: Value::Boolean(true)
                }],
                with_options,
            );
            assert_eq!(Format::Regex("(asdf)|(jkl)".into()), format.unwrap());
            assert_eq!(Envelope::None, envelope);
            assert!(if_not_exists);
            assert!(!materialized);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_source_csv() {
    let sql = "CREATE SOURCE foo \
               FROM FILE 'bar' WITH (tail = false) \
               FORMAT CSV WITH 3 COLUMNS";
    match verified_stmt(sql) {
        Statement::CreateSource {
            name,
            connector,
            with_options,
            format,
            envelope,
            if_not_exists,
            materialized,
        } => {
            assert_eq!("foo", name.to_string());
            assert_eq!(Connector::File { path: "bar".into() }, connector);
            assert_eq!(
                vec![SqlOption {
                    name: "tail".into(),
                    value: Value::Boolean(false)
                }],
                with_options
            );
            assert_eq!(
                Format::Csv {
                    n_cols: 3,
                    delimiter: ','
                },
                format.unwrap()
            );
            assert_eq!(Envelope::None, envelope);
            assert!(!if_not_exists);
            assert!(!materialized);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_source_csv_custom_delim() {
    let sql = "CREATE SOURCE foo \
               FROM FILE 'bar' WITH (tail = true) \
               FORMAT CSV WITH 3 COLUMNS DELIMITED BY '|'";
    match verified_stmt(sql) {
        Statement::CreateSource {
            name,
            connector,
            with_options,
            format,
            envelope,
            if_not_exists,
            materialized,
        } => {
            assert_eq!("foo", name.to_string());
            assert_eq!(Connector::File { path: "bar".into() }, connector);
            assert_eq!(
                vec![SqlOption {
                    name: "tail".into(),
                    value: Value::Boolean(true)
                }],
                with_options
            );
            assert_eq!(
                Format::Csv {
                    n_cols: 3,
                    delimiter: '|'
                },
                format.unwrap()
            );
            assert_eq!(Envelope::None, envelope);
            assert!(!if_not_exists);
            assert!(!materialized);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_materialized_invalid() {
    let sql = "CREATE MATERIALIZED OR VIEW foo as SELECT * from bar";
    let err = parse_sql_statements(sql).unwrap_err();
    assert_eq!(
        "Expected VIEW or SOURCE after CREATE MATERIALIZED, found: OR",
        err.message
    );
}

#[test]
fn parse_avro_object() {
    let sql = "CREATE SOURCE foo FROM AVRO OCF '/tmp/bar'";
    match verified_stmt(sql) {
        Statement::CreateSource {
            name,
            connector,
            format,
            with_options,
            ..
        } => {
            assert_eq!("foo", name.to_string());
            assert_eq!(
                Connector::AvroOcf {
                    path: "/tmp/bar".into(),
                },
                connector
            );
            assert!(with_options.is_empty());
            assert_eq!(None, format);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_source_registry() {
    let sql = "CREATE SOURCE foo FROM FILE 'bar' \
               FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:8081' \
               ENVELOPE DEBEZIUM";
    match verified_stmt(sql) {
        Statement::CreateSource {
            name,
            connector,
            with_options,
            format,
            envelope,
            if_not_exists,
            materialized,
        } => {
            assert_eq!("foo", name.to_string());
            assert_eq!(Connector::File { path: "bar".into() }, connector);
            assert!(with_options.is_empty());
            assert_eq!(
                Format::Avro(AvroSchema::CsrUrl {
                    url: "http://localhost:8081".into(),
                    seed: None,
                }),
                format.unwrap()
            );
            assert!(!if_not_exists);
            assert_eq!(Envelope::Debezium, envelope);
            assert!(!materialized);
        }
        _ => unreachable!(),
    }

    let sql = "CREATE SOURCE foo FROM FILE 'bar' \
               FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:8081' \
               SEED VALUE SCHEMA 'blah'";
    match verified_stmt(sql) {
        Statement::CreateSource { format, .. } => {
            assert_eq!(
                Format::Avro(AvroSchema::CsrUrl {
                    url: "http://localhost:8081".into(),
                    seed: Some(CsrSeed {
                        key_schema: None,
                        value_schema: "blah".into(),
                    }),
                }),
                format.unwrap()
            );
        }
        _ => unreachable!(),
    }

    let sql = "CREATE SOURCE foo FROM FILE 'bar' \
               FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:8081' \
               SEED KEY SCHEMA 'a' VALUE SCHEMA 'b'";
    match verified_stmt(sql) {
        Statement::CreateSource { format, .. } => {
            assert_eq!(
                Format::Avro(AvroSchema::CsrUrl {
                    url: "http://localhost:8081".into(),
                    seed: Some(CsrSeed {
                        key_schema: Some("a".into()),
                        value_schema: "b".into(),
                    }),
                }),
                format.unwrap()
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_source_default_envelope() {
    let sql = "CREATE SOURCE foo FROM FILE 'bar' \
               FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:8081'";
    match verified_stmt(sql) {
        Statement::CreateSource { envelope, .. } => {
            assert_eq!(Envelope::None, envelope);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_source_if_not_exists() {
    let sql = "CREATE SOURCE IF NOT EXISTS foo FROM FILE 'bar' FORMAT BYTES";
    match verified_stmt(sql) {
        Statement::CreateSource { if_not_exists, .. } => {
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    }

    let res = parse_sql_statements("CREATE SOURCE IF EXISTS foo FROM FILE 'bar' USING SCHEMA ''");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected NOT, found: EXISTS"));
}

#[test]
fn parse_create_sink() {
    let sql = "CREATE SINK foo FROM bar INTO FILE 'baz' FORMAT BYTES";
    match verified_stmt(sql) {
        Statement::CreateSink {
            name,
            from,
            connector,
            format,
            if_not_exists,
        } => {
            assert_eq!("foo", name.to_string());
            assert_eq!("bar", from.to_string());
            assert_eq!(Connector::File { path: "baz".into() }, connector);
            assert_eq!(Format::Bytes, format);
            assert!(!if_not_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_sink_if_not_exists() {
    let sql = "CREATE SINK IF NOT EXISTS foo FROM bar INTO FILE 'baz' FORMAT BYTES";
    match verified_stmt(sql) {
        Statement::CreateSink { if_not_exists, .. } => {
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    }

    let res = parse_sql_statements("CREATE SINK IF EXISTS foo FROM bar INTO 'baz'");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected NOT, found: EXISTS"));
}

#[test]
fn parse_create_index() {
    let sql = "CREATE INDEX foo ON myschema.bar (a, b)";
    match verified_stmt(sql) {
        Statement::CreateIndex {
            name,
            on_name,
            key_parts,
            if_not_exists,
        } => {
            assert_eq!("foo", name.to_string());
            assert_eq!("myschema.bar", on_name.to_string());
            assert_eq!(
                key_parts,
                vec![
                    Expr::Identifier(Ident::new("a")),
                    Expr::Identifier(Ident::new("b"))
                ]
            );
            assert!(!if_not_exists);
        }
        _ => unreachable!(),
    }

    let sql = "CREATE INDEX fizz ON baz (ascii(x), a IS NOT NULL, (EXISTS (SELECT y FROM boop WHERE boop.z = z)), delta)";
    match verified_stmt(sql) {
        Statement::CreateIndex {
            name,
            on_name,
            key_parts,
            if_not_exists,
        } => {
            assert_eq!("fizz", name.to_string());
            assert_eq!("baz", on_name.to_string());
            assert_matches!(key_parts[0], Expr::Function(..));
            assert_eq!(
                key_parts[1],
                Expr::IsNotNull(Box::new(Expr::Identifier(Ident::new("a"))))
            );
            if let Expr::Nested(expr) = &key_parts[2] {
                assert_matches!(**expr, Expr::Exists(..));
            } else {
                unreachable!();
            }
            assert_eq!(key_parts[3], Expr::Identifier(Ident::new("delta")));
            assert!(!if_not_exists);
        }
        _ => unreachable!(),
    }

    let sql = "CREATE INDEX ind ON tab ((col + 1))";
    match verified_stmt(sql) {
        Statement::CreateIndex {
            name,
            on_name,
            key_parts,
            if_not_exists,
        } => {
            assert_eq!("ind", name.to_string());
            assert_eq!("tab", on_name.to_string());
            assert_eq!(
                key_parts,
                vec![Expr::Nested(Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("col"))),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Value(Value::Number("1".to_string())))
                }))],
            );
            assert!(!if_not_exists);
        }
        _ => unreachable!(),
    }
    let sql = "CREATE INDEX qualifiers ON no_parentheses (alpha.omega)";
    match verified_stmt(sql) {
        Statement::CreateIndex {
            name,
            on_name,
            key_parts,
            if_not_exists,
        } => {
            assert_eq!("qualifiers", name.to_string());
            assert_eq!("no_parentheses", on_name.to_string());
            assert_eq!(
                key_parts,
                vec![Expr::CompoundIdentifier(vec![
                    Ident::new("alpha"),
                    Ident::new("omega"),
                ])],
            );
            assert!(!if_not_exists);
        }
        _ => unreachable!(),
    }
    let sql = "CREATE INDEX IF NOT EXISTS foo ON bar (baz)";
    match verified_stmt(sql) {
        Statement::CreateIndex { if_not_exists, .. } => {
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_invalid_create_index() {
    // Index names should not have a schema in front of it
    let res = parse_sql_statements("CREATE INDEX myschema.ind ON foo(b)");
    assert_eq!(
        ("\
Parse error:
CREATE INDEX myschema.ind ON foo(b)
                     ^
Expected ON, found: ."
            .to_string()),
        format!("{}", res.unwrap_err())
    );

    let res = parse_sql_statements("CREATE INDEX IF EXISTS myschema.ind ON foo(b)");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected NOT, found: EXISTS"));
}

#[test]
fn parse_drop_database() {
    match verified_stmt("DROP DATABASE mydb") {
        Statement::DropDatabase { name, if_exists } => {
            assert_eq!(name.to_string(), "mydb");
            assert!(!if_exists);
        }
        _ => unreachable!(),
    }

    match verified_stmt("DROP DATABASE IF EXISTS mydb") {
        Statement::DropDatabase { name, if_exists } => {
            assert_eq!(name.to_string(), "mydb");
            assert!(if_exists);
        }
        _ => unreachable!(),
    }

    let res = parse_sql_statements("DROP DATABASE mydb.nope");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected end of statement, found: ."));
}

#[test]
fn parse_drop_schema() {
    let sql = "DROP SCHEMA mydb.myschema";
    match verified_stmt(sql) {
        Statement::DropObjects {
            names, object_type, ..
        } => {
            assert_eq!(
                vec!["mydb.myschema"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert_eq!(ObjectType::Schema, object_type);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_table() {
    let sql = "DROP TABLE foo";
    match verified_stmt(sql) {
        Statement::DropObjects {
            object_type,
            if_exists,
            names,
            cascade,
        } => {
            assert_eq!(false, if_exists);
            assert_eq!(ObjectType::Table, object_type);
            assert_eq!(
                vec!["foo"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert_eq!(false, cascade);
        }
        _ => unreachable!(),
    }

    let sql = "DROP TABLE IF EXISTS foo, bar CASCADE";
    match verified_stmt(sql) {
        Statement::DropObjects {
            object_type,
            if_exists,
            names,
            cascade,
        } => {
            assert_eq!(true, if_exists);
            assert_eq!(ObjectType::Table, object_type);
            assert_eq!(
                vec!["foo", "bar"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert_eq!(true, cascade);
        }
        _ => unreachable!(),
    }

    let sql = "DROP TABLE";
    assert_eq!(
        ("\
Parse error:
DROP TABLE
          ^
Expected identifier, found: EOF"
            .to_string()),
        parse_sql_statements(sql).unwrap_err().to_string(),
    );

    let sql = "DROP TABLE IF EXISTS foo, bar CASCADE RESTRICT";
    assert_eq!(
        ("\
Parse error:
DROP TABLE IF EXISTS foo, bar CASCADE RESTRICT
                              ^^^^^^^^^^^^^^^^
Cannot specify both CASCADE and RESTRICT in DROP"
            .to_string()),
        parse_sql_statements(sql).unwrap_err().to_string(),
    );
}

#[test]
fn parse_drop_view() {
    let sql = "DROP VIEW myschema.myview";
    match verified_stmt(sql) {
        Statement::DropObjects {
            names, object_type, ..
        } => {
            assert_eq!(
                vec!["myschema.myview"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert_eq!(ObjectType::View, object_type);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_source() {
    let sql = "DROP SOURCE myschema.mydatasource";
    match verified_stmt(sql) {
        Statement::DropObjects {
            object_type,
            if_exists,
            names,
            cascade,
        } => {
            assert_eq!(false, if_exists);
            assert_eq!(ObjectType::Source, object_type);
            assert_eq!(
                vec!["myschema.mydatasource"],
                names.iter().map(|n| n.to_string()).collect::<Vec<_>>()
            );
            assert_eq!(false, cascade);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_index() {
    let sql = "DROP INDEX IF EXISTS myschema.myindex";
    match verified_stmt(sql) {
        Statement::DropObjects {
            object_type,
            if_exists,
            names,
            cascade,
        } => {
            assert_eq!(true, if_exists);
            assert_eq!(
                vec!["myschema.myindex"],
                names.iter().map(|n| n.to_string()).collect::<Vec<_>>()
            );
            assert_eq!(false, cascade);
            assert_eq!(ObjectType::Index, object_type);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_tail() {
    let sql = "TAIL foo.bar";
    match verified_stmt(sql) {
        Statement::Tail { name } => {
            assert_eq!("foo.bar", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_invalid_subquery_without_parens() {
    let res = parse_sql_statements("SELECT SELECT 1 FROM bar WHERE 1=1 FROM baz");
    assert_eq!(
        ("\
Parse error:
SELECT SELECT 1 FROM bar WHERE 1=1 FROM baz
              ^
Expected end of statement, found: 1"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_offset() {
    let ast = verified_query("SELECT foo FROM bar OFFSET 2 ROWS");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));
    let ast = verified_query("SELECT foo FROM bar WHERE foo = 4 OFFSET 2 ROWS");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));
    let ast = verified_query("SELECT foo FROM bar ORDER BY baz OFFSET 2 ROWS");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));
    let ast = verified_query("SELECT foo FROM bar WHERE foo = 4 ORDER BY baz OFFSET 2 ROWS");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));
    let ast = verified_query("SELECT foo FROM (SELECT * FROM bar OFFSET 2 ROWS) OFFSET 2 ROWS");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));
    match ast.body {
        SetExpr::Select(s) => match only(s.from).relation {
            TableFactor::Derived { subquery, .. } => {
                assert_eq!(subquery.offset, Some(Expr::Value(number("2"))));
            }
            _ => panic!("Test broke"),
        },
        _ => panic!("Test broke"),
    }
    let ast = verified_query("SELECT 'foo' OFFSET 0 ROWS");
    assert_eq!(ast.offset, Some(Expr::Value(number("0"))));
}

#[test]
fn parse_offset_no_row() {
    // Use unverified_query as the Query formatter is set to print ROWS by default
    let ast = unverified_query("SELECT foo FROM bar OFFSET 2");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));

    let ast = unverified_query("SELECT foo FROM bar WHERE foo = 4 OFFSET 2");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));

    let ast = unverified_query("SELECT foo FROM bar ORDER BY baz OFFSET 2 ");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));
    let ast = unverified_query("SELECT foo FROM bar WHERE foo = 4 ORDER BY baz OFFSET 2 ");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));
    let ast = unverified_query("SELECT foo FROM (SELECT * FROM bar OFFSET 2) OFFSET 2 ");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));
    let ast = unverified_query("SELECT foo FROM (SELECT * FROM bar OFFSET 2 ROWS) OFFSET 2 ");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));
    let ast = unverified_query("SELECT foo FROM (SELECT * FROM bar OFFSET 2) OFFSET 2 ROWS ");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));
    match ast.body {
        SetExpr::Select(s) => match only(s.from).relation {
            TableFactor::Derived { subquery, .. } => {
                assert_eq!(subquery.offset, Some(Expr::Value(number("2"))));
            }
            _ => panic!("Test broke"),
        },
        _ => panic!("Test broke"),
    }
    let ast = unverified_query("SELECT 'foo' OFFSET 0");
    assert_eq!(ast.offset, Some(Expr::Value(number("0"))));
}

#[test]
fn parse_singular_row_offset() {
    one_statement_parses_to(
        "SELECT foo FROM bar OFFSET 1 ROW",
        "SELECT foo FROM bar OFFSET 1 ROWS",
    );
}

#[test]
fn parse_fetch() {
    let fetch_first_two_rows_only = Some(Fetch {
        with_ties: false,
        percent: false,
        quantity: Some(Expr::Value(number("2"))),
    });
    let ast = verified_query("SELECT foo FROM bar FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query("SELECT 'foo' FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query("SELECT foo FROM bar FETCH FIRST ROWS ONLY");
    assert_eq!(
        ast.fetch,
        Some(Fetch {
            with_ties: false,
            percent: false,
            quantity: None,
        })
    );
    let ast = verified_query("SELECT foo FROM bar WHERE foo = 4 FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query("SELECT foo FROM bar ORDER BY baz FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query(
        "SELECT foo FROM bar WHERE foo = 4 ORDER BY baz FETCH FIRST 2 ROWS WITH TIES",
    );
    assert_eq!(
        ast.fetch,
        Some(Fetch {
            with_ties: true,
            percent: false,
            quantity: Some(Expr::Value(number("2"))),
        })
    );
    let ast = verified_query("SELECT foo FROM bar FETCH FIRST 50 PERCENT ROWS ONLY");
    assert_eq!(
        ast.fetch,
        Some(Fetch {
            with_ties: false,
            percent: true,
            quantity: Some(Expr::Value(number("50"))),
        })
    );
    let ast = verified_query(
        "SELECT foo FROM bar WHERE foo = 4 ORDER BY baz OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY",
    );
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query(
        "SELECT foo FROM (SELECT * FROM bar FETCH FIRST 2 ROWS ONLY) FETCH FIRST 2 ROWS ONLY",
    );
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    match ast.body {
        SetExpr::Select(s) => match only(s.from).relation {
            TableFactor::Derived { subquery, .. } => {
                assert_eq!(subquery.fetch, fetch_first_two_rows_only);
            }
            _ => panic!("Test broke"),
        },
        _ => panic!("Test broke"),
    }
    let ast = verified_query("SELECT foo FROM (SELECT * FROM bar OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY) OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.offset, Some(Expr::Value(number("2"))));
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    match ast.body {
        SetExpr::Select(s) => match only(s.from).relation {
            TableFactor::Derived { subquery, .. } => {
                assert_eq!(subquery.offset, Some(Expr::Value(number("2"))));
                assert_eq!(subquery.fetch, fetch_first_two_rows_only);
            }
            _ => panic!("Test broke"),
        },
        _ => panic!("Test broke"),
    }
}

#[test]
fn parse_fetch_variations() {
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH FIRST 10 ROW ONLY",
        "SELECT foo FROM bar FETCH FIRST 10 ROWS ONLY",
    );
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH NEXT 10 ROW ONLY",
        "SELECT foo FROM bar FETCH FIRST 10 ROWS ONLY",
    );
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH NEXT 10 ROWS WITH TIES",
        "SELECT foo FROM bar FETCH FIRST 10 ROWS WITH TIES",
    );
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH NEXT ROWS WITH TIES",
        "SELECT foo FROM bar FETCH FIRST ROWS WITH TIES",
    );
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH FIRST ROWS ONLY",
        "SELECT foo FROM bar FETCH FIRST ROWS ONLY",
    );
}

#[test]
fn lateral_derived() {
    fn chk(lateral_in: bool) {
        let lateral_str = if lateral_in { "LATERAL " } else { "" };
        let sql = format!(
            "SELECT * FROM customer LEFT JOIN {}\
             (SELECT * FROM order WHERE order.customer = customer.id LIMIT 3) AS order ON true",
            lateral_str
        );
        let select = verified_only_select(&sql);
        let from = only(select.from);
        assert_eq!(from.joins.len(), 1);
        let join = &from.joins[0];
        assert_eq!(
            join.join_operator,
            JoinOperator::LeftOuter(JoinConstraint::On(Expr::Value(Value::Boolean(true))))
        );
        if let TableFactor::Derived {
            lateral,
            ref subquery,
            alias: Some(ref alias),
        } = join.relation
        {
            assert_eq!(lateral_in, lateral);
            assert_eq!(Ident::new("order"), alias.name);
            assert_eq!(
                subquery.to_string(),
                "SELECT * FROM order WHERE order.customer = customer.id LIMIT 3"
            );
        } else {
            unreachable!()
        }
    }
    chk(false);
    chk(true);

    let sql = "SELECT * FROM customer LEFT JOIN LATERAL generate_series(1, customer.id)";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ("\
Parse error:
SELECT * FROM customer LEFT JOIN LATERAL generate_series(1, customer.id)
                                         ^^^^^^^^^^^^^^^
Expected subquery after LATERAL, found: generate_series"
            .to_string()),
        format!("{}", res.unwrap_err())
    );

    let sql = "SELECT * FROM a LEFT JOIN LATERAL (b CROSS JOIN c)";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ("\
Parse error:
SELECT * FROM a LEFT JOIN LATERAL (b CROSS JOIN c)
                                   ^
Expected SELECT, VALUES, or a subquery in the query body, found: b"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_start_transaction() {
    match verified_stmt("START TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE") {
        Statement::StartTransaction { modes } => assert_eq!(
            modes,
            vec![
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite),
                TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
            ]
        ),
        _ => unreachable!(),
    }

    // For historical reasons, PostgreSQL allows the commas between the modes to
    // be omitted.
    match one_statement_parses_to(
        "START TRANSACTION READ ONLY READ WRITE ISOLATION LEVEL SERIALIZABLE",
        "START TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE",
    ) {
        Statement::StartTransaction { modes } => assert_eq!(
            modes,
            vec![
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite),
                TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
            ]
        ),
        _ => unreachable!(),
    }

    verified_stmt("START TRANSACTION");
    one_statement_parses_to("BEGIN", "START TRANSACTION");
    one_statement_parses_to("BEGIN WORK", "START TRANSACTION");
    one_statement_parses_to("BEGIN TRANSACTION", "START TRANSACTION");

    verified_stmt("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
    verified_stmt("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
    verified_stmt("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    verified_stmt("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");

    let res = parse_sql_statements("START TRANSACTION ISOLATION LEVEL BAD");
    assert_eq!(
        ("\
Parse error:
START TRANSACTION ISOLATION LEVEL BAD
                                  ^^^
Expected isolation level, found: BAD"
            .to_string()),
        format!("{}", res.unwrap_err())
    );

    let res = parse_sql_statements("START TRANSACTION BAD");
    assert_eq!(
        ("\
Parse error:
START TRANSACTION BAD
                  ^^^
Expected transaction mode, found: BAD"
            .to_string()),
        format!("{}", res.unwrap_err())
    );

    let res = parse_sql_statements("START TRANSACTION READ ONLY,");
    assert_eq!(
        ("\
Parse error:
START TRANSACTION READ ONLY,
                            ^
Expected transaction mode, found: EOF"
            .to_string()),
        format!("{}", res.unwrap_err())
    );
}

#[test]
fn parse_set_transaction() {
    // SET TRANSACTION shares transaction mode parsing code with START
    // TRANSACTION, so no need to duplicate the tests here. We just do a quick
    // sanity check.
    match verified_stmt("SET TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE") {
        Statement::SetTransaction { modes } => assert_eq!(
            modes,
            vec![
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite),
                TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
            ]
        ),
        _ => unreachable!(),
    }
}

#[test]
fn parse_commit() {
    match verified_stmt("COMMIT") {
        Statement::Commit { chain: false } => (),
        _ => unreachable!(),
    }

    match verified_stmt("COMMIT AND CHAIN") {
        Statement::Commit { chain: true } => (),
        _ => unreachable!(),
    }

    one_statement_parses_to("COMMIT AND NO CHAIN", "COMMIT");
    one_statement_parses_to("COMMIT WORK AND NO CHAIN", "COMMIT");
    one_statement_parses_to("COMMIT TRANSACTION AND NO CHAIN", "COMMIT");
    one_statement_parses_to("COMMIT WORK AND CHAIN", "COMMIT AND CHAIN");
    one_statement_parses_to("COMMIT TRANSACTION AND CHAIN", "COMMIT AND CHAIN");
    one_statement_parses_to("COMMIT WORK", "COMMIT");
    one_statement_parses_to("COMMIT TRANSACTION", "COMMIT");
}

#[test]
fn parse_rollback() {
    match verified_stmt("ROLLBACK") {
        Statement::Rollback { chain: false } => (),
        _ => unreachable!(),
    }

    match verified_stmt("ROLLBACK AND CHAIN") {
        Statement::Rollback { chain: true } => (),
        _ => unreachable!(),
    }

    one_statement_parses_to("ROLLBACK AND NO CHAIN", "ROLLBACK");
    one_statement_parses_to("ROLLBACK WORK AND NO CHAIN", "ROLLBACK");
    one_statement_parses_to("ROLLBACK TRANSACTION AND NO CHAIN", "ROLLBACK");
    one_statement_parses_to("ROLLBACK WORK AND CHAIN", "ROLLBACK AND CHAIN");
    one_statement_parses_to("ROLLBACK TRANSACTION AND CHAIN", "ROLLBACK AND CHAIN");
    one_statement_parses_to("ROLLBACK WORK", "ROLLBACK");
    one_statement_parses_to("ROLLBACK TRANSACTION", "ROLLBACK");
}

#[test]
fn parse_explain() {
    let ast = verified_stmt("EXPLAIN DATAFLOW FOR SELECT 665");
    assert_eq!(
        ast,
        Statement::Explain {
            stage: Stage::Dataflow,
            query: Box::new(verified_query("SELECT 665")),
        }
    );

    let ast = verified_stmt("EXPLAIN PLAN FOR SELECT 665");
    assert_eq!(
        ast,
        Statement::Explain {
            stage: Stage::Plan,
            query: Box::new(verified_query("SELECT 665")),
        }
    );
}

#[test]
fn parse_show_columns() {
    let table_name = ObjectName(vec![Ident::new("mytable")]);
    assert_eq!(
        verified_stmt("SHOW COLUMNS FROM mytable"),
        Statement::ShowColumns {
            extended: false,
            full: false,
            table_name: table_name.clone(),
            filter: None,
        }
    );
    assert_eq!(
        verified_stmt("SHOW COLUMNS FROM mydb.mytable"),
        Statement::ShowColumns {
            extended: false,
            full: false,
            table_name: ObjectName(vec![Ident::new("mydb"), Ident::new("mytable")]),
            filter: None,
        }
    );
    assert_eq!(
        verified_stmt("SHOW EXTENDED COLUMNS FROM mytable"),
        Statement::ShowColumns {
            extended: true,
            full: false,
            table_name: table_name.clone(),
            filter: None,
        }
    );
    assert_eq!(
        verified_stmt("SHOW FULL COLUMNS FROM mytable"),
        Statement::ShowColumns {
            extended: false,
            full: true,
            table_name: table_name.clone(),
            filter: None,
        }
    );
    assert_eq!(
        verified_stmt("SHOW EXTENDED FULL COLUMNS FROM mytable"),
        Statement::ShowColumns {
            extended: true,
            full: true,
            table_name: table_name.clone(),
            filter: None,
        }
    );
    assert_eq!(
        verified_stmt("SHOW COLUMNS FROM mytable LIKE 'pattern'"),
        Statement::ShowColumns {
            extended: false,
            full: false,
            table_name: table_name.clone(),
            filter: Some(ShowStatementFilter::Like("pattern".into())),
        }
    );
    assert_eq!(
        verified_stmt("SHOW COLUMNS FROM mytable WHERE 1 = 2"),
        Statement::ShowColumns {
            extended: false,
            full: false,
            table_name,
            filter: Some(ShowStatementFilter::Where(verified_expr("1 = 2"))),
        }
    );
    one_statement_parses_to("SHOW FIELDS FROM mytable", "SHOW COLUMNS FROM mytable");
    one_statement_parses_to("SHOW COLUMNS IN mytable", "SHOW COLUMNS FROM mytable");
    one_statement_parses_to("SHOW FIELDS IN mytable", "SHOW COLUMNS FROM mytable");

    // unhandled things are truly unhandled
    match parse_sql_statements("SHOW COLUMNS FROM mytable FROM mydb") {
        Err(_) => {}
        Ok(val) => panic!("unexpected successful parse: {:?}", val),
    }
}

#[test]
fn parse_create_table_with_defaults() {
    let sql = "CREATE TABLE public.customer (
            customer_id integer DEFAULT nextval(public.customer_customer_id_seq),
            store_id smallint NOT NULL,
            first_name character varying(45) NOT NULL,
            last_name character varying(45) COLLATE \"es_ES\" NOT NULL,
            email character varying(50),
            address_id smallint NOT NULL,
            activebool boolean DEFAULT true NOT NULL,
            create_date date DEFAULT now()::text NOT NULL,
            last_update timestamp without time zone DEFAULT now() NOT NULL,
            last_update_tz timestamp with time zone,
            active integer NOT NULL
    ) WITH (fillfactor = 20, user_catalog_table = true, autovacuum_vacuum_threshold = 100)";
    match one_statement_parses_to(sql, "") {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            with_options,
            if_not_exists,
        } => {
            assert_eq!("public.customer", name.to_string());
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: "customer_id".into(),
                        data_type: DataType::Int,
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Default(verified_expr(
                                "nextval(public.customer_customer_id_seq)"
                            ))
                        }],
                    },
                    ColumnDef {
                        name: "store_id".into(),
                        data_type: DataType::SmallInt,
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "first_name".into(),
                        data_type: DataType::Varchar(Some(45)),
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "last_name".into(),
                        data_type: DataType::Varchar(Some(45)),
                        collation: Some(ObjectName(vec![Ident::with_quote('"', "es_ES")])),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "email".into(),
                        data_type: DataType::Varchar(Some(50)),
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: "address_id".into(),
                        data_type: DataType::SmallInt,
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull
                        }],
                    },
                    ColumnDef {
                        name: "activebool".into(),
                        data_type: DataType::Boolean,
                        collation: None,
                        options: vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Default(Expr::Value(Value::Boolean(true))),
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            }
                        ],
                    },
                    ColumnDef {
                        name: "create_date".into(),
                        data_type: DataType::Date,
                        collation: None,
                        options: vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Default(verified_expr("CAST(now() AS text)"))
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            }
                        ],
                    },
                    ColumnDef {
                        name: "last_update".into(),
                        data_type: DataType::Timestamp,
                        collation: None,
                        options: vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Default(verified_expr("now()")),
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            }
                        ],
                    },
                    ColumnDef {
                        name: "last_update_tz".into(),
                        data_type: DataType::TimestampTz,
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: "active".into(),
                        data_type: DataType::Int,
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull
                        }],
                    },
                ]
            );
            assert!(constraints.is_empty());
            assert_eq!(
                with_options,
                vec![
                    SqlOption {
                        name: "fillfactor".into(),
                        value: number("20")
                    },
                    SqlOption {
                        name: "user_catalog_table".into(),
                        value: Value::Boolean(true)
                    },
                    SqlOption {
                        name: "autovacuum_vacuum_threshold".into(),
                        value: number("100")
                    },
                ]
            );
            assert!(!if_not_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_from_pg_dump() {
    let sql = "CREATE TABLE public.customer (
            customer_id integer DEFAULT nextval('public.customer_customer_id_seq'::regclass) NOT NULL,
            store_id smallint NOT NULL,
            first_name character varying(45) NOT NULL,
            last_name character varying(45) NOT NULL,
            info text[],
            address_id smallint NOT NULL,
            activebool boolean DEFAULT true NOT NULL,
            create_date date DEFAULT now()::date NOT NULL,
            create_date1 date DEFAULT 'now'::text::date NOT NULL,
            last_update timestamp without time zone DEFAULT now(),
            active integer
        )";
    one_statement_parses_to(sql, "CREATE TABLE public.customer (\
            customer_id int DEFAULT nextval(CAST('public.customer_customer_id_seq' AS regclass)) NOT NULL, \
            store_id smallint NOT NULL, \
            first_name character varying(45) NOT NULL, \
            last_name character varying(45) NOT NULL, \
            info text[], \
            address_id smallint NOT NULL, \
            activebool boolean DEFAULT true NOT NULL, \
            create_date date DEFAULT CAST(now() AS date) NOT NULL, \
            create_date1 date DEFAULT CAST(CAST('now' AS text) AS date) NOT NULL, \
            last_update timestamp DEFAULT now(), \
            active int\
        )");
}

#[test]
fn parse_create_table_with_inherit() {
    let sql = "\
               CREATE TABLE bazaar.settings (\
               settings_id uuid PRIMARY KEY DEFAULT uuid_generate_v4() NOT NULL, \
               user_id uuid UNIQUE, \
               value text[], \
               use_metric boolean DEFAULT true\
               )";
    verified_stmt(sql);
}

#[test]
fn parse_create_table_if_not_exists() {
    let sql = "CREATE TABLE IF NOT EXISTS foo (bar int)";
    match verified_stmt(sql) {
        Statement::CreateTable { if_not_exists, .. } => {
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    }

    let res = parse_sql_statements("CREATE TABLE IF EXISTS foo (bar int)");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected NOT, found: EXISTS"));
}

#[ignore] // NOTE(benesch): this test is doomed. COPY data should not be tokenized/parsed.
#[test]
fn parse_copy_example() {
    let sql = r#"COPY public.actor (actor_id, first_name, last_name, last_update, value) FROM stdin;
1	PENELOPE	GUINESS	2006-02-15 09:34:33 0.11111
2	NICK	WAHLBERG	2006-02-15 09:34:33 0.22222
3	ED	CHASE	2006-02-15 09:34:33 0.312323
4	JENNIFER	DAVIS	2006-02-15 09:34:33 0.3232
5	JOHNNY	LOLLOBRIGIDA	2006-02-15 09:34:33 1.343
6	BETTE	NICHOLSON	2006-02-15 09:34:33 5.0
7	GRACE	MOSTEL	2006-02-15 09:34:33 6.0
8	MATTHEW	JOHANSSON	2006-02-15 09:34:33 7.0
9	JOE	SWANK	2006-02-15 09:34:33 8.0
10	CHRISTIAN	GABLE	2006-02-15 09:34:33 9.1
11	ZERO	CAGE	2006-02-15 09:34:33 10.001
12	KARL	BERRY	2017-11-02 19:15:42.308637+08 11.001
A Fateful Reflection of a Waitress And a Boat who must Discover a Sumo Wrestler in Ancient China
Kwara & Kogi
{"Deleted Scenes","Behind the Scenes"}
'awe':5 'awe-inspir':4 'barbarella':1 'cat':13 'conquer':16 'dog':18 'feminist':10 'inspir':6 'monasteri':21 'must':15 'stori':7 'streetcar':2
PHP	₱ USD $
\N  Some other value
\\."#;
    let ast = one_statement_parses_to(sql, "");
    println!("{:#?}", ast);
    //assert_eq!(sql, ast.to_string());
}

#[test]
fn parse_set() {
    let stmt = verified_stmt("SET a = b");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            variable: "a".into(),
            value: SetVariableValue::Ident("b".into()),
        }
    );

    let stmt = verified_stmt("SET a = 'b'");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            variable: "a".into(),
            value: SetVariableValue::Literal(Value::SingleQuotedString("b".into())),
        }
    );

    let stmt = verified_stmt("SET a = 0");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            variable: "a".into(),
            value: SetVariableValue::Literal(number("0")),
        }
    );

    let stmt = verified_stmt("SET a = DEFAULT");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            variable: "a".into(),
            value: SetVariableValue::Ident("DEFAULT".into()),
        }
    );

    let stmt = verified_stmt("SET LOCAL a = b");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: true,
            variable: "a".into(),
            value: SetVariableValue::Ident("b".into()),
        }
    );

    one_statement_parses_to("SET a TO b", "SET a = b");
    one_statement_parses_to("SET SESSION a = b", "SET a = b");

    assert_eq!(
        parse_sql_statements("SET").unwrap_err().to_string(),
        "\
Parse error:
SET
   ^
Expected identifier, found: EOF"
            .to_string(),
    );

    assert_eq!(
        parse_sql_statements("SET a b").unwrap_err().to_string(),
        "\
Parse error:
SET a b
      ^
Expected equals sign or TO, found: b"
            .to_string(),
    );

    assert_eq!(
        parse_sql_statements("SET a =").unwrap_err().to_string(),
        "\
Parse error:
SET a =
       ^
Expected variable value, found: EOF"
            .to_string(),
    );
}

#[test]
fn parse_show() {
    let stmt = verified_stmt("SHOW a");
    assert_eq!(
        stmt,
        Statement::ShowVariable {
            variable: "a".into()
        }
    );

    let stmt = verified_stmt("SHOW ALL");
    assert_eq!(
        stmt,
        Statement::ShowVariable {
            variable: "ALL".into()
        }
    )
}

#[test]
fn parse_array() {
    let expr = verified_expr("ARRAY[]");

    assert_eq!(expr, Expr::Value(Value::Array(vec![])));

    let expr = verified_expr("ARRAY[1, 'foo']");

    assert_eq!(
        expr,
        Expr::Value(Value::Array(vec![
            Value::Number("1".into()),
            Value::SingleQuotedString("foo".to_owned())
        ]))
    );

    let select = verified_only_select("SELECT ARRAY[]");

    assert_eq!(
        expr_from_projection(only(&select.projection)),
        &Expr::Value(Value::Array(vec![]))
    );

    let select = verified_only_select("SELECT ARRAY[1, 'foo']");

    assert_eq!(
        expr_from_projection(only(&select.projection)),
        &Expr::Value(Value::Array(vec![
            Value::Number("1".into()),
            Value::SingleQuotedString("foo".to_owned())
        ]))
    );
}

#[test]
fn parse_pg_array_datatype() {
    let sql = "SELECT '{{1,2},{3,4}}'::int[][]";
    let select = unverified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            expr: Box::new(Expr::Value(Value::SingleQuotedString(
                "{{1,2},{3,4}}".to_owned()
            ))),
            data_type: DataType::Array(Box::new(DataType::Array(Box::new(DataType::Int)))),
        },
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_json_ops() {
    use self::BinaryOperator::*;
    use self::Expr::*;

    for (op_string, op_enum) in vec![
        ("->", JsonGet),
        ("->>", JsonGetAsText),
        ("#>", JsonGetPath),
        ("#>>", JsonGetPathAsText),
        ("@>", JsonContainsJson),
        ("<@", JsonContainedInJson),
        ("?", JsonContainsField),
        ("?|", JsonContainsAnyFields),
        ("?&", JsonContainsAllFields),
        ("||", JsonConcat),
        ("#-", JsonDeletePath),
        ("@?", JsonContainsPath),
        ("@@", JsonApplyPathPredicate),
    ] {
        let sql = format!("a {} b", op_string);
        assert_matches!(
            &verified_expr(&sql),
            BinaryOp {op, ..} if *op == op_enum
        );
    }
}

#[test]
fn test_multiline_errors() {
    assert_eq!(
        parse_sql_statements("SELECT foo FROM\n")
            .unwrap_err()
            .to_string(),
        "\
Parse error:

^
Expected identifier, found: EOF"
            .to_string(),
    );

    assert_eq!(
        parse_sql_statements("\n\nSEL\n\nECT")
            .unwrap_err()
            .to_string(),
        "\
Parse error:
SEL
^^^
Expected a keyword at the beginning of a statement, found: SEL"
            .to_string(),
    );

    assert_eq!(
        parse_sql_statements("SELECT foo \nFROM bar+1 ORDER\n BY")
            .unwrap_err()
            .to_string(),
        "\
    Parse error:
FROM bar+1 ORDER
        ^
Expected end of statement, found: +"
            .to_string(),
    );
}

pub fn run_parser_method<F, T>(sql: &str, f: F) -> T
where
    F: Fn(&mut Parser) -> T,
    T: fmt::Debug + PartialEq,
{
    let mut tokenizer = sql_parser::tokenizer::Tokenizer::new(sql);
    let tokens = tokenizer.tokenize().unwrap();
    f(&mut Parser::new(sql.to_string(), tokens))
}

fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, ParserError> {
    Parser::parse_sql(sql.to_string())
}

fn one_statement_parses_to(sql: &str, canonical: &str) -> Statement {
    let mut statements = parse_sql_statements(&sql).unwrap();
    assert_eq!(statements.len(), 1);

    let only_statement = statements.pop().unwrap();
    if !canonical.is_empty() {
        assert_eq!(canonical, only_statement.to_string())
    }
    only_statement
}

fn verified_stmt(query: &str) -> Statement {
    one_statement_parses_to(query, query)
}

fn unverified_stmt(query: &str) -> Statement {
    one_statement_parses_to(query, "")
}

fn verified_query(sql: &str) -> Query {
    match verified_stmt(sql) {
        Statement::Query(query) => *query,
        _ => panic!("Expected Query"),
    }
}

fn unverified_query(sql: &str) -> Query {
    match unverified_stmt(sql) {
        Statement::Query(query) => *query,
        _ => panic!("Expected Query"),
    }
}

fn verified_only_select(query: &str) -> Select {
    match verified_query(query).body {
        SetExpr::Select(s) => *s,
        _ => panic!("Expected SetExpr::Select"),
    }
}

fn unverified_only_select(query: &str) -> Select {
    match unverified_query(query).body {
        SetExpr::Select(s) => *s,
        _ => panic!("Expected SetExpr::Select"),
    }
}

fn verified_expr(sql: &str) -> Expr {
    let ast = run_parser_method(sql, Parser::parse_expr).unwrap();
    assert_eq!(sql, &ast.to_string(), "round-tripping without changes");
    ast
}

pub fn only<T>(v: impl IntoIterator<Item = T>) -> T {
    let mut iter = v.into_iter();
    if let (Some(item), None) = (iter.next(), iter.next()) {
        item
    } else {
        panic!("only called on collection without exactly one item")
    }
}

pub fn expr_from_projection(item: &SelectItem) -> &Expr {
    match item {
        SelectItem::UnnamedExpr(expr) => expr,
        _ => panic!("Expected UnnamedExpr"),
    }
}

pub fn number(n: &'static str) -> Value {
    Value::Number(n.parse().unwrap())
}

fn verify_interval(sql: &str, value: IntervalValue, expected_error_str: Option<&str>) {
    // If there's a failure this shows every statement verified in this
    // test, pointing out which one failed
    println!("testing: {}", sql);
    match parse_sql_statements(sql) {
        Ok(_) => {
            let select = verified_only_select(sql);
            match expr_from_projection(only(&select.projection)) {
                Expr::Value(Value::Interval(iv)) => {
                    assert_eq!(&value, iv);
                }
                v => panic!("invalid value, expected interval for {}: {:?}", sql, v),
            }
        }
        Err(v) => match expected_error_str {
            Some(e) => assert_eq!(v.to_string(), e.to_string()),
            None => panic!("invalid value, expected interval for {}: {:?}", sql, v),
        },
    }
}
