use anyhow::bail;

use sql_parser::ast::visit::{self, Visit};
use sql_parser::ast::{Join, JoinOperator, Query, SetExpr, SetOperator, Statement};
use sql_parser::parser::parse_statements;

#[derive(Debug)]
pub struct Message {
    pub message: String,
    pub node: String,
    pub suggestion: Option<String>,
}

#[derive(Debug)]
pub struct Lint {
    pub messages: Vec<Message>,
}

impl Lint {
    pub fn from_stmt(stmt: &Statement) -> anyhow::Result<Lint> {
        let mut lint = Lint { messages: vec![] };
        lint.visit_statement(stmt);
        Ok(lint)
    }
    pub fn from_query(query: &Query) -> anyhow::Result<Lint> {
        let mut lint = Lint { messages: vec![] };
        lint.visit_query(query);
        Ok(lint)
    }
    pub fn from_string(sql: String) -> anyhow::Result<Lint> {
        let stmts = parse_statements(sql)?;
        if stmts.len() != 1 {
            bail!("expected exactly 1 statement");
        }
        Self::from_stmt(&stmts[0])
    }
    fn push<T: std::fmt::Display>(&mut self, node: T, message: String, suggestion: Option<T>) {
        self.messages.push(Message {
            message,
            node: node.to_string().trim().to_string(),
            suggestion: suggestion.map(|node| node.to_string().trim().to_string()),
        })
    }
}

impl<'ast> Visit<'ast> for Lint {
    fn visit_set_expr(&mut self, node: &'ast SetExpr) {
        if let SetExpr::SetOperation {
            op,
            all,
            left,
            right,
        } = node
        {
            if op == &SetOperator::Union && !all {
                self.push(
                    node,
                    format!("use `UNION ALL` instead of `UNION` when you do not require deduplication, or are certain that there is no duplication to remove"),
                    Some(&SetExpr::SetOperation {
                        op: SetOperator::Union,
                        all: true,
                        left: left.clone(),
                        right: right.clone(),
                    }),
                );
            }
        }

        visit::visit_set_expr(self, node);
    }
    fn visit_join(&mut self, node: &'ast Join) {
        match &node.join_operator {
            JoinOperator::CrossJoin => self.push(node, format!("avoid use of `CROSS JOIN`"), None),
            JoinOperator::LeftOuter(con)
            | JoinOperator::RightOuter(con)
            | JoinOperator::FullOuter(con) => {
                let suggestion = Join {
                    relation: node.relation.clone(),
                    join_operator: JoinOperator::Inner(con.clone()),
                };
                let msg = format!("use an `INNER JOIN` instead of `OUTER JOIN`, and track unmatched records separately");
                self.push(node, msg, Some(&suggestion));
            }
            _ => {}
        }

        visit::visit_join(self, node);
    }
}

#[test]
fn test_lint() {
    datadriven::walk("testdata", |tf| {
        tf.run(|tc| -> String {
            match tc.directive.as_str() {
                "lint" => {
                    println!("{}", tc.input);
                    let lint = Lint::from_string(tc.input.clone()).unwrap();
                    format!("{:?}\n", lint)
                }
                _ => panic!("unknown message type {}", tc.directive),
            }
        });
    });
}
