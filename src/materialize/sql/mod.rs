// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use failure::{bail, format_err};
use futures::{future, Future};
use lazy_static::lazy_static;
use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::{
    ASTNode, SQLObjectName, SQLQuery, SQLSelect, SQLSelectItem, SQLSetExpr, SQLStatement,
    TableFactor,
};
use sqlparser::sqlparser::Parser as SQLParser;
use sqlparser::sqlast::visit;
use sqlparser::sqlast::visit::Visit;
use std::collections::HashMap;

use crate::dataflow::{Dataflow, Expr, Plan, Schema, Source, Type, View};
use crate::server::ConnState;
use ore::future::FutureExt;

lazy_static! {
    static ref DUAL_SCHEMA: Schema = Schema(vec![(Some("x".into()), Type::String)]);
}

pub fn handle_query(stmt: String, conn_state: &ConnState) -> impl Future<Item = (), Error = failure::Error> {
    let meta_store = conn_state.meta_store.clone();
    future::lazy(move || {
        let mut stmts = SQLParser::parse_sql(&AnsiSqlDialect {}, stmt)?;
        if stmts.len() != 1 {
            bail!("expected one statement, but got {}", stmts.len());
        }
        let stmt = stmts.remove(0);

        let mut visitor = ObjectNameVisitor::new();
        visitor.visit_statement(&stmt);
        visitor.into_result().map(|object_names| (stmt, object_names))
    })
    .and_then(move |(stmt, object_names)| {
        meta_store.read_dataflows(object_names).map(|dataflows| (stmt, dataflows))
            .map(|(stmt, dataflows)| (meta_store, stmt, dataflows))
    })
    .and_then(|(meta_store, stmt, dataflows)| {
        let parser = Parser::new(dataflows.into_iter().map(|(n, d)| (n, (0, d.schema().to_owned()))));
        let dataflow = match parser.parse_statement(&stmt) {
            Ok(dataflow) => dataflow,
            Err(err) => return future::err(err).left(),
        };
        meta_store.new_dataflow(dataflow.name(), &dataflow).right()
    })
}

struct ObjectNameVisitor {
    recording: bool,
    object_names: Vec<String>,
    err: Option<failure::Error>,
}

impl ObjectNameVisitor {
    pub fn new() -> ObjectNameVisitor {
        ObjectNameVisitor {
            recording: false,
            object_names: Vec::new(),
            err: None,
        }
    }

    pub fn into_result(self) -> Result<Vec<String>, failure::Error> {
        match self.err {
            Some(err) => Err(err),
            None => Ok(self.object_names),
        }
    }
}

impl<'ast> Visit<'ast> for ObjectNameVisitor {
    fn visit_query(&mut self, query: &'ast SQLQuery) {
        self.recording = true;
        visit::visit_query(self, query);
    }

    fn visit_object_name(&mut self, object_name: &'ast SQLObjectName) {
        if self.err.is_some() || !self.recording {
            return
        }
        if object_name.0.len() != 1 {
            self.err = Some(format_err!("qualified names are not yet supported: {}", object_name.to_string()))
        } else {
            self.object_names.push(object_name.0[0].to_owned())
        }
    }
}

#[allow(dead_code)]
pub struct Parser {
    dataflows: HashMap<String, (usize, Schema)>,
}

#[allow(dead_code)]
impl Parser {
    pub fn new<I>(iter: I) -> Parser
    where
        I: IntoIterator<Item = (String, (usize, Schema))>,
    {
        Parser {
            dataflows: iter.into_iter().collect(),
        }
    }

    pub fn parse_statement(&self, stmt: &SQLStatement) -> Result<Dataflow, failure::Error> {
        match stmt {
            SQLStatement::SQLCreateView {
                name,
                query,
                materialized: true,
            } => {
                let (plan, schema) = self.parse_view_query(query)?;
                Ok(Dataflow::View(View {
                    name: self.parse_sql_object_name(name)?,
                    plan: plan,
                    schema: schema,
                }))
            }
            SQLStatement::SQLCreateDataSource {
                name,
                url,
                schema,
            } => {
                Ok(Dataflow::Source(Source {
                    name: self.parse_sql_object_name(name)?,
                    url: url.to_owned(),
                    schema: self.parse_avro_schema(schema)?,
                }))
            }
            _ => bail!("only CREATE MATERIALIZED VIEW AS allowed"),
        }
    }

    fn parse_view_query(&self, q: &SQLQuery) -> Result<(Plan, Schema), failure::Error> {
        if q.ctes.len() != 0 {
            bail!("CTEs are not yet supported");
        }
        if q.limit.is_some() {
            bail!("LIMIT is not supported in a view definition");
        }
        if q.order_by.is_some() {
            bail!("ORDER BY is not supported in a view definition");
        }
        match &q.body {
            SQLSetExpr::Select(select) => self.parse_view_select(select),
            _ => bail!("set operations are not yet supported"),
        }
    }

    fn parse_view_select(&self, s: &SQLSelect) -> Result<(Plan, Schema), failure::Error> {
        if s.having.is_some() {
            bail!("HAVING is not yet supported");
        } else if s.group_by.is_some() {
            bail!("GROUP BY is not yet supported");
        } else if s.joins.len() != 0 {
            bail!("JOIN is not yet supported");
        }

        let (plan, schema) = match &s.relation {
            Some(TableFactor::Table { name, .. }) => {
                let name = self.parse_sql_object_name(name)?;
                let schema = match self.dataflows.get(&name) {
                    None => bail!("no dataflow named {}", name),
                    Some((_version, schema)) => schema,
                };
                (Plan::Source(name), schema)
            }
            Some(TableFactor::Derived { .. }) => {
                bail!("nested subqueries are not yet supported");
            }
            None => {
                // https://en.wikipedia.org/wiki/DUAL_table
                (Plan::Source("dual".into()), &*DUAL_SCHEMA)
            }
        };

        let mut outputs = Vec::new();
        let mut pschema = Schema(Vec::new());
        for p in &s.projection {
            let (name, expr, typ) = self.parse_select_item(p, schema)?;
            outputs.push(expr);
            pschema.0.push((name.map(|s| s.to_owned()), typ));
        }

        let plan = Plan::Project {
            outputs: outputs,
            input: Box::new(plan),
        };

        Ok((plan, pschema))
    }

    fn parse_select_item<'a>(
        &self,
        s: &'a SQLSelectItem,
        schema: &Schema,
    ) -> Result<(Option<&'a str>, Expr, Type), failure::Error> {
        match s {
            SQLSelectItem::UnnamedExpression(e) => self.parse_expr(e, schema),
            _ => bail!(
                "complicated select items are not yet supported: {}",
                s.to_string()
            ),
        }
    }

    fn parse_expr<'a>(
        &self,
        e: &'a ASTNode,
        schema: &Schema,
    ) -> Result<(Option<&'a str>, Expr, Type), failure::Error> {
        match e {
            ASTNode::SQLIdentifier(name) => {
                let i = schema.index(name)?;
                let expr = Expr::Column(i);
                let typ = schema.0[i].1;
                Ok((Some(name), expr, typ))
            }
            _ => bail!(
                "complicated expressions are not yet supported: {}",
                e.to_string()
            ),
        }
    }

    fn parse_sql_object_name(&self, n: &SQLObjectName) -> Result<String, failure::Error> {
        if n.0.len() != 1 {
            bail!("qualified names are not yet supported: {}", n.to_string())
        }
        Ok(n.to_string())
    }

    fn parse_avro_schema(&self, schema: &str) -> Result<Schema, failure::Error> {
        use avro_rs::Schema as AvroSchema;
        let schema = AvroSchema::parse_str(schema)?;
        let mut out = Vec::new();
        match schema {
            AvroSchema::Record { fields, .. } => {
                for f in fields {
                    match f.schema {
                        AvroSchema::Long => out.push((Some(f.name), Type::Int)),
                        AvroSchema::String => out.push((Some(f.name), Type::String)),
                        _ => bail!("avro schemas do not yet support data types besides long and string")
                    }
                }
            }
            _ => bail!("avro schemas must have exactly one record")
        }
        Ok(Schema(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_view() -> Result<(), failure::Error> {
        let schema = Schema(vec![
            (None, Type::Int),
            (Some("a".into()), Type::String),
            (Some("b".into()), Type::String),
        ]);
        let version = 1;
        let parser = Parser::new(vec![("src".into(), (version, schema))]);

        let stmts = SQLParser::parse_sql(&AnsiSqlDialect {},
            "CREATE MATERIALIZED VIEW v AS SELECT b FROM src".into())?;
        let dataflow = parser.parse_statement(&stmts[0])?;
        assert_eq!(
            dataflow,
            Dataflow::View(View {
                name: "v".into(),
                plan: Plan::Project {
                    outputs: vec![Expr::Column(2)],
                    input: Box::new(Plan::Source("src".into())),
                },
                schema: Schema(vec![(Some("b".into()), Type::String)])
            })
        );

        Ok(())
    }

    #[test]
    fn test_basic_source() -> Result<(), failure::Error> {
        let parser = Parser::new(vec![]);

        let stmts = SQLParser::parse_sql(&AnsiSqlDialect {},
            r#"
                CREATE DATA SOURCE s FROM 'kafka://somewhere'
                USING SCHEMA '{
                    "type": "record",
                    "name": "foo",
                    "fields": [
                        {"name": "a", "type": "long", "default": 42},
                        {"name": "b", "type": "string"}
                    ]
                }'
            "#.into())?;
        let dataflow = parser.parse_statement(&stmts[0])?;
        assert_eq!(
            dataflow,
            Dataflow::Source(Source {
                name: "s".into(),
                url: "kafka://somewhere".into(),
                schema: Schema(vec![
                    (Some("a".into()), Type::Int),
                    (Some("b".into()), Type::String),
                ])
            })
        );

        Ok(())
    }
}
