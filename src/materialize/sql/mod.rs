// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use failure::bail;
use lazy_static::lazy_static;
use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::{
    ASTNode, SQLObjectName, SQLQuery, SQLSelect, SQLSelectItem, SQLSetExpr, SQLStatement,
    TableFactor,
};
use sqlparser::sqlparser::Parser as SQLParser;
use std::collections::HashMap;

use crate::dataflow::{Dataflow, Expr, Plan, Schema, Source, Type, View};

lazy_static! {
    static ref DUAL_SCHEMA: Schema = Schema(vec![(Some("x".into()), Type::String)]);
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

    pub fn parse_statement(&self, stmt: &str) -> Result<Dataflow, failure::Error> {
        let dialect = AnsiSqlDialect {};

        let mut stmts = SQLParser::parse_sql(&dialect, stmt.to_owned())?;
        if stmts.len() != 1 {
            bail!("expected one statement, but got {}", stmts.len());
        }
        let stmt = stmts.remove(0);

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
                    url: url,
                    schema: self.parse_avro_schema(schema)?,
                }))
            }
            _ => bail!("only CREATE MATERIALIZED VIEW AS allowed"),
        }
    }

    fn parse_view_query(&self, q: SQLQuery) -> Result<(Plan, Schema), failure::Error> {
        if q.ctes.len() != 0 {
            bail!("CTEs are not yet supported");
        }
        if q.limit.is_some() {
            bail!("LIMIT is not supported in a view definition");
        }
        if q.order_by.is_some() {
            bail!("ORDER BY is not supported in a view definition");
        }
        match q.body {
            SQLSetExpr::Select(select) => self.parse_view_select(select),
            _ => bail!("set operations are not yet supported"),
        }
    }

    fn parse_view_select(&self, s: SQLSelect) -> Result<(Plan, Schema), failure::Error> {
        if s.having.is_some() {
            bail!("HAVING is not yet supported");
        } else if s.group_by.is_some() {
            bail!("GROUP BY is not yet supported");
        } else if s.joins.len() != 0 {
            bail!("JOIN is not yet supported");
        }

        let (plan, schema) = match s.relation {
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
        for p in s.projection {
            let (name, expr, typ) = self.parse_select_item(p, schema)?;
            outputs.push(expr);
            pschema.0.push((name, typ));
        }

        let plan = Plan::Project {
            outputs: outputs,
            input: Box::new(plan),
        };

        Ok((plan, pschema))
    }

    fn parse_select_item(
        &self,
        s: SQLSelectItem,
        schema: &Schema,
    ) -> Result<(Option<String>, Expr, Type), failure::Error> {
        match s {
            SQLSelectItem::UnnamedExpression(e) => self.parse_expr(e, schema),
            _ => bail!(
                "complicated select items are not yet supported: {}",
                s.to_string()
            ),
        }
    }

    fn parse_expr(
        &self,
        e: ASTNode,
        schema: &Schema,
    ) -> Result<(Option<String>, Expr, Type), failure::Error> {
        match e {
            ASTNode::SQLIdentifier(name) => {
                let i = schema.index(&name)?;
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

    fn parse_sql_object_name(&self, n: SQLObjectName) -> Result<String, failure::Error> {
        if n.0.len() != 1 {
            bail!("qualified names are not yet supported: {}", n.to_string())
        }
        Ok(n.to_string())
    }

    fn parse_avro_schema(&self, schema: String) -> Result<Schema, failure::Error> {
        use avro_rs::Schema as AvroSchema;
        let schema = AvroSchema::parse_str(&schema)?;
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

        let dataflow = parser.parse_statement("CREATE MATERIALIZED VIEW v AS SELECT b FROM src")?;
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

        let dataflow = parser.parse_statement(r#"
            CREATE DATA SOURCE s FROM 'kafka://somewhere'
            USING SCHEMA '{
                "type": "record",
                "name": "foo",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }'
        "#)?;
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
