// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use serde::{Deserialize, Serialize};
use url::Url;

use super::func::{AggregateFunc, BinaryFunc, UnaryFunc, VariadicFunc};
use crate::repr::{Datum, Type};

/// System-wide update type.
pub type Diff = isize;

/// A named stream of data.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Dataflow {
    Source(Source),
    Sink(Sink),
    View(View),
}

impl Dataflow {
    /// Reports the name of this dataflow.
    pub fn name(&self) -> &str {
        match self {
            Dataflow::Source(src) => &src.name,
            Dataflow::Sink(sink) => &sink.name,
            Dataflow::View(view) => &view.name,
        }
    }

    /// Reports the type of the datums produced by this dataflow.
    pub fn typ(&self) -> Option<&Type> {
        match self {
            Dataflow::Source(src) => Some(&src.typ),
            Dataflow::Sink(_) => None,
            Dataflow::View(view) => Some(&view.typ),
        }
    }

    /// Collects the names of the dataflows that this dataflow depends upon.
    pub fn uses(&self) -> Vec<&str> {
        let mut out = Vec::new();
        match self {
            Dataflow::Source(_) => (),
            Dataflow::Sink(sink) => out.push(sink.from.as_str()),
            Dataflow::View(view) => view.plan.uses_inner(&mut out),
        }
        out
    }
}

impl metastore::Dataflow for Dataflow {}

/// A data source materializes data. It typically represents an external source
/// of data, like a topic from Apache Kafka.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub name: String,
    pub connector: SourceConnector,
    pub typ: Type,
}

#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Sink {
    pub name: String,
    pub from: String,
    pub connector: SinkConnector,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceConnector {
    Kafka(KafkaSourceConnector),
    Local(LocalSourceConnector),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSourceConnector {
    pub addr: std::net::SocketAddr,
    pub topic: String,
    pub raw_schema: String,
    #[serde(with = "url_serde")]
    pub schema_registry_url: Option<Url>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalSourceConnector {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SinkConnector {
    Kafka(KafkaSinkConnector),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnector {
    pub addr: std::net::SocketAddr,
    pub topic: String,
    pub schema_id: i32,
}

/// A view transforms one dataflow into another.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct View {
    pub name: String,
    pub plan: Plan,
    pub typ: Type,
}

#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Plan {
    /// Source data from another dataflow.
    Source(String),
    /// Project or permute the columns in a dataflow.
    Project {
        outputs: Vec<Expr>,
        /// Plan for the input.
        input: Box<Plan>,
    },
    /// Suppress duplicate tuples.
    Distinct(Box<Plan>),
    /// Union several dataflows of the same type.
    UnionAll(Vec<Plan>),
    /// Join two dataflows.
    Join {
        /// Expression to compute the key from the left input.
        left_key: Expr,
        /// Expression to compute the key from the right input.
        right_key: Expr,
        /// Plan for the left input.
        left: Box<Plan>,
        /// Plan for the right input.
        right: Box<Plan>,
        /// Include keys on the left that are not joined (for left/full outer join). The usize value is the number of columns on the right.
        include_left_outer: Option<usize>,
        /// Include keys on the right that are not joined (for right/full outer join). The usize value is the number of columns on the left.
        include_right_outer: Option<usize>,
    },
    MultiwayJoin {
        /// A list of participating plans.
        plans: Vec<Plan>,
        /// A list of the number of columns in the corresponding plan.
        arities: Vec<usize>,
        /// A list of (r1,c1) and (r2,c2) required equalities.
        equalities: Vec<((usize, usize), (usize, usize))>,
    },
    /// Filter records based on predicate.
    Filter { predicate: Expr, input: Box<Plan> },
    /// Aggregate records that share a key.
    Aggregate {
        key: Expr,
        aggs: Vec<Aggregate>,
        input: Box<Plan>,
    },
}

impl Plan {
    /// Collects the names of the dataflows that this plan depends upon.
    // Intentionally match on all field names to force changes to the AST to
    // be reflected in this function.
    #[allow(clippy::unneeded_field_pattern)]
    fn uses_inner<'a, 'b>(&'a self, out: &'b mut Vec<&'a str>) {
        match self {
            Plan::Source(name) => out.push(&name),
            Plan::Project { outputs: _, input } => input.uses_inner(out),
            Plan::Distinct(p) => p.uses_inner(out),
            Plan::UnionAll(ps) => ps.iter().for_each(|p| p.uses_inner(out)),
            Plan::Join {
                left_key: _,
                right_key: _,
                left,
                right,
                include_left_outer: _,
                include_right_outer: _,
            } => {
                left.uses_inner(out);
                right.uses_inner(out);
            }
            Plan::MultiwayJoin { plans, .. } => {
                for plan in plans {
                    plan.uses_inner(out);
                }
            }
            Plan::Filter {
                predicate: _,
                input,
            } => input.uses_inner(out),
            Plan::Aggregate {
                key: _,
                aggs: _,
                input,
            } => input.uses_inner(out),
        }
    }
}

#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct Aggregate {
    pub func: AggregateFunc,
    pub expr: Expr,
    pub distinct: bool,
}

#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Expr {
    /// The ambient value.
    Ambient,
    /// Tuple element selector.
    Column(usize, Box<Expr>),
    /// Tuple constructor.
    Tuple(Vec<Expr>),
    /// A literal value.
    Literal(Datum),
    /// A function call that takes one expression as an argument.
    CallUnary { func: UnaryFunc, expr: Box<Expr> },
    /// A function call that takes two expressions as arguments.
    CallBinary {
        func: BinaryFunc,
        expr1: Box<Expr>,
        expr2: Box<Expr>,
    },
    If {
        cond: Box<Expr>,
        then: Box<Expr>,
        els: Box<Expr>,
    },
    /// A function call that takes an arbitrary number of arguments.
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<Expr>,
    },
}

impl Expr {
    pub fn columns(is: &[usize], input: &Expr) -> Self {
        Expr::Tuple(
            is.iter()
                .map(|i| Expr::Column(*i, Box::new(input.clone())))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::error::Error;

    use super::*;
    use crate::repr::{FType, Type};

    /// Verify that a basic plan serializes and deserializes to JSON sensibly.
    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn Error>> {
        let dataflow = Dataflow::View(View {
            name: "report".into(),
            plan: Plan::Project {
                outputs: vec![
                    Expr::Column(1, Box::new(Expr::Ambient)),
                    Expr::Column(2, Box::new(Expr::Ambient)),
                ],
                input: Box::new(Plan::Join {
                    left_key: Expr::Column(0, Box::new(Expr::Ambient)),
                    right_key: Expr::Column(0, Box::new(Expr::Ambient)),
                    left: Box::new(Plan::Source("orders".into())),
                    right: Box::new(Plan::Distinct(Box::new(Plan::UnionAll(vec![
                        Plan::Source("customers2018".into()),
                        Plan::Source("customers2019".into()),
                    ])))),
                    include_left_outer: None,
                    include_right_outer: None,
                }),
            },
            typ: Type {
                name: None,
                nullable: false,
                ftype: FType::Tuple(vec![
                    Type {
                        name: Some("name".into()),
                        nullable: false,
                        ftype: FType::String,
                    },
                    Type {
                        name: Some("quantity".into()),
                        nullable: false,
                        ftype: FType::Int32,
                    },
                ]),
            },
        });

        let decoded: Dataflow = serde_json::from_str(&serde_json::to_string_pretty(&dataflow)?)?;
        assert_eq!(decoded, dataflow);

        Ok(())
    }
}
