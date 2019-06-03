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
        outputs: Vec<ScalarExpr>,
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
        left_key: Vec<ScalarExpr>,
        /// Expression to compute the key from the right input.
        right_key: Vec<ScalarExpr>,
        /// Plan for the left input.
        left: Box<Plan>,
        /// Plan for the right input.
        right: Box<Plan>,
        /// Include keys on the left that are not joined (for left/full outer join). The usize value is the number of columns on the right.
        include_left_outer: Option<usize>,
        /// Include keys on the right that are not joined (for right/full outer join). The usize value is the number of columns on the left.
        include_right_outer: Option<usize>,
    },
    /// Filter records based on predicate.
    Filter {
        predicate: ScalarExpr,
        input: Box<Plan>,
    },
    /// Aggregate records that share a key.
    Aggregate {
        key: Vec<ScalarExpr>,
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
    pub expr: ScalarExpr,
    pub distinct: bool,
}

#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum ScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    Literal(Datum),
    /// A function call that takes one expression as an argument.
    CallUnary {
        func: UnaryFunc,
        expr: Box<ScalarExpr>,
    },
    /// A function call that takes two expressions as arguments.
    CallBinary {
        func: BinaryFunc,
        expr1: Box<ScalarExpr>,
        expr2: Box<ScalarExpr>,
    },
    /// A function call that takes an arbitrary number of arguments.
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<ScalarExpr>,
    },
    If {
        cond: Box<ScalarExpr>,
        then: Box<ScalarExpr>,
        els: Box<ScalarExpr>,
    },
}

impl ScalarExpr {
    pub fn columns(is: &[usize]) -> Vec<ScalarExpr> {
        is.iter().map(|i| ScalarExpr::Column(*i)).collect()
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
                outputs: vec![ScalarExpr::Column(1), ScalarExpr::Column(2)],
                input: Box::new(Plan::Join {
                    left_key: ScalarExpr::Column(0),
                    right_key: ScalarExpr::Column(0),
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
