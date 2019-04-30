// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::func::{AggregateFunc, BinaryFunc, UnaryFunc};
use crate::repr::{Datum, Type};

/// System-wide notion of time.
pub type Time = u64;

/// System-wide update type.
pub type Diff = isize;

/// A named stream of data.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Dataflow {
    Source(Source),
    /// A view is a named transformation from one dataflow to another.
    View(View),
}

impl Dataflow {
    pub fn name(&self) -> &str {
        match self {
            Dataflow::Source(src) => &src.name,
            Dataflow::View(view) => &view.name,
        }
    }

    pub fn typ(&self) -> &Type {
        match self {
            Dataflow::Source(src) => &src.typ,
            Dataflow::View(view) => &view.typ,
        }
    }
}

/// A data source materializes data. It typically represents an external source
/// of data, like a topic from Apache Kafka.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub name: String,
    pub connector: Connector,
    pub typ: Type,
    pub raw_schema: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Connector {
    Kafka(KafkaConnector),
    Local(LocalConnector),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaConnector {
    pub addr: std::net::SocketAddr,
    pub topic: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalConnector {
    /// The id maps to a mutable queue in memory
    pub id: Uuid,
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    /// Filter records based on predicate.
    Filter { predicate: Expr, input: Box<Plan> },
    /// Aggregate records that share a key.
    Aggregate {
        key: Expr,
        aggs: Vec<Aggregate>,
        input: Box<Plan>,
    },
}

#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Aggregate {
    pub func: AggregateFunc,
    pub expr: Expr,
}

#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    // /// A function call that takes an arbitrary number of arguments.
    // CallMany {
    //     fn: fn(Vec<Datum>) -> Datum,
    //     exprs: Vec<Expr>,
    // }
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
