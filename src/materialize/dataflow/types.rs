// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use expr::RelationExpr;
use repr::RelationType;

/// System-wide update type.
pub type Diff = isize;

/// System-wide timestamp type.
pub type Timestamp = u64;

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
    pub fn typ(&self) -> &RelationType {
        match self {
            Dataflow::Source(src) => &src.typ,
            Dataflow::Sink(_) => panic!(
                "programming error: Dataflow.typ called on Sink variant, \
                 but sinks don't have a type"
            ),
            Dataflow::View(view) => &view.typ,
        }
    }

    /// Collects the names of the dataflows that this dataflow depends upon.
    pub fn uses(&self) -> Vec<&str> {
        let mut out = Vec::new();
        match self {
            Dataflow::Source(_) => (),
            Dataflow::Sink(sink) => out.push(sink.from.0.as_str()),
            Dataflow::View(view) => view.relation_expr.uses_inner(&mut out),
        }
        out
    }
}

impl metastore::Dataflow for Dataflow {}

/// A source materializes data. It typically represents an external source of
/// data, like a topic from Apache Kafka.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub name: String,
    pub connector: SourceConnector,
    pub typ: RelationType,
}

#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Sink {
    pub name: String,
    pub from: (String, RelationType),
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
pub struct LocalSourceConnector {
    pub uuid: Uuid,
}

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
    pub relation_expr: RelationExpr,
    pub typ: RelationType,
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::error::Error;

    use super::*;
    use repr::{ColumnType, ScalarType};

    /// Verify that a basic relation_expr serializes and deserializes to JSON sensibly.
    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn Error>> {
        let dataflow = Dataflow::View(View {
            name: "report".into(),
            relation_expr: RelationExpr::Project {
                outputs: vec![1, 2],
                input: Box::new(RelationExpr::Join {
                    inputs: vec![
                        RelationExpr::Get {
                            name: "orders".into(),
                            typ: RelationType {
                                column_types: vec![ColumnType::new(ScalarType::Int64).name("id")],
                            },
                        },
                        RelationExpr::Distinct {
                            input: Box::new(RelationExpr::Union {
                                left: Box::new(RelationExpr::Get {
                                    name: "customers2018".into(),
                                    typ: RelationType {
                                        column_types: vec![
                                            ColumnType::new(ScalarType::Int64).name("id")
                                        ],
                                    },
                                }),
                                right: Box::new(RelationExpr::Get {
                                    name: "customers2019".into(),
                                    typ: RelationType {
                                        column_types: vec![
                                            ColumnType::new(ScalarType::Int64).name("id")
                                        ],
                                    },
                                }),
                            }),
                        },
                    ],
                    variables: vec![vec![(0, 0), (1, 0)]],
                }),
            },
            typ: RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::String).name("name"),
                    ColumnType::new(ScalarType::Int32).name("quantity"),
                ],
            },
        });

        let decoded: Dataflow = serde_json::from_str(&serde_json::to_string_pretty(&dataflow)?)?;
        assert_eq!(decoded, dataflow);

        Ok(())
    }
}
