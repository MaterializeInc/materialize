use serde::{Deserialize, Serialize};

/// A named stream of data.
#[serde(rename_all = "snake_case")]
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Dataflow {
    Source(Source),
    /// A view is a named transformation from one dataflow to another.
    View(View),
}

/// A data source materializes data. It typically represents an external source
/// of data, like a topic from Apache Kafka.
#[serde(rename_all = "snake_case")]
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub name: String,
    pub url: String,
    pub schema: Schema,
}

/// A view transforms one dataflow into another.
#[serde(rename_all = "snake_case")]
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct View {
    pub name: String,
    pub plan: Plan,
    pub schema: Schema,
}

#[serde(rename_all = "snake_case")]
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
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
        /// Pairs of indices whose values must be equal.
        keys: Vec<(usize, usize)>,
        /// Plan for the left input.
        left: Box<Plan>,
        /// Plan for the right input.
        right: Box<Plan>,
    },
}

#[serde(rename_all = "snake_case")]
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Column(usize),
    Literal(Scalar),
}

#[serde(rename_all = "snake_case")]
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Scalar {
    // TODO(benesch): expand these to support the full set of Avro datatypes,
    // at the very least.
    Int(i64),
    String(String),
}

#[serde(rename_all = "snake_case")]
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Schema(pub Vec<(Option<String>, Type)>);

impl Schema {
    pub fn index(&self, name: &str) -> Result<usize, failure::Error> {
        for (i, (n, _)) in self.0.iter().enumerate() {
            match n {
                Some(n) if n == name => return Ok(i),
                _ => (),
            }
        }
        use failure::bail;
        bail!("unknown column {}", name)
    }
}

#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Type {
    Int,
    String,
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::error::Error;

    use super::*;

    /// Verify that a basic plan serializes and deserializes to JSON sensibly.
    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn Error>> {
        let dataflow = Dataflow::View(View {
            name: "report".into(),
            plan: Plan::Project {
                outputs: vec![Expr::Column(1), Expr::Column(2)],
                input: Box::new(Plan::Join {
                    keys: vec![(0, 0)],
                    left: Box::new(Plan::Source("orders".into())),
                    right: Box::new(Plan::Distinct(Box::new(Plan::UnionAll(vec![
                        Plan::Source("customers2018".into()),
                        Plan::Source("customers2019".into()),
                    ])))),
                }),
            },
            schema: Schema(Vec::new()), // XXX
        });

        let encoded = r#"{
  "view": {
    "name": "report",
    "plan": {
      "project": {
        "indices": [
          2,
          3
        ],
        "input": {
          "join": {
            "keys": [
              [
                0,
                0
              ]
            ],
            "left": {
              "source": "orders"
            },
            "right": {
              "distinct": {
                "union_all": [
                  {
                    "source": "customers2018"
                  },
                  {
                    "source": "customers2019"
                  }
                ]
              }
            }
          }
        }
      }
    }
  }
}"#;
        assert_eq!(encoded, serde_json::to_string_pretty(&dataflow)?);

        let decoded: Dataflow = serde_json::from_str(encoded)?;
        assert_eq!(decoded, dataflow);

        Ok(())
    }
}
