use serde::{Deserialize, Serialize};

/// A named stream of data.
#[serde(rename_all = "snake_case")]
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Dataflow {
    Source(Source),
    /// A view is a named transformation from one dataflow to another.
    View(View),
}

/// A data source materializes data. It typically represents an external source
/// of data, like a topic from Apache Kafka.
#[serde(rename_all = "snake_case")]
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Source {
    name: String,
    url: String,
    schema: (), // TODO: what type?
}

/// A view transforms one dataflow into another.
#[serde(rename_all = "snake_case")]
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct View {
    name: String,
    plan: Plan,
}

#[serde(rename_all = "snake_case")]
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Plan {
    /// Source data from another dataflow.
    Source(String),
    /// Project or permute the columns in a dataflow.
    Project {
        // TODO(benesch): the Project operator will need to learn how to
        // perform expression evaluation.

        /// Sequence (and order) of indices to be retained.
        indices: Vec<usize>,
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
                indices: vec![2, 3],
                input: Box::new(Plan::Join {
                    keys: vec![(0, 0)],
                    left: Box::new(Plan::Source("orders".into())),
                    right: Box::new(Plan::Distinct(Box::new(Plan::UnionAll(vec![
                        Plan::Source("customers2018".into()),
                        Plan::Source("customers2019".into()),
                    ])))),
                })
            }
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