// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use anyhow::bail;
use serde_json::value::Value;

#[derive(Debug)]
enum PostgresType {
    Text,
    Other,
}

/// Flatten a serde_json::Value::Object into a vector of column names and the Postgres type to use
/// for that column.
///
/// This will return an error if the provided Value is not an Object variant.
fn flatten_object(map: Value) -> Result<Vec<(String, PostgresType)>, anyhow::Error> {
    if let Value::Object(m) = map {
        let mut flattened = Vec::new();
        for (key, val) in m.into_iter() {
            flatten_single_value(&mut flattened, key, val);
        }
        Ok(flattened)
    } else {
        bail!("sample JSON is not an object");
    }
}

/// Flatten the provided Value using keys prefixed with the given String. New column names and
/// their Postgres types will be appended to the provided vector.
fn flatten_single_value(flattened: &mut Vec<(String, PostgresType)>, key: String, value: Value) {
    match value {
        Value::Array(v) => {
            v.into_iter().enumerate().for_each(|(i, v)| {
                let new_key = format!("{}__{}", key, i);
                flatten_single_value(flattened, new_key, v);
            });
        }
        Value::Bool(_) => flattened.push((key, PostgresType::Other)),
        Value::Number(_) => flattened.push((key, PostgresType::Other)),
        Value::String(_) => flattened.push((key, PostgresType::Text)),
        Value::Object(v) => {
            v.into_iter().for_each(|(k, v)| {
                let new_key = format!("{}__{}", key, k);
                flatten_single_value(flattened, new_key, v);
            });
        }
        Value::Null => {}
    }
}

/// Generates SQL commands to create views on json sources, flattened for easier
/// access. String fields will be converted to text to avoid extra quotes.
#[derive(Debug, clap::Parser)]
struct Args {
    /// Sample json object
    #[clap(parse(try_from_str = serde_json::from_str))]
    sample_json: Value,

    /// Name of json source
    #[clap(short = 's', long = "source-name", default_value = "json_source")]
    source_name: String,

    /// Name of intermediate json view, for converting from bytes to json
    #[clap(
        short = 'i',
        long = "intermediate-view-name",
        default_value = "jsonified"
    )]
    intermediate_view_name: String,

    /// Name of output view, containing flattened json leaf nodes and strings converted to text
    #[clap(short = 'o', long = "output-view-name", default_value = "flattened")]
    output_view_name: String,
}

fn main() -> Result<(), anyhow::Error> {
    let args: Args = ore::cli::parse_args();
    let flattened = flatten_object(args.sample_json)?;
    let selections: Vec<String> = flattened
        .iter()
        .map(|(k, v)| match v {
            PostgresType::Text => {
                let temp = format!("        data->'{}' AS {}", k.replace("__", "'->'"), k);
                let (left, right) = temp.rsplit_once("->").unwrap();
                format!("{}->>{}", left, right)
            }
            _ => format!("        data->'{}' AS {}", k.replace("__", "'->'"), k),
        })
        .collect();

    println!(
        "CREATE MATERIALIZED VIEW {intermediate} AS
    SELECT CAST(data AS jsonb) AS data
    FROM (
        SELECT convert_from(data, 'utf8') AS data
        FROM {source}
    );
CREATE MATERIALIZED VIEW {output} AS
    SELECT
{selections}
    FROM {intermediate};",
        intermediate = args.intermediate_view_name,
        source = args.source_name,
        output = args.output_view_name,
        selections = selections.join(",\n"),
    );
    Ok(())
}
