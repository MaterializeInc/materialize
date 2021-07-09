use serde_json::value::Value;
use structopt::StructOpt;

#[derive(Debug)]
enum PostgresType {
    Text,
    Other,
}

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Sample json was not an serde_json::Value::Object.")]
    NotAnObject,
}

fn flatten_object(map: Value) -> Result<Vec<(String, PostgresType)>, Error> {
    if let Value::Object(m) = map {
        let mut flattened = Vec::new();
        for (key, val) in m.into_iter() {
            flattened.extend(flatten_json(key, val));
        }
        Ok(flattened)
    } else {
        Err(Error::NotAnObject)
    }
}

fn flatten_json(key: String, value: Value) -> Vec<(String, PostgresType)> {
    let mut flattened: Vec<(String, PostgresType)> = Vec::new();
    match value {
        Value::Array(v) => {
            v.into_iter().enumerate().for_each(|(i, v)| {
                let new_key = format!("{}__{}", key, i);
                flattened.extend(flatten_json(new_key, v));
            });
        }
        Value::Bool(_) => flattened.push((key, PostgresType::Other)),
        Value::Number(_) => flattened.push((key, PostgresType::Other)),
        Value::String(_) => flattened.push((key, PostgresType::Text)),
        Value::Object(v) => {
            v.into_iter().for_each(|(i, v)| {
                let new_key = format!("{}__{}", key, i);
                flattened.extend(flatten_json(new_key, v));
            });
        }
        Value::Null => {}
    }
    flattened
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "json-flattened-view-gen",
    about = "Generates SQL commands to create views on json sources, flattened for easier access.\nString fields will be converted to text to avoid extra quotes."
)]
struct Opt {
    /// Sample json object
    #[structopt(parse(try_from_str = serde_json::from_str))]
    sample_json: Value,

    /// Name of json source
    #[structopt(short = "s", long = "source-name", default_value = "json_source")]
    source_name: String,

    /// Name of intermediate json view, for converting from bytes to json
    #[structopt(
        short = "i",
        long = "intermediate-view-name",
        default_value = "jsonified"
    )]
    intermediate_view_name: String,

    /// Name of output view, containing flattened json leaf nodes and strings converted to text
    #[structopt(short = "o", long = "output-view-name", default_value = "flattened")]
    output_view_name: String,
}

fn main() -> Result<(), Error> {
    let opt = Opt::from_args();
    let flattened = flatten_object(opt.sample_json)?;
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
        intermediate = opt.intermediate_view_name,
        source = opt.source_name,
        output = opt.output_view_name,
        selections = selections.join(",\n"),
    );
    Ok(())
}
