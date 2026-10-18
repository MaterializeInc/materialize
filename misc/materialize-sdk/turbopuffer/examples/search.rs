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

//! Keeps a Materialize table, source, or materialized view searchable in
//! turbopuffer, and searches it.
//!
//! ```text
//! export MATERIALIZE_URL="host=localhost port=6875 user=materialize"
//! export TURBOPUFFER_API_KEY=...
//! export SOURCE=materialize.public.articles KEY=id EMBED=body NAME=articles
//! cargo run --example search -- sink
//! cargo run --example search -- search "a cat on a windowsill"
//! ```
//!
//! `sink` runs until it is stopped, and resumes from its checkpoint when it
//! starts again. `search` prints the five closest rows of the incarnation
//! readers use.
//!
//! - `KEY`: the key columns, separated by commas, unique in the source.
//! - `EMBED`: a text column turbopuffer embeds for search by meaning.
//! - `FULL_TEXT`: text columns, separated by commas, for full-text search.
//!   With `EMBED` and `FULL_TEXT` both set, `search` runs both and lists each.
//! - `NAME`: the prefix of the sink's namespaces, `materialize-search` by
//!   default. `SINK_ID` names the checkpoint, `NAME` by default.
//! - `TURBOPUFFER_REGION`: `gcp-us-central1` by default.
//! - `MODEL` and `DIMS`: the embedding model, `nvidia/nemotron-3-embed-8b`
//!   with 1024 dimensions by default.

// The repository's clippy rules ask for `mz_ore` wrappers around
// `tokio::spawn`, which a standalone SDK cannot depend on.
#![allow(clippy::disallowed_methods)]

use std::error::Error;
use std::path::Path;

use materialize_sdk::{DurableOptions, DurableSink, Envelope, ObjectName};
use materialize_sdk_turbopuffer::{Connection, Reader, TurbopufferSink};
use openssl::ssl::{SslConnector, SslFiletype, SslMethod};
use openssl::x509::store::X509Lookup;
use postgres_openssl::MakeTlsConnector;
use serde_json::{Value, json};
use tokio_postgres::Config;

fn env(name: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(name).map_err(|_| format!("set {name}").into())
}

fn list(name: &str) -> Vec<String> {
    let value = std::env::var(name).unwrap_or_default();
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

/// A TLS connector that trusts the system's certificate authorities, as the
/// Postgres example's does.
fn tls() -> Result<MakeTlsConnector, Box<dyn Error>> {
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    let probed = openssl_probe::probe();
    let macos = Path::new("/etc/ssl/cert.pem");
    let file = probed
        .cert_file
        .or_else(|| macos.exists().then(|| macos.to_path_buf()));
    if let Some(file) = file {
        builder.set_ca_file(file)?;
    }
    for dir in probed.cert_dir.iter().filter_map(|dir| dir.to_str()) {
        builder
            .cert_store_mut()
            .add_lookup(X509Lookup::hash_dir())?
            .add_dir(dir, SslFiletype::PEM)?;
    }
    Ok(MakeTlsConnector::new(builder.build()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let region = std::env::var("TURBOPUFFER_REGION").unwrap_or_else(|_| "gcp-us-central1".into());
    let turbopuffer = Connection::new(&region, env("TURBOPUFFER_API_KEY")?);
    let name = std::env::var("NAME").unwrap_or_else(|_| "materialize-search".into());
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("sink") => sink(turbopuffer, name).await,
        Some("search") => {
            let text = args.collect::<Vec<_>>().join(" ");
            search(turbopuffer, name, &text).await
        }
        _ => Err("run with `sink`, or with `search` and the text to search for".into()),
    }
}

async fn sink(turbopuffer: Connection, name: String) -> Result<(), Box<dyn Error>> {
    let materialize: Config = env("MATERIALIZE_URL")?.parse()?;
    let source = env("SOURCE")?;
    let [database, schema, object] = source.split('.').collect::<Vec<_>>()[..] else {
        return Err("SOURCE is database.schema.name".into());
    };
    let source = ObjectName {
        database: Some(database.into()),
        schema: schema.into(),
        name: object.into(),
    };
    let key = list("KEY");
    if key.is_empty() {
        return Err("set KEY to the source's key columns".into());
    }
    let sink_id = std::env::var("SINK_ID").unwrap_or_else(|_| name.clone());

    let mut sink = TurbopufferSink::new(&turbopuffer, sink_id.as_str(), name.as_str())?
        .full_text_search(list("FULL_TEXT"));
    if let Ok(column) = std::env::var("EMBED") {
        let model = std::env::var("MODEL").unwrap_or_else(|_| "nvidia/nemotron-3-embed-8b".into());
        let dims = std::env::var("DIMS").map_or(Ok(1024), |dims| dims.parse())?;
        sink = sink.embed(column, model, dims);
    }
    // The connection uses TLS when the server offers it, as Materialize Cloud
    // does, and its URL's `sslmode` can require it.
    let tls = tls()?;
    let mut durable = DurableSink::new(sink_id.as_str(), sink, DurableOptions::default());
    if durable
        .setup(&materialize, tls.clone(), &source, Envelope::Upsert { key })
        .await?
    {
        println!("created the checkpoint for {sink_id}");
    }
    println!("keeping {database}.{schema}.{object} searchable in {name}__* as {sink_id}");
    durable.run(&materialize, tls).await?;
    Ok(())
}

async fn search(turbopuffer: Connection, name: String, text: &str) -> Result<(), Box<dyn Error>> {
    let reader = Reader::new(&turbopuffer, name)?;
    let embed = std::env::var("EMBED").ok();
    let full_text = list("FULL_TEXT");
    let mut queries = Vec::new();
    if let Some(column) = &embed {
        queries.push(("by meaning", json!([column, "ANN", ["Embed", text]])));
    }
    if let Some(column) = full_text.first() {
        queries.push(("by words", json!([column, "BM25", text])));
    }
    if queries.is_empty() {
        return Err("set EMBED or FULL_TEXT to the column to search".into());
    }
    let body = json!({
        "queries": queries
            .iter()
            .map(|(_, rank_by)| {
                let mut query = json!({"rank_by": rank_by, "limit": 5});
                // Vectors are long and say nothing to a person. turbopuffer
                // refuses both options at once.
                match &embed {
                    Some(column) => query["exclude_attributes"] = json!([format!("embed_{column}")]),
                    None => query["include_attributes"] = json!(true),
                }
                query
            })
            .collect::<Vec<_>>(),
    });
    println!("searching {} for {text:?}", reader.namespace().await?);
    let Some(response) = reader.query(body).await? else {
        println!("the view is empty");
        return Ok(());
    };
    let results = response["results"].as_array().cloned().unwrap_or_default();
    for ((label, _), result) in queries.iter().zip(results) {
        println!("\n{label}:");
        for row in result["rows"].as_array().into_iter().flatten() {
            print_row(row);
        }
    }
    Ok(())
}

fn print_row(row: &Value) {
    let Some(row) = row.as_object() else {
        return;
    };
    let hidden = ["id", "$dist", "$score", "deleted", "mz_timestamp"];
    let fields: Vec<String> = row
        .iter()
        .filter(|(key, _)| !hidden.contains(&key.as_str()) && !key.starts_with("mz_hash_"))
        .map(|(key, value)| match value {
            Value::String(text) if text.chars().count() > 80 => {
                format!("{key}: {}...", text.chars().take(80).collect::<String>())
            }
            Value::String(text) => format!("{key}: {text}"),
            other => format!("{key}: {other}"),
        })
        .collect();
    println!("  {}  {}", row["id"], fields.join(", "));
}
