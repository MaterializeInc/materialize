// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generates a JSON catalog of every system parameter that the config sync can
//! set, i.e. every parameter yielded by [`SystemVars::iter_synced`].
//!
//! The catalog is derived by instantiating the default [`SystemVars`] and
//! iterating it, so it reflects exactly what the binary registers. The output
//! contains no build version or git SHA, so it only changes when a parameter
//! does.
//!
//! Regenerate with `bin/gen-system-parameter-catalog`.

use std::path::PathBuf;

use anyhow::{Context, bail};
use mz_sql::session::vars::SystemVars;
use mz_sql::session::vars::constraints::ValueConstraint;
use serde::Serialize;

/// Version of the catalog format itself. Bump on any change to the shape of
/// [`Catalog`] or [`Parameter`] that existing consumers could misread.
const FORMAT_VERSION: u32 = 1;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Catalog {
    format_version: u32,
    parameters: Vec<Parameter>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Parameter {
    name: String,
    /// The type name the registry reports.
    #[serde(rename = "type")]
    type_name: String,
    /// The compiled-in default, in a form that parses back to the same value,
    /// see `defaults_round_trip_through_parse`.
    default: String,
    description: String,
    /// One of `environment`, `cluster`, `replica`.
    scope: &'static str,
    user_visible: bool,
    /// `None` when the parameter has no constraint.
    #[serde(skip_serializing_if = "Option::is_none")]
    constraint: Option<&'static str>,
    /// Name of the feature flag that must be enabled to get or set the
    /// parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    requires_feature_flag: Option<&'static str>,
    /// Whether the parameter is backed by a dyncfg, as opposed to a
    /// `VarDefinition` declared in `mz-sql`.
    dyncfg: bool,
}

/// Returns the kind of a constraint.
///
/// Domain constraints are opaque trait objects that cannot describe their
/// bounds, so only their kind is reported.
fn constraint_kind(constraint: &ValueConstraint) -> &'static str {
    match constraint {
        ValueConstraint::ReadOnly => "read_only",
        ValueConstraint::Fixed => "fixed",
        ValueConstraint::Domain(_) => "domain",
    }
}

/// Builds the catalog from the default [`SystemVars`], sorted by name.
fn build_catalog() -> Catalog {
    let vars = SystemVars::new();
    let mut parameters: Vec<_> = vars
        .iter_synced()
        .map(|var| {
            let name = var.name();
            let definition = vars
                .definition(name)
                .unwrap_or_else(|| panic!("synced var {name} should have a definition"));
            Parameter {
                name: name.to_string(),
                type_name: var.type_name().into_owned(),
                default: definition.default_value().format(),
                description: var.description().to_string(),
                scope: var.scope().as_str(),
                user_visible: definition.user_visible,
                constraint: definition.constraint.as_ref().map(constraint_kind),
                requires_feature_flag: definition
                    .require_feature_flag
                    .map(|f| f.flag.name.as_str()),
                dyncfg: vars.is_dyncfg_var(name),
            }
        })
        .collect();
    parameters.sort_by(|a, b| a.name.cmp(&b.name));
    Catalog {
        format_version: FORMAT_VERSION,
        parameters,
    }
}

/// Renders the catalog as pretty-printed JSON with a trailing newline.
fn render_catalog() -> Result<String, anyhow::Error> {
    let mut out = serde_json::to_string_pretty(&build_catalog())?;
    out.push('\n');
    Ok(out)
}

fn main() -> Result<(), anyhow::Error> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [output] = args.as_slice() else {
        bail!("usage: mz-system-parameter-catalog <output.json>");
    };
    let output = PathBuf::from(output);
    std::fs::write(&output, render_catalog()?)
        .with_context(|| format!("writing {}", output.display()))
}

#[cfg(test)]
mod tests {
    use mz_sql::session::vars::VarInput;

    use super::*;

    fn parameters() -> Vec<serde_json::Value> {
        let doc: serde_json::Value = serde_json::from_str(&render_catalog().unwrap()).unwrap();
        doc["parameters"].as_array().unwrap().clone()
    }

    fn find<'a>(params: &'a [serde_json::Value], name: &str) -> &'a serde_json::Value {
        params
            .iter()
            .find(|p| p["name"] == name)
            .unwrap_or_else(|| panic!("{name} missing from catalog"))
    }

    #[mz_ore::test]
    fn deterministic_and_sorted() {
        let a = render_catalog().unwrap();
        assert_eq!(a, render_catalog().unwrap());
        assert!(a.ends_with("}\n"));
        let params = parameters();
        let names: Vec<_> = params.iter().map(|p| p["name"].as_str().unwrap()).collect();
        let mut sorted = names.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(names, sorted, "names must be sorted and unique");
    }

    #[mz_ore::test]
    fn contains_every_synced_parameter() {
        let vars = SystemVars::new();
        let params = parameters();
        let expected: Vec<_> = vars.iter_synced().map(|v| v.name()).collect();
        assert!(!expected.is_empty());
        assert_eq!(params.len(), expected.len());
        for name in expected {
            find(&params, name);
        }
    }

    #[mz_ore::test]
    fn defaults_round_trip_through_parse() {
        let vars = SystemVars::new();
        let mut failures = Vec::new();
        for param in parameters() {
            let name = param["name"].as_str().unwrap();
            let default = param["default"].as_str().unwrap();
            let definition = vars.definition(name).unwrap();
            match definition.parse(VarInput::Flat(default)) {
                Ok(parsed) if parsed.format() == default => {}
                Ok(parsed) => failures.push(format!(
                    "{name}: {default:?} parses to {:?}",
                    parsed.format()
                )),
                Err(e) => failures.push(format!("{name}: {default:?} does not parse: {e}")),
            }
        }
        assert!(
            failures.is_empty(),
            "defaults that do not round-trip:\n{}",
            failures.join("\n")
        );
    }

    #[mz_ore::test]
    fn known_parameters() {
        let params = parameters();

        let dyncfg = find(&params, "enable_column_paged_batcher");
        assert_eq!(dyncfg["dyncfg"], true);
        assert_eq!(dyncfg["scope"], "replica");

        let flag = find(&params, "enable_alter_swap");
        assert_eq!(flag["dyncfg"], false);
        assert_eq!(flag["scope"], "environment");
    }
}
