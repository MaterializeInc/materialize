// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Any context outside of `Cargo.toml` that is necessary for generating BUILD targets.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, anyhow};
use camino::Utf8PathBuf;
use guppy::graph::{BuildTargetId, PackageMetadata};
use quote::ToTokens;

use crate::config::{CrateConfig, GlobalConfig};

#[derive(Default, Debug, Clone)]
pub struct CrateContext {
    /// Context from the crate's build script, if one exists.
    pub build_script: Option<BuildScriptContext>,
}

impl CrateContext {
    /// Generates necessary external (non-`Cargo.toml`) context for a crate.
    pub fn generate(
        config: &GlobalConfig,
        crate_config: &CrateConfig,
        metadata: &PackageMetadata<'_>,
    ) -> Result<CrateContext, anyhow::Error> {
        tracing::debug!(name = metadata.name(), "generating context");

        let mut context = CrateContext::default();

        if let Some(build) = metadata.build_target(&BuildTargetId::BuildScript) {
            if !crate_config.build().skip_proto_search() {
                let build_script_context =
                    BuildScriptContext::generate(config, build.path()).context("build script")?;
                context.build_script = Some(build_script_context);
            }
        }

        Ok(context)
    }
}

#[derive(Default, Debug, Clone)]
pub struct BuildScriptContext {
    /// What protobuf files, if any, this build script generates bindings for.
    pub generated_protos: Vec<String>,
    /// Relative paths of dependencies for the protobuf files we generate.
    pub proto_dependencies: BTreeSet<Utf8PathBuf>,
}

impl BuildScriptContext {
    pub fn generate(
        config: &GlobalConfig,
        build_script_path: impl AsRef<Path>,
    ) -> Result<BuildScriptContext, anyhow::Error> {
        let build_script_path = build_script_path.as_ref();
        tracing::debug!(?build_script_path, "generating build script context");

        let mut context = BuildScriptContext::default();

        let mut file = File::open(build_script_path)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;

        // Parse the AST of the build script to find any invocations of protobuf generation and the
        // files we're attempting to generate.
        let ast = syn::parse_file(&content)?;
        let proto_files: Vec<String> = ast
            .items
            .iter()
            .filter_map(|item| match item {
                syn::Item::Fn(func) => Some(func),
                _ => None,
            })
            .flat_map(|func| {
                func.block.stmts.iter().filter_map(|stmt| match stmt {
                    syn::Stmt::Expr(expr, _) => Some(expr),
                    _ => None,
                })
            })
            .filter(|expr| find_proto_build(config, expr))
            .map(|expr| find_proto_files(config, expr.to_token_stream()))
            .flat_map(|files| files.into_iter())
            // Remove the first component of the path.
            .map(|s| {
                tracing::debug!(path = s, "protobuf path");
                let mut path = Utf8PathBuf::new();
                for c in camino::Utf8PathBuf::from(s).components().skip(1) {
                    path.push(c);
                }
                path.to_string()
            })
            .collect();

        // Parse the protobuf files we just found to determine dependencies.
        let crate_root_path = build_script_path
            .parent()
            .ok_or_else(|| anyhow!("build script at the root of the filesystem?"))?;
        let proto_dependencies: BTreeSet<_> = proto_files
            .iter()
            .map(|sub_path| {
                tracing::debug!(?sub_path, "protobuf dependency");
                let full_path = crate_root_path.join(sub_path);
                parse_proto_dependencies(&full_path).context("parsing build script proto")
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flat_map(|paths| paths.into_iter())
            .collect();

        context.generated_protos = proto_files;
        context.proto_dependencies = proto_dependencies;

        Ok(context)
    }
}

// TODO(parkmycar): There is almost definitely a better way to do this AST parsing.
fn find_proto_build(config: &GlobalConfig, expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::Path(func_path) => {
            let calls_proto_gen = func_path.path.segments.iter().any(|segment| {
                config
                    .proto_build_crates
                    .iter()
                    .any(|proto_crate_names| segment.ident == proto_crate_names)
            });
            calls_proto_gen
        }
        syn::Expr::Call(call) => find_proto_build(config, &call.func),
        syn::Expr::MethodCall(call) => find_proto_build(config, &call.receiver),
        syn::Expr::Block(inner) => inner.block.stmts.iter().any(|s| match &s {
            syn::Stmt::Expr(expr, _) => find_proto_build(config, expr),
            _ => false,
        }),
        syn::Expr::Try(inner) => find_proto_build(config, &inner.expr),
        _ => false,
    }
}

fn find_proto_files(_config: &GlobalConfig, tokens: proc_macro2::TokenStream) -> Vec<String> {
    let mut files = Vec::new();
    for token in tokens {
        match token {
            proc_macro2::TokenTree::Literal(val) => {
                // Parse the token as a LitStr to remove the `"` around the literal val.
                let val = syn::parse2::<syn::LitStr>(val.to_token_stream())
                    .expect("shouldn't fail")
                    .value();
                if val.ends_with(".proto") {
                    files.push(val);
                }
            }
            proc_macro2::TokenTree::Group(group) => {
                let inner_files = find_proto_files(_config, group.stream());
                files.extend(inner_files);
            }
            _ => (),
        }
    }
    files
}

/// Opens and parses the provided [`Path`] to a protobuf file, returning all
/// imports from the file.
fn parse_proto_dependencies(
    full_path: &Path,
) -> Result<BTreeSet<camino::Utf8PathBuf>, anyhow::Error> {
    tracing::debug!(proto = ?full_path, "reading deps");

    let mut proto_file = File::open(full_path)?;
    let mut content = String::new();
    proto_file.read_to_string(&mut content)?;

    let proto_desc = protobuf_parse::pure::parse_dependencies(&content).context("parsing proto")?;
    let dependencies: BTreeSet<_> = proto_desc
        .dependency
        .into_iter()
        .map(camino::Utf8PathBuf::from)
        .collect();

    Ok(dependencies)
}
