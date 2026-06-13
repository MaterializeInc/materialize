// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generates a YAML file of every `metric!` definition in the Materialize
//! source tree for the user-facing metrics documentation.
//!
//! It walks the Rust AST with `syn`, visiting every `metric!` invocation
//! outside of test code.
//!
//! Regenerate with `bin/gen-metrics-catalog`.

use std::process;

use anyhow::{Context, bail};
use mz_ore::metrics::MetricVisibility;
use quote::ToTokens;
use serde::Serialize;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{
    Attribute, Expr, Ident, ImplItemFn, ItemFn, ItemImpl, ItemMod, Lit, Macro, Token, braced,
    bracketed,
};
use walkdir::WalkDir;

// Known directories that we want to ignore when walking the source tree.
static IGNORED_DIRS: &[&str] = &["target", "tests", "benches", "examples"];

/// Maps a `metric!`'s `visibility:` expression to a [`MetricVisibility`].
fn visibility_from_expr(expr: Option<&Expr>) -> MetricVisibility {
    let Some(Expr::Path(path)) = expr else {
        return MetricVisibility::Internal;
    };
    match path
        .path
        .segments
        .last()
        .map(|s| s.ident.to_string())
        .as_deref()
    {
        Some("Public") => MetricVisibility::Public,
        _ => MetricVisibility::Internal,
    }
}

/// The catalog as serialized to YAML.
#[derive(Serialize)]
struct Catalog {
    metrics: Vec<MetricDoc>,
}

/// A single `metric!` definition.
#[derive(Serialize)]
struct MetricDoc {
    name: String,
    help: String,
    /// The metric's label keys: the union of its `var_labels` and the keys of
    /// its `const_labels`, sorted and deduplicated. Omitted from the YAML when
    /// the metric has no labels.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    labels: Vec<String>,
    /// Repo-relative path to the file the metric is defined in.
    source: String,
    /// Whether customers should build dashboards and alerts on this metric.
    visibility: MetricVisibility,
}

/// Parses the token body of a `metric!` invocation. Mirrors the grammar in
/// `mz_ore::metric!`.
/// Only `name`, `help`, `subsystem`, `visibility`, and the label keys
/// (`var_labels` and the keys of `const_labels`) are retained; the remaining
/// fields are consumed but discarded so token parsing stays in sync with the
/// real macro grammar.
struct MetricArgs {
    name: Option<Expr>,
    help: Option<Expr>,
    subsystem: Option<Expr>,
    visibility: Option<Expr>,
    /// The keys of `const_labels` (the `k` in each `k => v` pair); the values
    /// are dropped.
    const_label_keys: Vec<Expr>,
    /// The `var_labels` names.
    var_labels: Vec<Expr>,
}

impl Parse for MetricArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = MetricArgs {
            name: None,
            help: None,
            subsystem: None,
            visibility: None,
            const_label_keys: Vec::new(),
            var_labels: Vec::new(),
        };
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "name" => args.name = Some(input.parse()?),
                "help" => args.help = Some(input.parse()?),
                "subsystem" => args.subsystem = Some(input.parse()?),
                "visibility" => args.visibility = Some(input.parse()?),
                // Retain the label keys; the const_label *values* are consumed
                // but discarded.
                "const_labels" => {
                    let content;
                    braced!(content in input);
                    while !content.is_empty() {
                        let k: Expr = content.parse()?;
                        content.parse::<Token![=>]>()?;
                        let _v: Expr = content.parse()?;
                        args.const_label_keys.push(k);
                        if content.peek(Token![,]) {
                            content.parse::<Token![,]>()?;
                        }
                    }
                }
                "var_labels" => {
                    let content;
                    bracketed!(content in input);
                    let labels = Punctuated::<Expr, Token![,]>::parse_terminated(&content)?;
                    args.var_labels.extend(labels);
                }
                _ => {
                    // Unknown field; consume one expression to stay in sync.
                    let _: Expr = input.parse()?;
                }
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(args)
    }
}

impl MetricArgs {
    fn into_doc(self, source: &str) -> MetricDoc {
        let name = self
            .name
            .as_ref()
            .map(expr_to_string)
            .unwrap_or_else(|| "<unknown>".into());
        // A `subsystem` prefixes the metric name, matching Prometheus'
        // `fq_name` (`<subsystem>_<name>`). It is frequently a runtime value
        // (e.g. `subsystem: component`), in which case we glob it to `*`.
        let name = match &self.subsystem {
            Some(subsystem) => format!("{}_{name}", subsystem_to_string(subsystem)),
            None => name,
        };
        let visibility = visibility_from_expr(self.visibility.as_ref());
        // A metric's labels are the union of its `var_labels` and the keys of
        // its `const_labels`.
        let mut labels: Vec<String> = self
            .const_label_keys
            .iter()
            .chain(self.var_labels.iter())
            .map(expr_to_string)
            .collect();
        labels.sort();
        labels.dedup();
        MetricDoc {
            name,
            help: self.help.as_ref().map(expr_to_string).unwrap_or_default(),
            labels,
            source: source.to_owned(),
            visibility,
        }
    }
}

/// Renders a `subsystem` for use as a metric-name prefix: a string literal is
/// used verbatim, while anything else (a runtime value like `component`) globs
/// to `*`, since its concrete value is only known at runtime.
fn subsystem_to_string(e: &Expr) -> String {
    match e {
        Expr::Lit(lit) => match &lit.lit {
            Lit::Str(s) => s.value(),
            _ => "*".into(),
        },
        _ => "*".into(),
    }
}

/// Whether the macro's path ends in `name`.
fn macro_named(mac: &Macro, name: &str) -> bool {
    mac.path
        .segments
        .last()
        .map(|s| s.ident == name)
        .unwrap_or(false)
}

/// Turns a Rust format string into a glob: `{}` and `{name}` become `*`,
/// while the escapes `{{` and `}}` collapse to literal `{` and `}`.
fn format_template_to_glob(fmt: &str) -> String {
    let mut out = String::new();
    let mut chars = fmt.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            // Handle escaped opening curly braces.
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                out.push('{');
            }
            //
            '{' => {
                // Consume the placeholder up to and including its closing `}`.
                for n in chars.by_ref() {
                    if n == '}' {
                        break;
                    }
                }
                // Replace the placeholder {} with *
                out.push('*');
            }
            // Handle escaped closing curly braces.
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                out.push('}');
            }
            _ => out.push(c),
        }
    }
    out
}

/// Extracts a `format!`'s template string and globs its placeholders, if the
/// first argument is a string literal.
fn format_macro_to_glob(mac: &Macro) -> Option<String> {
    let args = mac
        .parse_body_with(Punctuated::<Expr, Token![,]>::parse_terminated)
        .ok()?;
    match args.first()? {
        Expr::Lit(lit) => match &lit.lit {
            Lit::Str(s) => Some(format_template_to_glob(&s.value())),
            _ => None,
        },
        _ => None,
    }
}

/// Renders an expr for display:
///
/// * a string literal becomes its value;
/// * a `format!("mz_foo_{}_bar", ..)` becomes the template with each `{..}`
///   placeholder replaced by `*` (e.g. `mz_foo_*_bar`), since these values
///   are only known at runtime.
/// * anything else falls back to its token form.
fn expr_to_string(e: &Expr) -> String {
    match e {
        Expr::Lit(lit) => match &lit.lit {
            Lit::Str(s) => s.value(),
            _ => e.to_token_stream().to_string(),
        },
        Expr::Macro(m) if macro_named(&m.mac, "format") => {
            format_macro_to_glob(&m.mac).unwrap_or_else(|| e.to_token_stream().to_string())
        }
        _ => e.to_token_stream().to_string(),
    }
}

/// Whether `attrs` mark an item as test-only, so the user-facing catalog should
/// skip it (and everything inside it). This deliberately just checks whether any
/// attribute mentions `test`, which catches both test-runner attributes
/// (`test`, `mz_ore::test`, `tokio::test`, …) and `cfg(test)` gates.
///
/// It's a coarse over-approximation: a feature name that
/// merely contains "test" would also match. However the
/// simplicity is worth more than precision for a docs generator.
fn is_test_only(attrs: &[Attribute]) -> bool {
    attrs
        .iter()
        .any(|attr| attr.to_token_stream().to_string().contains("test"))
}

struct Collector<'a> {
    source: &'a str,
    out: &'a mut Vec<MetricDoc>,
}

impl<'ast> Visit<'ast> for Collector<'_> {
    // Prune test-only subtrees before they reach `visit_macro`, so the
    // user-facing catalog excludes test metrics.
    fn visit_item_mod(&mut self, node: &'ast ItemMod) {
        if !is_test_only(&node.attrs) {
            syn::visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_fn(&mut self, node: &'ast ItemFn) {
        if !is_test_only(&node.attrs) {
            syn::visit::visit_item_fn(self, node);
        }
    }

    fn visit_item_impl(&mut self, node: &'ast ItemImpl) {
        if !is_test_only(&node.attrs) {
            syn::visit::visit_item_impl(self, node);
        }
    }

    fn visit_impl_item_fn(&mut self, node: &'ast ImplItemFn) {
        if !is_test_only(&node.attrs) {
            syn::visit::visit_impl_item_fn(self, node);
        }
    }

    fn visit_macro(&mut self, mac: &'ast Macro) {
        if macro_named(mac, "metric") {
            match mac.parse_body::<MetricArgs>() {
                Ok(args) => self.out.push(args.into_doc(self.source)),
                Err(e) => eprintln!("warn: failed to parse metric! in {}: {e}", self.source),
            }
        } else if macro_named(mac, "vec") {
            // `syn` leaves a macro's body as opaque tokens, so a `metric!` nested
            // inside a `vec![..]` is never visited. Re-parse the body as an expression
            // list and run a fresh sub-visitor over it to reach those nested `metric!`s.
            if let Ok(exprs) = mac.parse_body_with(Punctuated::<Expr, Token![,]>::parse_terminated)
            {
                let mut sub = Collector {
                    source: self.source,
                    out: &mut *self.out,
                };
                for expr in &exprs {
                    sub.visit_expr(expr);
                }
            }
        }
        syn::visit::visit_macro(self, mac);
    }
}

fn run() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let (Some(src_root), Some(out_file)) = (args.next(), args.next()) else {
        bail!("usage: mz-metrics-catalog <src-root> <out-file>");
    };

    let mut entries: Vec<MetricDoc> = Vec::new();
    let walker = WalkDir::new(&src_root).into_iter().filter_entry(|e| {
        let name = e.file_name().to_str().unwrap_or("");
        // Prune test/bench/example trees and build output.
        !(e.file_type().is_dir() && IGNORED_DIRS.contains(&name))
    });
    for entry in walker {
        let entry = entry.context("walking source tree")?;
        let path = entry.path();
        if path.extension().map(|e| e == "rs").unwrap_or(false) {
            let content =
                std::fs::read_to_string(path).with_context(|| format!("reading {path:?}"))?;
            if !content.contains("metric!") {
                continue;
            }
            let ast = match syn::parse_file(&content) {
                Ok(ast) => ast,
                Err(e) => {
                    eprintln!("warn: skipping {path:?} (parse error: {e})");
                    continue;
                }
            };
            let source = path.to_string_lossy();
            let mut collector = Collector {
                source: &source,
                out: &mut entries,
            };
            collector.visit_file(&ast);
        }
    }

    // Some metrics are registered through `metric!`-wrapping macros whose names
    // are assembled at macro-expansion time (e.g. `concat!(stringify!(...))`),
    // so `syn` can't see them. Pull those directly from the crate that defines
    // them. See `mz_metrics::describe_metrics`.
    for (name, help, source) in mz_metrics::describe_metrics() {
        entries.push(MetricDoc {
            name,
            help,
            // TODO (SangJunBak): Populate labels
            labels: Vec::new(),
            source: source.to_owned(),
            visibility: MetricVisibility::Internal,
        });
    }

    // Pull tokio metrics
    for (name, help, source) in mz_ore::metrics::describe_runtime_metrics() {
        entries.push(MetricDoc {
            name,
            help,
            // TODO (SangJunBak): Populate labels
            labels: Vec::new(),
            source: source.to_owned(),
            visibility: MetricVisibility::Internal,
        });
    }

    entries.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.source.cmp(&b.source)));

    let catalog = Catalog { metrics: entries };
    let body = serde_yaml::to_string(&catalog).context("serializing catalog")?;
    let header = "# Generated by `bin/gen-metrics-catalog`. DO NOT EDIT.\n\
                  # Source of truth: `metric!` invocations in the Rust source tree.\n";
    std::fs::write(&out_file, format!("{header}{body}"))
        .with_context(|| format!("writing {out_file}"))?;
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err:#}");
        process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bare `{}` is a real placeholder and globs to `*`, e.g.
    /// `format!("mz_persist_{}_test_metric", name)`.
    #[mz_ore::test]
    fn format_template_to_glob_replaces_placeholder_with_star() {
        assert_eq!(
            format_template_to_glob("mz_persist_{}_test_metric"),
            "mz_persist_*_test_metric"
        );
    }

    // Each pair of brace is escaped, so nothing globs
    #[mz_ore::test]
    fn format_template_to_glob_even_number_of_brace_pairs() {
        assert_eq!(
            format_template_to_glob("mz_persist_{{}}_test_metric"),
            "mz_persist_{}_test_metric"
        );
    }

    /// `{{{}}}` is an escaped `{`, then a `{}` placeholder, then an escaped `}`,
    /// so it becomes `{*}`
    #[mz_ore::test]
    fn format_template_to_glob_odd_number_of_brace_pairs() {
        assert_eq!(
            format_template_to_glob("mz_persist_{{{}}}_test_metric"),
            "mz_persist_{*}_test_metric"
        );
    }

    // placeholder inside braces should not be globbed since each brace is escaped
    #[mz_ore::test]
    fn format_template_to_glob_even_number_inline_placeholder() {
        assert_eq!(
            format_template_to_glob("mz_persist_{{{{name}}}}_test_metric"),
            "mz_persist_{{name}}_test_metric"
        );
    }

    // placeholder inside braces should be globbed since it's not escaped
    #[mz_ore::test]
    fn format_template_to_glob_odd_number_inline_placeholder() {
        assert_eq!(
            format_template_to_glob("mz_persist_{{{name}}}_test_metric"),
            "mz_persist_{*}_test_metric"
        );
    }

    /// Parses a full `metric!(..)` invocation through the same `MetricArgs`
    /// path the collector uses, then renders it to a [`MetricDoc`].
    fn parse_metric(invocation: &str) -> MetricDoc {
        let mac: Macro = syn::parse_str(invocation).expect("valid macro invocation");
        let args = mac
            .parse_body::<MetricArgs>()
            .expect("valid metric! arguments");
        args.into_doc("test.rs")
    }

    /// The bare minimum: just the required `name` and `help`.
    #[mz_ore::test]
    fn parses_required_fields_only() {
        let doc = parse_metric(r#"metric!(name: "mz_foo", help: "a foo")"#);
        assert_eq!(doc.name, "mz_foo");
        assert_eq!(doc.help, "a foo");
    }

    /// A trailing comma after the last field is allowed by the macro grammar.
    #[mz_ore::test]
    fn allows_trailing_comma() {
        let doc = parse_metric(r#"metric!(name: "mz_foo", help: "a foo",)"#);
        assert_eq!(doc.name, "mz_foo");
        assert_eq!(doc.help, "a foo");
    }

    /// Every optional field present, in canonical macro order, across multiple
    /// lines — mirrors how real metrics are written in the tree. The literal
    /// `subsystem` is prepended to the name (`<subsystem>_<name>`).
    #[mz_ore::test]
    fn parses_realistic_multiline_invocation() {
        let doc = parse_metric(
            r#"metric!(
                name: "step_duration_seconds",
                help: "The time spent in each compute step_or_park call",
                subsystem: "compute",
                const_labels: {"cluster" => "compute"},
                var_labels: ["worker_id"],
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 32.0),
                rules: [rule_a(), rule_b()],
            )"#,
        );
        assert_eq!(doc.name, "compute_step_duration_seconds");
        assert_eq!(doc.help, "The time spent in each compute step_or_park call");
        // `var_labels` plus the keys of `const_labels`, sorted.
        assert_eq!(doc.labels, vec!["cluster", "worker_id"]);
    }

    /// Each optional `metric!` field parses on its own alongside the required
    /// `name`/`help`. The all-fields-together case is covered by
    /// `parses_realistic_multiline_invocation`.
    #[mz_ore::test]
    fn parses_each_optional_field() {
        let optionals = [
            r#"const_labels: {"cluster" => "compute"}"#,
            r#"var_labels: ["worker_id", "command_type"]"#,
            "buckets: histogram_seconds_buckets(0.1, 1.0)",
            "rules: [rule_a(), rule_b()]",
        ];
        for opt in optionals {
            let invocation = format!(r#"metric!(name: "mz_foo", help: "a foo metric", {opt})"#);
            let doc = parse_metric(&invocation);
            assert_eq!(doc.name, "mz_foo", "failed for: {invocation}");
            assert_eq!(doc.help, "a foo metric", "failed for: {invocation}");
        }
    }

    /// Empty `var_labels`/`rules` collections parse, and yield no labels.
    #[mz_ore::test]
    fn parses_empty_collections() {
        let doc = parse_metric(r#"metric!(name: "mz_foo", help: "h", var_labels: [], rules: [])"#);
        assert_eq!(doc.name, "mz_foo");
        assert!(doc.labels.is_empty());
    }

    /// `var_labels` and the keys of `const_labels` are merged into a single
    /// sorted, deduplicated `labels` list; the const_label *values* are dropped.
    #[mz_ore::test]
    fn collects_and_merges_label_keys() {
        let doc = parse_metric(
            r#"metric!(
                name: "mz_foo",
                help: "h",
                const_labels: {"cluster" => "compute", "build_type" => "release"},
                var_labels: ["worker_id", "command_type"],
            )"#,
        );
        assert_eq!(
            doc.labels,
            vec!["build_type", "cluster", "command_type", "worker_id"]
        );
    }

    /// Only `var_labels`, no `const_labels`.
    #[mz_ore::test]
    fn collects_var_labels_only() {
        let doc = parse_metric(r#"metric!(name: "mz_foo", help: "h", var_labels: ["source_id"])"#);
        assert_eq!(doc.labels, vec!["source_id"]);
    }

    /// A metric with no `var_labels` or `const_labels` has no labels.
    #[mz_ore::test]
    fn no_labels_renders_as_empty() {
        let doc = parse_metric(r#"metric!(name: "mz_foo", help: "h")"#);
        assert!(doc.labels.is_empty());
    }

    /// Multiple `const_labels` pairs, with a trailing comma inside the braces.
    #[mz_ore::test]
    fn parses_multiple_const_labels_with_trailing_comma() {
        let doc = parse_metric(
            r#"metric!(name: "mz_foo", help: "h", const_labels: {"a" => "1", "b" => "2",})"#,
        );
        assert_eq!(doc.name, "mz_foo");
    }

    /// A `format!` in the `name` position globs its placeholders to `*`.
    #[mz_ore::test]
    fn globs_format_macro_in_name() {
        let doc = parse_metric(r#"metric!(name: format!("mz_persist_{}_bytes", name), help: "h")"#);
        assert_eq!(doc.name, "mz_persist_*_bytes");
    }

    /// A `format!` in the `help` position is globbed too.
    #[mz_ore::test]
    fn globs_format_macro_in_help() {
        let doc =
            parse_metric(r#"metric!(name: "mz_foo", help: format!("total {} batches", name))"#);
        assert_eq!(doc.help, "total * batches");
    }

    /// Positional and named placeholders both glob to `*`.
    #[mz_ore::test]
    fn globs_format_macro_with_named_and_positional_placeholders() {
        let doc =
            parse_metric(r#"metric!(name: format!("mz_{0}_{kind}_total", a, kind), help: "h")"#);
        assert_eq!(doc.name, "mz_*_*_total");
    }

    /// A string-literal `subsystem` is prepended to the name, mirroring
    /// Prometheus' `fq_name` (`<subsystem>_<name>`).
    #[mz_ore::test]
    fn prepends_literal_subsystem() {
        let doc = parse_metric(r#"metric!(name: "requests_total", help: "h", subsystem: "http")"#);
        assert_eq!(doc.name, "http_requests_total");
    }

    /// A runtime (non-literal) `subsystem` globs to `*`, since its concrete
    /// value is only known at runtime (e.g. `subsystem: component`).
    #[mz_ore::test]
    fn globs_runtime_subsystem() {
        let doc =
            parse_metric(r#"metric!(name: "requests_total", help: "h", subsystem: component)"#);
        assert_eq!(doc.name, "*_requests_total");
    }

    /// A non-literal, non-`format!` `name` falls back to
    /// its token form.
    #[mz_ore::test]
    fn falls_back_to_token_form_for_non_literal_name() {
        let doc = parse_metric(r#"metric!(name: METRIC_NAME, help: "h")"#);
        assert_eq!(doc.name, "METRIC_NAME");
    }

    /// NOTE: the real `metric!` macro fixes field order; our parser is
    /// intentionally order-independent (it matches on the key) so the catalog
    /// stays robust to grammar tweaks. Document that leniency here.
    #[mz_ore::test]
    fn parser_tolerates_reordered_fields() {
        let doc = parse_metric(r#"metric!(help: "a foo", var_labels: ["x"], name: "mz_foo")"#);
        assert_eq!(doc.name, "mz_foo");
        assert_eq!(doc.help, "a foo");
    }

    /// A missing `name` renders as the `<unknown>` sentinel rather than failing.
    #[mz_ore::test]
    fn missing_name_renders_as_unknown() {
        let doc = parse_metric(r#"metric!(help: "h")"#);
        assert_eq!(doc.name, "<unknown>");
        assert_eq!(doc.help, "h");
    }

    /// A missing `help` renders as the empty string.
    #[mz_ore::test]
    fn missing_help_renders_as_empty() {
        let doc = parse_metric(r#"metric!(name: "mz_foo")"#);
        assert_eq!(doc.name, "mz_foo");
        assert_eq!(doc.help, "");
    }

    /// A `visibility: MetricVisibility::Public` annotation makes a metric
    /// `Public`; an explicit `Internal` stays `Internal`.
    #[mz_ore::test]
    fn parses_visibility_annotation() {
        let doc = parse_metric(
            r#"metric!(name: "mz_foo", help: "h", visibility: MetricVisibility::Public)"#,
        );
        assert_eq!(doc.visibility, MetricVisibility::Public);
        let doc = parse_metric(
            r#"metric!(name: "mz_foo", help: "h", visibility: MetricVisibility::Internal)"#,
        );
        assert_eq!(doc.visibility, MetricVisibility::Internal);
    }

    /// The `visibility:` annotation is recognized however the enum is qualified,
    /// since we key off the final path segment.
    #[mz_ore::test]
    fn parses_qualified_visibility_annotation() {
        let doc = parse_metric(
            r#"metric!(name: "mz_foo", help: "h", visibility: mz_ore::metrics::MetricVisibility::Public)"#,
        );
        assert_eq!(doc.visibility, MetricVisibility::Public);
    }

    /// A metric with no `visibility:` annotation defaults to `Internal`,
    /// matching the macro.
    #[mz_ore::test]
    fn visibility_defaults_to_internal() {
        let doc = parse_metric(r#"metric!(name: "mz_foo", help: "h")"#);
        assert_eq!(doc.visibility, MetricVisibility::Internal);
    }

    /// Parses `#[<attr>] fn f() {}` and returns its attributes, so `is_test_only`
    /// can be exercised against realistically parsed attributes.
    fn attrs_of(attr: &str) -> Vec<Attribute> {
        let item: ItemFn = syn::parse_str(&format!("{attr} fn f() {{}}")).expect("valid attribute");
        item.attrs
    }

    #[mz_ore::test]
    fn is_test_only_matches_test_items() {
        // `cfg(test)` and test-runner attributes (`test`, `mz_ore::test`,
        // `tokio::test`, …) mark an item as test-only.
        assert!(is_test_only(&attrs_of("#[cfg(test)]")));
        assert!(is_test_only(&attrs_of("#[test]"))); // allow(test-attribute)
        assert!(is_test_only(&attrs_of("#[mz_ore::test]")));
        assert!(is_test_only(&attrs_of("#[tokio::test]"))); // allow(test-attribute)
        assert!(is_test_only(&attrs_of("#[mz_ore::test(tokio::test)]")));
    }

    /// Attributes with no mention of `test` are not test-only, so metrics gated
    /// on a target, a feature, or carrying ordinary attributes stay documented.
    #[mz_ore::test]
    fn is_test_only_ignores_non_test_items() {
        assert!(!is_test_only(&attrs_of("#[cfg(unix)]")));
        assert!(!is_test_only(&attrs_of(r#"#[cfg(target_os = "linux")]"#)));
        assert!(!is_test_only(&attrs_of(r#"#[cfg(feature = "chrono")]"#)));
        assert!(!is_test_only(&attrs_of("#[inline]")));
        assert!(!is_test_only(&attrs_of("#[derive(Clone)]")));
    }

    /// Collects every metric name found by walking `src`.
    fn collect_names(src: &str) -> Vec<String> {
        let file: syn::File = syn::parse_str(src).expect("valid source");
        let mut out = Vec::new();
        Collector {
            source: "test.rs",
            out: &mut out,
        }
        .visit_file(&file);
        out.into_iter().map(|m| m.name).collect()
    }

    /// A `metric!` nested inside a `vec![..]` is still found: `syn` leaves the
    /// macro body as opaque tokens, so the collector re-parses and descends.
    #[mz_ore::test]
    fn collects_metric_nested_in_vec() {
        let names = collect_names(
            r#"
            fn f(name: &str) {
                let _ = vec![
                    make(metric!(name: "nested_in_vec", help: "h", const_labels: {"name" => name})),
                ];
            }
            "#,
        );
        assert!(
            names.contains(&"nested_in_vec".to_string()),
            "got {names:?}"
        );
    }

    /// We descend only into `vec!`, not arbitrary macros.
    #[mz_ore::test]
    fn ignores_metric_inside_macro_rules_definition() {
        let names = collect_names(
            r#"
            macro_rules! m {
                () => {
                    registry.register(metric!(name: concat!("a", "b"), help: "h"))
                };
            }
            "#,
        );
        assert!(
            names.is_empty(),
            "macro_rules! body must not be scraped, got {names:?}"
        );
    }
}
