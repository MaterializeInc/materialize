// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code generation.
//!
//! This module processes the IR to generate the `fold`, `visit`, and
//! `visit_mut` modules.

use std::collections::{BTreeMap, BTreeSet};

use fstrings::{f, format_args_f};
use itertools::Itertools;

use mz_ore::codegen::CodegenBuf;

use crate::ir::{Ir, Item, Type};

/// Generates a fold transformer for a mutable AST.
///
/// Returns a string of Rust code that should be compiled alongside the module
/// from which it was generated.
pub fn gen_fold(ir: &Ir) -> String {
    gen_fold_root(ir)
}

/// Generates a visitor for an immutable AST.
///
/// Returns a string of Rust code that should be compiled alongside the module
/// from which it was generated.
pub fn gen_visit(ir: &Ir) -> String {
    gen_visit_root(&VisitConfig { mutable: false }, ir)
}

/// Generates a visitor for a mutable AST.
///
/// Returns a string of Rust code that should be compiled alongside the module
/// from which it was generated.
pub fn gen_visit_mut(ir: &Ir) -> String {
    gen_visit_root(&VisitConfig { mutable: true }, ir)
}

pub fn gen_fold_root(ir: &Ir) -> String {
    let mut generics = BTreeMap::new();
    for (name, bounds) in &ir.generics {
        generics.insert(name.clone(), bounds.clone());
        generics.insert(f!("{name}2"), bounds.clone());
    }
    let trait_generics = trait_generics(&generics);
    let trait_generics_and_bounds = trait_generics_and_bounds(&generics);

    let mut buf = CodegenBuf::new();

    buf.start_block(f!("pub trait Fold<{trait_generics_and_bounds}>"));
    for (name, item) in &ir.items {
        match item {
            Item::Abstract => {
                // The intent is to replace `T::FooBar` with `T2::FooBar`. This
                // is a bit gross, but it seems reliable enough, and is so far
                // simpler than trying to use a structured type for `name`.
                let name2 = name.replacen("::", "2::", 1);
                let fn_name = fold_fn_name(name);
                buf.writeln(f!("fn {fn_name}(&mut self, node: {name}) -> {name2};"))
            }
            Item::Struct(_) | Item::Enum(_) => {
                let generics = item_generics(item, "");
                let generics2 = item_generics(item, "2");
                let fn_name = fold_fn_name(name);
                buf.start_block(f!(
                    "fn {fn_name}(&mut self, node: {name}{generics}) -> {name}{generics2}"
                ));
                buf.writeln(f!("{fn_name}(self, node)"));
                buf.end_block();
            }
        }
    }
    buf.end_block();

    for (name, item) in &ir.items {
        if let Item::Abstract = item {
            continue;
        }
        let generics = item_generics(item, "");
        let generics2 = item_generics(item, "2");
        let fn_name = fold_fn_name(name);
        buf.writeln(f!(
            "pub fn {fn_name}<F, {trait_generics_and_bounds}>(folder: &mut F, node: {name}{generics}) -> {name}{generics2}"
        ));
        buf.writeln(f!("where"));
        buf.writeln(f!("    F: Fold<{trait_generics}> + ?Sized,"));
        buf.start_block("");
        match item {
            Item::Struct(s) => {
                buf.start_block(name);
                for (i, f) in s.fields.iter().enumerate() {
                    let field_name = match &f.name {
                        Some(name) => name.clone(),
                        None => i.to_string(),
                    };
                    let binding = f!("node.{field_name}");
                    buf.start_line();
                    buf.write(f!("{field_name}: "));
                    gen_fold_element(&mut buf, &binding, &f.ty);
                    buf.write(",");
                    buf.end_line();
                }
                buf.end_block();
            }
            Item::Enum(e) => {
                buf.start_block("match node");
                for v in &e.variants {
                    buf.start_block(f!("{name}::{v.name}"));
                    for (i, f) in v.fields.iter().enumerate() {
                        let name = f.name.clone().unwrap_or_else(|| i.to_string());
                        let binding = format!("binding{}", i);
                        buf.writeln(f!("{}: {},", name, binding));
                    }
                    buf.restart_block("=>");
                    buf.start_block(f!("{name}::{v.name}"));
                    for (i, f) in v.fields.iter().enumerate() {
                        let field_name = match &f.name {
                            Some(name) => name.clone(),
                            None => i.to_string(),
                        };
                        let binding = format!("binding{}", i);
                        buf.start_line();
                        buf.write(f!("{field_name}: "));
                        gen_fold_element(&mut buf, &binding, &f.ty);
                        buf.write(",");
                        buf.end_line();
                    }
                    buf.end_block();
                    buf.end_block();
                }
                buf.end_block();
            }
            Item::Abstract => (),
        }
        buf.end_block();
    }

    buf.into_string()
}

fn gen_fold_element(buf: &mut CodegenBuf, binding: &str, ty: &Type) {
    match ty {
        Type::Primitive => buf.write(binding),
        Type::Abstract(ty) => {
            let fn_name = fold_fn_name(ty);
            buf.write(f!("folder.{fn_name}({binding})"));
        }
        Type::Option(ty) => {
            buf.write(f!("{binding}.map(|v| "));
            gen_fold_element(buf, "v", ty);
            buf.write(")")
        }
        Type::Vec(ty) => {
            buf.write(f!("{binding}.into_iter().map(|v| "));
            gen_fold_element(buf, "v", ty);
            buf.write(").collect()");
        }
        Type::Box(ty) => {
            buf.write("Box::new(");
            gen_fold_element(buf, &f!("*{binding}"), ty);
            buf.write(")");
        }
        Type::Local(s) => {
            let fn_name = fold_fn_name(s);
            buf.write(f!("folder.{fn_name}({binding})"));
        }
    }
}

struct VisitConfig {
    mutable: bool,
}

fn gen_visit_root(c: &VisitConfig, ir: &Ir) -> String {
    let trait_name = if c.mutable { "VisitMut" } else { "Visit" };
    let muta = if c.mutable { "mut " } else { "" };
    let trait_generics = trait_generics(&ir.generics);
    let trait_generics_and_bounds = trait_generics_and_bounds(&ir.generics);

    let mut buf = CodegenBuf::new();

    buf.start_block(f!(
        "pub trait {trait_name}<'ast, {trait_generics_and_bounds}>"
    ));
    for (name, item) in &ir.items {
        let generics = item_generics(item, "");
        let fn_name = visit_fn_name(c, name);
        buf.start_block(f!(
            "fn {fn_name}(&mut self, node: &'ast {muta}{name}{generics})"
        ));
        buf.writeln(f!("{fn_name}(self, node)"));
        buf.end_block();
    }
    buf.end_block();

    for (name, item) in &ir.items {
        let generics = item_generics(item, "");
        let fn_name = visit_fn_name(c, name);
        buf.writeln(f!(
            "pub fn {fn_name}<'ast, V, {trait_generics_and_bounds}>(visitor: &mut V, node: &'ast {muta}{name}{generics})"
        ));
        buf.writeln(f!("where"));
        buf.writeln(f!("    V: {trait_name}<'ast, {trait_generics}> + ?Sized,"));
        buf.start_block("");
        match item {
            Item::Struct(s) => {
                for (i, f) in s.fields.iter().enumerate() {
                    let binding = match &f.name {
                        Some(name) => f!("&{muta}node.{name}"),
                        None => f!("&{muta}node.{i}"),
                    };
                    gen_visit_element(c, &mut buf, &binding, &f.ty);
                }
            }
            Item::Enum(e) => {
                buf.start_block("match node");
                for v in &e.variants {
                    buf.start_block(f!("{name}::{v.name}"));
                    for (i, f) in v.fields.iter().enumerate() {
                        let name = f.name.clone().unwrap_or_else(|| i.to_string());
                        let binding = format!("binding{}", i);
                        buf.writeln(f!("{}: {},", name, binding));
                    }
                    buf.restart_block("=>");
                    for (i, f) in v.fields.iter().enumerate() {
                        let binding = format!("binding{}", i);
                        gen_visit_element(c, &mut buf, &binding, &f.ty);
                    }
                    buf.end_block();
                }
                buf.end_block();
            }
            Item::Abstract => (),
        }
        buf.end_block();
    }

    buf.into_string()
}

fn gen_visit_element(c: &VisitConfig, buf: &mut CodegenBuf, binding: &str, ty: &Type) {
    match ty {
        Type::Primitive => (),
        Type::Abstract(ty) => {
            let fn_name = visit_fn_name(c, ty);
            buf.writeln(f!("visitor.{fn_name}({binding});"));
        }
        Type::Option(ty) => {
            buf.start_block(f!("if let Some(v) = {binding}"));
            gen_visit_element(c, buf, "v", ty);
            buf.end_block();
        }
        Type::Vec(ty) => {
            buf.start_block(f!("for v in {binding}"));
            gen_visit_element(c, buf, "v", ty);
            buf.end_block();
        }
        Type::Box(ty) => {
            let binding = match c.mutable {
                true => format!("&mut *{}", binding),
                false => format!("&*{}", binding),
            };
            gen_visit_element(c, buf, &binding, ty);
        }
        Type::Local(s) => {
            let fn_name = visit_fn_name(c, s);
            buf.writeln(f!("visitor.{fn_name}({binding});"));
        }
    }
}

fn fold_fn_name(s: &str) -> String {
    let mut out = String::from("fold");
    write_fn_name(&mut out, s);
    out
}

fn visit_fn_name(c: &VisitConfig, s: &str) -> String {
    let mut out = String::from("visit");
    write_fn_name(&mut out, s);
    if c.mutable {
        out.push_str("_mut");
    }
    out
}

fn write_fn_name(out: &mut String, s: &str) {
    // Simplify associated type names so that e.g. `T::FooBar` becomes
    // `visit_foo_bar`.
    let s = s.splitn(2, "::").last().unwrap();
    for c in s.chars() {
        if c.is_ascii_uppercase() {
            out.push('_');
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
}

fn trait_generics(generics: &BTreeMap<String, BTreeSet<String>>) -> String {
    generics.keys().map(|id| format!("{}, ", id)).join("")
}

fn trait_generics_and_bounds(generics: &BTreeMap<String, BTreeSet<String>>) -> String {
    generics
        .iter()
        .map(|(ident, bounds)| {
            if bounds.len() == 0 {
                format!("{}, ", ident)
            } else {
                format!("{}: {}, ", ident, bounds.iter().join("+"))
            }
        })
        .join("")
}

fn item_generics(item: &Item, suffix: &str) -> String {
    if item.generics().is_empty() {
        "".into()
    } else {
        let generics = item
            .generics()
            .iter()
            .map(|g| f!("{g.name}{suffix}"))
            .join(", ");
        format!("<{}>", generics)
    }
}
