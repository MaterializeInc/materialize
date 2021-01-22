// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code generation.
//!
//! This module processes the IR to generate the `visit` and `visit_mut`
//! modules.

use fstrings::{f, format_args_f};
use itertools::Itertools;

use ore::codegen::CodegenBuf;

use crate::ir::{Ir, Item, Type};

/// Generates a visitor for an immutable AST.
///
/// Returns a string of Rust code that should be compiled alongside the module
/// from which it was generated.
pub fn gen_visit(ir: &Ir) -> String {
    gen_root(&Config { mutable: false }, ir)
}

/// Generates a visitor for a mutable AST.
///
/// Returns a string of Rust code that should be compiled alongside the module
/// from which it was generated.
pub fn gen_visit_mut(ir: &Ir) -> String {
    gen_root(&Config { mutable: true }, ir)
}

struct Config {
    mutable: bool,
}

fn generics_string_for_item(item: &Item) -> String {
    if item.generics().is_empty() {
        "".into()
    } else {
        format!("<{}>", item.generics().iter().map(|g| &g.name).join(", "))
    }
}

fn gen_root(c: &Config, ir: &Ir) -> String {
    let trait_name = if c.mutable { "VisitMut" } else { "Visit" };
    let muta = if c.mutable { "mut " } else { "" };

    let all_generics_with_bounds_str = ir
        .generics
        .iter()
        .map(|(ident, bounds)| {
            if bounds.len() == 0 {
                format!(", {}", ident.to_string())
            } else {
                let b = bounds
                    .iter()
                    .map(|b| b.to_string())
                    .collect::<Vec<String>>()
                    .join("+");
                format!(", {}: {}", ident, b)
            }
        })
        .collect::<Vec<String>>()
        .join("");

    let all_generics_no_bounds_str = ir
        .generics
        .keys()
        .map(|ident| format!(", {}", ident.to_string()))
        .collect::<Vec<String>>()
        .join("");

    let mut buf = CodegenBuf::new();

    buf.start_block(f!(
        "pub trait {trait_name}<'ast{all_generics_with_bounds_str}>"
    ));
    for (name, item) in &ir.items {
        let generics = generics_string_for_item(item);
        let fn_name = visit_fn_name(c, name);
        buf.start_block(f!(
            "fn {fn_name}(&mut self, node: &'ast {muta}{name}{generics})"
        ));
        buf.writeln(f!("{fn_name}(self, node)"));
        buf.end_block();
    }
    buf.end_block();

    for (name, item) in &ir.items {
        let generics = generics_string_for_item(item);
        let fn_name = visit_fn_name(c, name);
        buf.writeln(f!(
            "pub fn {fn_name}<'ast, V{all_generics_with_bounds_str}>(visitor: &mut V, node: &'ast {muta}{name}{generics})"
        ));
        buf.writeln(f!("where"));
        buf.writeln(f!(
            "    V: {trait_name}<'ast{all_generics_no_bounds_str}> + ?Sized,"
        ));
        buf.start_block("");
        match item {
            Item::Struct(s) => {
                for (i, f) in s.fields.iter().enumerate() {
                    let binding = match &f.name {
                        Some(name) => f!("&{muta}node.{name}"),
                        None => f!("&{muta}node.{i}"),
                    };
                    gen_element(c, &mut buf, &binding, &f.ty);
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
                        gen_element(c, &mut buf, &binding, &f.ty);
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

fn gen_element(c: &Config, buf: &mut CodegenBuf, binding: &str, ty: &Type) {
    match ty {
        Type::Primitive => (),
        Type::Abstract(ty) => {
            let fn_name = visit_fn_name(c, ty);
            buf.writeln(f!("visitor.{fn_name}({binding});"));
        }
        Type::Option(ty) => {
            buf.start_block(f!("if let Some(v) = {binding}"));
            gen_element(c, buf, "v", ty);
            buf.end_block();
        }
        Type::Vec(ty) => {
            buf.start_block(f!("for v in {binding}"));
            gen_element(c, buf, "v", ty);
            buf.end_block();
        }
        Type::Box(ty) => {
            let binding = match c.mutable {
                true => format!("&mut *{}", binding),
                false => format!("&*{}", binding),
            };
            gen_element(c, buf, &binding, ty);
        }
        Type::Local(s) => {
            let fn_name = visit_fn_name(c, s);
            buf.writeln(f!("visitor.{fn_name}({binding});"));
        }
    }
}

fn visit_fn_name(c: &Config, s: &str) -> String {
    let mut out = String::from("visit");
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
    if c.mutable {
        out.push_str("_mut");
    }
    out
}
