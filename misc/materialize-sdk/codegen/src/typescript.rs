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

//! The TypeScript types of the [`Model`], with a `bigint` for each decimal
//! string, and in a module of their own, so that `export type *` of the types
//! exports no function, a `fromJson` function for each type that holds one.

use crate::{
    HEADER, Kind, Model, Ty, assert_fits, doc_text, pascal, prefixed, variant_name, words,
};

pub(crate) fn types(model: &Model) -> String {
    let mut out = format!("{}\n\n", prefixed(HEADER, "// "));
    for def in &model.defs {
        out += &jsdoc(def.doc.as_deref(), "");
        match &def.kind {
            Kind::Enum(values) => {
                out += &format!("export type {} =\n", def.name);
                for value in values {
                    out += &jsdoc(value.doc.as_deref(), "  ");
                    out += &format!("  | \"{}\"\n", value.value);
                }
                out.pop();
                out += ";\n\n";
            }
            Kind::Struct(fields) => {
                out += &interface(&def.name, None, fields);
            }
            Kind::Union { tag, variants } => {
                let names: Vec<String> = variants
                    .iter()
                    .map(|v| variant_name(&def.name, &v.tag))
                    .collect();
                out += &union(&def.name, &names);
                for variant in variants {
                    out += &jsdoc(variant.doc.as_deref(), "");
                    let name = variant_name(&def.name, &variant.tag);
                    out += &interface(&name, Some((tag, &variant.tag)), &variant.fields);
                }
            }
        }
    }
    finish(out)
}

/// The module of converters, which imports the types from `./values.js`.
pub(crate) fn converters(model: &Model) -> String {
    let converted: Vec<&crate::Def> = model
        .defs
        .iter()
        .filter(|def| model.def_has_integers(&def.name))
        .collect();
    let names: Vec<&str> = converted.iter().map(|def| def.name.as_str()).collect();
    let mut out = format!(
        "{}\n\nimport type {{ {} }} from \"./values.js\";\n\n",
        prefixed(HEADER, "// "),
        names.join(", ")
    );
    for def in converted {
        out += &format!(
            "export function {}(json: any): {} {{\n",
            function(&def.name),
            def.name
        );
        match &def.kind {
            Kind::Enum(_) => unreachable!("an enum holds no integers"),
            Kind::Struct(fields) => out += &returned(model, fields, "  "),
            Kind::Union { tag, variants } => {
                out += &format!("  switch (json.{tag}) {{\n");
                for variant in variants {
                    out += &format!("    case \"{}\":\n", variant.tag);
                    out += &returned(model, &variant.fields, "      ");
                }
                out += &format!(
                    "    default:\n      throw new Error(`unknown {tag}: ${{json.{tag}}}`);\n  }}\n"
                );
            }
        }
        out += "}\n\n";
    }
    finish(out)
}

fn finish(mut out: String) -> String {
    out.pop();
    assert_fits(&out, 80, |line, _| {
        line.starts_with("//") || line.starts_with('*') || line.starts_with("/**")
    });
    out
}

/// `export type Name = A | B;`, split past 80 characters the way prettier
/// splits it.
fn union(name: &str, names: &[String]) -> String {
    let line = format!("export type {name} = {};\n\n", names.join(" | "));
    if line.trim_end().len() <= 80 {
        return line;
    }
    let inner = names.join(" | ");
    if inner.len() + 3 <= 80 {
        return format!("export type {name} =\n  {inner};\n\n");
    }
    format!("export type {name} =\n  | {};\n\n", names.join("\n  | "))
}

fn interface(name: &str, tag: Option<(&str, &str)>, fields: &[crate::Field]) -> String {
    let mut out = format!("export interface {name} {{\n");
    if let Some((tag, value)) = tag {
        out += &format!("  {tag}: \"{value}\";\n");
    }
    for field in fields {
        out += &jsdoc(field.doc.as_deref(), "  ");
        out += &format!("  {}: {};\n", field.name, ty(&field.ty));
    }
    out + "}\n\n"
}

fn ty(ty: &Ty) -> String {
    match ty {
        Ty::String => "string".into(),
        Ty::Bool => "boolean".into(),
        Ty::Integer(_) => "bigint".into(),
        Ty::Nullable(inner) => format!("{} | null", self::ty(inner)),
        Ty::Array(inner) => match **inner {
            Ty::Nullable(_) => format!("({})[]", self::ty(inner)),
            _ => format!("{}[]", self::ty(inner)),
        },
        Ty::Ref(name) => name.clone(),
    }
}

/// The `return` of a converter for an object with `fields`.
fn returned(model: &Model, fields: &[crate::Field], indent: &str) -> String {
    let converted: Vec<String> = fields
        .iter()
        .filter_map(|f| {
            convert(model, &format!("json.{}", f.name), &f.ty).map(|c| format!("{}: {c}", f.name))
        })
        .collect();
    if converted.is_empty() {
        return format!("{indent}return json;\n");
    }
    let mut out = format!("{indent}return {{\n{indent}  ...json,\n");
    for c in converted {
        out += &format!("{indent}  {c},\n");
    }
    out + &format!("{indent}}};\n")
}

/// The expression that converts `expr` of type `ty`, if it holds integers.
fn convert(model: &Model, expr: &str, ty: &Ty) -> Option<String> {
    match ty {
        Ty::String | Ty::Bool => None,
        Ty::Integer(_) => Some(format!("BigInt({expr})")),
        Ty::Nullable(inner) => {
            convert(model, expr, inner).map(|c| format!("{expr} === null ? null : {c}"))
        }
        Ty::Array(inner) => match &**inner {
            Ty::Ref(name) if model.def_has_integers(name) => {
                Some(format!("{expr}.map({})", function(name)))
            }
            inner => convert(model, "x", inner).map(|c| format!("{expr}.map((x: any) => {c})")),
        },
        Ty::Ref(name) => model
            .def_has_integers(name)
            .then(|| format!("{}({expr})", function(name))),
    }
}

/// `eventFromJson` for `Event`.
fn function(name: &str) -> String {
    let words = words(name);
    let (first, rest) = words.split_first().expect("a name");
    format!("{first}{}FromJson", pascal(&rest.join("_")))
}

fn jsdoc(doc: Option<&str>, indent: &str) -> String {
    let Some(doc) = doc else { return String::new() };
    let text = doc_text(doc);
    assert!(!text.contains("*/"), "a doc comment holds */");
    if !text.contains('\n') {
        return format!("{indent}/** {text} */\n");
    }
    format!(
        "{indent}/**\n{}\n{indent} */\n",
        prefixed(&text, &format!("{indent} * "))
    )
}
