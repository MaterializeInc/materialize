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

//! The Python types of the [`Model`], `TypedDict`s with an `int` for each
//! decimal string, and a `from_json` function for each type that holds one.
//!
//! The output is what black prints, so formatting the repository leaves it
//! unchanged.

use crate::{HEADER, Kind, Model, Ty, assert_fits, doc_text, prefixed, variant_name, words};

pub(crate) fn emit(model: &Model) -> String {
    let mut out = format!(
        "{}\n\n\"\"\"The values the protocol core hands to bindings, and the functions that\nturn their decimal strings into ``int``.\"\"\"\n\nfrom __future__ import annotations\n\nfrom typing import Any, Literal, TypedDict, cast\n",
        prefixed(HEADER, "# ")
    );
    // The types, which the package root re-exports, and not the converters.
    let mut names: Vec<String> = Vec::new();
    for def in &model.defs {
        names.push(def.name.clone());
        if let Kind::Union { variants, .. } = &def.kind {
            names.extend(variants.iter().map(|v| variant_name(&def.name, &v.tag)));
        }
    }
    names.sort();
    out += "\n__all__ = [\n";
    for name in names {
        out += &format!("    \"{name}\",\n");
    }
    out += "]\n";
    for def in &model.defs {
        match &def.kind {
            Kind::Enum(values) => {
                out += "\n";
                out += &comment(def.doc.as_deref(), "");
                out += &format!("{} = Literal[\n", def.name);
                for value in values {
                    out += &comment(value.doc.as_deref(), "    ");
                    out += &format!("    \"{}\",\n", value.value);
                }
                out += "]\n";
            }
            Kind::Struct(fields) => out += &class(&def.name, def.doc.as_deref(), None, fields),
            Kind::Union { tag, variants } => {
                let mut names = Vec::new();
                for variant in variants {
                    let name = variant_name(&def.name, &variant.tag);
                    out += &class(
                        &name,
                        variant.doc.as_deref(),
                        Some((tag, &variant.tag)),
                        &variant.fields,
                    );
                    names.push(name);
                }
                out += "\n\n";
                out += &comment(def.doc.as_deref(), "");
                out += &alias(&def.name, &names);
            }
        }
    }
    for def in model
        .defs
        .iter()
        .filter(|def| model.def_has_integers(&def.name))
    {
        out += &format!(
            "\n\ndef {}(value: Any) -> {}:\n",
            function(&def.name),
            def.name
        );
        match &def.kind {
            Kind::Enum(_) => unreachable!("an enum holds no integers"),
            Kind::Struct(fields) => out += &returned(model, &def.name, fields, "    "),
            Kind::Union { tag, variants } => {
                out += &format!("    tag = value[\"{tag}\"]\n");
                for variant in variants {
                    out += &format!("    if tag == \"{}\":\n", variant.tag);
                    out += &returned(
                        model,
                        &variant_name(&def.name, &variant.tag),
                        &variant.fields,
                        "        ",
                    );
                }
                out += &format!("    raise ValueError(f\"unknown {tag}: {{tag!r}}\")\n");
            }
        }
    }
    assert_fits(&out, 88, |line, in_docstring| {
        in_docstring || line.starts_with('#')
    });
    out
}

fn class(
    name: &str,
    doc: Option<&str>,
    tag: Option<(&str, &str)>,
    fields: &[crate::Field],
) -> String {
    let mut out = format!("\n\nclass {name}(TypedDict):\n");
    if let Some(doc) = doc {
        out += &docstring(doc);
        out += "\n";
    }
    if let Some((tag, value)) = tag {
        out += &format!("    {tag}: Literal[\"{value}\"]\n");
    }
    for field in fields {
        out += &comment(field.doc.as_deref(), "    ");
        out += &format!("    {}: {}\n", field.name, ty(&field.ty));
    }
    out
}

/// `Name = A | B`, split the way black splits a line over 88 characters.
fn alias(name: &str, names: &[String]) -> String {
    let line = format!("{name} = {}", names.join(" | "));
    if line.len() <= 88 {
        return line + "\n";
    }
    let inner = names.join(" | ");
    if inner.len() + 4 <= 88 {
        return format!("{name} = (\n    {inner}\n)\n");
    }
    format!("{name} = (\n    {}\n)\n", names.join("\n    | "))
}

fn ty(ty: &Ty) -> String {
    match ty {
        Ty::String => "str".into(),
        Ty::Bool => "bool".into(),
        Ty::Integer(_) => "int".into(),
        Ty::Nullable(inner) => format!("{} | None", self::ty(inner)),
        Ty::Array(inner) => format!("list[{}]", self::ty(inner)),
        Ty::Ref(name) => name.clone(),
    }
}

fn returned(model: &Model, name: &str, fields: &[crate::Field], indent: &str) -> String {
    let converted: Vec<String> = fields
        .iter()
        .filter_map(|f| {
            convert(model, &format!("value[\"{}\"]", f.name), &f.ty)
                .map(|c| format!("\"{}\": {c}", f.name))
        })
        .collect();
    if converted.is_empty() {
        return format!("{indent}return cast(\"{name}\", value)\n");
    }
    let mut out = format!(
        "{indent}return cast(\n{indent}    \"{name}\",\n{indent}    {{\n{indent}        **value,\n"
    );
    for c in converted {
        out += &format!("{indent}        {c},\n");
    }
    out + &format!("{indent}    }},\n{indent})\n")
}

fn convert(model: &Model, expr: &str, ty: &Ty) -> Option<String> {
    match ty {
        Ty::String | Ty::Bool => None,
        Ty::Integer(_) => Some(format!("int({expr})")),
        Ty::Nullable(inner) => {
            convert(model, expr, inner).map(|c| format!("None if {expr} is None else {c}"))
        }
        Ty::Array(inner) => convert(model, "x", inner).map(|c| format!("[{c} for x in {expr}]")),
        Ty::Ref(name) => model
            .def_has_integers(name)
            .then(|| format!("{}({expr})", function(name))),
    }
}

/// `event_from_json` for `Event`.
fn function(name: &str) -> String {
    format!("{}_from_json", words(name).join("_"))
}

fn comment(doc: Option<&str>, indent: &str) -> String {
    match doc {
        Some(doc) => prefixed(&doc_text(doc), &format!("{indent}# ")) + "\n",
        None => String::new(),
    }
}

fn docstring(doc: &str) -> String {
    let text = doc_text(doc);
    assert!(
        !text.contains("\"\"\"") && !text.contains('\\') && !text.ends_with('"'),
        "a docstring needs escaping"
    );
    if !text.contains('\n') {
        return format!("    \"\"\"{text}\"\"\"\n");
    }
    format!(
        "    \"\"\"{}\n    \"\"\"\n",
        prefixed(&text, "    ").trim_start()
    )
}
