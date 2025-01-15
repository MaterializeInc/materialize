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

use std::path::PathBuf;
use std::{env, fs};

use anyhow::{Context, Result};

const AST_DEFS_MOD: &str = "src/ast/defs.rs";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").context("Cannot read OUT_DIR env var")?);
    let ir = mz_walkabout::load(AST_DEFS_MOD)?;

    // Generate AST visitors.
    {
        let fold = mz_walkabout::gen_fold(&ir);
        let visit = mz_walkabout::gen_visit(&ir);
        let visit_mut = mz_walkabout::gen_visit_mut(&ir);
        fs::write(out_dir.join("fold.rs"), fold)?;
        fs::write(out_dir.join("visit.rs"), visit)?;
        fs::write(out_dir.join("visit_mut.rs"), visit_mut)?;
    }
    // Generate derived items for AST types modelling simple option names.
    {
        let parse = simple_option_names::gen_parse(&ir);
        let display = simple_option_names::gen_display(&ir);
        fs::write(out_dir.join("parse.simple_options.rs"), parse)?;
        fs::write(out_dir.join("display.simple_options.rs"), display)?;
    }

    Ok(())
}

mod simple_option_names {
    use mz_ore_build::codegen::CodegenBuf;
    use mz_walkabout::ir;

    // TODO: we might want to identify these enums using an attribute.
    const ENUMS: [&str; 2] = ["ExplainPlanOptionName", "ClusterFeatureName"];

    /// Generate `Parser` methods.
    pub fn gen_display(ir: &ir::Ir) -> String {
        let mut buf = CodegenBuf::new();
        for enum_name in ENUMS {
            if let Some(ir::Item::Enum(enum_item)) = ir.items.get(enum_name) {
                write_ast_display(enum_name, enum_item, &mut buf)
            }
        }
        buf.into_string()
    }

    fn write_ast_display(ident: &str, enum_item: &ir::Enum, buf: &mut CodegenBuf) {
        let typ = ident.to_string();
        let fn_fmt = "fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>)";
        buf.write_block(format!("impl AstDisplay for {typ}"), |buf| {
            buf.write_block(fn_fmt, |buf| {
                buf.write_block("match self", |buf| {
                    for v in enum_item.variants.iter().map(|v| &v.name) {
                        let ts = separate_tokens(v, ' ').to_uppercase();
                        buf.writeln(format!(r#"Self::{v} => f.write_str("{ts}"),"#));
                    }
                });
            });
        });
        buf.end_line();
    }

    /// Generate `AstDisplay` implementations.
    pub fn gen_parse(ir: &ir::Ir) -> String {
        let mut buf = CodegenBuf::new();
        buf.write_block("impl<'a> Parser<'a>", |buf| {
            for enum_name in ENUMS {
                if let Some(ir::Item::Enum(enum_item)) = ir.items.get(enum_name) {
                    write_fn_parse(enum_name, enum_item, buf)
                }
            }
        });
        buf.into_string()
    }

    fn write_fn_parse(ident: &str, enum_item: &ir::Enum, buf: &mut CodegenBuf) {
        let typ = ident.to_string();
        let msg = separate_tokens(&typ, ' ').to_lowercase();
        let fn_name = format!("parse_{}", separate_tokens(&typ, '_').to_lowercase());
        let fn_type = format!("Result<{typ}, ParserError>");
        buf.write_block(format!("fn {fn_name}(&mut self) -> {fn_type}"), |buf| {
            for v in enum_item.variants.iter().map(|v| &v.name) {
                let kws = separate_tokens(v, ',').to_uppercase();
                buf.write_block(format!("if self.parse_keywords(&[{kws}])"), |buf| {
                    buf.writeln(format!("return Ok({typ}::{v})"));
                });
            }
            buf.write_block("", |buf| {
                buf.writeln(format!(r#"let msg = "a valid {msg}".to_string();"#));
                buf.writeln(r#"Err(self.error(self.peek_pos(), msg))"#);
            });
        });
        buf.end_line();
    }

    fn separate_tokens(name: &str, sep: char) -> String {
        let mut buf = String::new();
        let mut prev = sep;
        for ch in name.chars() {
            if ch.is_uppercase() && prev != sep {
                buf.push(sep);
            }
            buf.push(ch);
            prev = ch;
        }
        buf
    }
}
