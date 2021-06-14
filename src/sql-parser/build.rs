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

use std::env;
use std::fs;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};

use ore::ascii::UncasedStr;
use ore::codegen::CodegenBuf;

const AST_DEFS_MOD: &str = "src/ast/defs.rs";
const KEYWORDS_LIST: &str = "src/keywords.txt";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").context("Cannot read OUT_DIR env var")?);

    // Generate keywords list and lookup table.
    {
        let file = fs::read_to_string(KEYWORDS_LIST)?;

        let keywords: Vec<(String, String)> = file
            .lines()
            .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
            .map(|l| {
                let parts: Vec<_> = l.split_whitespace().collect();
                if parts.len() != 2 {
                    bail!("malformed keyword: {}", l);
                }
                Ok((parts[0].to_owned(), parts[1].to_owned()))
            })
            .collect::<Result<Vec<_>>>()?;

        // Enforce that the keywords file is kept sorted. This is purely
        // cosmetic, but it cuts down on diff noise and merge conflicts.
        if let Some([(_, a), (_, b)]) = keywords.windows(2).find(|w| w[0].1 > w[1].1) {
            bail!("keywords list is not sorted: {:?} precedes {:?}", a, b);
        }

        let mut buf = CodegenBuf::new();

        buf.writeln("#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]");
        buf.start_block("pub enum Keyword");
        for (_, kw) in &keywords {
            buf.writeln(format!("{},", kw));
        }
        buf.end_block();

        buf.start_block("impl Keyword");
        buf.start_block("pub fn as_str(&self) -> &'static str");
        buf.start_block("match self");
        for (_, kw) in &keywords {
            buf.writeln(format!("Keyword::{} => {:?},", kw, kw.to_uppercase()));
        }
        buf.end_block();
        buf.end_block();

        buf.start_block("pub fn reserve_type(&self) -> ReserveType");
        buf.start_block("match self");
        for (reserve_type, kw) in &keywords {
            buf.writeln(format!("Keyword::{} => ReserveType::{},", kw, reserve_type));
        }
        buf.end_block();
        buf.end_block();
        buf.end_block();

        for (_, kw) in &keywords {
            buf.writeln(format!(
                "pub const {}: Keyword = Keyword::{};",
                kw.to_uppercase(),
                kw
            ));
        }

        let mut phf = phf_codegen::Map::new();
        for (_, kw) in &keywords {
            phf.entry(UncasedStr::new(kw), &format!("Keyword::{}", kw));
        }
        buf.writeln(format!(
            "static KEYWORDS: phf::Map<&'static UncasedStr, Keyword> = {};",
            phf.build()
        ));

        fs::write(out_dir.join("keywords.rs"), buf.into_string())?;
    }

    // Generate AST visitors.
    {
        let ir = walkabout::load(AST_DEFS_MOD)?;
        let fold = walkabout::gen_fold(&ir);
        let visit = walkabout::gen_visit(&ir);
        let visit_mut = walkabout::gen_visit_mut(&ir);
        fs::write(out_dir.join("fold.rs"), fold)?;
        fs::write(out_dir.join("visit.rs"), visit)?;
        fs::write(out_dir.join("visit_mut.rs"), visit_mut)?;
    }

    Ok(())
}
