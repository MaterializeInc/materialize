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

    // Generate AST visitors.
    {
        let ir = mz_walkabout::load(AST_DEFS_MOD)?;
        let fold = mz_walkabout::gen_fold(&ir);
        let visit = mz_walkabout::gen_visit(&ir);
        let visit_mut = mz_walkabout::gen_visit_mut(&ir);
        fs::write(out_dir.join("fold.rs"), fold)?;
        fs::write(out_dir.join("visit.rs"), visit)?;
        fs::write(out_dir.join("visit_mut.rs"), visit_mut)?;
    }

    Ok(())
}
