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

// Many things here only compile in the wasm32 target. We don't need to annotate it that way because
// this should only ever be built by wasm-build which specifies that.

use lol_alloc::{FreeListAllocator, LockedAllocator};
use mz_sql_lexer::lexer::{self, PosToken};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(typescript_custom_section)]
const KEYWORDS_TS_DEF: &str = r#"export function getKeywords(): string[];"#;

#[global_allocator]
static ALLOCATOR: LockedAllocator<FreeListAllocator> =
    LockedAllocator::new(FreeListAllocator::new());

#[wasm_bindgen(typescript_custom_section)]
const LEX_TS_DEF: &str = r#"export function lex(query: string): PosToken[];"#;

#[wasm_bindgen(js_name = PosToken, getter_with_clone, inspectable)]
#[derive(Debug)]
pub struct JsToken {
    pub kind: String,
    pub offset: usize,
}

impl From<PosToken> for JsToken {
    fn from(value: PosToken) -> Self {
        JsToken {
            kind: value.kind.to_string(),
            offset: value.offset,
        }
    }
}

/// Lexes a SQL query.
///
/// Returns a list of tokens alongside their corresponding byte offset in the
/// input string. Returns an error if the SQL query is lexically invalid.
///
/// See the module documentation for more information about the lexical
/// structure of SQL.
#[wasm_bindgen(skip_typescript)]
pub fn lex(query: &str) -> Result<Vec<JsValue>, JsError> {
    let lexed = lexer::lex(query).map_err(|e| JsError::new(&e.message))?;
    Ok(lexed
        .into_iter()
        .map(|token| JsValue::from(JsToken::from(token)))
        .collect())
}

#[wasm_bindgen(js_name = getKeywords, skip_typescript)]
// #[wasm_bindgen] cannot be applied directly to KEYWORDS, only to functions, structs, enums,
// impls, or extern blocks. Wrap this in a function.
pub fn get_keywords() -> Vec<JsValue> {
    mz_sql_lexer::keywords::KEYWORDS
        .keys()
        .map(|k| JsValue::from(k.to_string()))
        .collect()
}
