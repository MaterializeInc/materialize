// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use anyhow::bail;

use super::util;

pub fn extract_typ_mod(typ_mod: &[u64]) -> Result<Option<usize>, anyhow::Error> {
    let typ_mod =
        util::extract_typ_mod::<usize>("character varying", typ_mod, &[("length", 1, 10_485_760)])?;
    Ok(typ_mod.get(0).cloned())
}

pub fn format_str(
    s: &str,
    length: Option<usize>,
    fail_on_len: bool,
) -> Result<String, anyhow::Error> {
    Ok(match length {
        // Note that length is 1-indexed, so finding `None` means the string's
        // characters don't exceed the length, while finding `Some` means it
        // does.
        Some(length) => match s.char_indices().nth(length) {
            None => s.to_string(),
            Some((idx, _)) => {
                if !fail_on_len || s[idx..].chars().all(|c| c.is_ascii_whitespace()) {
                    s[..idx].into()
                } else {
                    bail!("{} exceeds maximum length of {}", s, length)
                }
            }
        },
        None => s.to_string(),
    })
}
