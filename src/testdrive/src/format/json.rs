// Copyright 2018 Flavien Raynaud
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use regex::Regex;

pub fn validate_sink<I>(
    has_key: bool,
    expected: I,
    actual: &[(Option<serde_json::Value>, Option<serde_json::Value>)],
    regex: &Option<Regex>,
    regex_replacement: &String,
) -> Result<(), String>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let expected: Vec<(Option<serde_json::Value>, Option<serde_json::Value>)> = expected
        .into_iter()
        .map(|v| {
            let mut deserializer = serde_json::Deserializer::from_str(v.as_ref()).into_iter();
            let key = if has_key {
                match deserializer.next() {
                    None => None,
                    Some(r) => r.map_err(|e| format!("parsing JSON: {}", e))?,
                }
            } else {
                None
            };
            let value = match deserializer.next() {
                None => None,
                Some(r) => r.map_err(|e| format!("parsing JSON: {}", e))?,
            };
            Ok((key, value))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut expected = expected.iter();
    let mut actual = actual.iter();
    let mut index = 0..;
    loop {
        let i = index.next().expect("known to exist");
        match (expected.next(), actual.next()) {
            (Some(e), Some(a)) => {
                let e_str = format!("{:#?}", e);
                let a_str = match &regex {
                    Some(regex) => regex
                        .replace_all(&format!("{:#?}", a).to_string(), regex_replacement.as_str())
                        .to_string(),
                    _ => format!("{:#?}", a),
                };

                if e_str != a_str {
                    return Err(format!(
                        "record {} did not match\nexpected:\n{}\n\nactual:\n{}",
                        i, e_str, a_str
                    ));
                }
            }
            (Some(e), None) => return Err(format!("missing record {}: {:#?}", i, e)),
            (None, Some(a)) => return Err(format!("extra record {}: {:#?}", i, a)),
            (None, None) => break,
        }
    }
    let expected: Vec<_> = expected.map(|e| format!("{:#?}", e)).collect();
    let actual: Vec<_> = actual.map(|a| format!("{:#?}", a)).collect();
    if !expected.is_empty() {
        Err(format!("missing records:\n{}", expected.join("\n")))
    } else if !actual.is_empty() {
        Err(format!("extra records:\n{}", actual.join("\n")))
    } else {
        Ok(())
    }
}
