// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared JWT group-claim extraction used by both OIDC and Frontegg
//! authenticators. Encapsulates the dot-separated claim-path resolution and
//! the array-vs-string normalization so the two authenticators behave
//! identically.

use std::collections::{BTreeMap, BTreeSet};

use tracing::warn;

/// Extracts group names from a JWT's unknown-claims map.
///
/// `claim_path` may be a bare claim name (e.g. `"groups"`) or a
/// dot-separated path into nested JSON objects (e.g.
/// `"customClaims.groups"`). Keys that contain a literal `.` are not
/// reachable; this is a known limitation matching CockroachDB's
/// `group_claim` semantics. Empty path segments (leading/trailing/double
/// dots, or an empty path) yield `None` and emit a `warn!`-level log so
/// misconfiguration is visible.
///
/// Returns `None` if the claim is absent (skip sync, preserve current state),
/// `Some(vec![])` if the claim is present but empty (revoke all sync-granted
/// roles), or `Some(vec![...])` with deduplicated, sorted group names
/// (exact case preserved — matching against catalog role names is
/// case-sensitive).
///
/// Accepts arrays of strings, single strings, or mixed arrays (non-string
/// elements are filtered out). Other JSON types are treated as absent.
pub fn extract_groups(
    claims: &BTreeMap<String, serde_json::Value>,
    claim_path: &str,
) -> Option<Vec<String>> {
    let value = resolve_claim_path(claims, claim_path)?;

    let raw_groups: Vec<String> = match value {
        serde_json::Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        serde_json::Value::String(s) => {
            if s.is_empty() {
                vec![]
            } else {
                vec![s.clone()]
            }
        }
        _ => {
            warn!(
                claim_path,
                "JWT group claim has unexpected type; skipping group sync"
            );
            return None;
        }
    };

    let groups: Vec<String> = raw_groups
        .into_iter()
        .filter(|g| !g.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    Some(groups)
}

/// Walks a dot-separated claim path into nested JSON objects. Returns
/// `None` if the path is empty, any segment is empty, an intermediate
/// segment is missing, or an intermediate segment resolves to a
/// non-object value.
fn resolve_claim_path<'a>(
    claims: &'a BTreeMap<String, serde_json::Value>,
    claim_path: &str,
) -> Option<&'a serde_json::Value> {
    let mut segments = claim_path.split('.');
    let first = segments
        .next()
        .expect("str::split always yields at least one segment");
    if first.is_empty() {
        warn!(
            claim_path,
            "JWT group claim path has an empty segment; skipping group sync"
        );
        return None;
    }
    let mut current = claims.get(first)?;
    for segment in segments {
        if segment.is_empty() {
            warn!(
                claim_path,
                "JWT group claim path has an empty segment; skipping group sync"
            );
            return None;
        }
        let obj = match current {
            serde_json::Value::Object(map) => map,
            _ => {
                warn!(
                    claim_path,
                    segment,
                    "JWT group claim intermediate segment is not an object; skipping group sync"
                );
                return None;
            }
        };
        current = obj.get(segment)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> BTreeMap<String, serde_json::Value> {
        serde_json::from_str(json).unwrap()
    }

    #[mz_ore::test]
    fn test_groups_array() {
        let c = parse(r#"{"groups":["analytics","platform_eng"]}"#);
        assert_eq!(
            extract_groups(&c, "groups"),
            Some(vec!["analytics".to_string(), "platform_eng".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_single_string() {
        let c = parse(r#"{"groups":"analytics"}"#);
        assert_eq!(
            extract_groups(&c, "groups"),
            Some(vec!["analytics".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_missing() {
        let c = parse(r#"{"other":"x"}"#);
        assert_eq!(extract_groups(&c, "groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_empty_array() {
        let c = parse(r#"{"groups":[]}"#);
        assert_eq!(extract_groups(&c, "groups"), Some(vec![]));
    }

    #[mz_ore::test]
    fn test_groups_empty_string() {
        let c = parse(r#"{"groups":""}"#);
        assert_eq!(extract_groups(&c, "groups"), Some(vec![]));
    }

    #[mz_ore::test]
    fn test_groups_mixed_case_preserved() {
        let c = parse(r#"{"groups":["Analytics","PLATFORM_ENG","analytics"]}"#);
        assert_eq!(
            extract_groups(&c, "groups"),
            Some(vec![
                "Analytics".to_string(),
                "PLATFORM_ENG".to_string(),
                "analytics".to_string()
            ])
        );
    }

    #[mz_ore::test]
    fn test_groups_non_string_filtered() {
        let c = parse(r#"{"groups":["valid",123,true,"also_valid"]}"#);
        assert_eq!(
            extract_groups(&c, "groups"),
            Some(vec!["also_valid".to_string(), "valid".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_array_all_non_strings() {
        let c = parse(r#"{"groups":[1,2,true,null]}"#);
        assert_eq!(extract_groups(&c, "groups"), Some(vec![]));
    }

    #[mz_ore::test]
    fn test_groups_non_array_non_string() {
        let c = parse(r#"{"groups":42}"#);
        assert_eq!(extract_groups(&c, "groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_null() {
        let c = parse(r#"{"groups":null}"#);
        assert_eq!(extract_groups(&c, "groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_object_claim() {
        let c = parse(r#"{"groups":{"team":"eng"}}"#);
        assert_eq!(extract_groups(&c, "groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_dedup_sorted() {
        let c = parse(r#"{"groups":["zebra","alpha","alpha","beta"]}"#);
        assert_eq!(
            extract_groups(&c, "groups"),
            Some(vec![
                "alpha".to_string(),
                "beta".to_string(),
                "zebra".to_string()
            ])
        );
    }

    #[mz_ore::test]
    fn test_groups_nested_path_array() {
        let c = parse(r#"{"customClaims":{"groups":["analytics","platform_eng"]}}"#);
        assert_eq!(
            extract_groups(&c, "customClaims.groups"),
            Some(vec!["analytics".to_string(), "platform_eng".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_nested_path_deep() {
        let c = parse(r#"{"a":{"b":{"c":["eng"]}}}"#);
        assert_eq!(extract_groups(&c, "a.b.c"), Some(vec!["eng".to_string()]));
    }

    #[mz_ore::test]
    fn test_groups_nested_path_missing() {
        let c = parse(r#"{"customClaims":{}}"#);
        assert_eq!(extract_groups(&c, "customClaims.groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_nested_intermediate_not_object() {
        let c = parse(r#"{"customClaims":"not_an_object"}"#);
        assert_eq!(extract_groups(&c, "customClaims.groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_path_leading_dot() {
        let c = parse(r#"{"groups":["eng"]}"#);
        assert_eq!(extract_groups(&c, ".groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_path_trailing_dot() {
        let c = parse(r#"{"customClaims":{"groups":["eng"]}}"#);
        assert_eq!(extract_groups(&c, "customClaims.groups."), None);
    }

    #[mz_ore::test]
    fn test_groups_path_double_dot() {
        let c = parse(r#"{"customClaims":{"groups":["eng"]}}"#);
        assert_eq!(extract_groups(&c, "customClaims..groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_path_empty() {
        let c = parse(r#"{"groups":["eng"]}"#);
        assert_eq!(extract_groups(&c, ""), None);
    }

    #[mz_ore::test]
    fn test_groups_boolean_claim() {
        let c = parse(r#"{"groups":true}"#);
        assert_eq!(extract_groups(&c, "groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_float_claim() {
        let c = parse(r#"{"groups":3.14}"#);
        assert_eq!(extract_groups(&c, "groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_array_with_nested_arrays() {
        let c = parse(r#"{"groups":[["nested"],"valid"]}"#);
        assert_eq!(
            extract_groups(&c, "groups"),
            Some(vec!["valid".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_array_with_null_elements() {
        let c = parse(r#"{"groups":["eng",null,"ops",null]}"#);
        assert_eq!(
            extract_groups(&c, "groups"),
            Some(vec!["eng".to_string(), "ops".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_array_with_object_elements() {
        let c = parse(r#"{"groups":["eng",{"name":"ops"},"analytics"]}"#);
        assert_eq!(
            extract_groups(&c, "groups"),
            Some(vec!["analytics".to_string(), "eng".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_array_with_empty_strings() {
        let c = parse(r#"{"groups":["","eng",""]}"#);
        assert_eq!(extract_groups(&c, "groups"), Some(vec!["eng".to_string()]));
    }

    #[mz_ore::test]
    fn test_groups_whitespace_only_single_string() {
        let c = parse(r#"{"groups":"  "}"#);
        assert_eq!(extract_groups(&c, "groups"), Some(vec!["  ".to_string()]));
    }

    #[mz_ore::test]
    fn test_groups_whitespace_names() {
        let c = parse(r#"{"groups":["  spaces  ","eng"]}"#);
        assert_eq!(
            extract_groups(&c, "groups"),
            Some(vec!["  spaces  ".to_string(), "eng".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_unicode_names() {
        let c = parse(r#"{"groups":["Développeurs","INGÉNIEURS"]}"#);
        assert_eq!(
            extract_groups(&c, "groups"),
            Some(vec!["Développeurs".to_string(), "INGÉNIEURS".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_special_characters() {
        let c = parse(r#"{"groups":["team-platform.eng","org_data-science","role/admin"]}"#);
        assert_eq!(
            extract_groups(&c, "groups"),
            Some(vec![
                "org_data-science".to_string(),
                "role/admin".to_string(),
                "team-platform.eng".to_string(),
            ])
        );
    }

    #[mz_ore::test]
    fn test_groups_no_case_folding() {
        let c = parse(r#"{"groups":["Eng","eng","ENG","eNg"]}"#);
        assert_eq!(
            extract_groups(&c, "groups"),
            Some(vec![
                "ENG".to_string(),
                "Eng".to_string(),
                "eNg".to_string(),
                "eng".to_string(),
            ])
        );
    }

    #[mz_ore::test]
    fn test_groups_large_array() {
        let items: Vec<String> = (0..100).map(|i| format!("\"group_{}\"", i)).collect();
        let c = parse(&format!(r#"{{"groups":[{}]}}"#, items.join(",")));
        let result = extract_groups(&c, "groups").unwrap();
        assert_eq!(result.len(), 100);
        assert_eq!(result[0], "group_0");
        assert_eq!(result[99], "group_99");
    }

    #[mz_ore::test]
    fn test_groups_nested_path_single_string() {
        let c = parse(r#"{"customClaims":{"groups":"analytics"}}"#);
        assert_eq!(
            extract_groups(&c, "customClaims.groups"),
            Some(vec!["analytics".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_nested_path_terminal_not_array_or_string() {
        let c = parse(r#"{"customClaims":{"groups":42}}"#);
        assert_eq!(extract_groups(&c, "customClaims.groups"), None);
    }
}
