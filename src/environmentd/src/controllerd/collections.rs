// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Read-only inspection of catalog-known Persist collections and committed bounds.

use mz_catalog::catalog::Catalog;
use mz_expr::like_pattern::{self, Matcher};
use mz_repr::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Patterns {
    pub id: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug)]
pub(super) struct Filters {
    id: Option<Matcher>,
    name: Option<Matcher>,
}

impl Patterns {
    pub(super) fn compile(self) -> Result<Filters, mz_expr::EvalError> {
        Ok(Filters {
            id: self
                .id
                .map(|value| like_pattern::compile(&value, false))
                .transpose()?,
            name: self
                .name
                .map(|value| like_pattern::compile(&value, false))
                .transpose()?,
        })
    }
}

impl Filters {
    fn matches(&self, id: &str, name: Option<&str>) -> bool {
        self.id.as_ref().is_none_or(|pattern| pattern.is_match(id))
            && self
                .name
                .as_ref()
                .is_none_or(|pattern| name.is_some_and(|name| pattern.is_match(name)))
    }
}

#[derive(Serialize)]
struct Observation {
    id: String,
    name: Option<String>,
    since: Option<Vec<Timestamp>>,
}

/// Returns the committed collection frontier, not physical Persist since or a
/// newly computed compaction proposal. Missing bounds remain null.
pub(super) fn inspect(catalog: &Catalog, filters: &Filters) -> Value {
    let state = catalog.state();
    let rows: Vec<_> = state
        .storage_metadata()
        .collection_metadata
        .keys()
        .filter_map(|id| {
            let name = catalog
                .try_get_entry_by_global_id(id)
                .map(|entry| state.resolve_full_name(entry.name(), None).to_string());
            filters
                .matches(&id.to_string(), name.as_deref())
                .then(|| Observation {
                    id: id.to_string(),
                    name,
                    since: state
                        .collection_compaction_bounds()
                        .get(id)
                        .map(|since| since.elements().to_vec()),
                })
        })
        .collect();
    serde_json::to_value(rows).expect("collection observations serialize")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn unavailable_and_empty_frontiers_are_distinct() {
        for (since, expected) in [
            (None, serde_json::json!(null)),
            (Some(vec![]), serde_json::json!([])),
        ] {
            let row = Observation {
                id: "u1".into(),
                name: None,
                since,
            };
            assert_eq!(
                serde_json::to_value(row).unwrap(),
                serde_json::json!({
                    "id": "u1", "name": null, "since": expected
                })
            );
        }
    }

    #[mz_ore::test]
    fn filters_use_sql_like_and_preserve_unnamed_inventory() {
        let all = Patterns::default().compile().unwrap();
        assert!(all.matches("s1", None));
        let filters = Patterns {
            id: Some("s_".into()),
            name: Some(r"%mz\_catalog.%".into()),
        }
        .compile()
        .unwrap();
        assert!(filters.matches("s1", Some("mz_catalog.mz_tables")));
        assert!(!filters.matches("s10", Some("mz_catalog.mz_tables")));
        assert!(!filters.matches("S1", Some("mz_catalog.mz_tables")));
        assert!(!filters.matches("s1", Some("mzx catalog.mz_tables")));
        assert!(!filters.matches("s1", None));
        let escaped = Patterns {
            id: None,
            name: Some(r"a\%b\_c\\d".into()),
        }
        .compile()
        .unwrap();
        assert!(escaped.matches("u1", Some(r"a%b_c\d")));
        assert!(!escaped.matches("u1", Some(r"axxb_c\d")));
        assert!(
            Patterns {
                id: Some("bad\\".into()),
                name: None
            }
            .compile()
            .is_err()
        );
    }
}
