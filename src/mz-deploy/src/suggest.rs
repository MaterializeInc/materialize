// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Nearest-name suggestions for names that did not resolve.
//!
//! Shared by the LSP's quick fixes and the deployment validation errors so a
//! misspelling reads the same however the user hit it.

/// Maximum number of suggestions returned by [`did_you_mean`].
pub(crate) const MAX_DID_YOU_MEAN: usize = 3;

/// Return up to [`MAX_DID_YOU_MEAN`] closest names from `candidates` to
/// `needle`, sorted by Damerau-Levenshtein distance ascending. Names whose
/// distance exceeds `max(2, needle.len() / 3)` are filtered out so unrelated
/// matches don't surface as suggestions.
///
/// Allocations only happen for surviving candidates, so passing a borrowed
/// slice (e.g. `pool.iter()`) is cheap even when the pool has thousands of
/// names.
pub(crate) fn did_you_mean<I, S>(needle: &str, candidates: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let threshold = std::cmp::max(2, needle.len() / 3);
    let mut scored: Vec<(usize, String)> = candidates
        .into_iter()
        .filter_map(|c| {
            let s = c.as_ref();
            let d = strsim::damerau_levenshtein(needle, s);
            (d <= threshold).then(|| (d, s.to_string()))
        })
        .collect();
    scored.sort_by_key(|(d, _)| *d);
    scored.truncate(MAX_DID_YOU_MEAN);
    scored.into_iter().map(|(_, s)| s).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn did_you_mean_returns_empty_for_no_close_match() {
        let candidates = ["customer_name", "customer_id", "shipping_address"];
        let out = did_you_mean("xyz", candidates.iter().map(|s| s.to_string()));
        assert!(out.is_empty(), "expected no matches, got {:?}", out);
    }

    #[mz_ore::test]
    fn did_you_mean_returns_exact_match_first() {
        let candidates = ["customer_name", "customer_id"];
        let out = did_you_mean("customer_name", candidates.iter().map(|s| s.to_string()));
        // "customer_name" (distance 0) and "customer_id" (distance 4) are both within
        // threshold max(2, 13/3) = 4, so both are returned, sorted by distance.
        assert_eq!(
            out,
            vec!["customer_name".to_string(), "customer_id".to_string()]
        );
    }

    #[mz_ore::test]
    fn did_you_mean_handles_transposition() {
        // Damerau-Levenshtein treats one transposition as distance 1.
        let candidates = ["customer_name"];
        let out = did_you_mean("cusotmer_name", candidates.iter().map(|s| s.to_string()));
        assert_eq!(out, vec!["customer_name".to_string()]);
    }

    #[mz_ore::test]
    fn did_you_mean_respects_max_three_limit() {
        // Provide many candidates that are all close enough to hit the limit.
        // "custoser_name" (distance 1 to): customer_name, custumer_name, cust_name, etc.
        let candidates = [
            "customer_name",   // distance 1
            "custumer_name",   // distance 1 (typo: transposition)
            "custoser_name_x", // distance 2 (one extra char)
            "customers",       // distance 6 (exceeds threshold, excluded)
            "x_custoser_name", // distance 2 (one extra char prefix)
        ];
        let out = did_you_mean("custoser_name", candidates.iter().map(|s| s.to_string()));
        // Should return at most 3, even though multiple candidates match.
        assert!(out.len() <= 3, "should cap at 3, got {:?}", out);
        // Best match (distance 1) comes first.
        assert_eq!(out[0], "customer_name");
    }

    #[mz_ore::test]
    fn did_you_mean_skips_empty_candidates() {
        let candidates: Vec<String> = Vec::new();
        let out = did_you_mean("anything", candidates);
        assert!(out.is_empty());
    }
}
