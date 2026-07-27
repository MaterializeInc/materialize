// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)]

pub use mz_pgrepr_consts::regproc::*;

#[cfg(test)]
mod tests {
    use super::*;

    /// [`name`] binary searches [`NAMES`], so the table must be sorted by OID
    /// and free of duplicate OIDs.
    #[mz_ore::test]
    fn names_is_sorted_by_oid() {
        for window in NAMES.windows(2) {
            assert!(
                window[0].0 < window[1].0,
                "NAMES is not sorted by OID: {} precedes {}",
                window[0].0,
                window[1].0,
            );
        }
    }

    #[mz_ore::test]
    // `oid` scans the table, so covering every entry is quadratic in its 626
    // rows. That is milliseconds natively and far too slow interpreted.
    #[cfg_attr(miri, ignore)]
    fn lookups_round_trip() {
        for (entry_oid, entry_name) in NAMES {
            assert_eq!(name(*entry_oid), Some(*entry_name));
            // Overloads sharing a rendering can only resolve back to one OID,
            // so ambiguity is the expected outcome for those.
            match oid(entry_name) {
                Ok(resolved) => assert_eq!(resolved, *entry_oid),
                Err(err) => assert_eq!(err, NameLookupError::Ambiguous),
            }
        }
        assert_eq!(name(0), None);
        assert_eq!(oid("no_such_function"), Err(NameLookupError::NotFound));
    }

    /// The renderings that matter to clients are the `pg_type` columns declared
    /// `regproc`, which is what motivates resolving names at all. Spot-check a
    /// few so a regeneration that dropped them fails loudly.
    #[mz_ore::test]
    fn type_io_functions_resolve() {
        for (func_oid, expected) in [
            (1242, "boolin"),
            (2436, "boolrecv"),
            (2400, "array_recv"),
            (2414, "textrecv"),
        ] {
            assert_eq!(name(func_oid), Some(expected), "regproc {func_oid}");
        }
    }
}
