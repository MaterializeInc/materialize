// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utility functions to transform parts of a single `MirRelationExpr`
//! into canonical form.

use crate::MirScalarExpr;

/// Fuse equivalence classes of a join.
///
/// This function makes it so that the same expression appears in only one
/// equivalence class. It also sorts and dedups the equivalence classes. (See
/// [`sort_dedup_equivalences`].)
///
/// This function should be called whenever a transformation has added
/// equivalence classes to a join.
///
/// [`sort_dedup_equivalences`]: expr::canonicalize::sort_dedup_equivalences
/// ```rust
/// use expr::MirScalarExpr;
/// use expr::canonicalize::fuse_equivalences;
///
/// let mut equivalences_to_fuse = vec![
///     vec![MirScalarExpr::Column(3), MirScalarExpr::Column(5)],
///     vec![MirScalarExpr::Column(0), MirScalarExpr::Column(3)]
/// ];
/// let expected = vec![
///     vec![MirScalarExpr::Column(0),
///         MirScalarExpr::Column(3),
///         MirScalarExpr::Column(5)]
/// ];
/// fuse_equivalences(&mut equivalences_to_fuse);
/// assert_eq!(expected, equivalences_to_fuse)
/// ````
pub fn fuse_equivalences(equivalences: &mut Vec<Vec<MirScalarExpr>>) {
    for index in 1..equivalences.len() {
        for inner in 0..index {
            if equivalences[index]
                .iter()
                .any(|pair| equivalences[inner].contains(pair))
            {
                let to_extend = std::mem::replace(&mut equivalences[index], Vec::new());
                equivalences[inner].extend(to_extend);
            }
        }
    }
    sort_dedup_equivalences(equivalences)
}

/// Sorts equivalence classes of a join and deduplicates expressions within each
/// equivalence class.
///
/// The equivalence classes are put into a canonical order, and the expressions
/// within an equivalence class are also put into a canonical order. If an
/// equivalence class has less than two expressions after deduplication, the
/// equivalence class is removed.
///
/// Using this function is recommended even if what it does seems to be overkill
/// because it will make it easier to track about what form the equivalences
/// are guaranteed to be in. For example, it s
///
/// ```rust
/// use expr::MirScalarExpr;
/// use expr::canonicalize::sort_dedup_equivalences;
///
/// let mut equivalences_to_sort = vec![
///     vec![MirScalarExpr::Column(1), MirScalarExpr::Column(4)],
///     vec![MirScalarExpr::Column(3), MirScalarExpr::Column(5), MirScalarExpr::Column(0), MirScalarExpr::Column(3)],
///     vec![MirScalarExpr::Column(2), MirScalarExpr::Column(2)],
/// ];
/// let expected = vec![
///     vec![MirScalarExpr::Column(0), MirScalarExpr::Column(3), MirScalarExpr::Column(5)],
///     vec![MirScalarExpr::Column(1), MirScalarExpr::Column(4)],
/// ];
/// sort_dedup_equivalences(&mut equivalences_to_sort);
/// assert_eq!(expected, equivalences_to_sort)
/// ````
pub fn sort_dedup_equivalences(equivalences: &mut Vec<Vec<MirScalarExpr>>) {
    for equivalence in equivalences.iter_mut() {
        equivalence.sort();
        equivalence.dedup();
    }
    equivalences.retain(|es| es.len() > 1);
    equivalences.sort();
}
