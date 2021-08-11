// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::{TryFrom, TryInto};

use anyhow::bail;

/// Validates a PG typmod from user input and returns a vector of the supplied
/// values whose positions align with those of `val_ranges`.
///
/// # Arguments
/// - `T`: The desired type of the `typ_mod`.
/// - `name`: The name to of the type to return in error messages
/// - `typ_mod`: The user-supplied typmod
/// - `val_ranges`: Valid `typ_mod` values. Tuples are of of the form
///   `(<attribute name>, <lower bound, inclusive>, <upper bound, inclusive>)`.
pub fn extract_typ_mod<T>(
    name: &str,
    typ_mod: &[u64],
    val_ranges: &[(&str, T, T)],
) -> Result<Vec<T>, anyhow::Error>
where
    T: TryFrom<u64> + std::fmt::Display + Ord + PartialOrd,
{
    if typ_mod.len() > val_ranges.len() {
        bail!("invalid {} type modifier", name)
    }
    let err = |attr: &str, lower: &T, upper: &T, value: &u64| -> Result<(), anyhow::Error> {
        bail!(
            "{} for type {} must be within [{}-{}], have {}",
            attr,
            name,
            lower,
            upper,
            value,
        )
    };
    let mut r: Vec<T> = vec![];
    for (v, (attr, lower, upper)) in typ_mod.iter().zip(val_ranges.iter()) {
        match (*v).try_into() {
            Ok(c) => {
                if &c < lower || &c > upper {
                    err(attr, lower, upper, v)?
                }
                r.push(c)
            }
            Err(_) => err(attr, lower, upper, v)?,
        };
    }
    Ok(r)
}
