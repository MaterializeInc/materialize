// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

//! Process environment utilities.

use std::env;
use std::ffi::OsStr;

/// Reports whether the environment variable `key` is set to a truthy value in
/// the current process's environment.
///
/// The empty string and the string "0" are considered false. All other values
/// are considered true.
pub fn is_var_truthy<K>(key: K) -> bool
where
    K: AsRef<OsStr>,
{
    match env::var_os(key) {
        None => false,
        Some(val) => val != "0" && val != "",
    }
}
