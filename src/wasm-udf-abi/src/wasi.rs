// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The WASI preview1 surface the host provides.
//!
//! This is the complete `wasi_snapshot_preview1` function list. The runtime
//! links a deterministic implementation of every entry, and validation
//! rejects any other import, so a module that validates always instantiates.

/// The import module name for WASI preview1.
pub const MODULE: &str = "wasi_snapshot_preview1";

/// A core Wasm value type, as used in WASI preview1 signatures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValType {
    I32,
    I64,
}

use ValType::{I32, I64};

/// A WASI preview1 function: its name, parameter types, and result types.
pub type Function = (&'static str, &'static [ValType], &'static [ValType]);

/// Every WASI preview1 function, with its core signature.
pub const FUNCTIONS: &[Function] = &[
    ("args_get", &[I32, I32], &[I32]),
    ("args_sizes_get", &[I32, I32], &[I32]),
    ("environ_get", &[I32, I32], &[I32]),
    ("environ_sizes_get", &[I32, I32], &[I32]),
    ("clock_res_get", &[I32, I32], &[I32]),
    ("clock_time_get", &[I32, I64, I32], &[I32]),
    ("fd_advise", &[I32, I64, I64, I32], &[I32]),
    ("fd_allocate", &[I32, I64, I64], &[I32]),
    ("fd_close", &[I32], &[I32]),
    ("fd_datasync", &[I32], &[I32]),
    ("fd_fdstat_get", &[I32, I32], &[I32]),
    ("fd_fdstat_set_flags", &[I32, I32], &[I32]),
    ("fd_fdstat_set_rights", &[I32, I64, I64], &[I32]),
    ("fd_filestat_get", &[I32, I32], &[I32]),
    ("fd_filestat_set_size", &[I32, I64], &[I32]),
    ("fd_filestat_set_times", &[I32, I64, I64, I32], &[I32]),
    ("fd_pread", &[I32, I32, I32, I64, I32], &[I32]),
    ("fd_prestat_get", &[I32, I32], &[I32]),
    ("fd_prestat_dir_name", &[I32, I32, I32], &[I32]),
    ("fd_pwrite", &[I32, I32, I32, I64, I32], &[I32]),
    ("fd_read", &[I32, I32, I32, I32], &[I32]),
    ("fd_readdir", &[I32, I32, I32, I64, I32], &[I32]),
    ("fd_renumber", &[I32, I32], &[I32]),
    ("fd_seek", &[I32, I64, I32, I32], &[I32]),
    ("fd_sync", &[I32], &[I32]),
    ("fd_tell", &[I32, I32], &[I32]),
    ("fd_write", &[I32, I32, I32, I32], &[I32]),
    ("path_create_directory", &[I32, I32, I32], &[I32]),
    ("path_filestat_get", &[I32, I32, I32, I32, I32], &[I32]),
    (
        "path_filestat_set_times",
        &[I32, I32, I32, I32, I64, I64, I32],
        &[I32],
    ),
    ("path_link", &[I32, I32, I32, I32, I32, I32, I32], &[I32]),
    (
        "path_open",
        &[I32, I32, I32, I32, I32, I64, I64, I32, I32],
        &[I32],
    ),
    ("path_readlink", &[I32, I32, I32, I32, I32, I32], &[I32]),
    ("path_remove_directory", &[I32, I32, I32], &[I32]),
    ("path_rename", &[I32, I32, I32, I32, I32, I32], &[I32]),
    ("path_symlink", &[I32, I32, I32, I32, I32], &[I32]),
    ("path_unlink_file", &[I32, I32, I32], &[I32]),
    ("poll_oneoff", &[I32, I32, I32, I32], &[I32]),
    ("proc_exit", &[I32], &[]),
    ("proc_raise", &[I32], &[I32]),
    ("sched_yield", &[], &[I32]),
    ("random_get", &[I32, I32], &[I32]),
    ("sock_accept", &[I32, I32, I32], &[I32]),
    ("sock_recv", &[I32, I32, I32, I32, I32, I32], &[I32]),
    ("sock_send", &[I32, I32, I32, I32, I32], &[I32]),
    ("sock_shutdown", &[I32, I32], &[I32]),
];

/// Imports whose host implementation returns fixed values instead of what a
/// guest would expect from a real system. Validation reports these as
/// warnings, because code relying on them is likely a bug.
pub const DETERMINIZED: &[(&str, &str)] = &[
    (
        "random_get",
        "returns a fixed pseudorandom sequence, identical in every instance",
    ),
    ("clock_time_get", "always returns the Unix epoch"),
    ("poll_oneoff", "returns immediately without sleeping"),
];

/// Looks up a WASI preview1 function by name.
pub fn function(name: &str) -> Option<&'static Function> {
    FUNCTIONS.iter().find(|(n, _, _)| *n == name)
}
