// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

//! Internal utility libraries for Materialize.
//!
//! **ore** (_n_): the raw material from which more valuable materials are extracted.
//! Modules are included in this crate when they are broadly useful but too
//! small to warrant their own crate.

#![deny(missing_docs, missing_debug_implementations)]

// This module presently only contains macros. Macros are always exported at the
// root of a crate, so this module is not public as it would appear empty.
mod assert;

pub mod ascii;
pub mod cast;
pub mod cli;
pub mod codegen;
pub mod collections;
pub mod env;
pub mod fmt;
pub mod future;
pub mod hash;
pub mod hint;
pub mod iter;
pub mod lex;
pub mod netio;
pub mod option;
pub mod panic;
pub mod result;
pub mod retry;
pub mod stats;
pub mod str;
pub mod sync;
pub mod test;
pub mod thread;
pub mod vec;
