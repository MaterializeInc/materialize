// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Internal utility libraries for Materialize.
//!
//! **ore** (_n_): the raw material from which more valuable materials are extracted.
//! Modules are included in this crate when they are broadly useful but too
//! small to warrant their own crate.

#![deny(missing_docs, missing_debug_implementations)]

// This module presently only contains macros. Macros are always exported at the
// root of a crate, so this module is not public as it would appear empty.
mod assert;

pub mod antichain;
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
