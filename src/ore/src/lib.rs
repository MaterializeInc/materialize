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

#![warn(missing_docs, missing_debug_implementations)]

#[cfg(feature = "test")]
pub mod assert;
pub mod cast;
pub mod cgroup;
#[cfg(feature = "cli")]
pub mod cli;
pub mod codegen;
pub mod collections;
pub mod display;
pub mod env;
pub mod fmt;
#[cfg(feature = "network")]
pub mod future;
pub mod hash;
pub mod hint;
pub mod id_gen;
pub mod iter;
pub mod lex;
#[cfg(feature = "metrics")]
pub mod metrics;
#[cfg(feature = "network")]
pub mod netio;
pub mod now;
pub mod option;
pub mod panic;
pub mod result;
#[cfg(feature = "network")]
pub mod retry;
pub mod stats;
pub mod str;
pub mod sync;
#[cfg(feature = "test")]
pub mod test;
pub mod thread;
pub mod vec;
