// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Activates the best default global memory allocator for the platform.
//!
//! A `#[global_allocator]` takes effect only in binaries that load the crate
//! defining it, so this crate re-exports `mz-alloc`. Binaries and benches that
//! want the platform's default allocator reference this crate, for example
//! with `use mz_alloc_default as _;`.

pub use mz_alloc;
