// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! We keep examples here as "tests" instead of in the examples/ directory to avoid extra linker
//! steps. To run an example, use something like:
//!
//! ```bash
//! $ cargo test -p mz-persist-client -- persistent_source_example --nocapture --ignored
//! ```

#[cfg(test)]
pub mod persistent_source_example;
