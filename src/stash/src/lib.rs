// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Facilities for durably storing metadata.

mod persist;
mod sqlite;
mod stash;

pub use crate::persist::PersistStash;
pub use crate::sqlite::SqliteStash;
pub use crate::stash::{Stash, StashError, StashOp};
