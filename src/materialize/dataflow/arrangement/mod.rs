// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Types related to the arrangement and management of collections.

pub mod context;
pub mod manager;

pub use context::Context;
pub use manager::{KeysOnlyHandle, KeysValsHandle, TraceManager};
