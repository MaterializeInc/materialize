// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

/// A type for all dataflow-types results
pub type Result<T> = std::result::Result<T, failure::Error>;
