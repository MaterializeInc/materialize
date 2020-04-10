// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

mod create_stream;
mod ingest;
mod verify;

pub use create_stream::build_create_stream;
pub use ingest::build_ingest;
pub use verify::build_verify;

const DEFAULT_KINESIS_TIMEOUT: Duration = Duration::from_millis(12700);
