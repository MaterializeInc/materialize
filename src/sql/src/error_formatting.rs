// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains functions that format errors into meaningful
//! messages for users of materialize's sql. They are intended to be
//! used in `Display` impl's in structured error-types in this crate.

use std::error::Error;
use std::fmt::Display;

/// Format a `tokio_postgres::Error` into a message that is meaningful
/// to a user.
// TODO(guswynn): optimize some allocations here
pub(crate) fn format_postgres_error(e: &tokio_postgres::Error) -> impl Display {
    // unfortunately we need a triple-nested if here...
    if let Some(cause) = e.source() {
        if let Some(io_err) = cause.downcast_ref::<std::io::Error>() {
            // TODO(guswynn): add timeout value, and link to more docs
            if io_err.kind() == std::io::ErrorKind::TimedOut {
                return format!(
                    "error communicating with postgres \
                    instance: {}. Timeouts can sometimes indicate \
                    a firewall. Does materialize have access to \
                    the postgres instance?",
                    e
                );
            }
        }
    }

    format!("error communicating with postgres instance: {}", e)
}
