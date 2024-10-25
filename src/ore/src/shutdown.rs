// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Convenience types to communicate shutdowns.

use std::rc::Weak;

/// Convenient wrapper around an optional `Weak` instance that can be used to check whether a
/// dataflow is shutting down.
///
/// Instances created through the `Default` impl act as if the dataflow never shuts down.
/// Instances created through [`ShutdownToken::new`] defer to the wrapped token.
#[derive(Debug, Clone, Default)]
pub struct ShutdownToken(Option<Weak<()>>);

impl ShutdownToken {
    /// Construct a `ShutdownToken` instance that defers to `token`.
    pub fn new(token: Weak<()>) -> Self {
        Self(Some(token))
    }

    /// Probe the token for dataflow shutdown.
    ///
    /// This method is meant to be used with the `?` operator: It returns `None` if the dataflow is
    /// in the process of shutting down and `Some` otherwise.
    pub fn probe(&self) -> Option<()> {
        match &self.0 {
            Some(t) => t.upgrade().map(|_| ()),
            None => Some(()),
        }
    }

    /// Returns whether the dataflow is in the process of shutting down.
    pub fn in_shutdown(&self) -> bool {
        self.probe().is_none()
    }

    /// Returns a reference to the wrapped `Weak`.
    pub fn get_inner(&self) -> Option<&Weak<()>> {
        self.0.as_ref()
    }
}
