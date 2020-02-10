// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rand::Rng;

/// Manages secret tokens for connections.
///
/// Pgwire specifies that every connection have a 32-bit secret associated with
/// it, that is known to both the client and the server. Cancellation requests
/// are required to authenticate with the secret of the connection that they
/// are targeting.
#[derive(Clone)]
pub struct SecretManager(Arc<RwLock<HashMap<u32, u32>>>);

impl SecretManager {
    /// Constructs a new `SecretManager`.
    pub fn new() -> SecretManager {
        SecretManager(Arc::new(RwLock::new(HashMap::new())))
    }

    /// Generates a secret for the given the connection with ID `conn_id`.
    pub fn generate(&self, conn_id: u32) {
        let mut inner = self.0.write().expect("lock poisoned");
        inner.insert(conn_id, rand::thread_rng().gen());
    }

    /// Releases resources for the connection with ID `conn_id`.
    pub fn free(&self, conn_id: u32) {
        let mut inner = self.0.write().expect("lock poisoned");
        inner.remove(&conn_id);
    }

    /// Returns the secret for the connection with ID `conn_id`, if it exists.
    pub fn get(&self, conn_id: u32) -> Option<u32> {
        let inner = self.0.read().expect("lock poisoned");
        inner.get(&conn_id).copied()
    }

    /// Reports whether the secret for the connection with ID `conn_id` is
    /// `secret`. This function does not distinguish between whether the secret
    /// did not match or whether `conn_id` was not present at all; `false` is
    /// returned in both cases.
    pub fn verify(&self, conn_id: u32, secret: u32) -> bool {
        if let Some(s) = self.get(conn_id) {
            s == secret
        } else {
            false
        }
    }
}
