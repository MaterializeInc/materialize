// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use tokio::time::{self, Duration};

/// Retries a fallible operation `f` with suitable defaults for a network
/// service.
pub async fn retry<F, U, T>(mut f: F) -> Result<T, String>
where
    F: FnMut() -> U,
    U: Future<Output = Result<T, String>>,
{
    let mut i = 0;
    let mut backoff = Duration::from_millis(200);
    loop {
        match f().await {
            Ok(t) => return Ok(t),
            Err(e) if i > 5 => return Err(e),
            Err(_) => i += 1,
        }
        time::delay_for(backoff).await;
        backoff *= 2;
    }
}
