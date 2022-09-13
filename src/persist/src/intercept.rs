// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Test utilities for trapping and injecting responses in external storage.

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;

use crate::location::{Atomicity, Blob, BlobMetadata, ExternalError};

/// Post-op closure for [Blob::delete].
pub type PostDeleteFn = Arc<
    dyn Fn(&str, Result<Option<usize>, ExternalError>) -> Result<Option<usize>, ExternalError>
        + Send
        + Sync,
>;

#[derive(Default)]
struct InterceptCore {
    post_delete: Option<PostDeleteFn>,
}

impl Debug for InterceptCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let InterceptCore { post_delete } = self;
        f.debug_struct("InterceptCore")
            .field(
                "post_delete",
                &post_delete.as_ref().map(|x| format!("{:p}", x)),
            )
            .finish()
    }
}

/// A handle for controlling the behavior of an intercept delegate.
#[derive(Clone, Debug, Default)]
pub struct InterceptHandle {
    core: Arc<Mutex<InterceptCore>>,
}

impl InterceptHandle {
    /// Sets a new [PostDeleteFn].
    ///
    /// Returns the previous closure, if any.
    pub fn set_post_delete(&self, f: Option<PostDeleteFn>) -> Option<PostDeleteFn> {
        let mut core = self.core.lock().expect("lock should not be poisoned");
        std::mem::replace(&mut core.post_delete, f)
    }
}

/// An intercept delegate to [Blob].
///
/// TODO: Tune this pattern to be most useful and then extend it to consensus
/// and the rest of blob.
#[derive(Debug)]
pub struct InterceptBlob {
    handle: InterceptHandle,
    blob: Arc<dyn Blob + Send + Sync>,
}

impl InterceptBlob {
    /// Returns a new [InterceptBlob].
    pub fn new(blob: Arc<dyn Blob + Send + Sync>, handle: InterceptHandle) -> Self {
        InterceptBlob { handle, blob }
    }
}

#[async_trait]
impl Blob for InterceptBlob {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        self.blob.get(key).await
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        self.blob.list_keys_and_metadata(key_prefix, f).await
    }

    async fn set(&self, key: &str, value: Bytes, atomic: Atomicity) -> Result<(), ExternalError> {
        self.blob.set(key, value, atomic).await
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        let ret = self.blob.delete(key).await;
        let post_delete = self
            .handle
            .core
            .lock()
            .expect("lock should not be poisoned")
            .post_delete
            .clone();
        match post_delete {
            Some(x) => x(key, ret),
            None => ret,
        }
    }
}
