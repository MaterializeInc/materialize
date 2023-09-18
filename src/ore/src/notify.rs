// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Utilities for notifying a task.

use std::sync::Arc;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

/// TODO
#[derive(Debug, Clone)]
pub struct NotifySender(Arc<Notify>);

impl NotifySender {
    /// TODO
    pub fn notify(&self) {
        self.0.notify_one()
    }
}

/// TODO
#[derive(Debug)]
pub struct NotifyReceiver {
    queue: Arc<Notify>,
    in_progress: Arc<Semaphore>,
}

impl NotifyReceiver {
    /// TODO
    pub async fn queued_and_ready(&self) -> NotifyPermit {
        let permit = Semaphore::acquire_owned(Arc::clone(&self.in_progress))
            .await
            .expect("semaphore should not be closed");
        self.queue.notified().await;

        NotifyPermit(permit)
    }
}
static_assertions::assert_not_impl_all!(NotifyReceiver: Clone);

/// TODO
#[derive(Debug)]
pub struct NotifyPermit(OwnedSemaphorePermit);

/// TODO
pub fn notifier() -> (NotifySender, NotifyReceiver) {
    let notify = Arc::new(Notify::new());

    let tx = NotifySender(Arc::clone(&notify));
    let rx = NotifyReceiver {
        queue: notify,
        in_progress: Arc::new(Semaphore::new(1)),
    };

    (tx, rx)
}
