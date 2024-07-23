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

//! Trigger channels.
//!
//! Trigger channels are a very simple form of channel that communicate only one
//! bit of information: whether the sending half of the channel is open or
//! closed.
//!
//! Here's an example of using a trigger channel to trigger work on a background
//! task.
//!
//! ```
//! # tokio_test::block_on(async {
//! use mz_ore::channel::trigger;
//!
//! // Create a new trigger channel.
//! let (trigger, trigger_rx) = trigger::channel();
//!
//! // Spawn a task to do some background work, but only once triggered.
//! tokio::spawn(async {
//!     // Wait for trigger to get dropped.
//!     trigger_rx.await;
//!
//!     // Do background work.
//! });
//!
//! // Do some prep work.
//!
//! // Fire `trigger` by dropping it.
//! drop(trigger);
//! # })
//! ```
//!
//! A trigger channel never errors. It is not an error to drop the receiver
//! before dropping the corresponding trigger.
//!
//! Trigger channels can be easily simulated with [`tokio::sync::oneshot`]
//! channels (and in fact the implementation uses oneshot channels under the
//! hood). However, using trigger channels can result in clearer code when the
//! additional features of oneshot channels are not required, as trigger
//! channels have a simpler API.

use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures::FutureExt;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;

/// The sending half of a trigger channel.
///
/// Dropping the trigger will cause the receiver to resolve.
#[derive(Debug)]
pub struct Trigger {
    _tx: oneshot::Sender<()>,
}

impl Trigger {
    /// Fire this [Trigger].
    ///
    /// NOTE: Dropping the trigger also fires it, but this method allows
    /// call-sites to be more explicit.
    pub fn fire(self) {
        // Dropping the Trigger is what fires the oneshot.
    }
}

/// The receiving half of a trigger channel.
///
/// Awaiting the receiver will block until the trigger is dropped.
#[derive(Debug)]
pub struct Receiver {
    rx: oneshot::Receiver<()>,
}

impl Receiver {
    /// Reports whether the channel has been triggered.
    pub fn is_triggered(&mut self) -> bool {
        matches!(self.rx.try_recv(), Err(TryRecvError::Closed))
    }
}

impl Future for Receiver {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let _ = ready!(self.rx.poll_unpin(cx));
        Poll::Ready(())
    }
}

/// Creates a new trigger channel.
pub fn channel() -> (Trigger, Receiver) {
    let (tx, rx) = oneshot::channel();
    let trigger = Trigger { _tx: tx };
    let trigger_rx = Receiver { rx };
    (trigger, trigger_rx)
}

#[cfg(test)]
mod tests {
    use crate::channel::trigger;

    #[cfg_attr(miri, ignore)] // error: unsupported operation: returning ready events from epoll_wait is not yet implemented
    #[mz_ore::test(tokio::test)]
    async fn test_trigger_channel() {
        let (trigger1, mut trigger1_rx) = trigger::channel();
        let (trigger2, trigger2_rx) = trigger::channel();

        crate::task::spawn(|| "test_trigger_channel", async move {
            assert!(!trigger1_rx.is_triggered());
            (&mut trigger1_rx).await;
            assert!(trigger1_rx.is_triggered());
            drop(trigger2);
        });

        drop(trigger1);
        trigger2_rx.await;
    }
}
