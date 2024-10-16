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

//! Channel utilities and extensions.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Future, FutureExt};
use prometheus::core::Atomic;
use tokio::sync::mpsc::{error, unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

use crate::metrics::PromLabelsExt;

pub mod trigger;

/// A trait describing a metric that can be used with an `instrumented_unbounded_channel`.
pub trait InstrumentedChannelMetric {
    /// Bump the metric, increasing the count of operators (send or receives) that occurred.
    fn bump(&self);
}

impl<'a, P, L> InstrumentedChannelMetric for crate::metrics::DeleteOnDropCounter<'a, P, L>
where
    P: Atomic,
    L: PromLabelsExt<'a>,
{
    fn bump(&self) {
        self.inc()
    }
}

/// A wrapper around tokio's mpsc unbounded channels that connects
/// metrics that are incremented when sends or receives happen.
pub fn instrumented_unbounded_channel<T, M>(
    sender_metric: M,
    receiver_metric: M,
) -> (
    InstrumentedUnboundedSender<T, M>,
    InstrumentedUnboundedReceiver<T, M>,
)
where
    M: InstrumentedChannelMetric,
{
    let (tx, rx) = unbounded_channel();

    (
        InstrumentedUnboundedSender {
            tx,
            metric: sender_metric,
        },
        InstrumentedUnboundedReceiver {
            rx,
            metric: receiver_metric,
        },
    )
}

/// A wrapper around tokio's `UnboundedSender` that increments a metric when a send occurs.
///
/// The metric is not dropped until this sender is dropped.
#[derive(Debug, Clone)]
pub struct InstrumentedUnboundedSender<T, M> {
    tx: UnboundedSender<T>,
    metric: M,
}

impl<T, M> InstrumentedUnboundedSender<T, M>
where
    M: InstrumentedChannelMetric,
{
    /// The same as `UnboundedSender::send`.
    pub fn send(&self, message: T) -> Result<(), error::SendError<T>> {
        let res = self.tx.send(message);
        self.metric.bump();
        res
    }
}

/// A wrapper around tokio's `UnboundedReceiver` that increments a metric when a recv _finishes_.
///
/// The metric is not dropped until this receiver is dropped.
#[derive(Debug)]
pub struct InstrumentedUnboundedReceiver<T, M> {
    rx: UnboundedReceiver<T>,
    metric: M,
}

impl<T, M> InstrumentedUnboundedReceiver<T, M>
where
    M: InstrumentedChannelMetric,
{
    /// The same as `UnboundedSender::recv`.
    pub async fn recv(&mut self) -> Option<T> {
        let res = self.rx.recv().await;
        self.metric.bump();
        res
    }

    /// The same as `UnboundedSender::try_recv`.
    pub fn try_recv(&mut self) -> Result<T, error::TryRecvError> {
        let res = self.rx.try_recv();

        if res.is_ok() {
            self.metric.bump();
        }
        res
    }
}

/// Extensions for oneshot channel types.
pub trait OneshotReceiverExt<T> {
    /// If the receiver is dropped without the value being observed, the provided closure will be
    /// called with the value that was left in the channel.
    ///
    /// This is useful in cases where you want to cleanup resources if the receiver of this value
    /// has gone away. If the sender and receiver are running on separate threads, it's possible
    /// for the sender to succeed, and for the receiver to be concurrently dropped, never realizing
    /// that it received a value.
    fn with_guard<F>(self, guard: F) -> GuardedReceiver<F, T>
    where
        F: FnMut(T);
}

impl<T> OneshotReceiverExt<T> for oneshot::Receiver<T> {
    fn with_guard<F>(self, guard: F) -> GuardedReceiver<F, T>
    where
        F: FnMut(T),
    {
        GuardedReceiver { guard, inner: self }
    }
}

/// A wrapper around [`oneshot::Receiver`] that will call the provided closure if there is a value
/// in the receiver when it's dropped.
#[derive(Debug)]
pub struct GuardedReceiver<F: FnMut(T), T> {
    guard: F,
    inner: oneshot::Receiver<T>,
}

// Note(parkmycar): If this Unpin requirement becomes too restrictive, we can refactor
// GuardedReceiver to use `pin_project`.
impl<F: FnMut(T) + Unpin, T> Future for GuardedReceiver<F, T> {
    type Output = Result<T, oneshot::error::RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll_unpin(cx)
    }
}

impl<F: FnMut(T), T> Drop for GuardedReceiver<F, T> {
    fn drop(&mut self) {
        // Close the channel so the sender is guaranteed to fail.
        self.inner.close();

        // If there was some value waiting in the channel call the guard with the value.
        if let Ok(x) = self.inner.try_recv() {
            (self.guard)(x)
        }
    }
}
