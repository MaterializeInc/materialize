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

//! lalala

use std::marker::Send;

/// lalala
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub enum Either2<T1, T2> {
    /// lalala
    One(T1),
    /// lalala
    Two(T2),
}

/// lalala
pub async fn select2_send<F1, F2>(
    f1: &mut F1,
    f2: &mut F2,
) -> Either2<<F1 as Selectable>::Item, <F2 as Selectable>::Item>
where
    F1: SelectableSend,
    F2: SelectableSend,
{
    tokio::select! {
        v1 = <F1 as SelectableSend>::recv(f1) => {
            Either2::One(v1)
        },
        v2 = <F2 as SelectableSend>::recv(f2) => {
            Either2::Two(v2)
        }
    }
}

/// lalala
pub async fn select2_send_biased<F1, F2>(
    f1: &mut F1,
    f2: &mut F2,
) -> Either2<<F1 as Selectable>::Item, <F2 as Selectable>::Item>
where
    F1: SelectableSend,
    F2: SelectableSend,
{
    tokio::select! {
        biased;
        v1 = <F1 as SelectableSend>::recv(f1) => {
            Either2::One(v1)
        },
        v2 = <F2 as SelectableSend>::recv(f2) => {
            Either2::Two(v2)
        }
    }
}

/// lalala
pub async fn select2<F1, F2>(
    f1: &mut F1,
    f2: &mut F2,
) -> Either2<<F1 as Selectable>::Item, <F2 as Selectable>::Item>
where
    F1: Selectable,
    F2: Selectable,
{
    tokio::select! {
        v1 = f1.recv() => {
            Either2::One(v1)
        },
        v2 = f2.recv() => {
            Either2::Two(v2)
        }
    }
}

/// lalala
#[async_trait::async_trait(?Send)]
pub trait Selectable: Unpin {
    /// lalala
    type Item;
    /// lalala
    async fn recv(&mut self) -> Self::Item;
}

/// lalala
#[async_trait::async_trait]
pub trait SelectableSend: Selectable {
    /// lalala
    async fn recv(&mut self) -> Self::Item;
}

/// lalala
#[derive(Debug)]
pub struct SelectableWrapper<S>(pub S);

#[async_trait::async_trait(?Send)]
impl<S: tokio_stream::StreamExt + Unpin> Selectable for SelectableWrapper<S> {
    type Item = Option<<S as futures::stream::Stream>::Item>;
    async fn recv(&mut self) -> Self::Item {
        self.0.next().await
    }
}
#[async_trait::async_trait]
impl<S: tokio_stream::StreamExt + Send + Unpin> SelectableSend for SelectableWrapper<S> {
    async fn recv(&mut self) -> Self::Item {
        self.0.next().await
    }
}

#[async_trait::async_trait(?Send)]
impl<T> Selectable for tokio::sync::watch::Receiver<T> {
    type Item = Option<()>;
    async fn recv(&mut self) -> Self::Item {
        self.changed().await.ok()
    }
}
#[async_trait::async_trait]
impl<T: Send + Sync> SelectableSend for tokio::sync::watch::Receiver<T> {
    async fn recv(&mut self) -> Self::Item {
        self.changed().await.ok()
    }
}

#[async_trait::async_trait(?Send)]
impl<T> Selectable for tokio::sync::mpsc::Receiver<T> {
    type Item = Option<T>;
    async fn recv(&mut self) -> Self::Item {
        self.recv().await
    }
}
#[async_trait::async_trait]
impl<T: Send + Sync> SelectableSend for tokio::sync::mpsc::Receiver<T> {
    async fn recv(&mut self) -> Self::Item {
        self.recv().await
    }
}
#[async_trait::async_trait(?Send)]
impl<T> Selectable for tokio::sync::mpsc::Sender<T> {
    type Item = ();
    async fn recv(&mut self) -> Self::Item {
        self.closed().await
    }
}
#[async_trait::async_trait]
impl<T: Send> SelectableSend for tokio::sync::mpsc::Sender<T> {
    async fn recv(&mut self) -> Self::Item {
        self.closed().await
    }
}

#[async_trait::async_trait(?Send)]
impl<T> Selectable for tokio::sync::mpsc::UnboundedReceiver<T> {
    type Item = Option<T>;
    async fn recv(&mut self) -> Self::Item {
        self.recv().await
    }
}
#[async_trait::async_trait]
impl<T: Send + Sync> SelectableSend for tokio::sync::mpsc::UnboundedReceiver<T> {
    async fn recv(&mut self) -> Self::Item {
        self.recv().await
    }
}
#[async_trait::async_trait(?Send)]
impl<T> Selectable for tokio::sync::mpsc::UnboundedSender<T> {
    type Item = ();
    async fn recv(&mut self) -> Self::Item {
        self.closed().await
    }
}
#[async_trait::async_trait]
impl<T: Send> SelectableSend for tokio::sync::mpsc::UnboundedSender<T> {
    async fn recv(&mut self) -> Self::Item {
        self.closed().await
    }
}

#[async_trait::async_trait(?Send)]
impl Selectable for tokio::time::Interval {
    type Item = tokio::time::Instant;
    async fn recv(&mut self) -> Self::Item {
        self.tick().await
    }
}
#[async_trait::async_trait]
impl SelectableSend for tokio::time::Interval {
    async fn recv(&mut self) -> Self::Item {
        self.tick().await
    }
}
