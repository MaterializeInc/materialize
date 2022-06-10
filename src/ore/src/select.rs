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

//! A `select!` macro that only accepts cancellation safe futures

use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};

/// `safe_select!` accepts the exact same syntax as [tokio::select!] but asserts that all futures
/// passed are cancellation safe
#[macro_export]
macro_rules! safe_select {
    // All input is normalized, now transform.
    (@ {
        // Whether we're in biased mode
        ($($biased:ident)?)

        // Normalized select branches
        $($bind:pat = $fut:expr, if $c:expr => $handle:expr, )+

        // Fallback expression used when all select branches have been disabled.
        ; $else:expr

    }) => {{
        tokio::select! {
            $($biased;)?
            $($bind = $crate::select::assert_cancel_safe($fut), if $c => {$handle}),+
            else => $else
        }
    }};

    // ==== Normalize =====

    // These rules match a single `safe_select!` branch and normalize it for processing by the first rule.

    (@ { $biased:tt $($t:tt)* } ) => {
        // No `else` branch
        $crate::safe_select!(@{ $biased $($t)*; panic!("all branches are disabled and there is no else branch") })
    };
    (@ { $biased:tt $($t:tt)* } else => $else:expr $(,)?) => {
        $crate::safe_select!(@{ $biased $($t)*; $else })
    };
    (@ { $biased:tt $($t:tt)* } $p:pat = $f:expr, if $c:expr => $h:block, $($r:tt)* ) => {
        $crate::safe_select!(@{ $biased $($t)* $p = $f, if $c => $h, } $($r)*)
    };
    (@ { $biased:tt $($t:tt)* } $p:pat = $f:expr => $h:block, $($r:tt)* ) => {
        $crate::safe_select!(@{ $biased $($t)* $p = $f, if true => $h, } $($r)*)
    };
    (@ { $biased:tt $($t:tt)* } $p:pat = $f:expr, if $c:expr => $h:block $($r:tt)* ) => {
        $crate::safe_select!(@{ $biased $($t)* $p = $f, if $c => $h, } $($r)*)
    };
    (@ { $biased:tt $($t:tt)* } $p:pat = $f:expr => $h:block $($r:tt)* ) => {
        $crate::safe_select!(@{ $biased $($t)* $p = $f, if true => $h, } $($r)*)
    };
    (@ { $biased:tt $($t:tt)* } $p:pat = $f:expr, if $c:expr => $h:expr ) => {
        $crate::safe_select!(@{ $biased $($t)* $p = $f, if $c => $h, })
    };
    (@ { $biased:tt $($t:tt)* } $p:pat = $f:expr => $h:expr ) => {
        $crate::safe_select!(@{ $biased $($t)* $p = $f, if true => $h, })
    };
    (@ { $biased:tt $($t:tt)* } $p:pat = $f:expr, if $c:expr => $h:expr, $($r:tt)* ) => {
        $crate::safe_select!(@{ $biased $($t)* $p = $f, if $c => $h, } $($r)*)
    };
    (@ { $biased:tt $($t:tt)* } $p:pat = $f:expr => $h:expr, $($r:tt)* ) => {
        $crate::safe_select!(@{ $biased $($t)* $p = $f, if true => $h, } $($r)*)
    };

    // ===== Entry points =====

    (biased; $p:pat = $($t:tt)* ) => { $crate::safe_select!(@{ (biased) } $p = $($t)*) };
    ( $p:pat = $($t:tt)* ) => { $crate::safe_select!(@{ () } $p = $($t)*) };
    () => { compile_error!("safe_select! requires at least one branch.") };
}

/// Marker trait that asserts a future is safe to cancel
///
/// # Safety
///
/// The the caller must ensure that the future is cancel safe
pub unsafe trait CancellationSafe: Future {}

/// No-op function to statically assert a future is cancellation safe
pub fn assert_cancel_safe<F: CancellationSafe>(f: F) -> F {
    f
}

/// A type that wraps a future to mark it as cancellation safe. This is useful in situations where
/// implementing the [CancellationSafe] trait is impossible (for example if it's an `impl Future`
/// return type of a function
#[derive(Debug)]
pub struct CancelSafe<F>(F);

impl<F: Future> CancelSafe<F> {
    /// Construct a CancelSafe wrapper with the given future.
    ///
    /// # Safety
    ///
    /// The the caller must ensure that the future is cancel safe
    pub unsafe fn new_unchecked(fut: F) -> Self {
        CancelSafe(fut)
    }
}

unsafe impl<F: Future> CancellationSafe for CancelSafe<F> {}

impl<F: Future> Future for CancelSafe<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: This is okay because `.0` is pinned when `self` is.
        unsafe { self.map_unchecked_mut(|s| &mut s.0).poll(cx) }
    }
}

/// A wrapper type that transparently derefs to the inner value. It overrides specific methods that
/// are known to be cancel safe but return un-nameable types by defining a function with the same
/// name and wrapping the return `impl Future` type in a `CancelSafe` wrapper.
#[derive(Debug)]
pub struct Selectable<T>(pub T);

impl<T> Deref for Selectable<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Selectable<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> Selectable<tokio::sync::mpsc::Receiver<T>> {
    /// Wrapper of [tokio::sync::mpsc::Receiver::recv] that wraps the returned future in CancelSafe
    pub fn recv(&mut self) -> CancelSafe<impl Future<Output = Option<T>> + '_> {
        // Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
        unsafe { CancelSafe::new_unchecked(self.0.recv()) }
    }
}

impl<T> Selectable<tokio::sync::mpsc::UnboundedReceiver<T>> {
    /// Wrapper of [tokio::sync::mpsc::UnboundedReceiver::recv] that wraps the returned future in CancelSafe
    pub fn recv(&mut self) -> CancelSafe<impl Future<Output = Option<T>> + '_> {
        // Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
        unsafe { CancelSafe::new_unchecked(self.0.recv()) }
    }
}

impl<T> Selectable<tokio::sync::mpsc::Sender<T>> {
    /// Wrapper of [tokio::sync::mpsc::Sender::closed] that wraps the returned future in CancelSafe
    pub fn closed(&self) -> CancelSafe<impl Future<Output = ()> + '_> {
        // Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
        unsafe { CancelSafe::new_unchecked(self.0.closed()) }
    }
}

impl<T> Selectable<tokio::sync::mpsc::UnboundedSender<T>> {
    /// Wrapper of [tokio::sync::mpsc::UnboundedSender::closed] that wraps the returned future in CancelSafe
    pub fn closed(&self) -> CancelSafe<impl Future<Output = ()> + '_> {
        // Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
        unsafe { CancelSafe::new_unchecked(self.0.closed()) }
    }
}

impl<T: Clone> Selectable<tokio::sync::broadcast::Receiver<T>> {
    /// Wrapper of [tokio::sync::broadcast::Receiver::recv] that wraps the returned future in CancelSafe
    pub fn recv(
        &mut self,
    ) -> CancelSafe<impl Future<Output = Result<T, tokio::sync::broadcast::error::RecvError>> + '_>
    {
        // Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
        unsafe { CancelSafe::new_unchecked(self.0.recv()) }
    }
}

impl<T> Selectable<tokio::sync::watch::Receiver<T>> {
    /// Wrapper of [tokio::sync::watch::Receiver::changed] that wraps the returned future in CancelSafe
    pub fn changed(
        &mut self,
    ) -> CancelSafe<impl Future<Output = Result<(), tokio::sync::watch::error::RecvError>> + '_>
    {
        // Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
        unsafe { CancelSafe::new_unchecked(self.0.changed()) }
    }
}

impl Selectable<tokio::net::TcpListener> {
    /// Wrapper of [tokio::net::TcpListener::accept] that wraps the returned future in CancelSafe
    pub fn accept(
        &self,
    ) -> CancelSafe<
        impl Future<Output = std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>> + '_,
    > {
        // Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
        unsafe { CancelSafe::new_unchecked(self.0.accept()) }
    }
}

impl Selectable<tokio::net::UnixListener> {
    /// Wrapper of [tokio::net::UnixListener::accept] that wraps the returned future in CancelSafe
    pub fn accept(
        &self,
    ) -> CancelSafe<
        impl Future<Output = std::io::Result<(tokio::net::UnixStream, tokio::net::unix::SocketAddr)>>
            + '_,
    > {
        // Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
        unsafe { CancelSafe::new_unchecked(self.0.accept()) }
    }
}

impl<R: tokio::io::AsyncReadExt + Unpin> Selectable<R> {
    /// Wrapper of [tokio::io::AsyncReadExt::read] that wraps the returned future in CancelSafe
    pub fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> CancelSafe<impl Future<Output = std::io::Result<usize>> + '_> {
        // Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
        unsafe { CancelSafe::new_unchecked(self.0.read(buf)) }
    }
    /// Wrapper of [tokio::io::AsyncReadExt::read_buf] that wraps the returned future in CancelSafe
    pub fn read_buf<'a, B: bytes::BufMut>(
        &'a mut self,
        buf: &'a mut B,
    ) -> CancelSafe<impl Future<Output = std::io::Result<usize>> + '_> {
        // Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
        unsafe { CancelSafe::new_unchecked(self.0.read_buf(buf)) }
    }
}

impl<R: tokio::io::AsyncWriteExt + Unpin> Selectable<R> {
    /// Wrapper of [tokio::io::AsyncWriteExt::write] that wraps the returned future in CancelSafe
    pub fn write<'a>(
        &'a mut self,
        src: &'a mut [u8],
    ) -> CancelSafe<impl Future<Output = std::io::Result<usize>> + '_> {
        // Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
        unsafe { CancelSafe::new_unchecked(self.0.write(src)) }
    }
    /// Wrapper of [tokio::io::AsyncWriteExt::write_buf] that wraps the returned future in CancelSafe
    pub fn write_buf<'a, B: bytes::Buf>(
        &'a mut self,
        src: &'a mut B,
    ) -> CancelSafe<impl Future<Output = std::io::Result<usize>> + '_> {
        // Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
        unsafe { CancelSafe::new_unchecked(self.0.write_buf(src)) }
    }
}

// Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
unsafe impl<St: ?Sized + futures::Stream + Unpin> CancellationSafe
    for futures::stream::Next<'_, St>
{
}

// Documented safe: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
unsafe impl<St: ?Sized + futures::TryStream + Unpin> CancellationSafe
    for futures::stream::TryNext<'_, St>
{
}
