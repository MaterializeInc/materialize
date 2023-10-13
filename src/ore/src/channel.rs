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

use async_trait::async_trait;

/// Extensions for the receiving end of asynchronous channels.
#[async_trait]
pub trait ReceiverExt<T: Send> {
    /// Receives all of the currently buffered elements on the channel, up to some max.
    ///
    /// This method returns `None` if the channel has been closed and there are no remaining
    /// messages in the channel's buffer.
    ///
    /// If there are no messages in the channel's buffer, but the channel is not yet closed, this
    /// method will sleep until a message is sent or the channel is closed. When woken it will
    /// return up to max currently buffered elements.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv_many` is used as the event in a `select!` statement
    /// and some other branch completes first, it is guaranteed that no messages were received on
    /// this channel.
    ///
    /// # Max Buffer Size
    ///
    /// The provided max buffer size should always be less than the total capacity of the channel.
    /// Otherwise a good value is probably a fraction of the total channel size, or however large
    /// a batch that your receiving component can handle.
    ///
    /// TODO(parkmycar): We should refactor this to use `impl Iterator` instead of `Vec` when
    /// "impl trait in trait" is supported.
    async fn recv_many(&mut self, max: usize) -> Option<Vec<T>>;
}

#[async_trait]
impl<T: Send> ReceiverExt<T> for tokio::sync::mpsc::Receiver<T> {
    async fn recv_many(&mut self, max: usize) -> Option<Vec<T>> {
        // Wait for a value to be ready.
        let first = self.recv().await?;
        let mut buffer = Vec::from([first]);

        // Note(parkmycar): It's very important for cancelation safety that we don't add any more
        // .await points other than the initial one.

        // Pull all of the remaining values off the channel.
        while let Ok(v) = self.try_recv() {
            buffer.push(v);

            // Break so we don't loop here continuously.
            if buffer.len() >= max {
                break;
            }
        }

        Some(buffer)
    }
}

#[async_trait]
impl<T: Send> ReceiverExt<T> for tokio::sync::mpsc::UnboundedReceiver<T> {
    async fn recv_many(&mut self, max: usize) -> Option<Vec<T>> {
        // Wait for a value to be ready.
        let first = self.recv().await?;
        let mut buffer = Vec::from([first]);

        // Note(parkmycar): It's very important for cancelation safety that we don't add any more
        // .await points other than the initial one.

        // Pull all of the remaining values off the channel.
        while let Ok(v) = self.try_recv() {
            buffer.push(v);

            // Break so we don't loop here continuously.
            if buffer.len() >= max {
                break;
            }
        }

        Some(buffer)
    }
}

// allow `futures::block_on` for testing.
#[allow(clippy::disallowed_methods)]
#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use futures::FutureExt;
    use tokio::sync::mpsc;

    use super::ReceiverExt;

    #[crate::test]
    fn smoke_test_tokio_mpsc() {
        let (tx, mut rx) = mpsc::channel(16);

        // Buffer a few elements.
        tx.try_send(1).expect("enough capacity");
        tx.try_send(2).expect("enough capacity");
        tx.try_send(3).expect("enough capacity");
        tx.try_send(4).expect("enough capacity");
        tx.try_send(5).expect("enough capacity");

        // Receive a max of three elements at once.
        let elements = block_on(rx.recv_many(3)).expect("values");
        assert_eq!(elements, [1, 2, 3]);

        // Receive the remaining elements.
        let elements = block_on(rx.recv_many(8)).expect("values");
        assert_eq!(elements, [4, 5]);
    }

    #[crate::test]
    fn smoke_test_tokio_unbounded() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Buffer a few elements.
        tx.send(1).expect("enough capacity");
        tx.send(2).expect("enough capacity");
        tx.send(3).expect("enough capacity");
        tx.send(4).expect("enough capacity");
        tx.send(5).expect("enough capacity");

        // Receive a max of three elements at once.
        let elements = block_on(rx.recv_many(3)).expect("values");
        assert_eq!(elements, [1, 2, 3]);

        // Receive the remaining elements.
        let elements = block_on(rx.recv_many(8)).expect("values");
        assert_eq!(elements, [4, 5]);
    }

    #[crate::test]
    fn test_tokio_mpsc_permit() {
        let (tx, mut rx) = mpsc::channel(16);

        // Reserve space for a few elements.
        let permit1 = tx.clone().try_reserve_owned().expect("enough capacity");
        let permit2 = tx.clone().try_reserve_owned().expect("enough capacity");
        let permit3 = tx.clone().try_reserve_owned().expect("enough capacity");

        // Close the channel.
        drop(tx);

        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        let mut recv_many = rx.recv_many(4);

        // The channel is closed, but there are outstanding permits, so we should return pending.
        assert!(recv_many.poll_unpin(&mut cx).is_pending());

        // Send data on the channel.
        permit1.send(1);
        permit2.send(2);
        permit3.send(3);

        // We should receive all of the data after a single poll.
        let elements = match recv_many.poll_unpin(&mut cx) {
            std::task::Poll::Ready(elements) => elements.expect("elements to be returned"),
            std::task::Poll::Pending => panic!("future didn't immediately return elements!"),
        };
        assert_eq!(elements, [1, 2, 3]);
        drop(recv_many);

        // Polling the channel one more time should return None since the channel is closed.
        let elements = match rx.recv_many(4).poll_unpin(&mut cx) {
            std::task::Poll::Ready(elements) => elements,
            std::task::Poll::Pending => panic!("future didn't immediately return"),
        };
        assert!(elements.is_none());
    }

    #[crate::test]
    fn test_empty_channel() {
        let (tx, mut rx) = mpsc::channel::<usize>(16);

        let recv_many = rx.recv_many(4);
        drop(tx);

        let elements = block_on(recv_many);
        assert!(elements.is_none());
    }

    #[crate::test]
    fn test_atleast_two_semantics() {
        let (tx, mut rx) = mpsc::channel(16);

        // Buffer a few elements.
        tx.try_send(1).expect("enough capacity");
        tx.try_send(2).expect("enough capacity");
        tx.try_send(3).expect("enough capacity");

        // Even though we specify a max of one, we'll receive at least 2.
        let elements = block_on(rx.recv_many(1)).expect("values");
        assert_eq!(elements, [1, 2]);
    }

    #[crate::test]
    fn test_cancelation_safety() {
        let (tx, mut rx) = mpsc::channel(16);

        // Buffer a few elements.
        tx.try_send(1).expect("enough capacity");
        tx.try_send(2).expect("enough capacity");
        tx.try_send(3).expect("enough capacity");

        let mut immediate_ready = Box::pin(async { 100 }).fuse();

        let mut count = 0;
        let mut result = vec![];

        loop {
            count += 1;

            block_on(async {
                futures::select_biased! {
                    single = &mut immediate_ready => result.push(single),
                    many = &mut rx.recv_many(2).fuse() => {
                        let values = many.expect("stream ended!");
                        result.extend(values);
                    },
                }
            });

            if count >= 3 {
                break;
            }
        }

        assert_eq!(result, [100, 1, 2, 3]);
    }

    #[crate::test]
    fn test_closed_channel() {
        let (tx, mut rx) = mpsc::channel(16);

        tx.try_send(1).expect("enough capacity");
        tx.try_send(2).expect("enough capacity");
        tx.try_send(3).expect("enough capacity");

        // Drop the sender to close it.
        drop(tx);

        // Make sure the buffer is larger than queued elements.
        let elements = block_on(rx.recv_many(4)).expect("elements");
        assert_eq!(elements, [1, 2, 3]);

        // Receiving again should return None.
        assert!(block_on(rx.recv_many(4)).is_none());
    }
}
