// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Threading and synchronization utilities.

use std::sync::Mutex;

/// A synchronized resource lottery.
///
/// Controls access to a non-[`Sync`] resource by allowing only one thread to
/// win the resource "lottery." A dummy resource is constructed for threads
/// that lose the lottery.
///
/// # Examples
///
/// ```rust
/// # use std::io;
/// # use std::io::Write;
/// # use std::thread;
/// # use ore::sync::Lottery;
///
/// struct Discarder;
///
/// impl io::Write for Discarder {
///     fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
///         Ok(buf.len())
///     }
///     fn flush(&mut self) -> io::Result<()> {
///         Ok(())
///     }
/// }
///
/// let stderr: Box<io::Write + Send> = Box::new(io::stderr());
/// let lottery = Lottery::new(stderr, || Box::new(Discarder));
/// crossbeam::thread::scope(|thread_scope| {
///     (0..5)
///         .into_iter()
///         .map(|_| {
///             thread_scope.spawn(|_| {
///                 write!(lottery.draw(), "Can you hear me?");
///             })
///         })
///         .for_each(|handle| handle.join().unwrap());
/// })
/// .unwrap()
/// ```
#[derive(Debug)]
pub struct Lottery<T, F>
where
    F: Fn() -> T,
{
    winner: Mutex<Option<T>>,
    losers: F,
}

impl<T, F> Lottery<T, F>
where
    F: Fn() -> T,
{
    /// Creates a new `Lottery` from the specified winner object and a function
    /// to construct loser objects.
    pub fn new(winner: T, losers: F) -> Lottery<T, F> {
        Lottery {
            winner: Mutex::new(Some(winner)),
            losers,
        }
    }

    /// Attempts to win the lottery. It returns the winner resource if this is
    /// the first thread to call `draw`. If another thread has already claimed
    /// the winner resource, it instead constructs and returns a loser resource.
    pub fn draw(&self) -> T {
        let mut guard = self.winner.lock().unwrap();
        match guard.take() {
            Some(t) => t,
            None => (self.losers)(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crossbeam::thread;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::Lottery;

    #[test]
    fn test_lottery() {
        let lottery = Lottery::new(true, || false);
        let loser_count = Arc::new(AtomicUsize::new(0));
        let winner_count = Arc::new(AtomicUsize::new(0));

        thread::scope(|scope| {
            for _ in 0..5 {
                scope.spawn(|_| {
                    match lottery.draw() {
                        true => winner_count.fetch_add(1, Ordering::SeqCst),
                        false => loser_count.fetch_add(1, Ordering::SeqCst),
                    };
                });
            }
        })
        .unwrap();

        assert_eq!(winner_count.load(Ordering::SeqCst), 1);
        assert_eq!(loser_count.load(Ordering::SeqCst), 4);
    }
}
