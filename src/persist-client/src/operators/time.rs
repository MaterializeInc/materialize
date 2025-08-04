use crate::operators::shard_source::ErrorHandler;
use anyhow::{Context, anyhow};
use differential_dataflow::lattice::Lattice;
use futures_util::FutureExt;
use futures_util::future::LocalBoxFuture;
use std::convert::Infallible;
use std::sync::{Arc, RwLock};
use timely::progress::{Antichain, Timestamp};

/// A range of times representing an (inclusive) range of possible as-of bounds for a dataflow.
#[derive(Debug, Clone, PartialEq)]
pub struct Bounds<T> {
    lower: Antichain<T>,
    upper: Antichain<T>,
}

impl<T: Timestamp + Lattice> Default for Bounds<T> {
    fn default() -> Self {
        Self {
            lower: Antichain::from_elem(T::minimum()),
            upper: Antichain::new(),
        }
    }
}

impl<T: Timestamp + Lattice> Bounds<T> {
    pub fn new_lower(lower: Antichain<T>) -> Self {
        Self {
            lower,
            upper: Antichain::new(),
        }
    }

    pub fn new_upper(upper: Antichain<T>) -> Self {
        Self {
            lower: Antichain::from_elem(T::minimum()),
            upper,
        }
    }

    pub fn lower(&self) -> &Antichain<T> {
        &self.lower
    }

    pub fn upper(&self) -> &Antichain<T> {
        &self.upper
    }

    pub fn merge(&mut self, other: &Self) {
        self.lower.join_assign(&other.lower);
        self.upper.meet_assign(&other.upper);
    }
}

/// Persist's async initialization interacts awkwardly with Timely Dataflow.
/// - Dataflow initialization can't run async code.
/// - We need to select as-of timestamps and things for our timely operators to initialize the
///   dataflow.
/// - We need to run async code to determine the available timestamps. (For example, grabbing a read
///   hold.
///
/// This struct avoids the trilemma by:
/// - Kicking off the timestamp determination in the background.
/// - Blocking any requests for the final bounds until all the background timestamp-determination
///   activity is complete.
#[derive(Debug)]
pub struct AsyncBounds<T> {
    error_handler: ErrorHandler,
    bounds: Arc<RwLock<Result<Bounds<T>, ()>>>,
    tx: tokio::sync::broadcast::Sender<Infallible>,
}
impl<T: Timestamp + Lattice + Sync> AsyncBounds<T> {
    pub fn new(error_handler: ErrorHandler) -> Self {
        Self {
            error_handler,
            bounds: Arc::new(RwLock::new(Ok(Bounds::default()))),
            tx: tokio::sync::broadcast::channel(1).0,
        }
    }
    pub fn merge_bounds(self, bound: &Bounds<T>) {
        if let Ok(bounds) = &mut *self.bounds.write().unwrap() {
            bounds.merge(bound);
        }
    }

    pub fn run<A>(
        &self,
        action: impl Future<Output = anyhow::Result<(Bounds<T>, A)>> + Send + 'static,
    ) -> LocalBoxFuture<'static, A>
    where
        A: Send + 'static,
    {
        let shared_bounds = Arc::clone(&self.bounds);
        let tx = self.tx.clone();
        let error_handler = self.error_handler.clone();
        async move {
            let result = mz_ore::task::spawn(|| "async bounds", async move {
                match action.await {
                    Ok((bound, data)) => {
                        if let Ok(bounds) = &mut *shared_bounds.write().unwrap() {
                            bounds.merge(&bound);
                        }
                        drop(tx); // Hold on to the sender until after we've finished determining the timestamp.
                        Ok(data)
                    }
                    Err(err) => {
                        *shared_bounds.write().unwrap() = Err(());
                        Err(err)
                    }
                }
            })
            .abort_on_drop()
            .await
            .unwrap_or_else(|e| Err(anyhow!(e)).context("background task failed"));

            match result {
                Ok(value) => value,
                Err(err) => error_handler.report_and_stop(err).await,
            }
        }
        .boxed_local()
    }

    pub fn bounds(&self) -> LocalBoxFuture<'static, Bounds<T>> {
        let bounds = Arc::clone(&self.bounds);
        let mut bounds_rx = self.tx.subscribe();
        let error_handler = self.error_handler.clone();
        async move {
            // There are never any sends on the broadcast channel, so this will only ever resolve when
            // all senders are gone, which happens once the dataflow builder is dropped and everyone
            // who was planning to has applied a bound.
            let _ = bounds_rx.recv().await;
            match &*bounds.read().unwrap() {
                Ok(bounds) => bounds.clone(),
                Err(_) => {
                    error_handler
                        .report_and_stop(anyhow!("task failed while computing bounds"))
                        .await;
                }
            }
        }
        .boxed_local()
    }
}

#[cfg(test)]
mod tests {
    use crate::operators::shard_source::ErrorHandler;
    use crate::operators::time::{AsyncBounds, Bounds};
    use futures_util::poll;
    use std::task::Poll;
    use std::time::Duration;
    use timely::progress::Antichain;
    use tokio::time::timeout;

    #[mz_ore::test(tokio::test)]
    async fn test_bounds() {
        const TIMEOUT: Duration = Duration::from_secs(1);

        let bounds: AsyncBounds<u64> = AsyncBounds::new(ErrorHandler::Halt("yikes"));

        let mut as_of = bounds.bounds();

        let (tx0, rx0) = tokio::sync::oneshot::channel();
        let mut f0 = bounds.run(async {
            let bounds = rx0.await?;
            Ok((bounds, ()))
        });

        let (tx1, rx1) = tokio::sync::oneshot::channel();
        let mut f1 = bounds.run(async {
            let bounds = rx1.await?;
            Ok((bounds, ()))
        });

        // Initially, none of the futures are ready.
        assert_eq!(poll!(&mut as_of), Poll::Pending);
        assert_eq!(poll!(&mut f0), Poll::Pending);
        assert_eq!(poll!(&mut f1), Poll::Pending);

        // When the first future is ready, we can't yet get the as_of, since another future is waiting.
        tx0.send(Bounds::new_lower(Antichain::from_elem(1)))
            .unwrap();

        assert_eq!(timeout(TIMEOUT, f0).await.unwrap(), ());
        assert_eq!(poll!(&mut as_of), Poll::Pending);
        assert_eq!(poll!(&mut f1), Poll::Pending);

        // Even after the second future is ready, the as_of shouldn't be, since we could always add another task.
        tx1.send(Bounds::new_lower(Antichain::from_elem(2)))
            .unwrap();

        assert_eq!(timeout(TIMEOUT, f1).await.unwrap(), ());
        assert_eq!(poll!(&mut as_of), Poll::Pending);

        // Once all tasks are complete and the utility is dropped, we get the aggregate bounds.
        drop(bounds);

        assert_eq!(
            timeout(TIMEOUT, as_of).await.unwrap(),
            Bounds::new_lower(Antichain::from_elem(2))
        );
    }
}
