// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pipelining reads to Blob

use crate::error::InvalidUsage;
use crate::fetch::{BatchFetcher, FetchedPart, LeasedBatchPart, SerdeLeasedBatchPart};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures::Stream;
use mz_persist_types::{Codec, Codec64};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;
use timely::progress::Timestamp;
use tokio::task::JoinHandle;

#[derive(Debug, Copy, Clone)]
pub(crate) struct PipelineParameters {
    pub(crate) max_inflight_bytes: usize,
    pub(crate) max_fetch_concurrency: usize,
}

/// A pipelined fetcher to concurrently fetch parts from [mz_persist::location::Blob]. The fetcher
/// has two knobs, [self::PipelineParameters::max_inflight_bytes] to control the maximum memory
/// footprint of any fetched / in-progress fetches, and [self::PipelineParameters::max_fetch_concurrency]
/// to control the maximum number of concurrent API calls to [mz_persist::location::Blob] allowed.
///
/// The pipeline is broken into two halves: an input handle and output stream. It is expected that
/// [PipelineFetcher::more_work] on the input is called frequently, wrapping all reads from the
/// output stream:
///
/// ```rust, norun
/// let (mut pipeline, mut pipeline_output) = PipelineFetcher::<K, V, T, D, ()>::new(params, batch_fetcher);
/// pipeline.push(part1);
/// while pipeline.more_work() {
///     let fetched_part = pipeline_output.next().await;
///     // etc
/// }
/// ```
///
/// To help ensure correct usage and the risk of partial reads, the output stream must be fully
/// consumed before dropping the input handle, otherwise it will panic.
#[derive(Debug)]
pub(crate) struct PipelineFetcher<K, V, T, D, M>
where
    T: Timestamp + Lattice + Codec64,
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
    M: Debug,
{
    params: PipelineParameters,
    batch_fetcher: BatchFetcher<K, V, T, D>,

    input_closed: bool,

    queue: VecDeque<(M, SerdeLeasedBatchPart)>,
    inflight_bytes: usize,
    concurrency_limit: Arc<tokio::sync::Semaphore>,
    pipelined_reads_tx: Option<
        tokio::sync::mpsc::UnboundedSender<(
            M,
            JoinHandle<(
                LeasedBatchPart<T>,
                Result<FetchedPart<K, V, T, D>, InvalidUsage<T>>,
            )>,
        )>,
    >,
    completed_read_rx: tokio::sync::mpsc::UnboundedReceiver<usize>,
}

impl<K, V, T, D, M> PipelineFetcher<K, V, T, D, M>
where
    T: Timestamp + Lattice + Codec64,
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
    M: Debug,
{
    pub(crate) fn new(
        params: PipelineParameters,
        batch_fetcher: BatchFetcher<K, V, T, D>,
    ) -> (
        Self,
        impl Stream<
            Item = (
                M,
                (
                    LeasedBatchPart<T>,
                    Result<FetchedPart<K, V, T, D>, InvalidUsage<T>>,
                ),
            ),
        >,
    ) {
        let concurrency = params.max_fetch_concurrency;
        let (part_tx, mut part_rx) = tokio::sync::mpsc::unbounded_channel::<(
            M,
            JoinHandle<(
                LeasedBatchPart<T>,
                Result<FetchedPart<K, V, T, D>, InvalidUsage<T>>,
            )>,
        )>();

        let (part_size_finished_tx, part_size_finished_rx) =
            tokio::sync::mpsc::unbounded_channel::<usize>();

        let output = async_stream::stream! {
            while let Some((metadata, fetch_handle)) = part_rx.recv().await {
                match fetch_handle.await {
                    Ok(fetched_part) => {
                        let _ = part_size_finished_tx.send(fetched_part.0.encoded_size_bytes);
                        yield (metadata, fetched_part)
                    },
                    // don't panic if the process is shutting down and the task is cancelled
                    Err(err) if err.is_cancelled() => break,
                    Err(err) => panic!("fetch part task failed: {}", err),
                }
            }
        };

        (
            Self {
                params,
                batch_fetcher,
                queue: VecDeque::new(),
                input_closed: false,
                inflight_bytes: 0,
                concurrency_limit: Arc::new(tokio::sync::Semaphore::new(concurrency)),
                completed_read_rx: part_size_finished_rx,
                pipelined_reads_tx: Some(part_tx),
            },
            output,
        )
    }

    /// Pushes another part and any additional metadata into the pipeline's queue.
    /// Pushing `None` will close the input to more values. Once the pipeline input
    /// is closed, pushing any additional parts will panic.
    pub(crate) fn push(&mut self, part: Option<(M, SerdeLeasedBatchPart)>) -> bool {
        let mut pushed = false;
        match part {
            Some(part) => {
                assert!(!self.input_closed, "pipeline input already closed");
                if !self.output_closed() {
                    self.queue.push_back(part);
                    pushed = true;
                }
            }
            None => {
                self.input_closed = true;
                pushed = true;
            }
        }
        self.enqueue_next();
        pushed
    }

    /// Indicates whether the pipeline is complete: when all parts ever pushed have been
    /// consumed by the output, or the output has been closed.
    ///
    /// This function is expected to be called frequently, wrapping any reads from the
    /// pipeline's output stream, to drive the internal state to completion.
    pub(crate) fn more_work(&mut self) -> bool {
        if self.output_closed() {
            return false;
        }

        self.enqueue_next();

        let draining_queue_finished = self.input_closed && self.queue.is_empty();
        let pipeline_finished = draining_queue_finished && self.inflight_bytes == 0;

        if draining_queue_finished {
            let _ = self.pipelined_reads_tx.take();
        }

        !pipeline_finished
    }

    fn enqueue_next(&mut self) {
        while let Ok(fetched_part_size) = self.completed_read_rx.try_recv() {
            self.inflight_bytes -= fetched_part_size;
        }

        while self.inflight_bytes < self.params.max_inflight_bytes {
            match self.queue.pop_front() {
                Some((metadata, part)) => {
                    let part_size = part.encoded_size_bytes;
                    let part_name = part.name();

                    let fetcher = self.batch_fetcher.clone();
                    let semaphore = Arc::clone(&self.concurrency_limit);
                    let fetch = mz_ore::task::spawn(|| part_name, async move {
                        let _permit = semaphore
                            .acquire_owned()
                            .await
                            .expect("semaphore not closed");
                        let fetch = fetcher
                            .fetch_leased_part(fetcher.leased_part_from_exchangeable(part))
                            .await;
                        fetch
                    });

                    let pipelined_reads_tx = self
                        .pipelined_reads_tx
                        .as_mut()
                        .expect("input should not be complete");
                    if let Err(_) = pipelined_reads_tx.send((metadata, fetch)) {
                        // the output has been closed. that's fine, nothing more to do here
                        return;
                    }
                    self.inflight_bytes += part_size;
                }
                None => break,
            }
        }
    }

    fn output_closed(&self) -> bool {
        if let Some(tx) = &self.pipelined_reads_tx {
            tx.is_closed()
        } else {
            true
        }
    }
}

impl<K, V, T, D, O> Drop for PipelineFetcher<K, V, T, D, O>
where
    T: Timestamp + Lattice + Codec64,
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
    O: Debug,
{
    fn drop(&mut self) {
        if !self.output_closed() {
            assert!(self.input_closed);
            assert!(self.pipelined_reads_tx.is_none());
            assert!(self.queue.is_empty());
            assert_eq!(self.inflight_bytes, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::fetch::LeasedBatchPart;
    use crate::pipeline::{PipelineFetcher, PipelineParameters};
    use crate::read::ReadHandle;
    use crate::tests::new_test_client;
    use crate::ShardId;
    use futures::stream::StreamExt;
    use futures_util::FutureExt;
    use std::sync::Arc;
    use timely::progress::Antichain;
    use tokio::pin;

    async fn setup() -> (
        ReadHandle<String, String, u64, i64>,
        Vec<LeasedBatchPart<u64>>,
    ) {
        let data = vec![
            (("1".to_owned(), "".to_owned()), 0u64, 1i64),
            (("2".to_owned(), "".to_owned()), 1, 1),
            (("3".to_owned(), "".to_owned()), 2, 1),
        ];

        let (mut write, mut read) = new_test_client()
            .await
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        let num_batches = 3;
        for i in 0..num_batches {
            write
                .expect_append(
                    &data[i as usize..(i + 1) as usize],
                    Antichain::from_elem(i),
                    Antichain::from_elem(i + 1),
                )
                .await;
        }

        let parts = read
            .snapshot(Antichain::from_elem(num_batches - 1))
            .await
            .expect("snapshot");

        (read, parts)
    }

    #[tokio::test]
    #[should_panic(expected = "assertion failed: self.input_closed")]
    async fn panic_on_nonempty_drop() {
        mz_ore::test::init_logging();
        let (read, _parts) = setup().await;

        let (pipeline, _pipeline_output) = PipelineFetcher::<String, String, u64, i64, ()>::new(
            PipelineParameters {
                max_inflight_bytes: 1,
                max_fetch_concurrency: 5,
            },
            read.clone("fetcher").await.batch_fetcher().await,
        );

        // should panic, we have no explicitly closed the input
        drop(pipeline);
    }

    #[tokio::test]
    #[should_panic(expected = "pipeline input already closed")]
    async fn panic_on_push_after_close() {
        mz_ore::test::init_logging();
        let (read, mut parts) = setup().await;

        let (mut pipeline, _pipeline_output) = PipelineFetcher::<String, String, u64, i64, ()>::new(
            PipelineParameters {
                max_inflight_bytes: 1,
                max_fetch_concurrency: 5,
            },
            read.clone("fetcher").await.batch_fetcher().await,
        );

        pipeline.push(None);

        let part = parts.pop().unwrap().into_exchangeable_part();
        // should panic, we've already closed our input
        pipeline.push(Some(((), part)));
    }

    #[tokio::test]
    async fn output_can_drop_before_input() {
        mz_ore::test::init_logging();
        let (read, mut parts) = setup().await;

        let (mut pipeline, pipeline_output) = PipelineFetcher::new(
            PipelineParameters {
                max_inflight_bytes: 1,
                max_fetch_concurrency: 5,
            },
            read.clone("fetcher").await.batch_fetcher().await,
        );

        let one = parts.pop().unwrap();
        assert!(pipeline.push(Some(((), one.into_exchangeable_part()))));

        drop(pipeline_output);

        // the output has dropped, we should no-op any further pushes on the inputs side
        let two = parts.pop().unwrap();
        assert!(!pipeline.push(Some(((), two.into_exchangeable_part()))));
        assert!(!pipeline.more_work());

        // verify we can drop the pipeline despite not explicitly closing the input
        drop(pipeline);
    }

    #[tokio::test]
    async fn end_to_end_with_small_inflight_bytes_bound() {
        mz_ore::test::init_logging();
        let (read, parts) = setup().await;

        let (mut pipeline, pipeline_output) = PipelineFetcher::new(
            PipelineParameters {
                max_inflight_bytes: 1,
                max_fetch_concurrency: 5,
            },
            read.clone("fetcher").await.batch_fetcher().await,
        );
        pin!(pipeline_output);

        let mut part_size = 0;
        for (i, part) in parts.into_iter().enumerate() {
            part_size = part.encoded_size_bytes;
            pipeline.push(Some((i, part.into_exchangeable_part())));
        }

        // inflight bytes is restrictive enough so only 1 part can be in flight at a time.
        // the rest should be in the queue until the pipeline is prodded
        assert_eq!(pipeline.queue.len(), 2);
        assert_eq!(pipeline.inflight_bytes, part_size);

        assert!(matches!(pipeline_output.next().await, Some((0, _))));
        assert_eq!(pipeline.queue.len(), 2);

        // prod the pipeline to keep transferring queued items to tasks
        assert!(pipeline.more_work());
        assert_eq!(pipeline.queue.len(), 1);
        assert!(matches!(pipeline_output.next().await, Some((1, _))));

        assert!(pipeline.more_work());
        assert_eq!(pipeline.queue.len(), 0);
        assert!(matches!(pipeline_output.next().await, Some((2, _))));

        // nothing in the queue, but the input has not yet been closed
        assert!(pipeline.more_work());
        assert_eq!(pipeline.inflight_bytes, 0);

        // close the input
        pipeline.push(None);

        // with no further input expected, and an empty queue,
        // we are done and the pipeline is safe to drop
        assert!(!pipeline.more_work());

        // with no more input expected, our output is complete
        assert!(matches!(pipeline_output.next().await, None));
    }

    #[tokio::test]
    async fn end_to_end_with_large_inflight_bytes_bound() {
        mz_ore::test::init_logging();
        let (read, parts) = setup().await;

        let (mut pipeline, pipeline_output) = PipelineFetcher::new(
            PipelineParameters {
                max_inflight_bytes: usize::MAX,
                max_fetch_concurrency: 5,
            },
            read.clone("fetcher").await.batch_fetcher().await,
        );
        pin!(pipeline_output);

        let mut inflight_bytes = 0;
        for (i, part) in parts.into_iter().enumerate() {
            inflight_bytes += part.encoded_size_bytes;
            pipeline.push(Some((i, part.into_exchangeable_part())));
        }
        pipeline.push(None);

        // the inflight bytes bound is large enough so everything
        // should have been pushed into tasks immediately
        assert_eq!(pipeline.queue.len(), 0);
        assert_eq!(pipeline.inflight_bytes, inflight_bytes);

        // nothing in the queue + our input was closed, but we have outstanding inflight tasks
        assert!(pipeline.more_work());

        assert!(matches!(pipeline_output.next().await, Some((0, _))));
        assert!(matches!(pipeline_output.next().await, Some((1, _))));
        assert!(matches!(pipeline_output.next().await, Some((2, _))));
        assert!(matches!(pipeline_output.next().await, None));

        // output has been flushed, we should be done!
        assert!(!pipeline.more_work());
    }

    #[tokio::test]
    async fn concurrency_limit() {
        mz_ore::test::init_logging();
        let (read, parts) = setup().await;

        let (mut pipeline, pipeline_output) = PipelineFetcher::new(
            PipelineParameters {
                max_inflight_bytes: usize::MAX,
                max_fetch_concurrency: 1,
            },
            read.clone("fetcher").await.batch_fetcher().await,
        );
        pin!(pipeline_output);

        let permit = Arc::clone(&pipeline.concurrency_limit)
            .acquire_owned()
            .await
            .expect("permit");

        let mut inflight_bytes = 0;
        for (i, part) in parts.into_iter().enumerate() {
            inflight_bytes += part.encoded_size_bytes;
            pipeline.push(Some((i, part.into_exchangeable_part())));
        }
        pipeline.push(None);

        // the inflight bytes bound is large enough so everything
        // should have been pushed into tasks immediately
        assert_eq!(pipeline.queue.len(), 0);
        assert_eq!(pipeline.inflight_bytes, inflight_bytes);

        // but we're holding the only permit, so nothing should complete yet
        assert!(pipeline.more_work());
        assert!(matches!(pipeline_output.next().now_or_never(), None));
        assert!(matches!(pipeline_output.next().now_or_never(), None));
        assert!(matches!(pipeline_output.next().now_or_never(), None));
        assert!(matches!(pipeline_output.next().now_or_never(), None));
        assert!(matches!(pipeline_output.next().now_or_never(), None));
        assert_eq!(pipeline.inflight_bytes, inflight_bytes);

        // release the 1 permit, allowing the fetches to proceed
        drop(permit);
        assert!(matches!(pipeline_output.next().await, Some((0, _))));
        assert!(matches!(pipeline_output.next().await, Some((1, _))));
        assert!(matches!(pipeline_output.next().await, Some((2, _))));
        assert!(matches!(pipeline_output.next().await, None));

        // output has been flushed, we should be done!
        assert!(!pipeline.more_work());
    }
}
