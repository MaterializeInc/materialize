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
use tracing::warn;

#[derive(Debug, Copy, Clone)]
pub(crate) struct PipelineParameters {
    pub(crate) max_queue_size: usize,
    pub(crate) max_inflight_bytes: usize,
    pub(crate) max_fetch_concurrency: usize,
}

#[derive(Debug)]
pub struct PipelineFetcher<K, V, T, D, O>
where
    T: Timestamp + Lattice + Codec64,
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
    O: Debug,
{
    params: PipelineParameters,
    batch_fetcher: BatchFetcher<K, V, T, D>,

    input_complete: bool,

    queue: VecDeque<(O, SerdeLeasedBatchPart)>,
    inflight_bytes: usize,
    concurrency_limit: Arc<tokio::sync::Semaphore>,
    pipelined_reads_tx: Option<
        tokio::sync::mpsc::UnboundedSender<(
            O,
            JoinHandle<(
                LeasedBatchPart<T>,
                Result<FetchedPart<K, V, T, D>, InvalidUsage<T>>,
            )>,
        )>,
    >,
    completed_read_rx: tokio::sync::mpsc::UnboundedReceiver<usize>,
}

impl<K, V, T, D, O> PipelineFetcher<K, V, T, D, O>
where
    T: Timestamp + Lattice + Codec64,
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
    O: Debug,
{
    pub(crate) fn new(
        params: PipelineParameters,
        batch_fetcher: BatchFetcher<K, V, T, D>,
    ) -> (
        Self,
        impl Stream<
            Item = (
                O,
                (
                    LeasedBatchPart<T>,
                    Result<FetchedPart<K, V, T, D>, InvalidUsage<T>>,
                ),
            ),
        >,
    ) {
        let concurrency = params.max_fetch_concurrency;
        let (part_tx, mut part_rx) = tokio::sync::mpsc::unbounded_channel::<(
            O,
            JoinHandle<(
                LeasedBatchPart<T>,
                Result<FetchedPart<K, V, T, D>, InvalidUsage<T>>,
            )>,
        )>();

        let (part_size_finished_tx, part_size_finished_rx) =
            tokio::sync::mpsc::unbounded_channel::<usize>();

        let output = async_stream::stream! {
            while let Some((opaque, fetch_handle)) = part_rx.recv().await {
                match fetch_handle.await {
                    Ok(fetched_part) => {
                        let _ = part_size_finished_tx.send(fetched_part.0.encoded_size_bytes);
                        yield (opaque, fetched_part)
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
                input_complete: false,
                inflight_bytes: 0,
                concurrency_limit: Arc::new(tokio::sync::Semaphore::new(concurrency)),
                completed_read_rx: part_size_finished_rx,
                pipelined_reads_tx: Some(part_tx),
            },
            output,
        )
    }

    pub(crate) fn push(&mut self, part: Option<(O, SerdeLeasedBatchPart)>) {
        match part {
            Some(part) => {
                assert!(!self.input_complete);
                self.queue.push_back(part);
            }
            None => self.input_complete = true,
        }
        self.enqueue_next();
    }

    pub(crate) fn has_work(&mut self) -> bool {
        self.enqueue_next();

        let draining_queue_finished = self.input_complete && self.queue.is_empty();
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
                Some((capabilities, part)) => {
                    let part_size = part.encoded_size_bytes;
                    let part_name = part.name();

                    let fetcher = self.batch_fetcher.clone();
                    let semaphore = Arc::clone(&self.concurrency_limit);
                    let fetch = mz_ore::task::spawn(|| part_name, async move {
                        let _permit = semaphore
                            .acquire_owned()
                            .await
                            .expect("semaphore not closed");
                        fetcher
                            .fetch_leased_part(fetcher.leased_part_from_exchangeable(part))
                            .await
                    });

                    let pipelined_reads_tx = self
                        .pipelined_reads_tx
                        .as_mut()
                        .expect("input should not be complete");
                    if let Err(err) = pipelined_reads_tx.send((capabilities, fetch)) {
                        warn!("pipelined reads receiver has hung up. this should only happen on process shut down. err: {:?}", err);
                        break;
                    }
                    self.inflight_bytes += part_size;
                }
                None => break,
            }
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
        assert!(self.input_complete);
        assert!(self.pipelined_reads_tx.is_none());
        assert_eq!(self.inflight_bytes, 0);
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{PipelineFetcher, PipelineParameters};
    use crate::read::ListenEvent;
    use crate::tests::new_test_client;
    use crate::ShardId;
    use timely::progress::Antichain;
    use tracing::info;

    fn push_next_part(pipeline: &mut PipelineFetcher<String, String, u64, i64, usize>) {}

    #[tokio::test]
    async fn it_works() {
        mz_ore::test::init_logging();

        let data = vec![
            (("1".to_owned(), "one".to_owned()), 0u64, 1i64),
            (("2".to_owned(), "two".to_owned()), 1, 1),
            (("3".to_owned(), "three".to_owned()), 2, 1),
        ];

        let (mut write, mut read) = new_test_client()
            .await
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;
        for i in 0..3u64 {
            write
                .expect_append(
                    &data[i as usize..(i + 1) as usize],
                    Antichain::from_elem(i),
                    Antichain::from_elem(i + 1),
                )
                .await;
        }

        let mut subscribe = read
            .clone("subscribe")
            .await
            .subscribe(Antichain::from_elem(0))
            .await
            .expect("subscribe");

        let (mut pipeline, pipeline_output) = PipelineFetcher::new(
            PipelineParameters {
                max_queue_size: 10,
                max_inflight_bytes: 100,
                max_fetch_concurrency: 5,
            },
            read.clone("fetcher").await.batch_fetcher().await,
        );

        let mut all_parts = vec![];
        for i in 0..3 {
            let events = subscribe.next().await;
            for event in events {
                if let ListenEvent::Updates(mut parts) = event {
                    all_parts.append(&mut parts);
                }
            }
        }

        info!("parts: {:?}", all_parts);
        for part in all_parts {
            pipeline.push(Some((0, part.into_exchangeable_part())));
        }
    }
}
