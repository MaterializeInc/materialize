use std::collections::HashMap;
use std::time::Duration;

use differential_dataflow::operators::Consolidate;
use differential_dataflow::{AsCollection, Collection};
use mz_persist_client::read::ListenEvent;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{Event, ToStreamAsync};
use timely::dataflow::Scope;

use mz_persist_client::{PersistClient, PersistLocation, ShardId};
use mz_repr::{Diff, Row, Timestamp};
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

pub(crate) fn persist_roundtrip<G>(
    collection: Collection<G, (Row, Row), Diff>,
) -> Collection<G, (Row, Row), Diff>
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = collection.scope();
    let active_worker_index = 0;
    let timeout = Duration::from_secs(5);

    let (write, read) = if active_worker_index == scope.index() {
        let persist_location = PersistLocation {
            consensus_uri: "sqlite:///tmp/mz-persist/consensus".into(),
            blob_uri: "file:///tmp/mz-persist/blob".into(),
        };
        let shard_id = ShardId::new();

        let (blob, consensus) = futures_executor::block_on(persist_location.open(timeout))
            .expect("cannot open persist location");
        let persist_client =
            futures_executor::block_on(PersistClient::new(timeout, blob, consensus))
                .expect("cannot open client");
        let (write, read) = futures_executor::block_on(
            persist_client.open::<Row, Row, Timestamp, Diff>(timeout, shard_id),
        )
        .expect("could not open persist shard");
        (Some(write), Some(read))
    } else {
        (None, None)
    };

    // Ensure the collection contains no 0 diffs, as persist chokes on those.
    // Aside from that, it seems to be a good idea to reduce the data as much
    // as possible before sending it over the network.
    let collection = collection.consolidate();

    let mut sink = OperatorBuilder::new("Logging Persist Sink".into(), scope.clone());
    let mut input = sink.new_input(
        &collection.inner,
        Exchange::new(move |_| active_worker_index as u64),
    );

    sink.build_async(
        scope.clone(),
        move |_capabilities, frontiers, scheduler| async move {
            let mut write = match write {
                Some(w) => w,
                None => return,
            };

            let mut buffer = Vec::new();
            let mut stash = HashMap::<_, Vec<_>>::new();

            while scheduler.notified().await {
                let frontier = &frontiers.borrow()[0];

                input.for_each(|_cap, data| {
                    data.swap(&mut buffer);
                    for update in buffer.drain(..) {
                        let ts = update.1;
                        stash.entry(ts).or_default().push(update);
                    }
                });

                let updates = stash
                    .iter()
                    .filter(|(ts, _updates)| !frontier.less_equal(ts))
                    .flat_map(|(_ts, updates)| updates.iter());

                write
                    .append(timeout, updates, frontier.clone())
                    .await
                    .expect("cannot append updates")
                    .expect("cannot append updates")
                    .expect("invalid/outdated upper");

                stash.retain(|ts, _updates| frontier.less_equal(ts));
            }
        },
    );

    let source_stream = async_stream::stream! {
        let read = match read {
            Some(r) => r,
            None => return,
        };

        let as_of = read.since();
        let mut snapshot = read
            .snapshot(timeout, as_of.clone())
            .await
            .expect("cannot create snapshot")
            .expect("invalid usage");

        loop {
            let next = snapshot.next(timeout).await.expect("cannot read snapshot");
            if next.is_empty() {
                break;
            }
            for update in next {
                yield Event::Message(update.1, update);
            }
        }

        let mut listen = read
            .listen(timeout, as_of.clone())
            .await
            .expect("cannot create listen")
            .expect("invalid usage");

        loop {
            let next = listen.next(timeout).await.expect("cannot read listen");
            for event in next {
                match event {
                    ListenEvent::Progress(upper) => {
                        yield Event::Progress(upper);
                    }
                    ListenEvent::Updates(updates) => {
                        for update in updates {
                            yield Event::Message(update.1, update);
                        }
                    }
                }
            }
        }
    };

    Box::pin(source_stream)
        .to_stream(&scope)
        .as_collection()
        .map(|(key, value)| (key.expect("key error"), value.expect("value error")))
