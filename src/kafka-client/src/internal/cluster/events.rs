// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Client-level error and statistics events, delivered like librdkafka's main
//! queue (`rk_rep`).
//!
//! librdkafka does not call `error_cb` and `stats_cb` from the thread that
//! raises them: it enqueues them on the main queue, which only the
//! application serves. With rust-rdkafka that is
//!
//! * consumers: `BaseConsumer::poll` and `StreamConsumer` streams. A consumer
//!   that is never polled (e.g. Materialize's connection validation) never
//!   sees these callbacks;
//! * `ThreadedProducer` and `FutureProducer`: a dedicated polling thread, so
//!   callbacks arrive promptly;
//! * admin clients: nobody; the callbacks never run.
//!
//! [`EventQueue`] reproduces this per client kind. Log lines do not go
//! through it; librdkafka calls `log_cb` synchronously.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::config::settings::ClientKind;
use crate::error::KafkaError;
use crate::internal::hooks::Hooks;
use crate::statistics::Statistics;

/// The maximum number of queued errors. librdkafka's queue is unbounded,
/// but its duplicate error suppression keeps it small; the oldest errors are
/// dropped beyond this.
const MAX_QUEUED_ERRORS: usize = 1000;

enum Event {
    Error(KafkaError, String),
    Stats(Statistics),
}

/// How events reach the hooks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Delivery {
    /// Delivered when raised, as by rust-rdkafka's producer polling thread.
    Inline,
    /// Delivered by [`EventQueue::serve`], called when the application polls.
    Queued,
    /// Never delivered, as for rust-rdkafka's admin client.
    Discarded,
}

type Wakeup = Box<dyn Fn() + Send + Sync>;

/// The error and statistics events of one client.
pub(crate) struct EventQueue {
    hooks: Arc<dyn Hooks>,
    delivery: Delivery,
    queue: Mutex<VecDeque<Event>>,
    /// The length of `queue`, readable without the lock.
    len: AtomicUsize,
    /// Serializes `serve` calls, so that events are delivered in order.
    serving: Mutex<()>,
    /// Called after an event is queued, to wake a polling application.
    wakeup: Mutex<Option<Wakeup>>,
}

impl EventQueue {
    pub(crate) fn new(hooks: Arc<dyn Hooks>, kind: ClientKind) -> EventQueue {
        EventQueue {
            hooks,
            delivery: match kind {
                ClientKind::Consumer => Delivery::Queued,
                ClientKind::Producer => Delivery::Inline,
                ClientKind::Admin => Delivery::Discarded,
            },
            queue: Mutex::new(VecDeque::new()),
            len: AtomicUsize::new(0),
            serving: Mutex::new(()),
            wakeup: Mutex::new(None),
        }
    }

    /// Raises an error event, for `ClientContext::error`.
    pub(crate) fn error(&self, error: KafkaError, reason: &str) {
        match self.delivery {
            Delivery::Inline => self.hooks.error(error, reason),
            Delivery::Queued => self.push(Event::Error(error, reason.to_string())),
            Delivery::Discarded => {}
        }
    }

    /// Raises a statistics event, for `ClientContext::stats`.
    pub(crate) fn stats(&self, statistics: Statistics) {
        match self.delivery {
            Delivery::Inline => self.hooks.stats(statistics),
            Delivery::Queued => self.push(Event::Stats(statistics)),
            Delivery::Discarded => {}
        }
    }

    fn push(&self, event: Event) {
        {
            let mut queue = self.queue.lock().expect("lock poisoned");
            match event {
                // Only the latest statistics of an unpolled client are kept.
                Event::Stats(_) => queue.retain(|e| !matches!(e, Event::Stats(_))),
                Event::Error(..) => {
                    let errors = queue
                        .iter()
                        .filter(|e| matches!(e, Event::Error(..)))
                        .count();
                    if errors >= MAX_QUEUED_ERRORS {
                        if let Some(pos) = queue.iter().position(|e| matches!(e, Event::Error(..)))
                        {
                            queue.remove(pos);
                        }
                    }
                }
            }
            queue.push_back(event);
            self.len.store(queue.len(), Ordering::Release);
        }
        if let Some(wakeup) = &*self.wakeup.lock().expect("lock poisoned") {
            wakeup();
        }
    }

    /// Registers the function that wakes a polling application when an
    /// event is queued. It is called without any queue lock held.
    pub(crate) fn set_wakeup(&self, wakeup: impl Fn() + Send + Sync + 'static) {
        *self.wakeup.lock().expect("lock poisoned") = Some(Box::new(wakeup));
    }

    /// Reports whether events wait to be served.
    pub(crate) fn has_pending(&self) -> bool {
        self.len.load(Ordering::Acquire) > 0
    }

    /// Delivers the queued events to the hooks, in order, on the calling
    /// thread, like `rd_kafka_poll(rk, 0)`. No lock of the queue is held
    /// while a callback runs.
    ///
    /// Returns false, delivering nothing, if another call is delivering
    /// events (on another thread, or further up this thread's stack when a
    /// callback polls the consumer); that call also delivers the events
    /// queued now, and wakes the consumer if more are queued afterwards.
    pub(crate) fn serve(&self) -> bool {
        if !self.has_pending() {
            return true;
        }
        // Whoever holds `serving` delivers everything queued, including
        // events pushed meanwhile. Not waiting for it keeps a callback that
        // polls the consumer (and so re-enters here) from deadlocking.
        let _serving = match self.serving.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return false,
            Err(std::sync::TryLockError::Poisoned(e)) => e.into_inner(),
        };
        loop {
            let event = {
                let mut queue = self.queue.lock().expect("lock poisoned");
                let event = queue.pop_front();
                self.len.store(queue.len(), Ordering::Release);
                event
            };
            match event {
                Some(Event::Error(error, reason)) => self.hooks.error(error, &reason),
                Some(Event::Stats(statistics)) => self.hooks.stats(statistics),
                None => return true,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::client::ClientContext;
    use crate::types::RDKafkaErrorCode;

    #[derive(Default)]
    struct Recorder {
        events: Mutex<Vec<String>>,
    }

    impl ClientContext for Arc<Recorder> {
        fn error(&self, error: KafkaError, reason: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("error {error}: {reason}"));
        }

        fn stats(&self, statistics: Statistics) {
            self.events
                .lock()
                .unwrap()
                .push(format!("stats {}", statistics.msg_cnt));
        }
    }

    fn error(n: i32) -> (KafkaError, String) {
        (
            KafkaError::Global(RDKafkaErrorCode::BrokerTransportFailure),
            n.to_string(),
        )
    }

    fn stats(n: u64) -> Statistics {
        Statistics {
            msg_cnt: n,
            ..Statistics::default()
        }
    }

    #[mz_ore::test]
    fn delivery_by_client_kind() {
        let recorder = Arc::new(Recorder::default());
        let hooks: Arc<dyn Hooks> = Arc::new(Arc::clone(&recorder));
        let events = |r: &Recorder| std::mem::take(&mut *r.events.lock().unwrap());

        // Producers deliver right away.
        let q = EventQueue::new(Arc::clone(&hooks), ClientKind::Producer);
        let (e, reason) = error(1);
        q.error(e, &reason);
        q.stats(stats(1));
        assert_eq!(events(&recorder).len(), 2);

        // Admin clients never deliver.
        let q = EventQueue::new(Arc::clone(&hooks), ClientKind::Admin);
        let (e, reason) = error(1);
        q.error(e, &reason);
        q.stats(stats(1));
        q.serve();
        assert!(events(&recorder).is_empty());
        assert!(!q.has_pending());

        // Consumers deliver when served, in order, keeping only the latest
        // statistics.
        let q = EventQueue::new(Arc::clone(&hooks), ClientKind::Consumer);
        let woken = Arc::new(AtomicBool::new(false));
        let w = Arc::clone(&woken);
        q.set_wakeup(move || w.store(true, Ordering::SeqCst));
        q.stats(stats(1));
        let (e, reason) = error(1);
        q.error(e, &reason);
        q.stats(stats(2));
        assert!(woken.load(Ordering::SeqCst));
        assert!(events(&recorder).is_empty());
        assert!(q.has_pending());
        q.serve();
        assert_eq!(
            events(&recorder),
            [
                "error Global error: BrokerTransportFailure (Local: Broker transport failure): 1",
                "stats 2"
            ]
        );
        assert!(!q.has_pending());

        // The oldest errors are dropped beyond the bound.
        for n in 0..1005 {
            let (e, reason) = error(n);
            q.error(e, &reason);
        }
        q.serve();
        let delivered = events(&recorder);
        assert_eq!(delivered.len(), MAX_QUEUED_ERRORS);
        assert!(delivered[0].ends_with(": 5"));
    }

    /// A callback that polls (serves) the same queue again.
    struct Reentrant {
        queue: std::sync::OnceLock<std::sync::Weak<EventQueue>>,
        seen: Mutex<Vec<String>>,
    }

    impl ClientContext for Arc<Reentrant> {
        fn error(&self, _error: KafkaError, reason: &str) {
            self.seen.lock().unwrap().push(reason.to_string());
            if let Some(queue) = self.queue.get().and_then(|q| q.upgrade()) {
                // Re-entering does not deadlock; the outer call delivers
                // what is still queued.
                let pending = queue.has_pending();
                assert_eq!(queue.serve(), !pending);
            }
        }
    }

    #[mz_ore::test]
    fn reentrant_serve_does_not_deadlock() {
        let ctx = Arc::new(Reentrant {
            queue: std::sync::OnceLock::new(),
            seen: Mutex::new(Vec::new()),
        });
        let hooks: Arc<dyn Hooks> = Arc::new(Arc::clone(&ctx));
        let q = Arc::new(EventQueue::new(hooks, ClientKind::Consumer));
        ctx.queue.set(Arc::downgrade(&q)).unwrap();
        for n in 0..3 {
            let (e, reason) = error(n);
            q.error(e, &reason);
        }
        assert!(q.serve());
        assert_eq!(*ctx.seen.lock().unwrap(), ["0", "1", "2"]);
        assert!(!q.has_pending());
    }
}
