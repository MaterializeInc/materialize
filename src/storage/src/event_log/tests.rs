// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
fn events_logged_before_drop_are_delivered_and_the_dataflow_shuts_down() {
    timely::execute::execute(
        timely::execute::Config {
            communication: timely::CommunicationConfig::Process(2),
            worker: Default::default(),
        },
        |worker| {
            let received: Rc<RefCell<Vec<usize>>> = Default::default();
            let consumer_received = Rc::clone(&received);
            let dataflow = render_event_log(
                worker,
                "test",
                "test/event-log",
                // Every event goes to worker 0.
                |_event: &usize| 0,
                move |mut events, _activator| consumer_received.borrow_mut().append(&mut events),
            );
            let logger = event_logger::<usize>(worker, "test/event-log").unwrap();

            // Logged between steps, so no step flushed the logger before the drop.
            logger.log(worker.index());
            drop(dataflow);
            logger.log(worker.index() + 100);

            // The dataflow shutting down on every worker ends this loop.
            for _ in 0..1000 {
                if !worker.has_dataflows() {
                    break;
                }
                worker.step_or_park(Some(Duration::from_millis(1)));
            }
            assert!(
                !worker.has_dataflows(),
                "the event log dataflow must shut down"
            );

            let mut received = received.borrow().clone();
            received.sort();
            if worker.index() == 0 {
                assert_eq!(received, vec![0, 1]);
            } else {
                assert!(received.is_empty());
            }
        },
    )
    .unwrap();
}
