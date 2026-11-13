// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Ordering of compute issue, cancellation, and frontend-owned retirement.

use std::sync::Mutex;

use crate::controller::PeekNotification;
use crate::protocol::response::PeekResponse;

type Retire = Box<dyn FnOnce(PeekNotification) + Send>;
type Cancel = Box<dyn FnOnce(PeekResponse) + Send>;

enum State {
    /// `cancel` is set once compute work is issued.
    Pending {
        retire: Retire,
        cancel: Option<Cancel>,
    },
    /// Holds the response for an issue attempt that arrives after retirement.
    Retired(PeekResponse),
}

/// A peek's exactly-once retirement, ordered against compute issue and cancellation.
///
/// Dropping the handle does not retire the peek. The owner must call
/// [`Self::complete`] or [`Self::cancel`] on every path where compute will not.
/// Callbacks run synchronously on the retiring thread, which can be the compute
/// instance task, so they must not block on that task.
#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub struct PeekLifecycle {
    #[derivative(Debug = "ignore")]
    state: Mutex<State>,
}

impl PeekLifecycle {
    /// Creates an unissued peek that reports its terminal notification to `retire`.
    pub fn new(retire: impl FnOnce(PeekNotification) + Send + 'static) -> Self {
        Self {
            state: Mutex::new(State::Pending {
                retire: Box::new(retire),
                cancel: None,
            }),
        }
    }

    /// Runs `issue` unless the peek is already retired.
    ///
    /// `issue` runs under the lifecycle lock and must not call back into this
    /// handle. It must return `Err` only if no compute peek was issued. In that
    /// case the peek stays unissued and the caller still owes retirement.
    ///
    /// Returns `Ok(None)` once issued. A later [`Self::cancel`] then passes its
    /// response to `cancel`. If already retired, calls neither callback and
    /// returns the response to send on the peek's result channel.
    ///
    /// # Panics
    ///
    /// Panics if the peek is already issued.
    pub(super) fn issue<E>(
        &self,
        issue: impl FnOnce() -> Result<(), E>,
        cancel: impl FnOnce(PeekResponse) + Send + 'static,
    ) -> Result<Option<PeekResponse>, E> {
        let mut state = self.state.lock().expect("peek lifecycle lock poisoned");
        match &mut *state {
            State::Retired(response) => Ok(Some(response.clone())),
            State::Pending { cancel: slot, .. } => {
                assert!(slot.is_none(), "peek issued twice");
                issue()?;
                *slot = Some(Box::new(cancel));
                Ok(None)
            }
        }
    }

    /// Retires the peek with `outcome` without canceling compute. No-op if already
    /// retired.
    pub fn complete(&self, outcome: PeekNotification) {
        self.retire(outcome, None);
    }

    /// Retires the peek as canceled. No-op if already retired.
    ///
    /// `response` is what the query receives: `Canceled`, or `Error` for a dropped
    /// dependency. The retirement callback receives `PeekNotification::Canceled`
    /// in both cases. An unissued peek is never issued. An issued peek gets a
    /// queued compute cancellation and can still deliver its result first.
    ///
    /// # Panics
    ///
    /// Panics if `response` is neither `Canceled` nor `Error`.
    pub fn cancel(&self, response: PeekResponse) {
        assert!(
            matches!(&response, PeekResponse::Canceled | PeekResponse::Error(_)),
            "peek canceled with a non-cancellation response"
        );
        self.retire(PeekNotification::Canceled, Some(response));
    }

    /// Retires the peek with `outcome` unless already retired. A `cancellation`
    /// response additionally cancels compute if the peek was issued.
    fn retire(&self, outcome: PeekNotification, cancellation: Option<PeekResponse>) {
        let previous = {
            let mut state = self.state.lock().expect("peek lifecycle lock poisoned");
            if matches!(&*state, State::Retired(_)) {
                return;
            }
            let response = cancellation.clone().unwrap_or(PeekResponse::Canceled);
            std::mem::replace(&mut *state, State::Retired(response))
        };
        let State::Pending { retire, cancel } = previous else {
            unreachable!("retired state returns above");
        };
        // The callbacks remove registry entries and enqueue compute commands.
        // Running them under the lifecycle lock would invert lock order with
        // those structures.
        if let (Some(cancel), Some(response)) = (cancel, cancellation) {
            cancel(response);
        }
        retire(outcome);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier, Mutex};

    use crate::controller::PeekNotification;
    use crate::protocol::response::{PeekError, PeekResponse};

    use super::PeekLifecycle;

    fn success() -> PeekNotification {
        PeekNotification::Success {
            rows: 1,
            result_size: 8,
        }
    }

    #[mz_ore::test]
    fn cancellation_before_issue_prevents_issue() {
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&outcomes);
        let lifecycle = PeekLifecycle::new(move |outcome| {
            captured.lock().expect("test lock poisoned").push(outcome)
        });
        lifecycle.cancel(PeekResponse::Canceled);
        let result = lifecycle.issue::<()>(
            || panic!("canceled peek issued"),
            |_| panic!("unissued peek canceled"),
        );
        assert_eq!(result, Ok(Some(PeekResponse::Canceled)));
        lifecycle.complete(success());
        assert_eq!(
            *outcomes.lock().expect("test lock poisoned"),
            vec![PeekNotification::Canceled]
        );
    }

    #[mz_ore::test]
    fn cancellation_after_issue_cancels_compute_once() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let retired = Arc::clone(&events);
        let lifecycle =
            PeekLifecycle::new(move |_| retired.lock().expect("test lock poisoned").push("retire"));
        let issued = Arc::clone(&events);
        let canceled = Arc::clone(&events);
        assert_eq!(
            lifecycle.issue::<()>(
                move || {
                    issued.lock().expect("test lock poisoned").push("issue");
                    Ok(())
                },
                move |_| canceled.lock().expect("test lock poisoned").push("cancel")
            ),
            Ok(None)
        );
        lifecycle.cancel(PeekResponse::Canceled);
        lifecycle.cancel(PeekResponse::Canceled);
        lifecycle.complete(success());
        assert_eq!(
            *events.lock().expect("test lock poisoned"),
            vec!["issue", "cancel", "retire"]
        );
    }

    #[mz_ore::test]
    fn completion_prevents_late_cancellation() {
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&outcomes);
        let lifecycle = PeekLifecycle::new(move |outcome| {
            captured.lock().expect("test lock poisoned").push(outcome)
        });
        assert_eq!(
            lifecycle.issue::<()>(|| Ok(()), |_| panic!("completed peek canceled")),
            Ok(None)
        );
        lifecycle.complete(success());
        lifecycle.cancel(PeekResponse::Canceled);
        lifecycle.complete(success());
        assert_eq!(
            *outcomes.lock().expect("test lock poisoned"),
            vec![success()]
        );
    }

    #[mz_ore::test]
    fn failed_issue_preserves_retirement_obligation() {
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&outcomes);
        let lifecycle = PeekLifecycle::new(move |outcome| {
            captured.lock().expect("test lock poisoned").push(outcome)
        });
        assert_eq!(
            lifecycle.issue(|| Err("issue failed"), |_| panic!("failed peek canceled")),
            Err("issue failed")
        );
        let error = PeekNotification::Error("issue failed".into());
        lifecycle.complete(error.clone());
        lifecycle.cancel(PeekResponse::Canceled);
        assert_eq!(*outcomes.lock().expect("test lock poisoned"), vec![error]);
    }

    #[mz_ore::test]
    fn cancellation_racing_issue_is_ordered_after_issue() {
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let events = Arc::new(Mutex::new(Vec::new()));
        let retired = Arc::clone(&events);
        let lifecycle = Arc::new(PeekLifecycle::new(move |_| {
            retired.lock().expect("test lock poisoned").push("retire")
        }));
        std::thread::scope(|scope| {
            let issuing = Arc::clone(&lifecycle);
            let entered_issue = Arc::clone(&entered);
            let release_issue = Arc::clone(&release);
            let issued = Arc::clone(&events);
            let canceled = Arc::clone(&events);
            let issue = scope.spawn(move || {
                issuing.issue::<()>(
                    move || {
                        entered_issue.wait();
                        release_issue.wait();
                        issued.lock().expect("test lock poisoned").push("issue");
                        Ok(())
                    },
                    move |_| canceled.lock().expect("test lock poisoned").push("cancel"),
                )
            });
            entered.wait();
            let (locked_tx, locked_rx) = std::sync::mpsc::channel();
            let canceling = &lifecycle;
            let cancel = scope.spawn(move || {
                let locked = matches!(
                    canceling.state.try_lock(),
                    Err(std::sync::TryLockError::WouldBlock)
                );
                locked_tx.send(locked).expect("issue thread alive");
                canceling.cancel(PeekResponse::Canceled);
            });
            let locked = locked_rx.recv().expect("cancellation thread alive");
            release.wait();
            assert!(locked, "issue must hold the lifecycle lock");
            assert_eq!(issue.join().expect("test thread panicked"), Ok(None));
            cancel.join().expect("test thread panicked");
        });
        assert_eq!(
            *events.lock().expect("test lock poisoned"),
            vec!["issue", "cancel", "retire"]
        );
    }

    #[mz_ore::test]
    fn dependency_drop_returns_error_but_logs_cancellation() {
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&outcomes);
        let lifecycle = PeekLifecycle::new(move |outcome| {
            captured.lock().expect("test lock poisoned").push(outcome)
        });
        let response = PeekResponse::Error(PeekError::unstructured("dependency dropped"));
        lifecycle.cancel(response.clone());
        assert_eq!(
            lifecycle.issue::<()>(
                || panic!("dropped peek issued"),
                |_| panic!("unissued peek canceled")
            ),
            Ok(Some(response))
        );
        assert_eq!(
            *outcomes.lock().expect("test lock poisoned"),
            vec![PeekNotification::Canceled]
        );
    }
}
