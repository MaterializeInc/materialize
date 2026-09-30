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
    Registered(Retire),
    Issued { retire: Retire, cancel: Cancel },
    Retired(PeekResponse),
}

/// A single retirement obligation shared with cancellation and compute.
///
/// Issue must run synchronously on the compute instance task. Its critical
/// section ensures cancellation either prevents issue or observes an issued
/// peek and sends cancellation after it. Neither issue nor the callbacks may
/// await. The issue closure must not reenter this handle.
#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub struct PeekLifecycle {
    #[derivative(Debug = "ignore")]
    state: Mutex<State>,
}

impl PeekLifecycle {
    /// Creates an unissued peek with one retirement obligation.
    pub fn new(retire: impl FnOnce(PeekNotification) + Send + 'static) -> Self {
        Self {
            state: Mutex::new(State::Registered(Box::new(retire))),
        }
    }

    /// Returns a terminal outcome without calling `issue` if already retired.
    /// A failed issue leaves retirement with the caller.
    pub fn issue<E>(
        &self,
        issue: impl FnOnce() -> Result<(), E>,
        cancel: impl FnOnce(PeekResponse) + Send + 'static,
    ) -> Result<Option<PeekResponse>, E> {
        let mut state = self.state.lock().expect("peek lifecycle lock poisoned");
        match &*state {
            State::Retired(outcome) => return Ok(Some(outcome.clone())),
            State::Issued { .. } => panic!("peek issued twice"),
            State::Registered(_) => {}
        }
        issue()?;
        let State::Registered(retire) =
            std::mem::replace(&mut *state, State::Retired(PeekResponse::Canceled))
        else {
            unreachable!("registration checked while holding the lock");
        };
        *state = State::Issued {
            retire,
            cancel: Box::new(cancel),
        };
        Ok(None)
    }

    /// Retires a controller completion once, without canceling compute again.
    pub fn complete(&self, outcome: PeekNotification) -> bool {
        self.retire(outcome, None)
    }

    /// Retires cancellation once and cancels compute only if issue succeeded.
    pub fn cancel(&self, response: PeekResponse) -> bool {
        assert!(matches!(
            &response,
            PeekResponse::Canceled | PeekResponse::Error(_)
        ));
        self.retire(PeekNotification::Canceled, Some(response))
    }

    fn retire(&self, outcome: PeekNotification, cancellation: Option<PeekResponse>) -> bool {
        let previous = {
            let mut state = self.state.lock().expect("peek lifecycle lock poisoned");
            if matches!(&*state, State::Retired(_)) {
                return false;
            }
            std::mem::replace(
                &mut *state,
                State::Retired(cancellation.clone().unwrap_or(PeekResponse::Canceled)),
            )
        };
        // Callbacks can remove registry entries and enqueue compute commands.
        // Keep both out of the lifecycle lock to avoid lock-order inversions.
        let retire = match previous {
            State::Registered(retire) => retire,
            State::Issued { retire, cancel } => {
                if let Some(response) = cancellation {
                    cancel(response);
                }
                retire
            }
            State::Retired(_) => unreachable!("terminal state checked under lock"),
        };
        retire(outcome);
        true
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
        assert!(lifecycle.cancel(PeekResponse::Canceled));
        let result = lifecycle.issue::<()>(
            || panic!("canceled peek issued"),
            |_| panic!("unissued peek canceled"),
        );
        assert_eq!(result, Ok(Some(PeekResponse::Canceled)));
        assert!(!lifecycle.complete(success()));
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
        assert!(lifecycle.cancel(PeekResponse::Canceled));
        assert!(!lifecycle.cancel(PeekResponse::Canceled));
        assert!(!lifecycle.complete(success()));
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
        assert!(lifecycle.complete(success()));
        assert!(!lifecycle.cancel(PeekResponse::Canceled));
        assert!(!lifecycle.complete(success()));
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
        assert!(lifecycle.complete(error.clone()));
        assert!(!lifecycle.cancel(PeekResponse::Canceled));
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
            let cancel = scope.spawn(|| lifecycle.cancel(PeekResponse::Canceled));
            release.wait();
            assert_eq!(issue.join().expect("test thread panicked"), Ok(None));
            assert!(cancel.join().expect("test thread panicked"));
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
        assert!(lifecycle.cancel(response.clone()));
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
