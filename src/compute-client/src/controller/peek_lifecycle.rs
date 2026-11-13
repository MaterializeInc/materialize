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
/// The owner must arrange completion or cancellation on every terminal path.
/// Dropping this handle does not invoke retirement. Callbacks run synchronously
/// and must not block waiting for the compute instance task.
///
/// # Panics
///
/// State-changing methods panic if the lifecycle lock is poisoned. Callback
/// panics propagate to the caller. Callbacks must not panic if retirement is
/// to be guaranteed: a panic after marking the lifecycle retired is not retried.
#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub struct PeekLifecycle {
    #[derivative(Debug = "ignore")]
    state: Mutex<State>,
}

impl PeekLifecycle {
    /// Creates an unissued peek whose terminal metadata is delivered to `retire`.
    ///
    /// The first completion or cancellation takes ownership of `retire` and
    /// invokes it outside the lifecycle lock, on the thread performing that
    /// operation. The callback receives execution metadata, not the query rows.
    pub fn new(retire: impl FnOnce(PeekNotification) + Send + 'static) -> Self {
        Self {
            state: Mutex::new(State::Registered(Box::new(retire))),
        }
    }

    /// Issues compute work unless retirement has already won.
    ///
    /// Call on the compute instance task. `issue` runs synchronously under the
    /// lifecycle lock and must not reenter this handle. It must return `Err`
    /// only if no compute peek was issued. On success, `cancel` is retained to
    /// enqueue cancellation on that task with the response supplied to
    /// [`Self::cancel`] if cancellation wins retirement.
    ///
    /// Returns `Ok(None)` after successful issue. If already retired, invokes
    /// neither callback and returns `Ok(Some(response))` for the caller to send
    /// through the peek's result channel. This is the saved cancellation response,
    /// or `PeekResponse::Canceled` if retirement came from [`Self::complete`].
    /// An issue error is returned unchanged and leaves the peek unissued, with
    /// retirement still owed by the caller.
    ///
    /// # Panics
    ///
    /// Panics if already issued but not retired. A panic in `issue` poisons the
    /// lifecycle lock.
    pub(super) fn issue<E>(
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

    /// Delivers terminal metadata to the retirement callback without canceling compute.
    ///
    /// `outcome` describes the controller's result, or an error that prevented
    /// issue. This method does not deliver a query result or clean up compute
    /// resources. The caller remains responsible for those.
    ///
    /// Returns `true` if this call performed retirement, or `false` if another
    /// completion or cancellation already retired the peek.
    pub fn complete(&self, outcome: PeekNotification) -> bool {
        self.retire(outcome, None)
    }

    /// Retires the peek as canceled and prevents or cancels compute issue.
    ///
    /// `response` is the requested cancellation result for the query, either
    /// `PeekResponse::Canceled` or `PeekResponse::Error`. In both cases the
    /// retirement callback receives `PeekNotification::Canceled`, so a
    /// dependency-drop error can be returned to the query but logged as cancellation.
    ///
    /// Before issue, saves `response` for the issue caller to deliver. After
    /// successful issue, passes it to the compute-cancellation callback before
    /// invoking retirement. Both callbacks run outside the lifecycle lock.
    /// This does not wait for compute cancellation: an issued peek can deliver
    /// its result before the queued cancellation is processed.
    ///
    /// Returns `true` if this call performed retirement. Returns `false` without
    /// invoking callbacks if the peek was already retired.
    ///
    /// # Panics
    ///
    /// Panics for any other response variant, even if already retired.
    pub fn cancel(&self, response: PeekResponse) -> bool {
        assert!(matches!(
            &response,
            PeekResponse::Canceled | PeekResponse::Error(_)
        ));
        self.retire(PeekNotification::Canceled, Some(response))
    }

    /// Claims retirement and delivers `outcome`, forwarding `cancellation` to
    /// compute only for an issued peek. Returns whether this call claimed retirement.
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
            let (locked_tx, locked_rx) = std::sync::mpsc::channel();
            let canceling = &lifecycle;
            let cancel = scope.spawn(move || {
                let locked = matches!(
                    canceling.state.try_lock(),
                    Err(std::sync::TryLockError::WouldBlock)
                );
                locked_tx.send(locked).expect("issue thread alive");
                canceling.cancel(PeekResponse::Canceled)
            });
            let locked = locked_rx.recv().expect("cancellation thread alive");
            release.wait();
            assert!(locked, "issue must hold the lifecycle lock");
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
