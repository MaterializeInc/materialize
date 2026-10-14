// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Completion of read-only transactions whose resources belong to the session.

use std::sync::Arc;
use std::time::Duration;

use mz_repr::Timestamp;
use mz_sql::session::vars::{EndTransactionAction, IsolationLevel};
use mz_timestamp_oracle::TimestampOracle;
use tokio::sync::watch;

use crate::session::{ReadOnlyCompletion, Session};
use crate::{AdapterError, ExecuteResponse, PeekClient};

pub(crate) async fn complete_read_only_transaction(
    session: &mut Session,
    client: &mut PeekClient,
    cancel: &mut watch::Receiver<()>,
    completion: ReadOnlyCompletion,
) -> Result<ExecuteResponse, AdapterError> {
    let phases = Arc::clone(&client.coordinator_client().metrics().qps);
    let completion_timer = phases.local_completion_total.start();
    // Cancellation while idle must not poison a later completion. Keep the session
    // intact across awaits so dropping this future still leaves teardown with its owner.
    cancel.borrow_and_update();
    client
        .coordinator_client()
        .metrics()
        .frontend_transaction_completions
        .inc();
    let result = async {
        session.apply_external_metadata_updates();
        let catalog = phases
            .local_completion_catalog
            .time(client.catalog_snapshot("end_transaction"))
            .await;
        let roles_timer = phases.local_completion_roles.start();
        mz_sql::rbac::check_session_roles(&catalog.for_session(session), session)?;
        roles_timer.finish();

        if let Some(context) = &completion.timestamp {
            match session.vars().transaction_isolation() {
                IsolationLevel::StrictSerializable => {
                    if let Some((timeline, timestamp)) = context.timestamp_to_linearize() {
                        client
                            .coordinator_client()
                            .metrics()
                            .frontend_transaction_waits
                            .inc();
                        phases
                            .local_completion_oracle
                            .time(async {
                                let oracle = client.ensure_oracle(timeline.clone()).await?;
                                wait_for_read_timestamp(&**oracle, *timestamp, cancel).await
                            })
                            .await?;
                    }
                }
                IsolationLevel::StrongSessionSerializable => {
                    if let Some((timeline, timestamp)) = context.timeline_timestamp() {
                        session
                            .ensure_timestamp_oracle(timeline.clone())
                            .apply_write(*timestamp);
                    }
                }
                IsolationLevel::ReadUncommitted
                | IsolationLevel::ReadCommitted
                | IsolationLevel::RepeatableRead
                | IsolationLevel::Serializable
                | IsolationLevel::BoundedStaleness(_) => {}
            }
        }
        Ok::<_, AdapterError>(())
    }
    .await;

    let cleanup_timer = phases.local_completion_cleanup.start();
    let action = if result.is_ok() {
        completion.action
    } else {
        EndTransactionAction::Rollback
    };
    let _ = session.clear_transaction();
    let params = session.vars_mut().end_transaction(action);
    cleanup_timer.finish();
    completion_timer.finish();
    result?;
    Ok(match action {
        EndTransactionAction::Commit => ExecuteResponse::TransactionCommitted { params },
        EndTransactionAction::Rollback => ExecuteResponse::TransactionRolledBack { params },
    })
}

async fn wait_for_read_timestamp(
    oracle: &(dyn TimestampOracle<Timestamp> + Send + Sync),
    timestamp: Timestamp,
    cancel: &mut watch::Receiver<()>,
) -> Result<(), AdapterError> {
    let wait = async {
        loop {
            let current = oracle.read_ts().await;
            if timestamp <= current {
                break;
            }
            let delay = Duration::from_millis(timestamp.saturating_sub(current).into());
            tokio::time::sleep(delay.min(Duration::from_secs(1))).await;
        }
    };
    tokio::select! {
        () = wait => Ok(()),
        _ = cancel.changed() => Err(AdapterError::Canceled),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use futures::FutureExt;
    use mz_timestamp_oracle::WriteTimestamp;

    #[derive(Debug, Default)]
    struct ReadOracle {
        timestamp: AtomicU64,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl TimestampOracle<Timestamp> for ReadOracle {
        async fn read_ts(&self) -> Timestamp {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.timestamp.load(Ordering::SeqCst).into()
        }
        async fn write_ts(&self) -> WriteTimestamp {
            unreachable!("read-only completion")
        }
        async fn peek_write_ts(&self) -> Timestamp {
            unreachable!("must use a linearized read")
        }
        async fn apply_write(&self, _: Timestamp) {
            unreachable!("read-only completion")
        }
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn completion_waits_for_the_shared_oracle() {
        let oracle = ReadOracle::default();
        let (_tx, mut cancel) = watch::channel(());
        let wait = wait_for_read_timestamp(&oracle, 10.into(), &mut cancel);
        tokio::pin!(wait);
        assert!(wait.as_mut().now_or_never().is_none());
        assert_eq!(oracle.calls.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_millis(10)).await;
        assert!(
            wait.as_mut().now_or_never().is_none(),
            "wall time alone cannot release the read"
        );
        oracle.timestamp.store(10, Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(10)).await;
        assert!(wait.await.is_ok());
    }

    #[mz_ore::test(tokio::test)]
    async fn completion_wait_is_cancellable_and_old_cancels_are_ignored() {
        let oracle = ReadOracle::default();
        let (tx, mut cancel) = watch::channel(());
        tx.send_replace(());
        cancel.borrow_and_update();
        {
            let wait = wait_for_read_timestamp(&oracle, 10.into(), &mut cancel);
            tokio::pin!(wait);
            assert!(wait.as_mut().now_or_never().is_none());
            tx.send_replace(());
            assert!(matches!(wait.await, Err(AdapterError::Canceled)));
        }
        cancel.borrow_and_update();
        oracle.timestamp.store(10, Ordering::SeqCst);
        assert!(
            wait_for_read_timestamp(&oracle, 10.into(), &mut cancel)
                .await
                .is_ok()
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn completion_wait_ends_when_the_connection_is_gone() {
        let oracle = ReadOracle::default();
        let (tx, mut cancel) = watch::channel(());
        drop(tx);
        assert!(matches!(
            wait_for_read_timestamp(&oracle, 10.into(), &mut cancel).await,
            Err(AdapterError::Canceled)
        ));
    }
}
