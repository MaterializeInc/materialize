// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Idempotence and transactions, following librdkafka 2.12's
//! `rdkafka_idempotence.c` and `rdkafka_txnmgr.c`: the same states, the same
//! transitions, the same error classification and texts.
//!
//! The transactional API calls run on the caller's thread and wait on the
//! producer's condition variable. Network work runs on one background task
//! per idempotent producer ([`ProducerInner::eos_task`]): acquiring or
//! bumping the producer id, AddPartitionsToTxn, and EndTxn. An API call that
//! times out leaves that work running and returns a retriable `__TIMED_OUT`;
//! calling the same API again resumes it, as in librdkafka.
//!
//! Intentional differences from librdkafka:
//!
//! * Sequence numbers are assigned when a batch is first sent, not when a
//!   record is enqueued, so records that time out or are purged before
//!   being sent leave no gap and need no epoch bump. A batch that fails after
//!   being sent still requires one.
//! * Purges (abort, abortable and fatal errors) only remove batches that
//!   were never sent. Batches waiting to be retried are retried until they
//!   are delivered or fail, like in-flight ones, so that sequence numbers
//!   stay consistent with the broker's.
//! * `OUT_OF_ORDER_SEQUENCE_NUMBER` for a batch sent after an earlier batch
//!   of the same partition failed with a retriable error is retried without
//!   an epoch bump (librdkafka bumps the epoch, which makes a transaction
//!   abortable). The broker rejected it only because the earlier batch is
//!   missing, and the retries resend both in order with their original
//!   sequence numbers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, MutexGuard};
use std::time::{Duration, Instant};

use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_protocol::messages::{
    AddPartitionsToTxnRequest, EndTxnRequest, InitProducerIdRequest, ProducerId, TopicName,
    TransactionalId,
};
use kafka_protocol::protocol::StrBytes;

use crate::config::RDKafkaLogLevel;
use crate::config::settings::DebugContext;
use crate::error::{KafkaError, KafkaResult, RDKafkaError};
use crate::internal::cluster::{Broker, CoordinatorKind};
use crate::internal::net::{NetError, RequestOptions};
use crate::internal::protocol;
use crate::internal::records::ProducerIdentity;
use crate::producer::ProducerContext;
use crate::types::RDKafkaErrorCode;
use crate::util::{Deadline, Timeout};

use super::ProducerInner;
use super::state::{Effects, PartKey, PartTxn, State};

/// librdkafka's idempotent producer states (`rd_kafka_idemp_state_t`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum IdempState {
    Init,
    FatalError,
    RequestPid,
    WaitPid,
    Assigned,
    DrainReset,
    DrainBump,
    WaitTxnAbort,
}

impl IdempState {
    pub fn name(self) -> &'static str {
        match self {
            IdempState::Init => "Init",
            IdempState::FatalError => "FatalError",
            IdempState::RequestPid => "RequestPID",
            IdempState::WaitPid => "WaitPID",
            IdempState::Assigned => "Assigned",
            IdempState::DrainReset => "DrainReset",
            IdempState::DrainBump => "DrainBump",
            IdempState::WaitTxnAbort => "WaitTxnAbort",
        }
    }
}

/// librdkafka's transaction states (`rd_kafka_txn_state_t`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TxnState {
    Init,
    WaitPid,
    ReadyNotAcked,
    Ready,
    InTransaction,
    BeginCommit,
    CommittingTransaction,
    CommitNotAcked,
    BeginAbort,
    AbortingTransaction,
    AbortNotAcked,
    AbortableError,
    FatalError,
}

impl TxnState {
    pub fn name(self) -> &'static str {
        match self {
            TxnState::Init => "Init",
            TxnState::WaitPid => "WaitPID",
            TxnState::ReadyNotAcked => "ReadyNotAcked",
            TxnState::Ready => "Ready",
            TxnState::InTransaction => "InTransaction",
            TxnState::BeginCommit => "BeginCommit",
            TxnState::CommittingTransaction => "CommittingTransaction",
            TxnState::CommitNotAcked => "CommitNotAcked",
            TxnState::BeginAbort => "BeginAbort",
            TxnState::AbortingTransaction => "AbortingTransaction",
            TxnState::AbortNotAcked => "AbortedNotAcked",
            TxnState::AbortableError => "AbortableError",
            TxnState::FatalError => "FatalError",
        }
    }

    /// librdkafka's `rd_kafka_txn_state_transition_is_valid`: `Some(true)`
    /// for a valid transition, `Some(false)` for one that is ignored, `None`
    /// for an invalid one.
    pub(super) fn transition(self, to: TxnState) -> Option<bool> {
        use TxnState::*;
        let valid = match to {
            Init => false,
            WaitPid => self == Init,
            ReadyNotAcked => self == WaitPid,
            Ready => matches!(self, ReadyNotAcked | CommitNotAcked | AbortNotAcked),
            InTransaction => self == Ready,
            BeginCommit => self == InTransaction,
            CommittingTransaction => self == BeginCommit,
            CommitNotAcked => matches!(self, BeginCommit | CommittingTransaction),
            BeginAbort => matches!(self, InTransaction | AbortingTransaction | AbortableError),
            AbortingTransaction => self == BeginAbort,
            AbortNotAcked => matches!(self, BeginAbort | AbortingTransaction),
            AbortableError => {
                if matches!(self, BeginAbort | AbortingTransaction | FatalError) {
                    return Some(false);
                }
                matches!(self, InTransaction | BeginCommit | CommittingTransaction)
            }
            FatalError => true,
        };
        valid.then_some(true)
    }
}

/// The transactional API call in progress, librdkafka's `txn_curr_api`.
#[derive(Debug, Default)]
pub(super) struct CurrentApi {
    pub name: Option<&'static str>,
    /// Whether a thread is inside the call (as opposed to a call that timed
    /// out and may be resumed).
    pub calling: bool,
    /// The result of the background operation, `Some(None)` for success.
    pub result: Option<Option<RDKafkaError>>,
}

/// Idempotence and transaction state.
pub(super) struct Eos {
    pub idempotent: bool,
    pub transactional: bool,
    pub pid: ProducerIdentity,
    pub idemp_state: IdempState,
    pub idemp_since: Instant,
    /// The number of producer ids or epochs acquired.
    pub epoch_cnt: i64,
    pub txn_state: TxnState,
    pub txn_since: Instant,
    /// The error that made the transaction abortable or the producer fail.
    pub txn_err: Option<(RDKafkaErrorCode, String)>,
    pub requires_epoch_bump: bool,
    /// AddPartitionsToTxn requests sent in this transaction. Without any,
    /// commit and abort need no EndTxn.
    pub req_cnt: u32,
    /// Messages of this transaction that failed delivery.
    pub dr_fails: u64,
    /// The last InitProducerId error, reported if `init_transactions` times
    /// out.
    pub init_err: Option<RDKafkaErrorCode>,
    /// Whether `send` may enqueue: only in a transaction.
    pub may_enq: bool,
    /// Partitions to add to the transaction.
    pub pending: BTreeSet<PartKey>,
    /// Partitions in the outstanding AddPartitionsToTxn request.
    pub registering: Vec<PartKey>,
    /// An EndTxn to send: `Some(true)` to commit.
    pub end_txn: Option<bool>,
    pub api: CurrentApi,
    /// The fatal error, once raised. The producer is unusable afterwards.
    pub fatal: Option<(RDKafkaErrorCode, String)>,
    /// When to (re)request a producer id.
    pub pid_at: Option<Instant>,
    /// When to (re)send AddPartitionsToTxn.
    pub register_at: Option<Instant>,
    /// The last backoff after `CONCURRENT_TRANSACTIONS` from AddPartitionsToTxn.
    pub concurrent_backoff: Option<Duration>,
}

impl Eos {
    pub fn new(idempotent: bool, transactional: bool, now: Instant) -> Eos {
        Eos {
            idempotent,
            transactional,
            pid: ProducerIdentity::NONE,
            // An idempotent producer acquires its id right away; a
            // transactional one in `init_transactions`.
            idemp_state: if idempotent && !transactional {
                IdempState::RequestPid
            } else {
                IdempState::Init
            },
            idemp_since: now,
            epoch_cnt: 0,
            txn_state: TxnState::Init,
            txn_since: now,
            txn_err: None,
            requires_epoch_bump: false,
            req_cnt: 0,
            dr_fails: 0,
            init_err: None,
            may_enq: !transactional,
            pending: BTreeSet::new(),
            registering: Vec::new(),
            end_txn: None,
            api: CurrentApi::default(),
            fatal: None,
            pid_at: (idempotent && !transactional).then_some(now),
            register_at: None,
            concurrent_backoff: None,
        }
    }

    /// Whether batches may be sent: the producer id is assigned, or the
    /// producer is not idempotent.
    pub fn may_send(&self) -> bool {
        self.fatal.is_none() && (!self.idempotent || self.idemp_state == IdempState::Assigned)
    }

    fn set_idemp_state(&mut self, state: IdempState) {
        if self.idemp_state != state {
            self.idemp_state = state;
            self.idemp_since = Instant::now();
        }
    }

    /// librdkafka's `rd_kafka_txn_set_state`. Invalid transitions are bugs
    /// in librdkafka; here they are ignored.
    fn set_txn_state(&mut self, state: TxnState) {
        if self.txn_state == state {
            return;
        }
        match self.txn_state.transition(state) {
            Some(true) => {}
            Some(false) => return,
            None => {
                debug_assert!(
                    false,
                    "invalid transaction state transition {:?} -> {:?}",
                    self.txn_state, state
                );
                return;
            }
        }
        if self.txn_state == TxnState::InTransaction {
            self.may_enq = false;
        } else if state == TxnState::InTransaction {
            self.may_enq = true;
        }
        self.txn_state = state;
        self.txn_since = Instant::now();
    }

    /// librdkafka's `rd_kafka_txn_require_state`.
    fn require(&self, states: &[TxnState]) -> Result<(), RDKafkaError> {
        if states.contains(&self.txn_state) {
            return Ok(());
        }
        Err(self.state_error())
    }

    /// The error for an API call in the wrong state: the stored error in the
    /// fatal and abortable states, else `__STATE`.
    fn state_error(&self) -> RDKafkaError {
        match (self.txn_state, &self.txn_err) {
            (TxnState::FatalError, Some((code, msg))) => {
                RDKafkaError::new(*code, msg.clone()).mark_fatal()
            }
            (TxnState::AbortableError, Some((code, msg))) => {
                RDKafkaError::new(*code, msg.clone()).mark_txn_requires_abort()
            }
            (state, _) => RDKafkaError::new(
                RDKafkaErrorCode::State,
                format!("Operation not valid in state {}", state.name()),
            ),
        }
    }

    /// librdkafka's `rd_kafka_txn_curr_api_set_result`: stores the result
    /// of the current API call, unless none is in progress or an error is
    /// already stored. Wakes the waiting caller.
    fn set_result(&mut self, error: Option<RDKafkaError>) {
        if self.api.name.is_none() {
            return;
        }
        if matches!(self.api.result, Some(Some(_))) {
            return;
        }
        self.api.result = Some(error);
    }
}

/// The error flags librdkafka derives from its error actions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Retry,
    Permanent,
    Fatal,
}

fn flag(error: RDKafkaError, action: Option<Action>) -> RDKafkaError {
    match action {
        Some(Action::Fatal) => error.mark_fatal(),
        Some(Action::Permanent) => error.mark_txn_requires_abort(),
        Some(Action::Retry) => error.mark_retriable(),
        None => error,
    }
}

/// librdkafka's `rd_kafka_txn_normalize_err`.
fn normalize(code: RDKafkaErrorCode) -> RDKafkaErrorCode {
    match code {
        RDKafkaErrorCode::InvalidProducerEpoch | RDKafkaErrorCode::ProducerFenced => {
            RDKafkaErrorCode::Fenced
        }
        RDKafkaErrorCode::TimedOutQueue => RDKafkaErrorCode::OperationTimedOut,
        code => code,
    }
}

fn pid_string(pid: ProducerIdentity) -> String {
    if pid.producer_id < 0 || pid.producer_epoch < 0 {
        "PID{Invalid}".to_string()
    } else {
        format!("PID{{Id:{},Epoch:{}}}", pid.producer_id, pid.producer_epoch)
    }
}

pub(super) fn pid_display(pid: ProducerIdentity) -> String {
    pid_string(pid)
}

impl<D> State<D> {
    /// Raises a fatal error, librdkafka's `rd_kafka_set_fatal_error0` (and
    /// `rd_kafka_txn_set_fatal_error` for transactional producers): every
    /// later call fails, records never sent are purged, and the error
    /// callback receives `__FATAL`.
    pub fn set_fatal(&mut self, code: RDKafkaErrorCode, reason: String, fx: &mut Effects<D>) {
        if self.eos.fatal.is_some() {
            return;
        }
        if self.eos.transactional {
            fx.log(
                RDKafkaLogLevel::Alert,
                "TXNERR",
                format!("Fatal transaction error: {reason} ({})", code.name()),
            );
        }
        let text = format!("Fatal error: {}: {reason}", code.err2str());
        fx.log(RDKafkaLogLevel::Emerg, "FATAL", text.clone());
        fx.errors
            .push((KafkaError::Global(RDKafkaErrorCode::Fatal), text));
        self.eos.fatal = Some((code, reason.clone()));
        self.eos.set_idemp_state(IdempState::FatalError);
        if self.eos.transactional {
            self.eos.txn_err = Some((code, reason.clone()));
            self.eos.set_txn_state(TxnState::FatalError);
            self.eos
                .set_result(Some(RDKafkaError::new(code, reason).mark_fatal()));
        }
        self.purge_unsent(fx);
        fx.wake_all = true;
        fx.wake_eos = true;
    }

    /// Makes the current transaction abortable, librdkafka's
    /// `rd_kafka_txn_set_abortable_error0`. Records never sent are purged.
    pub fn set_abortable(
        &mut self,
        code: RDKafkaErrorCode,
        requires_epoch_bump: bool,
        reason: String,
        fx: &mut Effects<D>,
    ) {
        if self.eos.fatal.is_some() || !self.eos.transactional {
            return;
        }
        if requires_epoch_bump {
            self.eos.requires_epoch_bump = true;
        }
        if self.eos.txn_err.is_some() {
            return;
        }
        if self.eos.txn_state.transition(TxnState::AbortableError) != Some(true) {
            return;
        }
        fx.log(
            RDKafkaLogLevel::Error,
            "TXNERR",
            format!(
                "Current transaction failed in state {}: {reason} ({}{})",
                self.eos.txn_state.name(),
                code.name(),
                if requires_epoch_bump {
                    ", requires epoch bump"
                } else {
                    ""
                }
            ),
        );
        self.eos.txn_err = Some((code, reason));
        self.eos.set_txn_state(TxnState::AbortableError);
        self.purge_unsent(fx);
    }

    /// Starts an epoch bump once all in-flight batches are answered,
    /// librdkafka's `rd_kafka_idemp_drain_epoch_bump0`. A transactional
    /// producer, if `allow_txn_abort`, first needs the transaction aborted.
    pub fn drain_epoch_bump(
        &mut self,
        allow_txn_abort: bool,
        code: RDKafkaErrorCode,
        reason: String,
        fx: &mut Effects<D>,
    ) {
        if self.eos.fatal.is_some() {
            return;
        }
        if allow_txn_abort && self.eos.transactional {
            self.eos.set_idemp_state(IdempState::WaitTxnAbort);
            self.set_abortable(code, true, reason, fx);
        } else {
            self.eos.set_idemp_state(IdempState::DrainBump);
            fx.wake_eos = true;
        }
    }

    /// Completes a drain once nothing is in flight, librdkafka's
    /// `rd_kafka_idemp_drain_done`.
    fn drain_done(&mut self, fx: &mut Effects<D>) {
        match self.eos.idemp_state {
            IdempState::DrainReset => {
                self.eos.set_idemp_state(IdempState::RequestPid);
                self.eos.pid_at = Some(Instant::now());
            }
            IdempState::DrainBump if self.eos.pid.producer_id >= 0 => {
                if self.eos.transactional || self.eos.pid.producer_epoch == i16::MAX {
                    // The coordinator bumps the epoch of a transactional
                    // producer; an exhausted epoch needs a new id.
                    self.eos.set_idemp_state(IdempState::RequestPid);
                    self.eos.pid_at = Some(Instant::now());
                } else {
                    let pid = ProducerIdentity {
                        producer_id: self.eos.pid.producer_id,
                        producer_epoch: self.eos.pid.producer_epoch + 1,
                    };
                    self.assign_pid(pid, fx);
                }
            }
            _ => {}
        }
    }

    /// Takes a new producer id or epoch into use: sequence numbers restart
    /// at 0 and batches awaiting retry are restamped when sent.
    fn assign_pid(&mut self, pid: ProducerIdentity, fx: &mut Effects<D>) {
        self.eos.pid = pid;
        self.eos.epoch_cnt += 1;
        for topic in &mut self.topics {
            for p in &mut topic.partitions {
                p.next_seq = 0;
                p.next_ack_seq = 0;
                p.retry_seq = None;
                p.acked = false;
            }
        }
        self.eos.set_idemp_state(IdempState::Assigned);
        self.idemp_state_change(fx);
        fx.wake_all = true;
    }

    /// librdkafka's `rd_kafka_txn_idemp_state_change`.
    fn idemp_state_change(&mut self, _fx: &mut Effects<D>) {
        let eos = &mut self.eos;
        if eos.idemp_state == IdempState::Assigned && eos.txn_state == TxnState::WaitPid {
            eos.set_txn_state(TxnState::ReadyNotAcked);
            eos.set_result(None);
        } else if eos.idemp_state == IdempState::Assigned
            && matches!(
                eos.txn_state,
                TxnState::BeginAbort | TxnState::AbortingTransaction
            )
        {
            eos.set_txn_state(TxnState::AbortNotAcked);
            eos.set_result(None);
        }
    }

    /// librdkafka's `rd_kafka_txn_complete`.
    fn txn_complete(&mut self) {
        self.clear_txn_partitions(false);
        self.eos.requires_epoch_bump = false;
        self.eos.req_cnt = 0;
        self.eos.set_txn_state(TxnState::Ready);
    }

    /// librdkafka's `rd_kafka_txn_endtxn_complete`.
    fn endtxn_complete(&mut self, fx: &mut Effects<D>) {
        let is_commit = self.eos.api.name == Some("commit_transaction");
        if self.eos.requires_epoch_bump && !is_commit {
            let code = self
                .eos
                .txn_err
                .as_ref()
                .map_or(RDKafkaErrorCode::NoError, |(c, _)| *c);
            self.drain_epoch_bump(false, code, "Transaction aborted".to_string(), fx);
            return;
        }
        self.eos.set_txn_state(if is_commit {
            TxnState::CommitNotAcked
        } else {
            TxnState::AbortNotAcked
        });
        self.eos.set_result(None);
    }
}

/// What the background task does next.
enum EosWork {
    Pid,
    Register(Vec<PartKey>),
    EndTxn(bool),
}

/// The outcome of one InitProducerId attempt.
enum PidOutcome {
    Assigned(ProducerIdentity),
    /// The coordinator lookup failed.
    Coordinator(NetError),
    /// The request failed; the broker's name for the error text.
    Failed(RDKafkaErrorCode, String),
}

/// The retry backoff of AddPartitionsToTxn and the producer id request.
const PID_RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// The backoff range after `CONCURRENT_TRANSACTIONS` from AddPartitionsToTxn,
/// which Kafka returns while it completes the previous transaction.
const CONCURRENT_RETRY_BACKOFF_MIN: Duration = Duration::from_millis(1);
const CONCURRENT_RETRY_BACKOFF_MAX: Duration = Duration::from_millis(20);

impl<C: ProducerContext> ProducerInner<C> {
    fn ensure_transactional(&self) -> Result<(), RDKafkaError> {
        if self.config.transactional_id.is_none() {
            return Err(RDKafkaError::new(
                RDKafkaErrorCode::NotConfigured,
                "The Transactional API requires transactional.id to be configured",
            ));
        }
        Ok(())
    }

    /// librdkafka's `rd_kafka_txn_curr_api_begin`.
    fn api_begin(&self, name: &'static str) -> Result<(), RDKafkaError> {
        self.ensure_transactional()?;
        let mut st = self.lock();
        let api = &mut st.eos.api;
        match api.name {
            Some(current) if current != name => Err(RDKafkaError::new(
                RDKafkaErrorCode::Conflict,
                format!("Conflicting {current} API call is already in progress"),
            )
            .mark_retriable()),
            Some(current) if api.calling => Err(RDKafkaError::new(
                RDKafkaErrorCode::PreviousInProgress,
                format!("Simultaneous {current} API calls not allowed"),
            )
            .mark_retriable()),
            Some(_) => {
                api.calling = true;
                Ok(())
            }
            None => {
                api.name = Some(name);
                api.calling = true;
                api.result = None;
                Ok(())
            }
        }
    }

    /// librdkafka's `rd_kafka_txn_curr_api_return`: a resumable call that
    /// fails retriably stays current, so that calling it again resumes it.
    fn api_return(&self, resumable: bool, result: Result<(), RDKafkaError>) -> KafkaResult<()> {
        let mut st = self.lock();
        let api = &mut st.eos.api;
        api.calling = false;
        let keep = resumable
            && result
                .as_ref()
                .err()
                .is_some_and(RDKafkaError::is_retriable);
        if !keep {
            api.name = None;
            api.result = None;
        }
        result.map_err(KafkaError::Transaction)
    }

    /// librdkafka's `rd_kafka_txn_op_req`: returns a result that arrived
    /// while no caller waited, or runs `op` and waits for its result until
    /// `deadline`.
    fn op_req(
        &self,
        op: impl FnOnce(&mut State<C::DeliveryOpaque>, &mut Effects<C::DeliveryOpaque>),
        deadline: Deadline,
    ) -> Result<(), RDKafkaError> {
        let mut st = self.lock();
        if let Some(result) = st.eos.api.result.take() {
            return result.map_or(Ok(()), Err);
        }
        let mut fx = Effects::default();
        op(&mut st, &mut fx);
        self.finish(st, fx);
        let st = self.lock();
        let mut st = self.wait_until(st, deadline, |st| st.eos.api.result.is_some());
        match st.eos.api.result.take() {
            Some(result) => result.map_or(Ok(()), Err),
            None => Err(RDKafkaError::new(
                RDKafkaErrorCode::OperationTimedOut,
                "Timed out waiting for operation to finish, retry call to resume",
            )
            .mark_retriable()),
        }
    }

    pub(crate) fn init_transactions(&self, timeout: Timeout) -> KafkaResult<()> {
        // Like librdkafka, an infinite timeout is capped, since there is no
        // coordinator yet to time out the call.
        let timeout = match timeout {
            Timeout::Never => Timeout::After(self.config.transaction_timeout * 2),
            t @ Timeout::After(_) => t,
        };
        let deadline = Deadline::new(timeout);
        self.api_begin("init_transactions")
            .map_err(KafkaError::Transaction)?;
        let result = self.op_req(
            |st, fx| {
                let eos = &mut st.eos;
                match eos.require(&[TxnState::Init, TxnState::WaitPid, TxnState::ReadyNotAcked]) {
                    Err(e) => eos.set_result(Some(e)),
                    Ok(()) if eos.txn_state == TxnState::ReadyNotAcked => {
                        eos.set_result(None);
                    }
                    Ok(()) => {
                        if eos.txn_state == TxnState::Init {
                            eos.set_txn_state(TxnState::WaitPid);
                        }
                        eos.init_err = None;
                        if matches!(eos.idemp_state, IdempState::Init | IdempState::RequestPid) {
                            eos.set_idemp_state(IdempState::RequestPid);
                            eos.pid_at = Some(Instant::now());
                            fx.wake_eos = true;
                        }
                    }
                }
            },
            deadline,
        );
        if let Err(e) = result {
            let e = if e.code() == RDKafkaErrorCode::OperationTimedOut {
                // A more meaningful InitProducerId error, if any.
                let init_err = self.lock().eos.init_err.map(normalize);
                match init_err {
                    Some(code) if code != RDKafkaErrorCode::OperationTimedOut => RDKafkaError::new(
                        code,
                        format!("Failed to initialize Producer ID: {}", code.err2str()),
                    )
                    .mark_retriable(),
                    _ => e,
                }
            } else {
                e
            };
            return self.api_return(true, Err(e));
        }
        let result = self.op_req(
            |st, _| {
                let eos = &mut st.eos;
                match eos.require(&[TxnState::ReadyNotAcked]) {
                    Ok(()) => {
                        eos.set_txn_state(TxnState::Ready);
                        eos.set_result(None);
                    }
                    Err(e) => eos.set_result(Some(e)),
                }
            },
            Deadline::never(),
        );
        self.api_return(false, result)
    }

    pub(crate) fn begin_transaction(&self) -> KafkaResult<()> {
        self.api_begin("begin_transaction")
            .map_err(KafkaError::Transaction)?;
        let result = self.op_req(
            |st, fx| {
                let eos = &mut st.eos;
                match eos.require(&[TxnState::Ready]) {
                    Ok(()) => {
                        eos.set_txn_state(TxnState::InTransaction);
                        eos.dr_fails = 0;
                        eos.txn_err = None;
                        fx.wake_all = true;
                        eos.set_result(None);
                    }
                    Err(e) => eos.set_result(Some(e)),
                }
            },
            Deadline::never(),
        );
        self.api_return(false, result)
    }

    pub(crate) fn commit_transaction(&self, timeout: Timeout) -> KafkaResult<()> {
        let deadline = Deadline::new(timeout);
        self.api_begin("commit_transaction")
            .map_err(KafkaError::Transaction)?;
        // Begin commit: no more records may be enqueued.
        let result = self.op_req(
            |st, _| {
                let eos = &mut st.eos;
                match eos.require(&[
                    TxnState::InTransaction,
                    TxnState::BeginCommit,
                    TxnState::CommittingTransaction,
                    TxnState::CommitNotAcked,
                ]) {
                    Ok(()) => {
                        if eos.txn_state == TxnState::InTransaction {
                            eos.set_txn_state(TxnState::BeginCommit);
                        }
                        eos.set_result(None);
                    }
                    Err(e) => eos.set_result(Some(e)),
                }
            },
            deadline,
        );
        if let Err(e) = result {
            return self.api_return(false, Err(e));
        }
        // Deliver everything that was produced in the transaction.
        if self.wait_flushed(deadline).is_err() {
            let e = RDKafkaError::new(
                RDKafkaErrorCode::OperationTimedOut,
                format!(
                    "Failed to flush all outstanding messages within the API timeout: \
                     {} message(s) remaining",
                    self.in_flight_count()
                ),
            )
            .mark_retriable();
            return self.api_return(true, Err(e));
        }
        let result = self.op_req(
            |st, fx| {
                let eos = &st.eos;
                if let Err(e) = eos.require(&[
                    TxnState::BeginCommit,
                    TxnState::CommittingTransaction,
                    TxnState::CommitNotAcked,
                ]) {
                    st.eos.set_result(Some(e));
                    return;
                }
                match eos.txn_state {
                    TxnState::CommitNotAcked => {
                        st.eos.set_result(None);
                        return;
                    }
                    TxnState::CommittingTransaction => return,
                    _ => {}
                }
                if eos.dr_fails > 0 {
                    let e = RDKafkaError::new(
                        RDKafkaErrorCode::Inconsistent,
                        format!(
                            "{} message(s) failed delivery (see individual delivery reports)",
                            eos.dr_fails
                        ),
                    )
                    .mark_txn_requires_abort();
                    st.set_abortable(e.code(), false, e.string(), fx);
                    st.eos.set_result(Some(e));
                    return;
                }
                if eos.req_cnt == 0 {
                    st.endtxn_complete(fx);
                    return;
                }
                if eos.pid.producer_id < 0 {
                    let e = RDKafkaError::new(
                        RDKafkaErrorCode::State,
                        format!(
                            "No PID available (idempotence state {})",
                            eos.idemp_state.name()
                        ),
                    )
                    .mark_retriable();
                    st.eos.set_result(Some(e));
                    return;
                }
                st.eos.end_txn = Some(true);
                st.eos.set_txn_state(TxnState::CommittingTransaction);
                fx.wake_eos = true;
            },
            deadline,
        );
        if let Err(e) = result {
            return self.api_return(true, Err(e));
        }
        let result = self.op_req(
            |st, _| match st.eos.require(&[TxnState::CommitNotAcked]) {
                Ok(()) => {
                    st.txn_complete();
                    st.eos.set_result(None);
                }
                Err(e) => st.eos.set_result(Some(e)),
            },
            Deadline::never(),
        );
        self.api_return(false, result)
    }

    pub(crate) fn abort_transaction(&self, timeout: Timeout) -> KafkaResult<()> {
        let deadline = Deadline::new(timeout);
        self.api_begin("abort_transaction")
            .map_err(KafkaError::Transaction)?;
        let result = self.op_req(
            |st, _| match st.eos.require(&[
                TxnState::InTransaction,
                TxnState::BeginAbort,
                TxnState::AbortingTransaction,
                TxnState::AbortableError,
                TxnState::AbortNotAcked,
            ]) {
                Ok(()) => {
                    if matches!(
                        st.eos.txn_state,
                        TxnState::InTransaction | TxnState::AbortableError
                    ) {
                        st.eos.set_txn_state(TxnState::BeginAbort);
                        st.clear_txn_partitions(true);
                    }
                    st.eos.set_result(None);
                }
                Err(e) => st.eos.set_result(Some(e)),
            },
            deadline,
        );
        if let Err(e) = result {
            return self.api_return(false, Err(e));
        }
        // Purge records never sent, then wait for the rest.
        let mut fx = Effects::default();
        let mut st = self.lock();
        st.purge_unsent(&mut fx);
        self.finish(st, fx);
        if self.wait_flushed(deadline).is_err() {
            let e = RDKafkaError::new(
                RDKafkaErrorCode::OperationTimedOut,
                format!(
                    "Failed to flush all outstanding messages within the API timeout: \
                     {} message(s) remaining: the event queue must be polled for delivery \
                     report events in a separate thread or prior to calling abort",
                    self.in_flight_count()
                ),
            )
            .mark_retriable();
            return self.api_return(true, Err(e));
        }
        let result = self.op_req(
            |st, fx| {
                if let Err(e) = st.eos.require(&[
                    TxnState::BeginAbort,
                    TxnState::AbortingTransaction,
                    TxnState::AbortNotAcked,
                ]) {
                    st.eos.set_result(Some(e));
                    return;
                }
                match st.eos.txn_state {
                    TxnState::AbortNotAcked => {
                        st.eos.set_result(None);
                        return;
                    }
                    TxnState::AbortingTransaction => return,
                    _ => {}
                }
                if st.eos.req_cnt == 0 {
                    st.endtxn_complete(fx);
                    return;
                }
                if !matches!(
                    st.eos.idemp_state,
                    IdempState::Assigned | IdempState::WaitTxnAbort
                ) {
                    // An epoch bump is in progress; its completion moves the
                    // transaction to AbortNotAcked.
                    return;
                }
                if st.eos.pid.producer_id < 0 {
                    let e = RDKafkaError::new(
                        RDKafkaErrorCode::State,
                        format!(
                            "No PID available (idempotence state {})",
                            st.eos.idemp_state.name()
                        ),
                    )
                    .mark_retriable();
                    st.eos.set_result(Some(e));
                    return;
                }
                st.eos.end_txn = Some(false);
                st.eos.set_txn_state(TxnState::AbortingTransaction);
                fx.wake_eos = true;
            },
            deadline,
        );
        if let Err(e) = result {
            return self.api_return(true, Err(e));
        }
        let result = self.op_req(
            |st, _| match st.eos.require(&[TxnState::AbortNotAcked]) {
                Ok(()) => {
                    st.txn_complete();
                    st.eos.set_result(None);
                }
                Err(e) => st.eos.set_result(Some(e)),
            },
            Deadline::never(),
        );
        self.api_return(false, result)
    }

    /// The idempotence and transaction task: acquires producer ids, adds
    /// partitions to transactions and ends transactions.
    pub(super) async fn eos_task(self: Arc<Self>) {
        let shutdown = self.shutdown.clone();
        loop {
            let (work, next) = {
                let mut st = self.lock();
                let mut fx = Effects::default();
                let work = self.next_eos_work(&mut st, &mut fx);
                let next = [st.eos.pid_at, st.eos.register_at]
                    .into_iter()
                    .flatten()
                    .min();
                self.finish(st, fx);
                (work, next)
            };
            let Some(work) = work else {
                let sleep = async {
                    match next {
                        Some(at) => tokio::time::sleep_until(at.into()).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = self.eos_notify.notified() => {}
                    _ = sleep => {}
                }
                continue;
            };
            let run = async {
                match work {
                    EosWork::Pid => self.request_pid().await,
                    EosWork::Register(parts) => self.register_partitions(parts).await,
                    EosWork::EndTxn(commit) => self.end_txn(commit).await,
                }
            };
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = run => {}
            }
        }
    }

    fn next_eos_work(
        &self,
        st: &mut State<C::DeliveryOpaque>,
        fx: &mut Effects<C::DeliveryOpaque>,
    ) -> Option<EosWork> {
        let now = Instant::now();
        if st.eos.fatal.is_some() {
            return None;
        }
        if matches!(
            st.eos.idemp_state,
            IdempState::DrainBump | IdempState::DrainReset
        ) && st.in_flight == 0
        {
            st.drain_done(fx);
        }
        if st.eos.idemp_state == IdempState::RequestPid && st.eos.pid_at.is_some_and(|at| at <= now)
        {
            st.eos.pid_at = None;
            st.eos.set_idemp_state(IdempState::WaitPid);
            return Some(EosWork::Pid);
        }
        if let Some(commit) = st.eos.end_txn.take() {
            return Some(EosWork::EndTxn(commit));
        }
        if st.eos.transactional
            && matches!(
                st.eos.txn_state,
                TxnState::InTransaction | TxnState::BeginCommit
            )
            && !st.eos.pending.is_empty()
            && st.eos.registering.is_empty()
            && st.eos.idemp_state == IdempState::Assigned
            && st.eos.register_at.is_none_or(|at| at <= now)
        {
            st.eos.register_at = None;
            let parts: Vec<PartKey> = std::mem::take(&mut st.eos.pending).into_iter().collect();
            st.eos.registering = parts.clone();
            st.eos.req_cnt += 1;
            return Some(EosWork::Register(parts));
        }
        None
    }

    /// Acquires a producer id (or, for a transactional producer that has
    /// one, a new epoch) with InitProducerId.
    async fn request_pid(&self) {
        let transactional_id = self.config.transactional_id.clone();
        let current = self.lock().eos.pid;
        let deadline = Deadline::new(self.config.socket_timeout);
        let outcome = async {
            let broker = match &transactional_id {
                Some(id) => {
                    self.cluster
                        .coordinator(CoordinatorKind::Transaction, id, deadline)
                        .await
                }
                None => self.cluster.any_broker(deadline).await,
            };
            let broker = match broker {
                Ok(b) => b,
                Err(e) => return PidOutcome::Coordinator(e),
            };
            let timeout_ms = transactional_id.as_ref().map_or(-1, |_| {
                i32::try_from(self.config.transaction_timeout.as_millis()).unwrap_or(i32::MAX)
            });
            let response = broker
                .request::<InitProducerIdRequest, _>(
                    |version| {
                        let mut req = InitProducerIdRequest::default()
                            .with_transactional_id(
                                transactional_id
                                    .as_ref()
                                    .map(|id| TransactionalId(StrBytes::from_string(id.clone()))),
                            )
                            .with_transaction_timeout_ms(timeout_ms);
                        if version >= 3 {
                            // KIP-360: ask for an epoch bump of the current id.
                            req.producer_id = ProducerId(current.producer_id);
                            req.producer_epoch = current.producer_epoch;
                        }
                        req
                    },
                    RequestOptions::default(),
                )
                .await;
            match response {
                Ok(r) => match protocol::broker_error(r.error_code) {
                    None => PidOutcome::Assigned(ProducerIdentity {
                        producer_id: r.producer_id.0,
                        producer_epoch: r.producer_epoch,
                    }),
                    Some(code) => PidOutcome::Failed(code, broker.name()),
                },
                Err(e) => PidOutcome::Failed(e.code(), broker.name()),
            }
        }
        .await;

        let mut st = self.lock();
        let mut fx = Effects::default();
        let retry_at = Instant::now() + PID_RETRY_BACKOFF;
        match outcome {
            PidOutcome::Assigned(pid) => {
                if st.eos.idemp_state != IdempState::WaitPid {
                    // Outdated.
                } else if pid.producer_id < 0 || pid.producer_epoch < 0 {
                    fx.log(
                        RDKafkaLogLevel::Warning,
                        "GETPID",
                        format!(
                            "Acquired invalid PID{{{},{}}}: ignoring",
                            pid.producer_id, pid.producer_epoch
                        ),
                    );
                    st.eos.set_idemp_state(IdempState::RequestPid);
                    st.eos.pid_at = Some(retry_at);
                } else {
                    st.assign_pid(pid, &mut fx);
                }
            }
            PidOutcome::Coordinator(e) => match e.code() {
                code @ (RDKafkaErrorCode::TransactionalIdAuthorizationFailed
                | RDKafkaErrorCode::ClusterAuthorizationFailed) => {
                    let reason = format!(
                        "Failed to find transaction coordinator: {}{}",
                        code.err2str(),
                        if e.message().is_empty() || e.message() == code.err2str() {
                            String::new()
                        } else {
                            format!(": {}", e.message())
                        }
                    );
                    st.set_fatal(code, reason, &mut fx);
                }
                RDKafkaErrorCode::BrokerDestroy => {}
                _ => {
                    if let Some(id) = &transactional_id {
                        self.cluster
                            .invalidate_coordinator(CoordinatorKind::Transaction, id);
                    }
                    st.eos.set_idemp_state(IdempState::RequestPid);
                    st.eos.pid_at = Some(retry_at);
                }
            },
            PidOutcome::Failed(code, broker) => {
                let errstr = format!(
                    "Failed to acquire {} PID from broker {broker}: {}",
                    if transactional_id.is_some() {
                        "transactional"
                    } else {
                        "idempotence"
                    },
                    code.err2str()
                );
                match code {
                    RDKafkaErrorCode::BrokerDestroy => {}
                    RDKafkaErrorCode::UnsupportedFeature
                    | RDKafkaErrorCode::InvalidTransactionTimeout
                    | RDKafkaErrorCode::TransactionalIdAuthorizationFailed
                    | RDKafkaErrorCode::ClusterAuthorizationFailed => {
                        st.set_fatal(code, errstr, &mut fx);
                    }
                    RDKafkaErrorCode::InvalidProducerEpoch | RDKafkaErrorCode::ProducerFenced => {
                        st.set_fatal(
                            RDKafkaErrorCode::Fenced,
                            format!("Producer fenced by newer instance: {errstr}"),
                            &mut fx,
                        );
                    }
                    _ => {
                        if let Some(id) = &transactional_id {
                            if matches!(
                                code,
                                RDKafkaErrorCode::NotCoordinator
                                    | RDKafkaErrorCode::CoordinatorNotAvailable
                                    | RDKafkaErrorCode::BrokerTransportFailure
                                    | RDKafkaErrorCode::OperationTimedOut
                            ) {
                                self.cluster
                                    .invalidate_coordinator(CoordinatorKind::Transaction, id);
                            }
                        }
                        st.eos.init_err = Some(code);
                        st.eos.set_idemp_state(IdempState::RequestPid);
                        st.eos.pid_at = Some(retry_at);
                        fx.log(
                            RDKafkaLogLevel::Warning,
                            "GETPID",
                            format!("{errstr}: retrying"),
                        );
                    }
                }
            }
        }
        self.finish(st, fx);
    }

    /// Sends AddPartitionsToTxn for `parts` and handles the response like
    /// librdkafka's `rd_kafka_txn_handle_AddPartitionsToTxn`.
    async fn register_partitions(&self, parts: Vec<PartKey>) {
        let transactional_id = self
            .config
            .transactional_id
            .clone()
            .expect("only transactional producers register partitions");
        let (pid, topics) = {
            let st = self.lock();
            let mut topics: BTreeMap<String, Vec<i32>> = BTreeMap::new();
            for key in &parts {
                topics
                    .entry(st.topics[key.topic].name.to_string())
                    .or_default()
                    .push(key.partition);
            }
            (st.eos.pid, topics)
        };
        let started = Instant::now();
        let deadline = Deadline::new(self.config.socket_timeout);
        let mut broker_name = String::new();
        let mut sent = false;
        let response: Result<Vec<(String, i32, i16)>, RDKafkaErrorCode> = async {
            let broker = self
                .cluster
                .coordinator(CoordinatorKind::Transaction, &transactional_id, deadline)
                .await
                .map_err(|e| e.code())?;
            broker_name = broker.name();
            sent = true;
            let response = broker
                .request::<AddPartitionsToTxnRequest, _>(
                    |_version| {
                        let mut req = AddPartitionsToTxnRequest::default();
                        req.v3_and_below_transactional_id =
                            TransactionalId(StrBytes::from_string(transactional_id.clone()));
                        req.v3_and_below_producer_id = ProducerId(pid.producer_id);
                        req.v3_and_below_producer_epoch = pid.producer_epoch;
                        req.v3_and_below_topics = topics
                            .iter()
                            .map(|(name, partitions)| {
                                AddPartitionsToTxnTopic::default()
                                    .with_name(TopicName(StrBytes::from_string(name.clone())))
                                    .with_partitions(partitions.clone())
                            })
                            .collect();
                        req
                    },
                    RequestOptions::default(),
                )
                .await
                .map_err(|e| e.code())?;
            let mut results = Vec::new();
            for t in &response.results_by_topic_v3_and_below {
                for p in &t.results_by_partition {
                    results.push((
                        t.name.0.as_str().to_string(),
                        p.partition_index,
                        p.partition_error_code,
                    ));
                }
            }
            Ok(results)
        }
        .await;

        let mut st = self.lock();
        let mut fx = Effects::default();
        let registering = std::mem::take(&mut st.eos.registering);
        // NOTE: librdkafka keeps counting a request that failed with a broker
        // error, and then sends EndTxn on abort, which Kafka rejects with a
        // fatal INVALID_TXN_STATE if no partition was ever added (e.g. after
        // CONCURRENT_TRANSACTIONS right after a commit). Only requests that
        // may have added partitions count here.
        let maybe_added = match &response {
            Err(_) => sent,
            Ok(results) => results.iter().any(|(_, _, code)| *code == 0),
        };
        if !maybe_added {
            st.eos.req_cnt = st.eos.req_cnt.saturating_sub(1);
        }
        if !matches!(
            st.eos.txn_state,
            TxnState::InTransaction | TxnState::BeginCommit
        ) {
            // Outdated: the transaction is being aborted.
            self.finish(st, fx);
            return;
        }
        let mut action = None;
        let mut err = None;
        let mut reset_coordinator = false;
        let mut retry_backoff = PID_RETRY_BACKOFF;
        let mut concurrent = false;
        let mut require_bump = false;
        let mut registered = BTreeSet::new();
        let debug = self.cluster.logger().debug_enabled(DebugContext::Eos);
        let mut debug_lines = Vec::new();
        match response {
            Err(code) => {
                err = Some(code);
                action = Some(Action::Retry);
                if matches!(
                    code,
                    RDKafkaErrorCode::BrokerTransportFailure
                        | RDKafkaErrorCode::OperationTimedOut
                        | RDKafkaErrorCode::NotCoordinator
                        | RDKafkaErrorCode::CoordinatorNotAvailable
                ) {
                    reset_coordinator = true;
                }
            }
            Ok(results) => {
                for (topic, partition, code) in results {
                    let key = registering
                        .iter()
                        .find(|k| k.partition == partition && &*st.topics[k.topic].name == topic);
                    let Some(key) = key.copied() else {
                        continue;
                    };
                    let Some(code) = protocol::broker_error(code) else {
                        registered.insert(key);
                        continue;
                    };
                    if debug {
                        debug_lines.push(format!(
                            "AddPartitionsToTxn response: partition \"{topic}\": [{partition}]: {}",
                            code.err2str()
                        ));
                    }
                    let (p_action, request_error) = match code {
                        RDKafkaErrorCode::NotCoordinator
                        | RDKafkaErrorCode::CoordinatorNotAvailable => {
                            reset_coordinator = true;
                            (Action::Retry, true)
                        }
                        RDKafkaErrorCode::ConcurrentTransactions => {
                            concurrent = true;
                            (Action::Retry, true)
                        }
                        RDKafkaErrorCode::CoordinatorLoadInProgress
                        | RDKafkaErrorCode::UnknownTopicOrPartition => (Action::Retry, true),
                        RDKafkaErrorCode::InvalidProducerEpoch
                        | RDKafkaErrorCode::ProducerFenced
                        | RDKafkaErrorCode::TransactionalIdAuthorizationFailed
                        | RDKafkaErrorCode::InvalidTransactionalState
                        | RDKafkaErrorCode::ClusterAuthorizationFailed => (Action::Fatal, true),
                        RDKafkaErrorCode::UnknownProducerId
                        | RDKafkaErrorCode::InvalidProducerIdMapping => {
                            require_bump = true;
                            (Action::Permanent, true)
                        }
                        _ => (Action::Permanent, false),
                    };
                    if code != RDKafkaErrorCode::OperationNotAttempted || err.is_none() {
                        err = Some(code);
                    }
                    if matches!(p_action, Action::Fatal | Action::Permanent) {
                        fx.log(
                            RDKafkaLogLevel::Error,
                            "ADDPARTS",
                            format!(
                                "Failed to add partition \"{topic}\" [{partition}] to transaction: {}",
                                code.err2str()
                            ),
                        );
                    }
                    action = Some(match (action, p_action) {
                        (Some(Action::Fatal), _) | (_, Action::Fatal) => Action::Fatal,
                        (Some(Action::Permanent), _) | (_, Action::Permanent) => Action::Permanent,
                        _ => Action::Retry,
                    });
                    if request_error {
                        break;
                    }
                }
            }
        }
        if !registered.is_empty() {
            st.eos.concurrent_backoff = None;
        }
        for key in &registering {
            if registered.contains(key) {
                let p = st.partition_mut(*key);
                if p.txn == PartTxn::Pending {
                    p.txn = PartTxn::InTxn;
                    if p.leader >= 0 {
                        fx.wake.insert(p.leader);
                    }
                }
            } else {
                st.eos.pending.insert(*key);
            }
        }
        if reset_coordinator {
            self.cluster
                .invalidate_coordinator(CoordinatorKind::Transaction, &transactional_id);
        }
        let code = err.map(normalize);
        match (action, code) {
            (Some(Action::Fatal), Some(code)) => {
                st.set_fatal(
                    code,
                    format!(
                        "Failed to add partitions to transaction: {}",
                        code.err2str()
                    ),
                    &mut fx,
                );
            }
            (Some(Action::Permanent), Some(code)) => {
                let reason = format!(
                    "Failed to add partition(s) to transaction on broker {broker_name}: {} \
                     (after {} ms)",
                    code.err2str(),
                    started.elapsed().as_millis()
                );
                if require_bump {
                    st.drain_epoch_bump(true, code, reason, &mut fx);
                } else {
                    st.set_abortable(code, false, reason, &mut fx);
                }
            }
            (Some(Action::Retry), _) => {
                if concurrent {
                    // NOTE: librdkafka always waits 20 ms. Right after a commit
                    // the coordinator is usually done within a few, so start
                    // lower and back off to that.
                    let backoff = st
                        .eos
                        .concurrent_backoff
                        .map_or(CONCURRENT_RETRY_BACKOFF_MIN, |b| {
                            (b * 2).min(CONCURRENT_RETRY_BACKOFF_MAX)
                        });
                    st.eos.concurrent_backoff = Some(backoff);
                    retry_backoff = backoff;
                }
                st.eos.register_at = Some(Instant::now() + retry_backoff);
            }
            _ => {
                if !st.eos.pending.is_empty() {
                    st.eos.register_at = Some(Instant::now());
                }
            }
        }
        fx.wake_eos = true;
        self.finish(st, fx);
        for line in debug_lines {
            self.cluster
                .logger()
                .debug(DebugContext::Eos, "ADDPARTS", format_args!("{line}"));
        }
    }

    /// Sends EndTxn until it succeeds or fails permanently, like librdkafka's
    /// `rd_kafka_txn_handle_EndTxn` with request retries.
    async fn end_txn(&self, commit: bool) {
        let transactional_id = self
            .config
            .transactional_id
            .clone()
            .expect("only transactional producers end transactions");
        loop {
            let pid = self.lock().eos.pid;
            let deadline = Deadline::new(self.config.socket_timeout);
            let mut coordinator = None;
            let result: Result<(), RDKafkaErrorCode> = async {
                let broker: Arc<Broker> = self
                    .cluster
                    .coordinator(CoordinatorKind::Transaction, &transactional_id, deadline)
                    .await
                    .map_err(|e| e.code())?;
                coordinator = Some(Arc::clone(&broker));
                let response = broker
                    .request::<EndTxnRequest, _>(
                        |_version| {
                            EndTxnRequest::default()
                                .with_transactional_id(TransactionalId(StrBytes::from_string(
                                    transactional_id.clone(),
                                )))
                                .with_producer_id(ProducerId(pid.producer_id))
                                .with_producer_epoch(pid.producer_epoch)
                                .with_committed(commit)
                        },
                        RequestOptions::default(),
                    )
                    .await
                    .map_err(|e| e.code())?;
                match protocol::broker_error(response.error_code) {
                    None => Ok(()),
                    Some(code) => Err(code),
                }
            }
            .await;
            let err = result.err();
            if err == Some(RDKafkaErrorCode::BrokerDestroy) {
                return;
            }
            if !self.handle_end_txn(commit, err, &transactional_id) {
                return;
            }
            if let Some(broker) = coordinator {
                broker
                    .counters()
                    .txretries
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            tokio::time::sleep(self.config.retry_backoff).await;
        }
    }

    /// Handles an EndTxn result like librdkafka's
    /// `rd_kafka_txn_handle_EndTxn`. Returns whether to retry the request.
    fn handle_end_txn(
        &self,
        commit: bool,
        err: Option<RDKafkaErrorCode>,
        transactional_id: &str,
    ) -> bool {
        let mut st = self.lock();
        let mut fx = Effects::default();
        let mut may_retry = false;
        let mut err = err;
        match st.eos.txn_state {
            TxnState::CommittingTransaction | TxnState::AbortingTransaction => {
                may_retry = true;
            }
            TxnState::AbortableError => {
                // The transaction failed locally meanwhile, typically in a
                // timeout. The EndTxn may still have succeeded.
                let (txn_code, txn_msg) = st
                    .eos
                    .txn_err
                    .clone()
                    .unwrap_or((RDKafkaErrorCode::Fail, String::new()));
                let error = match err {
                    Some(code) => RDKafkaError::new(
                        txn_code,
                        format!(
                            "EndTxn failed with {} but transaction had already failed due to: \
                             {txn_msg}",
                            code.name()
                        ),
                    )
                    .mark_txn_requires_abort(),
                    None if commit => RDKafkaError::new(
                        txn_code,
                        format!(
                            "Transaction commit succeeded on the broker but the transaction had \
                             already failed locally due to: {txn_msg}"
                        ),
                    )
                    .mark_fatal(),
                    // NOTE: librdkafka's text lacks a space here.
                    None => RDKafkaError::new(
                        txn_code,
                        format!(
                            "Transaction abort succeeded on the broker but the transactionhad \
                             already failed locally due to: {txn_msg}"
                        ),
                    )
                    .mark_txn_requires_abort(),
                };
                st.eos.set_result(Some(error));
                self.finish(st, fx);
                return false;
            }
            _ => {
                if err.is_none() {
                    err = Some(RDKafkaErrorCode::Outdated);
                }
            }
        }

        let mut action = None;
        let mut require_bump = false;
        match err {
            None | Some(RDKafkaErrorCode::Outdated) => {}
            Some(
                RDKafkaErrorCode::OperationTimedOut
                | RDKafkaErrorCode::TimedOutQueue
                | RDKafkaErrorCode::BrokerTransportFailure
                | RDKafkaErrorCode::CoordinatorNotAvailable
                | RDKafkaErrorCode::NotCoordinator,
            ) => {
                // Look the coordinator up again.
                self.cluster
                    .invalidate_coordinator(CoordinatorKind::Transaction, transactional_id);
                action = Some(Action::Retry);
            }
            Some(
                RDKafkaErrorCode::CoordinatorLoadInProgress
                | RDKafkaErrorCode::ConcurrentTransactions,
            ) => {
                action = Some(Action::Retry);
            }
            Some(
                RDKafkaErrorCode::UnknownProducerId | RDKafkaErrorCode::InvalidProducerIdMapping,
            ) => {
                action = Some(Action::Permanent);
                require_bump = true;
            }
            Some(
                RDKafkaErrorCode::InvalidProducerEpoch
                | RDKafkaErrorCode::ProducerFenced
                | RDKafkaErrorCode::TransactionalIdAuthorizationFailed
                | RDKafkaErrorCode::ClusterAuthorizationFailed
                | RDKafkaErrorCode::InvalidTransactionalState,
            ) => {
                action = Some(Action::Fatal);
            }
            Some(_) => action = Some(Action::Permanent),
        }
        let err = err.map(normalize);

        match (action, err) {
            (Some(Action::Fatal), Some(code)) => {
                st.set_fatal(
                    code,
                    format!("Failed to end transaction: {}", code.err2str()),
                    &mut fx,
                );
            }
            (Some(Action::Permanent), Some(code)) => {
                if require_bump && !commit {
                    // Abort failed due to an invalid producer id: bump the
                    // epoch, which completes the abort.
                    st.drain_epoch_bump(
                        false,
                        code,
                        format!("EndTxn abort failed: {}", code.err2str()),
                        &mut fx,
                    );
                    self.finish(st, fx);
                    return false;
                }
                // An abort goes back to its beginning, for the application to
                // retry it.
                if st.eos.txn_state == TxnState::AbortingTransaction {
                    st.eos.set_txn_state(TxnState::BeginAbort);
                }
                st.set_abortable(
                    code,
                    require_bump,
                    format!("Failed to end transaction: {}", code.err2str()),
                    &mut fx,
                );
            }
            (Some(Action::Retry), _) if may_retry => {
                self.finish(st, fx);
                return true;
            }
            _ => {}
        }
        match err {
            Some(code) => {
                let error = RDKafkaError::new(
                    code,
                    format!(
                        "EndTxn {} failed: {}",
                        if commit { "commit" } else { "abort" },
                        code.err2str()
                    ),
                );
                st.eos.set_result(Some(flag(error, action)));
            }
            None => st.endtxn_complete(&mut fx),
        }
        self.finish(st, fx);
        false
    }

    /// Waits until every enqueued message was reported, or `deadline`.
    pub(super) fn wait_flushed(&self, deadline: Deadline) -> Result<(), ()> {
        self.flushing
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.wake_all_senders();
        let st = self.lock();
        let st = self.wait_until(st, deadline, |_| {
            self.msg_cnt.load(std::sync::atomic::Ordering::SeqCst) == 0
        });
        drop(st);
        self.flushing
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        if self.msg_cnt.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            Ok(())
        } else {
            Err(())
        }
    }

    /// Blocks on the condition variable until `done` or `deadline`.
    pub(super) fn wait_until<'a>(
        &'a self,
        mut st: MutexGuard<'a, State<C::DeliveryOpaque>>,
        deadline: Deadline,
        mut done: impl FnMut(&State<C::DeliveryOpaque>) -> bool,
    ) -> MutexGuard<'a, State<C::DeliveryOpaque>> {
        assert!(
            !crate::internal::runtime::is_io_thread(),
            "mz_kafka_client: blocking producer call on a kafka-io runtime thread"
        );
        loop {
            if done(&st) {
                return st;
            }
            match deadline.remaining_duration() {
                None => st = self.cond.wait(st).expect("lock poisoned"),
                Some(remaining) if remaining.is_zero() => return st,
                Some(remaining) => {
                    st = self
                        .cond
                        .wait_timeout(st, remaining)
                        .expect("lock poisoned")
                        .0;
                }
            }
        }
    }
}
