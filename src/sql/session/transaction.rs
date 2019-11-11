// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

/// The current transaction status of a Session
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Not currently in a transaction
    Idle,
    /// Currently in a transaction
    InTransaction,
    /// Currently in a transaction block which is failed
    Failed,
}
