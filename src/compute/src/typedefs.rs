// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Convience typedefs for differential types.

#![allow(missing_docs)]

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};

use mz_repr::{Diff, Row, Timestamp};
use mz_storage::types::errors::DataflowError;

pub type RowSpine<K, V, T, R, O = usize> = OrdValSpine<K, V, T, R, O>;
pub type RowKeySpine<K, T, R, O = usize> = OrdKeySpine<K, T, R, O>;
pub type ErrSpine<K, T, R, O = usize> = OrdKeySpine<K, T, R, O>;
pub type TraceRowHandle<K, V, T, R> = TraceAgent<RowSpine<K, V, T, R>>;
pub type TraceErrHandle<K, T, R> = TraceAgent<ErrSpine<K, T, R>>;
pub type KeysValsHandle = TraceRowHandle<Row, Row, Timestamp, Diff>;
pub type ErrsHandle = TraceErrHandle<DataflowError, Timestamp, Diff>;
