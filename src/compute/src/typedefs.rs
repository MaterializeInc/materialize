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
use differential_dataflow::trace::implementations::ord::{ColKeySpine, ColValSpine};

use mz_ore::num::Overflowing;
use mz_repr::{Diff, Row, Timestamp};
use mz_storage_types::errors::DataflowError;

pub type RowSpine<K, V, T, R> = ColValSpine<K, V, T, R, Overflowing<u32>>;
pub type RowKeySpine<K, T, R> = ColKeySpine<K, T, R, Overflowing<u32>>;
pub type ErrSpine<K, T, R> = ColKeySpine<K, T, R, Overflowing<u32>>;
pub type ErrValSpine<K, T, R> = ColValSpine<K, DataflowError, T, R, Overflowing<u32>>;
pub type TraceRowHandle<K, V, T, R> = TraceAgent<RowSpine<K, V, T, R>>;
pub type TraceErrHandle<K, T, R> = TraceAgent<ErrSpine<K, T, R>>;
pub type KeysValsHandle = TraceRowHandle<Row, Row, Timestamp, Diff>;
pub type ErrsHandle = TraceErrHandle<DataflowError, Timestamp, Diff>;
