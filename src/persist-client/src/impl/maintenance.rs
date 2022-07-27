// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! All machines need maintenance
//!
//! Maintenance operations for persist, shared among active handles

use crate::r#impl::compact::CompactReq;
use crate::r#impl::gc::GcReq;
use crate::{Compactor, GarbageCollector, Machine};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist_types::{Codec, Codec64};
use std::fmt::Debug;
use timely::progress::Timestamp;

/// TODO: Actually implement lease expiration as a routine maintenance task
#[derive(Debug)]
pub struct LeaseExpiration;

/// Every handle to this shard may be occasionally asked to perform
/// routine maintenance after a successful compare_and_set operation.
///
/// For one-shot operations (like registering a reader) handles are
/// allowed to skip routine maintenance if necessary, as the same
/// maintenance operations will be recomputed by the next successful
/// compare_and_set of any handle.
///
/// Operations that run regularly once a handle is registered, such
/// as heartbeats, are expected to always perform maintenance.
#[must_use]
#[derive(Debug, Default)]
pub struct RoutineMaintenance {
    pub(crate) garbage_collection: Option<GcReq>,
    pub(crate) lease_expiration: Option<LeaseExpiration>,
}

impl RoutineMaintenance {
    pub(crate) fn perform(self, gc: &GarbageCollector) {
        if let Some(gc_req) = self.garbage_collection {
            gc.gc_and_truncate_background(gc_req);
        }

        if let Some(_lease_expiration) = self.lease_expiration {
            unimplemented!("lease expiration coming soon");
        }
    }
}

/// Writers may be asked to perform additional tasks beyond the
/// routine maintenance common to all handles. It is expected that
/// writers always perform maintenance.
#[must_use]
#[derive(Debug, Default)]
pub struct WriterMaintenance<T> {
    pub(crate) routine: RoutineMaintenance,
    pub(crate) compaction: Vec<CompactReq<T>>,
}

impl<T> WriterMaintenance<T>
where
    T: Timestamp + Lattice + Codec64,
{
    pub(crate) fn perform<K, V, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector,
        compactor: Option<&Compactor>,
    ) where
        K: Debug + Codec,
        V: Debug + Codec,
        D: Semigroup + Codec64,
    {
        self.routine.perform(gc);

        if let Some(compactor) = compactor {
            for req in self.compaction {
                compactor.compact_and_apply_background(machine, req);
            }
        }
    }
}
