// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A registry of every mz_dyncfg.

use mz_dyncfg::ConfigSet;

/// Returns a new ConfigSet containing every `Config`` in Materialize.
///
/// Each time this is called, it returns a new ConfigSet disconnected from any
/// others. It may be cloned and passed around, and updates to any of these
/// copies will be reflected in the clones. Values from a `ConfigSet` may be
/// copied to a disconnected `ConfigSet` via `ConfigUpdates`, which can be
/// passed over the network to do the same across processes.
///
/// TODO(cfg): Consider replacing this with a static global registry powered by
/// something like the `ctor` or `inventory` crate. This would solve the
/// dependency issue of this crate depending on every crate that uses dyncfgs.
/// However, on the other hand, it would involve managing the footgun of a
/// Config being linked into one binary but not the other.
pub fn all_dyncfgs() -> ConfigSet {
    let mut configs = ConfigSet::default();
    configs = mz_persist_client::cfg::all_dyncfgs(configs);
    configs = mz_txn_wal::all_dyncfgs(configs);
    configs = mz_compute_types::dyncfgs::all_dyncfgs(configs);
    configs = mz_adapter_types::dyncfgs::all_dyncfgs(configs);
    configs = mz_storage_types::dyncfgs::all_dyncfgs(configs);
    configs = mz_controller_types::dyncfgs::all_dyncfgs(configs);
    configs
}
