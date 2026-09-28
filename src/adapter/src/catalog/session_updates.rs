// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::active_compute_sink::ActiveSubscribe;
use crate::coord::ConnMeta;
use mz_catalog::builtin::{BuiltinTable, MZ_SESSIONS, MZ_SUBSCRIPTIONS};
use mz_catalog::catalog::BuiltinTableUpdate;
use mz_repr::{Datum, Diff, GlobalId, Row};
use uuid::Uuid;
pub(crate) fn pack_subscribe_update(
    id: GlobalId,
    subscribe: &ActiveSubscribe,
    session_uuid: Uuid,
    diff: Diff,
) -> BuiltinTableUpdate<&'static BuiltinTable> {
    let mut row = Row::default();
    let mut packer = row.packer();
    packer.push(Datum::String(&id.to_string()));
    packer.push(Datum::Uuid(session_uuid));
    packer.push(Datum::String(&subscribe.cluster_id.to_string()));

    let start_dt = mz_ore::now::to_datetime(subscribe.start_time);
    packer.push(Datum::TimestampTz(start_dt.try_into().expect("must fit")));

    let depends_on: Vec<_> = subscribe
        .depends_on
        .iter()
        .map(|id| id.to_string())
        .collect();
    packer.push_list(depends_on.iter().map(|s| Datum::String(s)));

    BuiltinTableUpdate::row(&*MZ_SUBSCRIPTIONS, row, diff)
}

pub(crate) fn pack_session_update(
    conn: &ConnMeta,
    diff: Diff,
) -> BuiltinTableUpdate<&'static BuiltinTable> {
    let connect_dt = mz_ore::now::to_datetime(conn.connected_at());
    BuiltinTableUpdate::row(
        &*MZ_SESSIONS,
        Row::pack_slice(&[
            Datum::Uuid(conn.uuid()),
            Datum::UInt32(conn.conn_id().unhandled()),
            Datum::String(&conn.authenticated_role_id().to_string()),
            Datum::from(conn.client_ip().map(|ip| ip.to_string()).as_deref()),
            Datum::TimestampTz(connect_dt.try_into().expect("must fit")),
        ]),
        diff,
    )
}
