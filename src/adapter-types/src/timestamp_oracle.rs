// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

/// Default value for `DynamicConfig::pg_connection_pool_max_size`.
pub const DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_SIZE: usize = 50;

/// Default value for `DynamicConfig::pg_connection_pool_max_wait`.
pub const DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_WAIT: Duration = Duration::from_secs(60);

/// Default value for `DynamicConfig::pg_connection_pool_ttl`.
pub const DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL: Duration = Duration::from_secs(300);

/// Default value for `DynamicConfig::pg_connection_pool_ttl_stagger`.
pub const DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL_STAGGER: Duration = Duration::from_secs(6);

/// Default value for `DynamicConfig::pg_connection_pool_connect_timeout`.
pub const DEFAULT_PG_TIMESTAMP_ORACLE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default value for `DynamicConfig::pg_connection_pool_tcp_user_timeout`.
pub const DEFAULT_PG_TIMESTAMP_ORACLE_TCP_USER_TIMEOUT: Duration = Duration::from_secs(30);
