-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER :'kafka_broker',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = SECRET kafka_username,
    SASL PASSWORD = SECRET kafka_password
);
