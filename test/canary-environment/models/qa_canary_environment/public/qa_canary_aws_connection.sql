-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE CONNECTION qa_canary_aws_connection TO AWS (
    ASSUME ROLE ARN = 'arn:aws:iam::400121260767:role/qa-canary-environment-iceberg-role',
    REGION = 'us-east-1'
);
