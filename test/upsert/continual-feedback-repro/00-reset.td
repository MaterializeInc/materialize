# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP TABLE IF EXISTS continual_feedback_repro_tbl CASCADE;
> DROP SOURCE IF EXISTS continual_feedback_repro_src CASCADE;
> DROP CONNECTION IF EXISTS continual_feedback_repro_conn CASCADE;
> DROP CLUSTER IF EXISTS storage_cluster CASCADE;
