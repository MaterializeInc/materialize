# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT privileges::text FROM mz_system_privileges WHERE mz_internal.mz_aclitem_grantee(privileges) LIKE 'u%';
r1=B/mz_system
