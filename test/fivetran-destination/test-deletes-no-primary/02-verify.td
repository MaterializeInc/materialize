# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT v1, v2, _fivetran_deleted FROM test.tester.test_deletes_no_primary
v1  v2  _fivetran_deleted
-------------------------
1   a   false
1   b   false
2   a   true
