# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT a, _fivetran_deleted FROM test.tester.""""
a  _fivetran_deleted
--------------------
1  true

> SELECT b, _fivetran_deleted FROM test.tester." "
b  _fivetran_deleted
--------------------
1  true

> SELECT c, _fivetran_deleted FROM test.tester."'"
c  _fivetran_deleted
--------------------
1  true

> SELECT d, _fivetran_deleted FROM test.tester."\"
d  _fivetran_deleted
--------------------
1  true
