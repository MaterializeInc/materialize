# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test exercises solely to test arrays at the boundary (e.g., by sending
# them through pgwire).

> SELECT ARRAY[ARRAY['a', 'b'], ARRAY[NULL, 'd']]
array
----
{{a,b},{NULL,d}}

> SELECT ARRAY[ARRAY['a', 'b'], ARRAY[NULL, 'd']]::text
array
----
{{a,b},{NULL,d}}
