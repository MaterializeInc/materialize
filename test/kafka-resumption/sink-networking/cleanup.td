# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP SINK output CASCADE;

> DROP SOURCE input CASCADE;

$ http-request method=DELETE url=http://toxiproxy:8474/proxies/kafka content-type=application/json
