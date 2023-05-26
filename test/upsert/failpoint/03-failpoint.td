# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This will trigger the failpoint
$ kafka-ingest format=bytes topic=failpoint key-format=bytes key-terminator=:
mammal:whale

> select st.name, st.type, st.status, st.error
  from mz_internal.mz_source_statuses st
  join mz_sources s on s.id = st.id
  where s.name = 'failpoint';
failpoint kafka stalled "${arg.error}"
