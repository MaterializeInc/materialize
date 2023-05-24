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

$ set-regex match="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d(\.\d\d\d)?" replacement="<TIMESTAMP>"

$ set-from-sql var=source_id
SELECT id FROM mz_sources WHERE name = 'failpoint'

> select * from mz_internal.mz_source_statuses where id = '${source_id}';
"${source_id}" failpoint kafka "<TIMESTAMP> UTC" stalled "${arg.error}" <null>
