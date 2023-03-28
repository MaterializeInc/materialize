# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test creating a Kafka source using SSH, with and without the CSR

# Specify the behaviour of the status history tables
$ set-regex match="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d(\.\d\d\d)?" replacement="<TIMESTAMP>"

$ set-from-sql var=source_id
SELECT id FROM mz_sources WHERE name = 'mz_source_dynamic'

# We don't check the actual error message here because `rdkafka` makes it
# _really_ hard to control this. Ideally we'd get an error about the ssh tunnel
# (which the code tries to communicate)
# but we actually get a `BrokerTransportFailure`.
#
# TODO(guswynn): investigate if we can make these errors better.
> select status from mz_internal.mz_source_statuses where id = '${source_id}';
stalled
