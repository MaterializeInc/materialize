# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The restarted sink resumes once the broker is back. The verification
# continues from where the one in kafka-restart-broker-sink.td stopped.
$ set-sql-timeout duration=120s

> INSERT INTO restart_sink_input VALUES ('two')

$ kafka-verify-data format=json sink=materialize.public.restart_sink sort-messages=true
{"before": null, "after": {"text": "two"}}

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'restart_sink'
running
