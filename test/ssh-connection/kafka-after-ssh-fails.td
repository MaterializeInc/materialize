# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test errors when ssh tunnels fail.

# Specify the behaviour of the status history tables
$ set-regex match="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d(\.\d\d\d)?" replacement="<TIMESTAMP>"

$ set-from-sql var=source_id
SELECT id FROM mz_sources WHERE name = 'mz_source_dynamic'

$ set-from-sql var=sink_id
SELECT id FROM mz_sinks WHERE name = 'mz_sink'

# Out of an abundance of caution, ingest new data to _ensure_
# we trigger the kafka sink failing in the later test.
$ kafka-ingest topic=thetopic format=bytes
three

# We don't check the actual error messages here because `rdkafka` makes it
# _really_ hard to control this. Ideally we'd get an error about the ssh tunnel
# (which the code tries to communicate)
# but we actually get a `BrokerTransportFailure`.
#
# TODO(guswynn): investigate if we can make these errors better.
> SELECT status FROM mz_internal.mz_source_statuses WHERE id = '${source_id}';
stalled

# It's unclear what's going on with sink errors.I (guswynn) *think* that
# after seeing a transaction failure, the sink restarts itself, which
# causes us to attempt to recreate the ssh tunnel. This fails,
# so we are forced by `rdkafka` to fallback to the untranslated address.
# Because we don't have a firewall, the sink is now healthy. So, we
# just check that we saw the error we expected. We actually see many different
# errors, both the consumer, the producer, and our ssh tunnel creation all
# report errors.
#
# TODO(guswynn): add a firewall.
> select count(*) > 0 from mz_internal.mz_sink_status_history WHERE sink_id = '${sink_id}' AND status = 'stalled';
true
