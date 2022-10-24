# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Specify the behaviour of the status history tables

# History starts out empty when there are no sources
> select * from mz_internal.mz_source_status_history

# History starts out empty when there are no sinks
> select * from mz_internal.mz_sink_status_history
