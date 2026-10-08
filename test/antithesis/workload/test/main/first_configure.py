#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sys

from materialize.antithesis.drivers import configure, kafka_sources, postgres_sources

status = configure.main()
postgres_sources.setup_main()
kafka_sources.setup_main()
sys.exit(status)
