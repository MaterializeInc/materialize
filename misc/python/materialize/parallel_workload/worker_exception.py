# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.data_ingest.query_error import QueryError


class WorkerFailedException(Exception):
    def __init__(self, message: str, cause: QueryError | None):
        super().__init__(message)
        self.cause = cause
