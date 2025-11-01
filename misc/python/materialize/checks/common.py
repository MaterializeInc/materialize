# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.checks.mzcompose_actions import MzcomposeAction, PromoteMz, WaitReadyMz


KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD = """
       $ set keyschema={
           "type": "record",
           "name": "Key",
           "fields": [
               {"name": "key1", "type": "string"}
           ]
         }

       $ set schema={
           "type" : "record",
           "name" : "test",
           "fields" : [
               {"name":"f1", "type":"string"}
           ]
         }
    """


def wait_ready_and_promote(mz_service: str) -> list[MzcomposeAction]:
    return [WaitReadyMz(mz_service), PromoteMz(mz_service)]
