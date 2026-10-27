# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Re-export symbols used in the Materialize codebase.
# This stub exists to fix AvroSerializer's broken __init__ in
# confluent-kafka >= 2.13 (metaclass hides it from type checkers).

# Allow access to confluent_kafka.admin submodule
from confluent_kafka import admin as admin
from confluent_kafka.cimpl import KafkaError as KafkaError
from confluent_kafka.cimpl import Message as Message
from confluent_kafka.cimpl import Producer as Producer
