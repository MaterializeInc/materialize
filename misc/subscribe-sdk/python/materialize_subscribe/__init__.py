# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A correct-by-construction client for consuming Materialize ``SUBSCRIBE``.

Consuming ``SUBSCRIBE`` durably requires a specific protocol: buffer until a
progress message closes a timestamp, apply the closed batch and persist its
frontier atomically, and resume with ``SNAPSHOT = false AS OF frontier - 1``.
Every step has a failure mode that is invisible in testing. This package
encodes the protocol so callers cannot get it wrong:

* The unit of consumption is a :class:`ConsistentBatch`, never a bare row.
* Resuming takes an opaque :class:`ResumeToken`; the ``- 1`` boundary
  arithmetic lives inside it.
* Failures surface as a small, typed :class:`SubscribeError` hierarchy.

The client is three layers, each usable on its own: :class:`RawStream` (via
:meth:`SubscribeClient.subscribe_raw`) is the decoded timestamped substrate; the
consistency engine buffers and releases below a frontier; a
:class:`ConsistentBatch` (one view) or :class:`CohortMoment` (a :class:`Cohort`
of N views) is the closed unit. A single view is the cohort of one.

Example::

    from materialize_subscribe import Subscribe, SubscribeClient

    with SubscribeClient.connect("postgres://materialize@localhost:6875") as client:
        stream = client.subscribe(
            Subscribe.object("winning_bids").envelope_upsert(["id"])
        )
        for batch in stream:
            for change in batch.updates:
                ...  # apply `change` to your sink
            # Persist `batch.resume_token.encode()` atomically with the applied
            # changes for exactly-once state; hand it back to `client.resume`.
"""

from __future__ import annotations

from .batch import Batcher, ConsistentBatch
from .client import BatchStream, RawStream, SubscribeClient
from .cohort import Cohort, CohortMoment, ViewChanges
from .envelope import (
    Change,
    Data,
    Decoder,
    Delete,
    Diff,
    DiffEnvelope,
    Envelope,
    KeyViolation,
    Progress,
    StreamMessage,
    Upsert,
    UpsertEnvelope,
)
from .errors import (
    BufferOverflow,
    CohortLagExceeded,
    CompactionHorizon,
    DependencyDropped,
    FatalError,
    InvalidToken,
    ProtocolError,
    SchemaMismatch,
    SubscribeError,
    TransientError,
)
from .statement import Subscribe
from .token import CohortToken, ResumeToken

__all__ = [
    "Batcher",
    "BatchStream",
    "BufferOverflow",
    "Change",
    "Cohort",
    "CohortLagExceeded",
    "CohortMoment",
    "CohortToken",
    "CompactionHorizon",
    "ConsistentBatch",
    "Data",
    "Decoder",
    "Delete",
    "DependencyDropped",
    "Diff",
    "DiffEnvelope",
    "Envelope",
    "FatalError",
    "InvalidToken",
    "KeyViolation",
    "Progress",
    "ProtocolError",
    "RawStream",
    "ResumeToken",
    "SchemaMismatch",
    "StreamMessage",
    "Subscribe",
    "SubscribeClient",
    "SubscribeError",
    "TransientError",
    "Upsert",
    "UpsertEnvelope",
    "ViewChanges",
]
