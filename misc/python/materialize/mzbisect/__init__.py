# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Automated consistency bisection for data-corruption incidents.

Given an object that produced (or participated in a query that produced) a
consistency error such as "Non-positive multiplicity in DistinctBy", this
package walks the object's dependency closure and probes every relation in it
to localize where the corruption is introduced, replacing the manual
"connect as mz_system, create an unbilled cluster, and potato-query your way
down the dependency tree" incident workflow.

Probes run on a scratch cluster created for the occasion, whose only replica
is INTERNAL and BILLED AS 'free' so the customer is not charged, and no
customer replica does any work:

* scan: `SELECT count(*)`, which surfaces persisted errors and errors thrown
  while hydrating the relation from persist.
* potato: `GROUP BY row(t.*) HAVING count(*) < 1`, which reports rows with
  non-positive multiplicities. Count is an accumulable aggregate, so negative
  multiplicities flow through as negative counts instead of erroring. The
  `count(*) > 1` variant reports duplicates, which are only evidence of
  corruption where uniqueness is expected, so they are reported informationally.
* arrangement: for indexed relations, an order-insensitive multiset checksum
  computed at one fixed AS OF both against each replica of the index's home
  cluster (reading the incumbent arrangement) and against the scratch cluster
  (reading fresh from persist, reported as the `fingerprint persist-read`
  baseline). A mismatch means the arrangement on that replica has diverged
  from the persisted inputs, the automated version of "rehydrate the index and
  see if the problem goes away".

Everything except the scratch cluster's CREATE/DROP is read-only.
"""

from materialize.mzbisect.run import run_bisect, run_scan

__all__ = ["run_bisect", "run_scan"]
