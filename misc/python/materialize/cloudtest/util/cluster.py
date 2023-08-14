# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


def cluster_pod_name(cluster_id: str, replica_id: str, process: int = 0) -> str:
    # Replica IDs now appear as `GlobalId`s in `mz_cluster_replicas`, while
    # the pod names still contain only the numeric ID.
    # TODO(teskje): Change pod names to contain the replica `GlobalId`.
    replica_id = replica_id.lstrip("u")

    return f"pod/cluster-{cluster_id}-replica-{replica_id}-{process}"


def cluster_service_name(cluster_id: str, replica_id: str) -> str:
    # Replica IDs now appear as `GlobalId`s in `mz_cluster_replicas`, while
    # the pod names still contain only the numeric ID.
    # TODO(teskje): Change pod names to contain the replica `GlobalId`.
    replica_id = replica_id.lstrip("u")

    return f"service/cluster-{cluster_id}-replica-{replica_id}"
