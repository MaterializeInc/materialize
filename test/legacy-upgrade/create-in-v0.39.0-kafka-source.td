# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The linked cluster for the source will be created by the materialize user
# when the source is created.
> CREATE TABLE IF NOT EXISTS linked_cluster_audit_event_user (priority int, user text)
> INSERT INTO linked_cluster_audit_event_user VALUES (39, 'materialize')
