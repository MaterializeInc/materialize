# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Support for testing Materialize under Antithesis.

Rendering the Kubernetes manifests (`render`), building and publishing the
images (`images`), classifying SQL errors (`sql`), and the pure models that
the driver's checks judge results with. The driver itself is
`materialize.antithesis_driver`. Nothing here asserts or depends on the
Antithesis runtime.
"""
