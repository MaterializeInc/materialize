# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.input_data.params.bytea_operation_param import (
    ByteaOperationParam,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    HASH_ALGORITHM_PARAM,
)
from materialize.output_consistency.input_data.params.string_operation_param import (
    StringOperationParam,
)
from materialize.output_consistency.input_data.return_specs.bytea_return_spec import (
    ByteaReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.string_return_spec import (
    StringReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperationOrFunction,
)

CRYPTO_OPERATION_TYPES: list[DbOperationOrFunction] = []

CRYPTO_OPERATION_TYPES.append(
    DbFunction(
        "digest",
        [StringOperationParam(), HASH_ALGORITHM_PARAM],
        ByteaReturnTypeSpec(),
    )
)

CRYPTO_OPERATION_TYPES.append(
    DbFunction(
        "digest",
        [ByteaOperationParam(), HASH_ALGORITHM_PARAM],
        ByteaReturnTypeSpec(),
    )
)

CRYPTO_OPERATION_TYPES.append(
    DbFunction(
        "hmac",
        [StringOperationParam(), StringOperationParam(), HASH_ALGORITHM_PARAM],
        ByteaReturnTypeSpec(),
    )
)

CRYPTO_OPERATION_TYPES.append(
    DbFunction(
        "hmac",
        [ByteaOperationParam(), ByteaOperationParam(), HASH_ALGORITHM_PARAM],
        ByteaReturnTypeSpec(),
    )
)

CRYPTO_OPERATION_TYPES.append(
    DbFunction(
        "md5",
        [ByteaOperationParam()],
        StringReturnTypeSpec(),
    )
)

CRYPTO_OPERATION_TYPES.append(
    DbFunction(
        "sha224",
        [ByteaOperationParam()],
        ByteaReturnTypeSpec(),
    )
)

CRYPTO_OPERATION_TYPES.append(
    DbFunction(
        "sha256",
        [ByteaOperationParam()],
        ByteaReturnTypeSpec(),
    )
)

CRYPTO_OPERATION_TYPES.append(
    DbFunction(
        "sha384",
        [ByteaOperationParam()],
        ByteaReturnTypeSpec(),
    )
)

CRYPTO_OPERATION_TYPES.append(
    DbFunction(
        "sha512",
        [ByteaOperationParam()],
        ByteaReturnTypeSpec(),
    )
)
