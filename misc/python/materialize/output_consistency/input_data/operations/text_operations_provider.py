# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mz_version import MzVersion
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.enum.enum_constant import EnumConstant
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.params.boolean_operation_param import (
    BooleanOperationParam,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    REGEX_FLAG_PARAM,
    REGEX_PARAM,
    REPETITIONS_PARAM,
    TEXT_TRIM_SPEC_PARAM,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    MaxSignedInt4OperationParam,
)
from materialize.output_consistency.input_data.params.text_operation_param import (
    TextOperationParam,
)
from materialize.output_consistency.input_data.return_specs.array_return_spec import (
    ArrayReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.boolean_return_spec import (
    BooleanReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.text_return_spec import (
    TextReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
    OperationRelevance,
)

TEXT_OPERATION_TYPES: list[DbOperationOrFunction] = []

TAG_REGEX = "regex"

TEXT_OPERATION_TYPES.append(
    DbOperation(
        "$ || $",
        [TextOperationParam(), TextOperationParam()],
        TextReturnTypeSpec(),
    )
)

# Matches regular expression, case sensitive
TEXT_OPERATION_TYPES.append(
    DbOperation(
        "$ ~ $",
        [TextOperationParam(), REGEX_PARAM],
        BooleanReturnTypeSpec(),
        tags={TAG_REGEX},
    )
)

# Matches regular expression, case insensitive
TEXT_OPERATION_TYPES.append(
    DbOperation(
        "$ ~* $",
        [TextOperationParam(), REGEX_PARAM],
        BooleanReturnTypeSpec(),
        tags={TAG_REGEX},
    )
)

# Does not match regular expression, case sensitive
TEXT_OPERATION_TYPES.append(
    DbOperation(
        "$ !~ $",
        [TextOperationParam(), REGEX_PARAM],
        BooleanReturnTypeSpec(),
    )
)

# Does not match regular expression, case insensitive
TEXT_OPERATION_TYPES.append(
    DbOperation(
        "$ !~* $",
        [TextOperationParam(), REGEX_PARAM],
        BooleanReturnTypeSpec(),
    )
)

# Matches LIKE pattern case sensitively, using SQL LIKE matching
TEXT_OPERATION_TYPES.append(
    DbOperation(
        "$ ~~ $",
        [TextOperationParam(), REGEX_PARAM],
        BooleanReturnTypeSpec(),
    )
)

# Matches LIKE pattern case insensitively (ILIKE), using SQL LIKE matching
TEXT_OPERATION_TYPES.append(
    DbOperation(
        "$ ~~* $",
        [TextOperationParam(), REGEX_PARAM],
        BooleanReturnTypeSpec(),
    )
)

# Does not match LIKE pattern case sensitively, using SQL LIKE matching
TEXT_OPERATION_TYPES.append(
    DbOperation(
        "$ !~~ $",
        [TextOperationParam(), REGEX_PARAM],
        BooleanReturnTypeSpec(),
    )
)

#  Does not match LIKE pattern case insensitively (ILIKE), using SQL LIKE matching
TEXT_OPERATION_TYPES.append(
    DbOperation(
        "$ !~~* $",
        [TextOperationParam(), REGEX_PARAM],
        BooleanReturnTypeSpec(),
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "ascii",
        [TextOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
        relevance=OperationRelevance.LOW,
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "btrim",
        [TextOperationParam(), TextOperationParam(optional=True)],
        TextReturnTypeSpec(),
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "bit_length",
        [TextOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
        relevance=OperationRelevance.LOW,
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "char_length",
        [TextOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)

chr_function = DbFunction(
    "chr",
    [
        MaxSignedInt4OperationParam(
            incompatibilities={
                ExpressionCharacteristics.NULL,
                ExpressionCharacteristics.MAX_VALUE,
                ExpressionCharacteristics.ZERO,
            }
        )
    ],
    TextReturnTypeSpec(),
)
# may introduce a usage of a new line or a backslash
chr_function.added_characteristics.add(
    ExpressionCharacteristics.TEXT_WITH_SPECIAL_SPACE_CHARS
)
chr_function.added_characteristics.add(
    ExpressionCharacteristics.TEXT_WITH_BACKSLASH_CHAR
)
TEXT_OPERATION_TYPES.append(chr_function)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "left",
        [TextOperationParam(), MaxSignedInt4OperationParam()],
        TextReturnTypeSpec(),
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "length",
        [TextOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "lower",
        [TextOperationParam()],
        TextReturnTypeSpec(),
    )
)


class LpadFunction(DbFunction):
    def __init__(self):
        super().__init__(
            "lpad",
            [
                TextOperationParam(),
                # do not use an arbitrary integer to avoid long durations
                REPETITIONS_PARAM,
                TextOperationParam(optional=True),
            ],
            TextReturnTypeSpec(),
        )

    def derive_characteristics(
        self, args: list[Expression]
    ) -> set[ExpressionCharacteristics]:
        length_arg = args[1]
        if isinstance(length_arg, EnumConstant) and length_arg.value == "0":
            return {
                ExpressionCharacteristics.TEXT_EMPTY
            } | super().derive_characteristics(args)

        return super().derive_characteristics(args)


TEXT_OPERATION_TYPES.append(LpadFunction())

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "ltrim",
        [TextOperationParam(), TextOperationParam(optional=True)],
        TextReturnTypeSpec(),
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "octet_length",
        [TextOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
        relevance=OperationRelevance.LOW,
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "parse_ident",
        [TextOperationParam(), BooleanOperationParam(optional=True)],
        ArrayReturnTypeSpec(DataTypeCategory.TEXT),
        relevance=OperationRelevance.LOW,
        is_enabled=False,
    )
)

TEXT_OPERATION_TYPES.append(
    DbOperation(
        "position($ IN $)",
        [TextOperationParam(), TextOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "regexp_match",
        [TextOperationParam(), REGEX_PARAM, REGEX_FLAG_PARAM],
        ArrayReturnTypeSpec(DataTypeCategory.TEXT),
        tags={TAG_REGEX},
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "regexp_replace",
        [TextOperationParam(), REGEX_PARAM, TextOperationParam()],
        ArrayReturnTypeSpec(DataTypeCategory.TEXT),
        tags={TAG_REGEX},
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "regexp_split_to_array",
        [TextOperationParam(), REGEX_PARAM, REGEX_FLAG_PARAM],
        ArrayReturnTypeSpec(DataTypeCategory.ARRAY),
        tags={TAG_REGEX},
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "repeat",
        [
            TextOperationParam(),
            # do not use an arbitrary integer to avoid crashing mz
            REPETITIONS_PARAM,
        ],
        TextReturnTypeSpec(),
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "replace",
        [TextOperationParam(), TextOperationParam(), TextOperationParam()],
        TextReturnTypeSpec(),
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "right",
        [TextOperationParam(), MaxSignedInt4OperationParam()],
        TextReturnTypeSpec(),
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "rtrim",
        [TextOperationParam(), TextOperationParam(optional=True)],
        TextReturnTypeSpec(),
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "split_part",
        [
            TextOperationParam(),
            TextOperationParam(),
            MaxSignedInt4OperationParam(
                incompatibilities={ExpressionCharacteristics.NEGATIVE}
            ),
        ],
        TextReturnTypeSpec(),
        relevance=OperationRelevance.LOW,
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "substring",
        [
            TextOperationParam(),
            MaxSignedInt4OperationParam(),
            MaxSignedInt4OperationParam(optional=True),
        ],
        TextReturnTypeSpec(),
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "translate",
        [TextOperationParam(), TextOperationParam(), TextOperationParam()],
        TextReturnTypeSpec(),
    )
)

TEXT_OPERATION_TYPES.append(
    DbOperation(
        "trim($ $ FROM $)",
        [TEXT_TRIM_SPEC_PARAM, TextOperationParam(), TextOperationParam()],
        TextReturnTypeSpec(),
        relevance=OperationRelevance.LOW,
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "upper",
        [TextOperationParam()],
        TextReturnTypeSpec(),
    )
)

TEXT_OPERATION_TYPES.append(
    DbFunction(
        "constant_time_eq",
        [TextOperationParam(), TextOperationParam()],
        BooleanReturnTypeSpec(),
        is_pg_compatible=False,
        since_mz_version=MzVersion.parse_mz("v0.77.0"),
    )
)
