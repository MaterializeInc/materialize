# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
    LIKE_PARAM,
    REGEX_FLAG_OPTIONAL_PARAM,
    REGEX_PARAM,
    REPETITIONS_PARAM,
    STRING_TRIM_SPEC_PARAM,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    MaxSignedInt4OperationParam,
)
from materialize.output_consistency.input_data.params.string_operation_param import (
    StringOperationParam,
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
from materialize.output_consistency.input_data.return_specs.string_return_spec import (
    StringReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
    OperationRelevance,
)

STRING_OPERATION_TYPES: list[DbOperationOrFunction] = []

TAG_REGEX = "regex"
TAG_STRING_LIKE_OP = "strlike"

STRING_OPERATION_TYPES.append(
    DbOperation(
        "$ || $",
        [StringOperationParam(), StringOperationParam()],
        StringReturnTypeSpec(),
    )
)

STRING_OPERATION_TYPES.append(
    DbOperation(
        "$ ~ $",
        [StringOperationParam(), REGEX_PARAM],
        BooleanReturnTypeSpec(),
        comment="matches regular expression, case sensitive",
        tags={TAG_REGEX},
    )
)

STRING_OPERATION_TYPES.append(
    DbOperation(
        "$ ~* $",
        [StringOperationParam(), REGEX_PARAM],
        BooleanReturnTypeSpec(),
        comment="matches regular expression, case insensitive",
        tags={TAG_REGEX},
    )
)

STRING_OPERATION_TYPES.append(
    DbOperation(
        "$ !~ $",
        [StringOperationParam(), REGEX_PARAM],
        BooleanReturnTypeSpec(),
        comment="does not match regular expression, case sensitive",
        tags={TAG_REGEX},
    )
)

STRING_OPERATION_TYPES.append(
    DbOperation(
        "$ !~* $",
        [StringOperationParam(), REGEX_PARAM],
        BooleanReturnTypeSpec(),
        comment="does not match regular expression, case insensitive",
        tags={TAG_REGEX},
    )
)

STRING_LIKE_OPERATION = DbOperation(
    "$ LIKE $",
    [StringOperationParam(), LIKE_PARAM],
    BooleanReturnTypeSpec(),
    tags={TAG_STRING_LIKE_OP},
    comment="case-sensitive SQL LIKE matching (equal to: $ ~~ $)",
)
STRING_OPERATION_TYPES.append(STRING_LIKE_OPERATION)

STRING_OPERATION_TYPES.append(
    DbOperation(
        "$ ILIKE $",
        [StringOperationParam(), LIKE_PARAM],
        BooleanReturnTypeSpec(),
        tags={TAG_STRING_LIKE_OP},
        comment="case-insensitive SQL LIKE matching (equal to: $ ~~* $)",
    )
)

STRING_NOT_LIKE_OPERATION = DbOperation(
    "$ NOT LIKE $",
    [StringOperationParam(), LIKE_PARAM],
    BooleanReturnTypeSpec(),
    tags={TAG_STRING_LIKE_OP},
    comment="negative case-sensitive SQL LIKE matching (equal to: $ !~~ $)",
)
STRING_OPERATION_TYPES.append(STRING_NOT_LIKE_OPERATION)

STRING_OPERATION_TYPES.append(
    DbOperation(
        "$ NOT ILIKE $",
        [StringOperationParam(), LIKE_PARAM],
        BooleanReturnTypeSpec(),
        tags={TAG_STRING_LIKE_OP},
        comment="negative case-insensitive SQL LIKE matching (equal to: $ !~~* $)",
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "ascii",
        [StringOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
        relevance=OperationRelevance.LOW,
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "btrim",
        [StringOperationParam(), StringOperationParam(optional=True)],
        StringReturnTypeSpec(),
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "bit_length",
        [StringOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
        relevance=OperationRelevance.LOW,
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "char_length",
        [StringOperationParam()],
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
    StringReturnTypeSpec(),
)
# may introduce a usage of a new line or a backslash
chr_function.added_characteristics.add(
    ExpressionCharacteristics.STRING_WITH_SPECIAL_SPACE_CHARS
)
chr_function.added_characteristics.add(
    ExpressionCharacteristics.STRING_WITH_BACKSLASH_CHAR
)
STRING_OPERATION_TYPES.append(chr_function)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "initcap",
        [StringOperationParam()],
        StringReturnTypeSpec(),
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "left",
        [StringOperationParam(), MaxSignedInt4OperationParam()],
        StringReturnTypeSpec(),
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "length",
        [StringOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)

LOWER_OPERATION = DbFunction(
    "lower",
    [StringOperationParam()],
    StringReturnTypeSpec(),
)
STRING_OPERATION_TYPES.append(LOWER_OPERATION)


class LpadFunction(DbFunction):
    def __init__(self):
        super().__init__(
            "lpad",
            [
                StringOperationParam(),
                # do not use an arbitrary integer to avoid long durations
                REPETITIONS_PARAM,
                StringOperationParam(optional=True),
            ],
            StringReturnTypeSpec(),
        )

    def derive_characteristics(
        self, args: list[Expression]
    ) -> set[ExpressionCharacteristics]:
        length_arg = args[1]
        if isinstance(length_arg, EnumConstant) and length_arg.value == "0":
            return {
                ExpressionCharacteristics.STRING_EMPTY
            } | super().derive_characteristics(args)

        return super().derive_characteristics(args)


STRING_OPERATION_TYPES.append(LpadFunction())

STRING_OPERATION_TYPES.append(
    DbFunction(
        "ltrim",
        [StringOperationParam(), StringOperationParam(optional=True)],
        StringReturnTypeSpec(),
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "octet_length",
        [StringOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
        relevance=OperationRelevance.LOW,
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "parse_ident",
        [StringOperationParam(), BooleanOperationParam(optional=True)],
        ArrayReturnTypeSpec(array_value_type_category=DataTypeCategory.STRING),
        relevance=OperationRelevance.LOW,
        is_enabled=False,
    )
)

STRING_OPERATION_TYPES.append(
    DbOperation(
        "position($ IN $)",
        [StringOperationParam(), StringOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "regexp_match",
        [StringOperationParam(), REGEX_PARAM, REGEX_FLAG_OPTIONAL_PARAM],
        ArrayReturnTypeSpec(array_value_type_category=DataTypeCategory.STRING),
        tags={TAG_REGEX},
    )
)

REGEXP_REPLACE = DbFunction(
    "regexp_replace",
    [
        StringOperationParam(),
        REGEX_PARAM,
        StringOperationParam(),
        REGEX_FLAG_OPTIONAL_PARAM,
    ],
    StringReturnTypeSpec(),
    tags={TAG_REGEX},
)
STRING_OPERATION_TYPES.append(REGEXP_REPLACE)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "regexp_split_to_array",
        [StringOperationParam(), REGEX_PARAM, REGEX_FLAG_OPTIONAL_PARAM],
        ArrayReturnTypeSpec(array_value_type_category=DataTypeCategory.ARRAY),
        tags={TAG_REGEX},
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "repeat",
        [
            StringOperationParam(),
            # do not use an arbitrary integer to avoid crashing mz
            REPETITIONS_PARAM,
        ],
        StringReturnTypeSpec(),
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "replace",
        [StringOperationParam(), StringOperationParam(), StringOperationParam()],
        StringReturnTypeSpec(),
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "right",
        [StringOperationParam(), MaxSignedInt4OperationParam()],
        StringReturnTypeSpec(),
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "rtrim",
        [StringOperationParam(), StringOperationParam(optional=True)],
        StringReturnTypeSpec(),
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "split_part",
        [
            StringOperationParam(),
            StringOperationParam(),
            MaxSignedInt4OperationParam(
                incompatibilities={ExpressionCharacteristics.NEGATIVE}
            ),
        ],
        StringReturnTypeSpec(),
        relevance=OperationRelevance.LOW,
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "substring",
        [
            StringOperationParam(),
            MaxSignedInt4OperationParam(),
            MaxSignedInt4OperationParam(optional=True),
        ],
        StringReturnTypeSpec(),
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "translate",
        [StringOperationParam(), StringOperationParam(), StringOperationParam()],
        StringReturnTypeSpec(),
    )
)

STRING_OPERATION_TYPES.append(
    DbOperation(
        "trim($ $ FROM $)",
        [STRING_TRIM_SPEC_PARAM, StringOperationParam(), StringOperationParam()],
        StringReturnTypeSpec(),
        relevance=OperationRelevance.LOW,
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "upper",
        [StringOperationParam()],
        StringReturnTypeSpec(),
    )
)

STRING_OPERATION_TYPES.append(
    DbFunction(
        "constant_time_eq",
        [StringOperationParam(), StringOperationParam()],
        BooleanReturnTypeSpec(),
        is_pg_compatible=False,
    )
)
