use once_cell::sync::Lazy;
use tower_lsp::lsp_types::{
    CompletionItem, CompletionItemKind, CompletionItemLabelDetails, Documentation,
};

// TODO: Turn into Markdown content.
pub static DOCS_LINK: Lazy<Documentation> =
    Lazy::new(|| Documentation::String("https://materialize.com/docs/sql/functions/".to_string()));

/// Contains all the functions
pub static FUNCTIONS: Lazy<Vec<CompletionItem>> = Lazy::new(|| {
    vec![
        CompletionItem {
            label: "CAST (cast_expr) -> T".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Generic function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Value as type `T`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "coalesce(x: T...) -> T?".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Generic function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("First non-_NULL_ arg, or _NULL_ if all are _NULL_".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "greatest(x: T...) -> T?".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Generic function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The maximum argument, or _NULL_ if all are _NULL_".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "least(x: T...) -> T?".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Generic function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The minimum argument, or _NULL_ if all are _NULL_".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "nullif(x: T, y: T) -> T?".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Generic function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("_NULL_ if `x == y`, else `x`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "array_agg(x: T) -> T[]".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Aggregate values (including nulls) as an array.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "avg(x: T) -> U".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Average of `T`'s values. <br><br> Returns `numeric` if `x` is `int`, `double` if `x` is `real`, else returns same type as `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "bool_and(x: T) -> T".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("_NULL_ if all values of `x` are _NULL_, otherwise true if all values of `x` are true, otherwise false.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "bool_or(x: T) -> T".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("_NULL_ if all values of `x` are _NULL_, otherwise true if any values of `x` are true, otherwise false.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "count(x: T) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Number of non-_NULL_ inputs.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_agg(expression) -> jsonb".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Aggregate values (including nulls) as a jsonb array.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_object_agg(keys, values) -> jsonb".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Aggregate keys and values (including nulls) as a jsonb object.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "max(x: T) -> T".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Maximum value among `T`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "min(x: T) -> T".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Minimum value among `T`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "stddev(x: T) -> U".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Historical alias for `stddev_samp`. *(imprecise)* <br><br> Returns `numeric` if `x` is `int`, `double` if `x` is `real`, else returns same type as `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "stddev_pop(x: T) -> U".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Population standard deviation of `T`'s values. *(imprecise)* <br><br> Returns `numeric` if `x` is `int`, `double` if `x` is `real`, else returns same type as `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "stddev_samp(x: T) -> U".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Sample standard deviation of `T`'s values. *(imprecise)* <br><br> Returns `numeric` if `x` is `int`, `double` if `x` is `real`, else returns same type as `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "string_agg(value: text, delimiter: text) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Concatenates the non-null input values into text. Each value after the first is preceded by the corresponding delimiter.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "sum(x: T) -> U".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Sum of `T`'s values <br><br> Returns `bigint` if `x` is `int` or `smallint`, `numeric` if `x` is `bigint` or `uint8`, `uint8` if `x` is `uint4` or `uint2`, else returns same type as `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "variance(x: T) -> U".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Historical alias for `var_samp`. *(imprecise)* <br><br> Returns `numeric` if `x` is `int`, `double` if `x` is `real`, else returns same type as `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "var_pop(x: T) -> U".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Population variance of `T`'s values. *(imprecise)* <br><br> Returns `numeric` if `x` is `int`, `double` if `x` is `real`, else returns same type as `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "var_samp(x: T) -> U".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Aggregate function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Sample variance of `T`'s values. *(imprecise)* <br><br> Returns `numeric` if `x` is `int`, `double` if `x` is `real`, else returns same type as `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "list_agg(x: any) -> L".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("List function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Aggregate values (including nulls) as a list. ([docs](/sql/functions/list_agg))".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "list_append(l: listany, e: listelementany) -> L".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("List function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Appends `e` to `l`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "list_cat(l1: listany, l2: listany) -> L".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("List function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Concatenates `l1` and `l2`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "list_length(l: listany) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("List function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Return the number of elements in `l`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "list_prepend(e: listelementany, l: listany) -> listany".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("List function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Prepends `e` to `l`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "map_length(m: mapany) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Map function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Return the number of elements in `m`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "abs(x: N) -> N".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The absolute value of `x`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "cbrt(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The cube root of `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "ceil(x: N) -> N".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The smallest integer >= `x`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "ceiling(x: N) -> N".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Alias of `ceil`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "exp(x: N) -> N".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Exponential of `x` (e raised to the given power)".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "floor(x: N) -> N".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The largest integer <= `x`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "ln(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Natural logarithm of `x`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "ln(x: numeric) -> numeric".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Natural logarithm of `x`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "log(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Base 10 logarithm of `x`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "log(x: numeric) -> numeric".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Base 10 logarithm of `x`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "log10(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Base 10 logarithm of `x`, same as `log`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "log10(x: numeric) -> numeric".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Base 10 logarithm of `x`, same as `log`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "log(b: numeric, x: numeric) -> numeric".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Base `b` logarithm of `x`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "mod(x: N, y: N) -> N".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`x % y`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pow(x: double precision, y: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Alias of `power`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pow(x: numeric, y: numeric) -> numeric".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Alias of `power`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "power(x: double precision, y: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`x` raised to the power of `y`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "power(x: numeric, y: numeric) -> numeric".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`x` raised to the power of `y`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "round(x: N) -> N".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`x` rounded to the nearest whole number. If `N` is `real` or `double precision`, rounds ties to the nearest even number. If `N` is `numeric`, rounds ties away from zero.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "round(x: numeric, y: int) -> numeric".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`x` rounded to `y` decimal places, while retaining the same [`numeric`](../types/numeric) scale; rounds ties away from zero.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "sqrt(x: numeric) -> numeric".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The square root of `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "sqrt(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The square root of `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "trunc(x: N) -> N".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Numbers function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`x` truncated toward zero to a whole number".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "cos(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The cosine of `x`, with `x` in radians.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "acos(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The inverse cosine of `x`, result in radians.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "cosh(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The hyperbolic cosine of `x`, with `x` as a hyperbolic angle.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "acosh(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The inverse hyperbolic cosine of `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "cot(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The cotangent of `x`, with `x` in radians.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "sin(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The sine of `x`, with `x` in radians.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "asin(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The inverse sine of `x`, result in radians.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "sinh(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The hyperbolic sine of `x`, with `x` as a hyperbolic angle.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "asinh(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The inverse hyperbolic sine of `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "tan(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The tangent of `x`, with `x` in radians.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "atan(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The inverse tangent of `x`, result in radians.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "tanh(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The hyperbolic tangent of `x`, with `x` as a hyperbolic angle.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "atanh(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The inverse hyperbolic tangent of `x`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "radians(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Converts degrees to radians.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "degrees(x: double precision) -> double precision".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Trigonometric function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Converts radians to degrees.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "ascii(s: str) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The ASCII value of `s`'s left-most character".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "btrim(s: str) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Trim all spaces from both sides of `s`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "btrim(s: str, c: str) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Trim any character in `c` from both sides of `s`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "bit_length(s: str) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Number of bits in `s`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "bit_length(b: bytea) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Number of bits in `b`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "char_length(s: str) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Number of code points in `s`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "chr(i: int) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Character with the given Unicode codepoint. Only supports codepoints that can be encoded in UTF-8. The NULL (0) character is not allowed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "concat(f: any, r: any...) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Concatenates the text representation of non-NULL arguments`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "concat_ws(sep: str, f: any, r: any...) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Concatenates the text representation of non-NULL arguments from `f` and `r` separated by `sep`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "convert_from(b: bytea, src_encoding: text) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Convert data `b` from original encoding specified by `src_encoding` into `text`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "decode(s: text, format: text) -> bytea".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Decode `s` using the specified textual representation.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "encode(b: bytea, format: text) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Encode `b` using the specified textual representation.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "get_byte(b: bytea, n: int) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Return the `n`th byte from `b`, where the left-most byte in `b` is at the 0th position.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "left(s: str, n: int) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The first `n` characters of `s`. If `n` is negative, all but the last `|n|` characters of `s`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "length(s: str) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Number of code points in `s`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "length(b: bytea) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Number of bytes in `s`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "length(s: bytea, encoding_name: str) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Number of code points in `s` after encoding".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "lower(s: str) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Convert `s` to lowercase.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "lpad(s: str, len: int) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Prepend `s` with spaces up to length `len`, or right truncate if `len` is less than the length of `s`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "lpad(s: str, len: int, p: str) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Prepend `s` with characters pulled from `p` up to length `len`, or right truncate if `len` is less than the length of `s`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "ltrim(s: str) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Trim all spaces from the left side of `s`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "ltrim(s: str, c: str) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Trim any character in `c` from the left side of `s`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "octet_length(s: str) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Number of bytes in `s`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "octet_length(b: bytea) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Number of bytes in `b`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "parse_ident(ident: str[, strict_mode: bool]) -> str[]".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Splits a qualified identifier into an array of identifiers, removing any quoting of individual identifiers. Extra characters after the last identifier are considered an error, unless `strict_mode` is `false` or elided, then such extra characters are ignored. (This behavior is useful for parsing names for objects like functions.) Note that this function does not truncate over-length identifiers. (From [PG](https://www.postgresql.org/docs/current/functions-string.html))".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "position(sub: str IN s: str) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The starting index of `sub` within `s` or `0` if `sub` is not a substring of `s`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "regexp_match(haystack: str, needle: str [, flags: str]]) -> str[]".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Matches the regular expression `needle` against haystack, returning a string array that contains the value of each capture group specified in `needle`, in order. If `flags` is set to the string `i` matches case-insensitively.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "repeat(s: str, n: int) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Replicate the string `n` times.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "replace(s: str, f: str, r: str) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`s` with all instances of `f` replaced with `r`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "right(s: str, n: int) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The last `n` characters of `s`. If `n` is negative, all but the first `|n|` characters of `s`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "rtrim(s: str) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Trim all spaces from the right side of `s`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "rtrim(s: str, c: str) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Trim any character in `c` from the right side of `s`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "split_part(s: str, d: s, i: int) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Split `s` on delimiter `d`. Return the `str` at index `i`, counting from 1.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "substring(s: str, start_pos: int) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Substring of `s` starting at `start_pos`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "substring(s: str, start_pos: int, l: int) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Substring starting at `start_pos` of length `l`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "substring('s' [FROM 'start_pos']? [FOR 'l']?) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Substring starting at `start_pos` of length `l`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "translate(s: str, from: str, to: str) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Any character in `s` that matches a character in `from` is replaced by the corresponding character in `to`. If `from` is longer than `to`, occurrences of the extra characters in `from` are removed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "trim([BOTH | LEADING | TRAILING]? ['c'? FROM]? 's') -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Trims any character in `c` from `s` on the specified side.<br/><br/>Defaults:<br/> &bull; Side: `BOTH`<br/> &bull; `'c'`: `' '` (space)".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "try_parse_monotonic_iso8601_timestamp(s: str) -> timestamp".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Parses a specific subset of ISO8601 timestamps, returning `NULL` instead of error on failure: `YYYY-MM-DDThh:mm:ss.sssZ`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "upper(s: str) -> str".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("String function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Convert `s` to uppercase.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "expression bool_op ALL(s: Scalars) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Scalar function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`true` if applying [bool_op](#boolean) to `expression` and every value of `s` evaluates to `true`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "expression bool_op ANY(s: Scalars) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Scalar function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`true` if applying [bool_op](#boolean) to `expression` and any value of `s` evaluates to `true`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "expression IN(s: Scalars) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Scalar function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`true` for each value in `expression` if it matches at least one element of `s`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "expression NOT IN(s: Scalars) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Scalar function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`true` for each value in `expression` if it does not match any elements of `s`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "expression bool_op SOME(s: Scalars) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Scalar function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`true` if applying [bool_op](#boolean) to `expression` and any value of `s` evaluates to `true`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "expression bool_op ALL(s: Query) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Subquery function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`s` must return exactly one column; `true` if applying [bool_op](#boolean) to `expression` and every value of `s` evaluates to `true`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "expression bool_op ANY(s: Query) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Subquery function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`s` must return exactly one column; `true` if applying [bool_op](#boolean) to `expression` and any value of `s` evaluates to `true`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "csv_extract(num_csv_col: int, col_name: string) -> col1: string, ... coln: string".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Subquery function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Extracts separated values from a column containing a CSV file formatted as a string.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "EXISTS(s: Query) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Subquery function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`true` if `s` returns at least one row".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "expression IN(s: Query) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Subquery function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`s` must return exactly one column; `true` for each value in `expression` if it matches at least one element of `s`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "NOT EXISTS(s: Query) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Subquery function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`true` if `s` returns zero rows".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "expression NOT IN(s: Query) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Subquery function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`s` must return exactly one column; `true` for each value in `expression` if it does not match any elements of `s`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "expression bool_op SOME(s: Query) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Subquery function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`s` must return exactly one column; `true` if applying [bool_op](#boolean) to `expression` and any value of `s` evaluates to `true`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "age(timestamp, timestamp) -> interval".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Subtracts one timestamp from another, producing a \"symbolic\" result that uses years and months, rather than just days.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "current_timestamp() -> timestamptz".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The `timestamp with time zone` representing when the query was executed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "date_bin(stride: interval, source: timestamp, origin: timestamp) -> timestamp".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Align `source` with `origin` along `stride`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "date_trunc(time_component: str, val: timestamp) -> timestamp".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Largest `time_component` <= `val`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "date_trunc(time_component: str, val: interval) -> interval".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Largest `time_component` <= `val`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "EXTRACT(extract_expr) -> numeric".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Specified time component from value".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "date_part(time_component: str, val: timestamp) -> float".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Specified time component from value".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "mz_now() -> mz_timestamp".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The logical time at which a query executes. Used for temporal filters and query timestamp introspection.
<br><br>
**Note**: This function generally behaves like an unmaterializable function, but
can be used in limited contexts in materialized views as a [temporal filter](/sql/patterns/temporal-filters/).
".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "now() -> timestamptz".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The `timestamp with time zone` representing when the query was executed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "timestamp AT TIME ZONE zone -> timestamptz".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Converts `timestamp` to the specified time zone, expressed as an offset from UTC. <br/><br/>**Known limitation:** You must explicitly cast the type for the time zone.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "timestamptz AT TIME ZONE zone -> timestamp".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Converts `timestamp with time zone` from UTC to the specified time zone, expressed as the local time. <br/><br/>**Known limitation:** You must explicitly cast the type for the time zone.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "timezone(zone, timestamp) -> timestamptz".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Converts `timestamp` to specified time zone, expressed as an offset from UTC. <br/><br/>**Known limitation:** You must explicitly cast the type for the time zone.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "timezone(zone, timestamptz) -> timestamp".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Converts `timestamp with time zone` from UTC to specified time zone, expressed as the local time. <br/><br/>**Known limitation:** You must explicitly cast the type for the time zone.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "to_timestamp(val: double precision) -> timestamptz".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Converts Unix epoch (seconds since 00:00:00 UTC on January 1, 1970) to timestamp".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "to_char(val: timestamp, format: str)".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Converts a timestamp into a string using the specified format.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "justify_days(val: interval) -> interval".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Adjust interval so 30-day time periods are represented as months.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "justify_hours(val: interval) -> interval".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Adjust interval so 24-hour time periods are represented as days.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "justify_interval(val: interval) -> interval".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Date and Time function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Adjust interval using justify_days and justify_hours, with additional sign adjustments.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "uuid_generate_v5(namespace: uuid, name: text) -> uuid".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("UUID function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Generates a [version 5 UUID](https://www.rfc-editor.org/rfc/rfc4122#page-7) (SHA-1) in the given namespace using the specified input name.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "jsonb_agg(expression) -> jsonb".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Aggregate values (including nulls) as a jsonb array.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_array_elements(j: jsonb) -> Col<jsonb>".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`j`'s elements if `j` is an array.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_array_elements_text(j: jsonb) -> Col<string>".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`j`'s elements if `j` is an array.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_array_length(j: jsonb) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Number of elements in `j`'s outermost array.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_build_array(x: ...) -> jsonb".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Output each element of `x` as a `jsonb` array. Elements can be of heterogenous types.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_build_object(x: ...) -> jsonb".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("The elements of x as a `jsonb` object. The argument list alternates between keys and values.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_each(j: jsonb) -> Col<(key: string, value: jsonb)>".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`j`'s outermost elements if `j` is an object.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_each_text(j: jsonb) -> Col<(key: string, value: string)>".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`j`'s outermost elements if `j` is an object.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_object_agg(keys, values) -> jsonb".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Aggregate keys and values (including nulls) as a `jsonb` object.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_object_keys(j: jsonb) -> Col<string>".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`j`'s outermost keys if `j` is an object.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_pretty(j: jsonb) -> string".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Pretty printed (i.e. indented) `j`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_typeof(j: jsonb) -> string".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Type of `j`'s outermost value. One of `object`, `array`, `string`, `number`, `boolean`, and `null`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "jsonb_strip_nulls(j: jsonb) -> jsonb".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`j` with all object fields with a value of `null` removed. Other `null` values remain.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "to_jsonb(v: T) -> jsonb".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("JSON function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("`v` as `jsonb`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "generate_series(start: int, stop: int) -> Col<int>".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Table function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Generate all integer values between `start` and `stop`, inclusive.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "generate_series(start: int, stop: int, step: int) -> Col<int>".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Table function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Generate all integer values between `start` and `stop`, inclusive, incrementing by `step` each time.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "generate_series(start: timestamp, stop: timestamp, step: interval) -> Col<timestamp>".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Table function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Generate all timestamp values between `start` and `stop`, inclusive, incrementing by `step` each time.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "generate_subscripts(a: anyarray, dim: int) -> Col<int>".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Table function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Generates a series comprising the valid subscripts of the `dim`'th dimension of the given array `a`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "regexp_extract(regex: str, haystack: str) -> Col<string>".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Table function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Values of the capture groups of `regex` as matched in `haystack`".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "unnest(a: anyarray)".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Table function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Expands the array `a` into a set of rows.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "unnest(l: anylist)".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Table function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Expands the list `l` into a set of rows.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "array_cat(a1: arrayany, a2: arrayany) -> arrayany".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Array function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Concatenates `a1` and `a2`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "array_fill(anyelement, int[], [, int[]]) -> anyarray".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Array function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns an array initialized with supplied value and dimensions, optionally with lower bounds other than 1. (From [PG](https://www.postgresql.org/docs/current/functions-array.html))".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "array_position(haystack: anycompatiblearray, needle: anycompatible) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Array function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the subscript of `needle` in `haystack`. Returns `null` if not found.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "array_position(haystack: anycompatiblearray, needle: anycompatible, skip: int) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Array function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the subscript of `needle` in `haystack`, skipping the first `skip` elements. Returns `null` if not found.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "array_to_string(a: anyarray, sep: text [, ifnull: text]) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Array function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Concatenates the elements of `array` together separated by `sep`. Null elements are omitted unless `ifnull` is non-null, in which case null elements are replaced with the value of `ifnull`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "array_remove(a: anyarray, e: anyelement) -> anyarray".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Array function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the array `a` without any elements equal to the given value `e`. The array must be one-dimensional. Comparisons are done using IS NOT DISTINCT FROM semantics, so it is possible to remove NULLs.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "digest(data: text, type: text) -> bytea".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Cryptography function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Computes a binary hash of the given text `data` using the specified `type` algorithm. Supported hash algorithms are: `md5`, `sha1`, `sha224`, `sha256`, `sha384`, and `sha512`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "digest(data: bytea, type: text) -> bytea".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Cryptography function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Computes a binary hash of the given bytea `data` using the specified `type` algorithm. The supported hash algorithms are the same as for the text variant of this function.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "hmac(data: text, key: text, type: text) -> bytea".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Cryptography function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Computes a hashed MAC of the given text `data` using the specified `key` and `type` algorithm. Supported hash algorithms are the same as for `digest`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "hmac(data: bytea, key: bytea, type: text) -> bytea".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Cryptography function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Computes a hashed MAC of the given bytea `data` using the specified `key` and `type` algorithm. The supported hash algorithms are the same as for `digest`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "md5(data: bytea) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Cryptography function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Computes the MD5 hash of the given bytea `data`. For PostgreSQL compatibility, returns a hex-encoded value of type `text` rather than `bytea`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "sha224(data: bytea) -> bytea".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Cryptography function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Computes the SHA-224 hash of the given bytea `data`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "sha256(data: bytea) -> bytea".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Cryptography function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Computes the SHA-256 hash of the given bytea `data`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "sha384(data: bytea) -> bytea".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Cryptography function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Computes the SHA-384 hash of the given bytea `data`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "sha512(data: bytea) -> bytea".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Cryptography function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Computes the SHA-512 hash of the given bytea `data`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "dense_rank() -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Window function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the rank of the current row within its partition without gaps, counting from 1. Rows that compare equal will have the same rank.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "first_value(value anycompatible) -> anyelement".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Window function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns `value` evaluated at the first row of the window frame. The default window frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "lag(value anycompatible [, offset integer [, default anycompatible ]]) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Window function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns `value` evaluated at the row that is `offset` rows before the current row within the partition; if there is no such row, instead returns `default` (which must be of a type compatible with `value`). If `offset` is `NULL`, `NULL` is returned instead. Both `offset` and `default` are evaluated with respect to the current row. If omitted, `offset` defaults to 1 and `default` to `NULL`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "last_value(value anycompatible) -> anyelement".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Window function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns `value` evaluated at the last row of the window frame. The default window frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "lead(value anycompatible [, offset integer [, default anycompatible ]]) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Window function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns `value` evaluated at the row that is `offset` rows after the current row within the partition; if there is no such row, instead returns `default` (which must be of a type compatible with `value`). If `offset` is `NULL`, `NULL` is returned instead. Both `offset` and `default` are evaluated with respect to the current row. If omitted, `offset` defaults to 1 and `default` to `NULL`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "rank() -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Window function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the rank of the current row within its partition with gaps (counting from 1): rows that compare equal will have the same rank, and then the rank is incremented by the number of rows that compared equal.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "row_number() -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Window function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the number of the current row within its partition, counting from 1. Rows that compare equal will be ordered in an unspecified way.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "mz_environment_id() -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("System information function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns a string containing a `uuid` uniquely identifying the Materialize environment.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "mz_uptime() -> interval".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("System information function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the length of time that the materialized process has been running.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "mz_version() -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("System information function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the server's version information as a human-readable string.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "mz_version_num() -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("System information function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the server's version as an integer having the format `XXYYYZZ`, where `XX` is the major version, `YYY` is the minor version and `ZZ` is the patch version.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "current_database() -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("System information function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the name of the current database.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "current_role() -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("System information function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Alias for `current_user`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "current_user() -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("System information function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the name of the user who executed the containing query.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "session_user() -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("System information function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the name of the user who initiated the database connection.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "mz_row_size(expr: Record) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("System information function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the number of bytes used to store a row.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "format_type(oid: int, typemod: int) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the canonical SQL name for the type specified by `oid` with `typemod` applied.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "current_schema() -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the name of the first non-implicit schema on the search path, or `NULL` if the search path is empty.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "current_schemas(include_implicit: bool) -> text[]".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the names of the schemas on the search path. The `include_implicit` parameter controls whether implicit schemas like `mz_catalog` and `pg_catalog` are included in the output.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "current_setting(setting_name: text[, missing_ok: bool]) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the value of the named setting or error if it does not exist. If `missing_ok` is true, return NULL if it does not exist.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "obj_description(oid: oid, catalog: text) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("PostgreSQL compatibility shim. Currently always returns `NULL`.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_backend_pid() -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the internal connection ID.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_cancel_backend(connection_id: int) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Cancels an in-progress query on the specified connection ID. Returns whether the connection ID existed (not if it cancelled a query).".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_column_size(expr: any) -> int".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the number of bytes used to store any individual data value.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_get_constraintdef(oid: oid[, pretty: bool]) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the constraint definition for the given `oid`. Currently always returns NULL since constraints aren't supported.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_get_indexdef(index: oid[, column: integer, pretty: bool]) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reconstructs the creating command for an index. (This is a decompiled reconstruction, not the original text of the command.) If column is supplied and is not zero, only the definition of that column is reconstructed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_get_ruledef(rule_oid: oid[, pretty bool]) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reconstructs the creating command for a rule. This function always returns NULL because Materialize does not support rules.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_get_userbyid(role: oid) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the role (user) name for the given `oid`. If no role matches the specified OID, the string `unknown (OID=oid)` is returned.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_get_viewdef(view_name: text[, pretty: bool]) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the underlying SELECT command for the given view.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_get_viewdef(view_oid: oid[, pretty: bool]) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the underlying SELECT command for the given view.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_get_viewdef(view_oid: oid[, wrap_column: integer]) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the underlying SELECT command for the given view.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_has_role([user: name or oid,] role: text or oid, privilege: text) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Alias for `has_role` for PostgreSQL compatibility.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_is_in_recovery() -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns if the a recovery is still in progress.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_table_is_visible(relation: oid) -> boolean".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reports whether the relation with the specified OID is visible in the search path.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_tablespace_location(tablespace: oid) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the path in the file system that the provided tablespace is on.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_type_is_visible(relation: oid) -> boolean".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reports whether the type with the specified OID is visible in the search path.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_function_is_visible(relation: oid) -> boolean".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reports whether the function with the specified OID is visible in the search path.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_typeof(expr: any) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the type of its input argument as a string.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_encoding_to_char(encoding_id: integer) -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("PostgreSQL compatibility shim. Not intended for direct use.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_postmaster_start_time() -> timestamptz".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns the time when the server started.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_relation_size(relation: regclass[, fork: text]) -> bigint".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Disk space used by the specified fork ('main', 'fsm', 'vm', or 'init') of the specified table or index. If no fork is specified, it defaults to 'main'. This function always returns -1 because Materialize does not store tables and indexes on local disk.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "pg_stat_get_numscans(oid: oid) -> bigint".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Number of sequential scans done when argument is a table, or number of index scans done when argument is an index. This function always returns -1 because Materialize does not collect statistics.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "version() -> text".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("PostgreSQL compatibility function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Returns a PostgreSQL-compatible version string.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,
    CompletionItem {
            label: "has_cluster_privilege([role: text or oid,] cluster: text, privilege: text) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Access privilege inquiry function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reports whether the role with the specified role name or OID has the privilege on the cluster with the specified cluster name. If the role is omitted then the `current_role` is assumed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "has_connection_privilege([role: text or oid,] connection: text or oid, privilege: text) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Access privilege inquiry function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reports whether the role with the specified role name or OID has the privilege on the connection with the specified connection name or OID. If the role is omitted then the `current_role` is assumed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "has_database_privilege([role: text or oid,] database: text or oid, privilege: text) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Access privilege inquiry function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reports whether the role with the specified role name or OID has the privilege on the database with the specified database name or OID. If the role is omitted then the `current_role` is assumed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "has_schema_privilege([role: text or oid,] schema: text or oid, privilege: text) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Access privilege inquiry function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reports whether the role with the specified role name or OID has the privilege on the schema with the specified schema name or OID. If the role is omitted then the `current_role` is assumed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "has_role([user: name or oid,] role: text or oid, privilege: text) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Access privilege inquiry function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reports whether the `user` has the privilege for `role`. `privilege` can either be `MEMBER` or `USAGE`, however currently this value is ignored. The `PUBLIC` pseudo-role cannot be used for the `user` nor the `role`. If the `user` is omitted then the `current_role` is assumed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "has_secret_privilege([role: text or oid,] secret: text or oid, privilege: text) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Access privilege inquiry function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reports whether the role with the specified role name or OID has the privilege on the secret with the specified secret name or OID. If the role is omitted then the `current_role` is assumed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "has_system_privilege([role: text or oid,] privilege: text) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Access privilege inquiry function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reports whether the role with the specified role name or OID has the system privilege. If the role is omitted then the `current_role` is assumed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "has_table_privilege([role: text or oid,] relation: text or oid, privilege: text) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Access privilege inquiry function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reports whether the role with the specified role name or OID has the privilege on the relation with the specified relation name or OID. If the role is omitted then the `current_role` is assumed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        ,    CompletionItem {
            label: "has_type_privilege([role: text or oid,] type: text or oid, privilege: text) -> bool".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Access privilege inquiry function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Reports whether the role with the specified role name or OID has the privilege on the type with the specified type name or OID. If the role is omitted then the `current_role` is assumed.".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }

]
});
