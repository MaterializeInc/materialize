# Generic type parameters in `#[sqlfunc]`

## The problem

SQL functions on arrays, lists, maps, and ranges are polymorphic: `list_list_concat` works on `list<int4>`, `list<text>`, and every other list type.
These functions used erased types (`DatumList<'a>`, `Datum<'a>`) and manually reconstructed output types via `output_type_expr`.

This had three consequences:

* **Readability:** The signature `fn list_element_concat(a: Option<DatumList<'a>>, b: Datum<'a>)` does not express that `b` must be the element type of `a`.
  A reader must consult `output_type_expr` and planner code to understand the type relationship.
* **Error-proneness:** Each polymorphic function manually writes an `output_type_expr` like `input_types[0].scalar_type.without_modifiers().nullable(true)`.
  These expressions are unverified at compile time and have produced bugs when the wrong input index or unwrap method was used.
* **Boilerplate:** ~30 functions carry `output_type_expr` annotations that follow one of a few patterns.
  The macro could derive these automatically if it understood the type relationship.

## Success criteria

* A function like `list_list_concat` can use `DatumList<'a, T>` in its signature, and the macro auto-derives the correct `output_type_expr`.
* `fn my_func<'a, T>(list: DatumList<'a, T>) -> T` is inferred to unwrap the element type from the input list type.
* Existing non-generic functions continue to work unchanged.
* No runtime performance regression — `PhantomData` is zero-sized and the generic parameter defaults to `Datum<'a>`.

## Out of scope

* Automatically deriving `output_type_expr` for the "construct" pattern (e.g., `array_create`, `record_create`), where the function has a stored `elem_type` field on an external struct.
  These already work correctly with `self.elem_type`.
* Supporting multiple independent generic parameters (e.g., `fn foo<K, V>(...)` for maps).
  This can be added later if needed.

## Implementation

### Part 1: phantom type parameters on container types

#### `DatumList`

```rust
pub struct DatumList<'a, T = Datum<'a>> {
    data: &'a [u8],
    _phantom: PhantomData<fn() -> T>,
}
```

Using `PhantomData<fn() -> T>` rather than `PhantomData<T>` makes the type covariant in `T` without implying ownership, which avoids `Drop` check complications.
`PhantomData<fn() -> T>` is unconditionally `Copy + Clone + Send + Sync`, so no additional bounds are needed on the manual trait impls.

Since the default is `T = Datum<'a>`, every existing use of `DatumList<'a>` continues to compile unchanged.
The existing manual trait impls (`Clone`, `Copy`, `Debug`, `PartialEq`, `Eq`, `Hash`, `Ord`, `PartialOrd`) carry the extra `T` parameter that they ignore.

A private `DatumList::new(data)` constructor centralizes `PhantomData` bookkeeping.

#### Typed iteration

`DatumList` and `DatumMap` provide `typed_iter()` which returns `DatumListTypedIter<'a, T>` or `DatumDictTypedIter<'a, T>`.
These wrap the untyped iterators and convert each element via `T::from_datum()`.

The `FromDatum<'a>` trait requires `Borrow<Datum<'a>>` as a supertrait, enabling typed elements to be used wherever raw `Datum` values are expected.
Since `T` is erased to `Datum<'a>` at runtime, the `from_datum` conversion is an identity and optimizes away.

```rust
pub trait FromDatum<'a>: Sized + PartialEq + std::borrow::Borrow<Datum<'a>> {
    fn from_datum(datum: Datum<'a>) -> Self;
}
```

#### `make_datum_list`

`RowArena::make_datum_list` accepts `impl IntoIterator<Item = T>` where `T: Borrow<Datum<'a>>`, returning `DatumList<'a, T>`.
This guarantees type safety: only elements of type `T` can be pushed.

```rust
pub fn make_datum_list<'a, T: Borrow<Datum<'a>>>(
    &'a self,
    iter: impl IntoIterator<Item = T>,
) -> DatumList<'a, T>
```

#### `Array`

```rust
pub struct Array<'a, T = Datum<'a>> {
    pub(crate) elements: DatumList<'a, T>,
    pub(crate) dims: ArrayDimensions<'a>,
}
```

`Array` contains `DatumList<'a, T>` as a field, propagating the type parameter without an extra `PhantomData`.
`dims()` and `elements()` are generic over `T`.

#### `DatumMap`

```rust
pub struct DatumMap<'a, T = Datum<'a>> {
    data: &'a [u8],
    _phantom: PhantomData<fn() -> T>,
}
```

Same pattern as `DatumList`.
`T` represents the value type (keys are always strings).
A private `DatumMap::new(data)` constructor centralizes `PhantomData` bookkeeping.

#### `Range`

Already generic (`Range<D>`).
No changes needed.

#### `Datum` enum

```rust
pub enum Datum<'a> {
    List(DatumList<'a>),           // DatumList<'a, Datum<'a>> via default
    Array(Array<'a>),              // Array<'a, Datum<'a>> via default
    Map(DatumMap<'a>),             // DatumMap<'a, Datum<'a>> via default
    Range(Range<DatumNested<'a>>),
    ...
}
```

No changes needed — the defaults keep `Datum` exactly as it is.

### Part 2: macro support for generic functions

#### What the user writes

```rust
#[sqlfunc(sqlname = "||", is_infix_op = true, propagates_nulls = false)]
fn list_list_concat<'a, T>(
    a: Option<DatumList<'a, T>>,
    b: Option<DatumList<'a, T>>,
    temp_storage: &'a RowArena,
) -> Option<DatumList<'a, T>> { ... }
```

```rust
#[sqlfunc(sqlname = "rangelower", is_monotone = true)]
fn range_lower<T>(a: Range<T>) -> Option<T> { ... }
```

```rust
#[sqlfunc(is_infix_op = true, sqlname = "->", propagates_nulls = true)]
fn map_get_value<'a, T: FromDatum<'a>>(a: DatumMap<'a, T>, target_key: &str) -> Option<T> { ... }
```

```rust
#[sqlfunc(sqlname = "||", is_infix_op = true, propagates_nulls = false)]
fn element_list_concat<'a, T>(
    a: T,
    b: Option<DatumList<'a, T>>,
    temp_storage: &'a RowArena,
) -> DatumList<'a, T> { ... }
```

These functions compile as-is because `T = Datum<'a>` is the default for every container type.
No type erasure pass is needed.

#### What the macro does

1. **Detects `T`** as a generic type parameter (not lifetime) in `func.sig.generics`.
2. **Classifies** how `T` appears in each parameter type and the return type using `classify_generic_usage`.
3. **Derives `output_type_expr`** from the structural relationship between input and output types.
4. **Erases `T`** in the generated associated types (replacing with `Datum<'a>`).
5. **Emits the function unchanged** — it compiles because `T` defaults to `Datum<'a>`.

The generated trait impl invokes the function directly.
Since `T = Datum<'a>` via the default, the types align:

```rust
impl crate::func::binary::EagerBinaryFunc for ListListConcat {
    type Input<'a> = (Option<DatumList<'a>>, Option<DatumList<'a>>);
    type Output<'a> = Option<DatumList<'a>>;

    fn call<'a>(&self, (a, b): Self::Input<'a>, temp_storage: &'a RowArena) -> Self::Output<'a> {
        list_list_concat(a, b, temp_storage)
    }
}
```

#### Generic usage classification

The macro classifies each type as one of:

* `Absent` — `T` does not appear
* `Bare` — `T` appears directly (possibly inside `Option`/`Result`)
* `InContainer(TypePath)` — `T` appears inside a generic type that isn't `Option`, `Result`, or `ExcludeNull`

Any generic type wrapping `T` that isn't one of the known wrapper types is treated as a container.
The macro stores the erased container type path (with `T` replaced by `Datum<'a>`) in the `InContainer` variant.
If a type doesn't implement `SqlContainerType`, the generated code won't compile — giving a clear error at the use site rather than a silent wrong answer.

For tuple types like `(T, DatumList<'_, T>)`, the classifier prefers container usages over bare.

#### `SqlContainerType` trait

Each container type implements `SqlContainerType` in `mz_repr`, providing `unwrap_element_type` and `wrap_element_type` associated functions.
The macro emits calls like `<DatumList<'_, Datum<'_>> as SqlContainerType>::unwrap_element_type(&input.scalar_type)`, letting Rust's type system resolve the correct behavior at compile time.
Lifetimes are elided to `'_` in the turbofish position; the compiler infers them from the `&self` lifetime on `output_sql_type`.

#### Output type derivation

The macro finds the first input parameter whose type contains `T` in a container, then derives the output type expression:

| Output usage | Source usage | Generated `output_type_expr` |
|---|---|---|
| Same container | Same container | `input.scalar_type.without_modifiers()` |
| `Bare` | `InContainer(C)` | `<C as SqlContainerType>::unwrap_element_type(&input.scalar_type).clone()` |
| `Bare` | `Bare` | `input.scalar_type.clone()` |
| `InContainer(Out)` | `InContainer(In)` | `<Out>::wrap_element_type(<In>::unwrap_element_type(&input.scalar_type).clone())` |

Nullability is derived from the return type: `Option<...>` → `.nullable(true)`, otherwise `.nullable(false)`.
`introduces_nulls` is also auto-derived from the `Option` wrapper on the output type.
For unary functions, `input_types[i]` becomes `input_type`.
If the user specifies an explicit `output_type_expr`, it takes precedence (escape hatch for complex cases).

### Migrated functions

Functions that have adopted the generic parameter (removing `output_type_expr`):

| Function | Signature |
|---|---|
| `list_list_concat` | `<T>(Option<DatumList<'a, T>>, Option<DatumList<'a, T>>) -> Option<DatumList<'a, T>>` |
| `list_element_concat` | `<T>(Option<DatumList<'a, T>>, T) -> DatumList<'a, T>` |
| `element_list_concat` | `<T>(T, Option<DatumList<'a, T>>) -> DatumList<'a, T>` |
| `list_remove` | `<T>(DatumList<'a, T>, T) -> DatumList<'a, T>` |
| `map_get_value` | `<T: FromDatum<'a>>(DatumMap<'a, T>, &str) -> Option<T>` |
| `cast_array_to_list_one_dim` | `<T>(Array<'a, T>) -> Result<DatumList<'a, T>, EvalError>` |
| `array_index` | `<T: FromDatum<'a>>(Array<'a, T>, Variadic<i64>) -> Option<T>` |
| `range_lower` | `<T>(Range<T>) -> Option<T>` |
| `range_upper` | `<T>(Range<T>) -> Option<T>` |
| `range_empty` | `<T>(Range<T>) -> bool` |
| `range_lower_inc` | `<T>(Range<T>) -> bool` |
| `range_upper_inc` | `<T>(Range<T>) -> bool` |
| `range_lower_inf` | `<T>(Range<T>) -> bool` |
| `range_upper_inf` | `<T>(Range<T>) -> bool` |
| `range_union` | `<T: Copy + Ord + Display + Debug>(Range<T>, Range<T>) -> Result<Range<T>, EvalError>` |
| `range_intersection` | `<T: Copy + Ord + Display + Debug>(Range<T>, Range<T>) -> Range<T>` |

Functions that should NOT be migrated:

* `array_create`, `array_fill`, `list_create`, `record_create`, `map_build_keys` — use stored `self.elem_type` fields on external structs.
* `regexp_split_to_array`, `string_to_array` — fixed output element types.
* `jsonb_add`, `jsonb_mul`, `jsonb_sub` — `without_modifiers()` here is about numeric precision, not container element types.
* `range_difference` — returns `Range<Datum<'a>>` not `Range<T>`, cannot be fully generic.
* `list_index`, `list_slice_linear` — depend on number of index arguments for type computation.

## Alternatives

### Alternative 1: macro-only erasure, no phantom data

The macro rewrites `DatumList<'a, T>` → `DatumList<'a>` before emitting the function.
Container types stay non-generic.

Pros:
* Zero changes to `mz_repr`.

Cons:
* The function as written doesn't compile outside the macro — confusing for tooling (rust-analyzer, goto-definition).
* Can't reuse the typed containers in non-macro contexts.
* Requires a full type-tree rewriting pass in the macro.

### Alternative 2: attribute-only annotation

```rust
#[sqlfunc(polymorphic, sqlname = "||")]
fn list_list_concat<'a>(...) -> ...
```

The `polymorphic` flag triggers `output_type_expr` auto-derivation via heuristics.

Rejected because:
* The type relationship is invisible in the signature.
* Doesn't compose for mixed cases like `element_list_concat` vs `list_element_concat` (different input positions).
