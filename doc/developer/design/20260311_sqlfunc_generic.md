# Generic type parameters in `#[sqlfunc]`

## The problem

SQL functions on arrays, lists, maps, and ranges are polymorphic: `list_list_concat` works on `list<int4>`, `list<text>`, and every other list type.
Today, these functions use erased types (`DatumList<'a>`, `Datum<'a>`) and manually reconstruct output types via `output_type_expr`.

This has three consequences:

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

## Solution proposal

### Overview

Two coordinated changes:

1. **Add a phantom type parameter `T`** with default `Datum<'a>` to `DatumList`, `Array`, and `DatumMap`, matching the pattern already established by `Range<D>`.
2. **Extend `#[sqlfunc]`** to detect `T` in function signatures and auto-derive `output_type_expr` from the structural relationship between input and output types.

The key insight is that no type erasure is needed in the macro.
Since `DatumList<'a, T>` defaults `T = Datum<'a>`, a function like `fn foo<'a, T>(a: DatumList<'a, T>) -> T` compiles as-is when called with `T = Datum<'a>`.
The macro only needs to analyze the generic parameter's position to derive the output type, then call the function normally.

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
The existing manual trait impls (`Clone`, `Copy`, `Debug`, `PartialEq`, `Eq`, `Hash`, `Ord`, `PartialOrd`) just need the added `T` parameter that they ignore:

```rust
impl<'a, T> Clone for DatumList<'a, T> { ... }  // body unchanged
impl<'a, T> Copy for DatumList<'a, T> {}
impl<'a, T> Debug for DatumList<'a, T> { ... }
// etc.
```

`InputDatumType` and `OutputDatumType` impls stay on `DatumList<'a>` (which is `DatumList<'a, Datum<'a>>` via the default):

```rust
// Unchanged — DatumList<'a> == DatumList<'a, Datum<'a>>
impl<'a, E> InputDatumType<'a, E> for DatumList<'a> { ... }
impl<'a, E> OutputDatumType<'a, E> for DatumList<'a> { ... }
```

#### `Array`

```rust
pub struct Array<'a, T = Datum<'a>> {
    pub(crate) elements: DatumList<'a, T>,
    pub(crate) dims: ArrayDimensions<'a>,
}
```

`Array` contains `DatumList<'a>` as a field, which naturally becomes `DatumList<'a, T>`, propagating the type parameter without an extra `PhantomData`.
`Array` currently derives `Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd`.
With the default, `Array<'a>` remains valid and the derives still work.

#### `DatumMap`

```rust
pub struct DatumMap<'a, T = Datum<'a>> {
    data: &'a [u8],
    _phantom: PhantomData<fn() -> T>,
}
```

Same pattern as `DatumList`.
`T` represents the value type (keys are always strings).

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
#[sqlfunc(sqlname = "rangelower", is_monotone = true, introduces_nulls = true)]
fn range_lower<'a, T>(a: Range<T>) -> Option<T> { ... }
```

```rust
#[sqlfunc(sqlname = "->", is_infix_op = true, propagates_nulls = true, introduces_nulls = true)]
fn map_get_value<'a, T>(a: DatumMap<'a, T>, target_key: &str) -> T { ... }
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
2. **Analyzes** the structural position of `T` in parameter types and the return type.
3. **Derives `output_type_expr`** from the relationship (see below).
4. **Strips `T`** from the generated unit struct (the struct is not generic).
5. **Emits the function unchanged** — it compiles because `T` defaults to `Datum<'a>`.

In the generated trait impl, the `call` method invokes the function.
Since `T = Datum<'a>` via the default, the types align:

```rust
impl crate::func::binary::EagerBinaryFunc for ListListConcat {
    type Input<'a> = (Option<DatumList<'a>>, Option<DatumList<'a>>);
    type Output<'a> = Option<DatumList<'a>>;

    fn call<'a>(&self, (a, b): Self::Input<'a>, temp_storage: &'a RowArena) -> Self::Output<'a> {
        // DatumList<'a> == DatumList<'a, Datum<'a>> == DatumList<'a, T> where T = Datum<'a>
        list_list_concat(a, b, temp_storage)
    }
    // ...
}
```

#### Output type derivation

The macro classifies each parameter type and the return type:

* **Container with T:** `DatumList<'a, T>`, `Array<'a, T>`, `DatumMap<'a, T>`, `Range<T>`
* **Bare T:** the element type itself
* **Concrete:** `&str`, `i64`, `bool`, etc. — not polymorphic

Then it finds the first input parameter whose type contains `T` in a container, and derives the output type expression:

| Output type | Source container at position `i` | Generated `output_type_expr` |
|---|---|---|
| `DatumList<'a, T>` | `DatumList<'a, T>` | `input_types[i].scalar_type.without_modifiers()` |
| `Array<'a, T>` | `Array<'a, T>` | `input_types[i].scalar_type.without_modifiers()` |
| Bare `T` | `DatumList<'a, T>` | `input_types[i]...unwrap_list_element_type().clone()` |
| Bare `T` | `Array<'a, T>` | `input_types[i]...unwrap_array_element_type().clone()` |
| Bare `T` | `Range<T>` | `input_types[i]...unwrap_range_element_type().clone()` |
| Bare `T` | `DatumMap<'a, T>` | `input_types[i]...unwrap_map_value_type().clone()` |

Nullability is derived from the return type: `Option<...>` → `.nullable(true)`, otherwise `.nullable(false)`.
For unary functions, `input_types[i]` becomes `input_type`.
If the user specifies an explicit `output_type_expr`, it takes precedence (escape hatch for complex cases).

### Affected functions

Functions that can adopt the generic parameter (removing `output_type_expr`):

| Function | Current signature | New signature |
|---|---|---|
| `list_list_concat` | `(Option<DatumList<'a>>, Option<DatumList<'a>>)` | `<T>(Option<DatumList<'a, T>>, Option<DatumList<'a, T>>)` |
| `list_element_concat` | `(Option<DatumList<'a>>, Datum<'a>)` | `<T>(Option<DatumList<'a, T>>, T)` |
| `element_list_concat` | `(Datum<'a>, Option<DatumList<'a>>)` | `<T>(T, Option<DatumList<'a, T>>)` |
| `list_remove` | `(DatumList<'a>, Datum<'a>)` | `<T>(DatumList<'a, T>, T)` |
| `array_array_concat` | `(Option<Array<'a>>, Option<Array<'a>>)` | `<T>(Option<Array<'a, T>>, Option<Array<'a, T>>)` |
| `array_remove` | `(Array<'a>, Datum<'a>)` | `<T>(Array<'a, T>, T)` |
| `range_lower` | `(Range<Datum<'a>>)` | `<T>(Range<T>)` |
| `range_upper` | `(Range<Datum<'a>>)` | `<T>(Range<T>)` |
| `map_get_value` | `(DatumMap<'a>, &str)` | `<T>(DatumMap<'a, T>, &str)` |

Functions that should NOT be migrated:
* `array_create`, `array_fill`, `list_create`, `record_create`, `map_build_keys` — use stored `self.elem_type` fields on external structs.
* `regexp_split_to_array`, `string_to_array` — fixed output element types.
* `jsonb_add`, `jsonb_mul`, `jsonb_sub` — `without_modifiers()` here is about numeric precision, not container element types.

### Implementation plan

1. **Add phantom `T` to `DatumList`, `Array`, `DatumMap` in `mz_repr`.**
   Update manual trait impls to carry the extra parameter.
   Verify everything compiles with `cargo check`.

2. **Add output type derivation logic to `sqlfunc.rs`.**
   Detect generic type params, analyze their positions, generate `output_type_expr`.
   Strip `T` from the generated struct's generics.

3. **Migrate functions** one category at a time: ranges (simplest, `Range` is already generic) → lists → arrays → maps.
   Each migration removes `output_type_expr` and adds `<T>` with typed containers.

4. **Add snapshot tests** for representative migrated functions.

## Minimal viable prototype

Start with `range_lower` and `range_upper` — they already use `Range<D>` which is generic.
Only step 2 of the implementation plan is needed (no phantom data changes).
This validates the macro changes in isolation before touching `mz_repr`.

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

## Open questions

* **`list_index`:** Uses `unwrap_list_nth_layer_type(input_types.len() - 1)` — depends on the number of index arguments.
  This cannot be expressed with a simple `T` and should keep using explicit `output_type_expr`.
* **`Array` derives:** `Array` currently derives traits.
  Adding `T` means the derives add `T: Clone`, `T: Debug`, etc. bounds.
  Since `T` defaults to `Datum<'a>` which satisfies all bounds, existing code compiles, but the extra bounds exist on the impls.
  If this is a problem, switch `Array` to manual impls (like `DatumList`).
