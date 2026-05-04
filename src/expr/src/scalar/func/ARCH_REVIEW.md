# Architecture review — `expr::scalar::func`

Scope: `src/expr/src/scalar/func/` and `impls/` (≈ 12,392 LOC of Rust).

## 1. Aspirational `enum_dispatch` migration is stalled

**Files**
- `src/expr/src/scalar/func/unary.rs:25` — `// This trait will eventually be
  annotated with #[enum_dispatch] to autogenerate the UnaryFunc enum`
- `src/expr/src/scalar/func/binary.rs:18` — same comment
- `src/expr/src/scalar/func/macros.rs:141-145` — `derive_unary!` is explicitly
  described as "temporary" and the comment says to replace with `enum_dispatch`

**Observation**
The `derive_unary!`, `derive_binary!`, and `derive_variadic!` macros each
generate an enum + ~10 parallel `match self { Self::$name(f) => f.method(), }`
arms. Every new variant added to `UnaryFunc` (329 variants), `BinaryFunc`
(222 variants), or `VariadicFunc` requires no macro changes — the macro handles
it. The `#[sqlfunc]` proc-macro further automates per-function boilerplate.

So the stall is **not causing current friction**: adding a function works
cleanly today. The risk is that the "temporary" comment creates the impression
of future work that may never land, and `enum_dispatch` as a crate is
not in `Cargo.toml` at all.

**Recommendation**
Either (a) remove the "will eventually use `enum_dispatch`" comments and
promote the current macro approach to official design, or (b) file a tracking
issue and link it from the comments so the intent is findable. As-is,
the comments are aspirational noise.

**Deletion test passes**: renaming the comment is not a relocation. The macros
function correctly and new functions are added without friction.

## 2. `introduces_nulls` conservatism in cast impls

**Files**
- `src/expr/src/scalar/func/impls/list.rs:70,147,229` — three separate
  `TODO? if typeconv was in expr, we could determine this` comments
- `src/expr/src/scalar/func/impls/array.rs:90,197` — same
- `src/expr/src/scalar/func/impls/range.rs:70` — same

**Observation**
`CastStringToList`, `CastList1ToList2`, `CastArrayToArray`, and similar
container-cast functions return `introduces_nulls = true` conservatively
because their accurate answer depends on `mz-sql::typeconv`, which is in a
different crate. This causes the optimizer to treat these casts as potentially
null-introducing even when the element types are non-nullable, which can
suppress null-elision rewrites.

**Seam**: `mz-expr` (evaluation layer) and `mz-sql` (type-conversion policy)
are intentionally decoupled; `typeconv` lives in `mz-sql` to avoid a cycle.
The fix would require either moving type-conversion metadata into `mz-repr`
(shared) or threading element-type nullable information into the function
struct at construction time.

**Recommendation**
Document the constraint in a single place (e.g., a module-level comment in
`impls.rs`) rather than repeating the TODO in 6+ sites. If the optimizer
impact is measurable, evaluate moving cast-nullability metadata into `mz-repr`.

## 3. (Honest skip) 34 per-type submodules in `impls/`

Each submodule corresponds to one SQL scalar type and contains functions whose
inputs or outputs involve that type. The split is mechanical and consistent.
Deletion test fails: collapsing into a single file would put 7K LOC behind one
open buffer. The per-type organization is correct.
