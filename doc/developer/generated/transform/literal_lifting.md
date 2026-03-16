---
source: src/transform/src/literal_lifting.rs
revision: 703a0c27c8
---

# mz-transform::literal_lifting

Implements `LiteralLifting`, which hoists literal scalar expressions out of `Map` operators and lifts them up through or around other operators wherever possible.
Lifting literals exposes a narrower, literal-free view of relations to operators like `Join`, enabling better planning, and the literals are re-introduced via a `Map` at a higher level where they can often be absorbed or further lifted.
The transform handles all major operator variants and maintains recursion safety via a `RecursionGuard`.
