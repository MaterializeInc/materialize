---
source: src/expr/src/virtual_syntax.rs
revision: 439c29db55
---

# mz-expr::virtual_syntax

Provides virtual node abstractions for recovering high-level SQL concepts that are desugared in the MIR.
Defines the `IR` marker trait and the `AlgExcept` trait, which lets IR types expose and reconstruct `EXCEPT`/`EXCEPT ALL` constructs that are otherwise represented as set-difference combinations in the plan tree.
