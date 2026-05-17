import Lake
open Lake DSL

package «mz-semantics» where
  -- No options needed yet.

@[default_target]
lean_lib «Mz» where
  -- Library auto-discovers files under `Mz/`.
