import Lake
open Lake DSL

package «formal-type-system» where
  leanOptions := #[
    ⟨`autoImplicit, false⟩
  ]

@[default_target]
lean_lib «FormalTypeSystem» where
  srcDir := "FormalTypeSystem"
