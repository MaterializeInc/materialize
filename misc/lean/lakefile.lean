-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

import Lake
open Lake DSL System

package mz where
  -- Makes `sorry` and every other warning fail the build, so an unfinished
  -- proof cannot pass CI.
  leanOptions := #[⟨`warningAsError, true⟩]

-- Builds every module under `Mz/`, not only those `Mz.lean` imports, so an
-- unimported model cannot skip the check.
@[default_target]
lean_lib Mz where
  globs := #[.andSubmodules `Mz]

def designDir : FilePath := __dir__ / ".." / ".." / "doc" / "developer" / "design"

/-- The design doc directories that contain at least one Lean file. -/
def designModelDirs : IO (Array String) := do
  if !(← designDir.pathExists) then
    return #[]
  let mut dirs := #[]
  for entry in ← designDir.readDir do
    if ← entry.path.isDir then
      let files ← entry.path.readDir
      if files.any (·.path.extension == some "lean") then
        dirs := dirs.push entry.fileName
  return dirs.qsort (· < ·)

-- Models attached to design docs, `doc/developer/design/<dir>/*.lean`. Each
-- directory is a module namespace, so a model's module is
-- `«<dir>».<File>`. The directories are listed when Lake elaborates this
-- file, and Lake caches the result. NOTE: run `lake -R build` after adding a
-- directory, or the new models are not built.
@[default_target]
lean_lib Design where
  srcDir := designDir
  roots := #[]
  globs := (run_io designModelDirs).map fun dir => .submodules (Lean.Name.mkSimple dir)
