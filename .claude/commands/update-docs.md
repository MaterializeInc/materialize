# Update generated documentation index

Refresh `doc/developer/generated/` to reflect source changes since each doc was last written.
Do **not** modify any source files.

## Procedure

### 1. Detect stale docs

For each `.md` file under `doc/developer/generated/`, read its YAML front-matter (`source` and `revision` fields).
Run:

```sh
git log --oneline <revision>..HEAD -- <source>
```

If there are commits, the doc is **stale**.
Collect the full list of stale files.

### 2. Detect deleted sources

For each `.md` file, check whether the `source` path still exists on disk.
If the source file was deleted, the doc is **orphaned** — delete the `.md` file and remove its `log.md` entry.

### 3. Detect new sources

Compare all `.rs` files under `src/` against the set of documented files.
Any `.rs` file without a corresponding `.md` is **new**.
Group new files by crate.

### 4. Update stale docs

For each stale doc:

1. Re-read the source file.
2. Rewrite the `.md` with updated content and a new `revision` from `git log -1 --format=%h -- <source>`.
3. Append an update entry to `log.md`.

### 5. Create docs for new files

Follow the same bottom-up process from `prompt.md`:

1. Document new leaf files first.
2. If a new directory module appeared, create `_module.md`.
3. If a new crate appeared, create all its docs including `_crate.md`.
4. Update `prompt.md` progress table for new crates.

### 6. Update parent docs if children changed

When leaf docs change significantly (new types, removed functionality), re-read the parent `_module.md` and `_crate.md` to check if they still accurately describe their children.
Update them if needed.

### 7. Update flows.md

If any of the stale files are referenced in `doc/developer/generated/flows.md`, re-read the updated docs and verify the flow descriptions are still accurate.
Update module paths or descriptions that changed.

### 8. Summary

After all updates, print:

```
Updated: N files
Deleted: N orphaned docs
Created: N new docs
Skipped: N unchanged docs
```

## Efficiency tips

* Start by running a single batch command to find all stale docs:
  ```sh
  find doc/developer/generated -name '*.md' -exec sh -c '
    rev=$(head -5 "$1" | grep "^revision:" | awk "{print \$2}")
    src=$(head -5 "$1" | grep "^source:" | awk "{print \$2}")
    if [ -n "$rev" ] && [ -n "$src" ]; then
      changes=$(git log --oneline "$rev..HEAD" -- "$src" 2>/dev/null | wc -l)
      if [ "$changes" -gt 0 ]; then echo "STALE($changes): $1 <- $src"; fi
    fi
  ' _ {} \;
  ```
* Process stale docs in parallel by crate using subagents.
* Skip files with 0 commits since their recorded revision — they are up to date.
