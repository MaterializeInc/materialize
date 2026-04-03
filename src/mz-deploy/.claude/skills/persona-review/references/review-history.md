# Review History

Track scores and key findings across reviews to measure progress.

## Template

When recording a new review, append a section with this format:

```
## Review: YYYY-MM-DD

### Scores

| Persona | Score | Delta | Key Theme |
|---------|-------|-------|-----------|
| Maya    | X/10  | +/-   | ...       |
| ...     |       |       |           |

### Consensus Issues (raised by 3+)

| Issue | Maya | Jake | Priya | Carlos | Elena | Marcus | Aisha |
|-------|:----:|:----:|:-----:|:------:|:-----:|:------:|:-----:|
| ...   |      |      |       |        |       |        |       |

### Resolved Since Last Review

- ...

### New Issues

- ...
```

---

## Review: 2026-03-10

### Scores

| Persona | Score | Key Theme |
|---------|-------|-----------|
| Maya    | 7.5/10 | CI/CD readiness improved, needs exit codes & JSON on observability commands |
| Jake    | 7.5/10 | Command grouping & aliases great, still needs quickstart workflow & glossary |
| Priya   | 7/10   | --dry-run on deploy resolved #1 ask; deployment locking & --keep-old still blockers |
| Carlos  | 7/10   | Testing & deploy lifecycle best-in-class; no DAG selection is biggest gap vs dbt |
| Elena   | 7.5/10 | Conditional go — phased rollout with CI wrapper; governance gaps for 50+ person org |
| Marcus  | 7.5/10 | Pipeline buildable today; list --output json is #1 missing piece |
| Aisha   | 8.5/10 | Best-in-class help structure & error recovery; --output json coverage incomplete |

### Consensus Issues (raised by 3+)

| Issue | Maya | Jake | Priya | Carlos | Elena | Marcus | Aisha |
|-------|:----:|:----:|:-----:|:------:|:-----:|:------:|:-----:|
| --output json on list/log/describe | yes | | yes | | | yes | yes |
| No exit code documentation | yes | | yes | | | yes | |
| No deployment locking/concurrency | yes | | yes | | yes | | |
| No --keep-old on deploy | yes | | yes | | | | |
| No workflow/quickstart in CLI help | | yes | | yes | | | yes |
| Materialize jargon still undefined | | yes | | yes | | | |

### Resolved Since Last Review

- --dry-run on deploy/promote (all 7 praised)
- --output json for dry-run with CI/CD Usage section
- Command grouping (Getting started / Develop / Infrastructure / Deploy)
- deploy/promote visible in top-level help

---

## Review: 2026-03-10 (second pass)

### Scores

| Persona | Score | Delta | Key Theme |
|---------|-------|-------|-----------|
| Maya    | 8.0/10 | +0.5 | CI/CD dry-run path solid; `--output json` on list/log/describe still missing |
| Jake    | 8.5/10 | +1.0 | Command grouping is a big win; still needs quickstart workflow & glossary |
| Priya   | 7.5/10 | +0.5 | Rollback documented but operationally expensive; no deployment locking |
| Carlos  | 7.5/10 | +0.5 | Test filtering excellent; no DAG selection for `stage` is biggest gap vs dbt |
| Elena   | 8.5/10 | +1.0 | Conditional go with confidence; CI story is now first-class |
| Marcus  | 8.0/10 | +0.5 | Pipeline buildable today; `list --output json` remains #1 blocker |
| Aisha   | 8.8/10 | +0.3 | Near best-in-class help structure; `log` pager is anti-composable |

### Consensus Issues (raised by 3+)

| Issue | Maya | Jake | Priya | Carlos | Elena | Marcus | Aisha |
|-------|:----:|:----:|:-----:|:------:|:-----:|:------:|:-----:|
| --output json on list/log/describe | yes | | yes | | | yes | yes |
| No exit code documentation | yes | | yes | | | yes | |
| No deployment locking/concurrency | yes | | yes | | yes | | |
| No workflow/quickstart in one place | | yes | | yes | | | |
| Materialize jargon undefined (cluster, sink, hydration) | | yes | | yes | | | |
| `log` pager (`less`) breaks pipelines | | | | | | yes | yes |

### Resolved Since Last Review

- --keep-old concern addressed via documented rollback path (git revert → stage → deploy)
- Workflow/quickstart partially resolved via command group ordering
- Test filtering with glob syntax + #test_name pin
- Profiles help now defines what a profile is inline
- wait help explains "ready" concretely (hydrated, within lag, healthy replica)

### New Issues

- `log` pipes through `less` — blocks CI pipelines; no `--no-pager` flag
- `apply network-policies` missing `=` status indicator (copy-edit miss)
- `describe` help page is thin — no flags, no `--output json` guidance
- `profiles` help buries "which profile is active?" after 60+ lines of TOML docs
- `--force` on deploy has no confirmation gate (no `--yes` required)
- No `--latest` convenience flag on wait/deploy
- `deploy` as canonical name vs `promote` — tautological `mz-deploy deploy`
- AI agent skill files scaffolded by `new` unexplained in help
- `wait` alias display wraps at 80 columns

---

## Review: 2026-03-10 (third pass)

### Scores

| Persona | Score | Delta | Key Theme |
|---------|-------|-------|-----------|
| Maya    | 8.7/10 | +0.7 | JSON output resolved; `--dry-run` inconsistent across `apply` subcommands |
| Jake    | 8.0/10 | -0.5 | Materialize jargon still undefined; `lock` still miscategorized |
| Priya   | 8.0/10 | +0.5 | JSON on read commands resolved; `wait --output json` and `--keep-old` still missing |
| Carlos  | 8.2/10 | +0.7 | Diff-based staging substantially addresses DAG selection concern |
| Elena   | 8.75/10 | +0.25 | Stronger conditional go; no `--latest` is daily friction |
| Marcus  | 8.5/10 | +0.5 | JSON coverage resolved (#1 blocker gone); deploy ID capture is new gap |
| Aisha   | 9.1/10 | +0.3 | First persona to cross 9.0; global `--output` flag description misleading |

### Consensus Issues (raised by 3+)

| Issue | Maya | Jake | Priya | Carlos | Elena | Marcus | Aisha |
|-------|:----:|:----:|:-----:|:------:|:-----:|:------:|:-----:|
| No deployment locking/concurrency | yes | | yes | | yes | | |
| Global `--output` flag says "for --dry-run" but applies broader | yes | | | | | yes | yes |
| `wait --output json` missing | | | yes | | yes | yes | |
| Materialize jargon undefined | | yes | | yes | | | |

### Resolved Since Last Review

- `--output json` on list/log/describe/abort (consensus #1 from two prior reviews)
- `promote` as canonical command name (was `deploy`)
- `log` pager TTY-aware (prints directly in non-interactive environments)
- `apply network-policies` now has `=` status indicator
- `describe` help page fleshed out with Flags, `--output json`, examples
- `wait` alias display no longer wraps at 80 columns
- `stage` diff-based deployment explicitly documented (Carlos: DAG selection downgraded)
- `apply` vs `stage` workflow relationship now clear

### New Issues

- Global `--output` flag description says "for --dry-run results" but list/log/describe/abort use it without --dry-run
- `stage` deploy ID capture undocumented for scripts (no machine-readable output from live run)
- `--dry-run` not documented on apply clusters/roles/connections/secrets subcommands
- `stage` description has line-break artifact mid-sentence ("not\nrecreated")
- `list` Related Commands says `mz-deploy deploy` instead of canonical `mz-deploy promote`
- No `MZ_DEPLOY_PROFILE` env var for setting active profile
- `test` exits successfully when filter matches no tests (dangerous in CI)
- `--verbose` secret leak warning absent from `apply secrets` help

---

## Review: 2026-03-10 (fourth pass)

### Scores

| Persona | Score | Delta | Key Theme |
|---------|-------|-------|-----------:|
| Maya    | 8.3/10 | -0.4 | More critical on re-read; `--dry-run` gaps on apply clusters/roles/connections/secrets |
| Priya   | 8.5/10 | +0.5 | `MZ_DEPLOY_PROFILE` and `--output` fixes helped; `wait --output json` still missing |
| Carlos  | 8.5/10 | +0.3 | `--dry-run --output json` on sources/tables appreciated; stage idempotency gap |
| Elena   | 9.0/10 | +0.25 | Crossed 9.0 — conditional go strengthened; deployment locking remains |
| Marcus  | 8.75/10 | +0.25 | `MZ_DEPLOY_PROFILE` was top concern, now resolved; deploy ID capture still gap |
| Aisha   | 9.3/10 | +0.2 | Global `--output` fix and `list` fix helped; stage line-break artifact persists |

### Consensus Issues (raised by 3+)

| Issue | Maya | Priya | Carlos | Elena | Marcus | Aisha |
|-------|:----:|:-----:|:------:|:-----:|:------:|:-----:|
| `--dry-run` missing on apply clusters/roles/connections/secrets | yes | yes | yes | | | yes |
| `wait --output json` missing | | yes | | yes | yes | yes |
| No deployment locking/concurrency | yes | yes | | yes | | |
| `test` exits 0 when filter matches nothing | yes | yes | | | yes | |
| Stage line-break artifact ("not\nrecreated") | | | | | yes | yes |

### Resolved Since Last Review

- Global `--output` flag description fixed ("Output format" not "for --dry-run results")
- `MZ_DEPLOY_PROFILE` env var added (Marcus's #1 concern)
- `--dry-run --output json` examples on `apply sources` and `apply tables`
- `list` Related Commands now says `promote` (was `deploy`)
- All `mz-deploy deploy` references eliminated in favor of `promote`

### New Issues

- `--dry-run` inconsistency: available on sources/tables but not clusters/roles/connections/secrets
- Stage idempotency undocumented — CI retries may produce unexpected behavior
- `MZ_DEPLOY_PROFILE` not listed in profiles resolution order / precedence chain
- Deploy ID still not machine-capturable from `stage` live run
- `--keep-old` or instant rollback path still absent

---

## Review: 2026-03-10 (fifth pass)

### Scores

| Persona | Score | Delta | Key Theme |
|---------|-------|-------|-----------|
| Maya    | 8.9/10 | +0.6 | `--dry-run` fully resolved; wants `import` command and exit code docs |
| Priya   | 9.0/10 | +0.5 | Crossed 9.0 — `wait --output json` and conflict detection resolved |
| Carlos  | 9.0/10 | +0.5 | Crossed 9.0 — full dry-run coverage; `SET api = stable` is standout feature |
| Elena   | 9.3/10 | +0.3 | Unconditional go for 50-person org; governance (RBAC) is last gap |
| Marcus  | 9.0/10 | +0.25 | Crossed 9.0 — exit codes are single remaining blocker |
| Aisha   | 9.5/10 | +0.2 | Highest score ever; all previous concerns resolved; polish items only |

### Consensus Issues (raised by 3+)

| Issue | Maya | Priya | Carlos | Elena | Marcus | Aisha |
|-------|:----:|:-----:|:------:|:-----:|:------:|:-----:|
| No centralized exit code documentation | yes | yes | | | yes | yes |
| `--output json` gaps on compile/debug/profiles | yes | | | | yes | yes |

### Resolved Since Last Review

- `--dry-run` on all apply subcommands (was #1 consensus issue)
- `wait --output json` (was #2 consensus issue)
- Conflict detection on `promote` (deployment locking, 3 personas across 4 reviews)
- `test` exits non-zero on empty filter match
- Stage line-break artifact fixed (flagged 3 consecutive reviews)
- `MZ_DEPLOY_PROFILE` in profiles resolution order docs
- `describe` help page fleshed out
- Stage idempotency and promote resumability documented

### New Issues

- No `import` command for existing infrastructure (Maya)
- `promote --timeout` not available (Maya)
- `stage` deploy ID not documented in non-dry-run JSON output (Marcus)
- `stage` help doing double duty as concepts guide — may need `help topics` (Aisha)
- No `plan` as standalone command (Maya, Carlos)
- Secret redaction not explicitly documented for verbose/dry-run output (Priya)

---

## Review: 2026-03-11 (sixth pass)

### Scores

| Persona | Score | Delta | Key Theme |
|---------|-------|-------|-----------|
| Maya    | 9.1/10 | +0.2 | `wait --timeout` and sources/tables explanation resolved; exit codes, `import`, `plan` remain |
| Jake    | 8.5/10 | +0.5 | Hydration defined inline; wallclock lag, sources/sinks still opaque; no quickstart workflow |
| Priya   | 9.2/10 | +0.2 | Stable API schemas praised; `--keep-old` and exit codes remain top asks |
| Carlos  | 9.2/10 | +0.2 | Test filtering resolved; no test coverage reporting; missing `delete source` |
| Elena   | 9.4/10 | +0.1 | Unconditional GO maintained; 9-week adoption plan laid out |
| Marcus  | 9.0/10 | — | Holds steady; exit codes still "table-stakes"; `debug --output json` resolved |
| Aisha   | 9.5/10 | — | Holds steady; found `network-policies/` vs `network_policies/` directory name bug |

### Consensus Issues (raised by 3+)

| Issue | Maya | Jake | Priya | Carlos | Elena | Marcus | Aisha |
|-------|:----:|:----:|:-----:|:------:|:-----:|:------:|:-----:|
| No centralized exit code documentation | yes | | yes | | | yes | yes |
| `compile --output json` missing | yes | | | | | yes | yes |

### Resolved Since Last Review

- `debug --output json` added (Marcus's gap)
- `wait --timeout` resolves Maya's `promote --timeout` concern
- Sources/tables create-only constraint clearly documented
- Jake re-enters at 8.5 (up from 8.0) — hydration defined, profiles help excellent

### New Issues

- `network-policies/` vs `network_policies/` directory name inconsistency (Aisha — bug)
- `delete source` missing — `apply sources` creates them but no delete equivalent (Carlos)
- `stage` non-dry-run JSON output undocumented — deploy ID not confirmed (Marcus)
- `profiles` help buries common operations after advanced config (Aisha)
- No test coverage reporting (Carlos)
- No `--quiet` flag for exit-code-only CI checks (Aisha, Maya)

---

## Review: 2026-03-12 (walkthrough tutorial review)

**Note:** This review evaluates the interactive walkthrough tutorial (Claude Code slash command), not the CLI help output. Scores reflect tutorial quality, not CLI quality.

### Scores

| Persona | Score | Key Theme |
|---------|-------|-----------|
| Maya    | 7.5/10 | CI/CD depth insufficient; missing rollback module, pipeline example, `import` path |
| Jake    | 7.0/10 | Cognitive overload in Modules 5-6; jargon undefined; no "you try it" moments |
| Priya   | 7.5/10 | Observability absent; failure scenarios stop at dev-time; rollback not practiced |
| Carlos  | 8.2/10 | Testing & stable API excellent vs dbt; slow start; hybrid format should let users write SQL |
| Elena   | 8.5/10 | Strong onboarding ROI; needs fast-track path, governance module, theme toggle |
| Marcus  | 7.5/10 | Deploy ID handling hostile to automation; CI/CD section too thin; env vars incomplete |
| Aisha   | 7.5/10 | Fantasy theme net negative; missing tutorial infrastructure (time estimates, cheat sheet) |

### Consensus Issues (raised by 3+)

| Issue | Maya | Jake | Priya | Carlos | Elena | Marcus | Aisha |
|-------|:----:|:----:|:-----:|:------:|:-----:|:------:|:-----:|
| CI/CD section too thin (Step 53) | yes | | yes | | | yes | |
| Module 5-6 back-to-back is a "wall" | yes | yes | | | | | yes |
| Hybrid format needs "you try it" moments | | yes | | yes | | | yes |
| Fantasy theme reduces signal-to-noise | yes | | | | yes | | yes |
| No rollback practice / only happy path | yes | | yes | | yes | | |
| Missing --output json teaching | | | yes | | | yes | yes |
| No save points / skip-ahead / fast-track | | | | | yes | | yes |

### Key Recommendations (prioritized)

1. Expand CI/CD into a real module with pipeline skeleton, `--deploy-id $SHA`, exit codes, `--output json`
2. Insert break point between Module 5 and Module 6
3. Add 2-3 "you write it" exercises for views and tests
4. Add rollback practice exercise in Module 7
5. Strip theme to subtitles — technical language in instructions, flavor in titles only
6. Add tutorial infrastructure — time estimates per module, "what you'll learn" headers, cheat sheet
7. Define Materialize jargon on first use (cluster, source, CDC, hydration, MV, blue-green)
8. Teach `--deploy-id $SHA` instead of "note the deploy ID"

---

## Review: 2026-03-14 (missing features focus)

### Scores

| Persona | Score | Delta | Key Theme |
|---------|-------|-------|-----------|
| Maya    | 8.8/10 | -0.3 | `import`, fast rollback, `compile --output json`, standalone `plan` |
| Jake    | 9.0/10 | +0.5 | Glossary/jargon (`wallclock lag`, `sinks`), post-scaffold guidance, shell completions |
| Priya   | 9.3/10 | +0.1 | Fast rollback (`--keep-old`), secret redaction in dry-run, deployment locking |
| Carlos  | 8.8/10 | -0.4 | DAG selection (`+model+`), test coverage reporting, watch mode, `--fail-fast` |
| Elena   | 9.1/10 | -0.3 | Governance/RBAC, `--latest` on wait/promote, plan-apply separation, `import` |
| Marcus  | 9.2/10 | +0.2 | `compile --output json`, deploy ID from `stage`, exit code granularity (0/1 only) |
| Aisha   | 9.0/10 | -0.5 | `--quiet`, shell completions, `help topics`, `--no-pager`, short vs long help |

### Consensus Issues (raised by 3+)

| Issue | Maya | Jake | Priya | Carlos | Elena | Marcus | Aisha |
|-------|:----:|:----:|:-----:|:------:|:-----:|:------:|:-----:|
| No standalone `plan`/`diff` command | yes | yes | yes | yes | yes | yes | yes |
| No `mz-deploy status` command | | yes | yes | | yes | yes | yes |
| `compile --output json` missing | yes | | | | | yes | yes |
| No fast rollback / `--keep-old` | yes | | yes | | yes | | |
| Exit codes only 0/1 (no granularity) | yes | | yes | | | yes | |
| No glossary / `help topics` | | yes | | yes | | | yes |
| No `--yes` documented on abort/promote | | | | | | yes | yes |

### Resolved Since Last Review

- Exit code documentation on every subcommand (was #1 consensus issue across multiple reviews)
- `delete source` now available (Carlos's concern from 2026-03-11)
- `lock` properly categorized under Infrastructure (Jake's concern)
- `profiles` help restructured — common operations first (Aisha's concern)
- Hydration partially defined inline in `wait` help

### New Issues

- `import` command for brownfield adoption (Maya, Elena)
- No shell completions (`mz-deploy completion bash/zsh/fish`) (Jake, Aisha)
- `--quiet` / `-q` global flag for CI (Maya, Aisha)
- No test coverage reporting (`--coverage`) (Carlos)
- DAG selection for partial execution (`+model+` syntax) (Carlos)
- Secret redaction undocumented for `--dry-run` output (Priya)
- `stage` non-dry-run JSON output still undocumented — deploy ID capture (Marcus, 3rd consecutive review)
- No `--fail-fast` for test suite (Carlos)
- No watch mode / dev server (`compile --watch`, `test --watch`) (Carlos)
- No `--no-pager` flag (Aisha)
- No `--no-color` / `NO_COLOR` support documented (Aisha)
- `--deploy-id` is a flag on `stage` but positional on consumers — consistency gap (Aisha)
- No `--latest` convenience flag on wait/promote (Elena, 2nd consecutive review)
- No schema contract validation at compile time for stable APIs (Carlos)
- No cross-project dependency versioning (Carlos)
- `walkthrough` missing `--no-skill` flag that `new` and `init` have (Aisha)
