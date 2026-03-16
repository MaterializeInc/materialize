## Workflow

The user runs iterative staging tests using this loop:

1. **Deploy**: Run `bin/staging-deploy` — waits for the HEAD commit's Buildkite build to produce a Docker image, deploys it to staging, and waits for staging to accept connections.
2. **Test**: The user provides queries as a single `.sql` file. Split it into individual statements on `;` and run each separately via `bin/mz --profile staging sql -- -c "<query>"`.
   - **Transient errors** (connection errors, timeouts, "not ready" style messages): retry with backoff, up to 2 minutes total per query.
   - **Definitive errors** (syntax errors, object not found, permission denied, etc.): stop immediately and report.
   - **Psql variables**: If a query contains `:'VAR_NAME'`-style substitutions, pass the corresponding environment variable via `-v`: `bin/mz --profile staging sql -- -v VAR_NAME="$VAR_NAME" -c "<query>"`. Multiple `-v` flags can be chained. The `--` args are forwarded directly to `psql`.
3. **Fix**: On a definitive failure, diagnose the root cause, attempt a code fix, and notify the user for review.
4. **Loop**: Wait for the user to push the fix, then return to step 1.

## Relevant context

- `bin/staging-deploy`: Python script that uses `bk` CLI to find the Buildkite build for HEAD, watches it, extracts the Docker tag from build annotations, disables/re-enables the staging region via `bin/mz --profile staging region enable --version <tag>`, then polls until `SELECT 1` succeeds.
- `bin/mz`: thin wrapper — runs `cargo run --bin mz -- "$@"`.
- SQL is run against staging with: `bin/mz --profile staging sql -- -c "<query>"`.
- For queries with psql variable substitution (`:'VAR'`): `bin/mz --profile staging sql -- -v VAR="$VAR" -c "<query>"`.
- `mz sql` uses `trailing_var_arg`, forwarding everything after `--` to `psql` unchanged.
- Queries like `VALIDATE CONNECTION` may be transiently failing after `CREATE CONNECTION` while the connection is being established; these should be retried.
