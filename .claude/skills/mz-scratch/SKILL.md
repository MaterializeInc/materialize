---
name: mz-scratch
description: >
  This skill should be used when the user wants to spin up or work on an AWS
  scratch EC2 instance via `bin/scratch`, "get a scratch box", "ssh to scratch",
  push local changes to scratch, run a remote Claude Code session on scratch,
  monitor a long-running remote operation, or mentions scratch machines,
  scratch instances, `bin/scratch` commands, `aws sso login`, or the
  `mz-scratch-admin` AWS profile. Use this skill even if the user just says
  "spin up a scratch box" or "run that on scratch" without being specific.
---

# Working with `bin/scratch`

`bin/scratch` manages Materialize's short-lived AWS EC2 dev/test instances
(the `mz-scratch-admin` account). Source: `misc/python/materialize/cli/scratch/`.
Subcommands: `login`, `list`/`ls`, `create`, `ssh`, `sftp`, `destroy`/`rm`,
`push`, `forward`, `go`, `claude`, `completion`. The EC2-level `mssh` wrapper
and `setup_ai_tools` helper live in `misc/python/materialize/scratch.py`.

## Login

`bin/scratch login` wraps `aws sso login`. Its default behavior opens a
browser on the host — that fails when running from Claude Code or over SSH
because the device code never reaches the user.

**Use `aws sso login --no-browser` directly** so the device URL lands on
stdout. Invoke with the right env so the correct SSO profile is used:

```bash
AWS_PROFILE=mz-scratch-admin AWS_DEFAULT_REGION=us-east-1 \
  aws sso login --no-browser
```

Present **both** URL forms to the user (different browsers/password managers
handle them differently):

1. Code-entry page: `https://materialize.awsapps.com/start/#/device` + the
   short code printed by the command.
2. Autofill URL: `.../#/device?user_code=XXXX-YYYY` (the command also prints
   this one).

Use markdown `[text](url)` links, **not OSC8 escape sequences** — Claude
Code's UI strips `\x1b` bytes, so raw OSC8 renders as literal garbage
(`]8;;URL\text]8;;\`). OSC8 is only safe when writing to a raw terminal
(scripts, `bash -c`).

Run the login in the background (e.g. `run_in_background: true`) and read
the output file to find the code. The command blocks until the user
completes auth in their browser.

**SSO sessions expire — re-auth ~every 12 hours.** If a `bin/scratch`
command errors with `The SSO session associated with this profile has
expired or is otherwise invalid`, re-run the login flow above. This will
happen silently mid-session during long workflows; surface it to the user
promptly and show both URLs again.

## Always `bin/scratch list` after `login`

Before creating anything, list existing instances so the user can reuse one
(scratch boxes are expensive — default AMI is `t3.2xlarge`). If an instance
already covers the user's need, ask before destroying/recreating.

## Expiry

Instances auto-expire via a `scratch-delete-after` EC2 tag. `--max-age-days N`
sets the tag at **create** time — and at `go`'s internal create path — but
**is ignored when `go` reuses an existing instance**. There is no first-class
extend-expiry subcommand. Extending means:

- `aws ec2 create-tags --resources <id> --tags Key=scratch-delete-after,Value=<unix_ts>`, or
- destroy and recreate with a new `--max-age-days`.

Code: `misc/python/materialize/scratch.py` writes the tag as a unix timestamp
(`str(delete_after.timestamp())`).

## `create` vs `go` vs `claude`

| Command | Behavior | Use when |
|---|---|---|
| `bin/scratch create <machine> --max-age-days N` | Non-interactive. Launches and exits. | Driving programmatically from a skill or automation. |
| `bin/scratch go <machine>` | Find-or-create, then **interactive SSH**. | User is driving the terminal themselves. Interactive TTY makes this awkward to run from Claude Code. |
| `bin/scratch claude <machine>` | Same as `go` but runs `claude --dangerously-skip-permissions --remote-control` inside a reattachable `screen` session on the remote. | User wants a long-lived Claude Code session on scratch that they'll reattach to. |

When a skill needs to launch a box and continue working with it
programmatically, prefer `create`.

## Remote developer environment setup

The `dev-box` AMI has `git` but **no** `gh`, no global git config, no extra
repo clones, and no Claude Code credentials. Set up via
`bin/scratch ssh <id> '<commands>'`:

### 1. Install `gh`

```bash
bin/scratch ssh <id> 'set -euo pipefail
sudo mkdir -p -m 755 /etc/apt/keyrings
sudo wget -qO /etc/apt/keyrings/githubcli-archive-keyring.gpg \
  https://cli.github.com/packages/githubcli-archive-keyring.gpg
sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
  | sudo tee /etc/apt/sources.list.d/github-cli.list >/dev/null
sudo apt-get update -qq && sudo apt-get install -y gh'
```

### 2. Auth `gh` + wire as git credential helper

Pipe the local token via stdin so it **never appears in a command-line
argument** (where it would show up in `ps`, remote shell history, etc.):

```bash
gh auth token | bin/scratch ssh <id> \
  'gh auth login --with-token && gh auth setup-git'
```

### 3. Propagate global git config

```bash
name=$(git config --global --get user.name)
email=$(git config --global --get user.email)
bin/scratch ssh <id> "git config --global user.name '$name' \
  && git config --global user.email '$email'"
```

### 4. Clone extra repos on request

```bash
bin/scratch ssh <id> 'git clone https://github.com/MaterializeInc/mz-release.git'
```

## Transferring Claude Code credentials

`setup_ai_tools` in `misc/python/materialize/scratch.py` tars
`~/.claude/.credentials.json`, `~/.claude/settings.json`, and
`~/.claude.json` from local and ships them via stdin. That path works on
Linux.

**On macOS, `~/.claude/.credentials.json` is a stale cache** — the live
OAuth token lives in the login Keychain. Ship this way instead:

```bash
TMP=$(mktemp -d) && trap "rm -rf $TMP" EXIT
mkdir -p "$TMP/.claude"
security find-generic-password -s "Claude Code-credentials" -w \
  > "$TMP/.claude/.credentials.json"
chmod 600 "$TMP/.claude/.credentials.json"
cp ~/.claude/settings.json "$TMP/.claude/settings.json"
cp ~/.claude.json "$TMP/.claude.json"
tar czf - -C "$TMP" .claude/.credentials.json .claude/settings.json .claude.json \
  | bin/scratch ssh <id> 'tar xzf - -C ~ && chmod 600 ~/.claude/.credentials.json'
```

This is a bug in `setup_ai_tools` (the file it reads on macOS is a cache,
not the source of truth). File a follow-up.

## Forwarding prompts to a remote Claude session

### Addressing protocol

| | |
|---|---|
| User → remote | `r-<name> <prompt>` (e.g. `r-release cut rc.3`) |
| Assistant relay | prefix each line `\|-<name> ` (e.g. `\|-release Queue run succeeded`) |
| List sessions | `r-list` |
| Names | `[a-zA-Z0-9_]+` |
| End session | No explicit end. Sessions die when the instance is destroyed. |
| Ambiguous bare `r-` | With ≥2 active sessions, reject and ask for `r-<name>`. |

### Session UUID map

Track `{ name → session_uuid }` locally in the conversation, keyed by the
scratch instance id.

- **First `r-<name>`**: spin up a fresh session with `claude -p --output-format
  stream-json --verbose --dangerously-skip-permissions`. Capture `.session_id`
  from the first `system|init` event in the stream.
- **Subsequent messages to that name**: use `claude -p --resume <uuid> ...`.
- **Do not rely on `--continue`** once multiple sessions exist — it resumes
  the most-recent conversation in cwd and will cross the streams.

Claude Code on `dev-box` is at `/usr/local/bin/claude` (pre-installed).

### Stream-json + Monitor for live relay

Default `claude -p` text mode emits only the final answer. Use stream-json
so tool uses, tool results, and intermediate events are visible.

Invoke with `run_in_background: true` so the SSH blocks locally without
blocking the conversation. Arm a `Monitor` on the output file piping through
jq to project events to short `type|name|payload` lines:

```bash
tail -n +1 -f <output-file> 2>/dev/null | grep --line-buffered '^{' |
  jq --unbuffered -rc '
    if .type=="assistant" then (.message.content//[])[] |
      if .type=="text" then "text|" + ((.text//"") | gsub("\n";"\\n"))
      elif .type=="tool_use" then "tool|" + (.name//"") + "|" + ((.input // {}) | tostring)
      else empty end
    elif .type=="user" then (.message.content//[])[] |
      if .type=="tool_result" then "result|" + (if (.content|type)=="string" then .content else (.content|tostring) end | gsub("\n";"\\n"))
      else empty end
    elif .type=="system" then "system|" + (.subtype//"init") + "|session=" + (.session_id//"")
    elif .type=="result" then "done|" + (.subtype//"") + "|session=" + (.session_id//"") + "|cost=" + ((.total_cost_usd//0)|tostring)
    else empty end'
```

**Do not:**

- Truncate the jq output with `.[0:N]` — cuts assistant answers mid-sentence.
- Pass `--include-partial-messages` to `claude -p` — fragments text into
  many partial-delta events; relay becomes noisy. Plain `--verbose
  --output-format stream-json` is enough.

Monitor's own notification payload is truncated (~500–800 chars) regardless
of filter. After the background task completes, **always fetch the canonical
full assistant text from the output file**:

```bash
grep '^{' <output-file> | \
  jq -r 'select(.type=="assistant") | .message.content[]? | select(.type=="text") | .text'
```

Then `TaskStop` the Monitor.

## Long-running operations: cron as wakeup, not `ScheduleWakeup`

Remote `claude -p` exits at the end of its turn. If remote Claude calls
`ScheduleWakeup`, the wakeup may persist to disk but nothing is running on
the scratch instance to fire it. **Invariably broken**.

Instead, in the prompt forwarded to the remote session, include:

> If you need a timed check-back, emit a marker `[wakeup: Nm, "prompt to
> send back"]` at the end of your response. Do NOT call ScheduleWakeup.

After the remote's turn completes:

1. Parse the final assistant text for a `[wakeup: Nm, "..."]` marker.
2. If present, `CronCreate` a one-shot (`recurring: false`) at `now + N`
   whose prompt forwards the inner string back to the same `r-<name>`
   session via the same stream-json + Monitor pattern.
3. On each fire, repeat: check for a new wakeup marker, schedule or stop.

### Cancel superseded crons

Whenever the user changes the plan, or the skill manually performs the work
a pending cron was going to do, **immediately `CronDelete` the stale job**.
Late-firing crons inject their own past prompts into the chat (it looks
like broken witchcraft to the user) and can thrash the remote side —
re-triggering merges, re-enqueues, or retries that have already been
handled.

Track the set of active cron IDs in the conversation so they can be
cancelled when plans evolve.

## Skill-tool state on the remote is authoritative

When you install a skill like `mz-release` both on the scratch instance
(populated with caches, state files) and locally (fresh install, empty
caches), they diverge. Local `mz_release_tool ... report` won't see the
RCs the remote has been tracking.

For deterministic read-only lookups, skip `claude -p` entirely and SSH
directly to run the skill tool:

```bash
bin/scratch ssh <id> 'export GITHUB_TOKEN=$(gh auth token) && cd ~/mz-release && \
  .claude/skills/mz-release/tools/mz_release_tool.sh <version> <cmd>'
```

No Claude API cost, uses the populated remote state. Reserve `claude -p`
for cases where the remote agent needs to reason or drive multi-step work.

## Destroy

```bash
bin/scratch destroy <id>        # prompts for confirmation
bin/scratch destroy -y <id>     # automation
bin/scratch destroy --all-mine  # bulk
```

Destruction is immediate and terminates any remote sessions plus their
on-disk state (including saved `claude -p` transcripts under
`~/.claude/projects/…`). Drop the local session-uuid map entries for that
instance afterward.

## Common mistakes

- Running `bin/scratch login` directly (opens a local browser that may not
  match the user's active profile). Use `aws sso login --no-browser` and
  show the URLs.
- Starting with `go` or `claude` when you need programmatic control —
  interactive SSH will hang.
- Passing `--max-age-days` to `go` expecting it to extend an existing
  instance. It only applies when `go` creates a fresh one.
- Embedding a GitHub token in a command-line argument (`gh auth login
  --with-token <token>`) — use stdin instead.
- Emitting OSC8 escape sequences directly in assistant text; Claude Code's
  UI strips them. Use markdown links.
- Letting remote Claude call `ScheduleWakeup` for its own check-backs.
  Parse a `[wakeup:]` marker and `CronCreate` locally instead.
- Forgetting to cancel stale crons when plans change.
- Using `claude -p --continue` once multiple named sessions exist on the
  same instance — use `--resume <uuid>` with the captured session id.

## TODO — harden via writing-skills TDD loop

This skill was authored one-shot from a live walkthrough on 2026-04-18,
not via the `writing-skills` RED-GREEN-REFACTOR pressure-testing process.
The requirements came from a real release cycle (not hypothesized usage),
but no subagent pressure scenarios were run to verify agents comply with
the prescribed protocol under stress.

Future work: dispatch subagents with scenarios covering the session
protocol (`r-<name>` / `|-<name>`), cron-as-wakeup discipline, stale-cron
cancellation, and the macOS Keychain Claude-credentials path, and
refactor this skill against the rationalizations they surface. See
`superpowers-extended-cc:writing-skills`.
