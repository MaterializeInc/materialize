---
name: persona-review
description: >
  Run persona-based DX reviews of mz-deploy's help output. Launch parallel
  sub-agents roleplaying as engineers with different backgrounds (Platform Engineer,
  Junior Dev, SRE, Data Engineer, CTO, DevOps Lead, Staff Engineer) who evaluate
  the CLI's developer experience, ergonomics, and documentation quality.

  Trigger when the user asks to: run a persona review, evaluate mz-deploy DX,
  get feedback on help text, run user personas, do a DX review, or test the
  help output with different engineer perspectives. Also trigger on phrases like
  "persona review", "DX review", "user feedback", or "evaluate the help".
---

# Persona Review for mz-deploy

Evaluate mz-deploy's developer experience by launching parallel sub-agents,
each roleplaying as an engineer persona with a distinct background. Agents
see ONLY the help output — never source code.

## Workflow

### 1. Build and capture help output

```bash
cargo build --bin mz-deploy 2>&1 | tail -3
```

If the build fails, stop and report the error.

Then capture all help output to a temp file:

```bash
{
  echo "=== TOP-LEVEL HELP ==="
  ./target/debug/mz-deploy --help
  echo ""
  echo "=== ALL EXTENDED HELP ==="
  ./target/debug/mz-deploy help --all
} > /tmp/mz-deploy-help-output.txt 2>&1
```

### 2. Select personas

Read [references/personas.md](references/personas.md) for the full persona
definitions and evaluation criteria.

**Default set (all 7):** Maya, Jake, Priya, Carlos, Elena, Marcus, Aisha

If the user specifies personas by name, run only those. If the user describes
a custom persona (e.g., "a Kafka expert" or "a compliance officer"), create
an ad-hoc persona definition following the same structure as the built-in ones.

### 3. Load review history

Read [references/review-history.md](references/review-history.md) to find
previous scores and unresolved issues. Include this context in each agent's
prompt so they can track what has improved and what regressed.

### 4. Launch persona agents in parallel

Launch one Agent per persona **in a single message** (parallel execution).

Each agent prompt must include:
- The persona definition (from personas.md)
- Previous review scores and unresolved issues (from review-history.md)
- Instruction to read `/tmp/mz-deploy-help-output.txt`
- Instruction to evaluate ONLY based on help output, never source code

Each agent must produce:
- Assessment of which previous concerns are resolved / partially resolved / unresolved
- Evaluation against their specific criteria (from personas.md)
- Remaining gaps and new issues found
- Overall ergonomics score (1-10) with justification
- Specific references to help text (quote exact phrases)

### 5. Synthesize results

After all agents complete, produce a summary:

**Score table** — All personas with scores, delta from last review, key theme

**Consensus matrix** — Issues raised by 3+ personas in a cross-reference table

**Resolved since last review** — What improved

**New issues** — Feedback not seen in previous reviews

**Notable details** — Expand on non-obvious or particularly insightful feedback

### 6. Update review history

Append the new review to [references/review-history.md](references/review-history.md)
following the template format defined in that file.

## Agent Prompt Template

Use this structure for each persona agent:

```
You are **{name}**, {role} with {experience}. {tools_and_background}.
You care about: {cares_about}.

In the previous review ({date}), you scored mz-deploy {score}/10.
Your key unresolved concerns were:
{previous_concerns_list}

YOUR TASK: Read /tmp/mz-deploy-help-output.txt (the COMPLETE help output
of mz-deploy). Evaluate based ONLY on help output, never source code.

Evaluate:
1. Previous concerns — which are resolved, partially resolved, unresolved?
{persona_specific_criteria}
N. Overall ergonomics score (1-10) with justification.

Write in first person as {name}. Quote exact help text. Be constructively
critical.
```

## Custom Personas

When the user requests a persona not in the built-in set, construct one with:
- **Role and background** — specific enough to inform evaluation priorities
- **Tools and experience** — what they compare mz-deploy against
- **Evaluation criteria** — 4-7 specific dimensions to assess
- No previous review history (first review for this persona)
