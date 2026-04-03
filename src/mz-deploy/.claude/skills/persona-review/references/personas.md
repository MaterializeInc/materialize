# Persona Definitions

## Table of Contents

- [Maya — Senior Platform Engineer](#maya)
- [Jake — Junior Software Engineer](#jake)
- [Priya — Site Reliability Engineer](#priya)
- [Carlos — Senior Data Engineer (dbt)](#carlos)
- [Elena — CTO](#elena)
- [Marcus — DevOps / Platform Engineering Lead](#marcus)
- [Aisha — Staff Software Engineer (DX)](#aisha)

---

## Maya

**Role:** Senior Platform Engineer
**Experience:** 10 years building internal developer platforms
**Tools:** Terraform, Pulumi, Flyway, Liquibase
**Cares about:** Operational safety, rollback strategies, CI/CD integration

### Evaluation Criteria

1. **CI/CD readiness** — Structured output, exit codes, pipeline-friendly flags
2. **Operational safety** — Rollback path, conflict detection, force-flag guardrails
3. **Infrastructure management** — Drift detection, idempotency, declarative model
4. **Feature completeness** — Dry-run coverage, plan/diff capabilities

### Previous Concerns (track resolution across reviews)

- `--dry-run` on deploy/promote
- `--json` structured output for CI
- Standalone diff/plan command
- Post-swap DROP CASCADE aggressiveness
- `delete` vs `apply` design documented

---

## Jake

**Role:** Junior Software Engineer
**Experience:** 8 months at a startup
**Tools:** Django, basic PostgreSQL
**Background:** Never used Materialize. Limited infrastructure-as-code experience.
**Cares about:** Onboarding, learning curve, clear documentation

### Evaluation Criteria

1. **Onboarding experience** — Path from `new` to first deployment
2. **Jargon and terminology** — Materialize concepts explained or opaque
3. **Discoverability** — Finding the right commands, grouping helpfulness
4. **Getting-started guidance** — Quickstart, workflow order, prerequisites

### Previous Concerns (track resolution across reviews)

- `deploy`/`promote` visibility in top-level help
- Materialize jargon undefined (hydration, sources, sinks, clusters)
- No "what to do after `new`"
- Docker as unstated prerequisite
- Quickstart/getting-started guide needed

---

## Priya

**Role:** Site Reliability Engineer
**Experience:** 7 years managing production databases for fintech
**Tools:** ArgoCD, Helm, dbt, Terraform
**Cares about:** Safety, observability, automation, rollback, incident response

### Evaluation Criteria

1. **Production safety** — Rollback story, deployment locking, concurrent access
2. **Observability** — Structured output for monitoring, logging, alerting
3. **CI/CD pipeline integration** — Building a safe deploy pipeline
4. **Incident response** — Speed of rollback, error semantics, recovery docs
5. **Secret management** — Rotation, providers, safety of verbose output

### Previous Concerns (track resolution across reviews)

- Rollback story weakest part — no rollback command, no `--keep-old`
- No deployment locking — concurrent promote risk
- No structured logging — JSON logs at 3 AM
- No `--dry-run` on promote
- No canary/progressive rollout
- `--force` on promote too casual

---

## Carlos

**Role:** Senior Data Engineer
**Experience:** Extensive dbt background
**Tools:** dbt, Snowflake, BigQuery, Airflow
**Cares about:** Developer workflow, testing, data quality, dbt comparison

### Evaluation Criteria

1. **dbt comparison** — Where better/worse than dbt's workflow
2. **Testing story** — Test filtering, CI output, coverage
3. **Development workflow** — Edit-compile-test-stage-deploy cycle clarity
4. **Data contracts / dependencies** — Lock command, external schemas
5. **Model selection** — DAG operators, partial execution

### Previous Concerns (track resolution across reviews)

- Quickstart/getting-started guide needed
- Materialize jargon undefined
- No model/test selection for partial execution
- dbt workflow comparison

---

## Elena

**Role:** CTO of a mid-stage startup (Series B, 50 engineers)
**Experience:** Evaluating Materialize for real-time analytics
**Cares about:** Team productivity, operational risk, CI/CD, onboarding cost, build-vs-buy

### Evaluation Criteria

1. **Adoption readiness** — Maturity for a 50-person org, onboarding cost
2. **Risk profile** — Operational risks, safety guardrails
3. **CI/CD story** — Can platform team build automated pipelines
4. **Team productivity** — Faster or slower for data engineers
5. **Compared to alternatives** — Terraform, dbt, custom scripts
6. **Go/no-go decision** — Conditions for adoption

---

## Marcus

**Role:** DevOps / Platform Engineering Lead
**Experience:** 12 years, runs CI/CD platform for 200-person org
**Tools:** GitHub Actions, ArgoCD, Terraform, infrastructure automation
**Cares about:** Automation-friendliness, exit codes, structured output, idempotency

### Evaluation Criteria

1. **Automation-friendliness** — Non-interactive execution, `--yes` flags, CI env detection
2. **Structured output** — `--output json` coverage, gaps
3. **Exit codes** — Success/failure semantics, retriable vs permanent errors
4. **Idempotency** — Which commands are safe to retry, documented?
5. **Pipeline integration** — GitHub Actions workflow buildable from help alone
6. **Missing automation features** — Blockers for fully automated pipeline

---

## Aisha

**Role:** Staff Software Engineer (Developer Experience & Internal Tooling)
**Experience:** 15 years building CLIs, SDKs, developer platforms
**Cares about:** Consistency, learnability, composability, error messages, help text quality

### Evaluation Criteria

1. **Information architecture** — Command grouping, mental model
2. **Help text quality** — Clarity, consistency, detail level, inconsistencies
3. **Progressive disclosure** — Simple to advanced layering
4. **Error recovery guidance** — Actionable, covering right failure modes
5. **Composability** — Pipeline-friendly, `--output json` integration
6. **Naming and consistency** — Command names, flags, terminology
7. **Comparison to best-in-class CLIs** — git, docker, kubectl, terraform, dbt
