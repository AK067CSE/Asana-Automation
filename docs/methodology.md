# Asana RL Environment Seed Data Generation

## Methodology and Research Notes

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Problem Framing (RL Environment Needs)](#problem-framing-rl-environment-needs)
3. [Data Model and Schema Overview](#data-model-and-schema-overview)
4. [Generation Pipeline (End-to-End)](#generation-pipeline-end-to-end)
5. [Table-by-Table Seed Data Methodology](#table-by-table-seed-data-methodology)
6. [Text Generation (LLM) Strategy](#text-generation-llm-strategy)
7. [Temporal Consistency Framework](#temporal-consistency-framework)
8. [Relational Consistency Framework](#relational-consistency-framework)
9. [Distribution & Realism Controls](#distribution--realism-controls)
10. [Validation and QA](#validation-and-qa)
11. [Known Gaps and Roadmap](#known-gaps-and-roadmap)
12. [References (Non-Exhaustive)](#references-non-exhaustive)

## Executive Summary

This repository generates a synthetic but realistic SQLite dataset that approximates an Asana workspace for use in an RL environment (e.g., computer-use agents executing project management workflows). The generator produces linked entities (organizations, teams, users, projects, sections, tasks, subtasks, comments, tags, and custom fields) while enforcing:

- Temporal plausibility (no “time travel” artifacts such as completion before creation)
- Referential integrity (no orphaned rows)
- Enterprise-like distributions (heterogeneous team sizes, due dates that avoid weekends, non-uniform completion rates)

The primary design goal is *behavioral realism* rather than pure volume: the dataset should induce realistic UI navigation patterns (searching, filtering, assigning, commenting, moving items across sections) without giving shortcut signals to an RL agent.

## Problem Framing (RL Environment Needs)

From an RL / evaluation standpoint, the dataset should support:

- **Naturalistic action sequences**
  - e.g., open project → scan sections → open task → comment → mark complete
- **Ambiguity and partial observability**
  - tasks can be unassigned
  - tasks can have missing descriptions
  - multiple projects can share similar names
- **Non-trivial policies**
  - due dates are correlated with project types
  - completion depends on project lifecycle and age
- **Edge cases**
  - overdue tasks
  - archived projects
  - sparse comment activity

## Data Model and Schema Overview

The authoritative schema is defined in `schema.sql`. It includes:

- `organizations`
- `teams`
- `users`
- `team_memberships`
- `projects`
- `sections`
- `tasks`
- `subtasks`
- `comments`
- `custom_field_definitions`
- `custom_field_values`
- `tags`
- `task_tags`

The schema also defines helpful indexes and two views (`active_tasks`, `project_overview`).

### Design decision: task hierarchy

Task hierarchy is represented explicitly via the `subtasks` table (`subtasks.task_id → tasks.id`). This avoids recursive self-joins in `tasks` and provides a clean separation between leaf-level work items (tasks) and nested work (subtasks) while preserving performance characteristics in SQLite.

### Design decision: custom fields

Custom fields are modeled as:

- `custom_field_definitions` (organization-scoped field *metadata*)
- `custom_field_values` (task-scoped *values*)

Values are stored in typed columns (`value_text`, `value_number`, `value_date`, `value_boolean`, `value_enum`) to support realistic query patterns while remaining flexible.

## Generation Pipeline (End-to-End)

The pipeline is orchestrated from `src/main.py`:

1. Initialize the SQLite schema (`schema.sql`)
2. Generate organizations, users, teams, and memberships (`src/generators/users.py`)
3. Generate projects and insert them to obtain DB IDs (`src/generators/projects.py`)
4. Generate sections *after projects have IDs* and insert them
5. Generate tasks, insert to obtain task IDs, then generate and insert subtasks (`src/generators/tasks.py`)
6. Generate comments for tasks (`src/generators/comments.py`)
7. Run validation (`src/utils/validation.py`) and log results

A key methodological principle here is: **generate high-level containers first, then insert to obtain IDs, then generate dependent entities** (sections depend on projects; subtasks depend on tasks; comment authors depend on memberships).

## Table-by-Table Seed Data Methodology

Below is a table-by-table methodology in a research-scientist style: each column is defined with a “source strategy” and a rationale.

### Table: `organizations`

| Column | Type | Source Strategy | Methodology & Justification |
|---|---|---|---|
| `id` | INTEGER | DB-generated | Autoincrement primary key ensures deterministic referential joins. |
| `name` | TEXT | Synthetic (templates) | Enterprise-credible organization naming; avoids placeholder strings ("Org 1"). |
| `domain` | TEXT | Derived from name | Unique domain constraint enforces realism and prevents duplicated workspaces. |
| `created_at`, `updated_at` | TIMESTAMP | System default or derived | Coherent timestamps support temporal queries and audit trails. |

### Table: `teams`

| Column | Type | Source Strategy | Methodology & Justification |
|---|---|---|---|
| `id` | INTEGER | DB-generated | Stable keys for membership and downstream team-aware logic. |
| `organization_id` | INTEGER (FK) | Derived | Each team belongs to exactly one organization. |
| `name` | TEXT | Synthetic (departmental lexicon) | Team naming captures organizational structure (e.g., “Platform”, “Demand Gen”). |
| `description` | TEXT | Synthetic (optional) | Optional text introduces realistic sparsity. |
| `created_at`, `updated_at` | TIMESTAMP | Derived | Within simulation window; supports team lifecycle analysis. |

### Table: `users`

| Column | Type | Source Strategy | Methodology & Justification |
|---|---|---|---|
| `id` | INTEGER | DB-generated | Enables stable assignment references. |
| `organization_id` | INTEGER (FK) | Derived | Users are scoped to a workspace. |
| `name` | TEXT | Hybrid: census-style + international mix | Produces realistic variability while avoiding obviously synthetic tokens. |
| `email` | TEXT | Derived | Realistic corporate formats; unique constraint enforces data hygiene. |
| `role` | TEXT | Categorical distribution | `(admin, member, guest)` with a skew toward `member` to reflect typical Asana usage. |
| `created_at`, `updated_at` | TIMESTAMP | Derived | Consistent with organization start and the simulated time window. |

### Table: `team_memberships`

| Column | Type | Source Strategy | Methodology & Justification |
|---|---|---|---|
| `id` | INTEGER | DB-generated | Stable membership records. |
| `team_id`, `user_id` | INTEGER (FK) | Derived | Sampling constrained by org membership. |
| `role` | TEXT | Distribution + heuristics | “owner” is rare; “member” is common. Avoids overly uniform assignments. |
| `created_at`, `updated_at` | TIMESTAMP | Derived | Timestamps align with user/team creation. |

### Table: `projects`

| Column | Type | Source Strategy | Methodology & Justification |
|---|---|---|---|
| `id` | INTEGER | DB-generated | Required for section and task generation. |
| `organization_id` | INTEGER (FK) | Derived | Projects are workspace-scoped. |
| `name` | TEXT | Hybrid: templates + LLM optional | Project names depend on department/project-type patterns (e.g., sprint vs campaign). |
| `description` | TEXT | Synthetic/LLM optional | Variable-length descriptions; can be empty to reflect real usage. |
| `status` | TEXT | Stochastic w/ lifecycle coupling | `active/completed/archived` tied to end dates and probability mass on “active”. |
| `start_date`, `end_date` | DATE | Temporal model | Timelines drawn from bounded distributions; ensures plausible planning horizons. |
| `created_at`, `updated_at` | TIMESTAMP | Derived | Coherent with `start_date` and current simulation time. |

### Table: `sections`

| Column | Type | Source Strategy | Methodology & Justification |
|---|---|---|---|
| `id` | INTEGER | DB-generated | Stable identifiers for task placement. |
| `project_id` | INTEGER (FK) | Derived | Generated *after project insertion* to avoid FK violations. |
| `name` | TEXT | Template library | Typical Asana boards (“To Do”, “In Progress”, “Done”) plus project-type variants. |
| `position` | INTEGER | Sequential | Encodes ordering within a project. |
| `created_at`, `updated_at` | TIMESTAMP | Derived | Aligns with project creation and activity. |

### Table: `tasks`

| Column | Type | Source Strategy | Methodology & Justification |
|---|---|---|---|
| `id` | INTEGER | DB-generated | Inserted before subtasks to get stable IDs. |
| `project_id` | INTEGER (FK) | Derived | Task belongs to exactly one project in this schema. |
| `section_id` | INTEGER (FK) | Derived | Task belongs to a project section. |
| `assignee_id` | INTEGER (FK) | Heuristic + distribution | Some tasks unassigned; assigned tasks restricted to team members when possible. |
| `name` | TEXT | Pattern + optional LLM | Department-aware action phrases reduce “Task 1” artifacts. |
| `description` | TEXT | Template/LLM + sparsity | Mix of empty, short, and structured descriptions. |
| `due_date` | DATE | Synthetic temporal model | Skewed toward near-term; 85% land on business days. |
| `completed` | BOOLEAN | Project-type dependent | Completion rates differ by project type; older tasks more likely complete. |
| `completed_at` | TIMESTAMP | Derived | If completed, sampled after `created_at` and before “now”. |
| `priority` | TEXT | Categorical distribution | Skew toward “medium”; includes small mass on “none”. |
| `position` | INTEGER | Sequential | Emulates UI ordering within a section. |
| `created_at`, `updated_at` | TIMESTAMP | Temporal model | Captures weekday effects and project lifecycle coupling. |

### Table: `subtasks`

| Column | Type | Source Strategy | Methodology & Justification |
|---|---|---|---|
| `id` | INTEGER | DB-generated | Stable keys. |
| `task_id` | INTEGER (FK) | Derived | Parent task chosen from inserted tasks. |
| `name` | TEXT | Template/pattern | Short action items; less verbose than top-level tasks. |
| `completed`, `completed_at` | BOOLEAN/TIMESTAMP | Derived | Completion correlated with parent completion, with noise. |
| `position` | INTEGER | Sequential | Ordering within parent task. |
| `created_at`, `updated_at` | TIMESTAMP | Derived | Consistent with parent lifecycle. |

### Table: `comments`

| Column | Type | Source Strategy | Methodology & Justification |
|---|---|---|---|
| `id` | INTEGER | DB-generated | Stable keys. |
| `task_id` | INTEGER (FK) | Derived | Comments attach to tasks. |
| `user_id` | INTEGER (FK) | Derived from membership | Comment authors reflect team participation; prevents unrealistic random authorship. |
| `content` | TEXT | LLM optional / templates | Covers update, question, review, block, approval styles. |
| `created_at`, `updated_at` | TIMESTAMP | Derived | Must be after task creation; often before completion. |

### Table: `tags` and `task_tags`

| Column | Type | Source Strategy | Methodology & Justification |
|---|---|---|---|
| `tags.name` | TEXT | Controlled vocabulary | Tags like “blocked”, “security”, “customer”, “tech-debt”. |
| `tags.color` | TEXT | Small palette | Matches UI conventions and improves realism. |
| `task_tags` | (task_id, tag_id) | Derived | Sparse bipartite relationships; avoids tagging everything. |

### Table: `custom_field_definitions` and `custom_field_values`

| Column | Type | Source Strategy | Methodology & Justification |
|---|---|---|---|
| `custom_field_definitions.field_type` | TEXT | Controlled enum | One of `(text, number, date, enum, boolean)`. |
| `custom_field_definitions.enum_options` | TEXT(JSON) | Generated | Enumerations like priority scale, effort points, status labels. |
| `custom_field_values.value_*` | typed | Generated conditional on type | Only one typed value populated per row; others remain NULL. |

## Text Generation (LLM) Strategy

LLM is used *selectively* for high-variance natural language fields (`tasks.name`, `tasks.description`, `comments.content`) to reduce templating artifacts.

### Prompt templates

The repo includes prompts under `prompts/`:

- `prompts/task_names.txt`
- `prompts/descriptions.txt`
- `prompts/comments.txt`
- `prompts/company_context.txt`

These prompts are parameterized by `department`, `project_type`, and `section_name`. The design principle is to constrain the LLM into *enterprise-safe, action-oriented language* while still allowing lexical diversity.

### Variability controls

- Temperature is configurable via `OPENAI_TEMPERATURE`.
- Prompts enforce output length bounds and “exactly one response” constraints.
- The generator can combine LLM outputs with pattern libraries to ensure coverage even when the API is disabled.

## Temporal Consistency Framework

A temporal generator (see `src/utils/temporal.py`) enforces:

- `created_at ≤ updated_at`
- `created_at ≤ completed_at` if `completed=1`
- Due dates sampled with business-day bias (weekday avoidance)
- Project start/end dates that create plausible task windows

### Business day logic

A large fraction of enterprise tasks avoid weekend due dates. The generator implements a “nearest business day” projection in the common case rather than strictly resampling.

## Relational Consistency Framework

Relational constraints are handled in two layers:

1. **Schema-level constraints**
   - Foreign keys and uniqueness constraints in SQLite
2. **Generator-level constraints**
   - Insert parent entities first, then generate children
   - Restrict task assignees and comment authors to valid workspace users

## Distribution & Realism Controls

The code uses distributional priors that approximate typical enterprise behavior:

- **Completion rate heterogeneity** by project type (e.g., sprint-like projects complete more)
- **Due date planning horizon** bounded (e.g., avoid all tasks due 365 days out)
- **Sparsity** in optional fields (descriptions, comments, tags) to mimic real workspaces

## Validation and QA

Validation is invoked from `src/main.py` using `src/utils/validation.py` and covers:

- Schema checks
- Temporal consistency checks
- Referential integrity checks
- Business rules checks
- Distribution checks (where supported by schema)

The generator is designed to *continue* even when non-critical validation categories report failures, because the dataset can still be useful for RL evaluation. Validation output is intended to guide iterative realism improvements.

## Known Gaps and Roadmap

### 1) Distribution validation expects columns not present in schema

Current logs may show errors like `no such column: p.department` during distribution validation. This occurs because `projects` in `schema.sql` do not currently store `department` or `project_type`, while some validation queries assume they exist.

**Options:**

- Extend schema to include `projects.department` and `projects.project_type`, and populate during generation.
- Or: revise the validator to infer department/project type from naming heuristics.

### 2) Comments may be sparse

Some runs generate `0` comments. For richer interaction traces, increase comment probability and/or add “event-driven comments” (blocked tasks, requests for review, etc.).

### 3) Company context mismatch (company size)

The prompts mention 50–200 employees, but the assignment target is 5,000–10,000 employees. The generator supports scaling, but the prompt context should be updated for narrative consistency.

## References (Non-Exhaustive)

These references inform *qualitative realism priors* and validation heuristics:

- Asana: “Anatomy of Work” reports (task completion / coordination patterns)
- PMI: general project management benchmarks (cycle time / planning horizons)
- Research on knowledge-work temporal rhythms (weekday intensity patterns)
- Public issue trackers / project templates for task naming conventions

---

**File:** `docs/methodology.md`  
**Scope:** Methodology + research framing + table-by-table generation strategy  
