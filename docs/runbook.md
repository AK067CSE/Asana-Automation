# Runbook: Generating Seed Data

This runbook documents how to run the generator, what outputs to expect, and how to interpret validation signals.

## Quick Start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Create a `.env` file (you can copy `.env.example` if present):

- `OPENAI_API_KEY=...` (required for LLM text generation if enabled)
- `DATABASE_PATH=output/asana_simulation.sqlite` (optional)

3. Run:

```bash
python src/main.py
```

The resulting SQLite DB should be written to:

- `output/asana_simulation.sqlite`

## Reproducible Fresh Runs

The database schema initialization will fail if the DB already exists and has tables.

Recommended workflow:

- Delete `output/asana_simulation.sqlite` before rerunning.

## Outputs

### SQLite database

The DB includes:

- Core Asana-like entities: orgs, teams, users, projects, sections, tasks, subtasks, comments
- Enrichment: tags and custom fields (schema supports these even if sparsely generated)

### Logs

The pipeline emits logs for:

- entity counts (how many teams/projects/tasks were generated)
- warnings about LLM/pattern formatting fallback
- validation summaries

## Validation: How to Interpret Signals

Validation is designed as a *diagnostic tool* rather than a strict “gate”.

- **Schema validation** failing is critical.
- **Referential integrity** failing is critical.
- **Temporal consistency** violations at low rates are expected in early iterations; target is to continuously reduce.
- **Distribution validation** depends on the schema containing the columns used in analytics queries.

## Common Issues and Fixes

### 1) `table organizations already exists`

Cause:

- Re-running without deleting the existing DB file.

Fix:

- Delete `output/asana_simulation.sqlite` and rerun.

### 2) Foreign key constraint failures on dependent entities

Cause:

- Child entities generated using placeholder IDs.

Fix:

- Insert parent rows first to obtain IDs, then generate children.

This repo follows:

- insert projects → then generate/insert sections
- insert tasks → then generate/insert subtasks

### 3) Validation query errors like `no such column: p.department`

Cause:

- Validator expects columns that are not in `projects` in the current schema.

Fix options:

- Add `department` and `project_type` columns to `projects` and populate them.
- Or adjust validation queries to align with current schema.

## QA Checklist (Research Scientist Lens)

Before submitting a dataset for RL evaluation, check:

- Referential integrity violations: should be 0
- Obvious artifacts: “Task 1/Task 2” prevalence should be near 0
- Due-date realism: strong weekday bias and bounded planning horizon
- Completion realism: completion rates vary by project type and project age
- Edge cases exist: overdue tasks, unassigned tasks, archived projects

## Performance

For large simulated org sizes, generation time depends heavily on:

- LLM calls (network latency + rate limits)
- volume of tasks and comments

If you need a “fast mode”, disable LLM generation and rely on pattern libraries.
