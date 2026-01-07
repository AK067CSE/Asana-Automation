# Asana Seed Data Generator (SQLite)

This repository generates a realistic, relational SQLite dataset that approximates an Asana workspace. The primary use case is seeding a project-management RL environment with data that supports realistic UI workflows (search/filter/assign/comment/complete).

## What this repo produces

Running `python src/main.py` creates:

- **SQLite database** at `output/asana_simulation.sqlite`
- **Entities**: organizations, teams, users, team memberships, projects, sections, tasks, subtasks, comments, tags, task-tags, custom fields

## Documentation

- `docs/methodology.md` — methodology (table-by-table generation strategy, temporal/relational consistency, validation)
- `docs/schema.md` — schema reference for `schema.sql`
- `docs/erd.md` — ERD instructions + text ERD
- `docs/runbook.md` — how to run + debug + interpret validation

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Create an environment file:

- Create `.env`
- Set:
  - `OPENAI_API_KEY` (required if LLM text generation is enabled)
  - Optional: `DATABASE_PATH` (defaults to `output/asana_simulation.sqlite`)

## Run

```bash
python src/main.py
```

## Re-running (fresh database)

`schema.sql` creates tables without `IF NOT EXISTS`, so reruns will fail if the DB already contains tables.

- Delete `output/asana_simulation.sqlite` before rerunning.

## Notes

- Validation runs at the end of generation. Some validation categories may be flagged depending on schema/validator alignment.
- Prompts (if using LLM generation) are stored in `prompts/`.