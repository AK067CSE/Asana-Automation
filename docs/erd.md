# Entity-Relationship Diagram (ERD)

This project uses a relational schema intended to mirror the core Asana entities used in project management workflows.

## How to Generate a Visual ERD

You can generate a diagram using any of:

- dbdiagram.io
- draw.io (diagrams.net)
- Lucidchart

### Option A: dbdiagram.io (recommended)

1. Open https://dbdiagram.io
2. Create a new diagram
3. Convert `schema.sql` to DBML:
   - If you already have a converter, use it.
   - Otherwise, paste the schema and manually define tables.
4. Export as PNG/SVG and save it as `docs/erd.png` or `docs/erd.svg`.

### Option B: diagrams.net

1. Open https://app.diagrams.net/
2. Create entities (tables) as boxes
3. Add connectors for foreign keys
4. Export to `docs/erd.png`

## Logical ERD (Text)

Below is a text ERD that matches `schema.sql`.

- `organizations (1) → (N) teams`
- `organizations (1) → (N) users`
- `organizations (1) → (N) projects`
- `organizations (1) → (N) tags`
- `organizations (1) → (N) custom_field_definitions`

- `teams (1) → (N) team_memberships`
- `users (1) → (N) team_memberships`

- `projects (1) → (N) sections`
- `projects (1) → (N) tasks`

- `sections (1) → (N) tasks`

- `users (0..1) → (N) tasks` via `tasks.assignee_id` (nullable)

- `tasks (1) → (N) subtasks`
- `tasks (1) → (N) comments`

- `users (1) → (N) comments`

- `custom_field_definitions (1) → (N) custom_field_values`
- `tasks (1) → (N) custom_field_values`

- `tasks (N) ↔ (N) tags` via `task_tags`

## ERD Design Notes (Research Perspective)

- **M:N relationships** are represented via explicit link tables (`team_memberships`, `task_tags`). This supports realistic UI behaviors like multi-team users and multi-tag tasks.
- **Task hierarchy** uses a dedicated `subtasks` table rather than a recursive `tasks.parent_id`. This keeps queries simpler and mirrors how many analytics pipelines separate parent/child task events.
- **Custom fields** are normalized into definitions + values to support organization-level field reuse and task-level heterogeneity.
