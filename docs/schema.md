# Database Schema (SQLite)

This document describes the SQLite schema used for the Asana-like simulation dataset.

The schema is defined in `schema.sql` and is designed to support realistic enterprise workflows for an Asana RL environment.

## Goals and Constraints

- Preserve **referential integrity** for all core entities (no orphaned records).
- Support **realistic interaction queries** needed by UI agents:
  - list tasks in a project/section
  - filter by assignee/due date/completion
  - attach tags/custom fields
  - read/write task comments
- Keep the schema **SQLite-friendly** (explicit FK constraints + indexes).

## Tables

### `organizations`

Represents a workspace/company boundary.

- **Primary key:** `id`
- **Uniqueness:** `domain` is unique (one verified domain per org).

### `teams`

Team groupings inside an organization.

- **Primary key:** `id`
- **Foreign keys:**
  - `organization_id → organizations.id`

### `users`

Users belonging to an organization.

- **Primary key:** `id`
- **Foreign keys:**
  - `organization_id → organizations.id`
- **Constraints:**
  - `email` is globally unique
  - `role ∈ {admin, member, guest}`

### `team_memberships`

Many-to-many link between users and teams.

- **Primary key:** `id`
- **Foreign keys:**
  - `team_id → teams.id`
  - `user_id → users.id`
- **Constraints:**
  - `UNIQUE(team_id, user_id)` prevents duplicates
  - `role ∈ {owner, member}`

### `projects`

Projects are the main containers for work.

- **Primary key:** `id`
- **Foreign keys:**
  - `organization_id → organizations.id`
- **Constraints:**
  - `status ∈ {active, completed, archived}`

### `sections`

Sections subdivide a project (e.g., list/board columns).

- **Primary key:** `id`
- **Foreign keys:**
  - `project_id → projects.id`

### `tasks`

Atomic work items.

- **Primary key:** `id`
- **Foreign keys:**
  - `project_id → projects.id`
  - `section_id → sections.id`
  - `assignee_id → users.id` (nullable; `ON DELETE SET NULL`)
- **Constraints:**
  - `priority ∈ {high, medium, low, none}`

### `subtasks`

Secondary work items nested under a task.

- **Primary key:** `id`
- **Foreign keys:**
  - `task_id → tasks.id`

### `comments`

Comments attached to tasks.

- **Primary key:** `id`
- **Foreign keys:**
  - `task_id → tasks.id`
  - `user_id → users.id`

### `custom_field_definitions`

Organization-scoped definitions of custom fields.

- **Primary key:** `id`
- **Foreign keys:**
  - `organization_id → organizations.id`
- **Constraints:**
  - `field_type ∈ {text, number, date, enum, boolean}`
  - `enum_options` stored as JSON (TEXT)

### `custom_field_values`

Task-scoped values for custom fields.

- **Primary key:** `id`
- **Foreign keys:**
  - `custom_field_definition_id → custom_field_definitions.id`
  - `task_id → tasks.id`

Values are stored in typed columns:

- `value_text`
- `value_number`
- `value_date`
- `value_boolean`
- `value_enum`

### `tags`

Organization-scoped labels.

- **Primary key:** `id`
- **Foreign keys:**
  - `organization_id → organizations.id`
- **Constraints:**
  - `UNIQUE(organization_id, name)`

### `task_tags`

Many-to-many link between tasks and tags.

- **Primary key:** composite `(task_id, tag_id)`
- **Foreign keys:**
  - `task_id → tasks.id`
  - `tag_id → tags.id`

## Indexes

Indexes are created for common access paths:

- tasks by `project_id`, `section_id`, `assignee_id`, `due_date`, `completed`
- comments by `task_id`, `user_id`
- memberships by `team_id`, `user_id`
- task_tags by `task_id`, `tag_id`
- custom_field_values by `task_id`

## Views

### `active_tasks`

Convenience view joining tasks with assignee + project + section metadata for incomplete tasks.

### `project_overview`

Aggregates task counts by project, including overdue counts.

## Notes for Extension

If you want validation and analytics by project type/department, consider adding columns to `projects` such as:

- `department TEXT`
- `project_type TEXT`

This makes distribution validation easier and avoids inferring structure from names.
