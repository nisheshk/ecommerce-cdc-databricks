# ecommerce-cdc-databricks

E-commerce Change Data Capture (CDC) streaming pipeline on Databricks, built to shared data engineering standards (`de-*` skills).

## Layout

| Path | Purpose |
|------|---------|
| `instructions/` | Short always-on rules: naming, audit columns, PII |
| `.agents/skills/` | Canonical skills. Edit them only here |
| `.claude/skills` | Symlink to `.agents/skills` for Claude Code |
| `src/ecommerce/` | Generator, pipelines (bronze/silver/gold), governance, transforms |
| `tests/` | Unit and integration tests |
| `resources/` | Bundle resource definitions (jobs, pipelines) |
| `scripts/` | Workspace sync and skill linting |

## How each tool picks it up

- **Claude Code:** `CLAUDE.md` plus the `.claude/skills` symlink.
- **Genie Code:** `deploy.yml` copies `.agents/skills/*` to `/Workspace/.assistant/skills/` and `instructions/.assistant_workspace_instructions.md` to `/Workspace/.assistant_workspace_instructions.md`. Each engineer can copy `instructions/.assistant_instructions.md` to `/Users/<email>/.assistant_instructions.md` for personal rules.
- **Cursor, Codex and others:** read `AGENTS.md` and `.agents/skills/` natively.
