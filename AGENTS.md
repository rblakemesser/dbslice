# dbslice – Agent Guidelines

This project follows the same working principles we use in cjdev. When building or modifying code here, follow these rules strictly:

1) No naked exception catches
- Do not write `except Exception: pass` or swallow errors. If a step fails, let the exception propagate. Fail loudly so the problem is fixed, not hidden.

2) Never fail silently
- Don’t add fallbacks that mask errors. If we can’t precisely deliver the intended state, we fail and report the error.

3) Fix the root cause; don’t paper over problems
- Avoid quick hacks that break or skip core functionality. If something essential (DDL/DML, sequences, constraints, uploads) fails, diagnose and fix it properly.

4) Idempotent and explicit
- Make steps safe to re-run (IF EXISTS/IF NOT EXISTS where appropriate). Prefer explicit DDL/DML that yields the exact, intended state.

5) Keep scope tight and focused
- Implement only what’s required. Don’t introduce unrelated changes. Keep code small, composable, and testable.

6) Deterministic tests
- Tests must be reliable and verify the same behaviors before and after changes. No flakiness.

7) No implicit defaults that surprise users
- Require explicit `--config` for actions. Don’t assume profiles. Be clear about inputs and outputs.

8) Observability and verbosity
- Emit structured outputs for audits and summaries (YAML). Avoid noisy preambles in CLI output that break parsing.

9) Security and hygiene
- Do not hardcode credentials in code. Use env variables or test-only fixtures. Keep example configs safe for open source.

10) Collaboration
- Leave the codebase better than you found it. Prefer readable, well-structured code over clever shortcuts.

11) Do not guess schema, always check
- Query the schema of the target database and check example records before you migrate family tables and their relationships in. Check the original repo for its definitions but do not trust them. Check independently. When you find inconsistencies, do not guess. Ask the operator for clarity. The whole point of this tool is to migrate complicated schemas that may have unintuitive relationships.

## Running Tests

The test suite expects a Postgres service named `db`. Run tests via Docker Compose to ensure networking and data fixtures match the expected environment.

### One‑shot (recommended)

```
docker compose up --exit-code-from tests tests
```

This brings up Postgres, waits for health, installs deps, runs pytest, and exits with pytest’s status code.

### Iterative workflow

1) Start the database once:

```
docker compose up -d db
```

2) Run the tests service on demand:

```
docker compose run --rm tests
```

3) Focus on a subset (override the command to add pytest args):

```
docker compose run --rm tests \
  sh -lc "pip install -U pip && pip install -e . && pip install -q pytest && pytest -q -k neuter"
```

Stop services when done:

```
docker compose down
```

### Notes

- The `db` container seeds a fixture schema from `tests/fixtures/minicom` at startup.
- Host debugging: Postgres is exposed on `localhost:54329`. You can connect with:

```
psql postgresql://postgres:postgres@localhost:54329/postgres
```

- Running pytest directly on the host will fail to resolve the hostname `db`. Prefer the Compose flows above. If you must run the CLI locally against the Compose DB, set:

```
export DATABASE_URL=postgresql://postgres:postgres@localhost:54329/postgres
```

## Failure Policy and Refactor Workflow

- Fail loudly: do not add broad catches or suppress import errors. If an import or DB step fails, let it error so we can fix the root cause.
- Preserve behavior during refactors unless the user explicitly requests a change. When a rename or shape change is requested (e.g., `families` → `table_groups`), perform a direct cutover without adding backward‑compatibility shims.
- Incremental, verified steps: after each refactor phase, run the full test suite via Docker Compose and proceed only when green.
- No implicit fallbacks: avoid “best effort” behavior that hides divergence from the intended state.
- No backward compatibility layers unless explicitly requested. This is a single‑user project; keep configuration canonical and remove deprecated keys/paths immediately.
- Keep modules focused: split large modules into logical units (schema, tables, indexes, constraints, sequences, triggers, functions, neuter) and re‑export to maintain the public API.

## Performance: Parallel Fanout

- Fanout concurrently: build shard CTAS and shard INSERTs in parallel using multiple DB connections.
- Async I/O: use asyncio + psycopg AsyncConnection, not threads.
- Generic knob: expose `--fanout-parallel N` to cap concurrency; always run fanout in parallel (no sequential fallback).
- DSN required: if a DSN cannot be determined for fanout, treat it as a bug and fail loudly.
- Pooling: open short‑lived connections per shard task; commit per task; cap with a semaphore to avoid overload.
- Safety: create destination tables before parallel inserts; add PKs/indexes after data load; set LOGGED after load, not during.
- Performance-first mindset: if you're trying to fix a sharding issue by deduplicating, THATS WRONG. Always fix the fanout strategy rather than working around problems.

## Separation of Concerns: Engine vs. Profile

Keep the Python code generic. All application‑ or schema‑specific behavior must live in the YAML profile (for example, profiles/cratejoy.yml), not in the engine code.

- No hardcoded table or column names in Python. Do not reference app tables like product, order, image, etc. Use config‑driven roots, deps, joins, and where clauses.
- Data selection belongs in YAML. Express what to copy (roots) and how dependents link using:
  - roots: name, table, id_col, selector (mode: list|sql, params) and optional shard.
  - families: root table and deps with parent_table, join expressions, and optional where, distinct, or sources for unions.
- Stage‑dependent selections. If a root must reference stage.* tables (e.g., copy only rows referenced by already‑staged data), represent that in the profile. Do not add Python helpers that query specific tables. If sequencing is required (second‑phase roots), add a separate root section or run a second migrate_tables pass with a filtered config.
- Generic knobs only in code. Concurrency flags, idempotence, validation, logging, and schema reconcilers are generic and acceptable. They must not assume domain schema.
- Redactions/Neuter live in YAML. Define neuter.targets in the profile; do not mutate data inline in Python.
- Env and secrets. Never hardcode credentials or tenant ids in code. Use env vars and YAML inputs.

Examples
- Good: Add `--validate-parallel N` to cap global FK validation workers and group by table to avoid lock contention. No table names baked in.
- Bad: Add a Python helper like migrate_referenced_images() that reads stage.listing_image. That belongs in the profile as a root (or post‑phase step) using SQL selectors and family deps.

When in doubt, keep the engine minimal and push behavior to the profile.

## Code Quality: DRY and Clean Pipelines

- Avoid copy/paste of core pipeline logic. If two CLI paths run the same migration phases (e.g., `--migrate` and `--restart`), factor a single helper to orchestrate precopy → selections → families → neuter → sequences → functions → triggers → constraints. Keep one source of truth.
- Prefer small, composable helpers in Python that accept config and return structured results. This keeps behavior consistent and testable across entrypoints.
- Beware drift when you “bolt on” a new flag by cloning code. It’s easy to miss a future bugfix or enhancement in one path. Refactor immediately to remove duplication.
- Keep output tidy and bounded. Emit counts or summaries, not giant ID lists, to avoid noisy logs and performance hiccups.
