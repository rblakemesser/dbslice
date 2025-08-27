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

