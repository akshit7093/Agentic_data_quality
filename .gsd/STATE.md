# State Dump - 2026-03-09

## Current Position
- Resolved `uvicorn` crash.
- Harmonized LLM configuration naming.
- Resolved Agent SQL syntax errors (identifier quoting).
- Updated all documentation and verified fix.

## Knowledge Gathered
- Pydantic v2 `BaseSettings` (if configured strictly) will crash on extra fields from `.env`.
- `app/api/routes.py` had hardcoded `LLM_MODEL` strings that didn't match `app/core/config.py`.

## Remaining Tasks
- [x] Gather details from `uvicorn` terminal output
- [x] Research codebase for context (config.py and .env)
- [x] Define symptoms and form hypotheses in `DEBUG.md`
- [x] Test hypotheses and isolate the root cause
- [x] Implement and verify fix
- [x] Update `STATE.md` and finalize
