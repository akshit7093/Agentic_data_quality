---
phase: 1
plan: 4
wave: 2
---

# Plan 1.4: Fix Empty Queries in Sandbox Engine

## Objective
The `ValidationEngine.execute_agent_query` crashes when `query` is `None` (usually due to an LLM hallucination where it omits the `query` field from the JSON payload). While Plan 1.3 fixed the native pushdown layer, the fallback layer inside `ValidationEngine` also needs to gracefully reject `None` queries to prevent crashing the entire agent.

## Context
- backend/app/validation/engine.py
- .gsd/DEBUG.md

## Tasks

<task type="auto">
  <name>Patch `ValidationEngine.execute_agent_query`</name>
  <files>backend/app/validation/engine.py</files>
  <action>
    - Add a check at the beginning of `execute_agent_query` (around line 44) to verify if `query` is falsy or `None`.
    - If `query` is falsy or `None`, return `{"status": "error", "error": "Agent provided an empty or null query. Please formulate a valid SQL or Pandas query string."}`.
    - This gracefully informs the agent to retry instead of throwing a generic server 500 exception via SQLAlchemy.
  </action>
  <verify>Run the validation scan again and ensure no 500 Internal Server errors occur during Agent validation.</verify>
  <done>Sandbox correctly rejects empty queries without crashing.</done>
</task>

## Success Criteria
- [ ] No `expected string or bytes-like object` traces log out from the engine.
