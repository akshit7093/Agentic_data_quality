---
phase: 1
plan: 3
wave: 2
---

# Plan 1.3: Native Pushdown Crash & None-Query Fix

## Objective
The agent crashes on native SQL pushdown and falls back to the in-memory sandbox. This defeats the purpose of Plan 1.1's pushdowns. This plan aims to correct the unhashable type slice error by properly handling the `dict` response from the native connector, and rejecting `query: None` anomalies.

## Context
- backend/app/agents/data_quality_agent.py
- .gsd/DEBUG.md

## Tasks

<task type="auto">
  <name>Patch `_execute_query_tool` Type Mismatches</name>
  <files>backend/app/agents/data_quality_agent.py</files>
  <action>
    - Add a check for `not query` after parsing `action_data`. If None, raise ValueError("Query not provided by LLM").
    - If `source_type` matches native SQL, assign `pushdown_results` directly to `tool_result` if `pushdown_results["status"] == "success"`.
    - Do not attempt to run `len(pushdown_results)` or `pushdown_results[:5]`.
    - Rely on the connector's internal truncation and count.
  </action>
  <verify>Run a UI validation scan against the test database and ensure Agent doesn't revert to sandbox queries.</verify>
  <done>Native SQL fallback works successfully without slicing dictionaries.</done>
</task>

## Success Criteria
- [ ] No `unhashable type: 'slice'` traces log out.
- [ ] No `None` type crashes for SQL.
