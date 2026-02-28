---
phase: 1
plan: 1
wave: 1
---

# Plan 1.1: Backend Agent Fixes (Connector Pushdown & Reporting)

## Objective
Fix the critical accuracy issues inside `data_quality_agent.py`. Currently, `execute_agent_query` falls back to an in-memory SQLite database composed of a 1000-row sample data, missing issues that exist in the full dataset. Furthermore, the final report generation trusts hallucinated facts injected into the LLM context.

## Context
- .gsd/SPEC.md
- backend/app/agents/data_quality_agent.py
- backend/app/agents/state.py
- backend/app/connectors/factory.py
- backend/app/validation/engine.py

## Tasks

<task type="auto">
  <name>Fix `_execute_query_tool` Native Pushdown</name>
  <files>backend/app/agents/data_quality_agent.py</files>
  <action>
    - Import `ConnectorFactory` if not already present.
    - Before calling `ValidationEngine().execute_agent_query()`, check `data_source_info.source_type`.
    - If it's a real database (e.g. postgresql, sqlite), instantiate the connector (`ConnectorFactory.create_connector`).
    - Connect to the DB and execute the query using the connector's native `execute_raw_query(query)` method.
    - Return the resulting row count and sample directly.
    - If native execution fails or for file-based sources, fallback to `engine.execute_agent_query`.
  </action>
  <verify>pytest backend/app/agents/test_data_quality_agent.py (if exists) or manual verification via the UI triggering a run against a large table.</verify>
  <done>Native SQL queries correctly target the live database rather than the sample-data sandbox.</done>
</task>

<task type="auto">
  <name>Generate Factual Report</name>
  <files>backend/app/agents/data_quality_agent.py</files>
  <action>
    - Update `_generate_report` to NOT parse the `<REPORT>` tag for numbers.
    - Save the agent's prose from the `<REPORT>` tag simply as `agent_narrative`.
    - Build `summary_report` string explicitly by iterating over `state.get("validation_results", [])`. Note how many failed, how many passed, using facts directly from the objects.
  </action>
  <verify>Run the backend data execution to ensure total_count and row counts reflect the reality, not just hallucinated counts.</verify>
  <done>The final text and report numbers match exactly what the query engine returned.</done>
</task>

## Success Criteria
- [ ] Agent validation accurately spots errors over full table sizes, not just 1000 rows.
- [ ] `summary_report` exactly matches the `validation_results` array without hallucinations.
