# Debug Session: SQL Pushdown Crash

## Symptom
The `uvicorn` logs show `Native SQL execution failed: unhashable type: 'slice'. Falling back to sandbox.` and another error `expected string or bytes-like object` when attempting to pushdown native queries. 

**When:** When the AI agent attempts to run a query tool payload.
**Expected:** The native connector executes the query and returns a properly parsed result dictionary.
**Actual:** The SQLite connector's `execute_raw_query` returns a dictionary payload, but `data_quality_agent.py` assumes it returns a list and tries to slice it `pushdown_results[:5]`, crashing with `unhashable type: 'slice'`. Furthermore, when LLMs hallucinate a payload missing the `query` field, `query = None` crashing SQL driver with `expected string or bytes-like object`.

## Hypotheses

| # | Hypothesis | Likelihood | Status |
|---|------------|------------|--------|
| 1 | `data_quality_agent.py` handles the return of `execute_raw_query` incorrectly. | 100% | CONFIRMED |

## Attempts

### Attempt 1
**Testing:** H1 — `data_quality_agent.py` incorrectly slices `pushdown_results` dict.
**Action:** Checked `sqlite.py` line 320 to verify what `execute_raw_query` returns.
**Result:** Returns `{"status": "success", "row_count": int, "sample_rows": list}`.
**Conclusion:** CONFIRMED.

## Resolution

**Root Cause:** The return type of the connector's native execution is a dictionary, not a list of rows. Thus slicing it throws an unhashable type exception. Additionally, `query` could be None if omitted by LLM.
**Fix:** Plan 1.3 to update `_execute_query_tool` to validate `query` is not None, and securely bridge the `Dict` API to `tool_result`.
