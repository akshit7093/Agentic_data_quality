---
status: investigating
trigger: "same issue of columns add print statements and log clearly showing before hand which colmns it recievde"
created: 2026-03-10T02:19:05Z
updated: 2026-03-10T02:39:00Z
---

## Current Focus
hypothesis: Column scope is lost or ignored between the API request and the LLM/Tool execution nodes.
test: Add extensive logging to trace `selected_columns` and `column_mapping` at every stage.
expecting: Identify exactly where the restricted list is dropped or bypassed.
next_action: Add `print()` and `logger.info()` to `routes.py`, `data_quality_agent.py`, and `tool_based_agent.py`.

## Symptoms
expected: 1. Only columns selected in the UI should be analyzed. 2. UI Slice filters (e.g. state='CA') should be applied to the data analyzed by the agent. 3. Pivots/subset views should be respected.
actual: 1. LLM/Tools see all columns in prompts/execution. 2. Agent analyzes the entire dataset regardless of UI filters.
errors: Persistent escape of column scope and filter constraints.

## Hypotheses
| # | Hypothesis | Likelihood | Status |
|---|------------|------------|--------|
| 1 | `selected_columns` in `AgentState` is not populated correctly from the request. | 30% | UNTESTED |
| 2 | `ValidationToolExecutor` is using `target_table` schema instead of `selected_columns` for placeholder replacement. | 40% | UNTESTED |
| 3 | LLM prompts are not strictly enforcing the scope, or the scope note is missing in some nodes. | 30% | UNTESTED |

## Evidence
- Previous fix implemented schema filtering in `_setup_connection` and prompt constraint in `_explore_data`.
- User reports it's still failing, suggesting a bypass or a regression.

## Eliminated
- None yet.
