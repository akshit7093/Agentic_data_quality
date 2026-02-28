---
phase: 2
level: 2
researched_at: 2026-02-28
---

# Phase 2 Research: UI Execution Trace & Actionable Filters

## Questions Investigated
1. How is the agent execution log currently formatted and rendered?
2. How can we segregate the execution trace into Thought, Action, Output, and Context?
3. How are validation results currently filtered and how can we add "Fixability" and manual ticket creation?

## Findings

### 1. Execution Log Structure
Currently, `backend/app/validation/engine.py` and `data_quality_agent.py` append raw strings to the `messages` array in the validation result. 
**Example Format:**
- `System: Connected to target...` (Context)
- ` ```json { "action": ... } ``` ` (Tool Call / Action)
- `QUERY RESULTS: { ... }` (Tool Output)
- Text outside code blocks or `<REPORT>` tags (Agent Thought)
- `<REPORT>...</REPORT>` (Final Output)

**Recommendation:** Instead of rewriting the entire backend logger which might break existing schemas, we can write a sophisticated React component `ExecutionTraceLog.tsx` that parses the `msg.content` strings into structured blocks using Regex/string-matching, rendering them as distinct visual cards (e.g., a "Brain" icon for thought, "Terminal" for action/command, "Database" for context).

### 2. Actionable Validation Results & Filters
`ValidationDetail.tsx` renders the results in a flat list. There are no filters.
**Recommendation:**
- Build a generic `FilterBar` component for the Results tab with dropdowns for:
  - **Column**: Extract column names from the rule target.
  - **Severity**: Critical, Warning, Info.
  - **Fixability**: Auto-fixable (AI), Manual Action Required.
- Embed the Quick Fix capabilities (currently in `ValidationFixExport.jsx`) directly into the `ValidationDetail.tsx` results list.
- Allow the user to input an optional `additional_instructions` text field before clicking "Auto-Fix".

### 3. Ticket Generation for Manual Errors
For errors that the AI cannot automatically fix (e.g. missing external data, physical discrepancies), the user requested a "ticket like structure".
**Recommendation:**
- Add a "File Ticket with AI" button for failed rows.
- This delegates to an endpoint (e.g., `POST /api/v1/validate/{id}/ticket`) that uses an LLM to generate a well-structured Jira/GitHub issue describing the failure, the impacted rows, the schema context, and suggested manual resolution steps.

## Decisions Made
| Decision | Choice | Rationale |
|----------|--------|-----------|
| Log Parsing | Frontend parser component | Keeps backend fast and backward-compatible with existing validations in Chroma/SQLite. |
| UI Integration | Merge fix logic into Detail View | The user is currently disjointed between `ValidationDetail` and `ValidationFixExport`. Bringing them together is better. |
| Ticketing | Dedicated Backend Endpoint | Generating structured tickets requires LLM prompting tailored to ticketing formats. |

## Ready for Planning
- [x] Questions answered
- [x] Approach selected
- [x] Dependencies identified
