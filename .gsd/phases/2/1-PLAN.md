---
phase: 2
plan: 1
wave: 1
---

# Plan 2.1: UI Execution Trace & Actionable Filters

## Objective
To significantly improve the frontend UX of the AI Data Quality Agent. We will separate the monolithic execution text logs into a highly readable, structured timeline (Thought, Action, Output, Context). We will also augment the Validation Results tab with advanced filtering (Column, Criticality, Fixability), integrated Quick Fix execution, and an AI Ticket Generation system for manual errors.

## Context
- `frontend/src/pages/ValidationDetail.tsx`
- `frontend/src/pages/ValidationFixExport.jsx`
- Backend endpoints for Quick-Fixes.

## Tasks

<task type="manual">
  <name>Create `ExecutionTraceViewer` Component</name>
  <files>frontend/src/components/ExecutionTraceViewer.tsx, frontend/src/pages/ValidationDetail.tsx</files>
  <action>
    - Build a regex-based parser in React that takes the raw `messages` array from the validation result and groups them temporally.
    - Extract System messages as **Context**.
    - Extract ` ```json { action: ... } ``` ` blocks as **Commands/Actions**.
    - Extract text outside of JSON/Report blocks as **Thoughts**.
    - Extract ` <REPORT>...</REPORT> ` or `QUERY RESULTS: ...` as **Output**.
    - Render these in a vertical timeline with distinct styled cards (e.g., using `lucide-react` icons like Brain, Terminal, Database).
    - Replace the crude map loop in `ValidationDetail.tsx` (the 'logs' tab) with this new component.
  </action>
  <verify>Open an existing validation and view the "Agent Execution Log" tab. The logs should be broken down visually instead of being a wall of text.</verify>
</task>

<task type="manual">
  <name>Implement Advanced Validation Filters</name>
  <files>frontend/src/pages/ValidationDetail.tsx</files>
  <action>
    - Add a `FilterBar` to the top of the "Validation Results" tab.
    - **Filters**: 
      1. Severity (Critical, Warning, Info)
      2. Fixability (AI Fixable vs Manual)
    - Automatically determine "Fixability" by checking if the rule type allows automated update statements (e.g., missing values/formatting are often AI-fixable; complex cross-table anomalies are manual).
    - Update the results list to respect these filters.
  </action>
  <verify>Select a severity filter and verify only matching rules are displayed.</verify>
</task>

<task type="manual">
  <name>Integrate AI Quick-Fixes directly in Results Tab</name>
  <files>frontend/src/pages/ValidationDetail.tsx</files>
  <action>
    - Add a checkbox selection to the results list.
    - Provide a master "Auto-Fix Selected" button.
    - Provide a text area: `Optional: Additional instructions for AI (e.g., "Set missing dates to today")`.
    - Hook the button up to the existing `POST /api/v1/validate/{id}/fix` API.
  </action>
  <verify>Select an AI-fixable error, type a custom instruction, and click Auto-Fix. Verify success alert.</verify>
</task>

<task type="manual">
  <name>Build Manual Error Ticket Endpoint</name>
  <files>backend/app/api/routes.py, backend/app/agents/ticketing_agent.py (new)</files>
  <action>
    - Create a fast `POST /api/v1/validate/{id}/ticket` endpoint.
    - Accept a `rule_name` or `rule_id` in the payload.
    - Use the LLM (LM Studio) to draft a markdown-formatted Jira/GitHub ticket based on the rule failure details and table schema.
    - Return the drafted string to the frontend.
  </action>
  <verify>Hit the endpoint via cURL or Swagger to ensure it returns a markdown ticket string.</verify>
</task>

<task type="manual">
  <name>Add "File Ticket" UI Action</name>
  <files>frontend/src/pages/ValidationDetail.tsx</files>
  <action>
    - For rules marked "Manual Action Required", render a "File Ticket with AI" button.
    - On click, call the new ticket endpoint.
    - Display the resulting ticket markdown in a modal where the user can copy it or (optionally) save it.
  </action>
  <verify>Click "File Ticket" on a manual error. Wait for the loading spinner, then verify the modal displays a valid Markdown ticket.</verify>
</task>

## Success Criteria
- [ ] The Agentic logs are beautifully structured and easy to read.
- [ ] Users can filter results by fixability.
- [ ] Users can trigger AI quick-fixes directly passing optional user constraints.
- [ ] Users can generate structured tickets for un-fixable manual interventions.
