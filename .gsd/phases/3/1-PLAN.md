---
phase: 3
plan: 1
wave: 1
---

# Plan 3.1: Data Slicing & Ticketing Fix

## Objective
Implement Pre-Validation Data Slicing and fix the broken Ticketing endpoint (`404 Rule not found`).

## Context
- .gsd/SPEC.md
- .gsd/ROADMAP.md
- backend/app/api/routes.py
- frontend/src/pages/ValidationDetail.tsx
- frontend/src/pages/Home.tsx (Target for Selection)

## Tasks

<task type="auto">
  <name>Fix Ticketing Endpoint</name>
  <files>
    - backend/app/api/routes.py
    - frontend/src/pages/ValidationDetail.tsx
  </files>
  <action>
    - Look into `POST /validate/{validation_id}/ticket`. The UI seems to be passing a `rule_name` or `rule_id` that doesn't exactly match the `ValidationResult.rule_name`.
    - Modify the lookup logic in `routes.py` line 872 to check both `r.rule_name` (as object attribute) and `r.rule_id`.
    - Ensure `ValidationResult` objects are dumped to dictionary properly if accessed via `.get()`.
  </action>
  <verify>Run the `generate_ticket` endpoint via UI and confirm a 200 OK + Markdown ticket.</verify>
  <done>Tickets generate successfully without 404s.</done>
</task>

<task type="auto">
  <name>Implement Pre-Validation Slicing</name>
  <files>
    - backend/app/api/routes.py
    - frontend/src/pages/Home.tsx
    - backend/app/models/validation.py
  </files>
  <action>
    - Modify the `/validate` POST endpoint to accept a `slice_query` or `filters` object.
    - Update `ExecuteValidation` UI in `Home.tsx` to render a pivot/filter UI (e.g., specific columns and values) prior to triggering validation.
    - Update Native Connectors (`postgresql.py`, `sqlite.py`) to inject the `slice_query` as a `WHERE` clause when sampling or validating data.
  </action>
  <verify>Execute a validation with a slice filter and verify the AI only analyzes the filtered subset.</verify>
  <done>Slicing UI exists and correctly restrains the validation agent scope.</done>
</task>

## Success Criteria
- [x] Ticketing bug is fixed.
- [x] Users can filter the dataset structurally before passing it to the validation agent.
