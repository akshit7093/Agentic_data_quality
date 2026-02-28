---
phase: 1
plan: 2
wave: 1
---

# Plan 1.2: UI Quick-Fix Exports and UI features

## Objective
Implement a "quick fix" export flow that allows users to seamlessly apply agent-recommended fixes for data validation issues, and export the corrected data as desired. 

## Context
- .gsd/SPEC.md
- backend/app/api/routes.py
- frontend/src/pages/ValidationFixExport.jsx

## Tasks

<task type="auto">
  <name>Implement Backend Endpoints for Quick Fixes / Export</name>
  <files>backend/app/api/routes.py</files>
  <action>
    - Add an endpoint `GET /api/v1/validate/{id}/quick-fixes` to return the `fix_recommendations` generated during validation.
    - Add an endpoint `POST /api/v1/validate/{id}/fix` accepting the selected `FixAction`.
    - Apply the fix transformations to the data (executing generated SQL/Pandas against an in-memory or staging table) and prepare the `export_result`.
    - Add an endpoint `POST /api/v1/validate/{id}/export` accepting `ExportConfig` and returning the generated file (CSV, Excel).
  </action>
  <verify>Call validation endpoints via cURL or browser to test retrieval of quick fixes and exporting an excel file.</verify>
  <done>User can fetch fixes, select actions, and download clean data files via API.</done>
</task>

<task type="auto">
  <name>UI Quick Fix & Export Component</name>
  <files>frontend/src/pages/ValidationFixExport.jsx</files>
  <action>
    - Load the validation `quick-fixes`.
    - Provide UI toggles or buttons to accept fixes (`auto_agent`).
    - On acceptance, trigger `POST /validate/{id}/fix`.
    - Present a standard export modal or download button that hits `POST /validate/{id}/export`.
  </action>
  <verify>Manually open the specific validation page in the browser and interact with the toggle + export buttons to ensure an excel/CSV downloads correctly.</verify>
  <done>The React frontend can correctly trigger auto-fixes downstream and output a valid file.</done>
</task>

## Success Criteria
- [ ] Users can visibly click "Fix" on generated agent recommendations.
- [ ] Export API handles both the raw untouched rows and modified clean rows based on `ExportConfig`.
