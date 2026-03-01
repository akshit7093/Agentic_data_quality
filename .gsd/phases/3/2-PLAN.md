---
phase: 3
plan: 2
wave: 2
---

# Plan 3.2: Dual Validation Agents & Custom Rules Flow

## Objective
Split the monotonic `DataQualityAgent` into distinct `SchemaValidationAgent` and `BusinessAnalystAgent` workflows, and improve the UI custom rule creation experience.

## Context
- .gsd/SPEC.md
- backend/app/agents/data_quality_agent.py
- frontend/src/pages/Home.tsx
- frontend/src/components/CreateRuleModal.tsx (to be created or modified)

## Tasks

<task type="auto">
  <name>Implement Schema vs. Business Agents</name>
  <files>
    - backend/app/api/routes.py
    - backend/app/models/validation.py
    - backend/app/agents/schema_agent.py (NEW)
    - backend/app/agents/business_agent.py (NEW)
    - backend/app/agents/data_quality_agent.py (Refactor)
  </files>
  <action>
    - Update `ValidationMode` enum in `models.py` to include `schema_only` and `business_analysis`.
    - Create `SchemaValidationAgent`: Focuses strictly on `pragma_table_info`, null references, type casting issues, and structural integrity.
    - Create `BusinessAnalystAgent`: Focuses on outlier detection, logical anomalies (e.g. negative prices, high fraud), and multidimensional insights.
    - Refactor `routes.py` and `data_quality_agent.py` so the workflow routes to the correct LangGraph implementation based on `validation_mode`.
  </action>
  <verify>Run validations on the same dataset using `schema_only` vs `business_analysis` and verify the execution traces reflect different agent focus areas.</verify>
  <done>Frontend dropdown allows selecting the agent type, and backend executes the requested graph.</done>
</task>

<task type="auto">
  <name>Custom Rule Interactive Discovery UI</name>
  <files>
    - frontend/src/pages/Home.tsx
    - backend/app/api/routes.py
  </files>
  <action>
    - Create a new API endpoint `GET /data-sources/{id}/schema` to quickly fetch table columns and data types without running a full validation.
    - When "Custom Rules" is selected in the UI, execute this endpoint.
    - Render a column-picker UI allowing users to point-and-click to build a rule (e.g. [Column: price] [Operator: >] [Value: 40]).
    - Translate this UI configuration into a SQL WHERE clause payload to send to the backend.
  </action>
  <verify>Navigate to Custom Rules on the UI, select a target, wait for schema fetch, and build a `price > 40` rule via UI.</verify>
  <done>Custom rules can be built using an interactive schema-aware dropdown instead of raw text.</done>
</task>

## Success Criteria
- [ ] Users can choose between Schema vs Business Data validation or both.
- [ ] Custom validation rules can be built interactively based on live schema data.
