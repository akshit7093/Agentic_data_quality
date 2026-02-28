---
phase: 3
plan: 3
wave: 2
---

# Plan 3.3: Data Exploration, Slicing & Pivot UI

## Objective
Implement an advanced Data Exploration, Slicing & Pivot section that triggers immediately after dataset selection and before validation configuration, providing type-aware dynamic filters and pivot capabilities.

## Context
- .gsd/SPEC.md
- frontend/src/pages/NewValidation.tsx

## Tasks

<task type="auto">
  <name>Implement Column Explorer</name>
  <files>
    - frontend/src/pages/NewValidation.tsx
    - frontend/src/components/ColumnExplorer.tsx
  </files>
  <action>
    - Create a `ColumnExplorer` component triggered after data source selection.
    - Implement semantic type detection logic (`datetime`, `int`, `float`, `boolean`, `string (low-cardinality)`, `string (high-cardinality)`).
    - Display the Column Summary Table with sample values and null percentages.
  </action>
  <verify>Load a dataset and confirm the semantic types are correctly identified and displayed in the summary table.</verify>
  <done>Semantic types are detected and the summary table renders correctly.</done>
</task>

<task type="auto">
  <name>Build Type-Aware Dynamic Filters & Preview</name>
  <files>
    - frontend/src/components/DynamicFilters.tsx
    - frontend/src/pages/NewValidation.tsx
  </files>
  <action>
    - Build filter widgets for each semantic type (Range slider/inputs for numeric, presets for datetime, multi-select for low-cardinality string, search for high-cardinality).
    - Enable multi-column compound filtering (AND/OR).
    - Implement Step 3 Data Slice Preview updating row count and column-wise null summary based on active filters before submitting to validate.
  </action>
  <verify>Apply a combination of filters and verify the slice preview metrics update accordingly.</verify>
  <done>Filters correctly constrain the dataset and update the preview metrics.</done>
</task>

<task type="auto">
  <name>Pivot Table Builder & Transition</name>
  <files>
    - frontend/src/components/PivotBuilder.tsx
    - frontend/src/pages/NewValidation.tsx
  </files>
  <action>
    - Implement an optional Pivot Table Builder giving users choices for Rows, Columns, Values, Aggregation, and Sorting.
    - Render the pivot table cleanly.
    - Implement the transition logic ensuring the user confirms the slice and/or pivot before moving to Validation Mode Selection.
  </action>
  <verify>Build a pivot table using the UI and confirm the transition to validation mode works smoothly.</verify>
  <done>Pivot building and validation transition execute seamlessly.</done>
</task>

## Success Criteria
- [ ] Column Explorer accurately identifies semantic types.
- [ ] Dynamic Filters provide appropriate inputs per semantic type and accurately filter the subset.
- [ ] Data Slice Preview shows accurate 'before/after' row counts and subset previews.
- [ ] The optional Pivot Table can group and aggregate the sliced data.
- [ ] The full workflow correctly transitions from Slice/Pivot into the standard Validation Mode selection.
