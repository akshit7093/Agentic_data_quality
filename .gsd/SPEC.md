# SPEC.md — Project Specification

> **Status**: `FINALIZED`

## Vision
To provide an Enterprise-Grade AI-Powered Data Quality Assurance Platform that connects to diverse multi-source data systems, utilizes LangGraph LLM agents for intelligent and deterministic data validation, and prevents hallucinations via RAG contextualization.

## Goals
1. Provide a unified system to seamlessly connect varied data sources (ADLS Gen2, Databricks, PostgreSQL, S3, local files) for data quality assessment.
2. Combine traditional deterministic validation rules (Great Expectations/Pandera) with AI-powered data profiling, rule recommendations, and anomaly detection.
3. Present actionable data quality insights, metrics, and detailed validation reports through an intuitive React dashboard.

## Non-Goals (Out of Scope)
- Real-time data streaming validation (planned for future iteration).
- Full data lineage tracking and transformation mapping.
- Collaborative natural language query interface for non-technical users.

## Users
- **Data Engineers:** To set up data source connections, configure complex validation pipelines, and debug underlying data quality issues.
- **Data Stewards & Analysts:** To review validation results, accept/reject AI-generated rules, and monitor overarching data quality trends.

## Constraints
- **Privacy & Security:** Must support local LLMs (e.g., Ollama, LM Studio) to ensure sensitive data does not leave the enterprise perimeter if required, alongside cloud providers (OpenAI, Anthropic).
- **AI Reliability:** Must strictly enforce RAG strategies via ChromaDB to provide precise context and strictly mitigate LLM hallucinations during validation mapping.
- **Performance:** Complex data profiling and validation across large datasets must be handled asynchronously to prevent API timeouts or UI blocking.

## Success Criteria
- [ ] System can successfully connect, ingest schemas, and profile both structured and semi-structured data sources.
- [ ] The LangGraph workflow successfully completes the `Connect -> Profile -> Validate -> Report` cycle without breaking.
- [ ] The UI accurately displays validation status, scores, and specific row/column level discrepancies.

## 📊 Feature: DATA EXPLORATION, SLICING & PIVOT
*This section is ALWAYS triggered immediately after the user selects or uploads a dataset — before any validation mode, analysis type, or further configuration.*

### STEP 1 — COLUMN EXPLORER (Auto-run on data load)
Upon receiving the dataset, immediately scan and display a Column Summary Table. Detect and label each column's semantic type, not just raw dtype: `datetime`, `int`, `float`, `boolean`, `string (low-cardinality)`, `string (high-cardinality)`, `categorical (ordered)`.

### STEP 2 — DYNAMIC FILTERS (Per Column, Type-Aware)
Present a Filters Panel with one filter widget per column, auto-selected based on the detected semantic type:
- **DATETIME**: Preset Range Dropdown or Manual Range Entry (FROM -> TO).
- **INT / FLOAT**: Range Slider (min↔max) or Manual Entry. Checkbox Multi-Select for low uniqueness.
- **BOOLEAN**: Toggle / Radio.
- **STRING (Low-Cardinality)**: Multi-Select Checkbox List with unique counts.
- **STRING (High-Cardinality)**: Search / Contains text input box with match mode toggles.
Allow MULTI-COLUMN COMPOUND FILTER combining filters using AND / OR logic.

### STEP 3 — DATA SLICE PREVIEW
After filters are applied, show row count before vs. after filtering, preview of the first 10 rows, and column-wise null summary.

### STEP 4 — PIVOT TABLE BUILDER (Optional but offered every time)
Offer a Pivot Builder (Rows, Columns, Values, Aggregation, Sort By, Top N Filter). Display the result as a clean formatted pivot table.

### TRANSITION
Only after the user confirms that the dataset slice is acceptable and the Pivot is reviewed (if requested), proceed to Validation Mode Selection.
