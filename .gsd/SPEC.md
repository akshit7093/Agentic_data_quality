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
