# Phase 3 Research: Slicing, Agents, & Ticketing Fixes

## Requirements Breakdown

### 1. Pre-Validation Data Slicing (Pivot-like Filtering)
**Goal:** Allow users to filter/slice the dataset *before* validation begins. 
**Details:**
- When a user selects a dataset, they should see a UI providing column distribution.
- They can select a specific "slice" (e.g., `payment_status = 'refunded'`).
- Sliced queries must be passed to the backend, so the validation engine only evaluates the filtered subset (e.g., appending a `WHERE` clause dynamically, or reading a filtered DataFrame in memory).

### 2. Dual Validation Agent Choices
**Goal:** Split monolithic AI validation into distinct logical roles.
**Details:**
- **Choice A: Schema & Structural Validation Agent:** Focuses strictly on data types, nulls, constraints, and statistical distributions. Does not invent business rules.
- **Choice B: Data Analyst & Business Analyst Agent:** Runs deep semantic searches, anomaly detection, multidimensional analysis, and insight generation.

### 3. Custom Rule Enhancements (Schema Discovery)
**Goal:** Streamline the custom rule creation UI.
**Details:**
- When selecting "Custom Rules", the frontend should automatically run a script (API call) to fetch the target table's columns and data types.
- The UI should expose these as a drop-down or interactive list so users can easily select a column and apply quick checks (e.g., `price > 40`).

### 4. Ticketing Endpoint Fix (`404 Not Found` / Rule Not Found)
**Goal:** Fix the `/api/v1/validate/{id}/ticket` endpoint.
**Details:**
- The current logs show `404 Not Found` or `Rule HighFraudRiskCheck not found`.
- **Root Cause Hypothesis:** The UI is submitting `HighFraudRiskCheck`, but the backend expects the dictionary key or `rule_id` inside the ValidationResult object. We must ensure the API route correctly matches the `rule_name` or `rule_id` from the saved results list. Also need to quickly ensure the route is correctly mounted without trailing slash issues.
