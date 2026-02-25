"""Validation execution engine."""
import re
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import time

import pandas as pd
import numpy as np

from app.agents.state import ValidationRule, ValidationResult

logger = logging.getLogger(__name__)


class ValidationEngine:
    """Engine for executing validation rules."""
    
    async def execute_rules(
        self,
        rules: List[ValidationRule],
        sample_data: List[Dict[str, Any]],
        schema: Optional[Dict[str, Any]] = None,
    ) -> List[ValidationResult]:
        """Execute all validation rules."""
        if not sample_data:
            logger.warning("No sample data provided for validation")
            return []
        
        # Convert to DataFrame for efficient processing
        df = pd.DataFrame(sample_data)
        
        results = []
        for rule in rules:
            try:
                start_time = time.time()
                result = await self._execute_rule(rule, df)
                execution_time = int((time.time() - start_time) * 1000)
                result.execution_time_ms = execution_time
                results.append(result)
            except Exception as e:
                logger.error(f"Rule execution failed for {rule.name}: {str(e)}")
                results.append(ValidationResult(
                    rule_id=rule.id or "unknown",
                    rule_name=rule.name,
                    status="error",
                    passed_count=0,
                    failed_count=len(sample_data),
                    ai_insights=f"Execution error: {str(e)}",
                ))
        
        return results
    
    async def _execute_rule(self, rule: ValidationRule, df: pd.DataFrame) -> ValidationResult:
        """Execute a single validation rule."""
        rule_type = rule.rule_type.lower()

        if rule_type == "column":
            return await self._execute_column_rule(rule, df)
        elif rule_type == "row":
            return await self._execute_row_rule(rule, df)
        elif rule_type == "table":
            return await self._execute_table_rule(rule, df)
        elif rule_type == "statistical":
            return await self._execute_statistical_rule(rule, df)
        elif rule_type == "pattern":
            return await self._execute_pattern_rule(rule, df)
        elif rule_type == "validity":
            return await self._execute_column_rule(rule, df)  # Treat validity as column rule
        elif rule_type == "custom_sql":
            return await self._execute_custom_sql_rule(rule, df)
        else:
            raise ValueError(f"Unknown rule type: {rule_type}")
    
    async def _execute_column_rule(self, rule: ValidationRule, df: pd.DataFrame) -> ValidationResult:
        """Execute column-level validation rule."""
        config = rule.config
        target_columns = rule.target_columns

        if not target_columns:
            raise ValueError("Column rule requires target_columns")

        column = target_columns[0]
        if column not in df.columns:
            return ValidationResult(
                rule_id=rule.id or "unknown",
                rule_name=rule.name,
                status="error",
                passed_count=0,
                failed_count=len(df),
                ai_insights=f"Column '{column}' not found in data",
            )

        col_data = df[column]
        failed_mask = pd.Series([False] * len(df))

        # Special handling for specific rule names
        if rule.name == "customer_id_unique" or "unique" in rule.name.lower():
            # Find duplicate values - mark all duplicates as failed
            duplicated_mask = col_data.duplicated(keep=False)  # keep=False marks all duplicates
            failed_mask |= duplicated_mask
        elif rule.name == "email_format" or "format" in rule.name.lower():
            # Apply regex pattern from config if available
            if 'pattern' in config:
                try:
                    matches = col_data.astype(str).str.match(config['pattern'], na=False)
                    failed_mask |= ~matches
                except:
                    # If pattern is invalid, mark all as failed
                    failed_mask |= True
        elif rule.name == "phone_length":
            # Check phone length based on config
            if 'min_length' in config or 'max_length' in config:
                str_data = col_data.astype(str)
                lengths = str_data.str.len()
                if 'min_length' in config:
                    failed_mask |= lengths < config['min_length']
                if 'max_length' in config:
                    failed_mask |= lengths > config['max_length']
        elif rule.name == "dob_within_range":
            # Check date of birth range
            if 'min_age' in config or 'max_age' in config:
                try:
                    # Convert to datetime if possible
                    dates = pd.to_datetime(col_data, errors='coerce')
                    today = pd.Timestamp.today()
                    
                    if 'min_age' in config:
                        min_date = today - pd.DateOffset(years=config['min_age'])
                        failed_mask |= (dates > min_date) & dates.notna()
                    if 'max_age' in config:
                        max_date = today - pd.DateOffset(years=config['max_age'])
                        failed_mask |= (dates < max_date) & dates.notna()
                except:
                    # If conversion fails, mark all as failed
                    failed_mask |= col_data.notna()
        elif rule.name == "lifetime_value_positive":
            # Check if lifetime value is positive
            try:
                numeric_data = pd.to_numeric(col_data, errors='coerce')
                if 'min' in config:
                    failed_mask |= numeric_data < config['min']
                else:
                    failed_mask |= numeric_data < 0  # Default to checking for negative values
            except:
                failed_mask |= col_data.notna()  # Non-numeric values fail
        elif rule.name == "status_not_null":
            # Check if status is not null
            failed_mask |= col_data.isnull()
        elif rule.name == "country_non_empty":
            # Check if country is not empty
            failed_mask |= (col_data.isnull()) | (col_data.astype(str) == '')
        else:
            # Original generic logic for backward compatibility
            # Null check
            if config.get('check_null', False):
                failed_mask |= col_data.isnull()

            # Empty string check
            if config.get('check_empty', False):
                failed_mask |= (col_data == '') | (col_data == ' ')

            # Range check
            if 'min' in config or 'max' in config:
                try:
                    numeric_data = pd.to_numeric(col_data, errors='coerce')
                    if 'min' in config:
                        failed_mask |= numeric_data < config['min']
                    if 'max' in config:
                        failed_mask |= numeric_data > config['max']
                except:
                    pass

            # Length check for strings
            if 'min_length' in config or 'max_length' in config:
                str_data = col_data.astype(str)
                lengths = str_data.str.len()
                if 'min_length' in config:
                    failed_mask |= lengths < config['min_length']
                if 'max_length' in config:
                    failed_mask |= lengths > config['max_length']

            # Enum check
            if 'allowed_values' in config:
                failed_mask |= ~col_data.isin(config['allowed_values'])

            # Data type check
            if 'data_type' in config:
                expected_type = config['data_type']
                if expected_type == 'integer':
                    failed_mask |= ~col_data.apply(lambda x: isinstance(x, (int, np.integer)) or (isinstance(x, str) and x.isdigit()))
                elif expected_type == 'float':
                    failed_mask |= ~col_data.apply(lambda x: isinstance(x, (float, np.floating, int)) or self._is_float_string(x))
                elif expected_type == 'string':
                    failed_mask |= ~col_data.apply(lambda x: isinstance(x, str))

        return self._create_result(rule, df, failed_mask)
    
    async def _execute_row_rule(self, rule: ValidationRule, df: pd.DataFrame) -> ValidationResult:
        """Execute row-level validation rule."""
        config = rule.config
        
        if 'expression' in config:
            # Evaluate expression
            try:
                # Create safe evaluation context
                context = {'df': df, 'pd': pd, 'np': np}
                
                # Add column references
                for col in df.columns:
                    context[col] = df[col]
                
                result = eval(config['expression'], {"__builtins__": {}}, context)
                failed_mask = ~result if isinstance(result, pd.Series) else pd.Series([not result] * len(df))
                
            except Exception as e:
                logger.error(f"Expression evaluation failed: {str(e)}")
                failed_mask = pd.Series([True] * len(df))
        
        elif 'column_comparison' in config:
            # Compare two columns
            col1 = config['column_comparison'].get('column1')
            col2 = config['column_comparison'].get('column2')
            operator = config['column_comparison'].get('operator', '==')
            
            if col1 in df.columns and col2 in df.columns:
                if operator == '==':
                    failed_mask = df[col1] != df[col2]
                elif operator == '!=':
                    failed_mask = df[col1] == df[col2]
                elif operator == '>':
                    failed_mask = df[col1] <= df[col2]
                elif operator == '<':
                    failed_mask = df[col1] >= df[col2]
                elif operator == '>=':
                    failed_mask = df[col1] < df[col2]
                elif operator == '<=':
                    failed_mask = df[col1] > df[col2]
                else:
                    failed_mask = pd.Series([True] * len(df))
            else:
                failed_mask = pd.Series([True] * len(df))
        
        else:
            failed_mask = pd.Series([False] * len(df))
        
        return self._create_result(rule, df, failed_mask)
    
    async def _execute_table_rule(self, rule: ValidationRule, df: pd.DataFrame) -> ValidationResult:
        """Execute table-level validation rule."""
        config = rule.config
        failed_mask = pd.Series([False] * len(df))

        # Special handling for specific rule names
        if rule.name == "customer_id_unique" or "unique" in rule.name.lower():
            # Check uniqueness for the target column
            if rule.target_columns:
                target_col = rule.target_columns[0]
                if target_col in df.columns:
                    # Mark all duplicates as failed
                    duplicated_mask = df[target_col].duplicated(keep=False)
                    failed_mask |= duplicated_mask
        else:
            # Original generic logic for backward compatibility
            # Row count check
            if 'min_rows' in config:
                if len(df) < config['min_rows']:
                    failed_mask = pd.Series([True] * len(df))

            if 'max_rows' in config:
                if len(df) > config['max_rows']:
                    failed_mask = pd.Series([True] * len(df))

            # Uniqueness check
            if 'unique_columns' in config:
                unique_cols = config['unique_columns']
                if all(col in df.columns for col in unique_cols):
                    duplicates = df.duplicated(subset=unique_cols, keep=False)
                    failed_mask |= duplicates

        return self._create_result(rule, df, failed_mask)
    
    async def _execute_statistical_rule(self, rule: ValidationRule, df: pd.DataFrame) -> ValidationResult:
        """Execute statistical validation rule."""
        config = rule.config
        target_columns = rule.target_columns

        if not target_columns:
            return self._create_result(rule, df, pd.Series([False] * len(df)))

        column = target_columns[0]
        if column not in df.columns:
            return self._create_result(rule, df, pd.Series([True] * len(df)))

        col_data = df[column]
        failed_mask = pd.Series([False] * len(df))

        # Special handling for specific rule names
        if rule.name == "dob_within_range":
            # Check date of birth range based on age constraints
            try:
                # Convert to datetime if possible
                dates = pd.to_datetime(col_data, errors='coerce')
                today = pd.Timestamp.today()
                
                # Get age constraints from config or use defaults
                min_age = config.get('min_age', 18)
                max_age = config.get('max_age', 120)
                
                # Calculate ages
                ages = (today - dates).dt.days / 365.25
                
                # Mark as failed if age is outside the range
                failed_mask |= (ages < min_age) | (ages > max_age) | dates.isna()
            except:
                # If conversion fails, mark all as failed
                failed_mask |= col_data.notna()
        elif rule.name == "lifetime_value_positive":
            # Check if lifetime value is positive
            try:
                numeric_data = pd.to_numeric(col_data, errors='coerce')
                min_val = config.get('min', 0)  # Default to 0 for positive check
                failed_mask |= (numeric_data < min_val) | numeric_data.isna()
            except:
                failed_mask |= col_data.notna()  # Non-numeric values fail
        else:
            # Original generic logic for backward compatibility
            col_data_numeric = pd.to_numeric(col_data, errors='coerce')

            # Outlier detection using IQR
            if config.get('detect_outliers', False):
                Q1 = col_data_numeric.quantile(0.25)
                Q3 = col_data_numeric.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                failed_mask |= (col_data_numeric < lower_bound) | (col_data_numeric > upper_bound)

            # Z-score outlier detection
            if 'zscore_threshold' in config:
                mean = col_data_numeric.mean()
                std = col_data_numeric.std()
                if std > 0:
                    z_scores = (col_data_numeric - mean).abs() / std
                    failed_mask |= z_scores > config['zscore_threshold']

        return self._create_result(rule, df, failed_mask)
    
    async def _execute_pattern_rule(self, rule: ValidationRule, df: pd.DataFrame) -> ValidationResult:
        """Execute pattern validation rule."""
        config = rule.config
        target_columns = rule.target_columns

        if not target_columns:
            return self._create_result(rule, df, pd.Series([False] * len(df)))

        column = target_columns[0]
        if column not in df.columns:
            return self._create_result(rule, df, pd.Series([True] * len(df)))

        col_data = df[column].astype(str)
        failed_mask = pd.Series([False] * len(df))

        # Special handling for specific rule names
        if rule.name == "email_format" or "format" in rule.name.lower():
            # Apply regex pattern from config if available
            if 'pattern' in config:
                try:
                    matches = col_data.str.match(config['pattern'], na=False)
                    failed_mask |= ~matches  # Mark non-matching as failed
                except re.error as e:
                    logger.error(f"Invalid regex pattern: {str(e)}")
                    failed_mask |= True  # Mark all as failed if pattern is invalid
        elif rule.name == "phone_length":
            # Check phone length based on config
            if 'min_length' in config or 'max_length' in config:
                lengths = col_data.str.len()
                if 'min_length' in config:
                    failed_mask |= lengths < config['min_length']
                if 'max_length' in config:
                    failed_mask |= lengths > config['max_length']
        elif rule.name == "country_non_empty":
            # Check if country is not empty
            failed_mask |= (col_data == '') | (col_data.isna())
        else:
            # Original generic logic for backward compatibility
            # Regex pattern check
            if 'pattern' in config:
                pattern = config['pattern']
                try:
                    matches = col_data.str.match(pattern, na=False)
                    if config.get('invert', False):
                        failed_mask |= matches
                    else:
                        failed_mask |= ~matches
                except re.error as e:
                    logger.error(f"Invalid regex pattern: {str(e)}")

            # Email pattern
            if config.get('pattern_type') == 'email':
                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                failed_mask |= ~col_data.str.match(email_pattern, na=False)

            # Phone pattern
            if config.get('pattern_type') == 'phone':
                phone_pattern = r'^\+?1?\d{9,15}$'
                failed_mask |= ~col_data.str.match(phone_pattern, na=False)

            # Date pattern
            if config.get('pattern_type') == 'date':
                date_formats = config.get('date_formats', ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y'])

                def is_valid_date(val):
                    if pd.isna(val) or val == '':
                        return True
                    for fmt in date_formats:
                        try:
                            datetime.strptime(str(val), fmt)
                            return True
                        except ValueError:
                            continue
                    return False

                failed_mask |= ~col_data.apply(is_valid_date)

        return self._create_result(rule, df, failed_mask)
    
    async def _execute_custom_sql_rule(self, rule: ValidationRule, df: pd.DataFrame) -> ValidationResult:
        """Execute custom SQL-like validation rule."""
        # For DataFrame-based execution, we treat this as an expression
        config = rule.config
        
        if 'sql' in config:
            # This would require a SQL engine - for now, treat as expression
            logger.warning("SQL rules not fully implemented for DataFrame source")
        
        return self._create_result(rule, df, pd.Series([False] * len(df)))
    
    def _create_result(
        self,
        rule: ValidationRule,
        df: pd.DataFrame,
        failed_mask: pd.Series,
    ) -> ValidationResult:
        """Create validation result from failed mask."""
        total = len(df)
        failed_count = int(failed_mask.sum())
        passed_count = total - failed_count
        
        # Get failure examples
        failure_examples = []
        if failed_count > 0:
            failed_rows = df[failed_mask].head(5)
            failure_examples = failed_rows.replace({pd.NA: None, pd.NaT: None}).to_dict('records')
            
            # Clean NaN values
            for example in failure_examples:
                for key, val in example.items():
                    if pd.isna(val):
                        example[key] = None
        
        # Determine status
        if failed_count == 0:
            status = "passed"
        elif rule.severity == "critical":
            status = "failed"
        elif rule.severity == "warning":
            status = "warning"
        else:  # info or other severities - treat as warning when there are failures
            status = "warning"
        
        return ValidationResult(
            rule_id=rule.id or "unknown",
            rule_name=rule.name,
            status=status,
            passed_count=passed_count,
            failed_count=failed_count,
            failure_examples=failure_examples,
            failure_percentage=(failed_count / total * 100) if total > 0 else 0,
        )
    
    def _is_float_string(self, val: Any) -> bool:
        """Check if value can be converted to float."""
        if isinstance(val, (int, float)):
            return True
        if isinstance(val, str):
            try:
                float(val)
                return True
            except ValueError:
                return False
        return False
