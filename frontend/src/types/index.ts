export interface DataSource {
  id: string;
  name: string;
  description?: string;
  source_type: string;
  connection_config: Record<string, any>;
  is_active: boolean;
  created_at: string;
}

export interface ValidationRule {
  id: string;
  name: string;
  description?: string;
  rule_type: 'column' | 'row' | 'table' | 'statistical' | 'pattern' | 'custom_sql';
  severity: 'info' | 'warning' | 'critical';
  target_columns: string[];
  config: Record<string, any>;
  expression?: string;
  is_ai_generated?: boolean;
  ai_confidence?: number;
  ai_rationale?: string;
}

export interface ValidationResult {
  rule_id: string;
  rule_name: string;
  status: 'passed' | 'failed' | 'warning' | 'error';
  passed_count: number;
  failed_count: number;
  failure_examples: Record<string, any>[];
  failure_percentage: number;
  execution_time_ms: number;
  ai_insights?: string;
  ai_suggestions?: string[];
}

export interface ValidationRun {
  id: string;
  data_source_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  current_step?: string;
  validation_mode: string;
  target_path: string;
  quality_score?: number;
  total_rules: number;
  passed_rules: number;
  failed_rules: number;
  warning_rules: number;
  records_processed: number;
  started_at?: string;
  completed_at?: string;
  error_message?: string;
  results?: ValidationResult[];
  result?: any; // Contains raw agent state including messages
  data_profile?: DataProfile;
  slice_filters?: Record<string, any>;
  chat_history?: any[];
}

export interface DataProfile {
  column_profiles: Record<string, ColumnProfile>;
  row_count: number;
  column_count: number;
  patterns_detected: Record<string, string[]>;
}

export interface ColumnProfile {
  type: string;
  null_count: number;
  null_percentage: number;
  unique_count: number;
  unique_percentage: number;
  min?: number;
  max?: number;
  mean?: number;
  median?: number;
  std?: number;
  min_length?: number;
  max_length?: number;
  avg_length?: number;
}

export interface LLMHealth {
  status: string;
  provider: string;
  model: string;
  response?: string;
  error?: string;
}

export interface DashboardStats {
  total_validations: number;
  total_data_sources: number;
  total_rules: number;
  average_quality_score: number;
  recent_validations: ValidationRun[];
}
