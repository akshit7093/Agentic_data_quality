import { useParams } from 'react-router-dom';
import {
  CheckCircle,
  XCircle,
  AlertCircle,
  ArrowLeft,
  Download,
  RefreshCw,
  Brain,
  FileText,
  BarChart3,
} from 'lucide-react';
import { Link } from 'react-router-dom';
import { useValidationStatus, useValidationResults } from '@/hooks/useValidations';

// Mock data for demonstration
const mockValidationDetail = {
  id: '1',
  target_path: 'customers.csv',
  status: 'completed',
  validation_mode: 'hybrid',
  quality_score: 92,
  total_rules: 15,
  passed_rules: 14,
  failed_rules: 1,
  warning_rules: 0,
  records_processed: 10000,
  sample_size: 1000,
  started_at: '2024-02-11T10:00:00Z',
  completed_at: '2024-02-11T10:30:00Z',
  data_profile: {
    row_count: 10000,
    column_count: 8,
    columns: [
      { name: 'customer_id', type: 'integer', null_percentage: 0 },
      { name: 'email', type: 'string', null_percentage: 2.5 },
      { name: 'first_name', type: 'string', null_percentage: 1.2 },
      { name: 'last_name', type: 'string', null_percentage: 1.2 },
      { name: 'phone', type: 'string', null_percentage: 15.3 },
      { name: 'created_at', type: 'datetime', null_percentage: 0 },
      { name: 'status', type: 'string', null_percentage: 0 },
      { name: 'revenue', type: 'float', null_percentage: 5.1 },
    ],
  },
};

const mockResults = [
  {
    rule_id: '1',
    rule_name: 'Customer ID Not Null',
    rule_type: 'column',
    severity: 'critical',
    status: 'passed',
    passed_count: 1000,
    failed_count: 0,
    failure_percentage: 0,
    execution_time_ms: 45,
    ai_insights: null,
  },
  {
    rule_id: '2',
    rule_name: 'Email Format Valid',
    rule_type: 'pattern',
    severity: 'critical',
    status: 'passed',
    passed_count: 975,
    failed_count: 0,
    failure_percentage: 0,
    execution_time_ms: 120,
    ai_insights: null,
  },
  {
    rule_id: '3',
    rule_name: 'Phone Number Valid',
    rule_type: 'pattern',
    severity: 'warning',
    status: 'failed',
    passed_count: 820,
    failed_count: 27,
    failure_percentage: 3.2,
    execution_time_ms: 150,
    ai_insights: 'Many phone numbers are missing country codes or have invalid formats. Consider standardizing phone numbers during data ingestion.',
    failure_examples: [
      { phone: '123-456-7890', customer_id: 123 },
      { phone: '555-0123', customer_id: 456 },
      { phone: 'invalid', customer_id: 789 },
    ],
  },
  {
    rule_id: '4',
    rule_name: 'Revenue Positive',
    rule_type: 'column',
    severity: 'warning',
    status: 'passed',
    passed_count: 949,
    failed_count: 0,
    failure_percentage: 0,
    execution_time_ms: 80,
    ai_insights: null,
  },
  {
    rule_id: '5',
    rule_name: 'Created At Valid Date',
    rule_type: 'column',
    severity: 'info',
    status: 'passed',
    passed_count: 1000,
    failed_count: 0,
    failure_percentage: 0,
    execution_time_ms: 60,
    ai_insights: null,
  },
];

export default function ValidationDetail() {
  const { id } = useParams<{ id: string }>();
  // const { data: validation } = useValidationStatus(id || null);
  // const { data: results } = useValidationResults(id || null);

  const validation = mockValidationDetail;
  const results = mockResults;

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'passed':
        return <CheckCircle className="w-5 h-5 text-success-500" />;
      case 'failed':
        return <XCircle className="w-5 h-5 text-danger-500" />;
      case 'warning':
        return <AlertCircle className="w-5 h-5 text-warning-500" />;
      default:
        return <AlertCircle className="w-5 h-5 text-gray-400" />;
    }
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'passed':
        return <span className="badge-success">Passed</span>;
      case 'failed':
        return <span className="badge-danger">Failed</span>;
      case 'warning':
        return <span className="badge-warning">Warning</span>;
      default:
        return <span className="badge">{status}</span>;
    }
  };

  const getSeverityBadge = (severity: string) => {
    switch (severity) {
      case 'critical':
        return <span className="badge-danger">Critical</span>;
      case 'warning':
        return <span className="badge-warning">Warning</span>;
      case 'info':
        return <span className="badge-info">Info</span>;
      default:
        return <span className="badge">{severity}</span>;
    }
  };

  const getScoreColor = (score: number) => {
    if (score >= 90) return 'text-success-600';
    if (score >= 70) return 'text-warning-600';
    return 'text-danger-600';
  };

  const getScoreBgColor = (score: number) => {
    if (score >= 90) return 'bg-success-500';
    if (score >= 70) return 'bg-warning-500';
    return 'bg-danger-500';
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <Link
            to="/validations"
            className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <ArrowLeft className="w-5 h-5" />
          </Link>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Validation Details</h1>
            <p className="text-sm text-gray-500">{validation.target_path}</p>
          </div>
        </div>
        <div className="flex items-center space-x-3">
          <button className="btn-secondary">
            <RefreshCw className="w-4 h-4 mr-2" />
            Re-run
          </button>
          <button className="btn-secondary">
            <Download className="w-4 h-4 mr-2" />
            Export
          </button>
        </div>
      </div>

      {/* Quality Score Card */}
      <div className="card">
        <div className="card-body">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-lg font-semibold text-gray-900">Quality Score</h2>
              <p className="text-sm text-gray-500 mt-1">
                Based on {validation.total_rules} validation rules
              </p>
            </div>
            <div className="flex items-center space-x-6">
              <div className="text-center">
                <div
                  className={`text-5xl font-bold ${getScoreColor(
                    validation.quality_score
                  )}`}
                >
                  {validation.quality_score}%
                </div>
                <div className="text-sm text-gray-500 mt-1">Overall Score</div>
              </div>
              <div className="w-32 h-32 relative">
                <svg className="w-full h-full transform -rotate-90">
                  <circle
                    cx="64"
                    cy="64"
                    r="56"
                    stroke="currentColor"
                    strokeWidth="12"
                    fill="transparent"
                    className="text-gray-200"
                  />
                  <circle
                    cx="64"
                    cy="64"
                    r="56"
                    stroke="currentColor"
                    strokeWidth="12"
                    fill="transparent"
                    strokeDasharray={`${validation.quality_score * 3.52} 351.86`}
                    className={getScoreColor(validation.quality_score)}
                  />
                </svg>
              </div>
            </div>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-4 gap-4 mt-6 pt-6 border-t border-gray-200">
            <div className="text-center">
              <div className="text-2xl font-bold text-success-600">
                {validation.passed_rules}
              </div>
              <div className="text-sm text-gray-500">Passed</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-danger-600">
                {validation.failed_rules}
              </div>
              <div className="text-sm text-gray-500">Failed</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-warning-600">
                {validation.warning_rules}
              </div>
              <div className="text-sm text-gray-500">Warnings</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-gray-900">
                {validation.records_processed.toLocaleString()}
              </div>
              <div className="text-sm text-gray-500">Records</div>
            </div>
          </div>
        </div>
      </div>

      {/* Data Profile */}
      <div className="card">
        <div className="card-header">
          <h2 className="text-lg font-semibold text-gray-900">Data Profile</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Column
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Type
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Null %
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Quality
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {validation.data_profile.columns.map((col) => (
                <tr key={col.name} className="hover:bg-gray-50">
                  <td className="px-6 py-3 whitespace-nowrap text-sm font-medium text-gray-900">
                    {col.name}
                  </td>
                  <td className="px-6 py-3 whitespace-nowrap text-sm text-gray-500">
                    {col.type}
                  </td>
                  <td className="px-6 py-3 whitespace-nowrap text-sm text-gray-500">
                    {col.null_percentage}%
                  </td>
                  <td className="px-6 py-3 whitespace-nowrap">
                    <div className="flex items-center">
                      <div className="w-16 bg-gray-200 rounded-full h-2 mr-2">
                        <div
                          className={`h-2 rounded-full ${
                            col.null_percentage < 5
                              ? 'bg-success-500'
                              : col.null_percentage < 20
                              ? 'bg-warning-500'
                              : 'bg-danger-500'
                          }`}
                          style={{ width: `${100 - col.null_percentage}%` }}
                        />
                      </div>
                      <span className="text-xs text-gray-500">
                        {100 - col.null_percentage}%
                      </span>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Validation Results */}
      <div className="card">
        <div className="card-header">
          <h2 className="text-lg font-semibold text-gray-900">Validation Results</h2>
        </div>
        <div className="divide-y divide-gray-200">
          {results.map((result) => (
            <div key={result.rule_id} className="p-6 hover:bg-gray-50">
              <div className="flex items-start justify-between">
                <div className="flex items-start space-x-3">
                  {getStatusIcon(result.status)}
                  <div>
                    <h3 className="text-sm font-medium text-gray-900">
                      {result.rule_name}
                    </h3>
                    <div className="flex items-center space-x-2 mt-1">
                      {getSeverityBadge(result.severity)}
                      <span className="text-xs text-gray-500">
                        {result.rule_type}
                      </span>
                    </div>
                  </div>
                </div>
                <div className="text-right">
                  {getStatusBadge(result.status)}
                  <p className="text-xs text-gray-500 mt-1">
                    {result.passed_count.toLocaleString()} passed
                    {result.failed_count > 0 &&
                      `, ${result.failed_count.toLocaleString()} failed`}
                  </p>
                </div>
              </div>

              {result.ai_insights && (
                <div className="mt-3 p-3 bg-purple-50 rounded-lg border border-purple-200">
                  <div className="flex items-start space-x-2">
                    <Brain className="w-4 h-4 text-purple-500 mt-0.5" />
                    <div>
                      <p className="text-sm font-medium text-purple-900">
                        AI Insight
                      </p>
                      <p className="text-sm text-purple-700 mt-1">
                        {result.ai_insights}
                      </p>
                    </div>
                  </div>
                </div>
              )}

              {result.failure_examples && result.failure_examples.length > 0 && (
                <div className="mt-3">
                  <p className="text-xs font-medium text-gray-500 mb-2">
                    Failure Examples:
                  </p>
                  <div className="bg-gray-50 rounded-lg p-3 overflow-x-auto">
                    <pre className="text-xs text-gray-700">
                      {JSON.stringify(result.failure_examples, null, 2)}
                    </pre>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
