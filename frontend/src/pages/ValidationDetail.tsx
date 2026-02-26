import { useParams, Link } from 'react-router-dom';
import {
  CheckCircle,
  XCircle,
  AlertCircle,
  ArrowLeft,
  RefreshCw,
  Brain,
  FileText,
  BarChart3,
  Activity,
  Database,
} from 'lucide-react';
import { useValidationStatus, useValidationResults } from '@/hooks/useValidations';

export default function ValidationDetail() {
  const { id } = useParams<{ id: string }>();
  const { data: validation, isLoading: statusLoading, error: statusError } = useValidationStatus(id || null);

  const isCompleted = validation?.status === 'completed';
  const { data: resultsData, isLoading: resultsLoading } = useValidationResults(id || null, isCompleted);

  const results = resultsData?.results || [];
  const dataProfile = validation?.data_profile;

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


  // Loading state
  if (statusLoading) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh]">
        <RefreshCw className="w-12 h-12 text-primary-500 animate-spin mb-4" />
        <h2 className="text-xl font-semibold text-gray-900">Loading Validation</h2>
        <p className="text-gray-500 mt-1">Fetching validation details...</p>
      </div>
    );
  }

  // Error state
  if (statusError) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh]">
        <XCircle className="w-12 h-12 text-danger-500 mb-4" />
        <h2 className="text-xl font-semibold text-gray-900">Validation Not Found</h2>
        <p className="text-gray-500 mt-1">This validation may have expired or doesn't exist.</p>
        <Link to="/validations" className="btn-primary mt-4">
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Validations
        </Link>
      </div>
    );
  }

  // Running / Pending state
  if (validation?.status === 'pending' || validation?.status === 'running') {
    return (
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center space-x-4">
          <Link
            to="/validations"
            className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <ArrowLeft className="w-5 h-5" />
          </Link>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Validation In Progress</h1>
            <p className="text-sm text-gray-500">{validation?.target_path || id}</p>
          </div>
        </div>

        {/* Progress Card */}
        <div className="card">
          <div className="card-body">
            <div className="flex flex-col items-center justify-center py-12">
              <div className="relative">
                <Activity className="w-16 h-16 text-primary-500 animate-pulse" />
              </div>
              <h2 className="text-xl font-semibold text-gray-900 mt-6">
                {validation?.status === 'pending' ? 'Starting Validation...' : 'Validation Running'}
              </h2>
              <p className="text-gray-500 mt-2">
                Current step: <span className="font-medium text-primary-600">{validation?.current_step || 'initializing'}</span>
              </p>

              {/* Progress Info */}
              <div className="grid grid-cols-3 gap-8 mt-8 text-center">
                <div>
                  <div className="text-2xl font-bold text-success-600">{validation?.passed_rules || 0}</div>
                  <div className="text-sm text-gray-500">Passed</div>
                </div>
                <div>
                  <div className="text-2xl font-bold text-danger-600">{validation?.failed_rules || 0}</div>
                  <div className="text-sm text-gray-500">Failed</div>
                </div>
                <div>
                  <div className="text-2xl font-bold text-gray-900">{validation?.total_rules || 0}</div>
                  <div className="text-sm text-gray-500">Total Rules</div>
                </div>
              </div>

              {/* Animated Progress Bar */}
              <div className="w-full max-w-md mt-8">
                <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
                  <div className="h-full bg-primary-500 rounded-full animate-pulse" style={{ width: '60%' }} />
                </div>
                <p className="text-xs text-gray-400 mt-2 text-center">
                  Auto-refreshing every 2 seconds...
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // Failed state
  if (validation?.status === 'failed') {
    return (
      <div className="space-y-6">
        <div className="flex items-center space-x-4">
          <Link
            to="/validations"
            className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <ArrowLeft className="w-5 h-5" />
          </Link>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Validation Failed</h1>
            <p className="text-sm text-gray-500">{validation?.target_path || id}</p>
          </div>
        </div>
        <div className="card">
          <div className="card-body">
            <div className="flex flex-col items-center py-8">
              <XCircle className="w-16 h-16 text-danger-500 mb-4" />
              <h2 className="text-xl font-semibold text-gray-900">Validation Failed</h2>
              <p className="text-gray-500 mt-2">{validation?.error_message || 'An unknown error occurred'}</p>
              <Link to="/validations/new" className="btn-primary mt-6">
                Try Again
              </Link>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // Completed state — show full results
  const qualityScore = validation?.quality_score ?? 0;
  const totalRules = validation?.total_rules ?? 0;
  const passedRules = validation?.passed_rules ?? 0;
  const failedRules = validation?.failed_rules ?? 0;

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
            <p className="text-sm text-gray-500">{validation?.target_path || id}</p>
          </div>
        </div>
        <div className="flex items-center space-x-3">
          <Link to="/validations/new" className="btn-secondary">
            <RefreshCw className="w-4 h-4 mr-2" />
            Re-run
          </Link>
        </div>
      </div>

      {/* Quality Score Card */}
      <div className="card">
        <div className="card-body">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-lg font-semibold text-gray-900">Quality Score</h2>
              <p className="text-sm text-gray-500 mt-1">
                Based on {totalRules} validation rules
              </p>
            </div>
            <div className="flex items-center space-x-6">
              <div className="text-center">
                <div
                  className={`text-5xl font-bold ${getScoreColor(qualityScore)}`}
                >
                  {qualityScore}%
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
                    strokeDasharray={`${qualityScore * 3.52} 351.86`}
                    className={getScoreColor(qualityScore)}
                  />
                </svg>
              </div>
            </div>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-4 gap-4 mt-6 pt-6 border-t border-gray-200">
            <div className="text-center">
              <div className="text-2xl font-bold text-success-600">
                {passedRules}
              </div>
              <div className="text-sm text-gray-500">Passed</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-danger-600">
                {failedRules}
              </div>
              <div className="text-sm text-gray-500">Failed</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-warning-600">
                {totalRules - passedRules - failedRules}
              </div>
              <div className="text-sm text-gray-500">Warnings</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-gray-900">
                {totalRules}
              </div>
              <div className="text-sm text-gray-500">Total Rules</div>
            </div>
          </div>
        </div>
      </div>

      {/* Metadata Card */}
      <div className="card">
        <div className="card-body">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Validation Info</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <p className="text-xs text-gray-500 uppercase">Target</p>
              <p className="text-sm font-medium text-gray-900">{validation?.target_path}</p>
            </div>
            <div>
              <p className="text-xs text-gray-500 uppercase">Mode</p>
              <p className="text-sm font-medium text-gray-900 capitalize">{validation?.validation_mode?.replace('_', ' ')}</p>
            </div>
            <div>
              <p className="text-xs text-gray-500 uppercase">Started</p>
              <p className="text-sm font-medium text-gray-900">
                {validation?.started_at ? new Date(validation.started_at).toLocaleString() : '—'}
              </p>
            </div>
            <div>
              <p className="text-xs text-gray-500 uppercase">Duration</p>
              <p className="text-sm font-medium text-gray-900">
                {validation?.started_at && validation?.completed_at
                  ? `${Math.round((new Date(validation.completed_at).getTime() - new Date(validation.started_at).getTime()) / 1000)}s`
                  : '—'}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Data Profile Section */}
      {dataProfile && (
        <div className="card">
          <div className="card-header border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center">
              <Database className="w-5 h-5 mr-2 text-primary-500" />
              Data Profile
            </h2>
          </div>
          <div className="card-body">
            <div className="grid grid-cols-2 gap-4 mb-6">
              <div className="bg-gray-50 rounded-lg p-4 text-center">
                <p className="text-3xl font-bold text-gray-900">{dataProfile.row_count?.toLocaleString() || 0}</p>
                <p className="text-sm text-gray-500 mt-1">Total Rows</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-4 text-center">
                <p className="text-3xl font-bold text-gray-900">{dataProfile.column_count || 0}</p>
                <p className="text-sm text-gray-500 mt-1">Total Columns</p>
              </div>
            </div>

            {dataProfile.column_profiles && Object.keys(dataProfile.column_profiles).length > 0 && (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Column</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Missing</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Unique</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Min/Max</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {Object.entries(dataProfile.column_profiles).map(([colName, profile]: [string, any]) => (
                      <tr key={colName} className="hover:bg-gray-50">
                        <td className="px-4 py-3 whitespace-nowrap text-sm font-medium text-gray-900">
                          {colName}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-800">
                            {profile.type}
                          </span>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-right">
                          <span className={profile.null_count > 0 ? 'text-warning-600 font-medium' : 'text-success-600'}>
                            {profile.null_percentage?.toFixed(1)}%
                          </span>
                          <span className="text-gray-400 text-xs ml-1">({profile.null_count})</span>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900">
                          {profile.unique_percentage?.toFixed(1)}% <span className="text-gray-400 text-xs">({profile.unique_count})</span>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-500">
                          {profile.min !== undefined && profile.max !== undefined ? (
                            <span>
                              {typeof profile.min === 'number' ? profile.min.toFixed(2) : profile.min} /{' '}
                              {typeof profile.max === 'number' ? profile.max.toFixed(2) : profile.max}
                            </span>
                          ) : (
                            '—'
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Validation Results */}
      <div className="card">
        <div className="card-header border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">
            Validation Results
            {resultsLoading && (
              <RefreshCw className="w-4 h-4 ml-2 inline animate-spin text-gray-400" />
            )}
          </h2>
        </div>

        {results.length === 0 ? (
          <div className="p-8 text-center text-gray-500">
            <BarChart3 className="w-10 h-10 mx-auto mb-3 text-gray-300" />
            <p>No detailed results available yet.</p>
          </div>
        ) : (
          <div className="divide-y divide-gray-200">
            {results.map((result: any, idx: number) => (
              <div key={result.rule_id || idx} className="p-6 hover:bg-gray-50">
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
                      {result.passed_count != null && `${result.passed_count.toLocaleString()} passed`}
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
        )}
      </div>
    </div>
  );
}
