import { useState, useMemo } from 'react';
import { useParams, Link } from 'react-router-dom';
import {
  CheckCircle,
  XCircle,
  AlertCircle,
  ArrowLeft,
  RefreshCw,
  Brain,
  BarChart3,
  Activity,
  Database,
  Terminal,
  Filter,
  Bot,
  Ticket,
  Loader2
} from 'lucide-react';
import { useValidationStatus, useValidationResults } from '@/hooks/useValidations';
import ExecutionTraceViewer from '@/components/ExecutionTraceViewer';

export default function ValidationDetail() {
  const { id } = useParams<{ id: string }>();
  const [activeTab, setActiveTab] = useState<'results' | 'logs'>('results');

  // Filters
  const [severityFilter, setSeverityFilter] = useState<string>('all');
  const [fixabilityFilter, setFixabilityFilter] = useState<string>('all');

  // Quick Fix State
  const [selectedFixes, setSelectedFixes] = useState<Set<string>>(new Set());
  const [fixInstructions, setFixInstructions] = useState<string>('');
  const [isFixing, setIsFixing] = useState(false);
  const [fixSuccess, setFixSuccess] = useState<any>(null);

  // Ticketing State
  const [isTicketing, setIsTicketing] = useState<string | null>(null);
  const [generatedTicket, setGeneratedTicket] = useState<string | null>(null);

  const { data: validation, isLoading: statusLoading, error: statusError } = useValidationStatus(id || null);

  const isCompleted = validation?.status === 'completed';
  const { data: resultsData, isLoading: resultsLoading } = useValidationResults(id || null, isCompleted);

  const results = resultsData?.results || [];
  const dataProfile = validation?.data_profile;

  const isRuleFixable = (rule: any) => {
    const type = rule.rule_type?.toLowerCase() || '';
    return ['not_null', 'regex', 'range', 'accepted_values', 'valid_values'].includes(type) || rule.fix_recommendations?.length > 0;
  };

  const filteredResults = useMemo(() => {
    return results.filter((r: any) => {
      const matchSev = severityFilter === 'all' || r.severity === severityFilter;
      const fixable = isRuleFixable(r);
      const matchFix = fixabilityFilter === 'all' ||
        (fixabilityFilter === 'fixable' && fixable) ||
        (fixabilityFilter === 'manual' && !fixable);
      return matchSev && matchFix;
    });
  }, [results, severityFilter, fixabilityFilter]);

  const handleAutoFix = async () => {
    if (selectedFixes.size === 0) return;
    setIsFixing(true);
    setFixSuccess(null);
    try {
      const instructions = Array.from(selectedFixes).map(ruleId => ({
        rule_id: ruleId,
        instruction: fixInstructions || "Fix automatically using best practices."
      }));

      const res = await fetch(`/api/v1/validate/${id}/fix`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fix_instructions: instructions, use_agent: true })
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setFixSuccess(data);
      setSelectedFixes(new Set());
    } catch (e) {
      console.error(e);
      alert("Failed to apply fixes");
    } finally {
      setIsFixing(false);
    }
  };

  const handleCreateTicket = async (ruleName: string) => {
    setIsTicketing(ruleName);
    try {
      const res = await fetch(`/api/v1/validate/${id}/ticket`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rule_name: ruleName })
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setGeneratedTicket(data.ticket_markdown);
    } catch (e) {
      console.error(e);
      alert("Failed to generate ticket");
    } finally {
      setIsTicketing(null);
    }
  };

  const toggleSelection = (ruleId: string) => {
    const next = new Set(selectedFixes);
    if (next.has(ruleId)) next.delete(ruleId);
    else next.add(ruleId);
    setSelectedFixes(next);
  };

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
            {validation?.slice_filters && Object.keys(validation.slice_filters).length > 0 && (
              <div className="col-span-2 md:col-span-4 mt-2">
                <p className="text-xs text-gray-500 uppercase mb-1">Active Slice Filters</p>
                <div className="flex flex-wrap gap-2">
                  {Object.entries(validation.slice_filters).map(([col, val]) => (
                    <span key={col} className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800">
                      {col} = {String(val)}
                    </span>
                  ))}
                </div>
              </div>
            )}
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

      {/* Tabs */}
      <div className="flex border-b border-gray-200 mt-8 mb-6">
        <button
          onClick={() => setActiveTab('results')}
          className={`pb-4 px-4 text-sm font-medium border-b-2 transition-colors flex items-center ${activeTab === 'results'
            ? 'border-primary-500 text-primary-600'
            : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
        >
          <BarChart3 className="w-4 h-4 mr-2" />
          Validation Results
        </button>
        <button
          onClick={() => setActiveTab('logs')}
          className={`pb-4 px-4 text-sm font-medium border-b-2 transition-colors flex items-center ${activeTab === 'logs'
            ? 'border-primary-500 text-primary-600'
            : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
        >
          <Terminal className="w-4 h-4 mr-2" />
          Agent Execution Log
        </button>
      </div>

      {/* Validation Results Tab */}
      {activeTab === 'results' && (
        <div className="card">
          <div className="card-header border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">
              Validation Results
              {resultsLoading && (
                <RefreshCw className="w-4 h-4 ml-2 inline animate-spin text-gray-400" />
              )}
            </h2>
          </div>

          {/* Filters Bar */}
          <div className="px-6 py-4 bg-gray-50 border-b border-gray-200 flex flex-wrap gap-4 items-center justify-between">
            <div className="flex items-center space-x-4">
              <div className="flex items-center text-sm font-medium text-gray-700">
                <Filter className="w-4 h-4 mr-2 text-gray-400" />
                Filters:
              </div>
              <select
                value={severityFilter}
                onChange={e => setSeverityFilter(e.target.value)}
                className="text-sm border-gray-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500"
              >
                <option value="all">All Severities</option>
                <option value="critical">Critical</option>
                <option value="warning">Warning</option>
                <option value="info">Info</option>
              </select>
              <select
                value={fixabilityFilter}
                onChange={e => setFixabilityFilter(e.target.value)}
                className="text-sm border-gray-300 rounded-md shadow-sm focus:border-primary-500 focus:ring-primary-500"
              >
                <option value="all">All Types</option>
                <option value="fixable">AI Auto-Fixable</option>
                <option value="manual">Manual Action Required</option>
              </select>
            </div>

            {selectedFixes.size > 0 && (
              <div className="flex items-center space-x-3 bg-blue-50 px-4 py-2 rounded-lg border border-blue-100 shadow-sm">
                <input
                  type="text"
                  placeholder="Optional context for AI fix..."
                  className="text-sm border-gray-300 rounded-md shadow-sm w-64"
                  value={fixInstructions}
                  onChange={e => setFixInstructions(e.target.value)}
                />
                <button
                  onClick={handleAutoFix}
                  disabled={isFixing}
                  className="btn-primary py-1.5 px-3 text-sm flex items-center"
                >
                  {isFixing ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Bot className="w-4 h-4 mr-2" />}
                  Auto-Fix ({selectedFixes.size})
                </button>
              </div>
            )}
          </div>

          {fixSuccess && (
            <div className="m-6 p-4 bg-emerald-50 border border-emerald-200 rounded-lg flex items-start">
              <CheckCircle className="w-5 h-5 text-emerald-500 mr-3 mt-0.5" />
              <div>
                <h3 className="text-sm font-medium text-emerald-800">Fixes Applied Successfully!</h3>
                <p className="text-sm text-emerald-600 mt-1">
                  Re-run validation to see the updated metrics. {fixSuccess.rows_removed} rows removed. fixed {fixSuccess.fixed_rows} rows.
                </p>
              </div>
              <button className="ml-auto text-emerald-500" onClick={() => setFixSuccess(null)}><XCircle className="w-5 h-5" /></button>
            </div>
          )}

          {filteredResults.length === 0 ? (
            <div className="p-8 text-center text-gray-500">
              <BarChart3 className="w-10 h-10 mx-auto mb-3 text-gray-300" />
              <p>No detailed results matching filter.</p>
            </div>
          ) : (
            <div className="divide-y divide-gray-200">
              {filteredResults.map((result: any, idx: number) => {
                const fixable = isRuleFixable(result);
                const isFailed = result.status === 'failed' || result.status === 'warning';

                return (
                  <div key={result.rule_id || idx} className="p-6 hover:bg-gray-50">
                    <div className="flex items-start justify-between">
                      <div className="flex items-start space-x-3">
                        {isFailed && fixable ? (
                          <input
                            type="checkbox"
                            className="mt-1 w-4 h-4 text-primary-600 rounded border-gray-300 focus:ring-primary-500 cursor-pointer"
                            checked={selectedFixes.has(result.rule_id)}
                            onChange={() => toggleSelection(result.rule_id)}
                          />
                        ) : (
                          getStatusIcon(result.status)
                        )}

                        <div>
                          <h3 className="text-sm font-medium text-gray-900 flex items-center">
                            {result.rule_name}
                            {fixable && isFailed && <span className="ml-2 px-2 py-0.5 text-[10px] font-bold uppercase bg-blue-100 text-blue-700 rounded-full border border-blue-200">AI Fixable</span>}
                            {!fixable && isFailed && <span className="ml-2 px-2 py-0.5 text-[10px] font-bold uppercase bg-amber-100 text-amber-700 rounded-full border border-amber-200">Manual Resolve</span>}
                          </h3>
                          <div className="flex items-center space-x-2 mt-1">
                            {getSeverityBadge(result.severity)}
                            <span className="text-xs text-gray-500">
                              {result.rule_type}
                            </span>
                          </div>
                        </div>
                      </div>
                      <div className="text-right flex flex-col items-end">
                        {getStatusBadge(result.status)}
                        <p className="text-xs text-gray-500 mt-1">
                          {result.passed_count != null && `${result.passed_count.toLocaleString()} passed`}
                          {result.failed_count > 0 &&
                            `, ${result.failed_count.toLocaleString()} failed`}
                        </p>
                        {isFailed && !fixable && (
                          <button
                            onClick={() => handleCreateTicket(result.rule_name)}
                            disabled={isTicketing === result.rule_name}
                            className="mt-3 inline-flex items-center px-2.5 py-1.5 border border-gray-300 shadow-sm text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50 transition-colors"
                          >
                            {isTicketing === result.rule_name ? <Loader2 className="w-3.5 h-3.5 mr-1.5 animate-spin" /> : <Ticket className="w-3.5 h-3.5 mr-1.5 text-gray-500" />}
                            File Data Ticket
                          </button>
                        )}
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
                );
              })}
            </div>
          )}
        </div>
      )}

      {/* Ticket Modal */}
      {generatedTicket && (
        <div className="fixed inset-0 z-50 overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
          <div className="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
            <div className="fixed inset-0 bg-gray-500 bg-opacity-75 transition-opacity" onClick={() => setGeneratedTicket(null)}></div>
            <span className="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
            <div className="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-3xl sm:w-full">
              <div className="bg-white px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
                <div className="sm:flex sm:items-start">
                  <div className="mx-auto flex-shrink-0 flex items-center justify-center h-12 w-12 rounded-full bg-blue-100 sm:mx-0 sm:h-10 sm:w-10">
                    <Ticket className="h-6 w-6 text-blue-600" aria-hidden="true" />
                  </div>
                  <div className="mt-3 text-center sm:mt-0 sm:ml-4 sm:text-left w-full">
                    <h3 className="text-lg leading-6 font-medium text-gray-900" id="modal-title">
                      Data Anomaly Ticket Draft
                    </h3>
                    <div className="mt-4">
                      <div className="bg-gray-50 rounded-lg border border-gray-200 p-4">
                        <pre className="text-sm font-mono text-gray-800 whitespace-pre-wrap overflow-x-auto max-h-[60vh] overflow-y-auto">
                          {generatedTicket}
                        </pre>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
              <div className="bg-gray-50 px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse border-t border-gray-200">
                <button
                  type="button"
                  className="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-blue-600 text-base font-medium text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 sm:ml-3 sm:w-auto sm:text-sm"
                  onClick={() => {
                    navigator.clipboard.writeText(generatedTicket);
                    alert("Copied to clipboard!");
                  }}
                >
                  Copy Markdown
                </button>
                <button
                  type="button"
                  className="mt-3 w-full inline-flex justify-center rounded-md border border-gray-300 shadow-sm px-4 py-2 bg-white text-base font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm"
                  onClick={() => setGeneratedTicket(null)}
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Agent Execution Logs Tab */}
      {activeTab === 'logs' && (
        <div className="card shadow-md overflow-hidden bg-white border-gray-200">
          <div className="px-6 py-6 overflow-y-auto max-h-[800px] border-gray-100 rounded-xl">
            <ExecutionTraceViewer messages={validation?.result?.messages || []} />
          </div>
        </div>
      )}
    </div>
  );
}
