import { Link } from 'react-router-dom';
import {
  Database,
  ClipboardCheck,
  ListChecks,
  TrendingUp,
  Activity,
  CheckCircle,
  XCircle,
  AlertCircle,
} from 'lucide-react';
import { useSystemHealth, useLLMHealth } from '@/hooks/useSystem';
import { useDataSources } from '@/hooks/useDataSources';

// Mock data for dashboard - would come from API in production
const mockStats = {
  totalValidations: 156,
  totalDataSources: 12,
  totalRules: 89,
  averageQualityScore: 87.5,
};

const mockRecentValidations = [
  {
    id: '1',
    target_path: 'customers.csv',
    status: 'completed',
    quality_score: 92,
    total_rules: 15,
    passed_rules: 14,
    failed_rules: 1,
    completed_at: '2024-02-11T10:30:00Z',
  },
  {
    id: '2',
    target_path: 'orders.parquet',
    status: 'completed',
    quality_score: 78,
    total_rules: 20,
    passed_rules: 16,
    failed_rules: 4,
    completed_at: '2024-02-11T09:15:00Z',
  },
  {
    id: '3',
    target_path: 'products.json',
    status: 'failed',
    quality_score: 45,
    total_rules: 12,
    passed_rules: 5,
    failed_rules: 7,
    completed_at: '2024-02-11T08:00:00Z',
  },
  {
    id: '4',
    target_path: 'inventory.xlsx',
    status: 'running',
    quality_score: null,
    total_rules: 18,
    passed_rules: 0,
    failed_rules: 0,
    completed_at: null,
  },
];

export default function Dashboard() {
  const { data: systemHealth } = useSystemHealth();
  const { data: llmHealth } = useLLMHealth();
  const { data: dataSources } = useDataSources();

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircle className="w-5 h-5 text-success-500" />;
      case 'failed':
        return <XCircle className="w-5 h-5 text-danger-500" />;
      case 'running':
        return <Activity className="w-5 h-5 text-primary-500 animate-pulse" />;
      default:
        return <AlertCircle className="w-5 h-5 text-gray-400" />;
    }
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'completed':
        return <span className="badge-success">Completed</span>;
      case 'failed':
        return <span className="badge-danger">Failed</span>;
      case 'running':
        return <span className="badge-info">Running</span>;
      default:
        return <span className="badge">{status}</span>;
    }
  };

  const getScoreColor = (score: number) => {
    if (score >= 90) return 'text-success-600';
    if (score >= 70) return 'text-warning-600';
    return 'text-danger-600';
  };

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
        <p className="mt-1 text-sm text-gray-500">
          Overview of your data quality metrics and recent activity
        </p>
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">
        <div className="card">
          <div className="card-body">
            <div className="flex items-center">
              <div className="p-3 bg-primary-100 rounded-lg">
                <ClipboardCheck className="w-6 h-6 text-primary-600" />
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-500">Total Validations</p>
                <p className="text-2xl font-bold text-gray-900">{mockStats.totalValidations}</p>
              </div>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-body">
            <div className="flex items-center">
              <div className="p-3 bg-success-100 rounded-lg">
                <Database className="w-6 h-6 text-success-600" />
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-500">Data Sources</p>
                <p className="text-2xl font-bold text-gray-900">
                  {dataSources?.length || mockStats.totalDataSources}
                </p>
              </div>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-body">
            <div className="flex items-center">
              <div className="p-3 bg-warning-100 rounded-lg">
                <ListChecks className="w-6 h-6 text-warning-600" />
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-500">Validation Rules</p>
                <p className="text-2xl font-bold text-gray-900">{mockStats.totalRules}</p>
              </div>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-body">
            <div className="flex items-center">
              <div className="p-3 bg-primary-100 rounded-lg">
                <TrendingUp className="w-6 h-6 text-primary-600" />
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-500">Avg Quality Score</p>
                <p className="text-2xl font-bold text-gray-900">
                  {mockStats.averageQualityScore}%
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* System Status */}
      <div className="card">
        <div className="card-header">
          <h2 className="text-lg font-semibold text-gray-900">System Status</h2>
        </div>
        <div className="card-body">
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
            <div className="flex items-center p-4 bg-gray-50 rounded-lg">
              <div
                className={`w-3 h-3 rounded-full mr-3 ${systemHealth?.status === 'healthy' ? 'bg-success-500' : 'bg-danger-500'
                  }`}
              />
              <div>
                <p className="text-sm font-medium text-gray-900">API Server</p>
                <p className="text-xs text-gray-500">{systemHealth?.status || 'Unknown'}</p>
              </div>
            </div>

            <div className="flex items-center p-4 bg-gray-50 rounded-lg">
              <div
                className={`w-3 h-3 rounded-full mr-3 ${llmHealth?.status === 'healthy' ? 'bg-success-500' : 'bg-danger-500'
                  }`}
              />
              <div>
                <p className="text-sm font-medium text-gray-900">LLM Service</p>
                <p className="text-xs text-gray-500">
                  {llmHealth?.provider || 'Unknown'} ({llmHealth?.model || 'N/A'})
                </p>
              </div>
            </div>

            <div className="flex items-center p-4 bg-gray-50 rounded-lg">
              <div className="w-3 h-3 rounded-full bg-success-500 mr-3" />
              <div>
                <p className="text-sm font-medium text-gray-900">Database</p>
                <p className="text-xs text-gray-500">Connected</p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Recent Validations */}
      <div className="card">
        <div className="card-header flex items-center justify-between">
          <h2 className="text-lg font-semibold text-gray-900">Recent Validations</h2>
          <Link to="/validations" className="text-sm text-primary-600 hover:text-primary-700">
            View all
          </Link>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Target
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Quality Score
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Rules
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Completed
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {mockRecentValidations.map((validation) => (
                <tr key={validation.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <Link
                      to={`/validations/${validation.id}`}
                      className="text-sm font-medium text-primary-600 hover:text-primary-700"
                    >
                      {validation.target_path}
                    </Link>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center space-x-2">
                      {getStatusIcon(validation.status)}
                      {getStatusBadge(validation.status)}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    {validation.quality_score !== null ? (
                      <span
                        className={`text-sm font-semibold ${getScoreColor(
                          validation.quality_score
                        )}`}
                      >
                        {validation.quality_score}%
                      </span>
                    ) : (
                      <span className="text-sm text-gray-400">-</span>
                    )}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center space-x-2 text-sm">
                      <span className="text-success-600">
                        {validation.passed_rules} passed
                      </span>
                      <span className="text-gray-400">|</span>
                      <span className="text-danger-600">
                        {validation.failed_rules} failed
                      </span>
                      <span className="text-gray-400">/</span>
                      <span className="text-gray-600">{validation.total_rules} total</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {validation.completed_at
                      ? new Date(validation.completed_at).toLocaleString()
                      : '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Quick Actions */}
      <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-3">
        <Link
          to="/datasources"
          className="card hover:shadow-md transition-shadow cursor-pointer"
        >
          <div className="card-body">
            <div className="flex items-center space-x-3">
              <div className="p-3 bg-primary-100 rounded-lg">
                <Database className="w-6 h-6 text-primary-600" />
              </div>
              <div>
                <h3 className="text-sm font-medium text-gray-900">Connect Data Source</h3>
                <p className="text-xs text-gray-500 mt-1">Add a new data source to validate</p>
              </div>
            </div>
          </div>
        </Link>

        <Link
          to="/validations"
          className="card hover:shadow-md transition-shadow cursor-pointer"
        >
          <div className="card-body">
            <div className="flex items-center space-x-3">
              <div className="p-3 bg-success-100 rounded-lg">
                <ClipboardCheck className="w-6 h-6 text-success-600" />
              </div>
              <div>
                <h3 className="text-sm font-medium text-gray-900">Run Validation</h3>
                <p className="text-xs text-gray-500 mt-1">Start a new data quality validation</p>
              </div>
            </div>
          </div>
        </Link>

        <Link
          to="/rules"
          className="card hover:shadow-md transition-shadow cursor-pointer"
        >
          <div className="card-body">
            <div className="flex items-center space-x-3">
              <div className="p-3 bg-warning-100 rounded-lg">
                <ListChecks className="w-6 h-6 text-warning-600" />
              </div>
              <div>
                <h3 className="text-sm font-medium text-gray-900">Manage Rules</h3>
                <p className="text-xs text-gray-500 mt-1">Create and edit validation rules</p>
              </div>
            </div>
          </div>
        </Link>
      </div>
    </div>
  );
}
