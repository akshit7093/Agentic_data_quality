import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Play,
  Plus,
  RefreshCw,
  CheckCircle,
  XCircle,
  AlertCircle,
  Activity,
  Brain,
  FileUp,
} from 'lucide-react';
import { useSubmitValidation, useRecommendRules } from '@/hooks/useValidations';
import { useDataSources } from '@/hooks/useDataSources';
import Modal from '@/components/Modal';

interface ValidationFormData {
  data_source_id: string;
  target_path: string;
  validation_mode: 'custom_rules' | 'ai_recommended' | 'hybrid';
  sample_size: number;
}

export default function Validations() {
  const navigate = useNavigate();
  const { data: dataSources } = useDataSources();
  const submitValidation = useSubmitValidation();
  const recommendRules = useRecommendRules();

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [formData, setFormData] = useState<ValidationFormData>({
    data_source_id: '',
    target_path: '',
    validation_mode: 'hybrid',
    sample_size: 1000,
  });

  // Mock validations data - would come from API
  const mockValidations = [
    {
      id: '1',
      target_path: 'customers.csv',
      status: 'completed',
      validation_mode: 'hybrid',
      quality_score: 92,
      total_rules: 15,
      passed_rules: 14,
      failed_rules: 1,
      records_processed: 10000,
      started_at: '2024-02-11T10:00:00Z',
      completed_at: '2024-02-11T10:30:00Z',
    },
    {
      id: '2',
      target_path: 'orders.parquet',
      status: 'completed',
      validation_mode: 'ai_recommended',
      quality_score: 78,
      total_rules: 20,
      passed_rules: 16,
      failed_rules: 4,
      records_processed: 50000,
      started_at: '2024-02-11T09:00:00Z',
      completed_at: '2024-02-11T09:15:00Z',
    },
    {
      id: '3',
      target_path: 'products.json',
      status: 'failed',
      validation_mode: 'custom_rules',
      quality_score: 45,
      total_rules: 12,
      passed_rules: 5,
      failed_rules: 7,
      records_processed: 5000,
      started_at: '2024-02-11T08:00:00Z',
      completed_at: '2024-02-11T08:05:00Z',
    },
    {
      id: '4',
      target_path: 'inventory.xlsx',
      status: 'running',
      validation_mode: 'hybrid',
      quality_score: null,
      total_rules: 18,
      passed_rules: 10,
      failed_rules: 2,
      records_processed: 25000,
      started_at: '2024-02-11T11:00:00Z',
      completed_at: null,
    },
  ];

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const result = await submitValidation.mutateAsync({
      ...formData,
      custom_rules: [],
    });
    if (result?.validation_id) {
      setIsModalOpen(false);
      navigate(`/validations/${result.validation_id}`);
    }
  };

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

  const getModeBadge = (mode: string) => {
    switch (mode) {
      case 'ai_recommended':
        return (
          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-purple-100 text-purple-800">
            <Brain className="w-3 h-3 mr-1" />
            AI
          </span>
        );
      case 'custom_rules':
        return (
          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-800">
            Custom
          </span>
        );
      case 'hybrid':
        return (
          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-primary-100 text-primary-800">
            <Brain className="w-3 h-3 mr-1" />
            Hybrid
          </span>
        );
      default:
        return <span className="badge">{mode}</span>;
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
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Validations</h1>
          <p className="mt-1 text-sm text-gray-500">
            Run and monitor data quality validations
          </p>
        </div>
        <button
          onClick={() => navigate('/validations/new')}
          className="btn-primary"
        >
          <Plus className="w-4 h-4 mr-2" />
          New Validation
        </button>
      </div>

      {/* Validations list */}
      <div className="card">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Target
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Mode
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
                  Records
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Duration
                </th>
                <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {mockValidations.map((validation) => (
                <tr
                  key={validation.id}
                  className="hover:bg-gray-50 cursor-pointer"
                  onClick={() => navigate(`/validations/${validation.id}`)}
                >
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      <FileUp className="w-4 h-4 text-gray-400 mr-2" />
                      <span className="text-sm font-medium text-gray-900">
                        {validation.target_path}
                      </span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    {getModeBadge(validation.validation_mode)}
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
                        {validation.passed_rules}
                      </span>
                      <span className="text-gray-400">/</span>
                      <span className="text-danger-600">
                        {validation.failed_rules}
                      </span>
                      <span className="text-gray-400">/</span>
                      <span className="text-gray-600">{validation.total_rules}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {validation.records_processed.toLocaleString()}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {validation.completed_at && validation.started_at
                      ? `${Math.round(
                          (new Date(validation.completed_at).getTime() -
                            new Date(validation.started_at).getTime()) /
                            1000
                        )}s`
                      : validation.status === 'running'
                      ? 'Running...'
                      : '-'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-right">
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        navigate(`/validations/${validation.id}`);
                      }}
                      className="text-primary-600 hover:text-primary-700 text-sm font-medium"
                    >
                      View
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* New Validation Modal */}
      <Modal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        title="New Validation"
      >
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="form-label">Data Source</label>
            <select
              className="form-select"
              value={formData.data_source_id}
              onChange={(e) =>
                setFormData({ ...formData, data_source_id: e.target.value })
              }
              required
            >
              <option value="">Select a data source</option>
              {dataSources?.map((source) => (
                <option key={source.id} value={source.id}>
                  {source.name} ({source.source_type})
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="form-label">Target Path / Table</label>
            <input
              type="text"
              className="form-input"
              placeholder="e.g., customers.csv or public.users"
              value={formData.target_path}
              onChange={(e) =>
                setFormData({ ...formData, target_path: e.target.value })
              }
              required
            />
          </div>

          <div>
            <label className="form-label">Validation Mode</label>
            <div className="space-y-2">
              <label className="flex items-center p-3 border border-gray-200 rounded-lg cursor-pointer hover:bg-gray-50">
                <input
                  type="radio"
                  name="validation_mode"
                  value="custom_rules"
                  checked={formData.validation_mode === 'custom_rules'}
                  onChange={(e) =>
                    setFormData({
                      ...formData,
                      validation_mode: e.target.value as any,
                    })
                  }
                  className="mr-3"
                />
                <div>
                  <span className="text-sm font-medium text-gray-900">Custom Rules Only</span>
                  <p className="text-xs text-gray-500">Use only your predefined validation rules</p>
                </div>
              </label>

              <label className="flex items-center p-3 border border-gray-200 rounded-lg cursor-pointer hover:bg-gray-50">
                <input
                  type="radio"
                  name="validation_mode"
                  value="ai_recommended"
                  checked={formData.validation_mode === 'ai_recommended'}
                  onChange={(e) =>
                    setFormData({
                      ...formData,
                      validation_mode: e.target.value as any,
                    })
                  }
                  className="mr-3"
                />
                <div>
                  <span className="text-sm font-medium text-gray-900">AI Recommended</span>
                  <p className="text-xs text-gray-500">Let AI generate rules based on data profiling</p>
                </div>
              </label>

              <label className="flex items-center p-3 border border-primary-200 bg-primary-50 rounded-lg cursor-pointer">
                <input
                  type="radio"
                  name="validation_mode"
                  value="hybrid"
                  checked={formData.validation_mode === 'hybrid'}
                  onChange={(e) =>
                    setFormData({
                      ...formData,
                      validation_mode: e.target.value as any,
                    })
                  }
                  className="mr-3"
                />
                <div>
                  <span className="text-sm font-medium text-primary-900">Hybrid (Recommended)</span>
                  <p className="text-xs text-primary-600">Combine custom rules with AI recommendations</p>
                </div>
              </label>
            </div>
          </div>

          <div>
            <label className="form-label">Sample Size</label>
            <input
              type="number"
              className="form-input"
              min={100}
              max={100000}
              value={formData.sample_size}
              onChange={(e) =>
                setFormData({
                  ...formData,
                  sample_size: parseInt(e.target.value),
                })
              }
            />
            <p className="text-xs text-gray-500 mt-1">
              Number of rows to sample for validation (100 - 100,000)
            </p>
          </div>

          <div className="flex justify-end space-x-3 pt-4">
            <button
              type="button"
              onClick={() => setIsModalOpen(false)}
              className="btn-secondary"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="btn-primary"
              disabled={submitValidation.isPending}
            >
              {submitValidation.isPending ? (
                <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
              ) : (
                <Play className="w-4 h-4 mr-2" />
              )}
              Start Validation
            </button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
