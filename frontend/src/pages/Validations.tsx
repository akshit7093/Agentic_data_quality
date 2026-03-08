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
  Clock,
  Inbox,
  Search,
  Settings2
} from 'lucide-react';
import { useValidations, useSubmitValidation } from '@/hooks/useValidations';
import { useDataSources } from '@/hooks/useDataSources';
import Modal from '@/components/Modal';

interface ValidationFormData {
  data_source_id: string;
  target_path: string;
  validation_mode: 'custom_rules' | 'ai_recommended' | 'hybrid';
  sample_size: number;
  full_scan?: boolean;
}

export default function Validations() {
  const navigate = useNavigate();
  const { data: dataSources } = useDataSources();
  const { data: validations, isLoading: validationsLoading, refetch: refetchValidations } = useValidations();
  const submitValidation = useSubmitValidation();

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [formData, setFormData] = useState<ValidationFormData>({
    data_source_id: '',
    target_path: '',
    validation_mode: 'hybrid',
    sample_size: 1000,
  });

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
        return <CheckCircle className="w-4 h-4 text-emerald-500" />;
      case 'failed':
        return <XCircle className="w-4 h-4 text-red-500" />;
      case 'running':
        return <Activity className="w-4 h-4 text-primary animate-pulse" />;
      case 'pending':
        return <Clock className="w-4 h-4 text-slate-400 animate-pulse" />;
      default:
        return <AlertCircle className="w-4 h-4 text-slate-400" />;
    }
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'completed':
        return <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">Completed</span>;
      case 'failed':
        return <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-red-500/10 text-red-400 border border-red-500/20">Failed</span>;
      case 'running':
        return <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-primary/10 text-primary border border-primary/20">Running</span>;
      case 'pending':
        return <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-slate-800 text-slate-300 border border-slate-700">Pending</span>;
      default:
        return <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-slate-800 text-slate-300 border border-slate-700 w-fit capitalize">{status}</span>;
    }
  };

  const getModeBadge = (mode: string) => {
    switch (mode) {
      case 'ai_recommended':
        return (
          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-purple-500/10 text-purple-400 border border-purple-500/20">
            <Brain className="w-3 h-3 mr-1" /> AI
          </span>
        );
      case 'custom_rules':
        return (
          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-slate-800 text-slate-300 border border-slate-700">
            <Settings2 className="w-3 h-3 mr-1" /> Custom
          </span>
        );
      case 'hybrid':
        return (
          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-primary/10 text-primary border border-primary/20">
            <Brain className="w-3 h-3 mr-1" /> Hybrid
          </span>
        );
      default:
        return <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-slate-800 text-slate-300">{mode}</span>;
    }
  };

  const getScoreColor = (score: number) => {
    if (score >= 90) return 'text-emerald-500';
    if (score >= 70) return 'text-yellow-500';
    return 'text-red-500';
  };

  return (
    <div className="space-y-8">
      {/* Page header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
            <Activity className="w-6 h-6 text-primary" />
            Validation Runs
          </h1>
          <p className="mt-1 text-sm text-slate-400">
            Run and monitor data quality validations across your connected data sources.
          </p>
        </div>
        <div className="flex gap-3">
          <button
            onClick={() => refetchValidations()}
            className="flex justify-center items-center gap-2 rounded-lg border border-royal-green-600 bg-transparent px-4 py-2 text-sm font-bold text-slate-300 transition-all hover:bg-royal-green-700 active:scale-95"
          >
            <RefreshCw className={`w-4 h-4 ${validationsLoading ? 'animate-spin' : ''}`} /> Refresh
          </button>
          <button
            onClick={() => navigate('/validations/new')}
            className="glow-button flex justify-center items-center gap-2 rounded-lg bg-primary px-4 py-2 text-sm font-bold text-white transition-all hover:bg-primary/90 active:scale-95"
          >
            <Plus className="w-4 h-4" />
            New Validation
          </button>
        </div>
      </div>

      {/* Loading state */}
      {validationsLoading && !validations && (
        <div className="flex flex-col items-center justify-center p-16 rounded-xl border border-royal-green-600 bg-royal-green-800/50">
          <RefreshCw className="w-10 h-10 text-primary animate-spin mb-4" />
          <span className="text-slate-400 font-medium">Loading validation history...</span>
        </div>
      )}

      {/* Empty state */}
      {!validationsLoading && (!validations || validations.length === 0) && (
        <div className="rounded-xl border border-royal-green-600 bg-royal-green-800 p-12 text-center flex flex-col items-center">
          <div className="mb-6 flex h-20 w-20 items-center justify-center rounded-full bg-royal-green-900 border border-royal-green-700">
            <Inbox className="w-10 h-10 text-slate-500" />
          </div>
          <h2 className="text-xl font-bold text-slate-100 mb-2">No Validations Yet</h2>
          <p className="text-slate-400 max-w-md mx-auto mb-8">
            Start a new validation to analyze your data quality. Select a data source,
            choose a target, and let the AI engine do the rest.
          </p>
          <button
            onClick={() => navigate('/validations/new')}
            className="glow-button inline-flex justify-center items-center gap-2 rounded-lg bg-primary px-6 py-3 font-bold text-white transition-all hover:bg-primary/90 active:scale-95"
          >
            <Play className="w-5 h-5 fill-current" />
            Run First Validation
          </button>
        </div>
      )}

      {/* Validations list */}
      {validations && validations.length > 0 && (
        <div className="rounded-xl border border-royal-green-600 bg-royal-green-800 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-royal-green-700">
              <thead className="bg-royal-green-900/50">
                <tr>
                  <th className="px-6 py-4 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">
                    Target
                  </th>
                  <th className="px-6 py-4 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">
                    Mode
                  </th>
                  <th className="px-6 py-4 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">
                    Status
                  </th>
                  <th className="px-6 py-4 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">
                    Quality
                  </th>
                  <th className="px-6 py-4 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">
                    Rules (P/F/T)
                  </th>
                  <th className="px-6 py-4 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">
                    Started
                  </th>
                  <th className="px-6 py-4 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">
                    Duration
                  </th>
                  <th className="px-6 py-4 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-royal-green-700 bg-transparent">
                {validations.map((validation: any) => (
                  <tr
                    key={validation.id}
                    className="hover:bg-royal-green-700/30 cursor-pointer transition-colors"
                    onClick={() => navigate(`/validations/${validation.id}`)}
                  >
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center">
                        <div className="h-8 w-8 rounded bg-royal-green-900 border border-royal-green-600 flex items-center justify-center mr-3">
                          <FileUp className="w-4 h-4 text-primary" />
                        </div>
                        <span className="text-sm font-bold text-slate-100">
                          {validation.target_path || '—'}
                        </span>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      {getModeBadge(validation.validation_mode || 'hybrid')}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center space-x-2">
                        {getStatusIcon(validation.status)}
                        {getStatusBadge(validation.status)}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      {validation.quality_score != null ? (
                        <div className="flex items-center gap-2">
                          <div className="w-16 h-1.5 bg-royal-green-900 rounded-full overflow-hidden">
                            <div
                              className={`h-full ${validation.quality_score >= 90 ? 'bg-emerald-500' :
                                validation.quality_score >= 70 ? 'bg-yellow-500' : 'bg-red-500'
                                }`}
                              style={{ width: `${validation.quality_score}%` }}
                            ></div>
                          </div>
                          <span className={`text-sm font-bold ${getScoreColor(validation.quality_score)}`}>
                            {validation.quality_score}%
                          </span>
                        </div>
                      ) : (
                        <span className="text-sm text-slate-500">—</span>
                      )}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center space-x-1.5 text-sm font-mono">
                        <span className="text-emerald-400 bg-emerald-400/10 px-1.5 py-0.5 rounded font-semibold">
                          {validation.passed_rules || 0}
                        </span>
                        <span className="text-slate-600">/</span>
                        <span className="text-red-400 bg-red-400/10 px-1.5 py-0.5 rounded font-semibold">
                          {validation.failed_rules || 0}
                        </span>
                        <span className="text-slate-600">/</span>
                        <span className="text-slate-300 bg-slate-800 px-1.5 py-0.5 rounded font-semibold">
                          {validation.total_rules || 0}
                        </span>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-400">
                      {validation.started_at
                        ? new Date(validation.started_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                        : '—'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-400 font-mono">
                      {validation.completed_at && validation.started_at
                        ? `${Math.round(
                          (new Date(validation.completed_at).getTime() -
                            new Date(validation.started_at).getTime()) /
                          1000
                        )}s`
                        : validation.status === 'running'
                          ? 'Running...'
                          : validation.status === 'pending'
                            ? 'Pending...'
                            : '—'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-right">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          navigate(`/validations/${validation.id}`);
                        }}
                        className="text-primary hover:text-primary/80 text-sm font-bold transition-colors"
                      >
                        View Details
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* New Validation Modal */}
      <Modal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        title="New Validation Run"
      >
        <form onSubmit={handleSubmit} className="space-y-6">
          <div className="space-y-2">
            <label className="text-sm font-bold text-slate-300">Data Source</label>
            <select
              className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-3 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
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

          <div className="space-y-2">
            <label className="text-sm font-bold text-slate-300">Target Path / Table</label>
            <div className="relative">
              <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                <Search className="h-5 w-5 text-slate-500" />
              </div>
              <input
                type="text"
                className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 pl-10 pr-4 py-3 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                placeholder="e.g., customers or s3://bucket/data.csv"
                value={formData.target_path}
                onChange={(e) =>
                  setFormData({ ...formData, target_path: e.target.value })
                }
                required
              />
            </div>
          </div>

          <div className="space-y-3">
            <label className="text-sm font-bold text-slate-300">Validation Engine Mode</label>
            <div className="grid grid-cols-1 gap-3">
              <label className={`flex items-start p-4 rounded-xl border-2 cursor-pointer transition-all ${formData.validation_mode === 'hybrid'
                ? 'border-primary bg-primary/10'
                : 'border-royal-green-600 bg-royal-green-900/50 hover:border-primary/50'
                }`}>
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
                  className="mt-1 mr-4 accent-primary w-4 h-4 bg-royal-green-800 border-royal-green-600"
                />
                <div>
                  <span className={`font-bold flex items-center gap-2 ${formData.validation_mode === 'hybrid' ? 'text-primary' : 'text-slate-200'
                    }`}>
                    <Brain className="w-5 h-5" /> Hybrid Mode (Recommended)
                  </span>
                  <p className="text-sm text-slate-400 mt-1">Combine your custom defined rules with AI-generated dynamic recommendations.</p>
                </div>
              </label>

              <label className={`flex items-start p-4 rounded-xl border-2 cursor-pointer transition-all ${formData.validation_mode === 'ai_recommended'
                ? 'border-primary bg-primary/10'
                : 'border-royal-green-600 bg-royal-green-900/50 hover:border-primary/50'
                }`}>
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
                  className="mt-1 mr-4 accent-primary w-4 h-4 bg-royal-green-800 border-royal-green-600"
                />
                <div>
                  <span className={`font-bold flex items-center gap-2 ${formData.validation_mode === 'ai_recommended' ? 'text-primary' : 'text-slate-200'
                    }`}>
                    <Activity className="w-5 h-5" /> AI Discovery
                  </span>
                  <p className="text-sm text-slate-400 mt-1">Let the AI autonomously profile data and generate all validation rules.</p>
                </div>
              </label>

              <label className={`flex items-start p-4 rounded-xl border-2 cursor-pointer transition-all ${formData.validation_mode === 'custom_rules'
                ? 'border-primary bg-primary/10'
                : 'border-royal-green-600 bg-royal-green-900/50 hover:border-primary/50'
                }`}>
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
                  className="mt-1 mr-4 accent-primary w-4 h-4 bg-royal-green-800 border-royal-green-600"
                />
                <div>
                  <span className={`font-bold flex items-center gap-2 ${formData.validation_mode === 'custom_rules' ? 'text-primary' : 'text-slate-200'
                    }`}>
                    <Settings2 className="w-5 h-5" /> Strict Custom
                  </span>
                  <p className="text-sm text-slate-400 mt-1">Only execute exactly the rules you have predefined in rule groups.</p>
                </div>
              </label>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <label className="text-sm font-bold text-slate-300">Sample Size Limit</label>
              <input
                type="number"
                className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-3 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all font-mono"
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
            </div>

            <div className="space-y-2 flex flex-col justify-end">
              <label className="flex h-[46px] items-center gap-3 p-3 rounded-lg border border-royal-green-600 bg-royal-green-900/50 cursor-pointer hover:border-primary/50 transition-colors">
                <input
                  type="checkbox"
                  className="w-5 h-5 rounded border-royal-green-600 bg-royal-green-900 text-primary focus:ring-primary focus:ring-offset-royal-green-900 accent-primary"
                  checked={formData.full_scan}
                  onChange={(e) =>
                    setFormData({
                      ...formData,
                      full_scan: e.target.checked,
                    })
                  }
                />
                <span className="text-sm font-bold text-slate-200">
                  SQL Push-down (Full Scan)
                </span>
              </label>
            </div>
          </div>

          <div className="flex justify-end gap-3 pt-6 border-t border-royal-green-600">
            <button
              type="button"
              onClick={() => setIsModalOpen(false)}
              className="flex items-center justify-center rounded-lg border border-royal-green-600 bg-transparent px-6 py-2.5 font-bold text-slate-300 transition-all hover:bg-royal-green-700 active:scale-95"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="glow-button flex items-center justify-center gap-2 rounded-lg bg-primary px-6 py-2.5 font-bold text-white transition-all hover:bg-primary/90 active:scale-95 disabled:opacity-50 disabled:active:scale-100"
              disabled={submitValidation.isPending}
            >
              {submitValidation.isPending ? (
                <RefreshCw className="w-5 h-5 animate-spin" />
              ) : (
                <Play className="w-5 h-5 fill-current" />
              )}
              Initialize Engine
            </button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
