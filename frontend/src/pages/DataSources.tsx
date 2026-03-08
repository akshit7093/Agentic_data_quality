import { useState } from 'react';
import {
  Database,
  Plus,
  Trash2,
  RefreshCw,
  Settings,
  Cloud,
  FileBox,
  Code
} from 'lucide-react';
import { useDataSources, useCreateDataSource, useDeleteDataSource, useTestConnection } from '@/hooks/useDataSources';
import { useSupportedSources } from '@/hooks/useSystem';
import Modal from '@/components/Modal';

interface DataSourceFormData {
  name: string;
  description: string;
  source_type: string;
  connection_config: Record<string, any>;
}

export default function DataSources() {
  const { data: dataSources, isLoading } = useDataSources();
  const { data: supportedTypes } = useSupportedSources();
  const createDataSource = useCreateDataSource();
  const deleteDataSource = useDeleteDataSource();
  const testConnection = useTestConnection();

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [expandedSource, setExpandedSource] = useState<string | null>(null);
  const [formData, setFormData] = useState<DataSourceFormData>({
    name: '',
    description: '',
    source_type: 'local_file',
    connection_config: {},
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    await createDataSource.mutateAsync(formData);
    setIsModalOpen(false);
    setFormData({
      name: '',
      description: '',
      source_type: 'local_file',
      connection_config: {},
    });
  };

  const getSourceIcon = (type: string) => {
    switch (type) {
      case 'local_file':
      case 'csv':
      case 'excel':
      case 'parquet':
        return <FileBox className="w-8 h-8" />;
      case 'postgresql':
      case 'mysql':
      case 'sqlserver':
        return <Database className="w-8 h-8" />;
      default:
        return <Cloud className="w-8 h-8" />;
    }
  };

  const getConnectionFields = (type: string) => {
    switch (type) {
      case 'local_file':
      case 'csv':
      case 'excel':
      case 'parquet':
        return (
          <div>
            <label className="block text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Base Path</label>
            <input
              type="text"
              className="w-full bg-slate-900 border border-primary/20 rounded-lg px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
              placeholder="/path/to/data/files"
              value={formData.connection_config.base_path || ''}
              onChange={(e) =>
                setFormData({
                  ...formData,
                  connection_config: { base_path: e.target.value },
                })
              }
            />
          </div>
        );
      case 'postgresql':
      case 'mysql':
        return (
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Host</label>
                <input
                  type="text"
                  className="w-full bg-slate-900 border border-primary/20 rounded-lg px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                  placeholder="localhost"
                  value={formData.connection_config.host || ''}
                  onChange={(e) =>
                    setFormData({
                      ...formData,
                      connection_config: { ...formData.connection_config, host: e.target.value },
                    })
                  }
                />
              </div>
              <div>
                <label className="block text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Port</label>
                <input
                  type="number"
                  className="w-full bg-slate-900 border border-primary/20 rounded-lg px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                  placeholder="5432"
                  value={formData.connection_config.port || ''}
                  onChange={(e) =>
                    setFormData({
                      ...formData,
                      connection_config: { ...formData.connection_config, port: e.target.value },
                    })
                  }
                />
              </div>
            </div>
            <div>
              <label className="block text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Database</label>
              <input
                type="text"
                className="w-full bg-slate-900 border border-primary/20 rounded-lg px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                placeholder="database_name"
                value={formData.connection_config.database || ''}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    connection_config: { ...formData.connection_config, database: e.target.value },
                  })
                }
              />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Username</label>
                <input
                  type="text"
                  className="w-full bg-slate-900 border border-primary/20 rounded-lg px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                  placeholder="username"
                  value={formData.connection_config.username || ''}
                  onChange={(e) =>
                    setFormData({
                      ...formData,
                      connection_config: { ...formData.connection_config, username: e.target.value },
                    })
                  }
                />
              </div>
              <div>
                <label className="block text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Password</label>
                <input
                  type="password"
                  className="w-full bg-slate-900 border border-primary/20 rounded-lg px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                  placeholder="password"
                  value={formData.connection_config.password || ''}
                  onChange={(e) =>
                    setFormData({
                      ...formData,
                      connection_config: { ...formData.connection_config, password: e.target.value },
                    })
                  }
                />
              </div>
            </div>
          </div>
        );
      default:
        return (
          <div>
            <label className="block text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Connection String</label>
            <input
              type="text"
              className="w-full bg-slate-900 border border-primary/20 rounded-lg px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
              placeholder="Enter connection details"
              value={formData.connection_config.connection_string || ''}
              onChange={(e) =>
                setFormData({
                  ...formData,
                  connection_config: { connection_string: e.target.value },
                })
              }
            />
          </div>
        );
    }
  };

  return (
    <div className="space-y-8">
      {/* Page header */}
      <div className="flex flex-col md:flex-row md:items-end justify-between gap-6 mb-10">
        <div className="flex flex-col gap-2">
          <h1 className="text-slate-100 text-4xl md:text-5xl font-black leading-tight tracking-tighter">Data Sources</h1>
          <p className="text-slate-400 text-lg max-w-2xl">Manage and monitor your enterprise data pipelines with real-time quality scanning.</p>
        </div>
        <button
          onClick={() => setIsModalOpen(true)}
          className="flex items-center justify-center gap-2 px-6 py-3 bg-primary text-black hover:bg-primary/90 transition-all rounded-lg font-bold text-sm tracking-wide shadow-[0_0_20px_rgba(16,183,127,0.4)]"
        >
          <Plus className="w-5 h-5" />
          <span>CONNECT NEW SOURCE</span>
        </button>
      </div>

      <div className="flex border-b border-primary/10 mb-8 overflow-x-auto whitespace-nowrap scrollbar-hide">
        <button className="px-6 py-4 text-primary border-b-2 border-primary font-bold text-sm tracking-wide">
          ALL SOURCES ({dataSources?.length || 0})
        </button>
      </div>

      {/* Data sources list */}
      {isLoading ? (
        <div className="flex items-center justify-center h-64">
          <RefreshCw className="w-8 h-8 text-primary animate-spin" />
        </div>
      ) : dataSources?.length === 0 ? (
        <div className="bg-surface-dark border border-primary/10 rounded-xl">
          <div className="text-center py-16">
            <Database className="w-16 h-16 text-primary/30 mx-auto mb-6" />
            <h3 className="text-xl font-bold text-slate-100 mb-2">No data sources connected</h3>
            <p className="text-slate-500 mb-6 max-w-md mx-auto">
              Connect your first data source to start parsing schemas and validating data quality across your infrastructure.
            </p>
            <button
              onClick={() => setIsModalOpen(true)}
              className="inline-flex items-center justify-center gap-2 px-6 py-3 bg-primary/10 text-primary border border-primary/30 hover:bg-primary/20 transition-all rounded-lg font-bold text-sm tracking-wide"
            >
              <Plus className="w-5 h-5" />
              <span>Add Data Source</span>
            </button>
          </div>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-2 xl:grid-cols-4 gap-6">
          {dataSources?.map((source) => (
            <div key={source.id} className="group relative flex flex-col bg-surface-dark border border-primary/10 p-6 rounded-xl transition-all hover:border-primary hover:shadow-[0_0_20px_rgba(16,183,127,0.2)] bg-gradient-to-b from-surface-dark to-black overflow-hidden">
              <div className="absolute top-0 left-0 w-full h-1 bg-primary scale-x-0 group-hover:scale-x-100 transition-transform origin-left"></div>
              <div className="flex items-start justify-between mb-6">
                <div className="size-12 rounded-lg bg-primary/10 flex items-center justify-center text-primary shadow-[0_0_15px_rgba(16,183,127,0.3)]">
                  {getSourceIcon(source.source_type)}
                </div>
                <span className="px-2 py-1 rounded text-[10px] font-black bg-primary/20 text-primary border border-primary/30 uppercase tracking-widest">{source.is_active ? 'Active' : 'Inactive'}</span>
              </div>
              <div className="mb-6">
                <h3 className="text-slate-100 font-bold text-xl mb-1">{source.name}</h3>
                <p className="text-slate-500 text-sm font-bold uppercase tracking-wider">{source.source_type}</p>
              </div>
              <div className="mt-auto flex items-center justify-between border-t border-primary/10 pt-4">
                <div className="flex flex-col">
                  <span className="text-[10px] text-slate-500 font-bold uppercase tracking-widest">Created</span>
                  <span className="text-xs text-slate-300 font-bold">{new Date(source.created_at).toLocaleDateString()}</span>
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => testConnection.mutate(source.id)}
                    className="p-2 rounded-lg bg-slate-900 border border-primary/10 text-primary hover:bg-primary hover:text-black transition-all"
                    title="Test Connection"
                  >
                    <RefreshCw className="w-4 h-4" />
                  </button>
                  <button
                    onClick={() => setExpandedSource(expandedSource === source.id ? null : source.id)}
                    className="p-2 rounded-lg bg-slate-900 border border-primary/10 text-slate-400 hover:text-primary transition-all"
                    title="Settings"
                  >
                    <Settings className="w-4 h-4" />
                  </button>
                  <button
                    onClick={() => deleteDataSource.mutate(source.id)}
                    className="p-2 rounded-lg bg-slate-900 border border-primary/10 text-red-500 hover:bg-red-500 hover:text-black transition-all"
                    title="Delete"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              </div>

              {expandedSource === source.id && (
                <div className="mt-4 pt-4 border-t border-primary/10">
                  <div className="flex flex-col gap-2">
                    <span className="text-[10px] text-slate-500 font-bold uppercase tracking-widest">Description</span>
                    <p className="text-sm text-slate-300 bg-slate-900 p-3 rounded border border-primary/5">{source.description || 'No description available for this source.'}</p>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Secondary Section */}
      <div className="mt-12 bg-surface-dark/40 border border-primary/10 rounded-2xl p-8 flex flex-col lg:flex-row items-center justify-between gap-8">
        <div className="flex-1">
          <h2 className="text-2xl font-black text-slate-100 mb-2 uppercase tracking-wide">Automate with API</h2>
          <p className="text-slate-400 mb-6 leading-relaxed max-w-xl">Connect your custom data pipelines using our REST API or CLI tool. Generate secure access tokens for CI/CD integrations.</p>
          <div className="flex gap-4">
            <button className="px-5 py-2.5 rounded-lg border border-primary/30 text-primary font-bold text-xs uppercase tracking-widest hover:bg-primary/10 transition-all">API Documentation</button>
            <button className="px-5 py-2.5 rounded-lg border border-primary/30 text-primary font-bold text-xs uppercase tracking-widest hover:bg-primary/10 transition-all">Get CLI Tool</button>
          </div>
        </div>
        <div className="w-full lg:w-1/3 bg-black rounded-xl p-6 border border-primary/10 relative overflow-hidden group">
          <div className="flex items-center gap-2 text-primary mb-4">
            <Code className="w-4 h-4" />
            <span className="text-[10px] font-black uppercase tracking-widest">dq-agent connect</span>
          </div>
          <div className="font-mono text-xs text-slate-400 space-y-2 relative z-10">
            <p><span className="text-primary">$</span> dq-agent init --token ****</p>
            <p><span className="text-primary">$</span> dq-agent link --source local_test</p>
            <p><span className="text-primary">$</span> dq-agent monitor --live</p>
            <p className="text-slate-600 animate-pulse">_</p>
          </div>
          <div className="absolute inset-0 bg-primary/5 opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none"></div>
        </div>
      </div>

      {/* Add Data Source Modal */}
      <Modal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        title="Connect Data Source"
      >
        <div className="bg-surface-dark p-6 rounded-xl border border-primary/20">
          <form onSubmit={handleSubmit} className="space-y-5">
            <div>
              <label className="block text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Name</label>
              <input
                type="text"
                className="w-full bg-slate-900 border border-primary/20 rounded-lg px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                placeholder="Production Database"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                required
              />
            </div>

            <div>
              <label className="block text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Description</label>
              <textarea
                className="w-full bg-slate-900 border border-primary/20 rounded-lg px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all resize-none"
                placeholder="Optional description"
                rows={2}
                value={formData.description}
                onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              />
            </div>

            <div>
              <label className="block text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Source Type</label>
              <select
                className="w-full bg-slate-900 border border-primary/20 rounded-lg px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all font-medium"
                value={formData.source_type}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    source_type: e.target.value,
                    connection_config: {},
                  })
                }
              >
                {supportedTypes?.source_types.map((type) => (
                  <option key={type} value={type} className="bg-slate-800 text-slate-200">
                    {type.replace(/_/g, ' ').toUpperCase()}
                  </option>
                ))}
              </select>
            </div>

            <div className="bg-black/50 p-5 rounded-lg border border-primary/10">
              <h4 className="flex items-center gap-2 text-xs font-black text-primary uppercase tracking-widest mb-4">
                <Settings className="w-3 h-3" /> Connection Config
              </h4>
              {getConnectionFields(formData.source_type)}
            </div>

            <div className="flex justify-end gap-3 pt-2">
              <button
                type="button"
                onClick={() => setIsModalOpen(false)}
                className="px-6 py-2.5 border border-primary/30 text-primary font-bold text-xs uppercase tracking-wide rounded-lg hover:bg-primary/10 transition-all"
              >
                Cancel
              </button>
              <button
                type="submit"
                className="flex items-center gap-2 px-6 py-2.5 bg-primary text-black font-bold text-xs uppercase tracking-wide rounded-lg hover:bg-primary/90 transition-all shadow-[0_0_15px_rgba(16,183,127,0.3)] disabled:opacity-50 disabled:cursor-not-allowed"
                disabled={createDataSource.isPending}
              >
                {createDataSource.isPending ? (
                  <RefreshCw className="w-4 h-4 animate-spin" />
                ) : (
                  <Plus className="w-4 h-4" />
                )}
                <span>Add Source</span>
              </button>
            </div>
          </form>
        </div>
      </Modal>
    </div>
  );
}
