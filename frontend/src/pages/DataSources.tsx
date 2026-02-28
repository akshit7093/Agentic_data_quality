import { useState } from 'react';
import {
  Database,
  Plus,
  Trash2,
  RefreshCw,
  ChevronDown,
  ChevronRight,
  File,
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
        return <File className="w-5 h-5 text-gray-400" />;
      case 'postgresql':
      case 'mysql':
      case 'sqlserver':
        return <Database className="w-5 h-5 text-primary-500" />;
      default:
        return <Database className="w-5 h-5 text-gray-400" />;
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
            <label className="form-label">Base Path</label>
            <input
              type="text"
              className="form-input"
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
          <>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="form-label">Host</label>
                <input
                  type="text"
                  className="form-input"
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
                <label className="form-label">Port</label>
                <input
                  type="number"
                  className="form-input"
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
              <label className="form-label">Database</label>
              <input
                type="text"
                className="form-input"
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
                <label className="form-label">Username</label>
                <input
                  type="text"
                  className="form-input"
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
                <label className="form-label">Password</label>
                <input
                  type="password"
                  className="form-input"
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
          </>
        );
      default:
        return (
          <div>
            <label className="form-label">Connection String</label>
            <input
              type="text"
              className="form-input"
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
    <div className="space-y-6">
      {/* Page header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Data Sources</h1>
          <p className="mt-1 text-sm text-gray-500">
            Manage your data source connections
          </p>
        </div>
        <button
          onClick={() => setIsModalOpen(true)}
          className="btn-primary"
        >
          <Plus className="w-4 h-4 mr-2" />
          Add Data Source
        </button>
      </div>

      {/* Data sources list */}
      {isLoading ? (
        <div className="flex items-center justify-center h-64">
          <RefreshCw className="w-8 h-8 text-primary-600 animate-spin" />
        </div>
      ) : dataSources?.length === 0 ? (
        <div className="card">
          <div className="card-body text-center py-12">
            <Database className="w-12 h-12 text-gray-300 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">No data sources</h3>
            <p className="text-sm text-gray-500 mb-4">
              Connect your first data source to start validating data quality
            </p>
            <button
              onClick={() => setIsModalOpen(true)}
              className="btn-primary"
            >
              <Plus className="w-4 h-4 mr-2" />
              Add Data Source
            </button>
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          {dataSources?.map((source) => (
            <div key={source.id} className="card">
              <div className="card-body">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-4">
                    {getSourceIcon(source.source_type)}
                    <div>
                      <h3 className="text-sm font-medium text-gray-900">{source.name}</h3>
                      <p className="text-xs text-gray-500">
                        {source.source_type} • {source.is_active ? 'Active' : 'Inactive'}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center space-x-2">
                    <button
                      onClick={() => testConnection.mutate(source.id)}
                      className="p-2 text-gray-400 hover:text-primary-600 hover:bg-primary-50 rounded-lg transition-colors"
                      title="Test Connection"
                    >
                      <RefreshCw className="w-4 h-4" />
                    </button>
                    <button
                      onClick={() => setExpandedSource(expandedSource === source.id ? null : source.id)}
                      className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
                    >
                      {expandedSource === source.id ? (
                        <ChevronDown className="w-4 h-4" />
                      ) : (
                        <ChevronRight className="w-4 h-4" />
                      )}
                    </button>
                    <button
                      onClick={() => deleteDataSource.mutate(source.id)}
                      className="p-2 text-gray-400 hover:text-danger-600 hover:bg-danger-50 rounded-lg transition-colors"
                      title="Delete"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                </div>

                {expandedSource === source.id && (
                  <div className="mt-4 pt-4 border-t border-gray-200">
                    <div className="grid grid-cols-2 gap-4 text-sm">
                      <div>
                        <span className="text-gray-500">Description:</span>
                        <p className="text-gray-900">{source.description || 'N/A'}</p>
                      </div>
                      <div>
                        <span className="text-gray-500">Created:</span>
                        <p className="text-gray-900">
                          {new Date(source.created_at).toLocaleString()}
                        </p>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Add Data Source Modal */}
      <Modal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        title="Add Data Source"
      >
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="form-label">Name</label>
            <input
              type="text"
              className="form-input"
              placeholder="My Data Source"
              value={formData.name}
              onChange={(e) => setFormData({ ...formData, name: e.target.value })}
              required
            />
          </div>

          <div>
            <label className="form-label">Description</label>
            <textarea
              className="form-textarea"
              placeholder="Optional description"
              rows={2}
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
            />
          </div>

          <div>
            <label className="form-label">Source Type</label>
            <select
              className="form-select"
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
                <option key={type} value={type}>
                  {type.replace(/_/g, ' ').toUpperCase()}
                </option>
              ))}
            </select>
          </div>

          <div className="border-t border-gray-200 pt-4">
            <h4 className="text-sm font-medium text-gray-900 mb-3">Connection Details</h4>
            {getConnectionFields(formData.source_type)}
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
              disabled={createDataSource.isPending}
            >
              {createDataSource.isPending ? (
                <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
              ) : (
                <Plus className="w-4 h-4 mr-2" />
              )}
              Add Data Source
            </button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
