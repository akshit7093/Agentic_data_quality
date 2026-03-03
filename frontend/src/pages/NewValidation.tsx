import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Play,
  ChevronRight,
  ChevronLeft,
  Database,
  Folder,
  FileText,
  Table,
  FileJson,
  CheckCircle,
  Brain,
  Settings,
  ChevronDown,
  Search,
  RefreshCw,
  AlertCircle,
  Upload,
  Eye,
} from 'lucide-react';
import { useDataSources } from '@/hooks/useDataSources';
import { useSubmitValidation } from '@/hooks/useValidations';
import { useLLMHealth } from '@/hooks/useSystem';
import { dataSourceApi, fileApi, ruleGroupApi } from '@/services/api';
import Modal from '@/components/Modal';
import DataExplorer from '@/components/DataExplorer';

interface DataResource {
  name: string;
  type: 'table' | 'file' | 'folder' | 'container' | 'directory';
  path: string;
  size?: string;
  rowCount?: number;
  columnCount?: number;
  columns?: { name: string; type: string }[];
  format?: string;
  children?: DataResource[];
}

interface PreviewData {
  columns: { name: string; type: string }[];
  rows: Record<string, any>[];
  total_rows: number;
  preview_count: number;
}

const VALIDATION_MODES = [
  {
    id: 'custom_rules',
    name: 'Custom Rules Only',
    description: 'Use only predefined validation rules',
    icon: Settings,
  },
  {
    id: 'ai_recommended',
    name: 'AI Recommended',
    description: 'Let AI generate rules based on data profiling',
    icon: Brain,
  },
  {
    id: 'hybrid',
    name: 'Hybrid (Recommended)',
    description: 'Combine custom rules with AI recommendations',
    icon: Database,
  },
  {
    id: 'schema_only',
    name: 'Schema Only',
    description: 'Specialized structural and datatypes analysis',
    icon: Table,
  },
  {
    id: 'business_analysis',
    name: 'Business Analysis',
    description: 'Specialized logic anomalies and multidimensional insights',
    icon: FileJson,
  },
];

// Source type display info
const SOURCE_INFO: Record<string, { label: string; icon: any; color: string }> = {
  'local-test': { label: 'Test Database', icon: Database, color: 'blue' },
  'file-upload': { label: 'Upload File', icon: FileText, color: 'green' },
  'adls-mock': { label: 'ADLS Gen2 Mock', icon: Folder, color: 'purple' },
  'local-files': { label: 'Local Test Files', icon: Folder, color: 'orange' },
};

export default function NewValidation() {
  const navigate = useNavigate();
  const { data: dataSources } = useDataSources();
  const submitValidation = useSubmitValidation();
  const { data: llmHealth } = useLLMHealth();

  const [step, setStep] = useState<'source' | 'browse' | 'explore' | 'config' | 'review'>('source');
  const [selectedDataSource, setSelectedDataSource] = useState<string>('');
  const [selectedResource, setSelectedResource] = useState<DataResource | null>(null);
  const [expandedResources, setExpandedResources] = useState<Set<string>>(new Set());
  const [searchQuery, setSearchQuery] = useState('');
  const [validationMode, setValidationMode] = useState('hybrid');
  const [sampleSize, setSampleSize] = useState(1000);
  const [sliceFilters, setSliceFilters] = useState<any[]>([]);

  // Discovery metadata state
  const [discoveryMetadata, setDiscoveryMetadata] = useState<any>(null);
  const [discoveryLoading, setDiscoveryLoading] = useState(false);

  const [customRules, setCustomRules] = useState<any[]>([]);
  const [newRuleColumn, setNewRuleColumn] = useState('');
  const [newRuleOperator, setNewRuleOperator] = useState('>');
  const [newRuleValue, setNewRuleValue] = useState('');

  const [isLoading, setIsLoading] = useState(false);
  const [showPreview, setShowPreview] = useState(false);
  const [showFullDataModal, setShowFullDataModal] = useState(false);
  const [fullDataPage, setFullDataPage] = useState(0);
  const FULL_DATA_PAGE_SIZE = 50;

  // Dynamic resource loading
  const [resources, setResources] = useState<DataResource[]>([]);
  const [resourcesLoading, setResourcesLoading] = useState(false);
  const [previewData, setPreviewData] = useState<PreviewData | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  // File upload
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);

  // Rule groups
  const [ruleGroups, setRuleGroups] = useState<any[]>([]);
  const [selectedRuleGroup, setSelectedRuleGroup] = useState<string>('');

  // Fetch resources when data source changes
  useEffect(() => {
    if (selectedDataSource && step === 'browse') {
      loadResources();
    }
  }, [selectedDataSource, step]);

  // Fetch rule groups when entering config step
  useEffect(() => {
    if (step === 'config') {
      ruleGroupApi.listGroups().then(data => {
        setRuleGroups(data.groups || []);
      }).catch(() => setRuleGroups([]));
    }
  }, [step]);

  const loadResources = async () => {
    setResourcesLoading(true);
    try {
      const data = await dataSourceApi.getResources(selectedDataSource);
      setResources(data);
    } catch (err) {
      console.error('Failed to load resources:', err);
      setResources([]);
    } finally {
      setResourcesLoading(false);
    }
  };

  const loadPreview = async (resource: DataResource) => {
    setPreviewLoading(true);
    try {
      const data = await dataSourceApi.getPreview(selectedDataSource, resource.path, 1000);
      setPreviewData(data);
    } catch (err) {
      console.error('Failed to load preview:', err);
      setPreviewData(null);
    } finally {
      setPreviewLoading(false);
    }
  };

  // Auto-discover filters when entering explore step with a selected resource
  useEffect(() => {
    if (step === 'explore' && selectedResource && selectedDataSource && !discoveryMetadata) {
      loadDiscovery();
    }
  }, [step, selectedResource, selectedDataSource]);

  const loadDiscovery = async () => {
    if (!selectedResource || !selectedDataSource) return;
    setDiscoveryLoading(true);
    try {
      const res = await fetch(
        `/api/v1/datasources/${selectedDataSource}/discover-filters?resource_path=${encodeURIComponent(selectedResource.path)}`,
        { method: 'POST' }
      );
      if (res.ok) {
        const data = await res.json();
        setDiscoveryMetadata(data);
      } else {
        console.error('Discovery failed:', await res.text());
      }
    } catch (err) {
      console.error('Discovery error:', err);
    } finally {
      setDiscoveryLoading(false);
    }
  };

  const handleFileUpload = async () => {
    if (!uploadFile) return;
    setUploading(true);
    try {
      await fileApi.upload(uploadFile);
      // Reload resources after upload
      await loadResources();
      setUploadFile(null);
    } catch (err) {
      console.error('Upload failed:', err);
    } finally {
      setUploading(false);
    }
  };

  const filteredResources = resources.filter(
    (r) =>
      r.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      r.columns?.some((c) => c.name.toLowerCase().includes(searchQuery.toLowerCase()))
  );

  const toggleExpand = (path: string) => {
    const newExpanded = new Set(expandedResources);
    if (newExpanded.has(path)) {
      newExpanded.delete(path);
    } else {
      newExpanded.add(path);
    }
    setExpandedResources(newExpanded);
  };

  const handleSubmit = async () => {
    if (!selectedResource) return;

    setIsLoading(true);
    try {
      const payload: any = {
        data_source_id: selectedDataSource,
        target_path: selectedResource.path,
        validation_mode: validationMode,
        sample_size: sampleSize,
        custom_rules: customRules,
      };

      if (sliceFilters.length > 0) {
        payload.slice_filters = sliceFilters;
      }

      const result = await submitValidation.mutateAsync(payload);

      if (result?.validation_id) {
        navigate(`/validations/${result.validation_id}`);
      }
    } finally {
      setIsLoading(false);
    }
  };

  const getSourceLabel = () => {
    const info = SOURCE_INFO[selectedDataSource];
    if (info) return info.label;
    const ds = dataSources?.find((d) => d.id === selectedDataSource);
    return ds?.name || 'Data Source';
  };

  const getResourceIcon = (type: string) => {
    switch (type) {
      case 'table': return <Table className="w-5 h-5 text-primary-500" />;
      case 'file': return <FileText className="w-5 h-5 text-green-500" />;
      case 'directory':
      case 'folder':
      case 'container': return <Folder className="w-5 h-5 text-yellow-500" />;
      default: return <FileJson className="w-5 h-5 text-gray-500" />;
    }
  };

  const renderStepIndicator = () => (
    <div className="flex items-center justify-center mb-8">
      {[
        { id: 'source', label: 'Data Source', icon: Database },
        { id: 'browse', label: 'Browse', icon: Folder },
        { id: 'explore', label: 'Explore & Slice', icon: Table },
        { id: 'config', label: 'Configure', icon: Settings },
        { id: 'review', label: 'Review', icon: CheckCircle },
      ].map((s, idx) => {
        const isActive = step === s.id;
        const isCompleted =
          (step === 'browse' && s.id === 'source') ||
          (step === 'explore' && ['source', 'browse'].includes(s.id)) ||
          (step === 'config' && ['source', 'browse', 'explore'].includes(s.id)) ||
          (step === 'review' && ['source', 'browse', 'explore', 'config'].includes(s.id));

        return (
          <div key={s.id} className="flex items-center">
            <div
              className={`flex items-center space-x-2 px-4 py-2 rounded-lg ${isActive
                ? 'bg-primary-100 text-primary-700'
                : isCompleted
                  ? 'text-success-600'
                  : 'text-gray-400'
                }`}
            >
              <s.icon className="w-5 h-5" />
              <span className="font-medium">{s.label}</span>
            </div>
            {idx < 3 && <ChevronRight className="w-5 h-5 text-gray-300 mx-2" />}
          </div>
        );
      })}
    </div>
  );

  const renderSourceStep = () => (
    <div className="space-y-6">
      <div className="text-center">
        <h2 className="text-xl font-semibold text-gray-900">Select Data Source</h2>
        <p className="text-gray-500 mt-1">Choose where your data is located</p>
      </div>

      <div className="grid grid-cols-2 gap-4">
        {/* Built-in: Test Database */}
        <button
          onClick={() => {
            setSelectedDataSource('local-test');
            setSelectedResource(null);
            setStep('browse');
          }}
          className={`p-6 rounded-xl border-2 text-left transition-all ${selectedDataSource === 'local-test'
            ? 'border-primary-500 bg-primary-50'
            : 'border-gray-200 hover:border-gray-300'
            }`}
        >
          <div className="flex items-start space-x-4">
            <div className="p-3 bg-blue-100 rounded-lg">
              <Database className="w-6 h-6 text-blue-600" />
            </div>
            <div>
              <h3 className="font-semibold text-gray-900">Test Database</h3>
              <p className="text-sm text-gray-500 mt-1">
                Local SQLite database with sample data
              </p>
              <div className="flex items-center space-x-4 mt-3 text-sm text-gray-600">
                <span>4 tables</span>
                <span>16,200 rows</span>
              </div>
            </div>
          </div>
        </button>

        {/* Built-in: File Upload */}
        <button
          onClick={() => {
            setSelectedDataSource('file-upload');
            setSelectedResource(null);
            setStep('browse');
          }}
          className={`p-6 rounded-xl border-2 text-left transition-all ${selectedDataSource === 'file-upload'
            ? 'border-primary-500 bg-primary-50'
            : 'border-gray-200 hover:border-gray-300'
            }`}
        >
          <div className="flex items-start space-x-4">
            <div className="p-3 bg-green-100 rounded-lg">
              <FileText className="w-6 h-6 text-green-600" />
            </div>
            <div>
              <h3 className="font-semibold text-gray-900">Upload File</h3>
              <p className="text-sm text-gray-500 mt-1">
                CSV, Excel, Parquet, or JSON files
              </p>
              <div className="flex items-center space-x-4 mt-3 text-sm text-gray-600">
                <span>Max 100MB</span>
              </div>
            </div>
          </div>
        </button>

        {/* Built-in: ADLS Gen2 Mock */}
        <button
          onClick={() => {
            setSelectedDataSource('adls-mock');
            setSelectedResource(null);
            setStep('browse');
          }}
          className={`p-6 rounded-xl border-2 text-left transition-all ${selectedDataSource === 'adls-mock'
            ? 'border-primary-500 bg-primary-50'
            : 'border-gray-200 hover:border-gray-300'
            }`}
        >
          <div className="flex items-start space-x-4">
            <div className="p-3 bg-purple-100 rounded-lg">
              <Folder className="w-6 h-6 text-purple-600" />
            </div>
            <div>
              <h3 className="font-semibold text-gray-900">ADLS Gen2 Mock</h3>
              <p className="text-sm text-gray-500 mt-1">
                Azure Data Lake Storage Gen2 test structure
              </p>
              <div className="flex items-center space-x-4 mt-3 text-sm text-gray-600">
                <span>4 containers</span>
                <span>150 files</span>
              </div>
            </div>
          </div>
        </button>

        {/* Built-in: Local Test Files */}
        <button
          onClick={() => {
            setSelectedDataSource('local-files');
            setSelectedResource(null);
            setStep('browse');
          }}
          className={`p-6 rounded-xl border-2 text-left transition-all ${selectedDataSource === 'local-files'
            ? 'border-primary-500 bg-primary-50'
            : 'border-gray-200 hover:border-gray-300'
            }`}
        >
          <div className="flex items-start space-x-4">
            <div className="p-3 bg-orange-100 rounded-lg">
              <Folder className="w-6 h-6 text-orange-600" />
            </div>
            <div>
              <h3 className="font-semibold text-gray-900">Local Test Files</h3>
              <p className="text-sm text-gray-500 mt-1">
                Structured, semi-structured &amp; unstructured test data
              </p>
              <div className="flex items-center space-x-4 mt-3 text-sm text-gray-600">
                <span>CSV, JSON, XML</span>
              </div>
            </div>
          </div>
        </button>

        {/* User-connected Data Sources (from API) */}
        {dataSources
          ?.filter((ds) => !Object.keys(SOURCE_INFO).includes(ds.id))
          .map((ds) => (
            <button
              key={ds.id}
              onClick={() => {
                setSelectedDataSource(ds.id);
                setSelectedResource(null);
                setStep('browse');
              }}
              className={`p-6 rounded-xl border-2 text-left transition-all ${selectedDataSource === ds.id
                ? 'border-primary-500 bg-primary-50'
                : 'border-gray-200 hover:border-gray-300'
                }`}
            >
              <div className="flex items-start space-x-4">
                <div className="p-3 bg-gray-100 rounded-lg">
                  <Database className="w-6 h-6 text-gray-600" />
                </div>
                <div>
                  <h3 className="font-semibold text-gray-900">{ds.name}</h3>
                  <p className="text-sm text-gray-500 mt-1">{ds.source_type}</p>
                  <div className="flex items-center space-x-2 mt-3">
                    <span
                      className={`w-2 h-2 rounded-full ${ds.is_active ? 'bg-green-500' : 'bg-gray-400'
                        }`}
                    />
                    <span className="text-sm text-gray-600">
                      {ds.is_active ? 'Active' : 'Inactive'}
                    </span>
                  </div>
                </div>
              </div>
            </button>
          ))}
      </div>
    </div>
  );

  const renderBrowseStep = () => (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-gray-900">Browse Resources</h2>
          <p className="text-gray-500 mt-1">Select a table or file to validate</p>
        </div>
        <div className="flex items-center space-x-2">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
            <input
              type="text"
              placeholder="Search..."
              className="pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-primary-500 focus:border-primary-500"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </div>
          <button
            onClick={loadResources}
            className="p-2 border border-gray-300 rounded-lg hover:bg-gray-50"
            title="Refresh"
          >
            <RefreshCw className={`w-5 h-5 text-gray-400 ${resourcesLoading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* File Upload Section (only for file-upload source) */}
      {selectedDataSource === 'file-upload' && (
        <div className="bg-green-50 border-2 border-dashed border-green-300 rounded-xl p-6">
          <div className="text-center">
            <Upload className="w-10 h-10 text-green-500 mx-auto mb-3" />
            <p className="font-medium text-gray-900 mb-1">Upload a File</p>
            <p className="text-sm text-gray-500 mb-4">CSV, Excel, Parquet, or JSON (max 100MB)</p>
            <div className="flex items-center justify-center space-x-3">
              <input
                type="file"
                accept=".csv,.xlsx,.xls,.parquet,.json,.jsonl"
                onChange={(e) => setUploadFile(e.target.files?.[0] || null)}
                className="text-sm"
              />
              {uploadFile && (
                <button
                  onClick={handleFileUpload}
                  disabled={uploading}
                  className="btn-primary text-sm px-4 py-2 disabled:opacity-50"
                >
                  {uploading ? (
                    <><RefreshCw className="w-4 h-4 mr-1 inline animate-spin" /> Uploading...</>
                  ) : (
                    <><Upload className="w-4 h-4 mr-1 inline" /> Upload</>
                  )}
                </button>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Breadcrumb */}
      <div className="card overflow-hidden">
        <div className="bg-gray-50 px-4 py-3 border-b border-gray-200">
          <div className="flex items-center space-x-2 text-sm text-gray-600">
            {getResourceIcon(selectedDataSource === 'local-test' ? 'table' : 'folder')}
            <span>{getSourceLabel()}</span>
            <ChevronRight className="w-4 h-4" />
            <span>{selectedDataSource === 'local-test' ? 'Tables' : 'Files'}</span>
          </div>
        </div>

        {/* Loading State */}
        {resourcesLoading && (
          <div className="flex items-center justify-center p-8">
            <RefreshCw className="w-6 h-6 text-primary-500 animate-spin mr-3" />
            <span className="text-gray-500">Loading resources...</span>
          </div>
        )}

        {/* Empty State */}
        {!resourcesLoading && filteredResources.length === 0 && (
          <div className="text-center p-8 text-gray-500">
            <Folder className="w-10 h-10 mx-auto mb-3 text-gray-300" />
            <p className="font-medium">No resources found</p>
            <p className="text-sm mt-1">
              {selectedDataSource === 'file-upload'
                ? 'Upload a file to get started'
                : 'No tables or files found in this data source'}
            </p>
          </div>
        )}

        {/* Resource List */}
        <div className="divide-y divide-gray-200 max-h-96 overflow-y-auto">
          {filteredResources.map((resource) => (
            <div key={resource.path}>
              <div
                onClick={() => {
                  if (resource.type === 'directory' || resource.type === 'folder') {
                    toggleExpand(resource.path);
                    return;
                  }
                  if (resource.columns) {
                    toggleExpand(resource.path);
                  }
                  setSelectedResource(resource);
                }}
                className={`flex items-center justify-between p-4 cursor-pointer hover:bg-gray-50 ${selectedResource?.path === resource.path ? 'bg-primary-50' : ''
                  }`}
              >
                <div className="flex items-center space-x-3">
                  {getResourceIcon(resource.type)}
                  <div>
                    <span className="font-medium text-gray-900">{resource.name}</span>
                    <span className="text-sm text-gray-500 ml-3">
                      {resource.rowCount != null && `${resource.rowCount.toLocaleString()} rows`}
                      {resource.columnCount != null && `, ${resource.columnCount} cols`}
                      {resource.size && ` • ${resource.size}`}
                      {resource.format && ` • ${resource.format.toUpperCase()}`}
                    </span>
                  </div>
                </div>
                <div className="flex items-center space-x-3">
                  {/* Preview button */}
                  {resource.type !== 'directory' && resource.type !== 'folder' && (
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        setSelectedResource(resource);
                        loadPreview(resource);
                        setShowPreview(true);
                      }}
                      className="p-1 hover:bg-gray-200 rounded text-gray-400 hover:text-primary-600"
                      title="Preview data"
                    >
                      <Eye className="w-4 h-4" />
                    </button>
                  )}
                  {resource.columns && (
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        toggleExpand(resource.path);
                      }}
                      className="p-1 hover:bg-gray-200 rounded"
                    >
                      {expandedResources.has(resource.path) ? (
                        <ChevronDown className="w-4 h-4 text-gray-400" />
                      ) : (
                        <ChevronRight className="w-4 h-4 text-gray-400" />
                      )}
                    </button>
                  )}
                  {selectedResource?.path === resource.path && (
                    <CheckCircle className="w-5 h-5 text-primary-600" />
                  )}
                </div>
              </div>

              {/* Column Preview */}
              {expandedResources.has(resource.path) && resource.columns && (
                <div className="pl-12 pr-4 pb-4">
                  <div className="bg-gray-50 rounded-lg p-3">
                    <p className="text-xs font-medium text-gray-500 mb-2">COLUMNS</p>
                    <div className="grid grid-cols-3 gap-2">
                      {resource.columns.map((col) => (
                        <div
                          key={col.name}
                          className="flex items-center space-x-2 text-sm"
                        >
                          <div className="w-2 h-2 rounded-full bg-primary-400" />
                          <span className="text-gray-700">{col.name}</span>
                          <span className="text-xs text-gray-400">({col.type})</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      <div className="flex justify-between">
        <button onClick={() => setStep('source')} className="btn-secondary">
          <ChevronLeft className="w-4 h-4 mr-2 inline" />
          Back
        </button>
        <button
          onClick={() => {
            if (selectedResource) {
              loadPreview(selectedResource);
            }
            setStep('explore');
          }}
          disabled={!selectedResource}
          className="btn-primary disabled:opacity-50"
        >
          Continue
          <ChevronRight className="w-4 h-4 ml-2 inline" />
        </button>
      </div>

      {/* Preview Modal */}
      <Modal
        isOpen={showPreview}
        onClose={() => setShowPreview(false)}
        title={`Preview: ${selectedResource?.name}`}
        size="lg"
      >
        {previewLoading ? (
          <div className="flex items-center justify-center p-8">
            <RefreshCw className="w-6 h-6 text-primary-500 animate-spin mr-3" />
            <span className="text-gray-500">Loading preview...</span>
          </div>
        ) : previewData ? (
          <div>
            <div className="mb-3 text-sm text-gray-500">
              Showing {previewData.preview_count} of {previewData.total_rows.toLocaleString()} rows
            </div>
            <div className="overflow-x-auto max-h-96">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50 sticky top-0">
                  <tr>
                    {previewData.columns.map((col) => (
                      <th
                        key={col.name}
                        className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase"
                      >
                        <div>{col.name}</div>
                        <div className="text-gray-300 font-normal">{col.type}</div>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {previewData.rows.map((row, idx) => (
                    <tr key={idx} className="hover:bg-gray-50">
                      {previewData.columns.map((col) => (
                        <td key={col.name} className="px-4 py-2 text-sm text-gray-600 whitespace-nowrap max-w-xs truncate">
                          {row[col.name] != null ? String(row[col.name]) : <span className="text-gray-300 italic">null</span>}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ) : (
          <div className="text-center p-8 text-gray-500">No preview available</div>
        )}
      </Modal>
    </div>
  );

  const renderExploreStep = () => (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-gray-900">Data Exploration & Slicing</h2>
        <p className="text-gray-500 mt-1">Review your dataset, apply filters, or build a pivot table</p>
      </div>

      <DataExplorer
        resource={selectedResource}
        previewData={previewData}
        sliceFilters={sliceFilters}
        setSliceFilters={setSliceFilters}
        discoveryMetadata={discoveryMetadata}
        discoveryLoading={discoveryLoading}
        dataSourceId={selectedDataSource}
        onViewCompleteData={() => setShowFullDataModal(true)}
      />

      {/* ── Full Data Viewer Modal ── */}
      {showFullDataModal && previewData && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
          <div className="bg-white rounded-xl shadow-2xl w-[90vw] max-h-[85vh] flex flex-col">
            <div className="flex items-center justify-between p-4 border-b">
              <div>
                <h3 className="text-lg font-semibold text-gray-900">Complete Data View</h3>
                <p className="text-sm text-gray-500">
                  {previewData.total_rows.toLocaleString()} rows × {previewData.columns.length} columns
                  {previewData.rows.length < previewData.total_rows &&
                    ` (showing ${previewData.rows.length.toLocaleString()} loaded)`}
                </p>
              </div>
              <button
                onClick={() => setShowFullDataModal(false)}
                className="p-2 rounded-lg hover:bg-gray-100 text-gray-500"
              >
                ✕
              </button>
            </div>
            <div className="flex-1 overflow-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50 sticky top-0">
                  <tr>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-400">#</th>
                    {previewData.columns.map((col) => (
                      <th key={col.name} className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">
                        {col.name}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {previewData.rows
                    .slice(fullDataPage * FULL_DATA_PAGE_SIZE, (fullDataPage + 1) * FULL_DATA_PAGE_SIZE)
                    .map((row, idx) => (
                      <tr key={idx} className="hover:bg-gray-50">
                        <td className="px-3 py-1.5 text-xs text-gray-400">
                          {fullDataPage * FULL_DATA_PAGE_SIZE + idx + 1}
                        </td>
                        {previewData.columns.map((col) => (
                          <td key={col.name} className="px-3 py-1.5 text-sm text-gray-800 max-w-[200px] truncate">
                            {row[col.name] != null ? String(row[col.name]) : <span className="text-gray-300 italic">null</span>}
                          </td>
                        ))}
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
            <div className="flex items-center justify-between p-3 border-t bg-gray-50">
              <span className="text-sm text-gray-500">
                Page {fullDataPage + 1} of {Math.ceil(previewData.rows.length / FULL_DATA_PAGE_SIZE)}
              </span>
              <div className="flex gap-2">
                <button
                  onClick={() => setFullDataPage(p => Math.max(0, p - 1))}
                  disabled={fullDataPage === 0}
                  className="px-3 py-1.5 text-sm border rounded-md disabled:opacity-40 hover:bg-gray-100"
                >
                  ← Previous
                </button>
                <button
                  onClick={() => setFullDataPage(p => Math.min(Math.ceil(previewData.rows.length / FULL_DATA_PAGE_SIZE) - 1, p + 1))}
                  disabled={(fullDataPage + 1) * FULL_DATA_PAGE_SIZE >= previewData.rows.length}
                  className="px-3 py-1.5 text-sm border rounded-md disabled:opacity-40 hover:bg-gray-100"
                >
                  Next →
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      <div className="flex justify-between">
        <button onClick={() => setStep('browse')} className="btn-secondary">
          <ChevronLeft className="w-4 h-4 mr-2 inline" />
          Back
        </button>
        <button onClick={() => setStep('config')} className="btn-primary">
          Continue to Configuration
          <ChevronRight className="w-4 h-4 ml-2 inline" />
        </button>
      </div>
    </div>
  );

  const renderConfigStep = () => (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-gray-900">Validation Configuration</h2>
        <p className="text-gray-500 mt-1">Choose how to validate your data</p>
      </div>

      {/* Selected Resource Summary */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            {getResourceIcon(selectedResource?.type || 'table')}
            <div>
              <p className="font-medium text-blue-900">{selectedResource?.name}</p>
              <p className="text-sm text-blue-700">
                {selectedResource?.rowCount != null && `${selectedResource.rowCount.toLocaleString()} rows`}
                {selectedResource?.columnCount != null && `, ${selectedResource.columnCount} columns`}
                {selectedResource?.size && ` • ${selectedResource.size}`}
              </p>
            </div>
          </div>
          <button
            onClick={() => {
              if (selectedResource) {
                loadPreview(selectedResource);
                setShowPreview(true);
              }
            }}
            className="text-sm text-blue-600 hover:text-blue-700 font-medium flex items-center space-x-1"
          >
            <Eye className="w-4 h-4" />
            <span>Preview Data</span>
          </button>
        </div>
      </div>

      {/* Validation Mode */}
      <div>
        <label className="form-label mb-3">Validation Mode</label>
        <div className="space-y-3">
          {VALIDATION_MODES.map((mode) => (
            <label
              key={mode.id}
              className={`flex items-start p-4 border-2 rounded-xl cursor-pointer transition-all ${validationMode === mode.id
                ? 'border-primary-500 bg-primary-50'
                : 'border-gray-200 hover:border-gray-300'
                }`}
            >
              <input
                type="radio"
                name="validationMode"
                value={mode.id}
                checked={validationMode === mode.id}
                onChange={(e) => setValidationMode(e.target.value)}
                className="mt-1"
              />
              <div className="ml-3 flex-1">
                <div className="flex items-center space-x-2">
                  <mode.icon
                    className={`w-5 h-5 ${validationMode === mode.id ? 'text-primary-600' : 'text-gray-400'
                      }`}
                  />
                  <span className="font-medium text-gray-900">{mode.name}</span>
                </div>
                <p className="text-sm text-gray-500 mt-1">{mode.description}</p>
              </div>
            </label>
          ))}
        </div>
      </div>

      {/* Sample Size */}
      <div>
        <label className="form-label">Sample Size</label>
        <div className="flex items-center space-x-4">
          <input
            type="range"
            min={100}
            max={10000}
            step={100}
            value={sampleSize}
            onChange={(e) => setSampleSize(Number(e.target.value))}
            className="flex-1"
          />
          <span className="w-20 text-right font-medium">{sampleSize.toLocaleString()}</span>
        </div>
        <p className="text-sm text-gray-500 mt-1">
          Number of records to sample for validation
        </p>
      </div>

      {/* Rule Group Selector — optional, shown for custom/hybrid modes */}
      {['custom_rules', 'hybrid'].includes(validationMode) && ruleGroups.length > 0 && (
        <div>
          <label className="form-label">Apply Rule Group <span className="text-gray-400 font-normal">(optional)</span></label>
          <div className="grid grid-cols-2 gap-3 mt-2">
            <label
              className={`flex items-center p-3 border-2 rounded-lg cursor-pointer transition-all ${!selectedRuleGroup ? 'border-primary-500 bg-primary-50' : 'border-gray-200 hover:border-gray-300'
                }`}
            >
              <input
                type="radio" name="ruleGroup" value=""
                checked={!selectedRuleGroup}
                onChange={() => setSelectedRuleGroup('')}
              />
              <span className="ml-2 text-sm font-medium text-gray-700">None — use only inline rules</span>
            </label>
            {ruleGroups.filter(g => g.is_active).map(g => (
              <label
                key={g.id}
                className={`flex items-start p-3 border-2 rounded-lg cursor-pointer transition-all ${selectedRuleGroup === g.id ? 'border-primary-500 bg-primary-50' : 'border-gray-200 hover:border-gray-300'
                  }`}
              >
                <input
                  type="radio" name="ruleGroup" value={g.id}
                  checked={selectedRuleGroup === g.id}
                  onChange={() => setSelectedRuleGroup(g.id)}
                />
                <div className="ml-2">
                  <span className="text-sm font-medium text-gray-800">{g.name}</span>
                  <span className="text-xs text-gray-500 ml-2">({g.rule_count} rules)</span>
                  {g.description && <p className="text-xs text-gray-400 mt-0.5">{g.description}</p>}
                </div>
              </label>
            ))}
          </div>
        </div>
      )}

      {/* Custom Rules Builder */}
      {['custom_rules', 'hybrid'].includes(validationMode) && (
        <div className="bg-white border rounded-lg p-4">
          <label className="form-label text-gray-900 flex items-center mb-1">
            Custom Validation Rules
          </label>
          <p className="text-sm text-gray-500 mb-4">
            Add specific SQL-like checks based on the columns.
          </p>

          {customRules.length > 0 && (
            <div className="flex flex-col gap-2 mb-4">
              {customRules.map((rule, idx) => (
                <div key={idx} className="flex items-center justify-between p-2 bg-gray-50 rounded border">
                  <div>
                    <span className="font-medium text-sm text-gray-800">{rule.name}</span>
                    <span className="text-xs text-gray-500 ml-2">
                      (col: {rule.target_columns?.join(', ')}, expr: {rule.expression})
                    </span>
                  </div>
                  <button
                    type="button"
                    className="text-red-500 hover:text-red-700 font-bold"
                    onClick={() => {
                      const next = [...customRules];
                      next.splice(idx, 1);
                      setCustomRules(next);
                    }}
                  >
                    &times;
                  </button>
                </div>
              ))}
            </div>
          )}

          <div className="flex space-x-2 items-center">
            {selectedResource?.columns ? (
              <select
                className="flex-1 border-gray-300 rounded-lg shadow-sm focus:ring-primary-500 py-2 px-3 text-sm"
                value={newRuleColumn}
                title="Column"
                onChange={(e) => setNewRuleColumn(e.target.value)}
              >
                <option value="">Select Column...</option>
                {selectedResource.columns.map((c) => (
                  <option key={c.name} value={c.name}>{c.name}</option>
                ))}
              </select>
            ) : (
              <input
                type="text"
                placeholder="Column name"
                className="flex-1 border-gray-300 rounded-lg shadow-sm focus:ring-primary-500 py-2 px-3 text-sm"
                value={newRuleColumn}
                onChange={(e) => setNewRuleColumn(e.target.value)}
              />
            )}

            <select
              title="Operator"
              className="w-28 border-gray-300 rounded-lg shadow-sm focus:ring-primary-500 py-2 px-3 text-sm"
              value={newRuleOperator}
              onChange={(e) => setNewRuleOperator(e.target.value)}
            >
              <option value="=">=</option>
              <option value=">">&gt;</option>
              <option value="<">&lt;</option>
              <option value=">=">&gt;=</option>
              <option value="<=">&lt;=</option>
              <option value="!=">!=</option>
              <option value="IS NULL">IS NULL</option>
              <option value="IS NOT NULL">IS NOT NULL</option>
            </select>

            {!['IS NULL', 'IS NOT NULL'].includes(newRuleOperator) && (
              <input
                type="text"
                placeholder="Value"
                className="flex-1 border-gray-300 rounded-lg shadow-sm focus:ring-primary-500 py-2 px-3 text-sm"
                value={newRuleValue}
                onChange={(e) => setNewRuleValue(e.target.value)}
              />
            )}

            <button
              type="button"
              className="btn-secondary px-4 py-2 text-sm whitespace-nowrap"
              disabled={!newRuleColumn || (!newRuleValue && !['IS NULL', 'IS NOT NULL'].includes(newRuleOperator))}
              onClick={() => {
                const expr = ['IS NULL', 'IS NOT NULL'].includes(newRuleOperator)
                  ? `${newRuleColumn} ${newRuleOperator}`
                  : `${newRuleColumn} ${newRuleOperator} ${newRuleValue}`;

                const safeName = `Check_${newRuleColumn}_${newRuleOperator.replace(/\s+/g, '_')}`;

                setCustomRules([
                  ...customRules,
                  {
                    name: safeName,
                    rule_type: "custom_sql",
                    severity: "high",
                    target_columns: [newRuleColumn],
                    rule_config: {},
                    expression: expr
                  }
                ]);
                setNewRuleColumn('');
                setNewRuleValue('');
              }}
            >
              Add Rule
            </button>
          </div>
        </div>
      )}



      {/* LLM Status */}
      <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
        <div className="flex items-center space-x-3">
          <Brain className="w-5 h-5 text-gray-400" />
          <div>
            <p className="text-sm font-medium text-gray-900">AI Model</p>
            <p className="text-sm text-gray-500">
              {llmHealth?.provider || 'Unknown'} ({llmHealth?.model || 'N/A'})
            </p>
          </div>
        </div>
        <div className="flex items-center space-x-2">
          <div
            className={`w-2 h-2 rounded-full ${llmHealth?.status === 'healthy' ? 'bg-green-500' : 'bg-red-500'
              }`}
          />
          <span
            className={`text-sm ${llmHealth?.status === 'healthy' ? 'text-green-600' : 'text-red-600'
              }`}
          >
            {llmHealth?.status === 'healthy' ? 'Connected' : 'Disconnected'}
          </span>
        </div>
      </div>

      <div className="flex justify-between">
        <button onClick={() => setStep('explore')} className="btn-secondary">
          <ChevronLeft className="w-4 h-4 mr-2 inline" />
          Back
        </button>
        <button onClick={() => setStep('review')} className="btn-primary">
          Review
          <ChevronRight className="w-4 h-4 ml-2 inline" />
        </button>
      </div>

      {/* Preview Modal */}
      <Modal
        isOpen={showPreview}
        onClose={() => setShowPreview(false)}
        title={`Preview: ${selectedResource?.name}`}
        size="lg"
      >
        {previewLoading ? (
          <div className="flex items-center justify-center p-8">
            <RefreshCw className="w-6 h-6 text-primary-500 animate-spin mr-3" />
            <span className="text-gray-500">Loading preview...</span>
          </div>
        ) : previewData ? (
          <div className="overflow-x-auto max-h-96">
            <div className="mb-3 text-sm text-gray-500">
              Showing {previewData.preview_count} of {previewData.total_rows.toLocaleString()} rows
            </div>
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50 sticky top-0">
                <tr>
                  {previewData.columns.map((col) => (
                    <th key={col.name} className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">
                      {col.name}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {previewData.rows.map((row, idx) => (
                  <tr key={idx}>
                    {previewData.columns.map((col) => (
                      <td key={col.name} className="px-4 py-2 text-sm text-gray-600">
                        {row[col.name] != null ? String(row[col.name]) : '—'}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="text-center p-8 text-gray-500">No preview available</div>
        )}
      </Modal>
    </div>
  );

  const renderReviewStep = () => (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-gray-900">Review & Start</h2>
        <p className="text-gray-500 mt-1">Confirm your validation settings</p>
      </div>

      <div className="card divide-y divide-gray-200">
        <div className="p-4 flex items-center justify-between">
          <div>
            <p className="text-sm text-gray-500">Data Source</p>
            <p className="font-medium text-gray-900">{getSourceLabel()}</p>
          </div>
          <button
            onClick={() => setStep('source')}
            className="text-sm text-primary-600 hover:text-primary-700"
          >
            Change
          </button>
        </div>

        <div className="p-4 flex items-center justify-between">
          <div>
            <p className="text-sm text-gray-500">Resource</p>
            <p className="font-medium text-gray-900">{selectedResource?.name}</p>
            <p className="text-xs text-gray-400">{selectedResource?.path}</p>
          </div>
          <button
            onClick={() => setStep('browse')}
            className="text-sm text-primary-600 hover:text-primary-700"
          >
            Change
          </button>
        </div>

        <div className="p-4 flex items-center justify-between">
          <div>
            <p className="text-sm text-gray-500">Validation Mode</p>
            <p className="font-medium text-gray-900">
              {VALIDATION_MODES.find((m) => m.id === validationMode)?.name}
            </p>
          </div>
          <button
            onClick={() => setStep('config')}
            className="text-sm text-primary-600 hover:text-primary-700"
          >
            Change
          </button>
        </div>

        <div className="p-4 flex items-center justify-between">
          <div>
            <p className="text-sm text-gray-500">Sample Size</p>
            <p className="font-medium text-gray-900">{sampleSize.toLocaleString()} records</p>
          </div>
          <button
            onClick={() => setStep('config')}
            className="text-sm text-primary-600 hover:text-primary-700"
          >
            Change
          </button>
        </div>

        {sliceFilters.length > 0 && (
          <div className="p-4 flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500">Active Slice Filters</p>
              <div className="flex flex-wrap gap-2 mt-1">
                {sliceFilters.map((f: any, i: number) => (
                  <span key={i} className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800">
                    {f.column}: {f.filter_type}
                    {f.selected_values ? ` (${f.selected_values.length})` : ''}
                    {f.min_value != null || f.max_value != null ? ` [${f.min_value ?? ''}–${f.max_value ?? ''}]` : ''}
                    {f.text_pattern ? ` "${f.text_pattern}"` : ''}
                  </span>
                ))}
              </div>
            </div>
            <button
              onClick={() => setStep('explore')}
              className="text-sm text-primary-600 hover:text-primary-700"
            >
              Change
            </button>
          </div>
        )}

        <div className="p-4 flex items-center justify-between">
          <div>
            <p className="text-sm text-gray-500">AI Model</p>
            <p className="font-medium text-gray-900">
              {llmHealth?.provider || 'Unknown'} ({llmHealth?.model || 'N/A'})
            </p>
          </div>
        </div>
      </div>

      <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
        <div className="flex items-start space-x-3">
          <AlertCircle className="w-5 h-5 text-yellow-600 mt-0.5" />
          <div>
            <p className="font-medium text-yellow-900">What happens next?</p>
            <ul className="text-sm text-yellow-700 mt-1 space-y-1">
              <li>• Data will be profiled and analyzed</li>
              <li>• AI will generate validation rules (if selected)</li>
              <li>• All rules will be executed against the data</li>
              <li>• A detailed report will be generated</li>
            </ul>
          </div>
        </div>
      </div>

      <div className="flex justify-between">
        <button onClick={() => setStep('config')} className="btn-secondary">
          <ChevronLeft className="w-4 h-4 mr-2 inline" />
          Back
        </button>
        <button
          onClick={handleSubmit}
          disabled={isLoading}
          className="btn-primary disabled:opacity-50"
        >
          {isLoading ? (
            <>
              <RefreshCw className="w-4 h-4 mr-2 inline animate-spin" />
              Starting...
            </>
          ) : (
            <>
              <Play className="w-4 h-4 mr-2 inline" />
              Start Validation
            </>
          )}
        </button>
      </div>
    </div>
  );

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">New Validation</h1>
          <p className="text-gray-500 mt-1">Run a data quality validation</p>
        </div>
      </div>

      {renderStepIndicator()}

      <div className="max-w-4xl mx-auto">
        {step === 'source' && renderSourceStep()}
        {step === 'browse' && renderBrowseStep()}
        {step === 'explore' && renderExploreStep()}
        {step === 'config' && renderConfigStep()}
        {step === 'review' && renderReviewStep()}
      </div>
    </div>
  );
}
