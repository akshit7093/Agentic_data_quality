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
  X,
} from 'lucide-react';
import { useDataSources } from '@/hooks/useDataSources';
import { useSubmitValidation } from '@/hooks/useValidations';
import { useLLMHealth } from '@/hooks/useSystem';
import { dataSourceApi, fileApi, ruleGroupApi } from '@/services/api';
import Modal from '@/components/Modal';
import DataExplorer from '@/components/DataExplorer';
import VisualPrep from '@/components/VisualPrep';
import DetailedDataView from '@/components/DetailedDataView';


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

type ValidationStep = 'source' | 'browse' | 'view_data' | 'prep' | 'explore' | 'config' | 'review';

export default function NewValidation() {
  const navigate = useNavigate();
  const { data: dataSources } = useDataSources();
  const submitValidation = useSubmitValidation();
  const { data: llmHealth } = useLLMHealth();

  const [step, setStep] = useState<ValidationStep>('source');
  const [selectedDataSource, setSelectedDataSource] = useState<string>('');
  const [selectedResource, setSelectedResource] = useState<DataResource | null>(null);
  const [columnMapping, setColumnMapping] = useState<Record<string, string>>({});
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);

  const [expandedResources, setExpandedResources] = useState<Set<string>>(new Set());
  const [searchQuery, setSearchQuery] = useState('');
  const [validationMode, setValidationMode] = useState('hybrid');
  const [sampleSize, setSampleSize] = useState(1000);
  const [sliceFilters, setSliceFilters] = useState<any[]>([]);

  // Discovery metadata state
  const [discoveryMetadata, setDiscoveryMetadata] = useState<any>(null);
  const [discoveryLoading, setDiscoveryLoading] = useState(false);

  // Template session state (set when user confirms a template mapping in VisualPrep)
  const [templateSessionId, setTemplateSessionId] = useState<string | null>(null);
  const [templateSessionInfo, setTemplateSessionInfo] = useState<any>(null);

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

  // Persistent component states
  const [prepState, setPrepState] = useState<any>(null);
  const [explorerState, setExplorerState] = useState<any>({
    filterDraft: {},
    pivotConfig: {
      dimension: '',
      dimension2: '',
      measure: '',
      agg: 'count',
      result: null,
      chart: null,
    }
  });

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
      loadDiscovery(templateSessionId);
    }
  }, [step, selectedResource, selectedDataSource]);

  const loadDiscovery = async (sessionId?: string | null) => {
    if (!selectedResource || !selectedDataSource) return;
    setDiscoveryLoading(true);
    try {
      const sid = sessionId !== undefined ? sessionId : templateSessionId;
      const url = `/api/v1/datasources/${selectedDataSource}/discover-filters?resource_path=${encodeURIComponent(selectedResource.path)}${sid ? `&template_session_id=${sid}` : ''}`;
      const res = await fetch(url, { method: 'POST' });
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
        column_mapping: columnMapping,
        selected_columns: selectedColumns,
        session_id: templateSessionId,
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
        { id: 'source', label: 'Source', icon: Database },
        { id: 'browse', label: 'Browse', icon: Folder },
        { id: 'view_data', label: 'View Data', icon: Eye },
        { id: 'prep', label: 'Prep', icon: Table },
        { id: 'explore', label: 'Explore', icon: Database },
        { id: 'config', label: 'Configure', icon: Settings },
        { id: 'review', label: 'Review', icon: CheckCircle },
      ].map((s, idx) => {
        const isActive = step === s.id;
        const isCompleted =
          (step === 'browse' && s.id === 'source') ||
          (step === 'view_data' && ['source', 'browse'].includes(s.id)) ||
          (step === 'prep' && ['source', 'browse', 'view_data'].includes(s.id)) ||
          (step === 'explore' && ['source', 'browse', 'view_data', 'prep'].includes(s.id)) ||
          (step === 'config' && ['source', 'browse', 'view_data', 'prep', 'explore'].includes(s.id)) ||
          (step === 'review' && ['source', 'browse', 'view_data', 'prep', 'explore', 'config'].includes(s.id));

        return (
          <div key={s.id} className="flex items-center">
            <button
              onClick={() => (isActive || isCompleted) && setStep(s.id as ValidationStep)}
              disabled={!isActive && !isCompleted}
              className={`flex items-center space-x-2 px-4 py-2 rounded-lg transition-all ${isActive
                ? 'bg-primary-100 text-primary-700 ring-2 ring-primary/20'
                : isCompleted
                  ? 'text-success-600 hover:bg-success-50 cursor-pointer'
                  : 'text-gray-400 cursor-not-allowed'
                }`}
              title={isCompleted ? `Go back to ${s.label}` : s.label}
            >
              <s.icon className="w-5 h-5" />
              <span className="font-medium text-xs">{s.label}</span>
            </button>
            {idx < 6 && <ChevronRight className="w-5 h-5 text-gray-300 mx-1" />}
          </div>
        );
      })}
    </div>
  );

  const renderSourceStep = () => (
    <div className="space-y-6">
      <div className="text-center">
        <h2 className="text-xl font-semibold text-slate-100 uppercase tracking-wide">Select Data Source</h2>
        <p className="text-slate-400 mt-1">Choose where your data is located</p>
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
            ? 'border-primary bg-primary/10'
            : 'border-royal-green-600 bg-royal-green-900/50 hover:border-royal-green-500 hover:bg-royal-green-800'
            }`}
        >
          <div className="flex items-start space-x-4">
            <div className="p-3 bg-blue-500/20 rounded-lg">
              <Database className="w-6 h-6 text-blue-400" />
            </div>
            <div>
              <h3 className="font-bold text-slate-100 uppercase tracking-tighter">Test Database</h3>
              <p className="text-sm text-slate-400 mt-1">
                Local SQLite database with sample data
              </p>
              <div className="flex items-center space-x-4 mt-3 text-sm text-slate-500 font-mono">
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
            ? 'border-primary bg-primary/10'
            : 'border-royal-green-600 bg-royal-green-900/50 hover:border-royal-green-500 hover:bg-royal-green-800'
            }`}
        >
          <div className="flex items-start space-x-4">
            <div className="p-3 bg-emerald-500/20 rounded-lg">
              <FileText className="w-6 h-6 text-emerald-400" />
            </div>
            <div>
              <h3 className="font-bold text-slate-100 uppercase tracking-tighter">Upload File</h3>
              <p className="text-sm text-slate-400 mt-1">
                CSV, Excel, Parquet, or JSON files
              </p>
              <div className="flex items-center space-x-4 mt-3 text-sm text-slate-500 font-mono">
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
            ? 'border-primary bg-primary/10'
            : 'border-royal-green-600 bg-royal-green-900/50 hover:border-royal-green-500 hover:bg-royal-green-800'
            }`}
        >
          <div className="flex items-start space-x-4">
            <div className="p-3 bg-purple-500/20 rounded-lg">
              <Folder className="w-6 h-6 text-purple-400" />
            </div>
            <div>
              <h3 className="font-bold text-slate-100 uppercase tracking-tighter">ADLS Gen2 Mock</h3>
              <p className="text-sm text-slate-400 mt-1">
                Azure Data Lake Storage Gen2 test structure
              </p>
              <div className="flex items-center space-x-4 mt-3 text-sm text-slate-500 font-mono">
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
            ? 'border-primary bg-primary/10'
            : 'border-royal-green-600 bg-royal-green-900/50 hover:border-royal-green-500 hover:bg-royal-green-800'
            }`}
        >
          <div className="flex items-start space-x-4">
            <div className="p-3 bg-orange-500/20 rounded-lg">
              <Folder className="w-6 h-6 text-orange-400" />
            </div>
            <div>
              <h3 className="font-bold text-slate-100 uppercase tracking-tighter">Local Test Files</h3>
              <p className="text-sm text-slate-400 mt-1">
                Structured, semi-structured & unstructured test data
              </p>
              <div className="flex items-center space-x-4 mt-3 text-sm text-slate-500 font-mono">
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
                ? 'border-primary bg-primary/10'
                : 'border-royal-green-600 bg-royal-green-900/50 hover:border-royal-green-500 hover:bg-royal-green-800'
                }`}
            >
              <div className="flex items-start space-x-4">
                <div className="p-3 bg-royal-green-800 rounded-lg">
                  <Database className="w-6 h-6 text-royal-green-300" />
                </div>
                <div>
                  <h3 className="font-bold text-slate-100 uppercase tracking-tighter">{ds.name}</h3>
                  <p className="text-sm text-slate-400 mt-1 capitalize">{ds.source_type}</p>
                  <div className="flex items-center space-x-2 mt-3">
                    <span
                      className={`w-2 h-2 rounded-full ${ds.is_active ? 'bg-primary shadow-[0_0_8px_theme(colors.primary)]' : 'bg-slate-600'
                        }`}
                    />
                    <span className="text-xs font-bold text-slate-300 uppercase">
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
          <h2 className="text-xl font-semibold text-slate-100 uppercase tracking-wide">Browse Resources</h2>
          <p className="text-slate-400 mt-1">Select a table or file to validate</p>
        </div>
        <div className="flex items-center space-x-2">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-slate-500" />
            <input
              type="text"
              placeholder="Search..."
              className="pl-10 pr-4 py-2 bg-royal-green-900 border border-royal-green-600 rounded-lg focus:ring-primary focus:border-primary text-slate-100 text-sm"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </div>
          <button
            onClick={loadResources}
            className="p-2 bg-royal-green-900 border border-royal-green-600 rounded-lg hover:bg-royal-green-800 transition-colors"
            title="Refresh"
          >
            <RefreshCw className={`w-5 h-5 text-slate-400 ${resourcesLoading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* File Upload Section (only for file-upload source) */}
      {selectedDataSource === 'file-upload' && (
        <div className="bg-emerald-500/5 border-2 border-dashed border-emerald-500/30 rounded-xl p-6">
          <div className="text-center">
            <Upload className="w-10 h-10 text-emerald-500 mx-auto mb-3" />
            <p className="font-bold text-slate-100 mb-1">Upload a Data File</p>
            <p className="text-sm text-slate-300 mb-4 font-mono">CSV, Excel, Parquet, or JSON (max 100MB)</p>
            <div className="flex items-center justify-center space-x-3">
              <input
                type="file"
                accept=".csv,.xlsx,.xls,.parquet,.json,.jsonl"
                onChange={(e) => setUploadFile(e.target.files?.[0] || null)}
                className="text-xs text-slate-400 file:mr-4 file:py-2 file:px-4 file:rounded-full file:border-0 file:text-xs file:font-bold file:bg-primary file:text-white hover:file:bg-primary/90"
              />
              {uploadFile && (
                <button
                  onClick={handleFileUpload}
                  disabled={uploading}
                  className="btn-primary text-xs px-4 py-2 disabled:opacity-50"
                >
                  {uploading ? (
                    <><RefreshCw className="w-3 h-3 mr-1 inline animate-spin" /> Uploading...</>
                  ) : (
                    <><Upload className="w-3 h-3 mr-1 inline" /> Confirm & Upload</>
                  )}
                </button>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Breadcrumb */}
      <div className="card overflow-hidden !shadow-none">
        <div className="bg-royal-green-800/50 px-4 py-3 border-b border-royal-green-600">
          <div className="flex items-center space-x-2 text-sm text-slate-100 font-bold uppercase tracking-widest">
            {getResourceIcon(selectedDataSource === 'local-test' ? 'table' : 'folder')}
            <span>{getSourceLabel()}</span>
            <ChevronRight className="w-4 h-4 text-slate-400" />
            <span className="text-primary">{selectedDataSource === 'local-test' ? 'Tables' : 'Files'}</span>
          </div>
        </div>

        {/* Loading State */}
        {resourcesLoading && (
          <div className="flex items-center justify-center p-12">
            <div className="flex flex-col items-center">
              <RefreshCw className="w-8 h-8 text-primary animate-spin mb-4" />
              <span className="text-slate-200 text-sm font-black tracking-widest uppercase">Discovering Resources...</span>
            </div>
          </div>
        )}

        {/* Empty State */}
        {!resourcesLoading && filteredResources.length === 0 && (
          <div className="text-center p-12 text-slate-500">
            <Folder className="w-12 h-12 mx-auto mb-4 opacity-20" />
            <p className="font-bold text-slate-400 uppercase">No resources found</p>
            <p className="text-sm mt-1">
              {selectedDataSource === 'file-upload'
                ? 'Upload a file to get started'
                : 'No compatible tables or files found in this source'}
            </p>
          </div>
        )}

        {/* Resource List */}
        <div className="divide-y divide-royal-green-600 max-h-96 overflow-y-auto">
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
                className={`flex items-center justify-between p-4 cursor-pointer transition-colors ${selectedResource?.path === resource.path ? 'bg-primary/10' : 'hover:bg-royal-green-800/30'
                  }`}
              >
                <div className="flex items-center space-x-4">
                  <div className="p-2 bg-royal-green-800 rounded">
                    {getResourceIcon(resource.type)}
                  </div>
                  <div>
                    <span className="font-bold text-slate-100 uppercase tracking-tight">{resource.name}</span>
                    <div className="flex items-center space-x-3 mt-0.5">
                      {resource.rowCount != null && (
                        <span className="text-[10px] font-mono text-slate-500 uppercase">
                          {resource.rowCount.toLocaleString()} rows
                        </span>
                      )}
                      {resource.columnCount != null && (
                        <span className="text-[10px] font-mono text-slate-500 uppercase">
                          {resource.columnCount} cols
                        </span>
                      )}
                      {resource.format && (
                        <span className="text-[10px] font-bold bg-royal-green-800 text-royal-green-300 px-1.5 py-0.5 rounded border border-royal-green-600">
                          {resource.format.toUpperCase()}
                        </span>
                      )}
                    </div>
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
                      className="p-2 bg-royal-green-800/50 hover:bg-primary/20 rounded-lg text-slate-400 hover:text-primary transition-all"
                      title="Quick Preview"
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
                      className="p-2 hover:bg-royal-green-700 rounded-lg"
                    >
                      {expandedResources.has(resource.path) ? (
                        <ChevronDown className="w-4 h-4 text-slate-400" />
                      ) : (
                        <ChevronRight className="w-4 h-4 text-slate-400" />
                      )}
                    </button>
                  )}
                  {selectedResource?.path === resource.path && (
                    <CheckCircle className="w-5 h-5 text-primary shadow-[0_0_10px_theme(colors.primary)]" />
                  )}
                </div>
              </div>

              {/* Column Preview */}
              {expandedResources.has(resource.path) && resource.columns && (
                <div className="pl-16 pr-4 pb-4">
                  <div className="bg-royal-green-900 border border-royal-green-700 rounded-lg p-4">
                    <p className="text-[10px] font-black text-slate-500 mb-3 uppercase tracking-widest border-b border-royal-green-700 pb-2">Schema Definition</p>
                    <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
                      {resource.columns.map((col) => (
                        <div
                          key={col.name}
                          className="flex items-center justify-between p-2 bg-royal-green-800/30 rounded border border-royal-green-700/50"
                        >
                          <span className="text-xs font-medium text-slate-300 truncate pr-2">{col.name}</span>
                          <span className="text-[9px] font-mono text-slate-500 uppercase">{col.type}</span>
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
              // Also trigger discovery early if possible
              loadDiscovery(templateSessionId);
            }
            setStep('view_data');
          }}
          disabled={!selectedResource}
          className="btn-primary disabled:opacity-50"
        >
          Continue to View Data
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
  const renderViewDataStep = () => (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-slate-100 uppercase tracking-widest">Data Inspection</h2>
          <p className="text-slate-400 mt-1 italic">Review the full dataset and column-level profiling metrics</p>
        </div>
        <div className="flex gap-2">
          <div className="flex items-center px-3 py-1 bg-royal-green-800 border border-royal-green-600 rounded-lg">
            <span className="text-[10px] font-black text-slate-500 uppercase mr-2">Resource:</span>
            <span className="text-xs font-mono font-bold text-primary truncate max-w-[200px]">{selectedResource?.name}</span>
          </div>
        </div>
      </div>

      <div className="h-[600px]">
        {previewData ? (
          <DetailedDataView
            previewData={previewData}
            discoveryMetadata={discoveryMetadata}
            pageSize={FULL_DATA_PAGE_SIZE}
          />
        ) : (
          <div className="flex flex-col items-center justify-center h-full bg-royal-green-900 border border-royal-green-700 rounded-2xl">
            <RefreshCw className="w-12 h-12 text-primary/20 animate-spin mb-4" />
            <p className="text-slate-500 font-bold uppercase tracking-widest">Loading inspection engine...</p>
          </div>
        )}
      </div>

      <div className="flex justify-between">
        <button onClick={() => setStep('browse')} className="btn-secondary">
          <ChevronLeft className="w-4 h-4 mr-2 inline" />
          Back to Browse
        </button>
        <button onClick={() => setStep('prep')} className="btn-primary">
          Continue to Column Mapping
          <ChevronRight className="w-4 h-4 ml-2 inline" />
        </button>
      </div>
    </div>
  );


  const renderPrepStep = () => (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-slate-100 uppercase tracking-widest">Visual Data Preparation</h2>
        <p className="text-slate-400 mt-1">Map your columns to a template or rename them for analysis</p>
      </div>

      <VisualPrep
        fileColumns={selectedResource?.columns || []}
        dataSourceId={selectedDataSource}
        resourcePath={selectedResource?.path || ''}
        initialState={prepState}
        onComplete={(mapping, selected, sessionId, sessionInfo, fullState) => {
          setColumnMapping(mapping);
          setSelectedColumns(selected);
          setPrepState(fullState);
          if (sessionId) {
            setTemplateSessionId(sessionId);
            setTemplateSessionInfo(sessionInfo);
          }
          // Reset discovery so it re-runs with template session in explore step
          setDiscoveryMetadata(null);
          setStep('explore');
        }}
      />

      <div className="flex justify-between">
        <button onClick={() => setStep('view_data')} className="btn-secondary">
          <ChevronLeft className="w-4 h-4 mr-2 inline" />
          Back
        </button>
      </div>
    </div>
  );

  const renderExploreStep = () => (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-slate-100 uppercase tracking-widest">Data Exploration & Slicing</h2>
        <p className="text-slate-400 mt-1">Review your dataset, apply filters, or build a pivot table</p>
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
        templateSessionId={templateSessionId}
        templateSessionInfo={templateSessionInfo}
        initialState={explorerState}
        onStateChange={(state) => setExplorerState(state)}
      />

      {/* ── Full Data Viewer Modal ── */}
      {showFullDataModal && previewData && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-md">
          <div className="bg-royal-green-900 rounded-xl shadow-[0_0_50px_rgba(0,0,0,0.5)] border border-royal-green-600 w-[95vw] h-[90vh] flex flex-col overflow-hidden">
            <div className="flex items-center justify-between p-6 border-b border-royal-green-700 bg-royal-green-800">
              <div>
                <h3 className="text-xl font-black text-slate-100 uppercase tracking-tighter">Enterprise Data Explorer</h3>
                <p className="text-xs font-mono text-slate-400 mt-1 uppercase tracking-widest">
                  {previewData.total_rows.toLocaleString()} rows · {previewData.columns.length} columns
                  {previewData.rows.length < previewData.total_rows &&
                    ` · showing ${previewData.rows.length.toLocaleString()} records`}
                </p>
              </div>
              <button
                onClick={() => setShowFullDataModal(false)}
                className="p-2 rounded-lg hover:bg-royal-green-700 text-slate-400 hover:text-white transition-colors"
              >
                <X className="w-6 h-6" />
              </button>
            </div>
            <div className="flex-1 overflow-auto bg-black/20">
              <table className="min-w-full divide-y divide-royal-green-700">
                <thead className="bg-royal-green-800 sticky top-0 z-10">
                  <tr>
                    <th className="px-4 py-3 text-left text-[10px] font-black text-slate-500 uppercase">#</th>
                    {previewData.columns.map((col) => (
                      <th key={col.name} className="px-4 py-3 text-left text-xs font-black text-slate-300 uppercase tracking-wider">
                        <div className="flex flex-col">
                          <span>{col.name}</span>
                          <span className="text-[9px] font-mono text-slate-500 lowercase font-normal">{col.type}</span>
                        </div>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-royal-green-800 bg-royal-green-900">
                  {previewData.rows
                    .slice(fullDataPage * FULL_DATA_PAGE_SIZE, (fullDataPage + 1) * FULL_DATA_PAGE_SIZE)
                    .map((row, idx) => (
                      <tr key={idx} className="hover:bg-royal-green-800/50 transition-colors group">
                        <td className="px-4 py-2 text-[10px] font-mono text-slate-500 font-bold group-hover:text-primary">
                          {fullDataPage * FULL_DATA_PAGE_SIZE + idx + 1}
                        </td>
                        {previewData.columns.map((col) => (
                          <td key={col.name} className="px-4 py-2 text-sm text-slate-300 font-mono max-w-[300px] truncate">
                            {row[col.name] != null ? String(row[col.name]) : <span className="text-slate-700 italic">null</span>}
                          </td>
                        ))}
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
            <div className="flex items-center justify-between p-4 border-t border-royal-green-700 bg-royal-green-800/50">
              <span className="text-xs font-bold text-slate-500 uppercase tracking-widest">
                Viewing Page {fullDataPage + 1} of {Math.ceil(previewData.rows.length / FULL_DATA_PAGE_SIZE)}
              </span>
              <div className="flex gap-2">
                <button
                  onClick={() => setFullDataPage(p => Math.max(0, p - 1))}
                  disabled={fullDataPage === 0}
                  className="px-4 py-2 text-xs font-bold bg-royal-green-900 border border-royal-green-600 rounded-lg text-slate-300 disabled:opacity-20 hover:bg-royal-green-700 transition-all uppercase"
                >
                  ← Previous
                </button>
                <button
                  onClick={() => setFullDataPage(p => Math.min(Math.ceil(previewData.rows.length / FULL_DATA_PAGE_SIZE) - 1, p + 1))}
                  disabled={(fullDataPage + 1) * FULL_DATA_PAGE_SIZE >= previewData.rows.length}
                  className="px-4 py-2 text-xs font-bold bg-primary text-white rounded-lg disabled:opacity-20 hover:bg-primary/90 transition-all uppercase"
                >
                  Next Page →
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      <div className="flex justify-between">
        <button onClick={() => setStep('prep')} className="btn-secondary">
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
        <h2 className="text-xl font-semibold text-slate-100 uppercase tracking-widest">Validation Configuration</h2>
        <p className="text-slate-400 mt-1">Choose how to validate your data</p>
      </div>

      {/* Selected Resource Summary */}
      <div className="bg-primary/10 border border-primary/20 rounded-lg p-5 shadow-[0_0_20px_rgba(34,197,94,0.1)]">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <div className="p-3 bg-royal-green-800 rounded-lg shadow-inner">
              {getResourceIcon(selectedResource?.type || 'table')}
            </div>
            <div>
              <p className="font-black text-slate-100 uppercase tracking-tight text-lg">{selectedResource?.name}</p>
              <p className="text-xs font-mono text-slate-400 uppercase tracking-widest mt-1">
                {selectedResource?.rowCount != null && `${selectedResource.rowCount.toLocaleString()} rows`}
                {selectedResource?.columnCount != null && ` · ${selectedResource.columnCount} columns`}
                {selectedResource?.size && ` · ${selectedResource.size}`}
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
            className="btn-secondary text-xs px-4 py-2 flex items-center space-x-2 bg-royal-green-800/50"
          >
            <Eye className="w-4 h-4" />
            <span>Interactive Preview</span>
          </button>
        </div>
      </div>

      {/* Validation Mode */}
      <div>
        <label className="form-label mb-3 uppercase tracking-widest text-[10px] font-black text-slate-500">Validation Mode</label>
        <div className="space-y-3">
          {VALIDATION_MODES.map((mode) => (
            <label
              key={mode.id}
              className={`flex items-start p-4 border-2 rounded-xl cursor-pointer transition-all ${validationMode === mode.id
                ? 'border-primary bg-primary/10 shadow-[0_0_15px_rgba(34,197,94,0.1)]'
                : 'border-royal-green-700 bg-royal-green-900/50 hover:border-royal-green-600 hover:bg-royal-green-800/50'
                }`}
            >
              <input
                type="radio"
                name="validationMode"
                value={mode.id}
                checked={validationMode === mode.id}
                onChange={(e) => setValidationMode(e.target.value)}
                className="mt-1 text-primary focus:ring-primary bg-royal-green-900 border-royal-green-600"
              />
              <div className="ml-4 flex-1">
                <div className="flex items-center space-x-3">
                  <mode.icon
                    className={`w-5 h-5 ${validationMode === mode.id ? 'text-primary' : 'text-slate-500'
                      }`}
                  />
                  <span className={`font-bold uppercase tracking-tight ${validationMode === mode.id ? 'text-slate-100' : 'text-slate-400'}`}>{mode.name}</span>
                </div>
                <p className="text-xs text-slate-500 mt-1 font-medium italic">{mode.description}</p>
              </div>
            </label>
          ))}
        </div>
      </div>

      {/* Sample Size */}
      <div className="bg-royal-green-900/50 border border-royal-green-700 rounded-xl p-5">
        <label className="form-label uppercase tracking-widest text-[10px] font-black text-slate-500 mb-4 block">Execution Payload Size</label>
        <div className="flex items-center space-x-6">
          <input
            type="range"
            min={100}
            max={10000}
            step={100}
            value={sampleSize}
            onChange={(e) => setSampleSize(Number(e.target.value))}
            className="flex-1 accent-primary bg-royal-green-700 h-1.5 rounded-lg appearance-none cursor-pointer"
          />
          <div className="bg-royal-green-800 border border-royal-green-600 px-4 py-2 rounded-lg">
            <span className="text-lg font-black text-primary font-mono">{sampleSize.toLocaleString()}</span>
            <span className="text-[10px] font-bold text-slate-500 ml-2 uppercase">Records</span>
          </div>
        </div>
        <p className="text-[10px] font-medium text-slate-500 mt-3 uppercase tracking-wider italic">
          High sample sizes increase AI processing latency and token consumption.
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
        <div className="bg-royal-green-900 border border-royal-green-700 rounded-xl p-5 shadow-2xl">
          <label className="form-label text-slate-100 flex items-center mb-1 font-black uppercase tracking-tighter text-lg">
            Logic Engine Configuration
          </label>
          <p className="text-xs text-slate-500 mb-6 uppercase tracking-wider font-bold">
            Define deterministic SQL predicates for strict enforcement
          </p>

          {customRules.length > 0 && (
            <div className="flex flex-col gap-2 mb-6">
              {customRules.map((rule, idx) => (
                <div key={idx} className="flex items-center justify-between p-3 bg-royal-green-800 border border-royal-green-600 rounded-lg group hover:border-primary/50 transition-all">
                  <div>
                    <span className="font-bold text-sm text-slate-200 uppercase tracking-tight">{rule.name}</span>
                    <p className="text-[10px] text-primary font-mono mt-0.5">
                      EXPR: {rule.expression}
                    </p>
                  </div>
                  <button
                    type="button"
                    className="p-1 px-2 text-slate-500 hover:text-red-400 font-black text-lg transition-colors"
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

          <div className="flex flex-wrap md:flex-nowrap gap-3 items-end">
            <div className="flex-1 min-w-[200px]">
              <label className="text-[10px] font-black text-slate-500 uppercase mb-1 block">Column Vector</label>
              {selectedResource?.columns ? (
                <select
                  className="w-full bg-royal-green-800 border-royal-green-600 text-slate-200 rounded-lg shadow-sm focus:ring-primary focus:border-primary py-2.5 px-3 text-sm font-bold uppercase"
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
                  className="w-full bg-royal-green-800 border-royal-green-600 text-slate-200 rounded-lg shadow-sm focus:ring-primary focus:border-primary py-2.5 px-3 text-sm"
                  value={newRuleColumn}
                  onChange={(e) => setNewRuleColumn(e.target.value)}
                />
              )}
            </div>

            <div className="w-40">
              <label className="text-[10px] font-black text-slate-500 uppercase mb-1 block">Operator</label>
              <select
                title="Operator"
                className="w-full bg-royal-green-800 border-royal-green-600 text-slate-200 rounded-lg shadow-sm focus:ring-primary focus:border-primary py-2.5 px-3 text-sm font-mono font-bold"
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
            </div>

            {!['IS NULL', 'IS NOT NULL'].includes(newRuleOperator) && (
              <div className="flex-1 min-w-[200px]">
                <label className="text-[10px] font-black text-slate-500 uppercase mb-1 block">Predicate Value</label>
                <input
                  type="text"
                  placeholder="Target Value"
                  className="w-full bg-royal-green-800 border-royal-green-600 text-slate-200 rounded-lg shadow-sm focus:ring-primary focus:border-primary py-2.5 px-3 text-sm font-mono"
                  value={newRuleValue}
                  onChange={(e) => setNewRuleValue(e.target.value)}
                />
              </div>
            )}

            <button
              type="button"
              className="btn-primary px-6 py-2.5 text-sm font-black uppercase tracking-tighter whitespace-nowrap min-w-[120px] shadow-[0_0_15px_rgba(34,197,94,0.3)] disabled:shadow-none"
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
              Append Rule
            </button>
          </div>
        </div>
      )}



      {/* LLM Status */}
      <div className="flex items-center justify-between p-4 bg-black/20 border border-royal-green-700/50 rounded-xl">
        <div className="flex items-center space-x-4">
          <div className="p-2 bg-royal-green-800 rounded-lg">
            <Brain className="w-5 h-5 text-primary shadow-[0_0_8px_theme(colors.primary)]" />
          </div>
          <div>
            <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest">Neural Compute Engine</p>
            <p className="text-sm font-bold text-slate-200 uppercase tracking-tighter">
              {llmHealth?.provider || 'Unknown'} · {llmHealth?.model || 'N/A'}
            </p>
          </div>
        </div>
        <div className="flex items-center space-x-3 bg-royal-green-900/80 px-4 py-1.5 rounded-full border border-royal-green-600">
          <div
            className={`w-2 h-2 rounded-full ${llmHealth?.status === 'healthy' ? 'bg-primary shadow-[0_0_8px_theme(colors.primary)]' : 'bg-red-500'
              }`}
          />
          <span
            className={`text-[10px] font-black uppercase tracking-widest ${llmHealth?.status === 'healthy' ? 'text-primary' : 'text-red-500'
              }`}
          >
            {llmHealth?.status === 'healthy' ? 'Synchronized' : 'Offline'}
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
            <table className="min-w-full divide-y divide-royal-green-700">
              <thead className="bg-royal-green-800 sticky top-0">
                <tr>
                  {previewData.columns.map((col) => (
                    <th key={col.name} className="px-4 py-3 text-left text-xs font-bold text-slate-400 uppercase tracking-wider">
                      {col.name}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-royal-green-900 divide-y divide-royal-green-800">
                {previewData.rows.map((row, idx) => (
                  <tr key={idx} className="hover:bg-royal-green-800/50 transition-colors">
                    {previewData.columns.map((col) => (
                      <td key={col.name} className="px-4 py-3 text-sm text-slate-300 font-mono">
                        {row[col.name] != null ? String(row[col.name]) : <span className="text-slate-600">null</span>}
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
        <h2 className="text-xl font-semibold text-slate-100 uppercase tracking-widest">Review Execution Plan</h2>
        <p className="text-slate-400 mt-1">Final confirmation of validation parameters</p>
      </div>

      <div className="card divide-y divide-royal-green-700 !shadow-none !border-royal-green-600 overflow-hidden">
        <div className="p-5 flex items-center justify-between bg-royal-green-900 hover:bg-royal-green-800 transition-colors">
          <div>
            <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1">Source Connector</p>
            <p className="font-bold text-slate-100 uppercase tracking-tighter text-lg">{getSourceLabel()}</p>
          </div>
          <button
            onClick={() => setStep('source')}
            className="text-xs font-black text-primary hover:text-white uppercase tracking-widest bg-primary/10 px-3 py-1.5 rounded-lg transition-all"
          >
            Modify
          </button>
        </div>

        <div className="p-5 flex items-center justify-between bg-royal-green-900 hover:bg-royal-green-800 transition-colors">
          <div>
            <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1">Target Resource</p>
            <p className="font-bold text-slate-100 uppercase tracking-tighter text-lg">{selectedResource?.name}</p>
            <p className="text-[10px] font-mono text-slate-500 mt-1">{selectedResource?.path}</p>
          </div>
          <button
            onClick={() => setStep('browse')}
            className="text-xs font-black text-primary hover:text-white uppercase tracking-widest bg-primary/10 px-3 py-1.5 rounded-lg transition-all"
          >
            Modify
          </button>
        </div>

        <div className="p-5 flex items-center justify-between bg-royal-green-900 hover:bg-royal-green-800 transition-colors">
          <div>
            <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1">Execution Mode</p>
            <p className="font-bold text-slate-100 uppercase tracking-tighter text-lg text-primary shadow-primary/20 drop-shadow-md">
              {VALIDATION_MODES.find((m) => m.id === validationMode)?.name}
            </p>
          </div>
          <button
            onClick={() => setStep('config')}
            className="text-xs font-black text-primary hover:text-white uppercase tracking-widest bg-primary/10 px-3 py-1.5 rounded-lg transition-all"
          >
            Modify
          </button>
        </div>

        <div className="p-5 flex items-center justify-between bg-royal-green-900 hover:bg-royal-green-800 transition-colors">
          <div>
            <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1">Verification Sample</p>
            <p className="font-bold text-slate-100 uppercase tracking-tighter text-lg font-mono">{sampleSize.toLocaleString()} records</p>
          </div>
          <button
            onClick={() => setStep('config')}
            className="text-xs font-black text-primary hover:text-white uppercase tracking-widest bg-primary/10 px-3 py-1.5 rounded-lg transition-all"
          >
            Modify
          </button>
        </div>

        {sliceFilters.length > 0 && (
          <div className="p-5 flex flex-col space-y-3 bg-royal-green-900">
            <div className="flex items-center justify-between">
              <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest">Active Partition Filters</p>
              <button
                onClick={() => setStep('explore')}
                className="text-xs font-black text-primary hover:text-white uppercase tracking-widest bg-primary/10 px-3 py-1.5 rounded-lg transition-all"
              >
                Edit Filters
              </button>
            </div>
            <div className="flex flex-wrap gap-2">
              {sliceFilters.map((f: any, i: number) => (
                <span key={i} className="inline-flex items-center px-3 py-1 rounded-full text-[10px] font-black font-mono border border-primary/30 bg-primary/10 text-primary uppercase">
                  {f.column}: {f.filter_type}
                  {f.selected_values ? ` [VALS:${f.selected_values.length}]` : ''}
                  {f.min_value != null || f.max_value != null ? ` [RANGE:${f.min_value ?? ''}→${f.max_value ?? ''}]` : ''}
                </span>
              ))}
            </div>
          </div>
        )}

        <div className="p-5 flex items-center justify-between bg-royal-green-900">
          <div>
            <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1">Compute Provider</p>
            <p className="font-bold text-slate-100 uppercase tracking-tighter">
              {llmHealth?.provider || 'Unknown'} · {llmHealth?.model || 'N/A'}
            </p>
          </div>
          <div className="flex items-center space-x-2 text-primary">
            <Brain className="w-5 h-5" />
            <span className="text-[10px] font-black uppercase tracking-widest">Neural Enabled</span>
          </div>
        </div>
      </div>

      <div className="bg-amber-500/10 border border-amber-500/30 rounded-xl p-5 shadow-[0_0_30px_rgba(245,158,11,0.05)]">
        <div className="flex items-start space-x-4">
          <div className="p-2 bg-amber-500/20 rounded-lg">
            <AlertCircle className="w-6 h-6 text-amber-500" />
          </div>
          <div className="flex-1">
            <p className="font-black text-amber-500 uppercase tracking-tighter text-lg">Execution Pipeline</p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-2 mt-3">
              <p className="text-xs font-bold text-slate-400 uppercase tracking-widest flex items-center"><span className="w-1.5 h-1.5 bg-amber-500 rounded-full mr-2" /> Data Profiling Initiation</p>
              <p className="text-xs font-bold text-slate-400 uppercase tracking-widest flex items-center"><span className="w-1.5 h-1.5 bg-amber-500 rounded-full mr-2" /> Heuristic Rule Generation</p>
              <p className="text-xs font-bold text-slate-400 uppercase tracking-widest flex items-center"><span className="w-1.5 h-1.5 bg-amber-500 rounded-full mr-2" /> Deterministic Logic Run</p>
              <p className="text-xs font-bold text-slate-400 uppercase tracking-widest flex items-center"><span className="w-1.5 h-1.5 bg-amber-500 rounded-full mr-2" /> Telemetry Dispatch</p>
            </div>
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
        {step === 'view_data' && renderViewDataStep()}
        {step === 'prep' && renderPrepStep()}
        {step === 'explore' && renderExploreStep()}
        {step === 'config' && renderConfigStep()}
        {step === 'review' && renderReviewStep()}
      </div>
    </div>
  );
}
