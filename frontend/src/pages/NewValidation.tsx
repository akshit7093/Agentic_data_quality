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
} from 'lucide-react';
import { useDataSources } from '@/hooks/useDataSources';
import { useSubmitValidation } from '@/hooks/useValidations';
import { useLLMHealth } from '@/hooks/useSystem';
import Modal from '@/components/Modal';

interface DataResource {
  name: string;
  type: 'table' | 'file' | 'folder' | 'container';
  path: string;
  size?: string;
  rowCount?: number;
  columnCount?: number;
  columns?: { name: string; type: string }[];
  children?: DataResource[];
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
];

// Mock resources for demonstration
const MOCK_RESOURCES: Record<string, DataResource[]> = {
  sqlite: [
    {
      name: 'customers',
      type: 'table',
      path: 'customers',
      rowCount: 1000,
      columnCount: 10,
      columns: [
        { name: 'customer_id', type: 'INTEGER' },
        { name: 'email', type: 'TEXT' },
        { name: 'first_name', type: 'TEXT' },
        { name: 'last_name', type: 'TEXT' },
        { name: 'phone', type: 'TEXT' },
        { name: 'date_of_birth', type: 'DATE' },
        { name: 'country', type: 'TEXT' },
        { name: 'created_at', type: 'TIMESTAMP' },
        { name: 'status', type: 'TEXT' },
        { name: 'lifetime_value', type: 'REAL' },
      ],
    },
    {
      name: 'orders',
      type: 'table',
      path: 'orders',
      rowCount: 5000,
      columnCount: 10,
      columns: [
        { name: 'order_id', type: 'INTEGER' },
        { name: 'customer_id', type: 'INTEGER' },
        { name: 'product_id', type: 'INTEGER' },
        { name: 'quantity', type: 'INTEGER' },
        { name: 'unit_price', type: 'REAL' },
        { name: 'total_amount', type: 'REAL' },
        { name: 'order_date', type: 'TIMESTAMP' },
        { name: 'shipping_address', type: 'TEXT' },
        { name: 'status', type: 'TEXT' },
        { name: 'discount_code', type: 'TEXT' },
      ],
    },
    {
      name: 'products',
      type: 'table',
      path: 'products',
      rowCount: 200,
      columnCount: 9,
    },
    {
      name: 'sales_transactions',
      type: 'table',
      path: 'sales_transactions',
      rowCount: 10000,
      columnCount: 8,
    },
  ],
};

export default function NewValidation() {
  const navigate = useNavigate();
  const { data: dataSources } = useDataSources();
  const submitValidation = useSubmitValidation();
  const { data: llmHealth } = useLLMHealth();

  const [step, setStep] = useState<'source' | 'browse' | 'config' | 'review'>('source');
  const [selectedDataSource, setSelectedDataSource] = useState<string>('');
  const [selectedResource, setSelectedResource] = useState<DataResource | null>(null);
  const [expandedResources, setExpandedResources] = useState<Set<string>>(new Set());
  const [searchQuery, setSearchQuery] = useState('');
  const [validationMode, setValidationMode] = useState('hybrid');
  const [sampleSize, setSampleSize] = useState(1000);
  const [isLoading, setIsLoading] = useState(false);
  const [showPreview, setShowPreview] = useState(false);

  // Get resources for selected data source
  const resources = selectedDataSource
    ? MOCK_RESOURCES[dataSources?.find((ds) => ds.id === selectedDataSource)?.source_type || 'sqlite'] || []
    : [];

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
      const result = await submitValidation.mutateAsync({
        data_source_id: selectedDataSource || 'local',
        target_path: selectedResource.path,
        validation_mode: validationMode,
        sample_size: sampleSize,
        custom_rules: [],
      });

      if (result?.validation_id) {
        navigate(`/validations/${result.validation_id}`);
      }
    } finally {
      setIsLoading(false);
    }
  };

  const renderStepIndicator = () => (
    <div className="flex items-center justify-center mb-8">
      {[
        { id: 'source', label: 'Data Source', icon: Database },
        { id: 'browse', label: 'Browse', icon: Folder },
        { id: 'config', label: 'Configure', icon: Settings },
        { id: 'review', label: 'Review', icon: CheckCircle },
      ].map((s, idx) => {
        const isActive = step === s.id;
        const isCompleted =
          (step === 'browse' && s.id === 'source') ||
          (step === 'config' && ['source', 'browse'].includes(s.id)) ||
          (step === 'review' && ['source', 'browse', 'config'].includes(s.id));

        return (
          <div key={s.id} className="flex items-center">
            <div
              className={`flex items-center space-x-2 px-4 py-2 rounded-lg ${
                isActive
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
        {/* Local Test Database */}
        <button
          onClick={() => {
            setSelectedDataSource('local-test');
            setStep('browse');
          }}
          className={`p-6 rounded-xl border-2 text-left transition-all ${
            selectedDataSource === 'local-test'
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

        {/* File Upload */}
        <button
          onClick={() => {
            setSelectedDataSource('file-upload');
            setStep('browse');
          }}
          className={`p-6 rounded-xl border-2 text-left transition-all ${
            selectedDataSource === 'file-upload'
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

        {/* ADLS Gen2 */}
        <button
          onClick={() => {
            setSelectedDataSource('adls-mock');
            setStep('browse');
          }}
          className={`p-6 rounded-xl border-2 text-left transition-all ${
            selectedDataSource === 'adls-mock'
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

        {/* Connected Data Sources */}
        {dataSources?.map((ds) => (
          <button
            key={ds.id}
            onClick={() => {
              setSelectedDataSource(ds.id);
              setStep('browse');
            }}
            className={`p-6 rounded-xl border-2 text-left transition-all ${
              selectedDataSource === ds.id
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
                    className={`w-2 h-2 rounded-full ${
                      ds.is_active ? 'bg-green-500' : 'bg-gray-400'
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
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
          <input
            type="text"
            placeholder="Search tables, columns..."
            className="pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-primary-500 focus:border-primary-500"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </div>
      </div>

      <div className="card overflow-hidden">
        <div className="bg-gray-50 px-4 py-3 border-b border-gray-200">
          <div className="flex items-center space-x-2 text-sm text-gray-600">
            <Database className="w-4 h-4" />
            <span>Test Database</span>
            <ChevronRight className="w-4 h-4" />
            <span>Tables</span>
          </div>
        </div>

        <div className="divide-y divide-gray-200 max-h-96 overflow-y-auto">
          {filteredResources.map((resource) => (
            <div key={resource.path}>
              <div
                onClick={() => {
                  if (resource.columns) {
                    toggleExpand(resource.path);
                  }
                  setSelectedResource(resource);
                }}
                className={`flex items-center justify-between p-4 cursor-pointer hover:bg-gray-50 ${
                  selectedResource?.path === resource.path ? 'bg-primary-50' : ''
                }`}
              >
                <div className="flex items-center space-x-3">
                  <Table className="w-5 h-5 text-primary-500" />
                  <div>
                    <span className="font-medium text-gray-900">{resource.name}</span>
                    <span className="text-sm text-gray-500 ml-3">
                      {resource.rowCount?.toLocaleString()} rows
                      {resource.columnCount && `, ${resource.columnCount} cols`}
                    </span>
                  </div>
                </div>
                <div className="flex items-center space-x-3">
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
          onClick={() => setStep('config')}
          disabled={!selectedResource}
          className="btn-primary disabled:opacity-50"
        >
          Continue
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
            <Table className="w-5 h-5 text-blue-600" />
            <div>
              <p className="font-medium text-blue-900">{selectedResource?.name}</p>
              <p className="text-sm text-blue-700">
                {selectedResource?.rowCount?.toLocaleString()} rows,{' '}
                {selectedResource?.columnCount} columns
              </p>
            </div>
          </div>
          <button
            onClick={() => setShowPreview(true)}
            className="text-sm text-blue-600 hover:text-blue-700 font-medium"
          >
            Preview Data
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
              className={`flex items-start p-4 border-2 rounded-xl cursor-pointer transition-all ${
                validationMode === mode.id
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
                    className={`w-5 h-5 ${
                      validationMode === mode.id ? 'text-primary-600' : 'text-gray-400'
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

      {/* LLM Status */}
      <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
        <div className="flex items-center space-x-3">
          <Brain className="w-5 h-5 text-gray-400" />
          <div>
            <p className="text-sm font-medium text-gray-900">AI Model</p>
            <p className="text-sm text-gray-500">
              {llmHealth?.provider} ({llmHealth?.model})
            </p>
          </div>
        </div>
        <div className="flex items-center space-x-2">
          <div
            className={`w-2 h-2 rounded-full ${
              llmHealth?.status === 'healthy' ? 'bg-green-500' : 'bg-red-500'
            }`}
          />
          <span
            className={`text-sm ${
              llmHealth?.status === 'healthy' ? 'text-green-600' : 'text-red-600'
            }`}
          >
            {llmHealth?.status === 'healthy' ? 'Connected' : 'Disconnected'}
          </span>
        </div>
      </div>

      <div className="flex justify-between">
        <button onClick={() => setStep('browse')} className="btn-secondary">
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
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                {selectedResource?.columns?.map((col) => (
                  <th
                    key={col.name}
                    className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase"
                  >
                    {col.name}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {[1, 2, 3, 4, 5].map((row) => (
                <tr key={row}>
                  {selectedResource?.columns?.map((col) => (
                    <td key={col.name} className="px-4 py-2 text-sm text-gray-600">
                      {col.name === 'customer_id' && row}
                      {col.name === 'email' && `user${row}@example.com`}
                      {col.name === 'first_name' && ['John', 'Jane', 'Bob', 'Alice', 'Charlie'][row - 1]}
                      {col.name === 'last_name' && ['Smith', 'Doe', 'Johnson', 'Williams', 'Brown'][row - 1]}
                      {col.name === 'phone' && `+1-555-000${row}`}
                      {col.name === 'status' && ['active', 'active', 'inactive', 'active', 'suspended'][row - 1]}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
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
            <p className="font-medium text-gray-900">{selectedResource?.name}</p>
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

        <div className="p-4 flex items-center justify-between">
          <div>
            <p className="text-sm text-gray-500">AI Model</p>
            <p className="font-medium text-gray-900">
              {llmHealth?.provider} ({llmHealth?.model})
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

      <div className="max-w-3xl mx-auto">
        {step === 'source' && renderSourceStep()}
        {step === 'browse' && renderBrowseStep()}
        {step === 'config' && renderConfigStep()}
        {step === 'review' && renderReviewStep()}
      </div>
    </div>
  );
}
