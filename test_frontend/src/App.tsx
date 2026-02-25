import { useState, useEffect } from 'react';
import { 
  Play, Settings, Database, FileText, BarChart3, 
  CheckCircle, AlertCircle, Brain, Server, Folder,
  ChevronRight, ChevronDown, Table, FileJson, FileCode,
  RefreshCw, ExternalLink
} from 'lucide-react';
import axios from 'axios';

// Types
interface LLMProvider {
  id: string;
  name: string;
  description: string;
  models: { id: string; name: string }[];
  needsKey: boolean;
  defaultUrl?: string;
}

interface DataResource {
  name: string;
  type: 'table' | 'file' | 'folder' | 'container';
  path: string;
  size?: string;
  rowCount?: number;
  columnCount?: number;
  children?: DataResource[];
}

const LLM_PROVIDERS: LLMProvider[] = [
  {
    id: 'ollama',
    name: 'Ollama (Local)',
    description: 'Run models locally - Recommended for privacy',
    models: [
      { id: 'llama3.2', name: 'Llama 3.2 (Recommended)' },
      { id: 'llama3.1', name: 'Llama 3.1' },
      { id: 'mistral', name: 'Mistral' },
      { id: 'codellama', name: 'CodeLlama' },
      { id: 'phi3', name: 'Phi-3' },
    ],
    needsKey: false,
    defaultUrl: 'http://localhost:11434',
  },
  {
    id: 'lmstudio',
    name: 'LM Studio (Local)',
    description: 'Use LM Studio local server',
    models: [{ id: 'local-model', name: 'Auto-detect' }],
    needsKey: false,
    defaultUrl: 'http://localhost:1234/v1',
  },
  {
    id: 'openai',
    name: 'OpenAI (Cloud)',
    description: 'GPT-4, GPT-3.5 Turbo',
    models: [
      { id: 'gpt-4', name: 'GPT-4' },
      { id: 'gpt-4-turbo', name: 'GPT-4 Turbo' },
      { id: 'gpt-3.5-turbo', name: 'GPT-3.5 Turbo' },
    ],
    needsKey: true,
  },
  {
    id: 'anthropic',
    name: 'Anthropic (Cloud)',
    description: 'Claude 3.5 Sonnet, Opus, Haiku',
    models: [
      { id: 'claude-3-5-sonnet-20241022', name: 'Claude 3.5 Sonnet (Recommended)' },
      { id: 'claude-3-opus-20240229', name: 'Claude 3 Opus' },
      { id: 'claude-3-haiku-20240307', name: 'Claude 3 Haiku' },
    ],
    needsKey: true,
  },
];

const DATA_TYPES = [
  { id: 'structured', name: 'Structured', icon: Table, description: 'Database tables with schema' },
  { id: 'semi_structured', name: 'Semi-Structured', icon: FileJson, description: 'JSON, XML files' },
  { id: 'unstructured', name: 'Unstructured', icon: FileText, description: 'Text documents, logs' },
  { id: 'adls', name: 'ADLS Gen2 Mock', icon: Folder, description: 'Cloud file system structure' },
];

// Mock data resources
const MOCK_RESOURCES: Record<string, DataResource[]> = {
  structured: [
    {
      name: 'customers',
      type: 'table',
      path: 'customers',
      rowCount: 1000,
      columnCount: 10,
    },
    {
      name: 'orders',
      type: 'table',
      path: 'orders',
      rowCount: 5000,
      columnCount: 10,
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
  semi_structured: [
    { name: 'application_logs.json', type: 'file', path: 'application_logs.json', size: '2.5 MB' },
    { name: 'user_events.json', type: 'file', path: 'user_events.json', size: '4.1 MB' },
    { name: 'system_configs.xml', type: 'file', path: 'system_configs.xml', size: '156 KB' },
  ],
  unstructured: [
    { name: 'support_tickets.json', type: 'file', path: 'support_tickets.json', size: '1.8 MB' },
    { name: 'product_reviews.json', type: 'file', path: 'product_reviews.json', size: '3.2 MB' },
  ],
  adls: [
    {
      name: 'raw-data',
      type: 'container',
      path: 'raw-data',
      children: [
        { name: 'customers', type: 'folder', path: 'raw-data/customers' },
        { name: 'orders', type: 'folder', path: 'raw-data/orders' },
        { name: 'products', type: 'folder', path: 'raw-data/products' },
      ],
    },
    {
      name: 'processed-data',
      type: 'container',
      path: 'processed-data',
      children: [
        { name: 'daily-aggregates', type: 'folder', path: 'processed-data/daily-aggregates' },
        { name: 'weekly-reports', type: 'folder', path: 'processed-data/weekly-reports' },
      ],
    },
    {
      name: 'curated-data',
      type: 'container',
      path: 'curated-data',
      children: [
        { name: 'analytics', type: 'folder', path: 'curated-data/analytics' },
        { name: 'ml-features', type: 'folder', path: 'curated-data/ml-features' },
      ],
    },
  ],
};

function App() {
  // State
  const [step, setStep] = useState<'llm' | 'data' | 'config' | 'running' | 'results'>('llm');
  const [selectedProvider, setSelectedProvider] = useState<LLMProvider>(LLM_PROVIDERS[0]);
  const [selectedModel, setSelectedModel] = useState<string>(LLM_PROVIDERS[0].models[0].id);
  const [baseUrl, setBaseUrl] = useState<string>(LLM_PROVIDERS[0].defaultUrl || '');
  const [apiKey, setApiKey] = useState<string>('');
  const [selectedDataType, setSelectedDataType] = useState<string>('structured');
  const [selectedResource, setSelectedResource] = useState<DataResource | null>(null);
  const [expandedFolders, setExpandedFolders] = useState<Set<string>>(new Set());
  const [validationMode, setValidationMode] = useState<string>('hybrid');
  const [sampleSize, setSampleSize] = useState<number>(1000);
  const [isRunning, setIsRunning] = useState(false);
  const [testResults, setTestResults] = useState<any>(null);
  const [llmStatus, setLlmStatus] = useState<'unknown' | 'connected' | 'error'>('unknown');

  // Test LLM connection
  const testLLMConnection = async () => {
    try {
      const response = await axios.get('/api/v1/llm/health');
      if (response.data.status === 'healthy') {
        setLlmStatus('connected');
      } else {
        setLlmStatus('error');
      }
    } catch {
      setLlmStatus('error');
    }
  };

  // Run validation
  const runValidation = async () => {
    setIsRunning(true);
    setStep('running');
    
    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      setTestResults({
        quality_score: 87.5,
        total_rules: 15,
        passed_rules: 13,
        failed_rules: 2,
        warning_rules: 0,
        records_processed: sampleSize,
        duration: '2.3s',
        results: [
          { rule_name: 'Customer ID Not Null', type: 'column', severity: 'critical', status: 'passed', failed: 0 },
          { rule_name: 'Email Format Valid', type: 'pattern', severity: 'critical', status: 'passed', failed: 0 },
          { rule_name: 'Phone Number Valid', type: 'pattern', severity: 'warning', status: 'failed', failed: 27 },
          { rule_name: 'Revenue Positive', type: 'column', severity: 'warning', status: 'passed', failed: 0 },
        ],
        solutions: [
          {
            title: 'Standardize Phone Numbers',
            description: 'Apply regex transformation to standardize phone number formats',
            impact: 'Will fix 27 records (2.7%)',
            autoApply: true,
          },
          {
            title: 'Remove Duplicate Emails',
            description: 'Identify and merge customer records with duplicate email addresses',
            impact: 'Will improve data integrity',
            autoApply: false,
          },
        ],
      });
      
      setStep('results');
    } catch (error) {
      console.error('Validation failed:', error);
    } finally {
      setIsRunning(false);
    }
  };

  // Toggle folder expansion
  const toggleFolder = (path: string) => {
    const newExpanded = new Set(expandedFolders);
    if (newExpanded.has(path)) {
      newExpanded.delete(path);
    } else {
      newExpanded.add(path);
    }
    setExpandedFolders(newExpanded);
  };

  // Render LLM selection step
  const renderLLMStep = () => (
    <div className="space-y-6">
      <div className="text-center mb-8">
        <h2 className="text-2xl font-bold text-gray-900">Select LLM Provider</h2>
        <p className="text-gray-500 mt-2">Choose the AI model for data quality analysis</p>
      </div>

      <div className="grid grid-cols-2 gap-4">
        {LLM_PROVIDERS.map((provider) => (
          <div
            key={provider.id}
            onClick={() => {
              setSelectedProvider(provider);
              setSelectedModel(provider.models[0].id);
              setBaseUrl(provider.defaultUrl || '');
            }}
            className={`p-4 rounded-xl border-2 cursor-pointer transition-all ${
              selectedProvider.id === provider.id
                ? 'border-primary-500 bg-primary-50'
                : 'border-gray-200 hover:border-gray-300'
            }`}
          >
            <div className="flex items-center space-x-3">
              <Brain className={`w-6 h-6 ${selectedProvider.id === provider.id ? 'text-primary-600' : 'text-gray-400'}`} />
              <div>
                <h3 className="font-semibold text-gray-900">{provider.name}</h3>
                <p className="text-sm text-gray-500">{provider.description}</p>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Provider-specific settings */}
      <div className="card p-6">
        <h3 className="font-semibold text-gray-900 mb-4">Configuration</h3>
        
        <div className="space-y-4">
          <div>
            <label className="form-label">Model</label>
            <select
              className="form-select"
              value={selectedModel}
              onChange={(e) => setSelectedModel(e.target.value)}
            >
              {selectedProvider.models.map((model) => (
                <option key={model.id} value={model.id}>{model.name}</option>
              ))}
            </select>
          </div>

          {(selectedProvider.id === 'ollama' || selectedProvider.id === 'lmstudio') && (
            <div>
              <label className="form-label">Base URL</label>
              <input
                type="text"
                className="form-input"
                value={baseUrl}
                onChange={(e) => setBaseUrl(e.target.value)}
                placeholder="http://localhost:11434"
              />
            </div>
          )}

          {selectedProvider.needsKey && (
            <div>
              <label className="form-label">API Key</label>
              <input
                type="password"
                className="form-input"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder="Enter API key"
              />
            </div>
          )}
        </div>

        {/* Connection test */}
        <div className="mt-4 flex items-center justify-between">
          <div className="flex items-center space-x-2">
            {llmStatus === 'connected' && <CheckCircle className="w-5 h-5 text-success-500" />}
            {llmStatus === 'error' && <AlertCircle className="w-5 h-5 text-danger-500" />}
            {llmStatus === 'unknown' && <Server className="w-5 h-5 text-gray-400" />}
            <span className={`text-sm ${
              llmStatus === 'connected' ? 'text-success-600' :
              llmStatus === 'error' ? 'text-danger-600' : 'text-gray-500'
            }`}>
              {llmStatus === 'connected' ? 'Connected' :
               llmStatus === 'error' ? 'Connection failed' : 'Not tested'}
            </span>
          </div>
          <button
            onClick={testLLMConnection}
            className="btn-secondary text-sm"
          >
            <RefreshCw className="w-4 h-4 mr-2 inline" />
            Test Connection
          </button>
        </div>
      </div>

      <div className="flex justify-end">
        <button
          onClick={() => setStep('data')}
          className="btn-primary"
        >
          Continue
          <ChevronRight className="w-4 h-4 ml-2 inline" />
        </button>
      </div>
    </div>
  );

  // Render data selection step
  const renderDataStep = () => (
    <div className="space-y-6">
      <div className="text-center mb-8">
        <h2 className="text-2xl font-bold text-gray-900">Select Data Source</h2>
        <p className="text-gray-500 mt-2">Choose the data to validate</p>
      </div>

      {/* Data type tabs */}
      <div className="flex space-x-2 mb-6">
        {DATA_TYPES.map((type) => (
          <button
            key={type.id}
            onClick={() => {
              setSelectedDataType(type.id);
              setSelectedResource(null);
            }}
            className={`flex items-center space-x-2 px-4 py-2 rounded-lg font-medium transition-colors ${
              selectedDataType === type.id
                ? 'bg-primary-600 text-white'
                : 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-50'
            }`}
          >
            <type.icon className="w-4 h-4" />
            <span>{type.name}</span>
          </button>
        ))}
      </div>

      {/* Data type description */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <p className="text-sm text-blue-800">
          <strong>{DATA_TYPES.find(t => t.id === selectedDataType)?.name}:</strong>{' '}
          {DATA_TYPES.find(t => t.id === selectedDataType)?.description}
        </p>
      </div>

      {/* Resource browser */}
      <div className="card">
        <div className="p-4 border-b border-gray-200">
          <h3 className="font-semibold text-gray-900">Available Resources</h3>
        </div>
        <div className="divide-y divide-gray-200">
          {MOCK_RESOURCES[selectedDataType]?.map((resource) => (
            <div key={resource.path}>
              <div
                onClick={() => {
                  if (resource.type === 'folder' || resource.type === 'container') {
                    toggleFolder(resource.path);
                  } else {
                    setSelectedResource(resource);
                  }
                }}
                className={`flex items-center justify-between p-4 cursor-pointer hover:bg-gray-50 ${
                  selectedResource?.path === resource.path ? 'bg-primary-50' : ''
                }`}
              >
                <div className="flex items-center space-x-3">
                  {resource.type === 'folder' || resource.type === 'container' ? (
                    expandedFolders.has(resource.path) ? (
                      <ChevronDown className="w-4 h-4 text-gray-400" />
                    ) : (
                      <ChevronRight className="w-4 h-4 text-gray-400" />
                    )
                  ) : (
                    <div className="w-4" />
                  )}
                  {resource.type === 'table' && <Table className="w-5 h-5 text-primary-500" />}
                  {resource.type === 'file' && <FileText className="w-5 h-5 text-gray-500" />}
                  {resource.type === 'folder' && <Folder className="w-5 h-5 text-yellow-500" />}
                  {resource.type === 'container' && <Database className="w-5 h-5 text-blue-500" />}
                  <div>
                    <span className="font-medium text-gray-900">{resource.name}</span>
                    {resource.rowCount && (
                      <span className="text-sm text-gray-500 ml-2">
                        ({resource.rowCount.toLocaleString()} rows, {resource.columnCount} cols)
                      </span>
                    )}
                    {resource.size && (
                      <span className="text-sm text-gray-500 ml-2">({resource.size})</span>
                    )}
                  </div>
                </div>
                {selectedResource?.path === resource.path && (
                  <CheckCircle className="w-5 h-5 text-primary-600" />
                )}
              </div>
              
              {/* Children */}
              {resource.children && expandedFolders.has(resource.path) && (
                <div className="pl-8">
                  {resource.children.map((child) => (
                    <div
                      key={child.path}
                      onClick={() => setSelectedResource(child)}
                      className={`flex items-center space-x-3 p-3 cursor-pointer hover:bg-gray-50 ${
                        selectedResource?.path === child.path ? 'bg-primary-50' : ''
                      }`}
                    >
                      <Folder className="w-4 h-4 text-yellow-500" />
                      <span className="text-gray-700">{child.name}</span>
                      {selectedResource?.path === child.path && (
                        <CheckCircle className="w-4 h-4 text-primary-600 ml-auto" />
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      <div className="flex justify-between">
        <button
          onClick={() => setStep('llm')}
          className="btn-secondary"
        >
          Back
        </button>
        <button
          onClick={() => setStep('config')}
          disabled={!selectedResource}
          className="btn-primary disabled:opacity-50 disabled:cursor-not-allowed"
        >
          Continue
          <ChevronRight className="w-4 h-4 ml-2 inline" />
        </button>
      </div>
    </div>
  );

  // Render config step
  const renderConfigStep = () => (
    <div className="space-y-6">
      <div className="text-center mb-8">
        <h2 className="text-2xl font-bold text-gray-900">Validation Configuration</h2>
        <p className="text-gray-500 mt-2">Configure validation settings</p>
      </div>

      {/* Selected resources summary */}
      <div className="bg-gray-50 rounded-lg p-4">
        <h3 className="text-sm font-medium text-gray-700 mb-2">Selected Configuration</h3>
        <div className="grid grid-cols-2 gap-4 text-sm">
          <div>
            <span className="text-gray-500">LLM:</span>
            <span className="ml-2 font-medium">{selectedProvider.name} ({selectedModel})</span>
          </div>
          <div>
            <span className="text-gray-500">Data Source:</span>
            <span className="ml-2 font-medium">{selectedResource?.name}</span>
          </div>
        </div>
      </div>

      {/* Validation mode */}
      <div>
        <label className="form-label">Validation Mode</label>
        <div className="space-y-3">
          {[
            { id: 'custom_rules', name: 'Custom Rules Only', desc: 'Use only predefined validation rules' },
            { id: 'ai_recommended', name: 'AI Recommended', desc: 'Let AI generate rules based on data profiling' },
            { id: 'hybrid', name: 'Hybrid (Recommended)', desc: 'Combine custom rules with AI recommendations' },
          ].map((mode) => (
            <label
              key={mode.id}
              className={`flex items-start p-4 border-2 rounded-lg cursor-pointer transition-all ${
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
              <div className="ml-3">
                <span className="font-medium text-gray-900">{mode.name}</span>
                <p className="text-sm text-gray-500">{mode.desc}</p>
              </div>
            </label>
          ))}
        </div>
      </div>

      {/* Sample size */}
      <div>
        <label className="form-label">Sample Size</label>
        <input
          type="number"
          className="form-input"
          value={sampleSize}
          onChange={(e) => setSampleSize(Number(e.target.value))}
          min={100}
          max={100000}
        />
        <p className="text-sm text-gray-500 mt-1">
          Number of records to sample for validation (100 - 100,000)
        </p>
      </div>

      <div className="flex justify-between">
        <button
          onClick={() => setStep('data')}
          className="btn-secondary"
        >
          Back
        </button>
        <button
          onClick={runValidation}
          className="btn-primary"
        >
          <Play className="w-4 h-4 mr-2 inline" />
          Start Validation
        </button>
      </div>
    </div>
  );

  // Render running step
  const renderRunningStep = () => (
    <div className="text-center py-16">
      <div className="animate-spin w-16 h-16 border-4 border-primary-500 border-t-transparent rounded-full mx-auto mb-6" />
      <h2 className="text-2xl font-bold text-gray-900 mb-2">Running Validation</h2>
      <p className="text-gray-500">Analyzing data quality with {selectedProvider.name}...</p>
      
      <div className="mt-8 max-w-md mx-auto">
        <div className="bg-gray-200 rounded-full h-2">
          <div className="bg-primary-500 h-2 rounded-full animate-pulse" style={{ width: '60%' }} />
        </div>
        <div className="flex justify-between text-sm text-gray-500 mt-2">
          <span>Connecting...</span>
          <span>Profiling data...</span>
          <span>Validating...</span>
        </div>
      </div>
    </div>
  );

  // Render results step
  const renderResultsStep = () => {
    if (!testResults) return null;
    
    return (
      <div className="space-y-6">
        <div className="text-center mb-8">
          <h2 className="text-2xl font-bold text-gray-900">Validation Results</h2>
          <p className="text-gray-500 mt-2">
            Completed in {testResults.duration}
          </p>
        </div>

        {/* Quality Score */}
        <div className="card p-6">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="text-lg font-semibold text-gray-900">Overall Quality Score</h3>
              <p className="text-gray-500">Based on {testResults.total_rules} validation rules</p>
            </div>
            <div className="text-center">
              <div className={`text-5xl font-bold ${
                testResults.quality_score >= 90 ? 'text-success-600' :
                testResults.quality_score >= 70 ? 'text-warning-600' : 'text-danger-600'
              }`}>
                {testResults.quality_score}%
              </div>
            </div>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-4 gap-4 mt-6 pt-6 border-t border-gray-200">
            <div className="text-center">
              <div className="text-2xl font-bold text-success-600">{testResults.passed_rules}</div>
              <div className="text-sm text-gray-500">Passed</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-danger-600">{testResults.failed_rules}</div>
              <div className="text-sm text-gray-500">Failed</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-warning-600">{testResults.warning_rules}</div>
              <div className="text-sm text-gray-500">Warnings</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-primary-600">{testResults.records_processed.toLocaleString()}</div>
              <div className="text-sm text-gray-500">Records</div>
            </div>
          </div>
        </div>

        {/* Results Table */}
        <div className="card">
          <div className="p-4 border-b border-gray-200">
            <h3 className="font-semibold text-gray-900">Validation Results</h3>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  <th className="text-left p-3 text-sm font-medium text-gray-700">Rule</th>
                  <th className="text-left p-3 text-sm font-medium text-gray-700">Type</th>
                  <th className="text-left p-3 text-sm font-medium text-gray-700">Severity</th>
                  <th className="text-left p-3 text-sm font-medium text-gray-700">Status</th>
                  <th className="text-left p-3 text-sm font-medium text-gray-700">Failed</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {testResults.results.map((result: any, idx: number) => (
                  <tr key={idx} className="hover:bg-gray-50">
                    <td className="p-3 font-medium">{result.rule_name}</td>
                    <td className="p-3 text-sm text-gray-600">{result.type}</td>
                    <td className="p-3">
                      <span className={`text-xs px-2 py-1 rounded-full ${
                        result.severity === 'critical' ? 'bg-red-100 text-red-800' :
                        result.severity === 'warning' ? 'bg-yellow-100 text-yellow-800' :
                        'bg-blue-100 text-blue-800'
                      }`}>
                        {result.severity}
                      </span>
                    </td>
                    <td className="p-3">
                      <span className={`font-medium ${
                        result.status === 'passed' ? 'text-success-600' : 'text-danger-600'
                      }`}>
                        {result.status.toUpperCase()}
                      </span>
                    </td>
                    <td className="p-3">{result.failed}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* AI Solutions */}
        <div className="card">
          <div className="p-4 border-b border-gray-200">
            <h3 className="font-semibold text-gray-900">🤖 AI-Recommended Solutions</h3>
          </div>
          <div className="p-4 space-y-4">
            {testResults.solutions.map((solution: any, idx: number) => (
              <div key={idx} className="border-l-4 border-primary-500 bg-gray-50 p-4 rounded-r-lg">
                <h4 className="font-medium text-gray-900">{solution.title}</h4>
                <p className="text-sm text-gray-600 mt-1">{solution.description}</p>
                <p className="text-sm text-primary-600 mt-2">
                  <strong>Impact:</strong> {solution.impact}
                </p>
                <div className="mt-3 space-x-2">
                  {solution.autoApply && (
                    <button className="btn-primary text-sm">
                      Apply Solution
                    </button>
                  )}
                  <button className="btn-secondary text-sm">
                    View Details
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="flex justify-between">
          <button
            onClick={() => setStep('config')}
            className="btn-secondary"
          >
            Back
          </button>
          <div className="space-x-2">
            <button
              onClick={() => window.open('/api/v1/reports/latest', '_blank')}
              className="btn-secondary"
            >
              <ExternalLink className="w-4 h-4 mr-2 inline" />
              View Full Report
            </button>
            <button
              onClick={() => {
                setStep('llm');
                setTestResults(null);
                setSelectedResource(null);
              }}
              className="btn-primary"
            >
              Run New Validation
            </button>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200">
        <div className="max-w-5xl mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <BarChart3 className="w-8 h-8 text-primary-600" />
              <div>
                <h1 className="text-xl font-bold text-gray-900">AI DQ Agent</h1>
                <p className="text-sm text-gray-500">Test Console</p>
              </div>
            </div>
            <div className="flex items-center space-x-4 text-sm">
              <div className="flex items-center space-x-2">
                <div className={`w-2 h-2 rounded-full ${
                  llmStatus === 'connected' ? 'bg-green-500' : 'bg-gray-400'
                }`} />
                <span className="text-gray-600">{selectedProvider.name}</span>
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Progress Steps */}
      <div className="bg-white border-b border-gray-200">
        <div className="max-w-5xl mx-auto px-4 py-4">
          <div className="flex items-center justify-center space-x-8">
            {[
              { id: 'llm', label: 'LLM', icon: Brain },
              { id: 'data', label: 'Data', icon: Database },
              { id: 'config', label: 'Config', icon: Settings },
              { id: 'results', label: 'Results', icon: BarChart3 },
            ].map((s, idx) => {
              const isActive = step === s.id;
              const isPast = ['llm', 'data', 'config', 'results'].indexOf(step) > idx;
              
              return (
                <div key={s.id} className="flex items-center">
                  <div className={`flex items-center space-x-2 ${
                    isActive ? 'text-primary-600' :
                    isPast ? 'text-green-600' : 'text-gray-400'
                  }`}>
                    <div className={`w-8 h-8 rounded-full flex items-center justify-center ${
                      isActive ? 'bg-primary-100' :
                      isPast ? 'bg-green-100' : 'bg-gray-100'
                    }`}>
                      <s.icon className="w-4 h-4" />
                    </div>
                    <span className="font-medium">{s.label}</span>
                  </div>
                  {idx < 3 && (
                    <ChevronRight className="w-5 h-5 text-gray-300 ml-8" />
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Main Content */}
      <main className="max-w-3xl mx-auto px-4 py-8">
        {step === 'llm' && renderLLMStep()}
        {step === 'data' && renderDataStep()}
        {step === 'config' && renderConfigStep()}
        {step === 'running' && renderRunningStep()}
        {step === 'results' && renderResultsStep()}
      </main>
    </div>
  );
}

export default App;
