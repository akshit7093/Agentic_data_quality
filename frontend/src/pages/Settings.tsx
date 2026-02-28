import { useState } from 'react';
import {
  Brain,
  Database,
  Shield,
  Bell,
  Mail,
  Save,
  RefreshCw,
  CheckCircle,
  XCircle,
} from 'lucide-react';
import { useLLMHealth } from '@/hooks/useSystem';

interface LLMSettings {
  provider: string;
  ollama_base_url: string;
  ollama_model: string;
  lmstudio_base_url: string;
  openai_api_key: string;
  openai_model: string;
  anthropic_api_key: string;
  anthropic_model: string;
}

export default function SettingsPage() {
  const { data: llmHealth, refetch: refetchLLMHealth } = useLLMHealth();

  const [activeTab, setActiveTab] = useState('llm');
  const [isTesting, setIsTesting] = useState(false);
  const [llmSettings, setLLMSettings] = useState<LLMSettings>({
    provider: 'ollama',
    ollama_base_url: 'http://localhost:11434',
    ollama_model: 'llama3.2',
    lmstudio_base_url: 'http://localhost:1234/v1',
    openai_api_key: '',
    openai_model: 'gpt-4',
    anthropic_api_key: '',
    anthropic_model: 'claude-3-5-sonnet-20241022',
  });

  const handleTestConnection = async () => {
    setIsTesting(true);
    await refetchLLMHealth();
    setIsTesting(false);
  };

  const handleSave = async () => {
    // Would save settings to backend
    alert('Settings saved!');
  };

  const tabs = [
    { id: 'llm', name: 'LLM Configuration', icon: Brain },
    { id: 'database', name: 'Database', icon: Database },
    { id: 'notifications', name: 'Notifications', icon: Bell },
    { id: 'security', name: 'Security', icon: Shield },
  ];

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Settings</h1>
        <p className="mt-1 text-sm text-gray-500">
          Configure your AI Data Quality Agent
        </p>
      </div>

      <div className="flex space-x-6">
        {/* Sidebar tabs */}
        <div className="w-64 flex-shrink-0">
          <nav className="space-y-1">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`w-full flex items-center px-3 py-2 text-sm font-medium rounded-lg transition-colors ${activeTab === tab.id
                    ? 'bg-primary-50 text-primary-700'
                    : 'text-gray-700 hover:bg-gray-100'
                  }`}
              >
                <tab.icon
                  className={`w-5 h-5 mr-3 ${activeTab === tab.id ? 'text-primary-600' : 'text-gray-400'
                    }`}
                />
                {tab.name}
              </button>
            ))}
          </nav>
        </div>

        {/* Content */}
        <div className="flex-1">
          {activeTab === 'llm' && (
            <div className="card">
              <div className="card-header">
                <h2 className="text-lg font-semibold text-gray-900">
                  LLM Configuration
                </h2>
              </div>
              <div className="card-body space-y-6">
                {/* Provider Selection */}
                <div>
                  <label className="form-label">LLM Provider</label>
                  <div className="grid grid-cols-2 gap-4 mt-2">
                    <label className="flex items-center p-4 border border-gray-200 rounded-lg cursor-pointer hover:bg-gray-50">
                      <input
                        type="radio"
                        name="provider"
                        value="ollama"
                        checked={llmSettings.provider === 'ollama'}
                        onChange={(e) =>
                          setLLMSettings({
                            ...llmSettings,
                            provider: e.target.value,
                          })
                        }
                        className="mr-3"
                      />
                      <div>
                        <span className="text-sm font-medium text-gray-900">
                          Ollama (Local)
                        </span>
                        <p className="text-xs text-gray-500">
                          Run models locally on your machine
                        </p>
                      </div>
                    </label>

                    <label className="flex items-center p-4 border border-gray-200 rounded-lg cursor-pointer hover:bg-gray-50">
                      <input
                        type="radio"
                        name="provider"
                        value="lmstudio"
                        checked={llmSettings.provider === 'lmstudio'}
                        onChange={(e) =>
                          setLLMSettings({
                            ...llmSettings,
                            provider: e.target.value,
                          })
                        }
                        className="mr-3"
                      />
                      <div>
                        <span className="text-sm font-medium text-gray-900">
                          LM Studio
                        </span>
                        <p className="text-xs text-gray-500">
                          Use LM Studio local server
                        </p>
                      </div>
                    </label>

                    <label className="flex items-center p-4 border border-gray-200 rounded-lg cursor-pointer hover:bg-gray-50">
                      <input
                        type="radio"
                        name="provider"
                        value="openai"
                        checked={llmSettings.provider === 'openai'}
                        onChange={(e) =>
                          setLLMSettings({
                            ...llmSettings,
                            provider: e.target.value,
                          })
                        }
                        className="mr-3"
                      />
                      <div>
                        <span className="text-sm font-medium text-gray-900">
                          OpenAI
                        </span>
                        <p className="text-xs text-gray-500">
                          Use OpenAI API (GPT-4, etc.)
                        </p>
                      </div>
                    </label>

                    <label className="flex items-center p-4 border border-gray-200 rounded-lg cursor-pointer hover:bg-gray-50">
                      <input
                        type="radio"
                        name="provider"
                        value="anthropic"
                        checked={llmSettings.provider === 'anthropic'}
                        onChange={(e) =>
                          setLLMSettings({
                            ...llmSettings,
                            provider: e.target.value,
                          })
                        }
                        className="mr-3"
                      />
                      <div>
                        <span className="text-sm font-medium text-gray-900">
                          Anthropic
                        </span>
                        <p className="text-xs text-gray-500">
                          Use Anthropic API (Claude)
                        </p>
                      </div>
                    </label>
                  </div>
                </div>

                {/* Provider-specific settings */}
                {llmSettings.provider === 'ollama' && (
                  <div className="space-y-4 border-t border-gray-200 pt-4">
                    <h3 className="text-sm font-medium text-gray-900">
                      Ollama Settings
                    </h3>
                    <div>
                      <label className="form-label">Base URL</label>
                      <input
                        type="text"
                        className="form-input"
                        value={llmSettings.ollama_base_url}
                        onChange={(e) =>
                          setLLMSettings({
                            ...llmSettings,
                            ollama_base_url: e.target.value,
                          })
                        }
                      />
                    </div>
                    <div>
                      <label className="form-label">Model</label>
                      <select
                        className="form-select"
                        value={llmSettings.ollama_model}
                        onChange={(e) =>
                          setLLMSettings({
                            ...llmSettings,
                            ollama_model: e.target.value,
                          })
                        }
                      >
                        <option value="llama3.2">Llama 3.2</option>
                        <option value="llama3.1">Llama 3.1</option>
                        <option value="mistral">Mistral</option>
                        <option value="codellama">CodeLlama</option>
                        <option value="phi3">Phi-3</option>
                      </select>
                    </div>
                  </div>
                )}

                {llmSettings.provider === 'lmstudio' && (
                  <div className="space-y-4 border-t border-gray-200 pt-4">
                    <h3 className="text-sm font-medium text-gray-900">
                      LM Studio Settings
                    </h3>
                    <div>
                      <label className="form-label">Base URL</label>
                      <input
                        type="text"
                        className="form-input"
                        value={llmSettings.lmstudio_base_url}
                        onChange={(e) =>
                          setLLMSettings({
                            ...llmSettings,
                            lmstudio_base_url: e.target.value,
                          })
                        }
                      />
                      <p className="text-xs text-gray-500 mt-1">
                        Default: http://localhost:1234/v1
                      </p>
                    </div>
                  </div>
                )}

                {llmSettings.provider === 'openai' && (
                  <div className="space-y-4 border-t border-gray-200 pt-4">
                    <h3 className="text-sm font-medium text-gray-900">
                      OpenAI Settings
                    </h3>
                    <div>
                      <label className="form-label">API Key</label>
                      <input
                        type="password"
                        className="form-input"
                        placeholder="sk-..."
                        value={llmSettings.openai_api_key}
                        onChange={(e) =>
                          setLLMSettings({
                            ...llmSettings,
                            openai_api_key: e.target.value,
                          })
                        }
                      />
                    </div>
                    <div>
                      <label className="form-label">Model</label>
                      <select
                        className="form-select"
                        value={llmSettings.openai_model}
                        onChange={(e) =>
                          setLLMSettings({
                            ...llmSettings,
                            openai_model: e.target.value,
                          })
                        }
                      >
                        <option value="gpt-4">GPT-4</option>
                        <option value="gpt-4-turbo">GPT-4 Turbo</option>
                        <option value="gpt-3.5-turbo">GPT-3.5 Turbo</option>
                      </select>
                    </div>
                  </div>
                )}

                {llmSettings.provider === 'anthropic' && (
                  <div className="space-y-4 border-t border-gray-200 pt-4">
                    <h3 className="text-sm font-medium text-gray-900">
                      Anthropic Settings
                    </h3>
                    <div>
                      <label className="form-label">API Key</label>
                      <input
                        type="password"
                        className="form-input"
                        placeholder="sk-ant-..."
                        value={llmSettings.anthropic_api_key}
                        onChange={(e) =>
                          setLLMSettings({
                            ...llmSettings,
                            anthropic_api_key: e.target.value,
                          })
                        }
                      />
                    </div>
                    <div>
                      <label className="form-label">Model</label>
                      <select
                        className="form-select"
                        value={llmSettings.anthropic_model}
                        onChange={(e) =>
                          setLLMSettings({
                            ...llmSettings,
                            anthropic_model: e.target.value,
                          })
                        }
                      >
                        <option value="claude-3-5-sonnet-20241022">
                          Claude 3.5 Sonnet
                        </option>
                        <option value="claude-3-opus-20240229">
                          Claude 3 Opus
                        </option>
                        <option value="claude-3-sonnet-20240229">
                          Claude 3 Sonnet
                        </option>
                        <option value="claude-3-haiku-20240307">
                          Claude 3 Haiku
                        </option>
                      </select>
                    </div>
                  </div>
                )}

                {/* Connection Status */}
                <div className="border-t border-gray-200 pt-4">
                  <div className="flex items-center justify-between">
                    <div>
                      <h3 className="text-sm font-medium text-gray-900">
                        Connection Status
                      </h3>
                      <div className="flex items-center mt-2">
                        {llmHealth?.status === 'healthy' ? (
                          <>
                            <CheckCircle className="w-5 h-5 text-success-500 mr-2" />
                            <span className="text-sm text-success-600">
                              Connected to {llmHealth.provider} ({llmHealth.model})
                            </span>
                          </>
                        ) : (
                          <>
                            <XCircle className="w-5 h-5 text-danger-500 mr-2" />
                            <span className="text-sm text-danger-600">
                              {llmHealth?.error || 'Not connected'}
                            </span>
                          </>
                        )}
                      </div>
                    </div>
                    <button
                      onClick={handleTestConnection}
                      disabled={isTesting}
                      className="btn-secondary"
                    >
                      {isTesting ? (
                        <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                      ) : (
                        <RefreshCw className="w-4 h-4 mr-2" />
                      )}
                      Test Connection
                    </button>
                  </div>
                </div>

                {/* Save button */}
                <div className="flex justify-end pt-4 border-t border-gray-200">
                  <button onClick={handleSave} className="btn-primary">
                    <Save className="w-4 h-4 mr-2" />
                    Save Settings
                  </button>
                </div>
              </div>
            </div>
          )}

          {activeTab === 'database' && (
            <div className="card">
              <div className="card-header">
                <h2 className="text-lg font-semibold text-gray-900">
                  Database Configuration
                </h2>
              </div>
              <div className="card-body space-y-4">
                <div>
                  <label className="form-label">Database URL</label>
                  <input
                    type="text"
                    className="form-input"
                    placeholder="postgresql://user:pass@localhost:5432/dbname"
                  />
                </div>
                <div>
                  <label className="form-label">Redis URL</label>
                  <input
                    type="text"
                    className="form-input"
                    placeholder="redis://localhost:6379/0"
                  />
                </div>
                <div className="flex justify-end pt-4 border-t border-gray-200">
                  <button className="btn-primary">
                    <Save className="w-4 h-4 mr-2" />
                    Save Settings
                  </button>
                </div>
              </div>
            </div>
          )}

          {activeTab === 'notifications' && (
            <div className="card">
              <div className="card-header">
                <h2 className="text-lg font-semibold text-gray-900">
                  Notification Settings
                </h2>
              </div>
              <div className="card-body space-y-4">
                <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
                  <div className="flex items-center space-x-3">
                    <Mail className="w-5 h-5 text-gray-400" />
                    <div>
                      <p className="text-sm font-medium text-gray-900">
                        Email Notifications
                      </p>
                      <p className="text-xs text-gray-500">
                        Receive email alerts for validation failures
                      </p>
                    </div>
                  </div>
                  <input
                    type="checkbox"
                    className="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                  />
                </div>

                <div>
                  <label className="form-label">Email Recipients</label>
                  <input
                    type="text"
                    className="form-input"
                    placeholder="admin@example.com, team@example.com"
                  />
                </div>

                <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
                  <div className="flex items-center space-x-3">
                    <Bell className="w-5 h-5 text-gray-400" />
                    <div>
                      <p className="text-sm font-medium text-gray-900">
                        Slack Notifications
                      </p>
                      <p className="text-xs text-gray-500">
                        Send alerts to Slack channel
                      </p>
                    </div>
                  </div>
                  <input
                    type="checkbox"
                    className="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                  />
                </div>

                <div>
                  <label className="form-label">Slack Webhook URL</label>
                  <input
                    type="text"
                    className="form-input"
                    placeholder="https://hooks.slack.com/services/..."
                  />
                </div>

                <div className="flex justify-end pt-4 border-t border-gray-200">
                  <button className="btn-primary">
                    <Save className="w-4 h-4 mr-2" />
                    Save Settings
                  </button>
                </div>
              </div>
            </div>
          )}

          {activeTab === 'security' && (
            <div className="card">
              <div className="card-header">
                <h2 className="text-lg font-semibold text-gray-900">
                  Security Settings
                </h2>
              </div>
              <div className="card-body space-y-4">
                <div>
                  <label className="form-label">Secret Key</label>
                  <input
                    type="password"
                    className="form-input"
                    placeholder="Change secret key for JWT signing"
                  />
                </div>

                <div>
                  <label className="form-label">Access Token Expiry (minutes)</label>
                  <input
                    type="number"
                    className="form-input"
                    defaultValue={30}
                  />
                </div>

                <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
                  <div>
                    <p className="text-sm font-medium text-gray-900">
                      Enable CORS
                    </p>
                    <p className="text-xs text-gray-500">
                      Allow cross-origin requests
                    </p>
                  </div>
                  <input
                    type="checkbox"
                    defaultChecked
                    className="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                  />
                </div>

                <div className="flex justify-end pt-4 border-t border-gray-200">
                  <button className="btn-primary">
                    <Save className="w-4 h-4 mr-2" />
                    Save Settings
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
