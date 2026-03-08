import { useState, useEffect } from 'react';
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
  Cpu,
  Box,
  Cloud,
  Zap,
  Globe,
  Server,
  Settings2,
  Info
} from 'lucide-react';
import { useLLMHealth } from '@/hooks/useSystem';
import { systemApi } from '@/services/api';

interface LLMSettings {
  provider: string;
  ollama_base_url: string;
  ollama_model: string;
  lmstudio_base_url: string;
  openai_api_key: string;
  openai_model: string;
  anthropic_api_key: string;
  anthropic_model: string;
  gemini_api_key: string;
  gemini_model: string;
  groq_api_keys: string;
  groq_model: string;
  openrouter_api_keys: string;
  openrouter_model: string;
}

const PROVIDERS = [
  { id: 'ollama', name: 'Ollama', type: 'Local Hosting', icon: Server },
  { id: 'lmstudio', name: 'LM Studio', type: 'Local Desktop', icon: Box },
  { id: 'openai', name: 'OpenAI', type: 'Cloud API', icon: Cloud },
  { id: 'anthropic', name: 'Anthropic', type: 'Cloud', icon: Brain },
  { id: 'gemini', name: 'Google Gemini', type: 'Fast Cloud', icon: Zap },
  { id: 'groq', name: 'Groq LPU', type: 'Hyper-fast', icon: Cpu },
  { id: 'openrouter', name: 'OpenRouter', type: 'Aggregated', icon: Globe },
];

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
    gemini_api_key: '',
    gemini_model: 'gemini-1.5-pro',
    groq_api_keys: '',
    groq_model: 'llama3-70b-8192',
    openrouter_api_keys: '',
    openrouter_model: 'google/gemini-pro-1.5',
  });

  const [saving, setSaving] = useState(false);

  useEffect(() => {
    const loadSettings = async () => {
      try {
        const data = await systemApi.getSettings();
        setLLMSettings(data);
      } catch (e) {
        console.error("Failed to load LLM settings from backend.", e);
      }
    };
    loadSettings();
  }, []);

  const handleTestConnection = async () => {
    setIsTesting(true);
    await refetchLLMHealth();
    setIsTesting(false);
  };

  const handleSave = async () => {
    try {
      setSaving(true);
      await systemApi.updateSettings(llmSettings);
      alert('Settings saved and successfully updated on the backend!');
      await refetchLLMHealth();
    } catch (e) {
      console.error(e);
      alert('Failed to save settings to backend.');
    } finally {
      setSaving(false);
    }
  };

  const tabs = [
    { id: 'llm', name: 'LLM Configuration' },
    { id: 'database', name: 'Database' },
    { id: 'notifications', name: 'Notifications' },
    { id: 'security', name: 'Security' },
  ];

  return (
    <div className="max-w-5xl">
      {/* Tabs Navigation */}
      <div className="mb-8 border-b border-royal-green-600">
        <div className="flex gap-8 overflow-x-auto pb-px">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`flex flex-col items-center border-b-2 px-1 pb-4 transition-all ${activeTab === tab.id
                ? 'border-primary text-primary'
                : 'border-transparent text-slate-400 hover:text-slate-200'
                }`}
            >
              <span className="text-sm font-bold whitespace-nowrap">{tab.name}</span>
            </button>
          ))}
        </div>
      </div>

      <div className="max-w-5xl">
        {activeTab === 'llm' && (
          <>
            {/* Header Section */}
            <div className="mb-8">
              <h1 className="text-2xl text-slate-100 font-bold mb-2">LLM Provider Configuration</h1>
              <p className="text-slate-400">Select and configure your preferred Large Language Model provider for system-wide operations.</p>
            </div>

            {/* Provider Cards Grid */}
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-7 gap-4 mb-10">
              {PROVIDERS.map((provider) => {
                const isActive = llmSettings.provider === provider.id;
                return (
                  <div
                    key={provider.id}
                    onClick={() => setLLMSettings({ ...llmSettings, provider: provider.id })}
                    className={`relative group cursor-pointer overflow-hidden rounded-xl border-2 p-4 transition-all ${isActive
                      ? 'border-primary bg-primary/5 hover:bg-primary/10'
                      : 'border-royal-green-600 bg-royal-green-800 hover:border-primary/50'
                      }`}
                  >
                    {isActive && (
                      <div className="absolute top-2 right-2 text-primary">
                        <CheckCircle className="w-4 h-4" />
                      </div>
                    )}
                    <div className={`mb-3 flex h-10 w-10 items-center justify-center rounded-lg transition-colors ${isActive
                      ? 'bg-primary text-white'
                      : 'bg-royal-green-700 text-slate-300 group-hover:bg-primary/20 group-hover:text-primary'
                      }`}>
                      <provider.icon className="w-5 h-5" />
                    </div>
                    <h3 className="font-bold text-[13px] text-slate-100 whitespace-nowrap">{provider.name}</h3>
                    <p className={`text-[10px] mt-1 whitespace-nowrap overflow-hidden text-ellipsis ${isActive ? 'text-primary font-medium' : 'text-slate-500'
                      }`}>
                      {provider.type}
                    </p>
                  </div>
                );
              })}
            </div>

            {/* Configuration Form */}
            <div className="rounded-xl border border-royal-green-600 bg-royal-green-800 p-6 lg:p-8">
              <h2 className="text-lg text-slate-100 font-bold mb-6 flex items-center gap-2">
                <Settings2 className="w-5 h-5 text-primary" />
                {PROVIDERS.find(p => p.id === llmSettings.provider)?.name} Settings
              </h2>

              <div className="space-y-6">
                {llmSettings.provider === 'ollama' && (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">API Host URL</label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="text"
                        value={llmSettings.ollama_base_url}
                        onChange={(e) => setLLMSettings({ ...llmSettings, ollama_base_url: e.target.value })}
                      />
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">Model</label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="text"
                        value={llmSettings.ollama_model}
                        onChange={(e) => setLLMSettings({ ...llmSettings, ollama_model: e.target.value })}
                      />
                    </div>
                  </div>
                )}

                {llmSettings.provider === 'lmstudio' && (
                  <div className="grid grid-cols-1 gap-6">
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">API Host URL</label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="text"
                        value={llmSettings.lmstudio_base_url}
                        onChange={(e) => setLLMSettings({ ...llmSettings, lmstudio_base_url: e.target.value })}
                      />
                    </div>
                  </div>
                )}

                {llmSettings.provider === 'openai' && (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">API Key</label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="password"
                        placeholder="sk-..."
                        value={llmSettings.openai_api_key}
                        onChange={(e) => setLLMSettings({ ...llmSettings, openai_api_key: e.target.value })}
                      />
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">Model</label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="text"
                        value={llmSettings.openai_model}
                        onChange={(e) => setLLMSettings({ ...llmSettings, openai_model: e.target.value })}
                      />
                    </div>
                  </div>
                )}

                {llmSettings.provider === 'anthropic' && (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">API Key</label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="password"
                        placeholder="sk-ant-..."
                        value={llmSettings.anthropic_api_key}
                        onChange={(e) => setLLMSettings({ ...llmSettings, anthropic_api_key: e.target.value })}
                      />
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">Model</label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="text"
                        value={llmSettings.anthropic_model}
                        onChange={(e) => setLLMSettings({ ...llmSettings, anthropic_model: e.target.value })}
                      />
                    </div>
                  </div>
                )}

                {llmSettings.provider === 'gemini' && (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">API Key</label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="password"
                        placeholder="AIza..."
                        value={llmSettings.gemini_api_key}
                        onChange={(e) => setLLMSettings({ ...llmSettings, gemini_api_key: e.target.value })}
                      />
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">Model</label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="text"
                        value={llmSettings.gemini_model}
                        onChange={(e) => setLLMSettings({ ...llmSettings, gemini_model: e.target.value })}
                      />
                    </div>
                  </div>
                )}

                {llmSettings.provider === 'groq' && (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">API Key(s) <span className="text-xs text-slate-500 font-normal ml-2">Comma separated for rotation</span></label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="password"
                        placeholder="gsk_..., gsk_..."
                        value={llmSettings.groq_api_keys}
                        onChange={(e) => setLLMSettings({ ...llmSettings, groq_api_keys: e.target.value })}
                      />
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">Model</label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="text"
                        value={llmSettings.groq_model}
                        onChange={(e) => setLLMSettings({ ...llmSettings, groq_model: e.target.value })}
                      />
                    </div>
                  </div>
                )}

                {llmSettings.provider === 'openrouter' && (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">API Key(s) <span className="text-xs text-slate-500 font-normal ml-2">Comma separated for rotation</span></label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="password"
                        placeholder="sk-or-v1-..."
                        value={llmSettings.openrouter_api_keys}
                        onChange={(e) => setLLMSettings({ ...llmSettings, openrouter_api_keys: e.target.value })}
                      />
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-slate-300">Model</label>
                      <input
                        className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                        type="text"
                        value={llmSettings.openrouter_model}
                        onChange={(e) => setLLMSettings({ ...llmSettings, openrouter_model: e.target.value })}
                      />
                    </div>
                  </div>
                )}

                <div className="flex items-center gap-4 pt-4 border-t border-royal-green-600">
                  <button
                    onClick={handleSave}
                    disabled={saving}
                    className="flex items-center justify-center gap-2 rounded-lg bg-primary px-8 py-3 font-bold text-white transition-all hover:bg-primary/90 active:scale-95 disabled:opacity-50"
                  >
                    <Save className="w-5 h-5" />
                    {saving ? 'Saving...' : 'Save Settings'}
                  </button>
                  <button
                    onClick={handleTestConnection}
                    disabled={isTesting}
                    className="flex items-center justify-center gap-2 rounded-lg border border-royal-green-600 bg-transparent px-8 py-3 font-bold text-slate-300 transition-all hover:bg-royal-green-700 active:scale-95 disabled:opacity-50"
                  >
                    <RefreshCw className={`w-5 h-5 ${isTesting ? 'animate-spin' : ''}`} />
                    Test Connection
                  </button>
                </div>
              </div>
            </div>

            {/* Secondary Alert/Info Section */}
            <div className="mt-8 grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="rounded-xl border border-primary/20 bg-primary/5 p-4 flex gap-4">
                <div className="text-primary pt-1">
                  {llmHealth?.status === 'healthy' ? (
                    <CheckCircle className="w-5 h-5" />
                  ) : (
                    <XCircle className="w-5 h-5 text-red-500" />
                  )}
                </div>
                <div>
                  <h4 className="font-bold text-primary">Connection Status</h4>
                  <p className="text-sm text-slate-400 mt-1">
                    {llmHealth?.status === 'healthy'
                      ? `Connected to ${llmHealth.provider} (${llmHealth.model})`
                      : llmHealth?.error || 'Not connected'}
                  </p>
                </div>
              </div>

              <div className="rounded-xl border border-royal-green-600 bg-royal-green-800 p-4 flex gap-4">
                <div className="text-yellow-500 pt-1">
                  <Info className="w-5 h-5" />
                </div>
                <div>
                  <h4 className="font-bold text-yellow-500">Provider Information</h4>
                  <p className="text-sm text-slate-400 mt-1">Make sure you have valid API keys for cloud providers. Local providers require the engine to be running.</p>
                </div>
              </div>
            </div>
          </>
        )}

        {/* Database, Notifications, Security tabs follow the same dark mode standard */}
        {activeTab === 'database' && (
          <div className="rounded-xl border border-royal-green-600 bg-royal-green-800 p-6 lg:p-8">
            <h2 className="text-lg font-bold mb-6 text-slate-100 flex items-center gap-2">
              <Database className="w-5 h-5 text-primary" />
              Database Configuration
            </h2>
            <div className="space-y-6">
              <div className="space-y-2">
                <label className="text-sm font-medium text-slate-300">Database URL</label>
                <input
                  type="text"
                  className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                  placeholder="postgresql://user:pass@localhost:5432/dbname"
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium text-slate-300">Redis URL</label>
                <input
                  type="text"
                  className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                  placeholder="redis://localhost:6379/0"
                />
              </div>
              <div className="flex justify-start pt-4 border-t border-royal-green-600">
                <button className="flex items-center justify-center gap-2 rounded-lg bg-primary px-8 py-3 font-bold text-white transition-all hover:bg-primary/90 active:scale-95">
                  <Save className="w-5 h-5" />
                  Save Settings
                </button>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'notifications' && (
          <div className="rounded-xl border border-royal-green-600 bg-royal-green-800 p-6 lg:p-8">
            <h2 className="text-lg font-bold mb-6 text-slate-100 flex items-center gap-2">
              <Bell className="w-5 h-5 text-primary" />
              Notification Settings
            </h2>
            <div className="space-y-6">
              {/* Email */}
              <div className="flex flex-col space-y-4">
                <div className="flex items-center justify-between p-4 bg-royal-green-900 rounded-lg border border-royal-green-600">
                  <div className="flex items-center space-x-3">
                    <Mail className="w-5 h-5 text-slate-400" />
                    <div>
                      <p className="text-sm font-medium text-slate-100">Email Notifications</p>
                      <p className="text-xs text-slate-400">Receive email alerts for validation failures</p>
                    </div>
                  </div>
                  <input type="checkbox" className="w-4 h-4 rounded border-royal-green-600 bg-royal-green-800 text-primary focus:ring-primary focus:ring-offset-royal-green-900 accent-primary" />
                </div>
                <div className="space-y-2 pl-2">
                  <label className="text-sm font-medium text-slate-300">Email Recipients</label>
                  <input
                    type="text"
                    className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                    placeholder="admin@example.com, team@example.com"
                  />
                </div>
              </div>

              {/* Slack */}
              <div className="flex flex-col space-y-4 pt-4 border-t border-royal-green-600">
                <div className="flex items-center justify-between p-4 bg-royal-green-900 rounded-lg border border-royal-green-600">
                  <div className="flex items-center space-x-3">
                    <Bell className="w-5 h-5 text-slate-400" />
                    <div>
                      <p className="text-sm font-medium text-slate-100">Slack Notifications</p>
                      <p className="text-xs text-slate-400">Send alerts to Slack channel</p>
                    </div>
                  </div>
                  <input type="checkbox" className="w-4 h-4 rounded border-royal-green-600 bg-royal-green-800 text-primary focus:ring-primary focus:ring-offset-royal-green-900 accent-primary" />
                </div>
                <div className="space-y-2 pl-2">
                  <label className="text-sm font-medium text-slate-300">Slack Webhook URL</label>
                  <input
                    type="text"
                    className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                    placeholder="https://hooks.slack.com/services/..."
                  />
                </div>
              </div>

              <div className="flex justify-start pt-4 border-t border-royal-green-600">
                <button className="flex items-center justify-center gap-2 rounded-lg bg-primary px-8 py-3 font-bold text-white transition-all hover:bg-primary/90 active:scale-95">
                  <Save className="w-5 h-5" />
                  Save Settings
                </button>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'security' && (
          <div className="rounded-xl border border-royal-green-600 bg-royal-green-800 p-6 lg:p-8">
            <h2 className="text-lg font-bold mb-6 text-slate-100 flex items-center gap-2">
              <Shield className="w-5 h-5 text-primary" />
              Security Settings
            </h2>
            <div className="space-y-6">
              <div className="space-y-2">
                <label className="text-sm font-medium text-slate-300">Secret Key</label>
                <input
                  type="password"
                  className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                  placeholder="Change secret key for JWT signing"
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium text-slate-300">Access Token Expiry (minutes)</label>
                <input
                  type="number"
                  className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
                  defaultValue={30}
                />
              </div>
              <div className="flex items-center justify-between p-4 bg-royal-green-900 rounded-lg border border-royal-green-600">
                <div>
                  <p className="text-sm font-medium text-slate-100">Enable CORS</p>
                  <p className="text-xs text-slate-400">Allow cross-origin requests</p>
                </div>
                <input
                  type="checkbox"
                  defaultChecked
                  className="w-4 h-4 rounded border-royal-green-600 bg-royal-green-800 text-primary focus:ring-primary focus:ring-offset-royal-green-900 accent-primary"
                />
              </div>
              <div className="flex justify-start pt-4 border-t border-royal-green-600">
                <button className="flex items-center justify-center gap-2 rounded-lg bg-primary px-8 py-3 font-bold text-white transition-all hover:bg-primary/90 active:scale-95">
                  <Save className="w-5 h-5" />
                  Save Settings
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
