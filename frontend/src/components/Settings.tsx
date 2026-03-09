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
  Info,
  Table,
  Plus,
  Trash2,
  Edit,
  Layout,
  X,
  ChevronDown,
  ChevronRight,
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

// ── Template types ─────────────────────────────────────────────
interface TplColumn {
  name: string;
  dtype_hint: string;
  description: string;
  required: boolean;
  aliases: string[];
}

interface DataTemplate {
  id: string;
  name: string;
  description: string;
  columns: TplColumn[];
  name_similarity_min: number;   // 0.0–1.0
  dtype_match_required: boolean;
  created_at?: string;
}

const DTYPE_OPTS = ['str', 'int', 'float', 'datetime', 'bool'];

// Map SQL-style type names (from seed) → dtype_hint values
const SQL_TO_DTYPE: Record<string, string> = {
  INTEGER: 'int', TEXT: 'str', REAL: 'float',
  BOOLEAN: 'bool', TIMESTAMP: 'datetime', DATE: 'datetime',
};

const DTYPE_BADGE: Record<string, string> = {
  str: 'bg-amber-500/15 text-amber-400 border-amber-500/30',
  int: 'bg-sky-500/15 text-sky-400 border-sky-500/30',
  float: 'bg-blue-500/15 text-blue-400 border-blue-500/30',
  datetime: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
  bool: 'bg-orange-500/15 text-orange-400 border-orange-500/30',
};

// ── Direct fetch helpers (bypasses broken templateApi stub) ────
const tplFetch = {
  list: async (): Promise<DataTemplate[]> => {
    const r = await fetch('/api/v1/templates');
    if (!r.ok) throw new Error('list failed');
    const d = await r.json();
    return d.templates ?? d ?? [];
  },
  create: async (body: Omit<DataTemplate, 'id' | 'created_at'>) => {
    const r = await fetch('/api/v1/templates', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) throw new Error('create failed');
    return r.json();
  },
  update: async (id: string, body: Partial<DataTemplate>) => {
    const r = await fetch(`/api/v1/templates/${id}`, {
      method: 'PUT', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) throw new Error('update failed');
    return r.json();
  },
  del: async (id: string) => {
    const r = await fetch(`/api/v1/templates/${id}`, { method: 'DELETE' });
    if (!r.ok) throw new Error('delete failed');
  },
};

// ── Empty column factory ───────────────────────────────────────
const emptyCol = (): TplColumn => ({ name: '', dtype_hint: 'str', description: '', required: false, aliases: [] });

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

  // ── Template state ────────────────────────────────────────────
  const [templates, setTemplates] = useState<DataTemplate[]>([]);
  const [tplLoading, setTplLoading] = useState(false);
  const [tplError, setTplError] = useState<string | null>(null);
  const [expandedTpl, setExpandedTpl] = useState<string | null>(null);
  const [isEditing, setIsEditing] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);   // null = create
  const [tplDraft, setTplDraft] = useState<Omit<DataTemplate, 'id' | 'created_at'>>({
    name: '', description: '', columns: [],
    name_similarity_min: 0.70, dtype_match_required: false,
  });
  const [tplSaving, setTplSaving] = useState(false);

  const loadTemplates = async () => {
    setTplLoading(true);
    setTplError(null);
    try {
      setTemplates(await tplFetch.list());
    } catch (e: any) {
      setTplError(e.message ?? 'Failed to load templates');
    } finally {
      setTplLoading(false);
    }
  };

  const openCreate = () => {
    setEditingId(null);
    setTplDraft({ name: '', description: '', columns: [emptyCol()], name_similarity_min: 0.70, dtype_match_required: false });
    setIsEditing(true);
  };

  const openEdit = (t: DataTemplate) => {
    setEditingId(t.id);
    setTplDraft({
      name: t.name, description: t.description,
      columns: t.columns.map(c => ({ ...c, aliases: c.aliases ?? [] })),
      name_similarity_min: t.name_similarity_min ?? 0.70,
      dtype_match_required: t.dtype_match_required ?? false,
    });
    setIsEditing(true);
  };

  const closeEditor = () => { setIsEditing(false); setEditingId(null); };

  const saveTemplate = async () => {
    if (!tplDraft.name.trim()) return;
    setTplSaving(true);
    try {
      const payload = { ...tplDraft, columns: tplDraft.columns.filter(c => c.name.trim()) };
      if (editingId) {
        await tplFetch.update(editingId, payload);
      } else {
        await tplFetch.create(payload);
      }
      closeEditor();
      await loadTemplates();
    } catch (e: any) {
      alert('Failed to save template: ' + (e.message ?? 'unknown error'));
    } finally {
      setTplSaving(false);
    }
  };

  const deleteTemplate = async (id: string, name: string) => {
    if (!confirm(`Delete template "${name}"? This cannot be undone.`)) return;
    try {
      await tplFetch.del(id);
      await loadTemplates();
    } catch (e: any) {
      alert('Failed to delete: ' + (e.message ?? ''));
    }
  };

  // Column helpers
  const setCol = (idx: number, patch: Partial<TplColumn>) =>
    setTplDraft(d => ({ ...d, columns: d.columns.map((c, i) => i === idx ? { ...c, ...patch } : c) }));
  const addCol = () => setTplDraft(d => ({ ...d, columns: [...d.columns, emptyCol()] }));
  const removeCol = (idx: number) => setTplDraft(d => ({ ...d, columns: d.columns.filter((_, i) => i !== idx) }));

  const handleTestConnection = async () => {
    setIsTesting(true);
    await refetchLLMHealth();
    setIsTesting(false);
  };

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

  useEffect(() => {
    if (activeTab === 'templates') loadTemplates();
  }, [activeTab]);

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
    { id: 'llm', name: 'LLM Configuration', icon: Cpu },
    { id: 'templates', name: 'Templates & Schemas', icon: Table },
    { id: 'database', name: 'Database', icon: Database },
    { id: 'notifications', name: 'Notifications', icon: Bell },
    { id: 'security', name: 'Security', icon: Shield },
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
              <tab.icon className={`w-5 h-5 mb-2 transition-colors ${activeTab === tab.id ? 'text-primary' : 'text-slate-500'}`} />
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
        {activeTab === 'templates' && (
          <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-xl font-bold text-slate-100 uppercase tracking-widest">Column Templates</h2>
                <p className="text-slate-400 mt-1 text-sm">Define expected schemas for fuzzy-matching incoming file columns in Visual Prep</p>
              </div>
              <div className="flex gap-2">
                <button
                  onClick={loadTemplates}
                  className="p-2 rounded-lg border border-royal-green-600 text-slate-400 hover:text-primary hover:border-primary transition-colors"
                >
                  <RefreshCw className={`w-4 h-4 ${tplLoading ? 'animate-spin' : ''}`} />
                </button>
                <button
                  onClick={openCreate}
                  className="flex items-center gap-2 bg-primary px-4 py-2 rounded-lg font-bold text-sm text-white hover:bg-primary/90 transition-all"
                >
                  <Plus className="w-4 h-4" /> Create Template
                </button>
              </div>
            </div>

            {/* Error banner */}
            {tplError && (
              <div className="flex items-center gap-3 p-4 rounded-xl bg-red-500/10 border border-red-500/30 text-red-400 text-sm">
                <XCircle className="w-5 h-5 shrink-0" />
                <span>{tplError}</span>
                <button onClick={loadTemplates} className="ml-auto text-xs underline">Retry</button>
              </div>
            )}

            {/* Loading */}
            {tplLoading && (
              <div className="flex items-center justify-center py-16">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary" />
              </div>
            )}

            {/* Empty state */}
            {!tplLoading && !tplError && templates.length === 0 && (
              <div className="rounded-xl border border-dashed border-royal-green-600 bg-royal-green-800/30 p-16 text-center flex flex-col items-center gap-4">
                <Layout className="w-12 h-12 text-slate-600" />
                <div>
                  <p className="text-slate-300 font-semibold text-lg">No templates yet</p>
                  <p className="text-slate-500 text-sm mt-1">Templates appear here once seeded or created. Check that the backend seed ran on startup.</p>
                </div>
                <button onClick={openCreate} className="flex items-center gap-2 bg-primary px-5 py-2 rounded-lg font-bold text-sm text-white hover:bg-primary/90 transition-all">
                  <Plus className="w-4 h-4" /> Create your first template
                </button>
              </div>
            )}

            {/* Template cards */}
            {!tplLoading && templates.length > 0 && (
              <div className="space-y-3">
                {templates.map(tpl => {
                  const expanded = expandedTpl === tpl.id;
                  return (
                    <div key={tpl.id} className="rounded-xl border border-royal-green-600 bg-royal-green-800/50 overflow-hidden transition-all">
                      {/* Card header */}
                      <div
                        className="flex items-center gap-4 px-6 py-4 cursor-pointer hover:bg-royal-green-800 transition-colors"
                        onClick={() => setExpandedTpl(expanded ? null : tpl.id)}
                      >
                        <div className="w-10 h-10 rounded-lg bg-primary/15 border border-primary/25 flex items-center justify-center shrink-0">
                          <Layout className="w-5 h-5 text-primary" />
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center flex-wrap gap-2">
                            <span className="text-slate-100 font-bold">{tpl.name}</span>
                            <span className="text-[10px] font-black uppercase tracking-widest bg-royal-green-700 text-slate-400 px-2 py-0.5 rounded">
                              {tpl.columns.length} cols
                            </span>
                            <span className={`text-[10px] font-black uppercase px-2 py-0.5 rounded border ${tpl.name_similarity_min >= 0.8 ? 'bg-primary/15 text-primary border-primary/30'
                                : tpl.name_similarity_min >= 0.65 ? 'bg-amber-500/15 text-amber-400 border-amber-500/30'
                                  : 'bg-red-500/15 text-red-400 border-red-500/30'
                              }`}>
                              name ≥ {Math.round((tpl.name_similarity_min ?? 0.7) * 100)}%
                            </span>
                            {tpl.dtype_match_required && (
                              <span className="text-[10px] font-black uppercase bg-purple-500/15 text-purple-400 border border-purple-500/30 px-2 py-0.5 rounded">
                                dtype enforced
                              </span>
                            )}
                          </div>
                          {tpl.description && <p className="text-slate-500 text-xs mt-0.5 truncate">{tpl.description}</p>}
                        </div>
                        <div className="flex items-center gap-1 shrink-0">
                          <button
                            onClick={e => { e.stopPropagation(); openEdit(tpl); }}
                            className="p-2 rounded-lg hover:bg-primary/10 text-slate-500 hover:text-primary transition-colors"
                            title="Edit"
                          >
                            <Edit className="w-4 h-4" />
                          </button>
                          <button
                            onClick={e => { e.stopPropagation(); deleteTemplate(tpl.id, tpl.name); }}
                            className="p-2 rounded-lg hover:bg-red-500/10 text-slate-500 hover:text-red-400 transition-colors"
                            title="Delete"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                          {expanded ? <ChevronDown className="w-4 h-4 text-slate-500 ml-1" /> : <ChevronRight className="w-4 h-4 text-slate-500 ml-1" />}
                        </div>
                      </div>

                      {/* Expanded column table */}
                      {expanded && (
                        <div className="border-t border-royal-green-700 overflow-x-auto">
                          <table className="w-full text-sm">
                            <thead className="bg-black/30 text-[10px] font-black uppercase tracking-widest text-slate-500">
                              <tr>
                                <th className="px-5 py-3 text-left">Column</th>
                                <th className="px-5 py-3 text-left">Type</th>
                                <th className="px-5 py-3 text-left">Description</th>
                                <th className="px-5 py-3 text-left">Aliases</th>
                                <th className="px-5 py-3 text-center">Req.</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-royal-green-700/40">
                              {tpl.columns.map((col, i) => (
                                <tr key={i} className="hover:bg-primary/3 transition-colors">
                                  <td className="px-5 py-3 font-mono font-bold text-slate-200 text-xs">{col.name}</td>
                                  <td className="px-5 py-3">
                                    <span className={`text-[10px] font-black uppercase px-2 py-0.5 rounded border ${DTYPE_BADGE[col.dtype_hint] ?? 'text-slate-400 bg-slate-500/10 border-slate-500/25'}`}>
                                      {col.dtype_hint}
                                    </span>
                                  </td>
                                  <td className="px-5 py-3 text-slate-400 text-xs">{col.description || '—'}</td>
                                  <td className="px-5 py-3">
                                    <div className="flex flex-wrap gap-1">
                                      {col.aliases?.length ? col.aliases.map(a => (
                                        <span key={a} className="text-[10px] font-mono bg-royal-green-700 text-slate-400 px-1.5 py-0.5 rounded">{a}</span>
                                      )) : <span className="text-slate-600 text-xs">—</span>}
                                    </div>
                                  </td>
                                  <td className="px-5 py-3 text-center">
                                    {col.required ? <span className="text-primary font-black">✓</span> : <span className="text-slate-600">—</span>}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}

            {/* ── Editor modal ── */}
            {isEditing && (
              <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-md p-4">
                <div className="bg-royal-green-800 border border-royal-green-600 rounded-2xl w-full max-w-3xl flex flex-col max-h-[92vh] shadow-2xl overflow-hidden animate-in fade-in zoom-in duration-200">
                  {/* Modal header */}
                  <div className="p-5 border-b border-royal-green-700 flex items-center justify-between bg-royal-green-900/60">
                    <h3 className="text-lg font-black text-white uppercase tracking-tight">
                      {editingId ? 'Edit Template' : 'New Template'}
                    </h3>
                    <button onClick={closeEditor} className="p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-royal-green-700 transition-colors">
                      <X className="w-5 h-5" />
                    </button>
                  </div>

                  {/* Modal body */}
                  <div className="flex-1 overflow-y-auto p-6 space-y-6">
                    {/* Name + description */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="space-y-1 md:col-span-2">
                        <label className="text-xs font-black text-slate-400 uppercase tracking-widest">Template Name *</label>
                        <input
                          value={tplDraft.name}
                          onChange={e => setTplDraft(d => ({ ...d, name: e.target.value }))}
                          placeholder="e.g. Customers"
                          className="w-full bg-black/20 border border-royal-green-700 rounded-lg px-4 py-2.5 text-white outline-none focus:border-primary focus:ring-1 focus:ring-primary transition-all"
                        />
                      </div>
                      <div className="space-y-1 md:col-span-2">
                        <label className="text-xs font-black text-slate-400 uppercase tracking-widest">Description</label>
                        <textarea
                          value={tplDraft.description}
                          onChange={e => setTplDraft(d => ({ ...d, description: e.target.value }))}
                          rows={2}
                          placeholder="Purpose of this template…"
                          className="w-full bg-black/20 border border-royal-green-700 rounded-lg px-4 py-2.5 text-white outline-none focus:border-primary focus:ring-1 focus:ring-primary transition-all resize-none"
                        />
                      </div>
                    </div>

                    {/* Matching settings */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 p-4 rounded-xl bg-black/20 border border-royal-green-700">
                      <div>
                        <div className="flex justify-between items-center mb-1">
                          <label className="text-xs font-black text-slate-400 uppercase tracking-widest">Name Similarity Min</label>
                          <span className={`text-sm font-black tabular-nums ${tplDraft.name_similarity_min >= 0.8 ? 'text-primary' : tplDraft.name_similarity_min >= 0.65 ? 'text-amber-400' : 'text-red-400'}`}>
                            {Math.round(tplDraft.name_similarity_min * 100)}%
                          </span>
                        </div>
                        <input
                          type="range" min={50} max={100} step={5}
                          value={Math.round(tplDraft.name_similarity_min * 100)}
                          onChange={e => setTplDraft(d => ({ ...d, name_similarity_min: Number(e.target.value) / 100 }))}
                          className="w-full accent-primary"
                        />
                        <div className="flex justify-between text-[9px] text-slate-600 mt-1 font-black uppercase tracking-widest">
                          <span>Fuzzy 50%</span><span>Exact 100%</span>
                        </div>
                      </div>
                      <div className="flex items-center justify-between">
                        <div>
                          <p className="text-xs font-black text-slate-400 uppercase tracking-widest">Require Dtype Match</p>
                          <p className="text-[11px] text-slate-600 mt-0.5">When ON, type must also match</p>
                        </div>
                        <button
                          onClick={() => setTplDraft(d => ({ ...d, dtype_match_required: !d.dtype_match_required }))}
                          className={`px-4 py-1.5 rounded-full text-xs font-black uppercase tracking-widest border transition-all ${tplDraft.dtype_match_required ? 'bg-primary/20 border-primary text-primary' : 'bg-royal-green-900 border-royal-green-600 text-slate-400'}`}
                        >
                          {tplDraft.dtype_match_required ? 'ON' : 'OFF'}
                        </button>
                      </div>
                    </div>

                    {/* Column definitions */}
                    <div>
                      <div className="flex items-center justify-between mb-3">
                        <h4 className="text-xs font-black text-slate-400 uppercase tracking-widest">
                          Column Definitions ({tplDraft.columns.filter(c => c.name.trim()).length})
                        </h4>
                        <button onClick={addCol} className="text-xs font-bold text-primary hover:text-primary/80 flex items-center gap-1">
                          <Plus className="w-3 h-3" /> Add Column
                        </button>
                      </div>

                      <div className="space-y-2">
                        {/* Header row */}
                        <div className="grid grid-cols-[1fr_90px_1fr_1fr_56px_28px] gap-2 px-2 text-[9px] font-black uppercase tracking-widest text-slate-600">
                          <span>Name *</span><span>Type</span><span>Description</span><span>Aliases (comma-sep)</span><span className="text-center">Req.</span><span />
                        </div>
                        {tplDraft.columns.map((col, idx) => (
                          <div key={idx} className="grid grid-cols-[1fr_90px_1fr_1fr_56px_28px] gap-2 items-center bg-black/10 px-2 py-2 rounded-lg border border-royal-green-700/40">
                            <input
                              placeholder="column_name"
                              value={col.name}
                              onChange={e => setCol(idx, { name: e.target.value })}
                              className="bg-transparent text-sm font-mono text-white outline-none border-b border-royal-green-700 focus:border-primary px-1 py-0.5"
                            />
                            <select
                              value={col.dtype_hint}
                              onChange={e => setCol(idx, { dtype_hint: e.target.value })}
                              className="bg-royal-green-700 text-[11px] rounded px-2 py-1 outline-none text-slate-200 border-none"
                            >
                              {DTYPE_OPTS.map(o => <option key={o} value={o}>{o}</option>)}
                            </select>
                            <input
                              placeholder="Description…"
                              value={col.description}
                              onChange={e => setCol(idx, { description: e.target.value })}
                              className="bg-transparent text-xs text-slate-300 outline-none border-b border-royal-green-700 focus:border-primary px-1 py-0.5"
                            />
                            <input
                              placeholder="alias1, alias2"
                              value={col.aliases.join(', ')}
                              onChange={e => setCol(idx, { aliases: e.target.value.split(',').map(s => s.trim()).filter(Boolean) })}
                              className="bg-transparent text-xs font-mono text-slate-400 outline-none border-b border-royal-green-700 focus:border-primary px-1 py-0.5"
                            />
                            <div className="flex justify-center">
                              <button
                                onClick={() => setCol(idx, { required: !col.required })}
                                className={`w-8 h-5 rounded-full transition-all ${col.required ? 'bg-primary' : 'bg-royal-green-700'}`}
                              >
                                <div className={`w-3.5 h-3.5 rounded-full bg-white shadow transition-transform mx-0.5 ${col.required ? 'translate-x-2.5' : 'translate-x-0'}`} />
                              </button>
                            </div>
                            <button onClick={() => removeCol(idx)} className="text-slate-600 hover:text-red-400 transition-colors p-1">
                              <X className="w-3.5 h-3.5" />
                            </button>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>

                  {/* Modal footer */}
                  <div className="p-5 border-t border-royal-green-700 bg-royal-green-900/60 flex justify-end gap-3">
                    <button onClick={closeEditor} className="px-6 py-2.5 rounded-lg font-bold text-sm text-slate-400 hover:text-white transition-all">
                      Cancel
                    </button>
                    <button
                      onClick={saveTemplate}
                      disabled={tplSaving || !tplDraft.name.trim()}
                      className="px-6 py-2.5 rounded-lg bg-primary font-bold text-sm text-white hover:bg-primary/90 transition-all flex items-center gap-2 disabled:opacity-50"
                    >
                      {tplSaving ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
                      {tplSaving ? 'Saving…' : editingId ? 'Save Changes' : 'Create Template'}
                    </button>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

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
