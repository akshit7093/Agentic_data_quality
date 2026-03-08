import { useState, useEffect, useCallback } from 'react';
import {
  Plus, Edit2, Trash2, Search, ListChecks, Brain, Code,
  BarChart3, FileText, FolderOpen, ChevronDown, ChevronRight,
  Save, ToggleLeft, ToggleRight, Layers,
} from 'lucide-react';
import Modal from '@/components/Modal';
import { ruleGroupApi } from '@/services/api';

// ── Types ──────────────────────────────────────────────────

interface GroupRule {
  id: string;
  rule_name: string;
  target_file: string;
  rule_type: string;
  severity: string;
  query: string;
  query_type: string;
  description: string;
  is_active: boolean;
  created_at: string;
}

interface RuleGroup {
  id: string;
  name: string;
  description: string;
  target_files: string[];
  rules: GroupRule[];
  rule_count: number;
  active_rules: number;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

// ── Component ──────────────────────────────────────────────

export default function Rules() {
  const [groups, setGroups] = useState<RuleGroup[]>([]);
  const [expandedGroup, setExpandedGroup] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [loading, setLoading] = useState(true);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showAddRuleModal, setShowAddRuleModal] = useState<string | null>(null);
  const [editingRule, setEditingRule] = useState<{ groupId: string; rule: GroupRule } | null>(null);

  // Form state for creating groups
  const [newGroupName, setNewGroupName] = useState('');
  const [newGroupDesc, setNewGroupDesc] = useState('');
  const [newGroupFiles, setNewGroupFiles] = useState('');

  // Form state for adding/editing rules
  const [ruleForm, setRuleForm] = useState({
    rule_name: '',
    target_file: '',
    rule_type: 'column',
    severity: 'warning',
    query: '',
    query_type: 'sql',
    description: '',
    is_active: true,
  });

  // ── Data Loading ──────────────────────────────────────────

  const loadGroups = useCallback(async () => {
    try {
      setLoading(true);
      const data = await ruleGroupApi.listGroups();
      setGroups(data.groups || []);
    } catch {
      console.error('Failed to load rule groups');
      setGroups([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadGroups(); }, [loadGroups]);

  const expandGroup = async (groupId: string) => {
    if (expandedGroup === groupId) {
      setExpandedGroup(null);
      return;
    }
    try {
      const data = await ruleGroupApi.getGroup(groupId);
      setGroups(prev => prev.map(g =>
        g.id === groupId ? { ...g, ...data.group } : g
      ));
      setExpandedGroup(groupId);
    } catch {
      console.error('Failed to load group details');
    }
  };

  // ── Group CRUD ────────────────────────────────────────────

  const createGroup = async () => {
    if (!newGroupName.trim()) return;
    try {
      await ruleGroupApi.createGroup({
        name: newGroupName.trim(),
        description: newGroupDesc.trim(),
        target_files: newGroupFiles.split(',').map(f => f.trim()).filter(Boolean),
      });
      setShowCreateModal(false);
      setNewGroupName('');
      setNewGroupDesc('');
      setNewGroupFiles('');
      loadGroups();
    } catch {
      console.error('Failed to create group');
    }
  };

  const deleteGroup = async (groupId: string) => {
    if (!confirm('Delete this group and all its rules?')) return;
    try {
      await ruleGroupApi.deleteGroup(groupId);
      loadGroups();
    } catch {
      console.error('Failed to delete group');
    }
  };

  const toggleGroup = async (groupId: string, isActive: boolean) => {
    try {
      await ruleGroupApi.updateGroup(groupId, { is_active: !isActive });
      loadGroups();
    } catch {
      console.error('Failed to toggle group');
    }
  };

  // ── Rule CRUD ─────────────────────────────────────────────

  const addRule = async (groupId: string) => {
    if (!ruleForm.rule_name.trim() || !ruleForm.query.trim()) return;
    try {
      await ruleGroupApi.addRule(groupId, ruleForm);
      setShowAddRuleModal(null);
      resetRuleForm();
      const data = await ruleGroupApi.getGroup(groupId);
      setGroups(prev => prev.map(g =>
        g.id === groupId ? { ...g, ...data.group } : g
      ));
      loadGroups();
    } catch {
      console.error('Failed to add rule');
    }
  };

  const updateRule = async () => {
    if (!editingRule) return;
    try {
      await ruleGroupApi.updateRule(editingRule.groupId, editingRule.rule.id, ruleForm);
      setEditingRule(null);
      resetRuleForm();
      const data = await ruleGroupApi.getGroup(editingRule.groupId);
      setGroups(prev => prev.map(g =>
        g.id === editingRule.groupId ? { ...g, ...data.group } : g
      ));
    } catch {
      console.error('Failed to update rule');
    }
  };

  const deleteRule = async (groupId: string, ruleId: string) => {
    if (!confirm('Delete this rule?')) return;
    try {
      await ruleGroupApi.deleteRule(groupId, ruleId);
      const data = await ruleGroupApi.getGroup(groupId);
      setGroups(prev => prev.map(g =>
        g.id === groupId ? { ...g, ...data.group } : g
      ));
      loadGroups();
    } catch {
      console.error('Failed to delete rule');
    }
  };

  const resetRuleForm = () => {
    setRuleForm({
      rule_name: '', target_file: '', rule_type: 'column',
      severity: 'warning', query: '', query_type: 'sql',
      description: '', is_active: true,
    });
  };

  const startEditRule = (groupId: string, rule: GroupRule) => {
    setEditingRule({ groupId, rule });
    setRuleForm({
      rule_name: rule.rule_name,
      target_file: rule.target_file,
      rule_type: rule.rule_type,
      severity: rule.severity,
      query: rule.query,
      query_type: rule.query_type,
      description: rule.description,
      is_active: rule.is_active,
    });
  };

  // ── Helpers ───────────────────────────────────────────────

  const getRuleTypeIcon = (type: string) => {
    switch (type) {
      case 'column': return <ListChecks className="w-4 h-4" />;
      case 'statistical': return <BarChart3 className="w-4 h-4" />;
      case 'custom_sql':
      case 'custom_pandas': return <Code className="w-4 h-4" />;
      default: return <Brain className="w-4 h-4" />;
    }
  };

  const getSeverityColor = (severity: string) => {
    switch (severity) {
      case 'critical': return 'bg-red-500/10 text-red-400 border-red-500/20';
      case 'warning': return 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20';
      default: return 'bg-primary/10 text-primary border-primary/20';
    }
  };

  const filteredGroups = groups.filter(g =>
    g.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    g.description.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
            <Layers className="w-6 h-6 text-primary" />
            Rule Groups
          </h1>
          <p className="mt-1 text-sm text-slate-400">
            Organize validation rules into named groups for modular execution
          </p>
        </div>
        <button
          onClick={() => setShowCreateModal(true)}
          className="glow-button flex justify-center items-center gap-2 rounded-lg bg-primary px-4 py-2 text-sm font-bold text-white transition-all hover:bg-primary/90 active:scale-95 whitespace-nowrap"
        >
          <Plus className="w-4 h-4" /> New Group
        </button>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-500" />
        <input
          type="text"
          placeholder="Search groups..."
          value={searchTerm}
          onChange={e => setSearchTerm(e.target.value)}
          className="w-full rounded-xl border border-royal-green-600 bg-royal-green-800/50 pl-11 pr-4 py-3 text-slate-100 placeholder:text-slate-500 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
        />
      </div>

      {/* Groups List */}
      {loading ? (
        <div className="flex flex-col items-center justify-center p-16 rounded-xl border border-royal-green-600 bg-royal-green-800/50">
          <div className="animate-spin rounded-full h-10 w-10 border-b-2 border-primary mb-4"></div>
          <span className="text-slate-400 font-medium">Loading rule groups...</span>
        </div>
      ) : filteredGroups.length === 0 ? (
        <div className="rounded-xl border border-royal-green-600 bg-royal-green-800 p-12 text-center flex flex-col items-center">
          <div className="mb-6 flex h-20 w-20 items-center justify-center rounded-full bg-royal-green-900 border border-royal-green-700">
            <FolderOpen className="w-10 h-10 text-slate-500" />
          </div>
          <p className="text-slate-300 font-medium text-lg">
            {groups.length === 0 ? 'No rule groups yet. Create one to get started!' : 'No groups match your search.'}
          </p>
        </div>
      ) : (
        <div className="space-y-4">
          {filteredGroups.map(group => (
            <div key={group.id} className="rounded-xl border border-royal-green-600 bg-royal-green-800 overflow-hidden transition-all duration-200">
              {/* Group Header */}
              <div
                onClick={() => expandGroup(group.id)}
                className={`flex items-center justify-between p-4 cursor-pointer hover:bg-royal-green-700/30 transition-colors ${expandedGroup === group.id ? 'border-b border-royal-green-600 bg-royal-green-900/30' : ''}`}
              >
                <div className="flex items-start gap-4 flex-1">
                  <div className="mt-1 border rounded-md border-royal-green-600 bg-royal-green-900 p-1 text-slate-400">
                    {expandedGroup === group.id
                      ? <ChevronDown className="w-4 h-4 text-primary" />
                      : <ChevronRight className="w-4 h-4" />
                    }
                  </div>
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="font-bold text-slate-100 text-base">{group.name}</span>
                      {!group.is_active && (
                        <span className="text-xs font-medium text-slate-500 bg-slate-800 px-2 py-0.5 rounded border border-slate-700">Disabled</span>
                      )}
                    </div>
                    {group.description && (
                      <p className="text-sm text-slate-400 mt-1 line-clamp-1">{group.description}</p>
                    )}
                  </div>
                </div>
                <div className="flex items-center gap-4 ml-4">
                  {group.target_files?.length > 0 && (
                    <div className="hidden sm:flex items-center gap-1.5 text-xs font-semibold px-2.5 py-1 rounded bg-royal-green-900 text-slate-300 border border-royal-green-700">
                      <FileText className="w-3.5 h-3.5 text-slate-400" />
                      {group.target_files.length} file{group.target_files.length > 1 ? 's' : ''}
                    </div>
                  )}
                  <span className="text-xs font-bold px-2.5 py-1 rounded bg-primary/10 text-primary border border-primary/20 whitespace-nowrap">
                    {group.rule_count ?? group.rules?.length ?? 0} rules
                  </span>

                  {/* Actions wrapper */}
                  <div className="flex items-center gap-2 border-l border-royal-green-600 pl-4">
                    <button
                      onClick={e => { e.stopPropagation(); toggleGroup(group.id, group.is_active); }}
                      className="p-1.5 rounded hover:bg-royal-green-700 transition-colors"
                      title={group.is_active ? 'Disable group' : 'Enable group'}
                    >
                      {group.is_active
                        ? <ToggleRight className="w-5 h-5 text-emerald-500" />
                        : <ToggleLeft className="w-5 h-5 text-slate-500" />
                      }
                    </button>
                    <button
                      onClick={e => { e.stopPropagation(); deleteGroup(group.id); }}
                      className="p-1.5 rounded hover:bg-red-500/20 text-slate-400 hover:text-red-400 transition-colors"
                      title="Delete group"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              </div>

              {/* Expanded: Rules List */}
              {expandedGroup === group.id && (
                <div className="p-5 bg-royal-green-900/30">
                  {/* Target files chips */}
                  {group.target_files?.length > 0 && (
                    <div className="flex items-center gap-2 flex-wrap mb-5">
                      <span className="text-xs font-medium text-slate-500 mr-1">Targets:</span>
                      {group.target_files.map((f, i) => (
                        <span key={i} className="flex items-center gap-1.5 text-xs font-medium px-2.5 py-1 rounded-full bg-royal-green-800 text-slate-300 border border-royal-green-600">
                          <FileText className="w-3 h-3 text-primary" />
                          {f}
                        </span>
                      ))}
                    </div>
                  )}

                  {/* Rules */}
                  {group.rules?.length > 0 ? (
                    <div className="space-y-3">
                      {group.rules.map(rule => (
                        <div key={rule.id} className={`flex flex-col lg:flex-row lg:items-center justify-between gap-4 p-4 rounded-xl border border-royal-green-700 bg-royal-green-800/80 transition-all ${rule.is_active ? '' : 'opacity-60 grayscale-[0.5]'}`}>
                          <div className="flex items-start gap-3 flex-1 min-w-0">
                            <div className="mt-0.5 p-2 bg-royal-green-900 rounded-lg text-primary border border-royal-green-600">
                              {getRuleTypeIcon(rule.rule_type)}
                            </div>
                            <div className="min-w-0 flex-1">
                              <div className="flex items-center gap-3 flex-wrap">
                                <span className="font-bold text-slate-100 text-sm">{rule.rule_name}</span>
                                <span className={`text-[10px] uppercase font-bold px-2 py-0.5 rounded border ${getSeverityColor(rule.severity)}`}>
                                  {rule.severity}
                                </span>
                                <span className="text-[10px] uppercase font-bold px-2 py-0.5 rounded border border-royal-green-600 bg-royal-green-900 text-slate-300">
                                  {rule.rule_type}
                                </span>
                              </div>
                              {rule.description && (
                                <p className="text-xs text-slate-400 mt-1 truncate">{rule.description}</p>
                              )}
                            </div>
                          </div>

                          <div className="flex items-center gap-4 lg:pl-4 lg:border-l border-royal-green-700 mt-2 lg:mt-0">
                            <span className={`text-xs font-bold px-2 py-1 rounded font-mono ${rule.query_type === 'pandas' ? 'text-emerald-400 bg-emerald-500/10' : 'text-blue-400 bg-blue-500/10'}`}>
                              {rule.query_type.toUpperCase()}
                            </span>

                            <div className="flex items-center gap-1">
                              <button
                                onClick={() => startEditRule(group.id, rule)}
                                className="p-2 rounded-lg text-slate-400 hover:text-slate-100 hover:bg-royal-green-700 transition-colors"
                              >
                                <Edit2 className="w-4 h-4" />
                              </button>
                              <button
                                onClick={() => deleteRule(group.id, rule.id)}
                                className="p-2 rounded-lg text-slate-400 hover:text-red-400 hover:bg-red-500/10 transition-colors"
                              >
                                <Trash2 className="w-4 h-4" />
                              </button>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="text-center py-8 rounded-xl border border-dashed border-royal-green-600 bg-royal-green-800/20">
                      <p className="text-slate-400 text-sm">No rules in this group yet.</p>
                    </div>
                  )}

                  {/* Add Rule Button */}
                  <button
                    onClick={() => { resetRuleForm(); setShowAddRuleModal(group.id); }}
                    className="w-full mt-4 flex items-center justify-center gap-2 py-3 rounded-lg border border-dashed border-primary/40 bg-primary/5 text-primary hover:bg-primary/10 hover:border-primary/60 transition-colors font-bold text-sm"
                  >
                    <Plus className="w-4 h-4" /> Add New Rule
                  </button>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Create Group Modal */}
      <Modal isOpen={showCreateModal} onClose={() => setShowCreateModal(false)} title="Create Rule Group">
        <div className="space-y-5">
          <div className="space-y-2">
            <label className="text-sm font-bold text-slate-300">Group Name *</label>
            <input
              value={newGroupName}
              onChange={e => setNewGroupName(e.target.value)}
              placeholder="e.g., Financial Metrics Validation"
              className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-3 text-slate-100 placeholder:text-slate-500 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm font-bold text-slate-300">Description</label>
            <textarea
              value={newGroupDesc}
              onChange={e => setNewGroupDesc(e.target.value)}
              placeholder="What is the purpose of this group?"
              rows={2}
              className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-3 text-slate-100 placeholder:text-slate-500 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all resize-y"
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm font-bold text-slate-300 flex items-center justify-between">
              <span>Target Files</span>
              <span className="text-xs font-normal text-slate-500">Comma-separated (optional)</span>
            </label>
            <input
              value={newGroupFiles}
              onChange={e => setNewGroupFiles(e.target.value)}
              placeholder="customers.csv, orders.json"
              className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-3 text-slate-100 placeholder:text-slate-500 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
            />
          </div>
          <div className="flex justify-end gap-3 pt-6 border-t border-royal-green-700 mt-2">
            <button
              onClick={() => setShowCreateModal(false)}
              className="flex items-center justify-center rounded-lg border border-royal-green-600 bg-transparent px-6 py-2.5 font-bold text-slate-300 transition-all hover:bg-royal-green-700 active:scale-95"
            >
              Cancel
            </button>
            <button
              onClick={createGroup}
              disabled={!newGroupName.trim()}
              className="glow-button flex items-center justify-center rounded-lg bg-primary px-6 py-2.5 font-bold text-white transition-all hover:bg-primary/90 active:scale-95 disabled:opacity-50 disabled:active:scale-100"
            >
              Create Group
            </button>
          </div>
        </div>
      </Modal>

      {/* Add / Edit Rule Modal */}
      <Modal
        isOpen={!!showAddRuleModal || !!editingRule}
        onClose={() => { setShowAddRuleModal(null); setEditingRule(null); resetRuleForm(); }}
        title={editingRule ? 'Edit Rule' : 'Add Rule'}
      >
        <div className="space-y-5">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
            <div className="space-y-2">
              <label className="text-sm font-bold text-slate-300">Rule Name *</label>
              <input
                value={ruleForm.rule_name}
                onChange={e => setRuleForm(p => ({ ...p, rule_name: e.target.value }))}
                placeholder="e.g., NullCheck"
                className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 placeholder:text-slate-500 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-bold text-slate-300">Target File</label>
              <input
                value={ruleForm.target_file}
                onChange={e => setRuleForm(p => ({ ...p, target_file: e.target.value }))}
                placeholder="Specific file or empty"
                className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 placeholder:text-slate-500 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
              />
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
            <div className="space-y-2">
              <label className="text-sm font-bold text-slate-300">Type</label>
              <select
                value={ruleForm.rule_type}
                onChange={e => setRuleForm(p => ({ ...p, rule_type: e.target.value }))}
                className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
              >
                <option value="column">Column</option>
                <option value="row">Row</option>
                <option value="table">Table</option>
                <option value="statistical">Statistical</option>
                <option value="custom_sql">Custom SQL</option>
                <option value="custom_pandas">Custom Pandas</option>
              </select>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-bold text-slate-300">Severity</label>
              <select
                value={ruleForm.severity}
                onChange={e => setRuleForm(p => ({ ...p, severity: e.target.value }))}
                className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
              >
                <option value="critical">Critical</option>
                <option value="warning">Warning</option>
                <option value="info">Info</option>
              </select>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-bold text-slate-300">Engine Type</label>
              <select
                value={ruleForm.query_type}
                onChange={e => setRuleForm(p => ({ ...p, query_type: e.target.value }))}
                className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
              >
                <option value="sql">SQL</option>
                <option value="pandas">Pandas</option>
              </select>
            </div>
          </div>

          <div className="space-y-2 border border-royal-green-600 rounded-lg overflow-hidden bg-royal-green-900 focus-within:ring-1 focus-within:ring-primary focus-within:border-primary transition-all">
            <div className="bg-royal-green-800 px-4 py-2 border-b border-royal-green-600 flex justify-between items-center">
              <label className="text-xs font-bold text-slate-300 uppercase tracking-wider">Query / Condition *</label>
              <span className={`text-[10px] font-bold px-2 py-0.5 rounded font-mono ${ruleForm.query_type === 'pandas' ? 'text-emerald-400 bg-emerald-500/10' : 'text-blue-400 bg-blue-500/10'}`}>
                {ruleForm.query_type.toUpperCase()}
              </span>
            </div>
            <textarea
              value={ruleForm.query}
              onChange={e => setRuleForm(p => ({ ...p, query: e.target.value }))}
              placeholder={ruleForm.query_type === 'pandas'
                ? "df[df['email'].isna()]"
                : "SELECT * FROM customers WHERE email IS NULL"
              }
              rows={4}
              className="w-full bg-transparent px-4 py-3 text-emerald-400 font-mono text-sm placeholder:text-royal-green-600 outline-none resize-y"
              spellCheck="false"
            />
          </div>

          <div className="space-y-2">
            <label className="text-sm font-bold text-slate-300">Description</label>
            <input
              value={ruleForm.description}
              onChange={e => setRuleForm(p => ({ ...p, description: e.target.value }))}
              placeholder="Explain what this rule prevents or enforces..."
              className="w-full rounded-lg border border-royal-green-600 bg-royal-green-900 px-4 py-2.5 text-slate-100 placeholder:text-slate-500 focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all"
            />
          </div>

          <div className="flex justify-end gap-3 pt-6 border-t border-royal-green-700 mt-2">
            <button
              onClick={() => { setShowAddRuleModal(null); setEditingRule(null); resetRuleForm(); }}
              className="flex items-center justify-center rounded-lg border border-royal-green-600 bg-transparent px-6 py-2.5 font-bold text-slate-300 transition-all hover:bg-royal-green-700 active:scale-95"
            >
              Cancel
            </button>
            <button
              onClick={() => editingRule ? updateRule() : addRule(showAddRuleModal!)}
              disabled={!ruleForm.rule_name.trim() || !ruleForm.query.trim()}
              className="glow-button flex items-center gap-2 justify-center rounded-lg bg-primary px-6 py-2.5 font-bold text-white transition-all hover:bg-primary/90 active:scale-95 disabled:opacity-50 disabled:active:scale-100"
            >
              <Save className="w-4 h-4" /> {editingRule ? 'Save Changes' : 'Create Rule'}
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
