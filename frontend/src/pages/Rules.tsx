import { useState, useEffect, useCallback } from 'react';
import {
  Plus, Edit2, Trash2, Search, ListChecks, Brain, Code,
  BarChart3, FileText, FolderOpen, ChevronDown, ChevronRight,
  Save, X, ToggleLeft, ToggleRight, Layers,
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
      // Reload group details
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
      case 'column': return <ListChecks size={14} />;
      case 'statistical': return <BarChart3 size={14} />;
      case 'custom_sql': case 'custom_pandas': return <Code size={14} />;
      default: return <Brain size={14} />;
    }
  };

  const getSeverityColor = (severity: string) => {
    switch (severity) {
      case 'critical': return 'bg-red-500/20 text-red-300 border-red-500/30';
      case 'warning': return 'bg-yellow-500/20 text-yellow-300 border-yellow-500/30';
      default: return 'bg-blue-500/20 text-blue-300 border-blue-500/30';
    }
  };

  const filteredGroups = groups.filter(g =>
    g.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    g.description.toLowerCase().includes(searchTerm.toLowerCase())
  );

  // ── Render ────────────────────────────────────────────────

  return (
    <div style={{ padding: '32px', maxWidth: 1200, margin: '0 auto' }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 28 }}>
        <div>
          <h1 style={{ fontSize: 28, fontWeight: 700, color: '#f1f5f9', margin: 0, display: 'flex', alignItems: 'center', gap: 10 }}>
            <Layers size={28} style={{ color: '#818cf8' }} />
            Rule Groups
          </h1>
          <p style={{ color: '#94a3b8', marginTop: 4, fontSize: 14 }}>
            Organize validation rules into named groups for continuous work
          </p>
        </div>
        <button
          onClick={() => setShowCreateModal(true)}
          style={{
            display: 'flex', alignItems: 'center', gap: 8,
            padding: '10px 20px', borderRadius: 8, border: 'none',
            background: 'linear-gradient(135deg, #818cf8, #6366f1)',
            color: 'white', fontWeight: 600, cursor: 'pointer', fontSize: 14,
          }}
        >
          <Plus size={16} /> New Group
        </button>
      </div>

      {/* Search */}
      <div style={{ position: 'relative', marginBottom: 20 }}>
        <Search size={16} style={{ position: 'absolute', left: 12, top: '50%', transform: 'translateY(-50%)', color: '#64748b' }} />
        <input
          type="text"
          placeholder="Search groups..."
          value={searchTerm}
          onChange={e => setSearchTerm(e.target.value)}
          style={{
            width: '100%', padding: '10px 12px 10px 36px', borderRadius: 8,
            border: '1px solid #334155', background: '#1e293b', color: '#f1f5f9',
            fontSize: 14, outline: 'none',
          }}
        />
      </div>

      {/* Groups List */}
      {loading ? (
        <div style={{ textAlign: 'center', padding: 60, color: '#94a3b8' }}>Loading...</div>
      ) : filteredGroups.length === 0 ? (
        <div style={{
          textAlign: 'center', padding: 60, background: '#1e293b',
          borderRadius: 12, border: '1px solid #334155',
        }}>
          <FolderOpen size={48} style={{ color: '#475569', marginBottom: 12 }} />
          <p style={{ color: '#94a3b8', fontSize: 16, margin: 0 }}>
            {groups.length === 0 ? 'No rule groups yet. Create one to get started!' : 'No groups match your search.'}
          </p>
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          {filteredGroups.map(group => (
            <div key={group.id} style={{
              background: '#1e293b', borderRadius: 12, border: '1px solid #334155',
              overflow: 'hidden', transition: 'border-color 0.2s',
            }}>
              {/* Group Header */}
              <div
                onClick={() => expandGroup(group.id)}
                style={{
                  display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                  padding: '16px 20px', cursor: 'pointer',
                  borderBottom: expandedGroup === group.id ? '1px solid #334155' : 'none',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                  {expandedGroup === group.id
                    ? <ChevronDown size={18} style={{ color: '#818cf8' }} />
                    : <ChevronRight size={18} style={{ color: '#64748b' }} />
                  }
                  <div>
                    <div style={{ fontWeight: 600, color: '#f1f5f9', fontSize: 15 }}>
                      {group.name}
                      {!group.is_active && (
                        <span style={{ marginLeft: 8, fontSize: 11, color: '#64748b', fontWeight: 400 }}>
                          (disabled)
                        </span>
                      )}
                    </div>
                    {group.description && (
                      <div style={{ color: '#94a3b8', fontSize: 12, marginTop: 2 }}>{group.description}</div>
                    )}
                  </div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                  {group.target_files?.length > 0 && (
                    <span style={{
                      fontSize: 11, padding: '3px 8px', borderRadius: 6,
                      background: '#334155', color: '#94a3b8',
                    }}>
                      {group.target_files.length} file{group.target_files.length > 1 ? 's' : ''}
                    </span>
                  )}
                  <span style={{
                    fontSize: 12, padding: '3px 10px', borderRadius: 6,
                    background: '#818cf8/15', color: '#818cf8', border: '1px solid #818cf840',
                  }}>
                    {group.rule_count ?? group.rules?.length ?? 0} rules
                  </span>
                  <button
                    onClick={e => { e.stopPropagation(); toggleGroup(group.id, group.is_active); }}
                    style={{ background: 'none', border: 'none', cursor: 'pointer', padding: 4 }}
                    title={group.is_active ? 'Disable group' : 'Enable group'}
                  >
                    {group.is_active
                      ? <ToggleRight size={20} style={{ color: '#22c55e' }} />
                      : <ToggleLeft size={20} style={{ color: '#64748b' }} />
                    }
                  </button>
                  <button
                    onClick={e => { e.stopPropagation(); deleteGroup(group.id); }}
                    style={{ background: 'none', border: 'none', cursor: 'pointer', padding: 4, color: '#ef4444' }}
                    title="Delete group"
                  >
                    <Trash2 size={16} />
                  </button>
                </div>
              </div>

              {/* Expanded: Rules List */}
              {expandedGroup === group.id && (
                <div style={{ padding: '12px 20px 20px' }}>
                  {/* Target files chips */}
                  {group.target_files?.length > 0 && (
                    <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 12 }}>
                      <span style={{ fontSize: 11, color: '#64748b', marginRight: 4, lineHeight: '24px' }}>Targets:</span>
                      {group.target_files.map((f, i) => (
                        <span key={i} style={{
                          fontSize: 11, padding: '4px 10px', borderRadius: 12,
                          background: '#0f172a', color: '#94a3b8', border: '1px solid #334155',
                        }}>
                          <FileText size={10} style={{ marginRight: 4, verticalAlign: 'middle' }} />
                          {f}
                        </span>
                      ))}
                    </div>
                  )}

                  {/* Rules */}
                  {group.rules?.length > 0 ? (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                      {group.rules.map(rule => (
                        <div key={rule.id} style={{
                          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                          padding: '10px 14px', background: '#0f172a', borderRadius: 8,
                          border: '1px solid #1e293b', opacity: rule.is_active ? 1 : 0.5,
                        }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 10, flex: 1, minWidth: 0 }}>
                            {getRuleTypeIcon(rule.rule_type)}
                            <div style={{ flex: 1, minWidth: 0 }}>
                              <div style={{ fontWeight: 500, color: '#e2e8f0', fontSize: 13 }}>
                                {rule.rule_name}
                              </div>
                              {rule.description && (
                                <div style={{ color: '#64748b', fontSize: 11, marginTop: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                  {rule.description}
                                </div>
                              )}
                            </div>
                            <span className={getSeverityColor(rule.severity)} style={{
                              fontSize: 10, padding: '2px 8px', borderRadius: 4, fontWeight: 600, textTransform: 'uppercase',
                              border: '1px solid',
                            }}>
                              {rule.severity}
                            </span>
                            <span style={{
                              fontSize: 10, padding: '2px 8px', borderRadius: 4,
                              background: rule.query_type === 'pandas' ? '#22c55e20' : '#3b82f620',
                              color: rule.query_type === 'pandas' ? '#22c55e' : '#60a5fa',
                              marginLeft: 6,
                            }}>
                              {rule.query_type.toUpperCase()}
                            </span>
                          </div>
                          <div style={{ display: 'flex', gap: 6, marginLeft: 12 }}>
                            <button
                              onClick={() => startEditRule(group.id, rule)}
                              style={{ background: 'none', border: 'none', cursor: 'pointer', padding: 4, color: '#94a3b8' }}
                            >
                              <Edit2 size={13} />
                            </button>
                            <button
                              onClick={() => deleteRule(group.id, rule.id)}
                              style={{ background: 'none', border: 'none', cursor: 'pointer', padding: 4, color: '#ef4444' }}
                            >
                              <Trash2 size={13} />
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p style={{ color: '#64748b', fontSize: 13, textAlign: 'center', padding: 20 }}>
                      No rules in this group yet.
                    </p>
                  )}

                  {/* Add Rule Button */}
                  <button
                    onClick={() => { resetRuleForm(); setShowAddRuleModal(group.id); }}
                    style={{
                      display: 'flex', alignItems: 'center', gap: 6, marginTop: 12,
                      padding: '8px 16px', borderRadius: 6, border: '1px dashed #334155',
                      background: 'transparent', color: '#94a3b8', cursor: 'pointer', fontSize: 12,
                      width: '100%', justifyContent: 'center',
                    }}
                  >
                    <Plus size={14} /> Add Rule
                  </button>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Create Group Modal */}
      <Modal isOpen={showCreateModal} onClose={() => setShowCreateModal(false)} title="Create Rule Group">
        <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
          <div>
            <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Group Name *</label>
            <input
              value={newGroupName}
              onChange={e => setNewGroupName(e.target.value)}
              placeholder="e.g., BMC Files"
              style={{
                width: '100%', padding: '10px 12px', borderRadius: 6,
                border: '1px solid #334155', background: '#0f172a', color: '#f1f5f9', fontSize: 14,
              }}
            />
          </div>
          <div>
            <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Description</label>
            <textarea
              value={newGroupDesc}
              onChange={e => setNewGroupDesc(e.target.value)}
              placeholder="What this group is for..."
              rows={2}
              style={{
                width: '100%', padding: '10px 12px', borderRadius: 6,
                border: '1px solid #334155', background: '#0f172a', color: '#f1f5f9', fontSize: 14, resize: 'vertical',
              }}
            />
          </div>
          <div>
            <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>
              Target Files <span style={{ color: '#64748b' }}>(comma-separated, optional)</span>
            </label>
            <input
              value={newGroupFiles}
              onChange={e => setNewGroupFiles(e.target.value)}
              placeholder="customers.csv, orders.json"
              style={{
                width: '100%', padding: '10px 12px', borderRadius: 6,
                border: '1px solid #334155', background: '#0f172a', color: '#f1f5f9', fontSize: 14,
              }}
            />
          </div>
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 8 }}>
            <button
              onClick={() => setShowCreateModal(false)}
              style={{
                padding: '8px 16px', borderRadius: 6, border: '1px solid #334155',
                background: 'transparent', color: '#94a3b8', cursor: 'pointer',
              }}
            >Cancel</button>
            <button
              onClick={createGroup}
              disabled={!newGroupName.trim()}
              style={{
                padding: '8px 20px', borderRadius: 6, border: 'none',
                background: newGroupName.trim() ? '#6366f1' : '#334155',
                color: 'white', fontWeight: 600, cursor: newGroupName.trim() ? 'pointer' : 'not-allowed',
              }}
            >Create</button>
          </div>
        </div>
      </Modal>

      {/* Add / Edit Rule Modal */}
      <Modal
        isOpen={!!showAddRuleModal || !!editingRule}
        onClose={() => { setShowAddRuleModal(null); setEditingRule(null); resetRuleForm(); }}
        title={editingRule ? 'Edit Rule' : 'Add Rule'}
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <div>
              <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Rule Name *</label>
              <input
                value={ruleForm.rule_name}
                onChange={e => setRuleForm(p => ({ ...p, rule_name: e.target.value }))}
                placeholder="NullCheck"
                style={{
                  width: '100%', padding: '8px 10px', borderRadius: 6,
                  border: '1px solid #334155', background: '#0f172a', color: '#f1f5f9', fontSize: 13,
                }}
              />
            </div>
            <div>
              <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Target File</label>
              <input
                value={ruleForm.target_file}
                onChange={e => setRuleForm(p => ({ ...p, target_file: e.target.value }))}
                placeholder="customers.csv"
                style={{
                  width: '100%', padding: '8px 10px', borderRadius: 6,
                  border: '1px solid #334155', background: '#0f172a', color: '#f1f5f9', fontSize: 13,
                }}
              />
            </div>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12 }}>
            <div>
              <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Type</label>
              <select
                value={ruleForm.rule_type}
                onChange={e => setRuleForm(p => ({ ...p, rule_type: e.target.value }))}
                style={{
                  width: '100%', padding: '8px 10px', borderRadius: 6,
                  border: '1px solid #334155', background: '#0f172a', color: '#f1f5f9', fontSize: 13,
                }}
              >
                <option value="column">Column</option>
                <option value="row">Row</option>
                <option value="table">Table</option>
                <option value="statistical">Statistical</option>
                <option value="custom_sql">Custom SQL</option>
                <option value="custom_pandas">Custom Pandas</option>
              </select>
            </div>
            <div>
              <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Severity</label>
              <select
                value={ruleForm.severity}
                onChange={e => setRuleForm(p => ({ ...p, severity: e.target.value }))}
                style={{
                  width: '100%', padding: '8px 10px', borderRadius: 6,
                  border: '1px solid #334155', background: '#0f172a', color: '#f1f5f9', fontSize: 13,
                }}
              >
                <option value="critical">Critical</option>
                <option value="warning">Warning</option>
                <option value="info">Info</option>
              </select>
            </div>
            <div>
              <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Query Type</label>
              <select
                value={ruleForm.query_type}
                onChange={e => setRuleForm(p => ({ ...p, query_type: e.target.value }))}
                style={{
                  width: '100%', padding: '8px 10px', borderRadius: 6,
                  border: '1px solid #334155', background: '#0f172a', color: '#f1f5f9', fontSize: 13,
                }}
              >
                <option value="sql">SQL</option>
                <option value="pandas">Pandas</option>
              </select>
            </div>
          </div>
          <div>
            <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Query *</label>
            <textarea
              value={ruleForm.query}
              onChange={e => setRuleForm(p => ({ ...p, query: e.target.value }))}
              placeholder={ruleForm.query_type === 'pandas'
                ? "df[df['email'].isna()]"
                : "SELECT * FROM customers WHERE email IS NULL"
              }
              rows={3}
              style={{
                width: '100%', padding: '10px 12px', borderRadius: 6,
                border: '1px solid #334155', background: '#0f172a', color: '#22c55e',
                fontSize: 13, fontFamily: 'monospace', resize: 'vertical',
              }}
            />
          </div>
          <div>
            <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Description</label>
            <input
              value={ruleForm.description}
              onChange={e => setRuleForm(p => ({ ...p, description: e.target.value }))}
              placeholder="What this rule checks..."
              style={{
                width: '100%', padding: '8px 10px', borderRadius: 6,
                border: '1px solid #334155', background: '#0f172a', color: '#f1f5f9', fontSize: 13,
              }}
            />
          </div>
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 8 }}>
            <button
              onClick={() => { setShowAddRuleModal(null); setEditingRule(null); resetRuleForm(); }}
              style={{
                padding: '8px 16px', borderRadius: 6, border: '1px solid #334155',
                background: 'transparent', color: '#94a3b8', cursor: 'pointer',
              }}
            ><X size={14} style={{ marginRight: 4 }} /> Cancel</button>
            <button
              onClick={() => editingRule ? updateRule() : addRule(showAddRuleModal!)}
              disabled={!ruleForm.rule_name.trim() || !ruleForm.query.trim()}
              style={{
                padding: '8px 20px', borderRadius: 6, border: 'none', display: 'flex', alignItems: 'center', gap: 6,
                background: ruleForm.rule_name.trim() && ruleForm.query.trim() ? '#6366f1' : '#334155',
                color: 'white', fontWeight: 600,
                cursor: ruleForm.rule_name.trim() && ruleForm.query.trim() ? 'pointer' : 'not-allowed',
              }}
            ><Save size={14} /> {editingRule ? 'Update' : 'Add'}</button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
