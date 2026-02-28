import { useState } from 'react';
import {
  Plus,
  Edit2,
  Trash2,
  Search,
  ListChecks,
  Brain,
  Code,
  BarChart3,
  FileText,
} from 'lucide-react';
import Modal from '@/components/Modal';

interface Rule {
  id: string;
  name: string;
  description: string;
  rule_type: string;
  severity: string;
  target_columns: string[];
  is_ai_generated: boolean;
  is_active: boolean;
}

// Mock rules data
const mockRules: Rule[] = [
  {
    id: '1',
    name: 'Customer ID Not Null',
    description: 'Ensures customer_id column has no null values',
    rule_type: 'column',
    severity: 'critical',
    target_columns: ['customer_id'],
    is_ai_generated: false,
    is_active: true,
  },
  {
    id: '2',
    name: 'Email Format Valid',
    description: 'Validates email addresses match standard format',
    rule_type: 'pattern',
    severity: 'critical',
    target_columns: ['email'],
    is_ai_generated: true,
    is_active: true,
  },
  {
    id: '3',
    name: 'Revenue Positive',
    description: 'Ensures revenue values are greater than zero',
    rule_type: 'column',
    severity: 'warning',
    target_columns: ['revenue'],
    is_ai_generated: false,
    is_active: true,
  },
  {
    id: '4',
    name: 'Phone Number Valid',
    description: 'Validates phone numbers are in correct format',
    rule_type: 'pattern',
    severity: 'warning',
    target_columns: ['phone'],
    is_ai_generated: true,
    is_active: true,
  },
  {
    id: '5',
    name: 'Unique Customer ID',
    description: 'Ensures customer_id values are unique',
    rule_type: 'table',
    severity: 'critical',
    target_columns: ['customer_id'],
    is_ai_generated: false,
    is_active: false,
  },
];

export default function Rules() {
  const [searchQuery, setSearchQuery] = useState('');
  const [filterType, setFilterType] = useState<string>('all');
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<Rule | null>(null);

  const filteredRules = mockRules.filter((rule) => {
    const matchesSearch =
      rule.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      rule.description.toLowerCase().includes(searchQuery.toLowerCase());
    const matchesType = filterType === 'all' || rule.rule_type === filterType;
    return matchesSearch && matchesType;
  });

  const getRuleTypeIcon = (type: string) => {
    switch (type) {
      case 'column':
        return <FileText className="w-4 h-4" />;
      case 'row':
        return <ListChecks className="w-4 h-4" />;
      case 'table':
        return <BarChart3 className="w-4 h-4" />;
      case 'pattern':
        return <Code className="w-4 h-4" />;
      case 'statistical':
        return <BarChart3 className="w-4 h-4" />;
      default:
        return <ListChecks className="w-4 h-4" />;
    }
  };

  const getSeverityBadge = (severity: string) => {
    switch (severity) {
      case 'critical':
        return <span className="badge-danger">Critical</span>;
      case 'warning':
        return <span className="badge-warning">Warning</span>;
      case 'info':
        return <span className="badge-info">Info</span>;
      default:
        return <span className="badge">{severity}</span>;
    }
  };

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Validation Rules</h1>
          <p className="mt-1 text-sm text-gray-500">
            Manage custom and AI-generated validation rules
          </p>
        </div>
        <button
          onClick={() => {
            setEditingRule(null);
            setIsModalOpen(true);
          }}
          className="btn-primary"
        >
          <Plus className="w-4 h-4 mr-2" />
          Create Rule
        </button>
      </div>

      {/* Filters */}
      <div className="flex items-center space-x-4">
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
          <input
            type="text"
            placeholder="Search rules..."
            className="form-input pl-10"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </div>
        <select
          className="form-select w-48"
          value={filterType}
          onChange={(e) => setFilterType(e.target.value)}
        >
          <option value="all">All Types</option>
          <option value="column">Column</option>
          <option value="row">Row</option>
          <option value="table">Table</option>
          <option value="pattern">Pattern</option>
          <option value="statistical">Statistical</option>
        </select>
      </div>

      {/* Rules list */}
      <div className="space-y-4">
        {filteredRules.map((rule) => (
          <div
            key={rule.id}
            className={`card ${!rule.is_active ? 'opacity-60' : ''}`}
          >
            <div className="card-body">
              <div className="flex items-start justify-between">
                <div className="flex items-start space-x-3">
                  <div
                    className={`p-2 rounded-lg ${rule.is_ai_generated
                        ? 'bg-purple-100 text-purple-600'
                        : 'bg-gray-100 text-gray-600'
                      }`}
                  >
                    {rule.is_ai_generated ? (
                      <Brain className="w-5 h-5" />
                    ) : (
                      getRuleTypeIcon(rule.rule_type)
                    )}
                  </div>
                  <div>
                    <div className="flex items-center space-x-2">
                      <h3 className="text-sm font-medium text-gray-900">
                        {rule.name}
                      </h3>
                      {rule.is_ai_generated && (
                        <span className="badge bg-purple-100 text-purple-800">
                          AI
                        </span>
                      )}
                      {!rule.is_active && (
                        <span className="badge bg-gray-100 text-gray-600">
                          Inactive
                        </span>
                      )}
                    </div>
                    <p className="text-sm text-gray-500 mt-1">
                      {rule.description}
                    </p>
                    <div className="flex items-center space-x-3 mt-2">
                      {getSeverityBadge(rule.severity)}
                      <span className="text-xs text-gray-400">
                        {rule.rule_type}
                      </span>
                      <span className="text-xs text-gray-400">
                        Columns: {rule.target_columns.join(', ')}
                      </span>
                    </div>
                  </div>
                </div>
                <div className="flex items-center space-x-2">
                  <button
                    onClick={() => {
                      setEditingRule(rule);
                      setIsModalOpen(true);
                    }}
                    className="p-2 text-gray-400 hover:text-primary-600 hover:bg-primary-50 rounded-lg transition-colors"
                  >
                    <Edit2 className="w-4 h-4" />
                  </button>
                  <button
                    onClick={() => { }}
                    className="p-2 text-gray-400 hover:text-danger-600 hover:bg-danger-50 rounded-lg transition-colors"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Create/Edit Rule Modal */}
      <Modal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        title={editingRule ? 'Edit Rule' : 'Create Rule'}
      >
        <form className="space-y-4">
          <div>
            <label className="form-label">Rule Name</label>
            <input
              type="text"
              className="form-input"
              placeholder="e.g., Email Format Valid"
              defaultValue={editingRule?.name}
            />
          </div>

          <div>
            <label className="form-label">Description</label>
            <textarea
              className="form-textarea"
              placeholder="Describe what this rule validates"
              rows={2}
              defaultValue={editingRule?.description}
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="form-label">Rule Type</label>
              <select className="form-select" defaultValue={editingRule?.rule_type}>
                <option value="column">Column</option>
                <option value="row">Row</option>
                <option value="table">Table</option>
                <option value="pattern">Pattern</option>
                <option value="statistical">Statistical</option>
              </select>
            </div>
            <div>
              <label className="form-label">Severity</label>
              <select className="form-select" defaultValue={editingRule?.severity}>
                <option value="critical">Critical</option>
                <option value="warning">Warning</option>
                <option value="info">Info</option>
              </select>
            </div>
          </div>

          <div>
            <label className="form-label">Target Columns</label>
            <input
              type="text"
              className="form-input"
              placeholder="e.g., email, phone (comma-separated)"
              defaultValue={editingRule?.target_columns.join(', ')}
            />
          </div>

          <div>
            <label className="form-label">Configuration</label>
            <textarea
              className="form-textarea font-mono"
              placeholder="JSON configuration for this rule"
              rows={4}
            />
          </div>

          <div className="flex items-center space-x-2">
            <input
              type="checkbox"
              id="is_active"
              defaultChecked={editingRule?.is_active ?? true}
              className="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
            />
            <label htmlFor="is_active" className="text-sm text-gray-700">
              Rule is active
            </label>
          </div>

          <div className="flex justify-end space-x-3 pt-4">
            <button
              type="button"
              onClick={() => setIsModalOpen(false)}
              className="btn-secondary"
            >
              Cancel
            </button>
            <button type="submit" className="btn-primary">
              {editingRule ? 'Update Rule' : 'Create Rule'}
            </button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
