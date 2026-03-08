import axios from 'axios';
import { DataSource, ValidationRule, ValidationRun, LLMHealth } from '@/types';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Data Sources
export const dataSourceApi = {
  getAll: () => api.get<DataSource[]>('/datasources').then(r => r.data),
  getById: (id: string) => api.get<DataSource>(`/datasources/${id}`).then(r => r.data),
  create: (data: Partial<DataSource>) => api.post<DataSource>('/datasources', data).then(r => r.data),
  update: (id: string, data: Partial<DataSource>) => api.put<DataSource>(`/datasources/${id}`, data).then(r => r.data),
  delete: (id: string) => api.delete(`/datasources/${id}`).then(r => r.data),
  testConnection: (id: string) => api.post(`/datasources/${id}/test`).then(r => r.data),
  getResources: (id: string, path?: string) =>
    api.get(`/datasources/${id}/resources`, { params: { path } }).then(r => r.data),
  getSchema: (id: string, resourcePath: string) =>
    api.get(`/datasources/${id}/schema`, { params: { resource_path: resourcePath } }).then(r => r.data),
  getPreview: (id: string, resourcePath: string, limit: number = 1000) =>
    api.get(`/datasources/${id}/preview`, { params: { resource_path: resourcePath, limit } }).then(r => r.data),
};

// Validation Rules
export const ruleApi = {
  getAll: (params?: { data_source_id?: string; is_active?: boolean }) =>
    api.get<ValidationRule[]>('/rules', { params }).then(r => r.data),
  create: (data: Partial<ValidationRule>) => api.post<ValidationRule>('/rules', data).then(r => r.data),
  update: (id: string, data: Partial<ValidationRule>) => api.put<ValidationRule>(`/rules/${id}`, data).then(r => r.data),
  delete: (id: string) => api.delete(`/rules/${id}`).then(r => r.data),
};

// Validations
export const validationApi = {
  getAll: () => api.get('/validations').then(r => r.data),
  submit: (data: {
    data_source_id: string;
    target_path: string;
    validation_mode: string;
    custom_rules?: Partial<ValidationRule>[];
    sample_size?: number;
  }) => api.post<{ validation_id: string; status: string }>('/validate', data).then(r => r.data),

  getStatus: (id: string) => api.get<ValidationRun>(`/validate/${id}`).then(r => r.data),
  getResults: (id: string) => api.get(`/validate/${id}/results`).then(r => r.data),
  getReport: (id: string, format: 'json' | 'pdf' | 'excel' = 'json') =>
    api.get(`/validate/${id}/report`, { params: { format } }).then(r => r.data),
};

// AI
export const aiApi = {
  recommendRules: (data_source_id: string, target_path: string, sample_size?: number) =>
    api.post('/ai/recommend-rules', { data_source_id, target_path, sample_size }).then(r => r.data),
  analyze: (data_source_id: string, target_path: string) =>
    api.post('/ai/analyze', { data_source_id, target_path }).then(r => r.data),
};

// System
export const systemApi = {
  health: () => api.get('/health').then(r => r.data),
  llmHealth: () => api.get<LLMHealth>('/llm/health').then(r => r.data),
  getSupportedSources: () => api.get<{ source_types: string[] }>('/supported-sources').then(r => r.data),
  getSettings: () => api.get('/settings').then(r => r.data),
  updateSettings: (data: any) => api.post('/settings', data).then(r => r.data),
};

// File Upload
export const fileApi = {
  upload: (file: File) => {
    const formData = new FormData();
    formData.append('file', file);
    return api.post('/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    }).then(r => r.data);
  },
};

// Rule Groups — self-prefixed routes at /api/v1/rules
const rulesBase = 'http://localhost:8000/api/v1/rules';
export const ruleGroupApi = {
  // Groups
  listGroups: () =>
    axios.get(`${rulesBase}/groups`).then(r => r.data),
  createGroup: (data: { name: string; description?: string; target_files?: string[] }) =>
    axios.post(`${rulesBase}/groups`, data).then(r => r.data),
  getGroup: (id: string) =>
    axios.get(`${rulesBase}/groups/${id}`).then(r => r.data),
  updateGroup: (id: string, data: any) =>
    axios.put(`${rulesBase}/groups/${id}`, data).then(r => r.data),
  deleteGroup: (id: string) =>
    axios.delete(`${rulesBase}/groups/${id}`).then(r => r.data),
  // Rules within groups
  addRule: (groupId: string, data: any) =>
    axios.post(`${rulesBase}/groups/${groupId}/rules`, data).then(r => r.data),
  updateRule: (groupId: string, ruleId: string, data: any) =>
    axios.put(`${rulesBase}/groups/${groupId}/rules/${ruleId}`, data).then(r => r.data),
  deleteRule: (groupId: string, ruleId: string) =>
    axios.delete(`${rulesBase}/groups/${groupId}/rules/${ruleId}`).then(r => r.data),
};

export default api;
