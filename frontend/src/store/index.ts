import { create } from 'zustand';
import { DataSource, ValidationRule, ValidationRun, LLMHealth, DashboardStats } from '@/types';

interface AppState {
  // Data
  dataSources: DataSource[];
  validationRules: ValidationRule[];
  validationRuns: ValidationRun[];
  llmHealth: LLMHealth | null;
  dashboardStats: DashboardStats | null;
  
  // UI State
  isLoading: boolean;
  error: string | null;
  selectedDataSource: DataSource | null;
  selectedValidation: ValidationRun | null;
  
  // Actions
  setDataSources: (sources: DataSource[]) => void;
  setValidationRules: (rules: ValidationRule[]) => void;
  setValidationRuns: (runs: ValidationRun[]) => void;
  setLLMHealth: (health: LLMHealth) => void;
  setDashboardStats: (stats: DashboardStats) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  setSelectedDataSource: (source: DataSource | null) => void;
  setSelectedValidation: (validation: ValidationRun | null) => void;
  
  // CRUD Actions
  addDataSource: (source: DataSource) => void;
  updateDataSource: (id: string, updates: Partial<DataSource>) => void;
  removeDataSource: (id: string) => void;
  addValidationRule: (rule: ValidationRule) => void;
  updateValidationRule: (id: string, updates: Partial<ValidationRule>) => void;
  removeValidationRule: (id: string) => void;
  addValidationRun: (run: ValidationRun) => void;
  updateValidationRun: (id: string, updates: Partial<ValidationRun>) => void;
}

export const useStore = create<AppState>((set) => ({
  // Initial State
  dataSources: [],
  validationRules: [],
  validationRuns: [],
  llmHealth: null,
  dashboardStats: null,
  isLoading: false,
  error: null,
  selectedDataSource: null,
  selectedValidation: null,
  
  // Actions
  setDataSources: (sources) => set({ dataSources: sources }),
  setValidationRules: (rules) => set({ validationRules: rules }),
  setValidationRuns: (runs) => set({ validationRuns: runs }),
  setLLMHealth: (health) => set({ llmHealth: health }),
  setDashboardStats: (stats) => set({ dashboardStats: stats }),
  setLoading: (loading) => set({ isLoading: loading }),
  setError: (error) => set({ error }),
  setSelectedDataSource: (source) => set({ selectedDataSource: source }),
  setSelectedValidation: (validation) => set({ selectedValidation: validation }),
  
  // CRUD Actions
  addDataSource: (source) => set((state) => ({
    dataSources: [...state.dataSources, source],
  })),
  updateDataSource: (id, updates) => set((state) => ({
    dataSources: state.dataSources.map((s) =>
      s.id === id ? { ...s, ...updates } : s
    ),
  })),
  removeDataSource: (id) => set((state) => ({
    dataSources: state.dataSources.filter((s) => s.id !== id),
  })),
  addValidationRule: (rule) => set((state) => ({
    validationRules: [...state.validationRules, rule],
  })),
  updateValidationRule: (id, updates) => set((state) => ({
    validationRules: state.validationRules.map((r) =>
      r.id === id ? { ...r, ...updates } : r
    ),
  })),
  removeValidationRule: (id) => set((state) => ({
    validationRules: state.validationRules.filter((r) => r.id !== id),
  })),
  addValidationRun: (run) => set((state) => ({
    validationRuns: [run, ...state.validationRuns],
  })),
  updateValidationRun: (id, updates) => set((state) => ({
    validationRuns: state.validationRuns.map((r) =>
      r.id === id ? { ...r, ...updates } : r
    ),
  })),
}));
