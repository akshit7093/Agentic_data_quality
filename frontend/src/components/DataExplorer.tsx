import React, { useMemo, useState } from 'react';
import { Filter, BarChart2, Table as TableIcon, AlertCircle, Loader2, Sparkles, X } from 'lucide-react';
import DataProfileDashboard from './DataProfileDashboard';

// ── Types ──────────────────────────────────────────────────────
interface ColumnInfo {
    name: string;
    type: string;
    null_count?: number;
    unique_count?: number;
    null_percent?: number;
}

interface PreviewData {
    columns: ColumnInfo[];
    rows: Record<string, any>[];
    total_rows: number;
}

interface DiscoveryColumn {
    type: string;
    originalType: string;
    stats: {
        total: number;
        nulls: number;
        nullPercent: number;
        unique: number;
        cardinality: number;
    };
    distribution: {
        topValues: { value: string; count: number }[];
        min: any;
        max: any;
        mean: number | null;
        median: number | null;
    };
    quality: {
        hasDuplicates: boolean;
        isPrimaryKey: boolean;
        issues: string[];
    };
    agentReasoning: string;
}

interface DiscoveryFilter {
    type: string;
    name: string;
    description: string;
    recommended: boolean;
    reason: string;
    config: {
        default: any;
        values: string[] | null;
        min: any;
        max: any;
    };
    ui: { component: string; config: Record<string, any> };
    confidence: number;
    traceId: string;
}

interface DiscoveryDimension {
    type: string;
    name: string;
    description: string;
    recommended: boolean;
    reason: string;
    timeGranularity: string | null;
    confidence: number;
    traceId: string;
}

interface DiscoveryMeasure {
    type: string;
    name: string;
    description: string;
    recommended: boolean;
    reason: string;
    aggregations: string[];
    confidence: number;
    traceId: string;
}

interface DiscoveryMetadata {
    filter_metadata: {
        dataset: { id: string; name: string; columns: number; rows: number };
        dataset_summary: {
            type_breakdown: Record<string, number>;
            avg_null_percent: number;
            total_quality_issues: number;
            primary_key_candidates: string[];
        };
        columns: Record<string, DiscoveryColumn>;
        filters: Record<string, DiscoveryFilter[]>;
        recommendations: any[];
        traceability: { timestamp: string; durationMs: number; trace: any[] };
    };
    pivot_metadata: {
        dimensions: Record<string, DiscoveryDimension>;
        measures: Record<string, DiscoveryMeasure>;
        suggestions: any[];
        traceability: { timestamp: string; durationMs: number; trace: any[] };
    };
    charts?: {
        overview: Record<string, string>;
        columns: Record<string, string>;
    };
}

interface FilterSelection {
    column: string;
    filter_type: string;
    selected_values?: any[];
    min_value?: any;
    max_value?: any;
    text_pattern?: string;
    is_negated?: boolean;
}

interface DataExplorerProps {
    resource: { name: string; columns?: ColumnInfo[] } | null;
    previewData: PreviewData | null;
    sliceFilters: FilterSelection[];
    setSliceFilters: React.Dispatch<React.SetStateAction<FilterSelection[]>>;
    discoveryMetadata: DiscoveryMetadata | null;
    discoveryLoading: boolean;
    dataSourceId: string;
    onViewCompleteData?: () => void;
}

type SemanticType = 'numeric' | 'categorical' | 'boolean' | 'datetime' | 'text' | 'unknown';

interface ColumnStats {
    name: string;
    originalType: string;
    semanticType: SemanticType;
    nullCount: number;
    nullPercent: number;
    uniqueCount: number;
    sampleValues: any[];
}

// ── Helper: map discovery type → semantic type ─────────────────
function toSemantic(disco: string): SemanticType {
    const m: Record<string, SemanticType> = {
        categorical: 'categorical',
        numeric_continuous: 'numeric',
        numeric_discrete: 'numeric',
        datetime: 'datetime',
        boolean: 'boolean',
        text_freeform: 'text',
        identifier: 'text',
    };
    return m[disco] || 'unknown';
}

// ── Semantic badge colors ──────────────────────────────────────
function semanticColor(t: SemanticType): string {
    const colors: Record<string, string> = {
        categorical: 'bg-purple-100 text-purple-800',
        numeric: 'bg-blue-100 text-blue-800',
        datetime: 'bg-green-100 text-green-800',
        boolean: 'bg-yellow-100 text-yellow-800',
        text: 'bg-gray-100 text-gray-800',
        unknown: 'bg-gray-100 text-gray-800',
    };
    return colors[t] || colors.unknown;
}

// ================================================================
// COMPONENT
// ================================================================
export default function DataExplorer({
    resource,
    previewData,
    sliceFilters,
    setSliceFilters,
    discoveryMetadata,
    discoveryLoading,
    dataSourceId,
    onViewCompleteData,
}: DataExplorerProps) {
    const [activeTab, setActiveTab] = useState<'columns' | 'filters' | 'pivot'>('columns');

    // Pivot Builder state
    const [pivotDimension, setPivotDimension] = useState<string>('');
    const [pivotMeasure, setPivotMeasure] = useState<string>('');
    const [pivotAgg, setPivotAgg] = useState<string>('count');
    const [pivotResult, setPivotResult] = useState<Record<string, any>[] | null>(null);
    const [pivotLoading, setPivotLoading] = useState(false);
    const [pivotChart, setPivotChart] = useState<string | null>(null);

    // Per-column filter draft state (user hasn't "applied" yet)
    const [filterDraft, setFilterDraft] = useState<Record<string, FilterSelection>>({});

    const dm = discoveryMetadata;
    const fm = dm?.filter_metadata;
    const pm = dm?.pivot_metadata;

    // ── Column stats (from discovery if available, else sample-calculated) ──
    const stats: ColumnStats[] = useMemo(() => {
        if (fm) {
            return Object.entries(fm.columns).map(([name, col]) => ({
                name,
                originalType: col.originalType,
                semanticType: toSemantic(col.type),
                nullCount: col.stats.nulls,
                nullPercent: col.stats.nullPercent,
                uniqueCount: col.stats.unique,
                sampleValues: col.distribution.topValues?.slice(0, 3).map(v => v.value) || [],
            }));
        }
        if (!previewData?.rows || !resource?.columns) return [];
        return resource.columns.map(col => {
            const values = previewData.rows.map(r => r[col.name]);
            const nonNull = values.filter(v => v != null && v !== '');
            const uniques = new Set(nonNull);
            let semanticType: SemanticType = 'unknown';
            const t = col.type.toLowerCase();
            if (t.includes('int') || t.includes('float') || t.includes('double') || t.includes('numeric') || t.includes('real'))
                semanticType = uniques.size <= 10 && previewData.rows.length > 20 ? 'categorical' : 'numeric';
            else if (t.includes('bool')) semanticType = 'boolean';
            else if (t.includes('date') || t.includes('time')) semanticType = 'datetime';
            else if (uniques.size <= 15 && previewData.rows.length > 0) semanticType = 'categorical';
            else semanticType = 'text';

            const nullCount = col.null_count ?? values.length - nonNull.length;
            const nullPercent = col.null_percent ?? (values.length > 0 ? Math.round((nullCount / values.length) * 100) : 0);
            const uniqueCount = col.unique_count ?? uniques.size;
            return { name: col.name, originalType: col.type, semanticType, nullCount, nullPercent, uniqueCount, sampleValues: Array.from(uniques).slice(0, 3) };
        });
    }, [fm, previewData, resource]);


    // ── Handle Apply Filters ──
    const handleApplyFilters = () => {
        const selections = Object.values(filterDraft).filter(s => {
            if (s.filter_type === 'multi_select') return s.selected_values && s.selected_values.length > 0;
            if (s.filter_type === 'range_slider' || s.filter_type === 'between') return s.min_value != null || s.max_value != null;
            if (s.filter_type === 'text_contains' || s.filter_type === 'text_starts_with' || s.filter_type === 'text_ends_with' || s.filter_type === 'text_exact' || s.filter_type === 'text_regex' || s.filter_type === 'search') return !!s.text_pattern;
            if (s.filter_type === 'toggle') return s.selected_values && s.selected_values.length > 0 && s.selected_values[0] !== 'all';
            if (s.filter_type === 'is_null' || s.filter_type === 'is_not_null') return true;
            if (s.filter_type === 'greater_than') return s.min_value != null;
            if (s.filter_type === 'less_than') return s.max_value != null;
            if (s.filter_type === 'single_select') return s.selected_values && s.selected_values.length > 0;
            return false;
        });
        setSliceFilters(selections);
    };

    const handleClearFilters = () => {
        setFilterDraft({});
        setSliceFilters([]);
    };

    // ── Pivot Generate (server-side) ──
    const handleGeneratePivot = async () => {
        if (!pivotDimension || !dataSourceId || !resource) return;
        setPivotLoading(true);
        try {
            const res = await fetch(`/api/v1/datasources/${dataSourceId}/apply-pivot`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    resource_path: resource.name,
                    dimensions: [pivotDimension],
                    measures: [{ column: pivotMeasure || '*', aggregation: pivotAgg }],
                    filters: sliceFilters.length > 0 ? sliceFilters : undefined,
                }),
            });
            const data = await res.json();
            setPivotResult(data.rows || []);
            setPivotChart(data.chart || null);
        } catch (e) {
            console.error('Pivot failed:', e);
        } finally {
            setPivotLoading(false);
        }
    };

    // ── Helper: update a single column filter draft ──
    const setColumnFilter = (column: string, updates: Partial<FilterSelection>) => {
        setFilterDraft(prev => {
            const existing = prev[column];
            return {
                ...prev,
                [column]: { ...existing, column, filter_type: existing?.filter_type || 'multi_select', ...updates },
            };
        });
    };

    // ── Render a single filter control for a column ──
    const renderFilterControl = (colName: string) => {
        const colFilters = fm?.filters[colName] || [];
        const recommended = colFilters.find(f => f.recommended);
        const activeFilterType = filterDraft[colName]?.filter_type || recommended?.type || colFilters[0]?.type || 'text_contains';
        const activeFilter = colFilters.find(f => f.type === activeFilterType) || recommended || colFilters[0];

        if (!activeFilter) {
            return <p className="text-xs text-gray-400 italic">No filter options available</p>;
        }

        // Select which filter type to use
        const filterTypeSelector = colFilters.length > 1 ? (
            <select
                className="text-xs border-gray-200 rounded bg-gray-100 px-1 py-0.5 mb-2 w-full"
                value={activeFilterType}
                onChange={e => setColumnFilter(colName, { filter_type: e.target.value, selected_values: undefined, min_value: undefined, max_value: undefined, text_pattern: undefined })}
            >
                {colFilters.filter(f => f.type !== 'is_null' && f.type !== 'is_not_null').map(f => (
                    <option key={f.type} value={f.type}>{f.name}{f.recommended ? ' ★' : ''}</option>
                ))}
            </select>
        ) : null;

        // ── Build the specific UI control ──
        let control = null;
        const draft = filterDraft[colName];

        if (activeFilterType === 'multi_select' || activeFilterType === 'exclude') {
            const values = activeFilter.config.values || [];
            const selected = new Set(draft?.selected_values || []);
            control = (
                <div className="space-y-1 max-h-36 overflow-y-auto border border-gray-200 rounded p-1.5 bg-white">
                    {values.slice(0, 50).map(v => (
                        <label key={v} className="flex items-center gap-2 py-0.5 text-sm hover:bg-gray-50 px-1 rounded cursor-pointer">
                            <input
                                type="checkbox"
                                checked={selected.has(v)}
                                className="rounded border-gray-300"
                                onChange={e => {
                                    const next = new Set(selected);
                                    e.target.checked ? next.add(v) : next.delete(v);
                                    setColumnFilter(colName, { filter_type: activeFilterType, selected_values: Array.from(next) });
                                }}
                            />
                            <span className="truncate">{v}</span>
                        </label>
                    ))}
                    {values.length > 50 && <p className="text-xs text-gray-400 px-1">...and {values.length - 50} more</p>}
                </div>
            );
        } else if (activeFilterType === 'single_select') {
            const values = activeFilter.config.values || [];
            control = (
                <select
                    className="w-full text-sm border-gray-300 rounded p-2 bg-white"
                    value={draft?.selected_values?.[0] || ''}
                    onChange={e => setColumnFilter(colName, { filter_type: 'single_select', selected_values: e.target.value ? [e.target.value] : [] })}
                >
                    <option value="">All</option>
                    {values.map(v => <option key={v} value={v}>{v}</option>)}
                </select>
            );
        } else if (activeFilterType === 'range_slider' || activeFilterType === 'between') {
            control = (
                <div className="flex items-center gap-2">
                    <input
                        type="number"
                        placeholder="Min"
                        className="w-1/2 text-sm border-gray-300 rounded p-2"
                        value={draft?.min_value ?? ''}
                        onChange={e => setColumnFilter(colName, { filter_type: activeFilterType, min_value: e.target.value ? Number(e.target.value) : undefined, max_value: draft?.max_value })}
                    />
                    <span className="text-gray-400">→</span>
                    <input
                        type="number"
                        placeholder="Max"
                        className="w-1/2 text-sm border-gray-300 rounded p-2"
                        value={draft?.max_value ?? ''}
                        onChange={e => setColumnFilter(colName, { filter_type: activeFilterType, max_value: e.target.value ? Number(e.target.value) : undefined, min_value: draft?.min_value })}
                    />
                </div>
            );
        } else if (activeFilterType === 'greater_than') {
            control = (
                <input type="number" placeholder="Greater than..." className="w-full text-sm border-gray-300 rounded p-2"
                    value={draft?.min_value ?? ''}
                    onChange={e => setColumnFilter(colName, { filter_type: 'greater_than', min_value: e.target.value ? Number(e.target.value) : undefined })}
                />
            );
        } else if (activeFilterType === 'less_than') {
            control = (
                <input type="number" placeholder="Less than..." className="w-full text-sm border-gray-300 rounded p-2"
                    value={draft?.max_value ?? ''}
                    onChange={e => setColumnFilter(colName, { filter_type: 'less_than', max_value: e.target.value ? Number(e.target.value) : undefined })}
                />
            );
        } else if (activeFilterType === 'toggle') {
            const val = draft?.selected_values?.[0] || 'all';
            control = (
                <div className="flex rounded-md overflow-hidden border border-gray-200">
                    {['all', 'true', 'false'].map(v => (
                        <button
                            key={v}
                            className={`flex-1 px-3 py-1.5 text-sm font-medium transition-colors ${val === v ? 'bg-primary-600 text-white' : 'bg-white text-gray-600 hover:bg-gray-50'}`}
                            onClick={() => setColumnFilter(colName, { filter_type: 'toggle', selected_values: [v] })}
                        >
                            {v.charAt(0).toUpperCase() + v.slice(1)}
                        </button>
                    ))}
                </div>
            );
        } else if (activeFilterType === 'date_range') {
            control = (
                <div className="flex items-center gap-2">
                    <input type="date" className="w-1/2 text-sm border-gray-300 rounded p-2"
                        value={draft?.min_value || ''}
                        onChange={e => setColumnFilter(colName, { filter_type: 'date_range', min_value: e.target.value, max_value: draft?.max_value })}
                    />
                    <span className="text-gray-400">→</span>
                    <input type="date" className="w-1/2 text-sm border-gray-300 rounded p-2"
                        value={draft?.max_value || ''}
                        onChange={e => setColumnFilter(colName, { filter_type: 'date_range', max_value: e.target.value, min_value: draft?.min_value })}
                    />
                </div>
            );
        } else if (activeFilterType === 'date_relative') {
            const presets = activeFilter.ui.config?.presets || ['last_7_days', 'last_30_days', 'last_90_days', 'last_year', 'this_year'];
            control = (
                <select className="w-full text-sm border-gray-300 rounded p-2 bg-white"
                    value={draft?.text_pattern || ''}
                    onChange={e => setColumnFilter(colName, { filter_type: 'date_relative', text_pattern: e.target.value })}
                >
                    <option value="">All time</option>
                    {presets.map((p: string) => <option key={p} value={p}>{p.replace(/_/g, ' ')}</option>)}
                </select>
            );
        } else if (['text_contains', 'text_starts_with', 'text_ends_with', 'text_exact', 'text_regex', 'search'].includes(activeFilterType)) {
            const placeholder = activeFilterType === 'search' ? 'Search...' : `${activeFilter.name}...`;
            control = (
                <input type="text" placeholder={placeholder} className="w-full text-sm border-gray-300 rounded p-2"
                    value={draft?.text_pattern || ''}
                    onChange={e => setColumnFilter(colName, { filter_type: activeFilterType, text_pattern: e.target.value })}
                />
            );
        }

        return (
            <div>
                {filterTypeSelector}
                {control}
            </div>
        );
    };


    // ── Loading / empty ──
    if (!resource || !previewData) {
        return (
            <div className="flex flex-col items-center justify-center p-12 text-gray-500 bg-gray-50 border border-gray-200 rounded-lg">
                <AlertCircle className="w-8 h-8 mb-2 text-gray-400" />
                <p>No preview data available to explore.</p>
                <p className="text-sm mt-1">Please go back and ensure the resource can be previewed.</p>
            </div>
        );
    }

    return (
        <div className="bg-white border border-gray-200 rounded-lg shadow-sm overflow-hidden">
            {/* Tab bar */}
            <div className="border-b border-gray-200 bg-gray-50 p-1 flex space-x-1">
                <button onClick={() => setActiveTab('columns')}
                    className={`flex items-center px-4 py-2 text-sm font-medium rounded-md transition-colors ${activeTab === 'columns' ? 'bg-white text-primary-700 shadow-sm' : 'text-gray-500 hover:text-gray-900 hover:bg-gray-100'}`}
                >
                    <TableIcon className="w-4 h-4 mr-2" /> Column Explorer
                </button>
                <button onClick={() => setActiveTab('filters')}
                    className={`flex items-center px-4 py-2 text-sm font-medium rounded-md transition-colors ${activeTab === 'filters' ? 'bg-white text-primary-700 shadow-sm' : 'text-gray-500 hover:text-gray-900 hover:bg-gray-100'}`}
                >
                    <Filter className="w-4 h-4 mr-2" /> Dynamic Filters
                    {sliceFilters.length > 0 && (
                        <span className="ml-2 bg-primary-100 text-primary-700 text-xs px-1.5 py-0.5 rounded-full">{sliceFilters.length}</span>
                    )}
                </button>
                <button onClick={() => setActiveTab('pivot')}
                    className={`flex items-center px-4 py-2 text-sm font-medium rounded-md transition-colors ${activeTab === 'pivot' ? 'bg-white text-primary-700 shadow-sm' : 'text-gray-500 hover:text-gray-900 hover:bg-gray-100'}`}
                >
                    <BarChart2 className="w-4 h-4 mr-2" /> Pivot Builder
                </button>
            </div>

            <div className="p-4">
                {/* ━━━━━━━━ COLUMNS TAB ━━━━━━━━ */}
                {activeTab === 'columns' && (
                    <div>
                        {discoveryLoading ? (
                            <div className="flex items-center justify-center py-12 text-gray-500">
                                <Loader2 className="w-5 h-5 mr-2 animate-spin" /> Analyzing columns...
                            </div>
                        ) : fm && fm.dataset_summary ? (
                            <DataProfileDashboard
                                dataset={fm.dataset}
                                datasetSummary={fm.dataset_summary}
                                columns={fm.columns}
                                charts={dm?.charts || { overview: {}, columns: {} }}
                                onViewCompleteData={onViewCompleteData || (() => { })}
                            />
                        ) : (
                            /* Fallback: simple table if no discovery data */
                            <div className="overflow-x-auto">
                                <table className="min-w-full divide-y divide-gray-200">
                                    <thead className="bg-gray-50">
                                        <tr>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Column</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Missing %</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Unique</th>
                                        </tr>
                                    </thead>
                                    <tbody className="bg-white divide-y divide-gray-200">
                                        {stats.map(s => (
                                            <tr key={s.name} className="hover:bg-gray-50">
                                                <td className="px-4 py-3 text-sm font-medium text-gray-900">{s.name}</td>
                                                <td className="px-4 py-3">
                                                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${semanticColor(s.semanticType)}`}>
                                                        {s.semanticType}
                                                    </span>
                                                </td>
                                                <td className="px-4 py-3 text-sm">{s.nullPercent}%</td>
                                                <td className="px-4 py-3 text-sm text-gray-500">{s.uniqueCount}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        )}
                    </div>
                )}

                {/* ━━━━━━━━ FILTERS TAB ━━━━━━━━ */}
                {activeTab === 'filters' && (
                    <div className="space-y-4">
                        {discoveryLoading ? (
                            <div className="flex items-center justify-center py-12 text-gray-500">
                                <Loader2 className="w-5 h-5 mr-2 animate-spin" /> Discovering filters...
                            </div>
                        ) : fm ? (
                            <>
                                <div className="flex items-center justify-between">
                                    <p className="text-sm text-gray-600">
                                        <Sparkles className="w-3.5 h-3.5 inline text-amber-500 mr-1" />
                                        Agent discovered filter options for {Object.keys(fm.filters).length} columns.
                                        Select values below, then click <strong>Apply Filters</strong>.
                                    </p>
                                    <div className="flex gap-2">
                                        {Object.keys(filterDraft).length > 0 && (
                                            <button onClick={handleClearFilters} className="text-sm text-red-600 hover:text-red-700 flex items-center gap-1">
                                                <X className="w-3.5 h-3.5" /> Clear All
                                            </button>
                                        )}
                                        <button onClick={handleApplyFilters} className="btn-primary text-sm py-1.5 px-4">
                                            Apply Filters ({Object.keys(filterDraft).length})
                                        </button>
                                    </div>
                                </div>

                                {sliceFilters.length > 0 && (
                                    <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 flex flex-wrap gap-2 items-center">
                                        <span className="text-xs text-blue-700 font-medium">Active:</span>
                                        {sliceFilters.map((f, i) => (
                                            <span key={i} className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800">
                                                {f.column}: {f.filter_type}
                                                {f.selected_values ? ` (${f.selected_values.length} values)` : ''}
                                                {f.min_value != null || f.max_value != null ? ` [${f.min_value ?? ''}–${f.max_value ?? ''}]` : ''}
                                                {f.text_pattern ? ` "${f.text_pattern}"` : ''}
                                            </span>
                                        ))}
                                    </div>
                                )}

                                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                    {Object.entries(fm.filters).map(([colName]) => {
                                        const col = fm.columns[colName];
                                        if (!col) return null;
                                        const sem = toSemantic(col.type);
                                        return (
                                            <div key={colName} className="border border-gray-200 rounded-lg p-3 bg-gray-50 flex flex-col">
                                                <label className="block text-sm font-medium text-gray-700 mb-1 truncate" title={colName}>
                                                    {colName}
                                                    <span className={`ml-2 text-xs px-1.5 py-0.5 rounded-full ${semanticColor(sem)}`}>{sem}</span>
                                                </label>
                                                <div className="text-xs text-gray-400 mb-2">
                                                    {col.stats.unique} unique · {col.stats.nullPercent}% null
                                                </div>
                                                {renderFilterControl(colName)}
                                            </div>
                                        );
                                    })}
                                </div>
                            </>
                        ) : (
                            /* Fallback: old simple filter UI if discovery hasn't run */
                            <div className="space-y-4">
                                <p className="text-sm text-gray-600 mb-4">
                                    Select columns below to drill down and create dataset slices before running validation.
                                </p>
                                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                    {stats.map(s => {
                                        const isDropdown = s.semanticType === 'categorical' || s.semanticType === 'boolean';
                                        return (
                                            <div key={s.name} className="border border-gray-200 rounded p-3 bg-gray-50 flex flex-col">
                                                <label className="block text-sm font-medium text-gray-700 mb-2 truncate" title={s.name}>
                                                    {s.name} <span className="text-xs text-gray-400 font-normal ml-1">({s.semanticType})</span>
                                                </label>
                                                {isDropdown ? (
                                                    <select className="w-full text-sm border-gray-300 rounded focus:ring-primary-500 p-2 bg-white"
                                                        value={(sliceFilters.find(f => f.column === s.name)?.selected_values || [''])[0] || ''}
                                                        onChange={e => {
                                                            const val = e.target.value;
                                                            setSliceFilters(prev => {
                                                                const next = prev.filter(f => f.column !== s.name);
                                                                if (val) next.push({ column: s.name, filter_type: 'single_select', selected_values: [val] });
                                                                return next;
                                                            });
                                                        }}
                                                    >
                                                        <option value="">All values</option>
                                                        {s.sampleValues.map(v => <option key={String(v)} value={String(v)}>{String(v)}</option>)}
                                                    </select>
                                                ) : (
                                                    <input
                                                        type={s.semanticType === 'numeric' ? 'number' : s.semanticType === 'datetime' ? 'date' : 'text'}
                                                        placeholder={`Filter ${s.name}...`}
                                                        className="w-full text-sm border-gray-300 rounded focus:ring-primary-500 p-2 bg-white"
                                                        value={sliceFilters.find(f => f.column === s.name)?.text_pattern || ''}
                                                        onChange={e => {
                                                            const val = e.target.value;
                                                            setSliceFilters(prev => {
                                                                const next = prev.filter(f => f.column !== s.name);
                                                                if (val) next.push({ column: s.name, filter_type: 'text_contains', text_pattern: val });
                                                                return next;
                                                            });
                                                        }}
                                                    />
                                                )}
                                            </div>
                                        );
                                    })}
                                </div>
                            </div>
                        )}
                    </div>
                )}

                {/* ━━━━━━━━ PIVOT TAB ━━━━━━━━ */}
                {activeTab === 'pivot' && (
                    <div className="space-y-6">
                        {discoveryLoading ? (
                            <div className="flex items-center justify-center py-12 text-gray-500">
                                <Loader2 className="w-5 h-5 mr-2 animate-spin" /> Discovering pivots...
                            </div>
                        ) : pm ? (
                            <>
                                {/* Suggestions */}
                                {pm.suggestions.length > 0 && (
                                    <div className="space-y-2">
                                        <h4 className="text-sm font-medium text-gray-700 flex items-center gap-1">
                                            <Sparkles className="w-3.5 h-3.5 text-amber-500" /> Agent Suggestions
                                        </h4>
                                        <div className="flex gap-3 overflow-x-auto pb-2">
                                            {pm.suggestions.slice(0, 5).map((s: any, i: number) => (
                                                <button
                                                    key={i}
                                                    className="flex-shrink-0 border border-gray-200 rounded-lg p-3 bg-white hover:border-primary-300 hover:shadow-sm transition-all text-left min-w-[200px]"
                                                    onClick={() => {
                                                        setPivotDimension(s.dimensions?.[0] || '');
                                                        setPivotMeasure(s.measures?.[0]?.column || '');
                                                        setPivotAgg(s.measures?.[0]?.aggregation || 'count');
                                                    }}
                                                >
                                                    <div className="text-sm font-medium text-gray-900 truncate">{s.name}</div>
                                                    <div className="text-xs text-gray-400 mt-1 truncate">{s.reasoning}</div>
                                                </button>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {/* Manual builder */}
                                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 flex flex-col md:flex-row gap-4 items-end">
                                    <div className="flex-1">
                                        <label className="block text-sm font-medium text-gray-700 mb-1">Dimension (Row)</label>
                                        <select className="w-full text-sm border-gray-300 rounded p-2" value={pivotDimension} onChange={e => setPivotDimension(e.target.value)}>
                                            <option value="">Select Dimension...</option>
                                            {Object.entries(pm.dimensions).map(([col, dim]) => (
                                                <option key={col} value={col}>{dim.name}</option>
                                            ))}
                                        </select>
                                    </div>
                                    <div className="w-48">
                                        <label className="block text-sm font-medium text-gray-700 mb-1">Aggregation</label>
                                        <select className="w-full text-sm border-gray-300 rounded p-2" value={pivotAgg} onChange={e => setPivotAgg(e.target.value)}>
                                            <option value="count">COUNT</option>
                                            <option value="sum">SUM</option>
                                            <option value="average">AVG</option>
                                            <option value="min">MIN</option>
                                            <option value="max">MAX</option>
                                            <option value="median">MEDIAN</option>
                                        </select>
                                    </div>
                                    <div className="flex-1">
                                        <label className="block text-sm font-medium text-gray-700 mb-1">Measure (Value)</label>
                                        <select className="w-full text-sm border-gray-300 rounded p-2" value={pivotMeasure} onChange={e => setPivotMeasure(e.target.value)} disabled={pivotAgg === 'count'}>
                                            <option value="">Select Measure...</option>
                                            {Object.entries(pm.measures).map(([col, meas]) => (
                                                <option key={col} value={col}>{meas.name}</option>
                                            ))}
                                        </select>
                                    </div>
                                    <button
                                        onClick={handleGeneratePivot}
                                        disabled={!pivotDimension || (pivotAgg !== 'count' && !pivotMeasure) || pivotLoading}
                                        className="btn-primary py-2 px-6 disabled:opacity-50 flex items-center gap-2"
                                    >
                                        {pivotLoading && <Loader2 className="w-4 h-4 animate-spin" />}
                                        Generate
                                    </button>
                                </div>

                                {pivotResult && (
                                    <div className="space-y-4">
                                        {/* Pivot Chart (matplotlib) */}
                                        {pivotChart && (
                                            <div className="border border-gray-200 rounded-lg overflow-hidden bg-gray-900 p-4 flex justify-center">
                                                <img src={pivotChart} alt="Pivot visualization" className="max-w-full h-auto rounded" />
                                            </div>
                                        )}
                                        {/* Pivot Table */}
                                        <div className="border border-gray-200 rounded-lg overflow-hidden">
                                            <div className="bg-white p-3 border-b border-gray-200">
                                                <h4 className="font-medium text-sm text-gray-700">Server-Side Pivot Results ({pivotResult.length} groups)</h4>
                                            </div>
                                            <div className="overflow-x-auto max-h-[400px]">
                                                <table className="min-w-full divide-y divide-gray-200">
                                                    <thead className="bg-gray-50 sticky top-0">
                                                        <tr>
                                                            {pivotResult.length > 0 && Object.keys(pivotResult[0]).map(k => (
                                                                <th key={k} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">{k}</th>
                                                            ))}
                                                        </tr>
                                                    </thead>
                                                    <tbody className="bg-white divide-y divide-gray-200">
                                                        {pivotResult.map((row, idx) => (
                                                            <tr key={idx} className="hover:bg-gray-50">
                                                                {Object.values(row).map((v, ci) => (
                                                                    <td key={ci} className="px-4 py-2 whitespace-nowrap text-sm text-gray-900">
                                                                        {typeof v === 'number' ? (v % 1 !== 0 ? (v as number).toFixed(2) : v) : String(v ?? 'null')}
                                                                    </td>
                                                                ))}
                                                            </tr>
                                                        ))}
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>
                                    </div>
                                )}
                                {!pivotResult && (
                                    <div className="text-center py-12 border border-gray-200 rounded-lg bg-gray-50 border-dashed">
                                        <p className="text-gray-500">Select dimensions and measures or click a suggestion above to generate a pivot table.</p>
                                    </div>
                                )}
                            </>
                        ) : (
                            /* Fallback: old-style pivot */
                            <div className="text-center py-12 border border-gray-200 rounded bg-gray-50 border-dashed">
                                <p className="text-gray-500">Pivot discovery not available. Select a data source and resource first.</p>
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}
