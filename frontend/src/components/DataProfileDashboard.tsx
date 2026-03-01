import React, { useState } from 'react';
import {
    Database, Columns, AlertTriangle, Key, Eye,
    ChevronDown, ChevronUp, Sparkles, BarChart2
} from 'lucide-react';

// ── Types ──────────────────────────────────────────

interface ColumnStats {
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
        min: any; max: any; mean: number | null; median: number | null;
    };
    quality: { hasDuplicates: boolean; isPrimaryKey: boolean; issues: string[] };
    agentReasoning: string;
}

interface DatasetSummary {
    type_breakdown: Record<string, number>;
    avg_null_percent: number;
    total_quality_issues: number;
    primary_key_candidates: string[];
}

interface Charts {
    overview: Record<string, string>;
    columns: Record<string, string>;
}

interface Props {
    dataset: { id: string; name: string; columns: number; rows: number };
    datasetSummary: DatasetSummary;
    columns: Record<string, ColumnStats>;
    charts: Charts;
    onViewCompleteData: () => void;
}

// ── Type badge colors ──────────────────────────────
const typeColors: Record<string, string> = {
    categorical: 'bg-violet-500/20 text-violet-300 border-violet-500/30',
    numeric_continuous: 'bg-blue-500/20 text-blue-300 border-blue-500/30',
    numeric_discrete: 'bg-sky-500/20 text-sky-300 border-sky-500/30',
    datetime: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30',
    text_freeform: 'bg-amber-500/20 text-amber-300 border-amber-500/30',
    boolean: 'bg-orange-500/20 text-orange-300 border-orange-500/30',
    identifier: 'bg-slate-500/20 text-slate-300 border-slate-500/30',
};

const typeLabels: Record<string, string> = {
    categorical: 'Categorical',
    numeric_continuous: 'Numeric',
    numeric_discrete: 'Discrete',
    datetime: 'DateTime',
    text_freeform: 'Text',
    boolean: 'Boolean',
    identifier: 'ID',
};

// ── Null quality color ──────────────────────────────
function nullColor(pct: number): string {
    if (pct === 0) return 'text-emerald-400';
    if (pct < 5) return 'text-amber-400';
    return 'text-red-400';
}

// ================================================================
// MAIN COMPONENT
// ================================================================
export default function DataProfileDashboard({
    dataset, datasetSummary, columns, charts, onViewCompleteData,
}: Props) {
    const [expandedCol, setExpandedCol] = useState<string | null>(null);
    const colEntries = Object.entries(columns);

    return (
        <div className="space-y-6">
            {/* ━━━ SECTION 1: KPI Overview Cards ━━━ */}
            <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                <KpiCard
                    icon={<Database className="w-5 h-5 text-indigo-400" />}
                    label="Total Rows"
                    value={dataset.rows.toLocaleString()}
                    color="indigo"
                />
                <KpiCard
                    icon={<Columns className="w-5 h-5 text-violet-400" />}
                    label="Columns"
                    value={String(dataset.columns)}
                    color="violet"
                />
                <KpiCard
                    icon={<AlertTriangle className="w-5 h-5 text-amber-400" />}
                    label="Avg Missing"
                    value={`${datasetSummary.avg_null_percent}%`}
                    color="amber"
                    alert={datasetSummary.avg_null_percent > 5}
                />
                <KpiCard
                    icon={<AlertTriangle className="w-5 h-5 text-red-400" />}
                    label="Quality Issues"
                    value={String(datasetSummary.total_quality_issues)}
                    color="red"
                    alert={datasetSummary.total_quality_issues > 0}
                />
                <button
                    onClick={onViewCompleteData}
                    className="flex flex-col items-center justify-center p-4 rounded-xl border border-dashed border-indigo-500/40 bg-indigo-500/5 hover:bg-indigo-500/10 transition-all group"
                >
                    <Eye className="w-5 h-5 text-indigo-400 group-hover:scale-110 transition-transform" />
                    <span className="text-xs text-indigo-300 mt-1 font-medium">View All Data</span>
                </button>
            </div>

            {/* ━━━ SECTION 2: Overview Charts (matplotlib) ━━━ */}
            {charts.overview && Object.keys(charts.overview).length > 0 && (
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    {charts.overview.type_distribution && (
                        <ChartCard title="Column Types" chart={charts.overview.type_distribution} />
                    )}
                    {charts.overview.null_heatmap && (
                        <ChartCard title="Data Completeness" chart={charts.overview.null_heatmap} />
                    )}
                    {charts.overview.correlation_matrix && (
                        <ChartCard title="Correlation Matrix" chart={charts.overview.correlation_matrix} />
                    )}
                </div>
            )}

            {/* ━━━ SECTION 3: Column Cards Grid ━━━ */}
            <div>
                <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-2">
                    <Sparkles className="w-3.5 h-3.5 text-amber-400" />
                    Column Profiles ({colEntries.length})
                </h3>
                <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
                    {colEntries.map(([name, col]) => (
                        <ColumnCard
                            key={name}
                            name={name}
                            col={col}
                            chart={charts.columns?.[name]}
                            isExpanded={expandedCol === name}
                            onToggle={() => setExpandedCol(expandedCol === name ? null : name)}
                        />
                    ))}
                </div>
            </div>
        </div>
    );
}


// ── KPI Card ────────────────────────────────────────
function KpiCard({ icon, label, value, color, alert }: {
    icon: React.ReactNode; label: string; value: string;
    color: string; alert?: boolean;
}) {
    return (
        <div className={`flex flex-col items-center justify-center p-4 rounded-xl border
            ${alert
                ? 'border-red-500/30 bg-red-500/5'
                : `border-${color}-500/20 bg-${color}-500/5`
            }`}
        >
            {icon}
            <div className={`text-2xl font-bold mt-1 ${alert ? 'text-red-300' : 'text-white'}`}>
                {value}
            </div>
            <div className="text-xs text-gray-400 mt-0.5">{label}</div>
        </div>
    );
}


// ── Chart Card (wraps matplotlib PNG) ───────────────
function ChartCard({ title, chart }: { title: string; chart: string }) {
    return (
        <div className="rounded-xl border border-gray-700/50 bg-gray-800/40 overflow-hidden">
            <div className="px-4 py-2 border-b border-gray-700/50">
                <h4 className="text-sm font-medium text-gray-300 flex items-center gap-2">
                    <BarChart2 className="w-3.5 h-3.5 text-indigo-400" />
                    {title}
                </h4>
            </div>
            <div className="p-2 flex justify-center">
                <img
                    src={chart}
                    alt={title}
                    className="max-w-full h-auto rounded"
                    loading="lazy"
                />
            </div>
        </div>
    );
}


// ── Column Card ─────────────────────────────────────
function ColumnCard({ name, col, chart, isExpanded, onToggle }: {
    name: string; col: ColumnStats; chart?: string;
    isExpanded: boolean; onToggle: () => void;
}) {
    const s = col.stats;
    const typeCls = typeColors[col.type] || typeColors.identifier;
    const typeLabel = typeLabels[col.type] || col.type;

    return (
        <div className={`rounded-xl border transition-all ${isExpanded
            ? 'border-indigo-500/40 bg-gray-800/60 shadow-lg shadow-indigo-500/5'
            : 'border-gray-700/40 bg-gray-800/30 hover:border-gray-600/50'
            }`}>
            {/* Header */}
            <button
                onClick={onToggle}
                className="w-full flex items-center justify-between p-3 text-left"
            >
                <div className="flex items-center gap-2 min-w-0">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded-md text-xs font-medium border ${typeCls}`}>
                        {typeLabel}
                    </span>
                    <span className="text-sm font-semibold text-white truncate">{name}</span>
                    {col.quality.isPrimaryKey && (
                        <Key className="w-3 h-3 text-amber-400 flex-shrink-0" />
                    )}
                </div>
                <div className="flex items-center gap-3 flex-shrink-0">
                    <span className={`text-xs font-mono ${nullColor(s.nullPercent)}`}>
                        {s.nullPercent}% null
                    </span>
                    {isExpanded
                        ? <ChevronUp className="w-4 h-4 text-gray-500" />
                        : <ChevronDown className="w-4 h-4 text-gray-500" />
                    }
                </div>
            </button>

            {/* Quick Stats Bar */}
            <div className="px-3 pb-2 flex gap-3 text-xs text-gray-400">
                <span>{s.unique.toLocaleString()} unique</span>
                <span>·</span>
                <span>{s.cardinality.toFixed(1)}% cardinality</span>
                <span>·</span>
                <span className="text-gray-500">{col.originalType}</span>
            </div>

            {/* Null bar indicator */}
            <div className="mx-3 mb-2 h-1.5 rounded-full bg-gray-700/50 overflow-hidden">
                <div
                    className={`h-full rounded-full transition-all ${s.nullPercent === 0 ? 'bg-emerald-500' :
                        s.nullPercent < 5 ? 'bg-amber-500' : 'bg-red-500'
                        }`}
                    style={{ width: `${Math.max(s.nullPercent > 0 ? 2 : 0, Math.min(100, 100 - s.nullPercent))}%` }}
                />
            </div>

            {/* Mini Chart */}
            {chart && !isExpanded && (
                <div className="px-2 pb-2">
                    <img src={chart} alt={`${name} distribution`} className="w-full h-auto rounded opacity-90" loading="lazy" />
                </div>
            )}

            {/* Expanded Detail */}
            {isExpanded && (
                <div className="border-t border-gray-700/40 p-3 space-y-3">
                    {/* Large Chart */}
                    {chart && (
                        <div className="rounded-lg overflow-hidden border border-gray-700/30">
                            <img src={chart} alt={`${name} distribution`} className="w-full h-auto" />
                        </div>
                    )}

                    {/* Stats Table */}
                    <div className="grid grid-cols-2 gap-2 text-xs">
                        <StatRow label="Total" value={s.total.toLocaleString()} />
                        <StatRow label="Missing" value={`${s.nulls.toLocaleString()} (${s.nullPercent}%)`}
                            alert={s.nullPercent > 5} />
                        <StatRow label="Unique" value={s.unique.toLocaleString()} />
                        <StatRow label="Cardinality" value={`${s.cardinality.toFixed(1)}%`} />
                        {col.distribution.min != null && (
                            <StatRow label="Min" value={String(col.distribution.min)} />
                        )}
                        {col.distribution.max != null && (
                            <StatRow label="Max" value={String(col.distribution.max)} />
                        )}
                        {col.distribution.mean != null && (
                            <StatRow label="Mean" value={col.distribution.mean.toFixed(2)} />
                        )}
                        {col.distribution.median != null && (
                            <StatRow label="Median" value={col.distribution.median.toFixed(2)} />
                        )}
                    </div>

                    {/* Top Values */}
                    {col.distribution.topValues.length > 0 && (
                        <div>
                            <h5 className="text-xs font-medium text-gray-400 mb-1">Top Values</h5>
                            <div className="space-y-1">
                                {col.distribution.topValues.slice(0, 8).map((tv, i) => (
                                    <div key={i} className="flex items-center gap-2">
                                        <div className="flex-1 h-4 bg-gray-700/30 rounded overflow-hidden">
                                            <div
                                                className="h-full bg-indigo-500/40 rounded"
                                                style={{
                                                    width: `${(tv.count / col.distribution.topValues[0].count) * 100}%`
                                                }}
                                            />
                                        </div>
                                        <span className="text-xs text-gray-300 w-24 truncate">{tv.value}</span>
                                        <span className="text-xs text-gray-500 w-12 text-right">{tv.count}</span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}

                    {/* Quality Issues */}
                    {col.quality.issues.length > 0 && (
                        <div className="flex flex-wrap gap-1.5">
                            {col.quality.issues.map((issue, i) => (
                                <span key={i} className="inline-flex items-center px-2 py-0.5 rounded text-xs bg-red-500/10 text-red-300 border border-red-500/20">
                                    <AlertTriangle className="w-3 h-3 mr-1" /> {issue}
                                </span>
                            ))}
                        </div>
                    )}

                    {/* Agent Reasoning */}
                    {col.agentReasoning && (
                        <div className="text-xs text-gray-500 italic flex items-start gap-1.5 bg-gray-700/20 rounded-lg p-2">
                            <Sparkles className="w-3 h-3 text-amber-400 flex-shrink-0 mt-0.5" />
                            {col.agentReasoning}
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}


// ── Stat Row ────────────────────────────────────────
function StatRow({ label, value, alert }: { label: string; value: string; alert?: boolean }) {
    return (
        <div className="flex justify-between items-center px-2 py-1 rounded bg-gray-700/20">
            <span className="text-gray-400">{label}</span>
            <span className={`font-mono ${alert ? 'text-red-300' : 'text-gray-200'}`}>{value}</span>
        </div>
    );
}
