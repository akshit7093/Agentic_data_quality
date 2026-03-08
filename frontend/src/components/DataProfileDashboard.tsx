import React, { useState, useMemo } from 'react';
import {
    Database, Columns, AlertTriangle, Key, Eye,
    ChevronDown, ChevronUp, Activity, BarChart2,
    Hash, Calendar, ToggleLeft, Type, Layers, ArrowUpRight
} from 'lucide-react';

// ── Types ────────────────────────────────────────────────────────

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

// ── Type metadata ───────────────────────────────────────────────

const TYPE_META: Record<string, { label: string; icon: React.FC<any>; color: string; bg: string; border: string }> = {
    categorical: { label: 'Categorical', icon: Layers, color: 'text-indigo-400', bg: 'bg-indigo-500/10', border: 'border-indigo-500/25' },
    numeric_continuous: { label: 'Numeric', icon: Activity, color: 'text-blue-400', bg: 'bg-blue-500/10', border: 'border-blue-500/25' },
    numeric_discrete: { label: 'Discrete', icon: Hash, color: 'text-sky-400', bg: 'bg-sky-500/10', border: 'border-sky-500/25' },
    datetime: { label: 'DateTime', icon: Calendar, color: 'text-emerald-400', bg: 'bg-emerald-500/10', border: 'border-emerald-500/25' },
    text_freeform: { label: 'Text', icon: Type, color: 'text-amber-400', bg: 'bg-amber-500/10', border: 'border-amber-500/25' },
    boolean: { label: 'Boolean', icon: ToggleLeft, color: 'text-orange-400', bg: 'bg-orange-500/10', border: 'border-orange-500/25' },
    identifier: { label: 'ID', icon: Hash, color: 'text-slate-400', bg: 'bg-slate-500/10', border: 'border-slate-500/25' },
};

function getTypeMeta(type: string) {
    return TYPE_META[type] ?? { label: type, icon: Hash, color: 'text-slate-400', bg: 'bg-slate-500/10', border: 'border-slate-500/25' };
}

function nullSeverity(pct: number): string {
    if (pct === 0) return 'text-emerald-400';
    if (pct < 5) return 'text-amber-400';
    return 'text-red-400';
}

function nullBarColor(pct: number): string {
    if (pct === 0) return 'bg-emerald-500';
    if (pct < 5) return 'bg-amber-500';
    return 'bg-red-500';
}

// ── Tab IDs ─────────────────────────────────────────────────────
type Tab = 'overview' | 'columns';

// ================================================================
// MAIN COMPONENT
// ================================================================
export default function DataProfileDashboard({
    dataset, datasetSummary, columns, charts, onViewCompleteData,
}: Props) {
    const [activeTab, setActiveTab] = useState<Tab>('overview');
    const [expandedCol, setExpandedCol] = useState<string | null>(null);
    const [typeFilter, setTypeFilter] = useState<string>('all');

    const colEntries = useMemo(() => Object.entries(columns), [columns]);
    const allTypes = useMemo(() => {
        const seen = new Set<string>();
        colEntries.forEach(([, c]) => seen.add(c.type));
        return ['all', ...Array.from(seen)];
    }, [colEntries]);

    const filteredCols = useMemo(() =>
        typeFilter === 'all' ? colEntries : colEntries.filter(([, c]) => c.type === typeFilter),
        [colEntries, typeFilter]
    );

    const healthScore = useMemo(() => {
        if (!colEntries.length) return 100;
        const totalIssues = colEntries.reduce((sum, [, c]) => sum + c.quality.issues.length, 0);
        const maxScore = colEntries.length * 3;
        return Math.max(0, Math.round(100 - (totalIssues / maxScore) * 100));
    }, [colEntries]);

    return (
        <div className="space-y-6 text-slate-100">

            {/* ── KPI Bar ─────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
                <KpiCard
                    icon={<Database className="w-4 h-4" />}
                    label="Rows"
                    value={dataset.rows.toLocaleString()}
                    accent="blue"
                />
                <KpiCard
                    icon={<Columns className="w-4 h-4" />}
                    label="Columns"
                    value={String(dataset.columns)}
                    accent="indigo"
                />
                <KpiCard
                    icon={<Activity className="w-4 h-4" />}
                    label="Avg Missing"
                    value={`${datasetSummary.avg_null_percent.toFixed(1)}%`}
                    accent={datasetSummary.avg_null_percent > 5 ? 'red' : 'emerald'}
                    alert={datasetSummary.avg_null_percent > 5}
                />
                <KpiCard
                    icon={<AlertTriangle className="w-4 h-4" />}
                    label="Issues"
                    value={String(datasetSummary.total_quality_issues)}
                    accent={datasetSummary.total_quality_issues > 0 ? 'amber' : 'emerald'}
                    alert={datasetSummary.total_quality_issues > 0}
                />
                <button
                    onClick={onViewCompleteData}
                    className="flex flex-col items-center justify-center gap-1.5 p-4 rounded-xl
                               border border-dashed border-primary/30 bg-primary/5
                               hover:bg-primary/10 hover:border-primary/50 transition-all group"
                >
                    <Eye className="w-4 h-4 text-primary group-hover:scale-110 transition-transform" />
                    <span className="text-[11px] text-primary font-semibold tracking-wide">View All Data</span>
                </button>
            </div>

            {/* ── Tabs ────────────────────────────────────────────────── */}
            <div className="flex items-center gap-1 border-b border-slate-800 pb-0">
                {([['overview', 'Overview', BarChart2], ['columns', 'Columns', Columns]] as const).map(([id, label, Icon]) => (
                    <button
                        key={id}
                        onClick={() => setActiveTab(id)}
                        className={`flex items-center gap-2 px-4 py-2.5 text-sm font-semibold tracking-wide
                                    border-b-2 transition-all -mb-px
                                    ${activeTab === id
                                ? 'border-primary text-primary'
                                : 'border-transparent text-slate-500 hover:text-slate-300'
                            }`}
                    >
                        <Icon className="w-4 h-4" />
                        {label}
                        {id === 'columns' && (
                            <span className="ml-0.5 px-1.5 py-0 text-[10px] rounded-full bg-slate-800 text-slate-400">
                                {colEntries.length}
                            </span>
                        )}
                    </button>
                ))}
            </div>

            {/* ── OVERVIEW TAB ──────────────────────────────────────── */}
            {activeTab === 'overview' && (
                <div className="space-y-6">
                    {/* Health score + type breakdown */}
                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
                        {/* Health score */}
                        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
                            <p className="text-[11px] font-bold text-slate-500 uppercase tracking-widest mb-3">Profile Health</p>
                            <div className="flex items-end gap-3">
                                <span className={`text-5xl font-black tabular-nums
                                    ${healthScore >= 90 ? 'text-emerald-400' : healthScore >= 70 ? 'text-amber-400' : 'text-red-400'}`}>
                                    {healthScore}
                                </span>
                                <span className="text-slate-500 text-lg mb-1.5 font-semibold">/ 100</span>
                            </div>
                            <div className="mt-3 h-1.5 bg-slate-800 rounded-full overflow-hidden">
                                <div
                                    className={`h-full rounded-full transition-all duration-700
                                        ${healthScore >= 90 ? 'bg-emerald-500' : healthScore >= 70 ? 'bg-amber-500' : 'bg-red-500'}`}
                                    style={{ width: `${healthScore}%` }}
                                />
                            </div>
                            <p className="text-[11px] text-slate-600 mt-2 font-medium">
                                {healthScore >= 90 ? 'Excellent quality' : healthScore >= 70 ? 'Needs attention' : 'Critical issues found'}
                            </p>
                        </div>

                        {/* Type breakdown */}
                        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 col-span-2">
                            <p className="text-[11px] font-bold text-slate-500 uppercase tracking-widest mb-3">Column Type Breakdown</p>
                            <div className="flex flex-wrap gap-2">
                                {Object.entries(datasetSummary.type_breakdown || {}).map(([type, count]) => {
                                    const meta = getTypeMeta(type);
                                    const Icon = meta.icon;
                                    return (
                                        <div key={type}
                                            className={`flex items-center gap-2 px-3 py-1.5 rounded-lg border ${meta.bg} ${meta.border}`}>
                                            <Icon className={`w-3.5 h-3.5 ${meta.color}`} />
                                            <span className={`text-xs font-semibold ${meta.color}`}>{meta.label}</span>
                                            <span className="text-xs text-slate-500 font-mono">{count}</span>
                                        </div>
                                    );
                                })}
                            </div>
                            {datasetSummary.primary_key_candidates?.length > 0 && (
                                <div className="mt-4 flex items-center gap-2 text-[11px] text-slate-500">
                                    <Key className="w-3 h-3 text-amber-400" />
                                    <span>PK candidates:</span>
                                    {datasetSummary.primary_key_candidates.map(k => (
                                        <span key={k} className="px-1.5 py-0.5 rounded bg-amber-500/10 text-amber-400 border border-amber-500/20 font-mono">
                                            {k}
                                        </span>
                                    ))}
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Overview charts from chart engine */}
                    {charts.overview && Object.keys(charts.overview).length > 0 && (
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
                            {charts.overview.type_distribution && (
                                <OverviewChartCard
                                    title="Column Types"
                                    subtitle="Semantic type distribution"
                                    chart={charts.overview.type_distribution}
                                />
                            )}
                            {charts.overview.null_heatmap && (
                                <OverviewChartCard
                                    title="Data Completeness"
                                    subtitle="Missing values per column"
                                    chart={charts.overview.null_heatmap}
                                />
                            )}
                            {charts.overview.correlation_matrix && (
                                <OverviewChartCard
                                    title="Correlation Matrix"
                                    subtitle="Pearson correlation between numerics"
                                    chart={charts.overview.correlation_matrix}
                                />
                            )}
                        </div>
                    )}
                </div>
            )}

            {/* ── COLUMNS TAB ───────────────────────────────────────── */}
            {activeTab === 'columns' && (
                <div className="space-y-4">
                    {/* Type filter pills */}
                    <div className="flex flex-wrap gap-2">
                        {allTypes.map(t => {
                            const meta = t === 'all' ? null : getTypeMeta(t);
                            return (
                                <button
                                    key={t}
                                    onClick={() => setTypeFilter(t)}
                                    className={`px-3 py-1 rounded-lg text-[11px] font-semibold border transition-all
                                        ${typeFilter === t
                                            ? 'bg-primary text-black border-primary'
                                            : 'bg-transparent text-slate-400 border-slate-700 hover:border-slate-500 hover:text-slate-200'
                                        }`}
                                >
                                    {t === 'all' ? `All  ·  ${colEntries.length}` : (meta?.label ?? t)}
                                </button>
                            );
                        })}
                    </div>

                    {/* Column grid */}
                    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
                        {filteredCols.map(([name, col]) => (
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
            )}
        </div>
    );
}


// ── KPI Card ─────────────────────────────────────────────────────

const ACCENT_MAP: Record<string, string> = {
    blue: 'text-blue-400 bg-blue-500/10 border-blue-500/20',
    indigo: 'text-indigo-400 bg-indigo-500/10 border-indigo-500/20',
    emerald: 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20',
    amber: 'text-amber-400 bg-amber-500/10 border-amber-500/20',
    red: 'text-red-400 bg-red-500/10 border-red-500/20',
};

function KpiCard({ icon, label, value, accent = 'blue', alert = false }: {
    icon: React.ReactNode; label: string; value: string;
    accent?: string; alert?: boolean;
}) {
    const cls = ACCENT_MAP[accent] ?? ACCENT_MAP.blue;
    return (
        <div className={`rounded-xl border p-4 flex items-center gap-3 transition-all
            ${alert ? 'bg-red-500/5 border-red-500/20' : 'bg-slate-900 border-slate-800 hover:border-slate-700'}`}>
            <div className={`w-9 h-9 rounded-lg border flex items-center justify-center shrink-0 ${cls}`}>
                {icon}
            </div>
            <div>
                <div className="text-xl font-black tabular-nums text-slate-100">{value}</div>
                <div className="text-[10px] text-slate-500 font-semibold uppercase tracking-widest">{label}</div>
            </div>
        </div>
    );
}


// ── Overview Chart Card ──────────────────────────────────────────

function OverviewChartCard({ title, subtitle, chart }: { title: string; subtitle: string; chart: string }) {
    return (
        <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden">
            <div className="px-4 pt-4 pb-2">
                <p className="text-sm font-semibold text-slate-200">{title}</p>
                <p className="text-[11px] text-slate-500 mt-0.5">{subtitle}</p>
            </div>
            <div className="px-3 pb-3 flex justify-center bg-black/10">
                <img
                    src={chart}
                    alt={title}
                    className="max-w-full h-auto rounded opacity-95 hover:opacity-100 transition-opacity"
                    loading="lazy"
                />
            </div>
        </div>
    );
}


// ── Column Card ──────────────────────────────────────────────────

function ColumnCard({ name, col, chart, isExpanded, onToggle }: {
    name: string; col: ColumnStats; chart?: string;
    isExpanded: boolean; onToggle: () => void;
}) {
    const s = col.stats;
    const meta = getTypeMeta(col.type);
    const Icon = meta.icon;
    const hasIssues = col.quality.issues.length > 0;

    return (
        <div className={`rounded-xl border transition-all duration-200
            ${isExpanded
                ? 'border-primary/40 bg-slate-900 shadow-[0_0_24px_rgba(16,185,129,0.06)]'
                : 'border-slate-800 bg-slate-900 hover:border-slate-700'
            }`}>

            {/* Header row */}
            <button onClick={onToggle} className="w-full flex items-center gap-3 p-4 text-left">
                {/* Type badge */}
                <span className={`shrink-0 inline-flex items-center gap-1.5 px-2 py-1 rounded-lg border text-[11px] font-semibold
                                  ${meta.bg} ${meta.border} ${meta.color}`}>
                    <Icon className="w-3 h-3" />
                    {meta.label}
                </span>

                {/* Column name */}
                <span className="flex-1 text-sm font-semibold text-slate-100 truncate">{name}</span>

                {/* Badges */}
                <div className="flex items-center gap-1.5 shrink-0">
                    {col.quality.isPrimaryKey && (
                        <span className="w-5 h-5 flex items-center justify-center rounded bg-amber-500/15 border border-amber-500/25">
                            <Key className="w-2.5 h-2.5 text-amber-400" />
                        </span>
                    )}
                    {hasIssues && (
                        <span className="w-5 h-5 flex items-center justify-center rounded bg-red-500/15 border border-red-500/25">
                            <AlertTriangle className="w-2.5 h-2.5 text-red-400" />
                        </span>
                    )}
                    <span className={`text-[11px] font-semibold tabular-nums ${nullSeverity(s.nullPercent)}`}>
                        {s.nullPercent === 0 ? '100%' : `${(100 - s.nullPercent).toFixed(0)}%`}
                    </span>
                    {isExpanded
                        ? <ChevronUp className="w-4 h-4 text-slate-500" />
                        : <ChevronDown className="w-4 h-4 text-slate-500" />
                    }
                </div>
            </button>

            {/* Quick stats row */}
            <div className="px-4 pb-2 flex gap-4 text-[11px] text-slate-500 font-medium">
                <span className="tabular-nums">{s.unique.toLocaleString()} distinct</span>
                <span className="text-slate-700">·</span>
                <span className="tabular-nums">{s.nullPercent.toFixed(1)}% missing</span>
                <span className="text-slate-700">·</span>
                <span className="font-mono text-slate-600">{col.originalType}</span>
            </div>

            {/* Completeness bar */}
            <div className="mx-4 mb-3 h-1 rounded-full bg-slate-800 overflow-hidden">
                <div
                    className={`h-full rounded-full transition-all duration-500 ${nullBarColor(s.nullPercent)}`}
                    style={{ width: `${Math.max(s.nullPercent > 0 ? 2 : 0, 100 - s.nullPercent)}%` }}
                />
            </div>

            {/* Mini chart (collapsed) */}
            {chart && !isExpanded && (
                <div className="px-3 pb-3">
                    <img src={chart} alt={`${name} distribution`}
                        className="w-full h-auto rounded-lg opacity-90 hover:opacity-100 transition-opacity"
                        loading="lazy" />
                </div>
            )}

            {/* Expanded detail */}
            {isExpanded && (
                <div className="border-t border-slate-800 bg-black/20 p-4 space-y-4">
                    {/* Full chart */}
                    {chart && (
                        <div className="rounded-lg overflow-hidden border border-slate-800">
                            <img src={chart} alt={`${name} distribution`} className="w-full h-auto" />
                        </div>
                    )}

                    {/* Stats grid */}
                    <div className="grid grid-cols-2 gap-2">
                        <StatPair label="Total rows" value={s.total.toLocaleString()} />
                        <StatPair label="Missing" value={`${s.nulls.toLocaleString()} (${s.nullPercent}%)`} alert={s.nullPercent > 5} />
                        <StatPair label="Unique values" value={s.unique.toLocaleString()} />
                        <StatPair label="Cardinality" value={`${s.cardinality.toFixed(1)}%`} />
                        {col.distribution.min != null && (
                            <StatPair label="Min" value={String(col.distribution.min)} />
                        )}
                        {col.distribution.max != null && (
                            <StatPair label="Max" value={String(col.distribution.max)} />
                        )}
                        {col.distribution.mean != null && (
                            <StatPair label="Mean" value={col.distribution.mean.toFixed(3)} />
                        )}
                        {col.distribution.median != null && (
                            <StatPair label="Median" value={col.distribution.median.toFixed(3)} />
                        )}
                    </div>

                    {/* Top values */}
                    {col.distribution.topValues.length > 0 && (
                        <div>
                            <p className="text-[11px] font-bold text-slate-500 uppercase tracking-widest mb-2">
                                Top Values
                            </p>
                            <div className="space-y-1.5">
                                {col.distribution.topValues.slice(0, 8).map((tv, i) => {
                                    const pct = col.distribution.topValues[0].count > 0
                                        ? (tv.count / col.distribution.topValues[0].count) * 100
                                        : 0;
                                    return (
                                        <div key={i} className="flex items-center gap-2.5">
                                            <span className="w-28 text-[11px] text-slate-400 truncate font-mono"
                                                title={tv.value}>
                                                {tv.value === '' ? <span className="italic text-slate-600">empty</span> : tv.value}
                                            </span>
                                            <div className="flex-1 h-1.5 bg-slate-800 rounded-full overflow-hidden">
                                                <div className="h-full bg-primary/60 rounded-full"
                                                    style={{ width: `${pct}%` }} />
                                            </div>
                                            <span className="text-[11px] font-mono text-slate-500 w-10 text-right">
                                                {tv.count.toLocaleString()}
                                            </span>
                                        </div>
                                    );
                                })}
                            </div>
                        </div>
                    )}

                    {/* Quality issues */}
                    {hasIssues && (
                        <div className="flex flex-wrap gap-1.5">
                            {col.quality.issues.map((issue, i) => (
                                <span key={i}
                                    className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[11px]
                                                 bg-red-500/10 text-red-300 border border-red-500/20">
                                    <AlertTriangle className="w-2.5 h-2.5" />
                                    {issue}
                                </span>
                            ))}
                        </div>
                    )}

                    {/* Agent insight */}
                    {col.agentReasoning && (
                        <div className="flex items-start gap-2.5 bg-primary/5 border border-primary/15
                                        rounded-lg p-3 text-[11px] text-slate-400 leading-relaxed">
                            <ArrowUpRight className="w-3.5 h-3.5 text-primary shrink-0 mt-0.5" />
                            <span>{col.agentReasoning}</span>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}


// ── Stat Pair ────────────────────────────────────────────────────

function StatPair({ label, value, alert }: { label: string; value: string; alert?: boolean }) {
    return (
        <div className="flex items-center justify-between px-3 py-2 rounded-lg bg-black/30 border border-slate-800/60">
            <span className="text-[11px] text-slate-500 font-medium">{label}</span>
            <span className={`text-[11px] font-mono font-semibold ${alert ? 'text-red-400' : 'text-slate-300'}`}>
                {value}
            </span>
        </div>
    );
}