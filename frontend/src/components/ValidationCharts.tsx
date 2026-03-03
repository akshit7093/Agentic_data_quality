/**
 * ValidationCharts — Interactive visualizations for validation results.
 *
 * Renders 3 charts computed from the results array:
 *   1. Severity Distribution — Donut chart (PieChart)
 *   2. Pass / Fail by Rule Type — Horizontal bar chart
 *   3. Column Health Grid — Color-coded cells
 */
import { useMemo } from 'react';
import {
    PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, Tooltip,
    ResponsiveContainer, Legend, RadialBarChart, RadialBar,
    CartesianGrid,
} from 'recharts';

// ── Colors ────────────────────────────────────────────────────

const SEVERITY_COLORS: Record<string, string> = {
    critical: '#ef4444',
    warning: '#f59e0b',
    info: '#3b82f6',
    unknown: '#94a3b8',
};

const STATUS_COLORS: Record<string, string> = {
    passed: '#22c55e',
    failed: '#ef4444',
    warning: '#f59e0b',
};

// ── Types ─────────────────────────────────────────────────────

interface Props {
    results: any[];
    qualityScore: number;
}

// ── Component ─────────────────────────────────────────────────

export default function ValidationCharts({ results, qualityScore }: Props) {
    // ── Severity Distribution ──
    const severityData = useMemo(() => {
        const counts: Record<string, number> = {};
        results.forEach(r => {
            const s = r.severity || 'unknown';
            counts[s] = (counts[s] || 0) + 1;
        });
        return Object.entries(counts).map(([name, value]) => ({
            name: name.charAt(0).toUpperCase() + name.slice(1),
            value,
            fill: SEVERITY_COLORS[name] || SEVERITY_COLORS.unknown,
        }));
    }, [results]);

    // ── Pass / Fail by Rule Type ──
    const ruleTypeData = useMemo(() => {
        const types: Record<string, { passed: number; failed: number; warning: number }> = {};
        results.forEach(r => {
            const type = r.rule_type || 'other';
            if (!types[type]) types[type] = { passed: 0, failed: 0, warning: 0 };
            const status = r.status || 'failed';
            if (status === 'passed') types[type].passed++;
            else if (status === 'warning') types[type].warning++;
            else types[type].failed++;
        });
        return Object.entries(types).map(([name, counts]) => ({
            name: name.length > 20 ? name.slice(0, 18) + '…' : name,
            fullName: name,
            ...counts,
        }));
    }, [results]);

    // ── Score Gauge ──
    const scoreGauge = useMemo(() => [
        { name: 'Score', value: qualityScore, fill: qualityScore >= 90 ? '#22c55e' : qualityScore >= 70 ? '#f59e0b' : '#ef4444' },
    ], [qualityScore]);

    // ── Column × Rule Heatmap ──
    const heatmapData = useMemo(() => {
        const failedByRule: Record<string, { rule: string; failed: number; total: number }> = {};
        results.forEach(r => {
            const key = r.rule_name || 'Unknown';
            if (!failedByRule[key]) failedByRule[key] = { rule: key, failed: 0, total: 0 };
            failedByRule[key].total++;
            if (r.status === 'failed') failedByRule[key].failed++;
        });
        return Object.values(failedByRule)
            .sort((a, b) => b.failed - a.failed)
            .slice(0, 15); // Top 15 rules with most failures
    }, [results]);

    if (results.length === 0) {
        return (
            <div className="text-center py-12 text-gray-500">
                No results data to visualize.
            </div>
        );
    }

    return (
        <div className="space-y-8">
            {/* Row 1: Score Gauge + Severity Donut */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {/* Quality Score Gauge */}
                <div className="card">
                    <div className="card-body">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Quality Score</h3>
                        <div className="flex items-center justify-center">
                            <ResponsiveContainer width={200} height={200}>
                                <RadialBarChart
                                    cx="50%" cy="50%"
                                    innerRadius="70%" outerRadius="100%"
                                    startAngle={180} endAngle={0}
                                    barSize={18}
                                    data={scoreGauge}
                                >
                                    <RadialBar
                                        dataKey="value"
                                        cornerRadius={10}
                                        background={{ fill: '#e5e7eb' }}
                                    />
                                </RadialBarChart>
                            </ResponsiveContainer>
                            <div className="absolute text-center">
                                <div className={`text-4xl font-bold ${qualityScore >= 90 ? 'text-success-600' : qualityScore >= 70 ? 'text-warning-600' : 'text-danger-600'}`}>
                                    {qualityScore}%
                                </div>
                            </div>
                        </div>
                        <div className="text-center text-xs text-gray-500 mt-2">
                            {qualityScore >= 90 ? 'Excellent' : qualityScore >= 70 ? 'Needs Attention' : 'Critical Issues'}
                        </div>
                    </div>
                </div>

                {/* Severity Distribution Donut */}
                <div className="card">
                    <div className="card-body">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Severity Distribution</h3>
                        <ResponsiveContainer width="100%" height={220}>
                            <PieChart>
                                <Pie
                                    data={severityData}
                                    cx="50%" cy="50%"
                                    innerRadius={55} outerRadius={85}
                                    paddingAngle={3}
                                    dataKey="value"
                                    label={({ name, value }) => `${name}: ${value}`}
                                    labelLine={false}
                                >
                                    {severityData.map((entry, i) => (
                                        <Cell key={i} fill={entry.fill} stroke="none" />
                                    ))}
                                </Pie>
                                <Tooltip />
                                <Legend iconType="circle" iconSize={8} />
                            </PieChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </div>

            {/* Row 2: Rule Type Breakdown */}
            <div className="card">
                <div className="card-body">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Results by Rule Type</h3>
                    <ResponsiveContainer width="100%" height={Math.max(200, ruleTypeData.length * 40)}>
                        <BarChart data={ruleTypeData} layout="vertical" margin={{ left: 100, right: 20, top: 5, bottom: 5 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                            <XAxis type="number" />
                            <YAxis type="category" dataKey="name" width={100} tick={{ fontSize: 12 }} />
                            <Tooltip
                                content={({ active, payload }) => {
                                    if (!active || !payload?.length) return null;
                                    const d = payload[0].payload;
                                    return (
                                        <div className="bg-white border border-gray-200 rounded-lg shadow-lg p-3 text-xs">
                                            <div className="font-semibold mb-1">{d.fullName}</div>
                                            <div className="text-green-600">Passed: {d.passed}</div>
                                            <div className="text-red-500">Failed: {d.failed}</div>
                                            {d.warning > 0 && <div className="text-yellow-500">Warning: {d.warning}</div>}
                                        </div>
                                    );
                                }}
                            />
                            <Bar dataKey="passed" stackId="a" fill={STATUS_COLORS.passed} radius={[0, 0, 0, 0]} />
                            <Bar dataKey="failed" stackId="a" fill={STATUS_COLORS.failed} radius={[0, 0, 0, 0]} />
                            <Bar dataKey="warning" stackId="a" fill={STATUS_COLORS.warning} radius={[0, 4, 4, 0]} />
                            <Legend iconType="square" iconSize={10} />
                        </BarChart>
                    </ResponsiveContainer>
                </div>
            </div>

            {/* Row 3: Rule Failure Heatmap */}
            {heatmapData.length > 0 && (
                <div className="card">
                    <div className="card-body">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Rule Failure Overview</h3>
                        <div className="grid grid-cols-1 gap-2">
                            {heatmapData.map(item => {
                                const failPercent = item.total > 0 ? (item.failed / item.total) * 100 : 0;
                                return (
                                    <div key={item.rule} className="flex items-center gap-3">
                                        <div className="w-48 text-xs text-gray-700 truncate font-medium" title={item.rule}>
                                            {item.rule}
                                        </div>
                                        <div className="flex-1 h-6 bg-gray-100 rounded-full overflow-hidden relative">
                                            <div
                                                className="h-full rounded-full transition-all"
                                                style={{
                                                    width: `${100 - failPercent}%`,
                                                    backgroundColor: failPercent === 0 ? '#22c55e' : failPercent < 30 ? '#86efac' : failPercent < 70 ? '#fbbf24' : '#ef4444',
                                                }}
                                            />
                                            <div className="absolute inset-0 flex items-center justify-center text-[10px] font-semibold text-gray-700">
                                                {item.failed === 0 ? '✓ Pass' : `${item.failed}/${item.total} failed`}
                                            </div>
                                        </div>
                                        <div className="w-16 text-right text-xs font-mono text-gray-500">
                                            {(100 - failPercent).toFixed(0)}%
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
