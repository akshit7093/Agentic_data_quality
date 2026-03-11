import React, { useState } from 'react';
import {
    ChevronLeft,
    ChevronRight,
    Search,
    Filter,
    Download,
    Table as TableIcon,
    Hash,
    Type,
    Calendar,
    ToggleLeft,
    Layers,
    Activity
} from 'lucide-react';

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
    stats: {
        nullPercent: number;
        unique: number;
    };
}

interface DiscoveryMetadata {
    filter_metadata: {
        columns: Record<string, DiscoveryColumn>;
    };
}

interface DetailedDataViewProps {
    previewData: PreviewData;
    discoveryMetadata?: DiscoveryMetadata | null;
    pageSize?: number;
}

const TYPE_ICONS: Record<string, React.FC<any>> = {
    categorical: Layers,
    numeric_continuous: Activity,
    numeric_discrete: Hash,
    datetime: Calendar,
    text_freeform: Type,
    boolean: ToggleLeft,
    identifier: Hash,
};

export default function DetailedDataView({
    previewData,
    discoveryMetadata,
    pageSize = 50
}: DetailedDataViewProps) {
    const [currentPage, setCurrentPage] = useState(0);
    const [searchTerm, setSearchTerm] = useState('');

    const filteredRows = previewData.rows.filter(row =>
        Object.values(row).some(val =>
            String(val).toLowerCase().includes(searchTerm.toLowerCase())
        )
    );

    const totalPages = Math.ceil(filteredRows.length / pageSize);
    const paginatedRows = filteredRows.slice(currentPage * pageSize, (currentPage + 1) * pageSize);

    const getDiscoveryCol = (name: string): DiscoveryColumn | null => {
        return discoveryMetadata?.filter_metadata?.columns?.[name] || null;
    };

    const getNullSeverityColor = (pct: number) => {
        if (pct === 0) return 'text-emerald-400';
        if (pct < 5) return 'text-amber-400';
        return 'text-red-400';
    };

    return (
        <div className="flex flex-col h-full bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden shadow-2xl">
            {/* Header Toolbar */}
            <div className="flex items-center justify-between px-6 py-4 border-b border-slate-800 bg-slate-900/50">
                <div className="flex items-center gap-4">
                    <div className="p-2 bg-primary/10 rounded-lg">
                        <TableIcon className="w-5 h-5 text-primary" />
                    </div>
                    <div>
                        <h3 className="text-sm font-black text-slate-100 uppercase tracking-widest">Data Inspector</h3>
                        <p className="text-[10px] text-slate-500 font-bold uppercase tracking-tighter">
                            Showing {filteredRows.length.toLocaleString()} of {previewData.total_rows.toLocaleString()} records
                        </p>
                    </div>
                </div>

                <div className="flex items-center gap-3">
                    <div className="relative">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
                        <input
                            type="text"
                            placeholder="GENERIC SCAN..."
                            className="bg-slate-800 border border-slate-700 rounded-lg pl-9 pr-4 py-2 text-xs font-bold text-slate-200 focus:ring-1 focus:ring-primary outline-none w-64 tracking-widest"
                            value={searchTerm}
                            onChange={(e) => {
                                setSearchTerm(e.target.value);
                                setCurrentPage(0);
                            }}
                        />
                    </div>
                    <button className="p-2 bg-slate-800 border border-slate-700 rounded-lg text-slate-400 hover:text-primary transition-colors">
                        <Download className="w-4 h-4" />
                    </button>
                </div>
            </div>

            {/* Main Grid Area */}
            <div className="flex-1 overflow-auto custom-scrollbar bg-black/20">
                <table className="w-full border-collapse">
                    <thead className="sticky top-0 z-20 bg-slate-900 shadow-sm">
                        <tr>
                            <th className="px-4 py-4 text-left border-r border-b border-slate-800 w-12 bg-slate-900/80 backdrop-blur-sm">
                                <span className="text-[10px] font-black text-slate-600">ID</span>
                            </th>
                            {previewData.columns.map((col) => {
                                const disco = getDiscoveryCol(col.name);
                                const nullPct = disco?.stats?.nullPercent ?? 0;
                                const uniqueCount = disco?.stats?.unique ?? 0;
                                const Icon = (disco && TYPE_ICONS[disco.type]) || Type;

                                return (
                                    <th key={col.name} className="px-4 py-4 text-left border-r border-b border-slate-800 min-w-[200px] group transition-colors hover:bg-slate-800/40">
                                        <div className="flex flex-col gap-2">
                                            <div className="flex items-center justify-between">
                                                <div className="flex items-center gap-2">
                                                    <Icon className="w-3.5 h-3.5 text-primary/70" />
                                                    <span className="text-xs font-black text-slate-100 uppercase tracking-tight truncate max-w-[140px]" title={col.name}>
                                                        {col.name}
                                                    </span>
                                                </div>
                                                <Filter className="w-3 h-3 text-slate-600 opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer hover:text-primary" />
                                            </div>

                                            {/* Column Metrics Mini-Panel */}
                                            <div className="flex items-center gap-3">
                                                <div className="flex flex-col">
                                                    <span className="text-[9px] font-bold text-slate-500 uppercase tracking-tighter">Missing</span>
                                                    <span className={`text-[10px] font-black font-mono ${getNullSeverityColor(nullPct)}`}>
                                                        {nullPct.toFixed(1)}%
                                                    </span>
                                                </div>
                                                <div className="w-[1px] h-6 bg-slate-800" />
                                                <div className="flex flex-col">
                                                    <span className="text-[9px] font-bold text-slate-500 uppercase tracking-tighter">Unique</span>
                                                    <span className="text-[10px] font-black font-mono text-slate-300">
                                                        {uniqueCount.toLocaleString()}
                                                    </span>
                                                </div>
                                            </div>

                                            {/* Sparkline/Bar placeholder for completeness */}
                                            <div className="h-1 bg-slate-800 rounded-full overflow-hidden w-full">
                                                <div
                                                    className={`h-full rounded-full transition-all duration-700 ${nullPct > 5 ? 'bg-red-500' : 'bg-primary'}`}
                                                    style={{ width: `${100 - nullPct}%` }}
                                                />
                                            </div>
                                        </div>
                                    </th>
                                );
                            })}
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/50">
                        {paginatedRows.length > 0 ? (
                            paginatedRows.map((row, idx) => (
                                <tr key={idx} className="hover:bg-primary/5 transition-colors group">
                                    <td className="px-4 py-3 border-r border-slate-800/30 text-[10px] font-mono font-bold text-slate-600 group-hover:text-primary transition-colors">
                                        {currentPage * pageSize + idx + 1}
                                    </td>
                                    {previewData.columns.map((col) => {
                                        const val = row[col.name];
                                        const isNull = val === null || val === undefined;
                                        const isEmpty = val === '';
                                        return (
                                            <td key={col.name} className="px-4 py-3 border-r border-slate-800/30 text-xs font-mono text-slate-300 truncate max-w-[300px]" title={String(val)}>
                                                {isNull ? (
                                                    <span className="text-slate-700 italic lowercase font-normal">null</span>
                                                ) : isEmpty ? (
                                                    <span className="text-slate-700 italic lowercase font-normal">empty</span>
                                                ) : (
                                                    String(val)
                                                )}
                                            </td>
                                        );
                                    })}
                                </tr>
                            ))
                        ) : (
                            <tr>
                                <td colSpan={previewData.columns.length + 1} className="py-20 text-center">
                                    <div className="flex flex-col items-center gap-3 text-slate-500">
                                        <Search className="w-8 h-8 opacity-20" />
                                        <p className="text-sm font-bold uppercase tracking-widest">No records found matching filters</p>
                                    </div>
                                </td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </div>

            {/* Pagination Footer */}
            <div className="px-6 py-4 border-t border-slate-800 bg-slate-900 flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <span className="text-[10px] font-black text-slate-500 uppercase tracking-widest">
                        Page {currentPage + 1} of {Math.max(1, totalPages)}
                    </span>
                    <span className="text-slate-700 mx-2">|</span>
                    <span className="text-[10px] font-black text-slate-500 uppercase tracking-widest">
                        {filteredRows.length.toLocaleString()} total entries
                    </span>
                </div>

                <div className="flex gap-2">
                    <button
                        onClick={() => setCurrentPage(p => Math.max(0, p - 1))}
                        disabled={currentPage === 0}
                        className="flex items-center gap-2 px-4 py-2 bg-slate-800 border border-slate-700 rounded-lg text-xs font-black text-slate-300 uppercase tracking-widest hover:bg-slate-700 disabled:opacity-20 disabled:cursor-not-allowed transition-all"
                    >
                        <ChevronLeft className="w-4 h-4" /> Previous
                    </button>
                    <button
                        onClick={() => setCurrentPage(p => Math.min(totalPages - 1, p + 1))}
                        disabled={currentPage >= totalPages - 1}
                        className="flex items-center gap-2 px-6 py-2 bg-primary border border-primary/20 rounded-lg text-xs font-black text-black uppercase tracking-widest hover:bg-primary/90 disabled:opacity-20 disabled:cursor-not-allowed transition-all shadow-[0_0_15px_rgba(16,183,127,0.3)]"
                    >
                        Next <ChevronRight className="w-4 h-4" />
                    </button>
                </div>
            </div>
        </div>
    );
}
