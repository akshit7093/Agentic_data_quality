import React, { useState, useEffect } from 'react';
import {
    Database,
    ArrowRight,
    ChevronRight,
    Sparkles,
    Layout,
    Trash2,
    Search,
    RefreshCw,
    GripVertical,
    Plus,
    X,
    Info,
    Edit3
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

interface Column {
    name: string;
    type: string;
}

interface Template {
    id: string;
    name: string;
    description: string;
    columns: { name: string; type: string; description?: string }[];
}

interface MatchResult {
    id: string; // Unique ID for the mapping row
    template_column: string | null;
    template_type: string | null;
    file_column: string | null;
    file_type: string | null;
    score: number;
    output_name: string;
    status: 'matched' | 'suggested' | 'manual' | 'missing';
}

interface VisualPrepProps {
    fileColumns: Column[];
    dataSourceId: string;
    resourcePath: string;
    initialState?: {
        selectedTemplateId: string;
        mappings: MatchResult[];
        nameThreshold: number;
        requireDtype: boolean;
    } | null;
    onComplete: (
        mapping: Record<string, string>,
        selectedColumns: string[],
        sessionId?: string,
        sessionInfo?: any,
        fullState?: any
    ) => void;
}

export default function VisualPrep({ fileColumns, dataSourceId, resourcePath, initialState, onComplete }: VisualPrepProps) {
    const [templates, setTemplates] = useState<Template[]>([]);
    const [selectedTemplateId, setSelectedTemplateId] = useState<string>(initialState?.selectedTemplateId || '');
    const [mappings, setMappings] = useState<MatchResult[]>(initialState?.mappings || []);
    const [loading, setLoading] = useState(false);
    const [applying, setApplying] = useState(false);
    const [nameThreshold, setNameThreshold] = useState(initialState?.nameThreshold || 70);
    const [requireDtype, setRequireDtype] = useState(initialState?.requireDtype ?? true);
    const [matchReport, setMatchReport] = useState<any>(null);

    // Derived lists for sections
    const usedFileCols = new Set(mappings.map(m => m.file_column).filter(Boolean));
    const availableFileCols = fileColumns.filter(c => !usedFileCols.has(c.name));

    const selectedTemplate = templates.find(t => t.id === selectedTemplateId);
    const usedTemplateCols = new Set(mappings.map(m => m.template_column).filter(Boolean));
    const availableTemplateCols = selectedTemplate?.columns.filter(c => !usedTemplateCols.has(c.name)) || [];

    useEffect(() => {
        fetchTemplates();
    }, []);

    const fetchTemplates = async () => {
        try {
            const res = await fetch('/api/v1/templates');
            const data = await res.json();
            setTemplates(data.templates || []);
        } catch (e) {
            console.error('Failed to fetch templates:', e);
        }
    };

    const handleTemplateChange = async (templateId: string) => {
        setSelectedTemplateId(templateId);
        setMatchReport(null);
        if (!templateId) {
            setMappings([]);
            return;
        }
        await runMatch(templateId);
    };

    const runMatch = async (templateId: string) => {
        setLoading(true);
        try {
            const res = await fetch(`/api/v1/templates/${templateId}/match`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    source_id: dataSourceId,
                    resource_path: resourcePath,
                    name_similarity_min: nameThreshold / 100,
                    dtype_match_required: requireDtype,
                })
            });
            const data = await res.json();
            setMatchReport(data);

            const fileDtypes = data.file_dtypes || {};

            // Convert backend matches to internal mappings
            const initialMappings: MatchResult[] = data.matches.map((m: any, idx: number) => ({
                id: `match-${idx}`,
                template_column: m.template_col,
                template_type: m.template_dtype,
                file_column: m.file_col,
                file_type: m.file_col ? fileDtypes[m.file_col] : null,
                score: m.overall_score,
                output_name: m.output_name || m.template_col,
                status: m.file_col
                    ? (m.overall_score >= nameThreshold / 100 ? 'matched' : 'suggested')
                    : 'missing',
            }));

            setMappings(initialMappings);
        } catch (e) {
            console.error('Matching failed:', e);
        } finally {
            setLoading(false);
        }
    };

    const addManualMapping = (fileColName?: string, templateColName?: string) => {
        const fileCol = fileColumns.find(c => c.name === fileColName);
        const templateCol = selectedTemplate?.columns.find(c => c.name === templateColName);

        const newMatch: MatchResult = {
            id: `manual-${Date.now()}`,
            template_column: templateColName || null,
            template_type: templateCol?.type || null,
            file_column: fileColName || null,
            file_type: fileCol?.type || null,
            score: 0,
            output_name: templateColName || fileColName || 'new_column',
            status: 'manual',
        };
        setMappings(prev => [...prev, newMatch]);
    };

    const removeMapping = (id: string) => {
        setMappings(prev => prev.filter(m => m.id !== id));
    };

    const updateMapping = (id: string, updates: Partial<MatchResult>) => {
        setMappings(prev => prev.map(m => m.id === id ? { ...m, ...updates } : m));
    };

    // ── DRAG AND DROP HANDLERS ──────────────────────────────────────────

    const onDragStart = (e: React.DragEvent, type: 'file' | 'template', name: string) => {
        e.dataTransfer.setData('type', type);
        e.dataTransfer.setData('name', name);
    };

    const onDropOnMapping = (e: React.DragEvent, mappingId: string) => {
        e.preventDefault();
        const type = e.dataTransfer.getData('type') as 'file' | 'template';
        const name = e.dataTransfer.getData('name');

        if (type === 'file') {
            const col = fileColumns.find(c => c.name === name);
            updateMapping(mappingId, { file_column: name, file_type: col?.type || null });
        } else if (type === 'template') {
            const col = selectedTemplate?.columns.find(c => c.name === name);
            updateMapping(mappingId, { template_column: name, template_type: col?.type || null, output_name: name });
        }
    };

    const onDropOnNew = (e: React.DragEvent) => {
        e.preventDefault();
        const type = e.dataTransfer.getData('type') as 'file' | 'template';
        const name = e.dataTransfer.getData('name');

        if (type === 'file') {
            addManualMapping(name, undefined);
        } else if (type === 'template') {
            addManualMapping(undefined, name);
        }
    };

    const onDragOver = (e: React.DragEvent) => e.preventDefault();

    const handleConfirm = async () => {
        const confirmedMappings = mappings
            .filter(m => m.file_column)
            .map(m => ({
                template_col: m.output_name,
                file_col: m.file_column as string,
                output_name: m.output_name,
            }));

        const missingTemplateColsCount = mappings.filter(m => !m.file_column && m.template_column).length;
        if (missingTemplateColsCount > 0) {
            console.info(`Proceeding with ${missingTemplateColsCount} missing template columns.`);
        }

        // Prepare final analysis columns
        const selectedColumns = confirmedMappings.map(m => m.output_name);

        const fullState = {
            selectedTemplateId,
            mappings,
            nameThreshold,
            requireDtype,
        };

        if (selectedTemplateId) {
            setApplying(true);
            try {
                const res = await fetch(`/api/v1/templates/${selectedTemplateId}/apply`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        source_id: dataSourceId,
                        resource_path: resourcePath,
                        confirmed_mappings: confirmedMappings,
                        extra_columns: [], // handled via confirmed_mappings now
                    })
                });
                const data = await res.json();

                const finalMapping: Record<string, string> = {};
                confirmedMappings.forEach(c => { if (c.file_col !== c.output_name) finalMapping[c.file_col] = c.output_name; });

                onComplete(finalMapping, selectedColumns, data.session_id, {
                    columns: data.columns,
                    row_count: data.row_count,
                    templateName: selectedTemplate?.name || 'Template',
                    rename_map: data.rename_map,
                }, fullState);
            } catch (e) {
                console.error('Apply failed:', e);
            } finally {
                setApplying(false);
            }
        } else {
            const finalMapping: Record<string, string> = {};
            confirmedMappings.forEach(c => { if (c.file_col !== c.output_name) finalMapping[c.file_col] = c.output_name; });
            onComplete(finalMapping, selectedColumns, undefined, undefined, fullState);
        }
    };

    return (
        <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-700 w-full px-6 mx-auto">
            {/* Header / Template Selection */}
            <div className="bg-royal-green-900/40 border border-royal-green-600 rounded-2xl p-6 backdrop-blur-md shadow-xl">
                <div className="flex flex-col md:flex-row md:items-center justify-between gap-6">
                    <div className="flex items-center gap-4">
                        <div className="p-3 bg-primary/20 rounded-xl">
                            <Layout className="w-6 h-6 text-primary" />
                        </div>
                        <div>
                            <h3 className="text-xl font-black uppercase tracking-tight text-slate-100">Column Alignment</h3>
                            <p className="text-sm text-slate-400">Map your source columns to standard industry schemas or manual targets.</p>
                        </div>
                    </div>

                    <div className="flex flex-wrap gap-3">
                        {templates.map(t => (
                            <button
                                key={t.id}
                                onClick={() => handleTemplateChange(t.id)}
                                className={`px-4 py-2 rounded-xl border-2 transition-all text-xs font-bold uppercase tracking-tight ${selectedTemplateId === t.id
                                    ? 'border-primary bg-primary/10 text-primary shadow-[0_0_20px_rgba(34,197,94,0.1)]'
                                    : 'border-royal-green-600 bg-black/20 text-slate-400 hover:border-royal-green-400'
                                    }`}
                            >
                                {t.name}
                            </button>
                        ))}
                        <button
                            onClick={() => handleTemplateChange('')}
                            className={`px-4 py-2 rounded-xl border-2 border-dashed transition-all text-xs font-bold uppercase tracking-tight ${selectedTemplateId === ''
                                ? 'border-slate-500 bg-slate-500/10 text-slate-300'
                                : 'border-royal-green-600 text-slate-500 hover:border-royal-green-400'
                                }`}
                        >
                            Manual
                        </button>
                    </div>
                </div>

                {selectedTemplateId && (
                    <motion.div
                        initial={{ height: 0, opacity: 0 }}
                        animate={{ height: 'auto', opacity: 1 }}
                        className="mt-6 pt-6 border-t border-royal-green-700/50 overflow-hidden"
                    >
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
                            <div className="lg:col-span-2">
                                <div className="flex justify-between items-center mb-2">
                                    <label className="text-[10px] font-black text-slate-500 uppercase tracking-widest">Similarity Sensitivity</label>
                                    <span className="text-sm font-black text-primary">{nameThreshold}%</span>
                                </div>
                                <input
                                    type="range" min={50} max={100} step={5}
                                    value={nameThreshold}
                                    onChange={e => setNameThreshold(Number(e.target.value))}
                                    className="w-full h-1.5 bg-royal-green-800 rounded-lg appearance-none cursor-pointer accent-primary"
                                />
                            </div>
                            <div className="flex items-center justify-between border-l border-royal-green-700/50 pl-8">
                                <div>
                                    <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest">Type-Strict</p>
                                    <p className="text-[10px] text-slate-600 mt-0.5 font-bold">Enforce Typed Alignment</p>
                                </div>
                                <button
                                    onClick={() => setRequireDtype(v => !v)}
                                    className={`w-12 h-6 rounded-full transition-colors relative ${requireDtype ? 'bg-primary' : 'bg-royal-green-800'}`}
                                >
                                    <div className={`absolute top-1 w-4 h-4 bg-white rounded-full transition-all ${requireDtype ? 'left-7' : 'left-1'}`} />
                                </button>
                            </div>
                            <div className="flex items-center justify-end">
                                <button
                                    onClick={() => runMatch(selectedTemplateId)}
                                    disabled={loading}
                                    className="flex items-center gap-2 text-[10px] font-black uppercase px-4 py-2 bg-royal-green-800 hover:bg-royal-green-700 rounded-lg text-slate-300 transition-all active:scale-95"
                                >
                                    <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
                                    Rescan Schema
                                </button>
                            </div>
                        </div>
                    </motion.div>
                )}
            </div>

            {/* main 3-Section Layout */}
            <div className="grid grid-cols-1 lg:grid-cols-12 gap-6 h-[750px]">

                {/* Section 1: Source Columns */}
                <div className="lg:col-span-3 flex flex-col bg-royal-green-950/40 border border-royal-green-700 rounded-2xl overflow-hidden backdrop-blur-sm">
                    <div className="p-4 border-b border-royal-green-700 bg-black/20 flex items-center gap-2">
                        <Database className="w-4 h-4 text-slate-500" />
                        <h4 className="text-xs font-black uppercase tracking-widest text-slate-300">File Columns</h4>
                    </div>
                    <div className="p-3">
                        <div className="relative mb-3">
                            <Search className="w-3.5 h-3.5 absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" />
                            <input
                                type="text"
                                placeholder="Filter..."
                                className="w-full bg-black/20 border border-royal-green-700 rounded-lg pl-9 pr-3 py-1.5 text-[11px] text-slate-100 focus:ring-1 focus:ring-primary outline-none"
                            />
                        </div>
                    </div>
                    <div className="flex-1 overflow-y-auto p-2 space-y-2 custom-scrollbar">
                        {availableFileCols.map(col => (
                            <div
                                key={col.name}
                                draggable
                                onDragStart={(e) => onDragStart(e, 'file', col.name)}
                                className="p-3 bg-royal-green-800/40 border border-royal-green-600/50 rounded-xl cursor-grab active:cursor-grabbing hover:border-primary/50 hover:bg-royal-green-800/60 transition-all group"
                            >
                                <div className="flex items-center justify-between">
                                    <span className="text-xs font-bold text-slate-100 truncate pr-2" title={col.name}>{col.name}</span>
                                    <GripVertical className="w-3.5 h-3.5 text-slate-600 group-hover:text-primary transition-colors" />
                                </div>
                                <span className="text-[10px] font-mono text-slate-500 mt-1 block">{col.type}</span>
                            </div>
                        ))}
                    </div>
                </div>

                {/* Section 2: Template Columns */}
                <div className="lg:col-span-3 flex flex-col bg-royal-green-950/40 border border-royal-green-700 rounded-2xl overflow-hidden backdrop-blur-sm">
                    <div className="p-4 border-b border-royal-green-700 bg-black/20 flex items-center gap-2">
                        <Layout className="w-4 h-4 text-slate-500" />
                        <h4 className="text-xs font-black uppercase tracking-widest text-slate-300">Template Specs</h4>
                    </div>
                    <div className="flex-1 overflow-y-auto p-2 space-y-2 custom-scrollbar">
                        {!selectedTemplateId ? (
                            <div className="h-full flex items-center justify-center p-6 text-center">
                                <p className="text-[10px] font-black uppercase tracking-widest text-slate-600 italic">Select a template above</p>
                            </div>
                        ) : (
                            availableTemplateCols.map(col => (
                                <div
                                    key={col.name}
                                    draggable
                                    onDragStart={(e) => onDragStart(e, 'template', col.name)}
                                    className="p-3 bg-primary/5 border border-primary/20 rounded-xl cursor-grab active:cursor-grabbing hover:border-primary/50 hover:bg-primary/10 transition-all group"
                                >
                                    <div className="flex items-center justify-between">
                                        <span className="text-xs font-bold text-primary truncate pr-2" title={col.name}>{col.name}</span>
                                        <GripVertical className="w-3.5 h-3.5 text-primary/40 group-hover:text-primary transition-colors" />
                                    </div>
                                    <div className="flex items-center justify-between mt-1">
                                        <span className="text-[10px] font-mono text-slate-500">{col.type}</span>
                                        <span className="text-[9px] font-black uppercase bg-primary/20 text-primary px-1.5 py-0.5 rounded">Standard</span>
                                    </div>
                                </div>
                            ))
                        )}
                    </div>
                </div>

                {/* Section 3: Alignment Result */}
                <div className="lg:col-span-6 flex flex-col bg-black/30 border border-royal-green-600 rounded-3xl overflow-hidden shadow-2xl relative">
                    <div className="p-5 border-b border-royal-green-700 flex items-center justify-between bg-black/20">
                        <div className="flex items-center gap-3">
                            <Sparkles className="w-4 h-4 text-amber-500" />
                            <h4 className="text-sm font-black uppercase tracking-widest text-slate-100">Final Alignment</h4>
                        </div>
                        <div className="flex gap-2">
                            {matchReport && (
                                <div className="flex items-center gap-4 mr-4">
                                    <div className="flex items-center gap-2">
                                        <div className="w-2.5 h-2.5 rounded-full bg-primary shadow-[0_0_10px_rgba(34,197,94,0.5)]" />
                                        <span className="text-[10px] font-black uppercase text-slate-400">Match {Math.round(matchReport.overall_coverage * 100)}%</span>
                                    </div>
                                </div>
                            )}
                            <button
                                onClick={() => addManualMapping()}
                                className="flex items-center gap-2 px-3 py-1.5 bg-royal-green-800 hover:bg-royal-green-700 rounded-lg text-[10px] font-black uppercase text-slate-200 transition-all"
                            >
                                <Plus className="w-3 h-3" />
                                Add Row
                            </button>
                        </div>
                    </div>

                    <div className="flex-1 overflow-y-auto p-5 space-y-4 custom-scrollbar" onDrop={onDropOnNew} onDragOver={onDragOver}>
                        {loading ? (
                            <div className="h-full flex flex-col items-center justify-center gap-4">
                                <div className="w-12 h-12 border-4 border-primary/20 border-t-primary rounded-full animate-spin"></div>
                                <p className="text-xs font-black uppercase tracking-[0.2em] text-slate-500">Intelligent Mapping in Progress...</p>
                            </div>
                        ) : mappings.length === 0 ? (
                            <div className="h-full flex flex-col items-center justify-center text-center p-10 border-2 border-dashed border-royal-green-800 rounded-2xl m-2">
                                <Layout className="w-12 h-12 text-royal-green-800 mb-4" />
                                <p className="text-sm text-slate-500 font-bold max-w-xs">
                                    No mappings established. Drag items from the left or select a template to begin.
                                </p>
                            </div>
                        ) : (
                            <AnimatePresence mode="popLayout">
                                {mappings.map((m) => (
                                    <motion.div
                                        key={m.id}
                                        layout
                                        initial={{ opacity: 0, x: 20 }}
                                        animate={{ opacity: 1, x: 0 }}
                                        exit={{ opacity: 0, scale: 0.9 }}
                                        className="group relative flex items-center gap-4 p-4 bg-royal-green-900/40 border border-royal-green-700 hover:border-primary/40 rounded-2xl transition-all"
                                        onDrop={(e) => onDropOnMapping(e, m.id)}
                                        onDragOver={onDragOver}
                                    >
                                        <div className="flex-1 min-w-0">
                                            <p className="text-[10px] font-black text-slate-600 uppercase tracking-widest mb-2">Source Data</p>
                                            <div className={`p-3 rounded-xl border-2 border-dashed transition-all flex items-center justify-between min-h-[50px] ${m.file_column ? 'bg-royal-green-800/40 border-royal-green-600' : 'bg-black/20 border-royal-green-800'
                                                }`}>
                                                {m.file_column ? (
                                                    <div className="flex flex-col">
                                                        <span className="text-sm font-black text-slate-100">{m.file_column}</span>
                                                        <span className="text-[10px] font-mono text-slate-500">{m.file_type || 'Unknown'}</span>
                                                    </div>
                                                ) : (
                                                    <span className="text-[10px] font-bold text-slate-700 italic">Drop source col here</span>
                                                )}
                                                {m.file_column && (
                                                    <button onClick={() => updateMapping(m.id, { file_column: null, file_type: null })} className="p-1 hover:text-red-400 text-slate-600 transition-colors">
                                                        <X className="w-3.5 h-3.5" />
                                                    </button>
                                                )}
                                            </div>
                                        </div>

                                        <div className="flex flex-col items-center pt-6 gap-2">
                                            {m.score > 0 && (
                                                <div className="flex flex-col items-center">
                                                    <span className={`text-[10px] font-black ${m.score > 0.8 ? 'text-primary' : 'text-amber-500'}`}>{Math.round(m.score * 100)}%</span>
                                                    <div className="w-8 h-1 bg-royal-green-800 rounded-full overflow-hidden">
                                                        <div className={`h-full ${m.score > 0.8 ? 'bg-primary' : 'bg-amber-500'}`} style={{ width: `${m.score * 100}%` }} />
                                                    </div>
                                                </div>
                                            )}
                                            <ArrowRight className={`w-5 h-5 ${m.score > 0.8 ? 'text-primary' : 'text-slate-700'}`} />
                                        </div>

                                        <div className="flex-1 min-w-0">
                                            <p className="text-[10px] font-black text-slate-600 uppercase tracking-widest mb-2">Target Schema</p>
                                            <div className="flex items-center gap-2">
                                                <div className={`flex-1 p-3 rounded-xl border-2 border-dashed transition-all flex items-center justify-between min-h-[50px] ${m.template_column ? 'bg-primary/5 border-primary/20' : 'bg-black/20 border-royal-green-800'
                                                    }`}>
                                                    {m.template_column ? (
                                                        <div className="flex flex-col">
                                                            <div className="flex items-center gap-2">
                                                                <input
                                                                    type="text"
                                                                    value={m.output_name}
                                                                    onChange={(e) => updateMapping(m.id, { output_name: e.target.value })}
                                                                    className="bg-transparent border-none p-0 text-sm font-black text-primary outline-none focus:ring-0 w-full"
                                                                />
                                                                <Edit3 className="w-3 h-3 text-primary/40" />
                                                            </div>
                                                            <span className="text-[10px] font-mono text-slate-500">{m.template_type || 'Unknown'}</span>
                                                        </div>
                                                    ) : (
                                                        <input
                                                            type="text"
                                                            value={m.output_name}
                                                            onChange={(e) => updateMapping(m.id, { output_name: e.target.value })}
                                                            placeholder="Custom Name..."
                                                            className="bg-transparent border-none p-0 text-sm font-black text-slate-100 outline-none focus:ring-0 w-full placeholder:text-slate-700"
                                                        />
                                                    )}
                                                    {m.template_column && (
                                                        <button onClick={() => updateMapping(m.id, { template_column: null, template_type: null })} className="p-1 hover:text-red-400 text-slate-600 transition-colors">
                                                            <X className="w-3.5 h-3.5" />
                                                        </button>
                                                    )}
                                                </div>
                                            </div>
                                        </div>

                                        <button
                                            onClick={() => removeMapping(m.id)}
                                            className="opacity-0 group-hover:opacity-100 absolute -right-3 top-1/2 -translate-y-1/2 p-2 bg-red-500/20 hover:bg-red-500 text-red-400 hover:text-white rounded-full transition-all border border-red-500/30"
                                        >
                                            <Trash2 className="w-4 h-4" />
                                        </button>
                                    </motion.div>
                                ))}
                            </AnimatePresence>
                        )}

                        {/* Drop hint when dragging */}
                        <div className="p-8 border-2 border-dashed border-royal-green-800 rounded-3xl flex flex-col items-center justify-center gap-3 group-hover:border-primary/40 transition-colors">
                            <Plus className="w-6 h-6 text-royal-green-800" />
                            <p className="text-[10px] font-black uppercase text-slate-600 tracking-[0.2em]">Drop to create new mapping</p>
                        </div>
                    </div>

                    <div className="p-6 bg-black/40 border-t border-royal-green-700 backdrop-blur-md">
                        <div className="flex flex-col md:flex-row items-center justify-between gap-6">
                            <div className="flex items-center gap-6">
                                <div className="flex items-center gap-3">
                                    <div className="p-2 bg-amber-500/10 rounded-lg">
                                        <Info className="w-4 h-4 text-amber-500" />
                                    </div>
                                    <div className="text-left">
                                        <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Selected For Quality Scan</p>
                                        <p className="text-sm font-black text-slate-100">
                                            {mappings.filter(m => m.file_column).length} Columns
                                            <span className="text-slate-500 mx-2">/</span>
                                            <span className="text-slate-500">{fileColumns.length} Total</span>
                                        </p>
                                    </div>
                                </div>
                            </div>

                            <button
                                onClick={handleConfirm}
                                disabled={applying || mappings.filter(m => m.file_column).length === 0}
                                className="relative group overflow-hidden px-10 py-4 bg-primary rounded-2xl shadow-[0_0_30px_rgba(34,197,94,0.3)] hover:shadow-[0_0_50px_rgba(34,197,94,0.5)] transition-all active:scale-95 disabled:opacity-40 disabled:grayscale disabled:shadow-none"
                            >
                                <div className="relative z-10 flex items-center gap-3">
                                    {applying ? (
                                        <>
                                            <RefreshCw className="w-5 h-5 animate-spin" />
                                            <span className="font-black uppercase tracking-tighter text-black">Locking Schema...</span>
                                        </>
                                    ) : (
                                        <>
                                            <span className="font-black uppercase tracking-tighter text-black">Initialize Data Quality Agent</span>
                                            <ChevronRight className="w-5 h-5 text-black group-hover:translate-x-1 transition-transform" />
                                        </>
                                    )}
                                </div>
                                <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 -translate-x-full group-hover:translate-x-full transition-transform duration-1000" />
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
