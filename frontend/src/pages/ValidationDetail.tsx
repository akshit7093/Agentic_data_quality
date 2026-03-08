import { useState, useMemo, useEffect, useRef } from 'react';
import { useParams, Link } from 'react-router-dom';
import {
  CheckCircle, XCircle, AlertCircle, ArrowLeft, RefreshCw,
  Brain, BarChart3, Activity, Database, Terminal, Filter,
  Sparkles, Bot, Ticket, Loader2, Download, PieChart,
  ChevronDown, ChevronRight, Code, User, ExternalLink,
} from 'lucide-react';
import { useValidationStatus, useValidationResults } from '@/hooks/useValidations';
import ExecutionTraceViewer from '@/components/ExecutionTraceViewer';
import ValidationCharts from '@/components/ValidationCharts';
import MermaidDiagram from '@/components/MermaidDiagram';
import * as XLSX from 'xlsx-js-style';
import ChatbotPanel from '@/components/ChatbotPanel';

// ═══════════════════════════════════════════════════════════════
//  CITY BUILDER ANIMATION
// ═══════════════════════════════════════════════════════════════

const STEPS = [
  { id: 'connect', label: 'Connecting to data source', short: 'CONNECT', color: '#10b981' },
  { id: 'profile', label: 'Profiling schema & columns', short: 'PROFILE', color: '#3b82f6' },
  { id: 'explore', label: 'Exploring data patterns', short: 'EXPLORE', color: '#8b5cf6' },
  { id: 'generate', label: 'Generating validation rules', short: 'GENERATE', color: '#f59e0b' },
  { id: 'execute', label: 'Executing rule suite', short: 'EXECUTE', color: '#ef4444' },
  { id: 'analyze', label: 'Analyzing anomalies', short: 'ANALYZE', color: '#06b6d4' },
  { id: 'report', label: 'Compiling final report', short: 'REPORT', color: '#10b981' },
];

// Pixel font via box-shadows (agent character)
function AgentSprite({ x, facing }: { x: number; facing: 'left' | 'right' }) {
  const flip = facing === 'left' ? 'scaleX(-1)' : 'scaleX(1)';
  return (
    <div
      className="absolute bottom-0"
      style={{
        left: x,
        transform: flip,
        transition: 'left 1.8s linear',
        imageRendering: 'pixelated',
        width: 16,
        height: 24,
      }}
    >
      {/* Head */}
      <div style={{ position: 'absolute', top: 0, left: 4, width: 8, height: 8, background: '#fcd34d', borderRadius: 1 }} />
      {/* Visor */}
      <div style={{ position: 'absolute', top: 2, left: 5, width: 5, height: 3, background: '#10b981', opacity: 0.9, borderRadius: 1 }} />
      {/* Body */}
      <div style={{ position: 'absolute', top: 9, left: 3, width: 10, height: 8, background: '#1e40af' }} />
      {/* Belt */}
      <div style={{ position: 'absolute', top: 14, left: 3, width: 10, height: 2, background: '#10b981' }} />
      {/* Left leg */}
      <div className="agent-leg-l" style={{ position: 'absolute', top: 17, left: 4, width: 3, height: 6, background: '#1e3a8a' }} />
      {/* Right leg */}
      <div className="agent-leg-r" style={{ position: 'absolute', top: 17, left: 8, width: 3, height: 6, background: '#1e3a8a' }} />
      {/* Tool arm */}
      <div style={{ position: 'absolute', top: 10, left: 13, width: 5, height: 2, background: '#94a3b8', borderRadius: 1 }} />
      {/* Tool */}
      <div style={{ position: 'absolute', top: 9, left: 17, width: 3, height: 4, background: '#f59e0b', borderRadius: 1 }} />
    </div>
  );
}

interface Building {
  id: number;
  x: number;
  width: number;
  targetH: number;
  currentH: number;
  color: string;
  windows: { row: number; col: number }[];
  label: string;
}

function CityBuilderScene({ currentStep }: { currentStep: string }) {
  const [buildings, setBuildings] = useState<Building[]>([]);
  const [agentX, setAgentX] = useState(20);
  const [facing, setFacing] = useState<'left' | 'right'>('right');
  const [termLines, setTermLines] = useState<string[]>([]);
  const [stepIdx, setStepIdx] = useState(0);
  const [particles, setParticles] = useState<{ id: number; x: number; y: number }[]>([]);
  const intervalRef = useRef<any>(null);
  const lineRef = useRef<HTMLDivElement>(null);

  const GROUND = 120;
  const SCENE_W = 700;

  const TERMINAL_LINES = [
    '> Initializing HALE validation engine v2.4...',
    '> Loading connector registry...',
    '> Establishing database handshake...',
    '> Schema introspection complete ✓',
    '> Sampling 10,000 rows for profiling...',
    '> Computing column statistics...',
    '> Detecting semantic data types...',
    '> null_check: scanning 9 columns...',
    '> range_bounds: [min=0.12, max=499.98]',
    '> Generating LLM rule candidates...',
    '> Deduplicating rule vectors...',
    '> Compiling SQL execution plan...',
    '> Running 44 validation checks...',
    '> Evaluating anomaly patterns...',
    '> Cross-referencing RAG metadata...',
    '> Scoring quality matrix...',
    '> Building summary report...',
    '> Mermaid diagram generated ✓',
    '> Saving results to database...',
    '> Validation complete ✓',
  ];

  // Grow buildings as steps progress
  useEffect(() => {
    const idx = STEPS.findIndex(s => s.id === currentStep);
    const count = Math.max(idx + 1, 1);

    setBuildings(prev => {
      const next: Building[] = [];
      for (let i = 0; i < count && i < STEPS.length; i++) {
        const existing = prev[i];
        const x = 60 + i * 90;
        const w = 55 + (i % 3) * 10;
        const h = 30 + i * 14 + (i % 2) * 20;
        const rows = Math.floor(h / 14);
        const cols = Math.floor(w / 14);
        const wins: { row: number; col: number }[] = [];
        for (let r = 0; r < rows; r++)
          for (let c = 0; c < cols; c++)
            if (Math.random() > 0.35) wins.push({ row: r, col: c });

        next.push(existing
          ? { ...existing, targetH: h }
          : { id: i, x, width: w, targetH: h, currentH: 0, color: STEPS[i].color, windows: wins, label: STEPS[i].short });
      }
      return next;
    });
  }, [currentStep]);

  // Animate building growth
  useEffect(() => {
    const t = setInterval(() => {
      setBuildings(prev => prev.map(b =>
        b.currentH < b.targetH
          ? { ...b, currentH: Math.min(b.currentH + 4, b.targetH) }
          : b
      ));
    }, 30);
    return () => clearInterval(t);
  }, []);

  // Move agent
  useEffect(() => {
    intervalRef.current = setInterval(() => {
      setAgentX(prev => {
        const next = prev + (facing === 'right' ? 2 : -2);
        if (next > SCENE_W - 40) { setFacing('left'); return prev; }
        if (next < 10) { setFacing('right'); return prev; }
        return next;
      });
    }, 50);
    return () => clearInterval(intervalRef.current);
  }, [facing]);

  // Emit construction particles near agent
  useEffect(() => {
    const t = setInterval(() => {
      setParticles(prev => {
        const newPts = Array.from({ length: 2 }, (_, i) => ({
          id: Date.now() + i,
          x: agentX + 8 + (Math.random() - 0.5) * 30,
          y: GROUND - 10 - Math.random() * 40,
        }));
        return [...prev.slice(-12), ...newPts];
      });
    }, 200);
    return () => clearInterval(t);
  }, [agentX]);

  // Stream terminal lines
  useEffect(() => {
    let i = 0;
    const t = setInterval(() => {
      if (i < TERMINAL_LINES.length) {
        setTermLines(prev => [...prev.slice(-18), TERMINAL_LINES[i]]);
        i++;
      }
    }, 900);
    return () => clearInterval(t);
  }, []);

  useEffect(() => {
    if (lineRef.current) lineRef.current.scrollTop = lineRef.current.scrollHeight;
  }, [termLines]);

  // Step ticker
  useEffect(() => {
    const t = setInterval(() => {
      setStepIdx(i => (i + 1) % STEPS.length);
    }, 2800);
    return () => clearInterval(t);
  }, []);

  const activeStep = STEPS[stepIdx];

  return (
    <div className="relative w-full overflow-hidden rounded-2xl border border-slate-700/60"
      style={{ background: 'linear-gradient(180deg,#020817 0%,#050d1a 60%,#0c1a0e 100%)' }}>

      {/* Stars */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        {Array.from({ length: 40 }, (_, i) => (
          <div key={i} className="absolute rounded-full bg-white"
            style={{
              width: Math.random() > 0.8 ? 2 : 1,
              height: Math.random() > 0.8 ? 2 : 1,
              left: `${Math.random() * 100}%`,
              top: `${Math.random() * 55}%`,
              opacity: 0.3 + Math.random() * 0.5,
              animation: `twinkle ${2 + Math.random() * 3}s ease-in-out infinite`,
              animationDelay: `${Math.random() * 4}s`,
            }} />
        ))}
      </div>

      {/* Moon */}
      <div className="absolute top-5 right-12 w-10 h-10 rounded-full"
        style={{ background: '#fef3c7', boxShadow: '0 0 20px #fef3c766, 0 0 40px #fde68a33' }} />

      {/* Cityscape SVG */}
      <div className="relative" style={{ height: 180 }}>
        <svg width="100%" height="180" viewBox={`0 0 ${SCENE_W} 180`} preserveAspectRatio="xMidYMax meet"
          style={{ position: 'absolute', bottom: 0 }}>

          {/* Ground */}
          <rect x="0" y={180 - 20} width={SCENE_W} height={20} fill="#0d2318" />
          <line x1="0" y1={180 - 20} x2={SCENE_W} y2={180 - 20} stroke="#10b981" strokeWidth="1" strokeOpacity="0.4" />

          {/* Grid lines on ground */}
          {Array.from({ length: 14 }, (_, i) => (
            <line key={i} x1={i * 50} y1={180 - 20} x2={i * 50 + 10} y2={180}
              stroke="#10b981" strokeWidth="0.5" strokeOpacity="0.2" />
          ))}

          {/* Buildings */}
          {buildings.map(b => (
            <g key={b.id}>
              {/* Building body */}
              <rect
                x={b.x} y={180 - 20 - b.currentH}
                width={b.width} height={b.currentH}
                fill="#0f1f2e" stroke={b.color} strokeWidth="1" strokeOpacity="0.7"
              />
              {/* Roof accent */}
              <rect
                x={b.x} y={180 - 20 - b.currentH}
                width={b.width} height={3}
                fill={b.color} opacity="0.8"
              />
              {/* Windows */}
              {b.windows.slice(0, Math.floor((b.currentH / b.targetH) * b.windows.length)).map((w, wi) => (
                <rect
                  key={wi}
                  x={b.x + 5 + w.col * 14}
                  y={180 - 20 - b.currentH + 8 + w.row * 14}
                  width={8} height={6}
                  fill={b.color} opacity={0.3 + Math.random() * 0.4}
                  rx={1}
                />
              ))}
              {/* Antenna */}
              {b.currentH >= b.targetH && (
                <>
                  <line
                    x1={b.x + b.width / 2} y1={180 - 20 - b.currentH}
                    x2={b.x + b.width / 2} y2={180 - 20 - b.currentH - 12}
                    stroke={b.color} strokeWidth="1" opacity="0.9"
                  />
                  <circle cx={b.x + b.width / 2} cy={180 - 20 - b.currentH - 14}
                    r={2.5} fill={b.color} opacity="0.9">
                    <animate attributeName="opacity" values="0.9;0.2;0.9" dur="1.5s" repeatCount="indefinite" />
                  </circle>
                </>
              )}
              {/* Label */}
              {b.currentH >= b.targetH * 0.7 && (
                <text
                  x={b.x + b.width / 2} y={180 - 20 - b.currentH / 2}
                  textAnchor="middle" fill={b.color} fontSize="7"
                  fontFamily="monospace" opacity="0.7"
                  transform={`rotate(-90, ${b.x + b.width / 2}, ${180 - 20 - b.currentH / 2})`}
                >
                  {b.label}
                </text>
              )}
            </g>
          ))}

          {/* Particles */}
          {particles.map(p => (
            <circle key={p.id} cx={p.x} cy={p.y} r={1.5}
              fill="#f59e0b" opacity="0.7">
              <animate attributeName="cy" values={`${p.y};${p.y - 20}`} dur="0.8s" fill="freeze" />
              <animate attributeName="opacity" values="0.7;0" dur="0.8s" fill="freeze" />
            </circle>
          ))}

          {/* Scan line effect */}
          <rect x="0" y="0" width={SCENE_W} height="180" fill="url(#scanlines)" opacity="0.04" />
          <defs>
            <pattern id="scanlines" width="1" height="3" patternUnits="userSpaceOnUse">
              <rect width="1" height="1" fill="white" />
            </pattern>
          </defs>
        </svg>

        {/* Agent (DOM, not SVG, for simpler CSS animation) */}
        <div className="absolute" style={{ bottom: 20, left: 0, width: '100%', height: 30, pointerEvents: 'none' }}>
          <AgentSprite x={agentX} facing={facing} />
        </div>
      </div>

      {/* Bottom HUD */}
      <div className="px-5 py-4 border-t border-slate-800/80"
        style={{ background: 'rgba(2,8,23,0.85)' }}>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">

          {/* Active step indicator */}
          <div>
            <div className="flex items-center gap-2 mb-3">
              <div className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ background: activeStep.color }} />
              <span className="text-[10px] font-mono font-bold text-slate-500 uppercase tracking-widest">AGENT STATUS</span>
            </div>
            <div className="font-mono text-sm font-bold mb-3" style={{ color: activeStep.color }}>
              <span className="opacity-50 mr-2">▶</span>{activeStep.label}
              <span className="inline-block w-2 h-4 ml-1 align-middle animate-pulse"
                style={{ background: activeStep.color, opacity: 0.8 }} />
            </div>
            {/* Step progress dots */}
            <div className="flex gap-1.5 flex-wrap">
              {STEPS.map((s, i) => (
                <div key={s.id} className="flex items-center gap-1">
                  <div className="w-2 h-2 rounded-full transition-all duration-500"
                    style={{
                      background: i <= stepIdx ? s.color : '#1e293b',
                      boxShadow: i === stepIdx ? `0 0 8px ${s.color}` : 'none',
                    }} />
                  {i < STEPS.length - 1 && (
                    <div className="w-4 h-px transition-all duration-500"
                      style={{ background: i < stepIdx ? s.color : '#1e293b' }} />
                  )}
                </div>
              ))}
            </div>
          </div>

          {/* Terminal output */}
          <div className="rounded-lg border border-slate-800 overflow-hidden"
            style={{ background: '#020817' }}>
            <div className="flex items-center gap-1.5 px-3 py-1.5 border-b border-slate-800">
              <div className="w-2.5 h-2.5 rounded-full bg-red-500/70" />
              <div className="w-2.5 h-2.5 rounded-full bg-amber-500/70" />
              <div className="w-2.5 h-2.5 rounded-full bg-emerald-500/70" />
              <span className="ml-2 text-[9px] font-mono text-slate-600">hale-agent — validation-process</span>
            </div>
            <div ref={lineRef} className="p-3 h-24 overflow-hidden font-mono text-[10px] space-y-0.5">
              {termLines.map((line, i) => (
                <div key={i} className="flex items-start gap-1.5"
                  style={{ animation: i === termLines.length - 1 ? 'fadeSlideIn 0.3s ease' : 'none' }}>
                  <span className="text-emerald-500 shrink-0">$</span>
                  <span className={i === termLines.length - 1 ? 'text-slate-200' : 'text-slate-500'}>
                    {line}
                  </span>
                </div>
              ))}
              <div className="flex items-center gap-1">
                <span className="text-emerald-500">$</span>
                <span className="inline-block w-1.5 h-3 bg-emerald-500 animate-pulse" />
              </div>
            </div>
          </div>
        </div>
      </div>

      <style>{`
        @keyframes twinkle { 0%,100%{opacity:0.2} 50%{opacity:1} }
        @keyframes fadeSlideIn { from{opacity:0;transform:translateY(4px)} to{opacity:1;transform:translateY(0)} }
        @keyframes agentWalk { 0%,100%{transform:translateY(0)} 50%{transform:translateY(-2px)} }
        .agent-leg-l { animation: legSwingL 0.4s ease-in-out infinite; transform-origin: top center; }
        .agent-leg-r { animation: legSwingR 0.4s ease-in-out infinite; transform-origin: top center; }
        @keyframes legSwingL { 0%,100%{transform:skewX(8deg)} 50%{transform:skewX(-8deg)} }
        @keyframes legSwingR { 0%,100%{transform:skewX(-8deg)} 50%{transform:skewX(8deg)} }
      `}</style>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
//  SCORE RING
// ═══════════════════════════════════════════════════════════════

function ScoreRing({ score }: { score: number }) {
  const r = 52;
  const circ = 2 * Math.PI * r;
  const dash = (score / 100) * circ;
  const color = score >= 90 ? '#10b981' : score >= 70 ? '#f59e0b' : '#ef4444';

  return (
    <div className="relative w-32 h-32 shrink-0">
      <svg viewBox="0 0 120 120" className="w-full h-full -rotate-90">
        <circle cx="60" cy="60" r={r} fill="none" stroke="#1e293b" strokeWidth="10" />
        <circle cx="60" cy="60" r={r} fill="none" stroke={color} strokeWidth="10"
          strokeDasharray={`${dash} ${circ}`} strokeLinecap="round"
          style={{ transition: 'stroke-dasharray 1.5s ease', filter: `drop-shadow(0 0 6px ${color}88)` }}
        />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-3xl font-black tabular-nums" style={{ color }}>{score}</span>
        <span className="text-[9px] text-slate-500 font-bold uppercase tracking-widest">score</span>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
//  BADGE HELPERS
// ═══════════════════════════════════════════════════════════════

const statusBadge = (s: string) => {
  const map: Record<string, string> = {
    passed: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/25',
    failed: 'bg-red-500/10 text-red-400 border-red-500/25',
    warning: 'bg-amber-500/10 text-amber-400 border-amber-500/25',
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-black uppercase tracking-widest border ${map[s] || 'bg-slate-500/10 text-slate-400 border-slate-500/25'}`}>
      {s}
    </span>
  );
};

const severityBadge = (s: string) => {
  const map: Record<string, string> = {
    critical: 'bg-red-600/15 text-red-400 border-red-600/30',
    warning: 'bg-amber-600/15 text-amber-400 border-amber-600/30',
    info: 'bg-blue-600/15 text-blue-400 border-blue-600/30',
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-black uppercase tracking-widest border ${map[s] || 'bg-slate-600/15 text-slate-400 border-slate-600/30'}`}>
      {s}
    </span>
  );
};

const statusIcon = (s: string) => {
  if (s === 'passed') return <CheckCircle className="w-4 h-4 text-emerald-500" />;
  if (s === 'failed') return <XCircle className="w-4 h-4 text-red-500" />;
  if (s === 'warning') return <AlertCircle className="w-4 h-4 text-amber-500" />;
  return <AlertCircle className="w-4 h-4 text-slate-500" />;
};

// ═══════════════════════════════════════════════════════════════
//  MAIN COMPONENT
// ═══════════════════════════════════════════════════════════════

export default function ValidationDetail() {
  const { id } = useParams<{ id: string }>();
  const [activeTab, setActiveTab] = useState<'results' | 'charts' | 'workflow' | 'logs' | 'chatbot'>('results');
  const [showExportMenu, setShowExportMenu] = useState(false);

  const [severityFilter, setSeverityFilter] = useState('all');
  const [fixabilityFilter, setFixabilityFilter] = useState('all');

  const [selectedFixes, setSelectedFixes] = useState<Set<string>>(new Set());
  const [fixInstructions, setFixInstructions] = useState('');
  const [isFixing, setIsFixing] = useState(false);
  const [fixSuccess, setFixSuccess] = useState<any>(null);

  const [isTicketing, setIsTicketing] = useState(false);
  const [generatedTicket, setGeneratedTicket] = useState<string | null>(null);
  const [selectedTickets, setSelectedTickets] = useState<Set<string>>(new Set());
  const [users, setUsers] = useState<any[]>([]);
  const [assignedUser, setAssignedUser] = useState('');
  const [createdTickets, setCreatedTickets] = useState<any[]>([]);

  useEffect(() => {
    fetch('/api/v1/users').then(r => r.json()).then(d => {
      setUsers(d);
      if (d.length > 0) setAssignedUser(d[0].username);
    }).catch(console.error);
  }, []);

  const [expandedColumns, setExpandedColumns] = useState<Set<string>>(new Set());
  const [expandedCommands, setExpandedCommands] = useState<Set<string>>(new Set());
  const [expandedReasoning, setExpandedReasoning] = useState<Set<string>>(new Set());

  const toggle = (set: Set<string>, setter: any, key: string) => {
    const next = new Set(set);
    next.has(key) ? next.delete(key) : next.add(key);
    setter(next);
  };

  const { data: validation, isLoading: statusLoading, error: statusError } = useValidationStatus(id || null);
  const isCompleted = validation?.status === 'completed';
  const { data: resultsData, isLoading: resultsLoading } = useValidationResults(id || null, isCompleted);
  const results = resultsData?.results || [];
  const dataProfile = validation?.data_profile;

  const isRuleFixable = (rule: any) => {
    const t = rule.rule_type?.toLowerCase() || '';
    return ['not_null', 'regex', 'range', 'accepted_values', 'valid_values'].includes(t) || rule.fix_recommendations?.length > 0;
  };

  const filteredResults = useMemo(() => results.filter((r: any) => {
    const matchSev = severityFilter === 'all' || r.severity === severityFilter;
    const fixable = isRuleFixable(r);
    const matchFix = fixabilityFilter === 'all'
      || (fixabilityFilter === 'fixable' && fixable)
      || (fixabilityFilter === 'manual' && !fixable);
    return matchSev && matchFix;
  }), [results, severityFilter, fixabilityFilter]);

  const handleAutoFix = async () => {
    if (!selectedFixes.size) return;
    setIsFixing(true); setFixSuccess(null);
    try {
      const res = await fetch(`/api/v1/validate/${id}/fix`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ fix_instructions: Array.from(selectedFixes).map(ruleId => ({ rule_id: ruleId, instruction: fixInstructions || 'Fix automatically.' })), use_agent: true }),
      });
      if (!res.ok) throw new Error(await res.text());
      setFixSuccess(await res.json());
      setSelectedFixes(new Set());
    } catch (e) { alert('Failed to apply fixes'); }
    finally { setIsFixing(false); }
  };

  const handleGenerateTicket = async () => {
    if (!selectedTickets.size) return;
    setIsTicketing(true);
    try {
      const res = await fetch(`/api/v1/validate/${id}/ticket`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rule_names: Array.from(selectedTickets) }),
      });
      if (!res.ok) throw new Error(await res.text());
      setGeneratedTicket((await res.json()).ticket_markdown);
    } catch (e) { alert('Failed to generate ticket'); }
    finally { setIsTicketing(false); }
  };

  const handleDispatchTicket = async () => {
    if (!generatedTicket || !assignedUser) return;
    try {
      const res = await fetch(`/api/v1/validate/${id}/notify`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticket_markdown: generatedTicket, assigned_to: assignedUser, rule_names: Array.from(selectedTickets) }),
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      alert(`Success: ${data.message}`);
      setCreatedTickets(prev => [...prev, data.ticket]);
      setGeneratedTicket(null); setSelectedTickets(new Set());
    } catch (e) { alert('Failed to assign ticket'); }
  };

  // ── LOADING ─────────────────────────────────────────────────
  if (statusLoading) return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] gap-4">
      <div className="relative">
        <div className="w-14 h-14 rounded-full border-2 border-slate-800" />
        <div className="absolute inset-0 w-14 h-14 rounded-full border-2 border-emerald-500 border-t-transparent animate-spin" />
        <div className="absolute inset-2 w-10 h-10 rounded-full border-2 border-slate-700 border-b-transparent animate-spin" style={{ animationDirection: 'reverse', animationDuration: '0.8s' }} />
      </div>
      <p className="font-mono text-sm text-slate-500 animate-pulse">Loading validation...</p>
    </div>
  );

  // ── ERROR ────────────────────────────────────────────────────
  if (statusError) return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] gap-4">
      <XCircle className="w-12 h-12 text-red-500" />
      <h2 className="text-xl font-bold text-slate-100">Validation not found</h2>
      <Link to="/validations" className="flex items-center gap-2 px-4 py-2 rounded-lg bg-slate-800 border border-slate-700 text-slate-200 hover:border-emerald-500/50 transition-all text-sm font-medium">
        <ArrowLeft className="w-4 h-4" /> Back to Validations
      </Link>
    </div>
  );

  // ── RUNNING / PENDING ────────────────────────────────────────
  if (validation?.status === 'pending' || validation?.status === 'running') {
    const step = validation?.current_step || 'connect';
    return (
      <div className="space-y-6 max-w-4xl mx-auto">
        <div className="flex items-center gap-4">
          <Link to="/validations" className="p-2 rounded-lg bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200 hover:border-slate-700 transition-all">
            <ArrowLeft className="w-4 h-4" />
          </Link>
          <div>
            <h1 className="text-xl font-black text-slate-100 tracking-tight">Validation Running</h1>
            <p className="text-sm font-mono text-slate-500">{validation?.target_path || id}</p>
          </div>
          <div className="ml-auto flex items-center gap-2 px-3 py-1.5 rounded-full border border-emerald-500/30 bg-emerald-500/5">
            <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
            <span className="text-[11px] font-mono font-bold text-emerald-400 uppercase tracking-widest">
              {validation?.status}
            </span>
          </div>
        </div>

        {/* Stats row */}
        <div className="grid grid-cols-3 gap-3">
          {[
            { label: 'Passed', value: validation?.passed_rules || 0, color: 'text-emerald-400', bg: 'bg-emerald-500/5 border-emerald-500/20' },
            { label: 'Failed', value: validation?.failed_rules || 0, color: 'text-red-400', bg: 'bg-red-500/5 border-red-500/20' },
            { label: 'Rules', value: validation?.total_rules || 0, color: 'text-slate-100', bg: 'bg-slate-900 border-slate-800' },
          ].map(s => (
            <div key={s.label} className={`rounded-xl border p-4 text-center ${s.bg}`}>
              <div className={`text-3xl font-black tabular-nums ${s.color}`}>{s.value}</div>
              <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mt-1">{s.label}</div>
            </div>
          ))}
        </div>

        {/* City animation */}
        <CityBuilderScene currentStep={step} />

        <p className="text-center text-[11px] font-mono text-slate-600">Auto-refreshing every 2s</p>
      </div>
    );
  }

  // ── AGENT FAILED ─────────────────────────────────────────────
  if (validation?.status === 'failed') return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Link to="/validations" className="p-2 rounded-lg bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200 transition-all">
          <ArrowLeft className="w-4 h-4" />
        </Link>
        <h1 className="text-xl font-black text-slate-100">Validation Failed</h1>
      </div>
      <div className="rounded-2xl border border-red-500/20 bg-red-500/5 p-12 flex flex-col items-center gap-4">
        <XCircle className="w-14 h-14 text-red-500" />
        <p className="text-slate-300 text-center">{validation?.error_message || 'An unknown error occurred'}</p>
        <Link to="/validations/new" className="px-5 py-2.5 rounded-lg bg-slate-800 border border-slate-700 text-slate-200 hover:border-emerald-500/50 transition-all font-medium text-sm">
          Try Again
        </Link>
      </div>
    </div>
  );

  // ── COMPLETED ────────────────────────────────────────────────
  const qualityScore = validation?.quality_score ?? 0;
  const totalRules = validation?.total_rules ?? 0;
  const passedRules = validation?.passed_rules ?? 0;
  const failedRules = validation?.failed_rules ?? 0;
  const warnRules = totalRules - passedRules - failedRules;
  const failRate = totalRules > 0 ? ((failedRules / totalRules) * 100).toFixed(1) : '0.0';
  const scoreColor = qualityScore >= 90 ? '#10b981' : qualityScore >= 70 ? '#f59e0b' : '#ef4444';

  const TABS = [
    { id: 'results', label: 'Results', Icon: BarChart3 },
    { id: 'charts', label: 'Charts', Icon: PieChart },
    { id: 'workflow', label: 'Workflow', Icon: Activity },
    { id: 'logs', label: 'Agent Log', Icon: Terminal },
    { id: 'chatbot', label: 'AI Chat', Icon: Bot },
  ] as const;

  return (
    <div className="space-y-5">
      {/* ── HEADER ──────────────────────────────────────────── */}
      <div className="flex items-start justify-between gap-4 flex-wrap">
        <div className="flex items-center gap-3">
          <Link to="/validations"
            className="p-2 rounded-lg bg-slate-900 border border-slate-800 text-slate-400 hover:text-emerald-400 hover:border-emerald-500/40 transition-all">
            <ArrowLeft className="w-4 h-4" />
          </Link>
          <div>
            <h1 className="text-2xl font-black text-slate-100 tracking-tight">Validation Detail</h1>
            <p className="text-sm font-mono text-slate-500 mt-0.5">{validation?.target_path || id}</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {/* Export */}
          <div className="relative">
            <button onClick={() => setShowExportMenu(!showExportMenu)}
              className="flex items-center gap-2 px-3 py-2 rounded-lg bg-slate-900 border border-slate-800 text-slate-300 hover:border-slate-600 hover:text-slate-100 transition-all text-sm font-medium">
              <Download className="w-4 h-4" /> Export <ChevronDown className="w-3 h-3" />
            </button>
            {showExportMenu && (
              <div className="absolute right-0 mt-1 w-44 bg-slate-900 border border-slate-700 rounded-xl shadow-xl z-20 py-1.5 overflow-hidden">
                {[
                  {
                    label: 'CSV', action: () => {
                      const headers = ['rule_name', 'rule_type', 'severity', 'status', 'passed_count', 'failed_count', 'ai_insights'];
                      const rows = [headers.join(','), ...results.map((r: any) => headers.map(h => { const v = String(r[h] ?? '').replace(/"/g, '""'); return v.includes(',') ? `"${v}"` : v; }).join(','))];
                      const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([rows.join('\n')], { type: 'text/csv' })); a.download = `validation_${id}.csv`; a.click();
                    }
                  },
                  {
                    label: 'Excel', action: () => {
                      const ws = XLSX.utils.json_to_sheet(results.map((r: any) => ({ 'Rule': r.rule_name, 'Type': r.rule_type, 'Severity': r.severity, 'Status': r.status, 'Passed': r.passed_count ?? '', 'Failed': r.failed_count ?? '', 'Insight': r.ai_insights ?? '' })));
                      const wb = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(wb, ws, 'Results'); XLSX.writeFile(wb, `validation_${id}.xlsx`);
                    }
                  },
                  {
                    label: 'JSON', action: () => {
                      const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([JSON.stringify({ validation, results }, null, 2)], { type: 'application/json' })); a.download = `validation_${id}.json`; a.click();
                    }
                  },
                  { label: 'PDF Report', action: () => window.open(`http://localhost:8000/api/v1/validate/${id}/export?format=pdf`, '_blank') },
                ].map(item => (
                  <button key={item.label} onClick={() => { setShowExportMenu(false); item.action(); }}
                    className="w-full text-left px-4 py-2 text-sm text-slate-300 hover:bg-slate-800 hover:text-slate-100 flex items-center gap-2 transition-colors">
                    <Download className="w-3.5 h-3.5 text-slate-500" /> {item.label}
                  </button>
                ))}
              </div>
            )}
          </div>
          <Link to="/validations/new"
            className="flex items-center gap-2 px-3 py-2 rounded-lg bg-slate-900 border border-slate-800 text-slate-300 hover:border-emerald-500/40 hover:text-emerald-400 transition-all text-sm font-medium">
            <RefreshCw className="w-4 h-4" /> Re-run
          </Link>
        </div>
      </div>

      {/* ── SCORE CARD ──────────────────────────────────────── */}
      <div className="rounded-2xl border border-slate-800 overflow-hidden"
        style={{ background: 'linear-gradient(135deg,#0a0f1e 0%,#0d1a12 100%)' }}>
        <div className="p-6 flex flex-col sm:flex-row items-start sm:items-center gap-6">
          <ScoreRing score={qualityScore} />
          <div className="flex-1">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-[10px] font-black text-slate-500 uppercase tracking-widest">Quality Score</span>
              <span className="px-2 py-0.5 rounded-full border text-[10px] font-black uppercase"
                style={{ borderColor: `${scoreColor}44`, color: scoreColor, background: `${scoreColor}10` }}>
                {qualityScore >= 90 ? 'Excellent' : qualityScore >= 70 ? 'Needs Attention' : 'Critical'}
              </span>
            </div>
            <p className="text-slate-400 text-sm">Based on {totalRules} validation rules</p>
            {/* Progress bar */}
            <div className="mt-3 h-1.5 bg-slate-800 rounded-full overflow-hidden">
              <div className="h-full rounded-full transition-all duration-1000"
                style={{ width: `${qualityScore}%`, background: `linear-gradient(90deg, ${scoreColor}88, ${scoreColor})` }} />
            </div>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 sm:gap-4">
            {[
              { label: 'Passed', value: passedRules, color: '#10b981' },
              { label: 'Failed', value: failedRules, color: '#ef4444' },
              { label: 'Warnings', value: warnRules, color: '#f59e0b' },
              { label: 'Fail Rate', value: `${failRate}%`, color: '#94a3b8' },
            ].map(s => (
              <div key={s.label} className="text-center px-3 py-2 rounded-xl bg-black/30 border border-slate-800/60">
                <div className="text-xl font-black tabular-nums" style={{ color: s.color }}>{s.value}</div>
                <div className="text-[9px] font-black text-slate-600 uppercase tracking-widest mt-0.5">{s.label}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Metadata row */}
        <div className="px-6 py-3 border-t border-slate-800/60 flex flex-wrap gap-6"
          style={{ background: 'rgba(0,0,0,0.2)' }}>
          {[
            { label: 'Mode', value: validation?.validation_mode?.replace('_', ' ') || '—' },
            { label: 'Started', value: validation?.started_at ? new Date(validation.started_at).toLocaleString() : '—' },
            {
              label: 'Duration', value: validation?.started_at && validation?.completed_at
                ? `${Math.round((new Date(validation.completed_at).getTime() - new Date(validation.started_at).getTime()) / 1000)}s`
                : '—'
            },
          ].map(m => (
            <div key={m.label}>
              <span className="text-[9px] font-black text-slate-600 uppercase tracking-widest">{m.label} </span>
              <span className="text-[11px] font-mono text-slate-300 capitalize">{m.value}</span>
            </div>
          ))}
          {validation?.slice_filters && Object.keys(validation.slice_filters).length > 0 && (
            <div className="flex items-center gap-2">
              <span className="text-[9px] font-black text-slate-600 uppercase tracking-widest">Filters</span>
              {Object.entries(validation.slice_filters).map(([col, val]) => (
                <span key={col} className="px-2 py-0.5 rounded-md text-[10px] font-mono bg-blue-500/10 text-blue-300 border border-blue-500/20">
                  {col}={String(val)}
                </span>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* ── DATA PROFILE ────────────────────────────────────── */}
      {dataProfile && (
        <div className="rounded-2xl border border-slate-800 bg-slate-900 overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-800 flex items-center gap-2">
            <Database className="w-4 h-4 text-emerald-500" />
            <h2 className="text-sm font-bold text-slate-200 uppercase tracking-widest">Data Profile</h2>
            <div className="ml-auto flex gap-4">
              <div className="text-center">
                <span className="text-lg font-black text-slate-100">{dataProfile.row_count?.toLocaleString() || 0}</span>
                <span className="text-[10px] text-slate-500 font-bold ml-1.5 uppercase">rows</span>
              </div>
              <div className="text-center">
                <span className="text-lg font-black text-slate-100">{dataProfile.column_count || 0}</span>
                <span className="text-[10px] text-slate-500 font-bold ml-1.5 uppercase">cols</span>
              </div>
            </div>
          </div>
          {dataProfile.column_profiles && Object.keys(dataProfile.column_profiles).length > 0 && (
            <div className="overflow-x-auto">
              <table className="w-full text-left">
                <thead>
                  <tr className="border-b border-slate-800">
                    {['Column', 'Type', 'Missing', 'Unique', 'Range'].map(h => (
                      <th key={h} className="px-4 py-3 text-[10px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(dataProfile.column_profiles).map(([col, p]: [string, any]) => (
                    <tr key={col} className="border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors">
                      <td className="px-4 py-3 font-mono text-sm font-medium text-slate-200">{col}</td>
                      <td className="px-4 py-3">
                        <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-slate-800 text-slate-300">{p.type}</span>
                      </td>
                      <td className="px-4 py-3 font-mono text-sm">
                        <span className={p.null_count > 0 ? 'text-amber-400' : 'text-emerald-400'}>
                          {p.null_percentage?.toFixed(1)}%
                        </span>
                        <span className="text-slate-600 text-xs ml-1">({p.null_count})</span>
                      </td>
                      <td className="px-4 py-3 font-mono text-sm text-slate-300">
                        {p.unique_percentage?.toFixed(1)}%
                        <span className="text-slate-600 text-xs ml-1">({p.unique_count})</span>
                      </td>
                      <td className="px-4 py-3 font-mono text-xs text-slate-500">
                        {p.min !== undefined && p.max !== undefined
                          ? `${typeof p.min === 'number' ? p.min.toFixed(2) : p.min} – ${typeof p.max === 'number' ? p.max.toFixed(2) : p.max}`
                          : '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* ── TABS ────────────────────────────────────────────── */}
      <div className="flex items-center gap-0.5 border-b border-slate-800">
        {TABS.map(({ id: tid, label, Icon }) => (
          <button key={tid} onClick={() => setActiveTab(tid as any)}
            className={`flex items-center gap-1.5 px-4 py-3 text-sm font-semibold border-b-2 -mb-px transition-all
              ${activeTab === tid
                ? 'border-emerald-500 text-emerald-400'
                : 'border-transparent text-slate-500 hover:text-slate-300 hover:border-slate-700'}`}>
            <Icon className="w-4 h-4" />
            <span className="hidden sm:inline">{label}</span>
          </button>
        ))}
      </div>

      {/* ── TAB: CHARTS ─────────────────────────────────────── */}
      {activeTab === 'charts' && <ValidationCharts results={results} qualityScore={qualityScore} />}

      {/* ── TAB: WORKFLOW ────────────────────────────────────── */}
      {activeTab === 'workflow' && (
        <div className="rounded-2xl border border-slate-800 bg-slate-900 overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-800 flex items-center gap-2">
            <Activity className="w-4 h-4 text-emerald-500" />
            <h2 className="text-sm font-bold text-slate-200 uppercase tracking-widest">Agent Execution Path</h2>
          </div>
          <div className="p-5">
            {(validation?.result?.summary_report?.mermaid_diagram || resultsData?.raw_state?.summary_report?.mermaid_diagram)
              ? <MermaidDiagram chart={validation?.result?.summary_report?.mermaid_diagram || resultsData?.raw_state?.summary_report?.mermaid_diagram} />
              : <div className="text-center py-12 text-slate-500 font-mono text-sm">No workflow diagram available.</div>}
          </div>
        </div>
      )}

      {/* ── TAB: CHATBOT ─────────────────────────────────────── */}
      {activeTab === 'chatbot' && id && (
        <ChatbotPanel validationId={id} initialHistory={validation?.chat_history || []} />
      )}

      {/* ── TAB: RESULTS ─────────────────────────────────────── */}
      {activeTab === 'results' && (
        <div className="space-y-4">
          {/* Filter bar */}
          <div className="flex flex-wrap items-center gap-3 px-4 py-3 rounded-xl border border-slate-800 bg-slate-900">
            <Filter className="w-3.5 h-3.5 text-emerald-500 shrink-0" />
            {[
              { value: severityFilter, onChange: setSeverityFilter, opts: [['all', 'All Severities'], ['critical', 'Critical'], ['warning', 'Warning'], ['info', 'Info']] },
              { value: fixabilityFilter, onChange: setFixabilityFilter, opts: [['all', 'All Types'], ['fixable', 'Auto-fixable'], ['manual', 'Manual']] },
            ].map((sel, i) => (
              <select key={i} value={sel.value} onChange={e => sel.onChange(e.target.value)}
                className="text-[11px] font-bold bg-slate-800 border border-slate-700 rounded-lg text-slate-200 px-3 py-1.5 focus:ring-emerald-500 focus:border-emerald-500 cursor-pointer">
                {sel.opts.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
              </select>
            ))}

            {/* Ticket staging */}
            {selectedTickets.size > 0 && (
              <div className="flex items-center gap-3 ml-auto bg-violet-500/10 px-3 py-2 rounded-lg border border-violet-500/25">
                <span className="text-[10px] font-black text-violet-400 uppercase tracking-widest flex items-center gap-1.5">
                  <div className="w-1.5 h-1.5 rounded-full bg-violet-500 animate-pulse" />
                  {selectedTickets.size} staged
                </span>
                <button onClick={handleGenerateTicket} disabled={isTicketing}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-[10px] font-black uppercase tracking-widest transition-all">
                  {isTicketing ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Ticket className="w-3.5 h-3.5" />}
                  Generate Ticket
                </button>
              </div>
            )}

            {/* Auto-fix */}
            {selectedFixes.size > 0 && (
              <div className="flex items-center gap-3 ml-auto bg-emerald-500/10 px-3 py-2 rounded-lg border border-emerald-500/25">
                <input type="text" placeholder="Add context..."
                  className="text-[11px] font-medium bg-slate-800 border border-slate-700 rounded-lg text-slate-200 px-3 py-1.5 w-48 focus:ring-emerald-500 focus:border-emerald-500"
                  value={fixInstructions} onChange={e => setFixInstructions(e.target.value)} />
                <button onClick={handleAutoFix} disabled={isFixing}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-black font-black text-[10px] uppercase tracking-widest transition-all">
                  {isFixing ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Bot className="w-3.5 h-3.5" />}
                  Auto-Fix ({selectedFixes.size})
                </button>
              </div>
            )}
          </div>

          {/* Fix success */}
          {fixSuccess && (
            <div className="flex items-start gap-3 p-4 rounded-xl border border-emerald-500/25 bg-emerald-500/5">
              <CheckCircle className="w-5 h-5 text-emerald-500 shrink-0 mt-0.5" />
              <div className="flex-1">
                <p className="text-sm font-bold text-emerald-300">Fixes Applied Successfully</p>
                <p className="text-sm text-slate-400 mt-0.5">{fixSuccess.rows_removed} rows removed · {fixSuccess.fixed_rows} rows fixed</p>
              </div>
              <button onClick={() => setFixSuccess(null)} className="text-slate-500 hover:text-slate-300">
                <XCircle className="w-4 h-4" />
              </button>
            </div>
          )}

          {/* Results list */}
          {filteredResults.length === 0 ? (
            <div className="text-center py-12 text-slate-500 font-mono text-sm">No results match the current filter.</div>
          ) : (
            <div className="space-y-3">
              {Object.entries(
                filteredResults.reduce((acc: Record<string, any[]>, r: any) => {
                  const col = r.column_name || 'Table Level';
                  if (!acc[col]) acc[col] = [];
                  acc[col].push(r);
                  return acc;
                }, {})
              ).map(([colName, colResults]: [string, any]) => {
                const anomalies = colResults.filter((r: any) => r.status !== 'passed').length;
                const isOpen = expandedColumns.has(colName);

                return (
                  <div key={colName} className={`rounded-xl border transition-all duration-200 overflow-hidden
                    ${isOpen ? 'border-emerald-500/30 bg-slate-900' : 'border-slate-800 bg-slate-900 hover:border-slate-700'}`}>

                    {/* Column header */}
                    <button onClick={() => toggle(expandedColumns, setExpandedColumns, colName)}
                      className="w-full flex items-center justify-between px-5 py-4 text-left group">
                      <div className="flex items-center gap-3">
                        {isOpen ? <ChevronDown className="w-4 h-4 text-slate-500" /> : <ChevronRight className="w-4 h-4 text-slate-500 group-hover:text-slate-300 transition-colors" />}
                        <Database className="w-3.5 h-3.5 text-emerald-500" />
                        <span className="font-mono text-sm font-bold text-slate-200">{colName}</span>
                        <span className="text-[10px] text-slate-600 font-bold">{colResults.length} rules</span>
                      </div>
                      <div>
                        {anomalies > 0
                          ? <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[10px] font-black border bg-red-500/10 border-red-500/25 text-red-400">
                            <span className="w-1.5 h-1.5 rounded-full bg-red-500 animate-pulse" />
                            {anomalies} {anomalies === 1 ? 'issue' : 'issues'}
                          </span>
                          : <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[10px] font-black border bg-emerald-500/10 border-emerald-500/20 text-emerald-400">
                            <CheckCircle className="w-3 h-3" /> Clean
                          </span>}
                      </div>
                    </button>

                    {/* Rule rows */}
                    {isOpen && (
                      <div className="border-t border-slate-800 divide-y divide-slate-800/60">
                        {colResults.map((result: any, idx: number) => {
                          const fixable = isRuleFixable(result);
                          const isFailed = result.status !== 'passed';
                          const cmdKey = `${colName}_${idx}`;

                          return (
                            <div key={result.rule_id || idx}
                              className={`px-5 py-4 transition-all duration-200 hover:bg-slate-800/30
                                border-l-2 ${isFailed ? 'border-l-red-500/40' : 'border-l-emerald-500/20'}`}>

                              <div className="flex items-start justify-between gap-4">
                                <div className="flex items-start gap-3 min-w-0">
                                  {/* Checkbox or icon */}
                                  {isFailed && fixable
                                    ? <input type="checkbox"
                                      className="mt-0.5 w-4 h-4 rounded border-slate-600 bg-slate-800 text-emerald-500 focus:ring-emerald-500 cursor-pointer shrink-0"
                                      checked={selectedFixes.has(result.rule_id)}
                                      onChange={() => toggle(selectedFixes, setSelectedFixes, result.rule_id)} />
                                    : <div className="mt-0.5 shrink-0">{statusIcon(result.status)}</div>}

                                  <div className="min-w-0">
                                    <div className="flex items-center gap-2 flex-wrap">
                                      <span className="font-mono text-sm font-bold text-slate-200 truncate">{result.rule_name}</span>
                                      {/* badges */}
                                      {fixable && isFailed && (
                                        <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md text-[9px] font-black bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                                          <Bot className="w-2.5 h-2.5" /> Auto-fix
                                        </span>
                                      )}
                                      {!fixable && isFailed && (
                                        <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md text-[9px] font-black bg-amber-500/10 text-amber-400 border border-amber-500/20">
                                          <AlertCircle className="w-2.5 h-2.5" /> Manual
                                        </span>
                                      )}
                                      {result.check_origin === 'llm_generated'
                                        ? <span className="inline-flex items-center px-1.5 py-0.5 rounded-md text-[9px] font-black bg-violet-500/10 text-violet-400 border border-violet-500/20">AI</span>
                                        : <span className="inline-flex items-center px-1.5 py-0.5 rounded-md text-[9px] font-black bg-slate-700 text-slate-400 border border-slate-600">Static</span>}
                                    </div>
                                    <div className="flex items-center gap-2 mt-1.5">
                                      {severityBadge(result.severity)}
                                      <span className="text-[10px] font-mono text-slate-600">{result.rule_type}</span>
                                    </div>
                                  </div>
                                </div>

                                {/* Right side */}
                                <div className="flex flex-col items-end gap-1.5 shrink-0">
                                  {statusBadge(result.status)}
                                  <div className="text-[10px] font-mono text-slate-600">
                                    {result.passed_count != null && <span className="text-emerald-500/70">{result.passed_count.toLocaleString()} pass</span>}
                                    {result.failed_count > 0 && <span className="text-red-500 ml-1.5">{result.failed_count.toLocaleString()} fail</span>}
                                  </div>
                                  {isFailed && !fixable && (
                                    <div className="flex items-center gap-1.5">
                                      <span className="text-[9px] font-black text-violet-400 opacity-70">Ticket</span>
                                      <input type="checkbox"
                                        className="w-3.5 h-3.5 rounded border-slate-600 bg-slate-800 text-violet-500 focus:ring-violet-500 cursor-pointer"
                                        checked={selectedTickets.has(result.rule_name)}
                                        onChange={() => toggle(selectedTickets, setSelectedTickets, result.rule_name)} />
                                    </div>
                                  )}
                                </div>
                              </div>

                              {/* Expand buttons */}
                              <div className="flex items-center gap-2 mt-3">
                                {result.executed_query && (
                                  <button onClick={() => toggle(expandedCommands, setExpandedCommands, cmdKey)}
                                    className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[10px] font-black uppercase tracking-widest border transition-all
                                      ${expandedCommands.has(cmdKey) ? 'bg-slate-100 text-black border-white' : 'bg-slate-800 border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-600'}`}>
                                    <Code className="w-3 h-3" /> SQL Query
                                  </button>
                                )}
                                {(result.agent_reasoning || result.agent_comprehension) && (
                                  <button onClick={() => toggle(expandedReasoning, setExpandedReasoning, cmdKey)}
                                    className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[10px] font-black uppercase tracking-widest border transition-all
                                      ${expandedReasoning.has(cmdKey) ? 'bg-violet-600 text-white border-violet-500' : 'bg-slate-800 border-violet-900/30 text-violet-400 hover:text-violet-200 hover:border-violet-700/50'}`}>
                                    <Brain className="w-3 h-3" /> AI Reasoning
                                  </button>
                                )}
                              </div>

                              {/* SQL block */}
                              {expandedCommands.has(cmdKey) && result.executed_query && (
                                <div className="mt-3 rounded-xl overflow-hidden border border-slate-700 bg-black/60">
                                  <div className="flex items-center justify-between px-4 py-2 border-b border-slate-800">
                                    <span className="text-[10px] font-mono font-bold text-slate-500 flex items-center gap-1.5">
                                      <Terminal className="w-3 h-3 text-emerald-500" /> SQL Execution Trace
                                    </span>
                                    <button onClick={() => navigator.clipboard.writeText(result.executed_query)}
                                      className="text-[10px] font-black text-emerald-500 hover:text-emerald-300 uppercase tracking-widest transition-colors">
                                      Copy
                                    </button>
                                  </div>
                                  <pre className="text-emerald-400 text-xs p-4 overflow-x-auto font-mono leading-relaxed">{result.executed_query}</pre>
                                </div>
                              )}

                              {/* Reasoning block */}
                              {expandedReasoning.has(cmdKey) && (
                                <div className="mt-3 rounded-xl border border-violet-500/20 bg-violet-900/10 overflow-hidden">
                                  {result.agent_reasoning && (
                                    <div className="px-4 py-3 border-b border-violet-500/15">
                                      <p className="text-[9px] font-black text-violet-400 uppercase tracking-widest mb-1.5 flex items-center gap-1.5">
                                        <Sparkles className="w-3 h-3" /> Rule Rationale
                                      </p>
                                      <p className="text-sm text-slate-300 leading-relaxed">{result.agent_reasoning}</p>
                                    </div>
                                  )}
                                  {result.agent_comprehension && (
                                    <div className="px-4 py-3">
                                      <p className="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5 flex items-center gap-1.5">
                                        <Bot className="w-3 h-3 text-emerald-500" /> Agent Feedback
                                      </p>
                                      <p className="text-sm text-slate-400 leading-relaxed whitespace-pre-line">{result.agent_comprehension}</p>
                                    </div>
                                  )}
                                </div>
                              )}

                              {/* AI Insights */}
                              {result.ai_insights && (
                                <div className="mt-3 flex items-start gap-3 p-3 rounded-xl bg-violet-500/8 border border-violet-500/15">
                                  <Brain className="w-4 h-4 text-violet-400 shrink-0 mt-0.5" />
                                  <p className="text-sm text-slate-300 italic leading-relaxed">"{result.ai_insights}"</p>
                                </div>
                              )}

                              {/* Failure examples */}
                              {result.failure_examples?.length > 0 && (
                                <div className="mt-3 rounded-lg border border-slate-800 bg-black/30 overflow-hidden">
                                  <div className="px-3 py-1.5 border-b border-slate-800">
                                    <span className="text-[9px] font-black text-slate-600 uppercase tracking-widest">Failure Examples</span>
                                  </div>
                                  <pre className="text-xs font-mono text-slate-400 p-3 overflow-x-auto leading-relaxed">
                                    {JSON.stringify(result.failure_examples, null, 2)}
                                  </pre>
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}

      {/* ── TAB: AGENT LOG ────────────────────────────────────── */}
      {activeTab === 'logs' && (
        <div className="rounded-2xl border border-slate-800 bg-slate-900 overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-800 flex items-center gap-2">
            <Terminal className="w-4 h-4 text-emerald-500" />
            <h2 className="text-sm font-bold text-slate-200 uppercase tracking-widest">Agent Execution Trace</h2>
          </div>
          <div className="p-5 overflow-y-auto max-h-[800px]">
            <ExecutionTraceViewer messages={validation?.result?.messages || []} />
          </div>
        </div>
      )}

      {/* ── CREATED TICKETS ──────────────────────────────────── */}
      {createdTickets.length > 0 && (
        <div className="space-y-4 pt-4">
          <h2 className="text-sm font-black text-slate-300 uppercase tracking-widest flex items-center gap-2">
            <Ticket className="w-4 h-4 text-violet-400" /> Active Tickets
            <span className="px-2 py-0.5 rounded-full bg-violet-500/10 border border-violet-500/20 text-violet-400 text-[10px]">
              {createdTickets.length}
            </span>
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {createdTickets.map(t => (
              <div key={t.id} className="relative rounded-xl border border-slate-800 bg-slate-900 p-5 hover:border-violet-500/30 transition-all group overflow-hidden">
                <div className="absolute inset-0 bg-gradient-to-br from-violet-500/3 to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
                <div className="flex justify-between items-start mb-3">
                  <span className="font-mono text-[10px] text-violet-400 font-black">#{t.id}</span>
                  <span className="px-2 py-0.5 rounded-full text-[10px] font-black bg-amber-500/10 border border-amber-500/20 text-amber-400 flex items-center gap-1">
                    <span className="w-1.5 h-1.5 rounded-full bg-amber-500 animate-pulse" />{t.status}
                  </span>
                </div>
                <div className="flex items-center gap-2 mb-2">
                  <User className="w-3.5 h-3.5 text-emerald-500" />
                  <span className="text-sm font-bold text-slate-200">{t.assigned_to}</span>
                </div>
                <p className="font-mono text-[10px] text-slate-500 line-clamp-2">{t.rule_names?.join(' · ')}</p>
                <div className="flex items-center justify-between mt-4 pt-3 border-t border-slate-800">
                  <span className="font-mono text-[9px] text-slate-700">{new Date(t.created_at).toLocaleString()}</span>
                  <button className="flex items-center gap-1 text-[10px] font-black text-emerald-500 hover:text-emerald-300 transition-colors uppercase tracking-widest">
                    View <ExternalLink className="w-3 h-3" />
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── TICKET MODAL ─────────────────────────────────────── */}
      {generatedTicket && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/85 backdrop-blur-md">
          <div className="relative w-full max-w-3xl rounded-2xl border border-slate-700 shadow-2xl overflow-hidden"
            style={{ background: 'linear-gradient(180deg,#0d1117 0%,#090e18 100%)' }}>
            {/* Header */}
            <div className="px-6 py-4 border-b border-slate-800 flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-lg bg-violet-500/15 border border-violet-500/25">
                  <Ticket className="w-5 h-5 text-violet-400" />
                </div>
                <div>
                  <h3 className="font-black text-slate-100 uppercase tracking-widest text-sm">Issue Ticket Draft</h3>
                  <p className="text-[10px] text-slate-500 font-mono mt-0.5">Generated by HALE AI Engine</p>
                </div>
              </div>
              <button onClick={() => setGeneratedTicket(null)} className="p-2 text-slate-500 hover:text-slate-200 hover:bg-slate-800 rounded-lg transition-all">
                <XCircle className="w-5 h-5" />
              </button>
            </div>
            {/* Body */}
            <div className="p-6 max-h-[55vh] overflow-y-auto">
              <div className="rounded-xl border border-slate-800 bg-black/40 overflow-hidden">
                <div className="flex items-center justify-between px-4 py-2.5 border-b border-slate-800">
                  <div className="flex items-center gap-1.5">
                    <div className="w-2.5 h-2.5 rounded-full bg-red-500/70" />
                    <div className="w-2.5 h-2.5 rounded-full bg-amber-500/70" />
                    <div className="w-2.5 h-2.5 rounded-full bg-emerald-500/70" />
                    <span className="ml-2 text-[9px] font-mono text-slate-600">ticket-draft.md</span>
                  </div>
                  <button onClick={() => navigator.clipboard.writeText(generatedTicket || '')}
                    className="text-[10px] font-black text-emerald-500 hover:text-emerald-300 uppercase tracking-widest transition-colors">
                    Copy
                  </button>
                </div>
                <pre className="text-sm font-mono text-emerald-300/90 p-5 whitespace-pre-wrap leading-relaxed overflow-x-auto">
                  {generatedTicket}
                </pre>
              </div>
            </div>
            {/* Footer */}
            <div className="px-6 py-4 border-t border-slate-800 flex flex-wrap items-center justify-between gap-4"
              style={{ background: 'rgba(0,0,0,0.3)' }}>
              <div className="flex items-center gap-3">
                <User className="w-4 h-4 text-slate-500" />
                <span className="text-[10px] font-black text-slate-500 uppercase tracking-widest">Assign to:</span>
                <select value={assignedUser} onChange={e => setAssignedUser(e.target.value)}
                  className="bg-slate-800 border border-slate-700 rounded-lg text-sm font-bold text-slate-200 px-3 py-1.5 focus:ring-emerald-500 focus:border-emerald-500 cursor-pointer">
                  {users.map(u => <option key={u.id} value={u.username}>{u.username} — {u.role}</option>)}
                </select>
              </div>
              <div className="flex items-center gap-3">
                <button onClick={() => setGeneratedTicket(null)}
                  className="px-4 py-2 rounded-lg bg-slate-800 border border-slate-700 text-slate-300 hover:text-slate-100 text-sm font-medium transition-all">
                  Cancel
                </button>
                <button onClick={handleDispatchTicket} disabled={!assignedUser}
                  className="px-5 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-black font-black text-sm uppercase tracking-widest transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-[0_0_20px_rgba(16,185,129,0.3)]">
                  Dispatch Ticket
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}