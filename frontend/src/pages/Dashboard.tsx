import { Link } from 'react-router-dom';
import {
  Database,
  ClipboardCheck,
  ListChecks,
  TrendingUp,
  Activity,
  FileText,
  Server,
  Cpu,
  HardDrive
} from 'lucide-react';
import { useSystemHealth, useLLMHealth } from '@/hooks/useSystem';
import { useDataSources } from '@/hooks/useDataSources';

// Mock data for dashboard - would come from API in production
const mockStats = {
  totalValidations: 156,
  totalDataSources: 12,
  totalRules: 89,
  averageQualityScore: 87.5,
};

const mockRecentValidations = [
  {
    id: '1',
    target_path: 'customers.csv',
    status: 'completed',
    quality_score: 92,
    total_rules: 15,
    passed_rules: 14,
    failed_rules: 1,
    completed_at: '2024-02-11T10:30:00Z',
  },
  {
    id: '2',
    target_path: 'orders.parquet',
    status: 'completed',
    quality_score: 78,
    total_rules: 20,
    passed_rules: 16,
    failed_rules: 4,
    completed_at: '2024-02-11T09:15:00Z',
  },
  {
    id: '3',
    target_path: 'products.json',
    status: 'failed',
    quality_score: 45,
    total_rules: 12,
    passed_rules: 5,
    failed_rules: 7,
    completed_at: '2024-02-11T08:00:00Z',
  },
  {
    id: '4',
    target_path: 'inventory.xlsx',
    status: 'running',
    quality_score: null,
    total_rules: 18,
    passed_rules: 0,
    failed_rules: 0,
    completed_at: null,
  },
];

export default function Dashboard() {
  const { data: systemHealth } = useSystemHealth();
  const { data: llmHealth } = useLLMHealth();
  const { data: dataSources } = useDataSources();

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'completed':
        return <span className="px-2 py-1 rounded text-[10px] font-black bg-primary/20 text-primary border border-primary/30 uppercase tracking-widest">Success</span>;
      case 'failed':
        return <span className="px-2 py-1 rounded text-[10px] font-black bg-red-500/20 text-red-500 border border-red-500/30 uppercase tracking-widest">Failed</span>;
      case 'running':
        return <span className="px-2 py-1 rounded text-[10px] font-black bg-slate-700/50 text-slate-300 border border-slate-600 uppercase tracking-widest">Running</span>;
      default:
        return <span className="px-2 py-1 rounded text-[10px] font-black bg-slate-700/50 text-slate-300 border border-slate-600 uppercase tracking-widest">{status}</span>;
    }
  };

  const getScoreColorBg = (score: number | null) => {
    if (score === null) return 'bg-primary/20 animate-pulse';
    if (score >= 90) return 'bg-primary';
    if (score >= 70) return 'bg-primary/70';
    return 'bg-red-500';
  };

  return (
    <div className="space-y-8">
      <div className="flex flex-col md:flex-row md:items-end justify-between gap-6 mb-2">
        <div className="flex flex-col gap-2">
          <h1 className="text-slate-100 text-3xl md:text-4xl font-black leading-tight tracking-tighter">Dashboard</h1>
          <p className="text-slate-400 text-sm md:text-base max-w-2xl">Overview of your data quality metrics and recent activity.</p>
        </div>
      </div>

      {/* Metrics Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="bg-surface-dark border border-primary/20 p-6 rounded-xl relative overflow-hidden group hover:border-primary transition-all shadow-sm">
          <div className="absolute -right-2 -top-2 opacity-5 group-hover:opacity-10 transition-opacity">
            <ClipboardCheck className="w-24 h-24 text-primary" strokeWidth={1.5} />
          </div>
          <p className="text-slate-200 text-[10px] font-black tracking-widest uppercase relative z-10">TOTAL VALIDATIONS</p>
          <div className="flex items-end gap-3 mt-2 relative z-10">
            <h3 className="text-3xl font-black text-slate-100">{mockStats.totalValidations}</h3>
            <span className="text-primary text-[10px] font-black mb-1 flex items-center bg-primary/10 px-2 py-0.5 rounded uppercase tracking-widest">
              <TrendingUp className="w-3 h-3 mr-1" /> 12%
            </span>
          </div>
        </div>

        <div className="bg-surface-dark border border-primary/20 p-6 rounded-xl relative overflow-hidden group hover:border-primary transition-all shadow-sm">
          <div className="absolute -right-2 -top-2 opacity-5 group-hover:opacity-10 transition-opacity">
            <Database className="w-24 h-24 text-primary" strokeWidth={1.5} />
          </div>
          <p className="text-slate-200 text-[10px] font-black tracking-widest uppercase relative z-10">TOTAL DATA SOURCES</p>
          <div className="flex items-end gap-3 mt-2 relative z-10">
            <h3 className="text-3xl font-black text-slate-100">
              {dataSources?.length || mockStats.totalDataSources}
            </h3>
            <span className="text-slate-400 text-[10px] font-black mb-1 bg-slate-800 px-2 py-0.5 rounded uppercase tracking-widest">0%</span>
          </div>
        </div>

        <div className="bg-surface-dark border border-primary/20 p-6 rounded-xl relative overflow-hidden group hover:border-primary transition-all shadow-sm">
          <div className="absolute -right-2 -top-2 opacity-5 group-hover:opacity-10 transition-opacity">
            <ListChecks className="w-24 h-24 text-primary" strokeWidth={1.5} />
          </div>
          <p className="text-slate-200 text-[10px] font-black tracking-widest uppercase relative z-10">TOTAL RECORDS</p>
          <div className="flex items-end gap-3 mt-2 relative z-10">
            <h3 className="text-3xl font-black text-slate-100">{mockStats.totalRules}</h3>
            <span className="text-primary text-[10px] font-black mb-1 flex items-center bg-primary/10 px-2 py-0.5 rounded uppercase tracking-widest">
              <TrendingUp className="w-3 h-3 mr-1" /> 5%
            </span>
          </div>
        </div>

        <div className="bg-surface-dark border border-primary/20 p-6 rounded-xl relative overflow-hidden group hover:border-primary transition-all shadow-sm">
          <div className="absolute -right-2 -top-2 opacity-5 group-hover:opacity-10 transition-opacity">
            <Activity className="w-24 h-24 text-primary" strokeWidth={1.5} />
          </div>
          <p className="text-slate-200 text-[10px] font-black tracking-widest uppercase relative z-10">HEALTH SCORE</p>
          <div className="flex items-end gap-3 mt-2 relative z-10">
            <h3 className="text-3xl font-black text-slate-100">{mockStats.averageQualityScore}%</h3>
            <span className="text-primary text-[10px] font-black mb-1 flex items-center bg-primary/10 px-2 py-0.5 rounded uppercase tracking-widest">
              <TrendingUp className="w-3 h-3 mr-1" /> 2.1%
            </span>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Recent Validations Table */}
        <div className="lg:col-span-2 bg-surface-dark border border-primary/10 rounded-xl overflow-hidden flex flex-col shadow-sm">
          <div className="px-6 py-5 border-b border-primary/10 flex items-center justify-between">
            <h2 className="text-lg font-black text-slate-100 tracking-wide">RECENT VALIDATIONS</h2>
            <Link to="/validations" className="text-primary text-xs font-bold uppercase tracking-widest hover:underline">
              View All
            </Link>
          </div>
          <div className="flex-1 overflow-x-auto">
            <table className="w-full text-left">
              <thead className="bg-black/40 text-slate-200 text-[10px] uppercase font-black tracking-[0.2em] border-b border-primary/20">
                <tr>
                  <th className="px-6 py-4">Target Asset</th>
                  <th className="px-6 py-4">Success Rate</th>
                  <th className="px-6 py-4">Status</th>
                  <th className="px-6 py-4">Last Checked</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-primary/10">
                {mockRecentValidations.map((validation) => (
                  <tr key={validation.id} className="hover:bg-primary/5 transition-colors group">
                    <td className="px-6 py-4 flex items-center gap-3">
                      <FileText className="w-5 h-5 text-slate-500 group-hover:text-primary transition-colors" />
                      <Link
                        to={`/validations/${validation.id}`}
                        className="text-slate-200 font-bold hover:text-primary transition-colors"
                      >
                        {validation.target_path}
                      </Link>
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex items-center gap-3">
                        <div className="w-24 h-1.5 bg-slate-800 rounded-full overflow-hidden relative">
                          <div
                            className={`h-full ${getScoreColorBg(validation.quality_score)}`}
                            style={validation.quality_score ? { width: `${validation.quality_score}%` } : { width: '100%' }}
                          />
                        </div>
                        <span className="text-xs font-bold text-slate-300">
                          {validation.quality_score !== null ? `${validation.quality_score}%` : '---'}
                        </span>
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      {getStatusBadge(validation.status)}
                    </td>
                    <td className="px-6 py-4 text-xs font-bold text-slate-400 tracking-wider">
                      {validation.completed_at
                        ? new Date(validation.completed_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                        : 'Active'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* System Status Sidebar */}
        <div className="space-y-6">
          <div className="bg-surface-dark border border-primary/10 p-6 rounded-xl shadow-sm">
            <h2 className="text-lg font-black text-slate-100 mb-6 tracking-wide uppercase">System Status</h2>
            <div className="space-y-4">
              <div className="p-4 bg-black/40 border border-primary/5 rounded-lg flex items-center gap-4">
                <div className="size-10 rounded-lg bg-primary/10 flex items-center justify-center text-primary royal-glow">
                  <Server className="w-5 h-5" />
                </div>
                <div className="flex-1">
                  <div className="flex items-center justify-between">
                    <h4 className="text-slate-200 font-bold text-sm">API Server</h4>
                    <span className={`size-2 rounded-full ${systemHealth?.status === 'healthy' ? 'bg-primary shadow-[0_0_8px_rgba(16,183,127,0.6)]' : 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.6)]'}`}></span>
                  </div>
                  <p className="text-[10px] text-slate-400 font-black uppercase tracking-widest mt-1">
                    {systemHealth?.status === 'healthy' ? 'Operational • 99.9% Uptime' : 'Issues Detected'}
                  </p>
                </div>
              </div>

              <div className="p-4 bg-black/40 border border-primary/5 rounded-lg flex items-center gap-4">
                <div className="size-10 rounded-lg bg-primary/10 flex items-center justify-center text-primary royal-glow">
                  <Cpu className="w-5 h-5" />
                </div>
                <div className="flex-1">
                  <div className="flex items-center justify-between">
                    <h4 className="text-slate-200 font-bold text-sm">LLM Service</h4>
                    <span className={`size-2 rounded-full ${llmHealth?.status === 'healthy' ? 'bg-primary shadow-[0_0_8px_rgba(16,183,127,0.6)]' : 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.6)]'}`}></span>
                  </div>
                  <p className="text-[10px] text-slate-400 font-black uppercase tracking-widest mt-1">
                    {llmHealth?.status === 'healthy' ? `Operational • ${llmHealth.provider}` : 'Degraded'}
                  </p>
                </div>
              </div>

              <div className="p-4 bg-black/40 border border-primary/5 rounded-lg flex items-center gap-4">
                <div className="size-10 rounded-lg bg-primary/10 flex items-center justify-center text-primary royal-glow">
                  <HardDrive className="w-5 h-5" />
                </div>
                <div className="flex-1">
                  <div className="flex items-center justify-between">
                    <h4 className="text-slate-200 font-bold text-sm">Database</h4>
                    <span className="size-2 bg-primary rounded-full shadow-[0_0_8px_rgba(16,183,127,0.6)]"></span>
                  </div>
                  <p className="text-xs text-slate-500 font-medium mt-1">Operational • Low latency</p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
