import { ReactNode } from 'react';
import { Link, useLocation } from 'react-router-dom';
import {
  LayoutDashboard,
  Database,
  ClipboardCheck,
  ListChecks,
  Settings,
  Menu,
  X,
  Search,
  Bell,
  ChevronRight,
  User
} from 'lucide-react';
import { useState, useMemo } from 'react';
import { useLLMHealth } from '@/hooks/useSystem';
import LogoImage from '@/components/793abe9dc7835161df534163b32ce4bb.png';

interface LayoutProps {
  children: ReactNode;
}

const navigation = [
  { name: 'Dashboard', href: '/', icon: LayoutDashboard },
  { name: 'Data Sources', href: '/datasources', icon: Database },
  { name: 'Validations', href: '/validations', icon: ClipboardCheck },
  { name: 'Rules', href: '/rules', icon: ListChecks },
];

export default function Layout({ children }: LayoutProps) {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const location = useLocation();
  useLLMHealth();

  const currentPage = useMemo(() => {
    const item = [...navigation, { name: 'Settings', href: '/settings' }].find(i => i.href === location.pathname);
    return item?.name || 'Overview';
  }, [location.pathname]);

  return (
    <div className="flex h-screen overflow-hidden bg-black text-slate-100 font-display">
      {/* Mobile sidebar overlay */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black/80 z-40 lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Mobile sidebar */}
      <div
        className={`fixed inset-y-0 left-0 z-50 w-64 bg-emerald-dark border-r border-primary/10 flex flex-col transform transition-transform duration-200 ease-in-out lg:hidden ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'
          }`}
      >
        <div className="p-6 flex items-center justify-between">
          <div className="flex flex-col items-center gap-1">
            <img
              src={LogoImage}
              alt="QC Agent Logo"
              className="w-12 h-auto filter invert hue-rotate-180 drop-shadow-[0_0_8px_rgba(16,185,129,0.8)]"
            />
            <div className="text-[10px] text-primary font-black tracking-widest uppercase filter drop-shadow-[0_0_5px_rgba(16,185,129,0.4)]">QC AGENT</div>
          </div>
          <button onClick={() => setSidebarOpen(false)} className="text-slate-400 hover:text-white">
            <X className="w-5 h-5" />
          </button>
        </div>
        <nav className="flex-1 px-4 py-4 space-y-2 overflow-y-auto">
          {navigation.map((item) => {
            const isActive = location.pathname === item.href;
            return (
              <Link
                key={item.name}
                to={item.href}
                onClick={() => setSidebarOpen(false)}
                className={`flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${isActive
                  ? 'bg-primary text-black font-semibold'
                  : 'text-slate-300 hover:bg-primary/10 hover:text-primary'
                  } group`}
              >
                <item.icon className="w-5 h-5" />
                <span>{item.name}</span>
              </Link>
            );
          })}
        </nav>
        <div className="p-4 border-t border-primary/10">
          <Link
            to="/settings"
            onClick={() => setSidebarOpen(false)}
            className={`flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${location.pathname === '/settings'
              ? 'bg-primary text-black font-semibold'
              : 'text-slate-400 hover:bg-primary/10 hover:text-primary'
              }`}
          >
            <Settings className="w-5 h-5" />
            <span>Settings</span>
          </Link>
        </div>
      </div>

      {/* Desktop Sidebar */}
      <aside className="hidden lg:flex w-64 flex-shrink-0 bg-emerald-dark border-r border-primary/20 flex-col">
        <div className="flex flex-col items-center justify-center w-full py-4 space-y-2">
          <img
            src={LogoImage}
            alt="QC Agent Logo"
            className="w-24 h-auto filter invert hue-rotate-180 drop-shadow-[0_0_15px_rgba(16,185,129,0.9)]"
          />
          <div className="text-[10px] text-primary font-black tracking-[0.4em] uppercase filter drop-shadow-[0_0_8px_rgba(16,185,129,0.6)]">QC AGENT</div>
        </div>
        <nav className="flex-1 px-4 py-4 space-y-2">
          {navigation.map((item) => {
            const isActive = location.pathname === item.href;
            return (
              <Link
                key={item.name}
                to={item.href}
                className={`flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${isActive
                  ? 'bg-primary text-black font-semibold'
                  : 'text-slate-200 hover:bg-primary/10 hover:text-primary'
                  } group`}
              >
                <item.icon className="w-5 h-5" />
                <span>{item.name}</span>
              </Link>
            );
          })}
        </nav>
        <div className="p-4 border-t border-primary/20">
          <Link
            to="/settings"
            className={`flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${location.pathname === '/settings'
              ? 'bg-primary text-black font-semibold'
              : 'text-slate-200 hover:bg-primary/10 hover:text-primary'
              }`}
          >
            <Settings className="w-5 h-5" />
            <span>Settings</span>
          </Link>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-y-auto bg-black flex flex-col relative w-full h-full">
        {/* Header */}
        <header className="h-16 border-b border-primary/20 flex items-center justify-between px-4 sm:px-8 bg-black/80 backdrop-blur-md sticky top-0 z-10">
          <div className="flex items-center gap-2">
            <button
              onClick={() => setSidebarOpen(true)}
              className="mr-3 lg:hidden text-slate-300 hover:text-primary"
            >
              <Menu className="w-6 h-6" />
            </button>
            <span className="text-slate-400 hidden sm:inline">Overview</span>
            <ChevronRight className="text-slate-600 w-4 h-4 hidden sm:inline" />
            <span className="text-slate-100 font-semibold">{currentPage}</span>
          </div>
          <div className="flex items-center gap-4">
            <div className="relative hidden md:block">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500 w-4 h-4" />
              <input
                className="bg-emerald-dark border border-primary/20 rounded-lg pl-10 pr-4 py-1.5 text-sm focus:ring-1 focus:ring-primary focus:border-primary outline-none text-slate-100 w-64 placeholder:text-slate-400"
                placeholder="Search validations..."
                type="text"
              />
            </div>
            <button className="w-9 h-9 flex items-center justify-center rounded-lg bg-emerald-dark border border-primary/20 text-slate-300 hover:text-primary hover:border-primary transition-all">
              <Bell className="w-4 h-4" />
            </button>
            <div className="w-9 h-9 rounded-full bg-primary/20 border border-primary/40 flex items-center justify-center text-primary">
              <User className="w-5 h-5" />
            </div>
          </div>
        </header>

        {/* Page content */}
        <div className="p-4 sm:p-8 w-full max-w-[1600px] mx-auto">{children}</div>
      </main>
    </div>
  );
}