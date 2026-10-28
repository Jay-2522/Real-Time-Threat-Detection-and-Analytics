import { useSession } from "../auth/session";
import { NavLink, Outlet } from "react-router-dom";
import { Activity, BarChart3, BrainCircuit, Crosshair, FolderKanban, Gauge, GitBranch, LayoutDashboard, Network, Radio, Search, Server, ShieldCheck, Siren, Users } from "lucide-react";
import { config } from "../config/environment";

const defaultNavigationItems = [
  { label: "Overview", href: "/", icon: LayoutDashboard },
  { label: "Live Stream", href: "/live", icon: Radio },
  { label: "ML Predictions", href: "/predictions", icon: BrainCircuit },
  { label: "Alerts", href: "/alerts", icon: Siren },
  { label: "Incidents", href: "/incidents", icon: FolderKanban },
  { label: "Detection Engineering", href: "/detection-engineering", icon: ShieldCheck },
];

const featureNavigationItems = [
  { label: "Risk Analytics", href: "/risk", icon: BarChart3, enabled: config.features.riskAnalytics },
  { label: "Threat Intelligence", href: "/threat-intelligence", icon: Crosshair, enabled: config.features.threatIntelligence },
  { label: "Threat Hunting", href: "/threat-hunting", icon: GitBranch, enabled: config.features.threatHunting },
  { label: "MITRE ATT&CK", href: "/mitre-attack", icon: Network, enabled: config.features.mitreAttack },
  { label: "Entities", href: "/entities", icon: Users, enabled: config.features.entities },
  { label: "Attack Timeline", href: "/attack-timeline", icon: Activity, enabled: config.features.attackTimeline },
  { label: "Search", href: "/search", icon: Search, enabled: config.features.search },
  { label: "System Health", href: "/health", icon: Server, enabled: config.features.systemHealth },
  { label: "Observability", href: "/observability", icon: Gauge, enabled: config.features.observability },
];

const navigationItems = [...defaultNavigationItems, ...featureNavigationItems.filter((item) => item.enabled)];

export function DashboardLayout() {
  const { user, logout } = useSession();
  return (
    <div className="min-h-screen bg-surface-950 text-slate-100">
      <aside className="fixed inset-y-0 left-0 z-30 hidden w-72 border-r border-white/10 bg-surface-900/95 px-5 py-6 lg:block">
        <div className="flex items-center gap-3">
          <div className="grid h-10 w-10 place-items-center rounded-md bg-cyan-400/15 text-cyan-300">
            <Activity size={22} />
          </div>
          <div>
            <p className="text-sm font-semibold text-cyan-200">Streaming Security Analytics (local demo)</p>
          </div>
        </div>

        <nav className="mt-10 space-y-1">
          {navigationItems.map((item) => (
            <NavLink
              key={item.href}
              to={item.href}
              end={item.href === "/"}
              className={({ isActive }) =>
                `flex w-full items-center gap-3 rounded-md px-3 py-2.5 text-left text-sm transition ${
                  isActive ? "bg-cyan-400/12 text-cyan-100" : "text-slate-400 hover:bg-white/5 hover:text-slate-100"
                }`
              }
            >
              <item.icon size={18} />
              <span>{item.label}</span>
            </NavLink>
          ))}
        </nav>
      </aside>

      <div className="lg:pl-72">
        <header className="sticky top-0 z-20 border-b border-white/10 bg-surface-950/88 px-4 py-4 backdrop-blur md:px-8">
          <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
            <div>
              <h1 className="text-xl font-semibold text-white">Streaming Security Analytics (local demo)</h1>
            </div>
            <div className="flex items-center gap-3">
              <span>{user.username} · {user.role}</span>
              <button type="button" onClick={() => void logout()}>Sign out</button>
            </div>
          </div>
          <nav className="mt-4 flex gap-2 overflow-x-auto lg:hidden">
            {navigationItems.map((item) => (
              <NavLink key={item.href} to={item.href} end={item.href === "/"} className={({ isActive }) => `whitespace-nowrap rounded-md px-3 py-2 text-sm ${isActive ? "bg-cyan-400/12 text-cyan-100" : "bg-white/5 text-slate-400"}`}>
                {item.label}
              </NavLink>
            ))}
          </nav>
        </header>

        <main className="px-4 py-6 md:px-8">
          <div className="mb-4 inline-flex rounded-full border border-amber-400/30 bg-amber-400/10 px-3 py-1 text-xs font-medium text-amber-100">
            Event source: {config.eventSourceLabel}
          </div>
          <Outlet />
        </main>
      </div>
    </div>
  );
}
