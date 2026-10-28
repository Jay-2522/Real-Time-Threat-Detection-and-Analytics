import { motion } from "framer-motion";
import { AlertTriangle, Crosshair, Globe2, Network, Radar, ShieldAlert } from "lucide-react";
import { Bar, BarChart, CartesianGrid, Cell, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { MetricCard } from "../components/dashboard/MetricCard";
import { ThreatGraphPanel } from "../components/threat/ThreatGraphPanel";
import { SeverityBadge } from "../components/ui/SeverityBadge";
import { ApiError, EmptyState } from "../components/ui/DataState";
import { useAsyncData } from "../hooks/useAsyncData";
import { threatIntelService } from "../services/threatIntelService";

export function ThreatIntelligencePage() {
  const overview = useAsyncData(threatIntelService.overview, [], 10_000);
  const iocs = useAsyncData(() => threatIntelService.iocs(25), [], 10_000);
  const entities = useAsyncData(() => threatIntelService.highRiskEntities(10), [], 10_000);
  const heatmap = useAsyncData(threatIntelService.heatmap, [], 10_000);
  const graph = useAsyncData(() => threatIntelService.graph(80), [], 10_000);
  const severity = Object.entries(overview.data?.severity_counts ?? {}).map(([label, value]) => ({ label, value }));

  return (
    <div className="space-y-6">
      <ApiError errors={[overview.error, iocs.error, entities.error, heatmap.error, graph.error]} />
      <section className="rounded-lg border border-cyan-400/20 bg-surface-900 p-6 shadow-panel">
        <p className="text-sm font-medium text-cyan-200">Local demonstration enrichment</p>
        <h1 className="mt-2 text-2xl font-semibold text-white">Threat Intelligence — Local Demonstration Data</h1>
        <p className="mt-2 text-sm text-slate-400">Counts are derived from live pipeline events. IOC feeds, country watchlists, and ATT&amp;CK mappings are local demonstration data, not external intelligence feeds.</p>
      </section>
      {overview.data ? <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricCard label="Enriched Events" value={overview.data?.total_enriched_events ?? 0} detail="Threat events processed." icon={Radar} />
        <MetricCard label="IOC Matches" value={overview.data?.ioc_matches ?? 0} detail="Local feed correlations." icon={Crosshair} tone="amber" />
        <MetricCard label="Rule Hits" value={overview.data?.rule_hits ?? 0} detail="Detection engineering matches." icon={ShieldAlert} tone="red" />
        <MetricCard label="High Risk Entities" value={overview.data?.high_risk_entities ?? 0} detail="Entities above risk 70." icon={AlertTriangle} tone="amber" />
        <MetricCard label="Timelines" value={overview.data?.active_timelines ?? 0} detail="Attack chains created." icon={Network} tone="emerald" />
      </section> : <EmptyState />}
      <section className="grid gap-4 xl:grid-cols-[1.2fr_.8fr]">
        <motion.article initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
          <h2 className="font-semibold text-white">Threat Relationship Graph</h2>
          <div className="mt-4"><ThreatGraphPanel graph={graph.data ?? { nodes: [], edges: [] }} /></div>
        </motion.article>
        <article className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
          <h2 className="font-semibold text-white">Threat Score Distribution</h2>
          <div className="mt-4 h-72">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={severity} dataKey="value" nameKey="label" outerRadius={96}>
                  {severity.map((entry, index) => <Cell key={entry.label} fill={["#22d3ee", "#34d399", "#f59e0b", "#ef4444", "#a855f7"][index % 5]} />)}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <h3 className="mt-4 text-sm font-semibold text-white">Top Targeted Entities</h3>
          <div className="mt-3 space-y-2">
            {(entities.data ?? []).map((entity) => (
              <div key={entity.entity_id} className="flex items-center justify-between rounded-md bg-white/5 px-3 py-2 text-sm">
                <span className="text-slate-200">{entity.entity_id}</span>
                <span className="text-cyan-200">{entity.risk_score.toFixed(1)}</span>
              </div>
            ))}
            {entities.data && !entities.data.length ? <EmptyState /> : null}
          </div>
        </article>
      </section>
      <section className="grid gap-4 xl:grid-cols-2">
        <article className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
          <h2 className="flex items-center gap-2 font-semibold text-white"><Globe2 size={18} /> Geographic Risk Heatmap</h2>
          <div className="mt-4 h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={heatmap.data ?? []}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,.15)" />
                <XAxis dataKey="country" stroke="#94a3b8" />
                <YAxis stroke="#94a3b8" />
                <Tooltip />
                <Bar dataKey="risk_score" fill="#22d3ee" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>
        <article className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
          <h2 className="font-semibold text-white">Auto-refreshing IOC activity</h2>
          <div className="mt-4 overflow-hidden rounded-md border border-white/10">
            {(iocs.data ?? []).map((ioc) => (
              <div key={`${ioc.id}-${ioc.ioc_value}`} className="grid grid-cols-[1fr_auto] gap-3 border-b border-white/10 px-4 py-3 last:border-b-0">
                <div>
                  <p className="font-mono text-sm text-white">{ioc.ioc_value}</p>
                  <p className="mt-1 text-xs text-slate-500">{ioc.feed_name} · {ioc.description}</p>
                </div>
                <SeverityBadge severity={ioc.severity} />
              </div>
            ))}
            {iocs.data && !iocs.data.length ? <EmptyState className="p-4" /> : null}
          </div>
        </article>
      </section>
    </div>
  );
}
