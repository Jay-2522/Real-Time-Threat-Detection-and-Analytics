import { Activity, AlertTriangle, BarChart3, DatabaseZap, Gauge, LineChart, RadioTower, Server } from "lucide-react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { MetricCard } from "../components/dashboard/MetricCard";
import { ApiError, EmptyState } from "../components/ui/DataState";
import { useAsyncData } from "../hooks/useAsyncData";
import { monitoringService } from "../services/monitoringService";

const statusTone: Record<string, string> = {
  healthy: "border-emerald-400/25 bg-emerald-400/10 text-emerald-200",
  observing: "border-cyan-400/25 bg-cyan-400/10 text-cyan-200",
  degraded: "border-amber-400/25 bg-amber-400/10 text-amber-200",
  down: "border-red-400/25 bg-red-400/10 text-red-200",
};

export function ObservabilityPage() {
  const overview = useAsyncData(monitoringService.overview, [], 10_000);
  const services = useAsyncData(monitoringService.services, [], 10_000);
  const pipeline = useAsyncData(monitoringService.pipeline, [], 10_000);
  const errors = useAsyncData(monitoringService.errors, [], 10_000);
  const metrics = useAsyncData(monitoringService.metricsSummary, [], 10_000);

  const metricRows = Object.entries(metrics.data?.key_metrics ?? {}).map(([label, value]) => ({
    label: label.replace(/_/g, " "),
    value: Number(value.toFixed(2)),
  }));

  return (
    <div className="space-y-6">
      <ApiError errors={[overview.error, services.error, pipeline.error, errors.error, metrics.error]} />
      <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <p className="text-sm font-medium text-cyan-200">Local Prometheus + optional Grafana</p>
            <h1 className="mt-2 text-2xl font-semibold text-white">Observability</h1>
            <p className="mt-2 max-w-3xl text-sm text-slate-400">Production-style metrics for the free local streaming analytics stack, including API latency, pipeline throughput, ML inference, storage persistence, and SOC alerting.</p>
          </div>
          <div className="flex flex-wrap gap-3">
            <a className="rounded-md border border-cyan-400/30 bg-cyan-400/10 px-4 py-2 text-sm text-cyan-100" href="http://localhost:9090" target="_blank" rel="noreferrer">
              Prometheus
            </a>
            <a className="rounded-md border border-emerald-400/30 bg-emerald-400/10 px-4 py-2 text-sm text-emerald-100" href="http://localhost:3000" target="_blank" rel="noreferrer">
              Grafana (optional)
            </a>
          </div>
        </div>
      </section>

      {overview.data ? <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Healthy Services" value={`${overview.data?.services_healthy ?? 0}/${overview.data?.services_total ?? 0}`} detail="Prometheus targets and local dependency checks." icon={Server} tone="emerald" />
        <MetricCard label="Pipeline Events" value={overview.data?.total_events ?? 0} detail="Persisted telemetry event records." icon={RadioTower} tone="cyan" />
        <MetricCard label="Anomaly scores" value={overview.data?.total_predictions ?? 0} detail="Stored anomaly-score records." icon={Activity} tone="amber" />
        <MetricCard label="Open Incidents" value={overview.data?.open_incidents ?? 0} detail="SOC incidents still open or investigating." icon={AlertTriangle} tone={(overview.data?.open_incidents ?? 0) > 0 ? "red" : "emerald"} />
      </section> : <EmptyState />}

      <section className="grid gap-4 xl:grid-cols-[1.4fr_1fr]">
        <article className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
          <div className="flex items-center gap-3">
            <div className="grid h-10 w-10 place-items-center rounded-md bg-cyan-400/12 text-cyan-200">
              <Gauge size={18} />
            </div>
            <div>
              <h2 className="font-semibold text-white">Service Health</h2>
              <p className="text-sm text-slate-400">Scrape targets and local platform dependencies.</p>
            </div>
          </div>
          <div className="mt-5 grid gap-3 md:grid-cols-2">
            {(services.data ?? []).map((service) => (
              <div key={service.name} className="rounded-md border border-white/10 bg-white/[0.03] p-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="font-medium text-white">{service.name}</p>
                    <p className="mt-1 text-xs uppercase text-slate-500">{service.category}</p>
                  </div>
                  <span className={`rounded-full border px-2.5 py-1 text-xs ${statusTone[service.status] ?? statusTone.degraded}`}>{service.status}</span>
                </div>
                <p className="mt-3 text-sm text-slate-400">{service.detail}</p>
                {service.endpoint ? <p className="mt-3 truncate text-xs text-slate-500">{service.endpoint}</p> : null}
              </div>
            ))}
            {!services.data?.length ? <p className="text-sm text-slate-400">Waiting for monitoring service data.</p> : null}
          </div>
        </article>

        <article className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
          <div className="flex items-center gap-3">
            <div className="grid h-10 w-10 place-items-center rounded-md bg-emerald-400/12 text-emerald-200">
              <BarChart3 size={18} />
            </div>
            <div>
              <h2 className="font-semibold text-white">Metrics Summary</h2>
              <p className="text-sm text-slate-400">Prometheus key counters.</p>
            </div>
          </div>
          <div className="mt-5 h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={metricRows} layout="vertical" margin={{ left: 12, right: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,.15)" />
                <XAxis type="number" stroke="#64748b" />
                <YAxis type="category" dataKey="label" width={132} stroke="#94a3b8" tick={{ fontSize: 11 }} />
                <Tooltip contentStyle={{ background: "#111827", border: "1px solid rgba(255,255,255,.1)", color: "#e5e7eb" }} />
                <Bar dataKey="value" fill="#22d3ee" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
          <p className="mt-3 text-xs text-slate-500">Prometheus: {metrics.data?.prometheus_available ? "available" : "degraded"}</p>
        </article>
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Analytics Metrics" value={pipeline.data?.analytics_metrics ?? 0} detail="Spark analytics records persisted." icon={LineChart} tone="cyan" />
        <MetricCard label="Feature Snapshots" value={pipeline.data?.feature_snapshots ?? 0} detail="ML feature engineering snapshots." icon={DatabaseZap} tone="emerald" />
        <MetricCard label="Dead Letters" value={errors.data?.deadletter_events ?? 0} detail="Malformed or unprocessable records." icon={AlertTriangle} tone={(errors.data?.deadletter_events ?? 0) > 0 ? "amber" : "emerald"} />
        <MetricCard label="Critical Alerts" value={errors.data?.critical_alerts ?? 0} detail="Critical SOC alerts generated." icon={AlertTriangle} tone={(errors.data?.critical_alerts ?? 0) > 0 ? "red" : "emerald"} />
      </section>
    </div>
  );
}
