import { StatusEditor } from "./StatusEditor";
import { ChevronDown, Siren } from "lucide-react";
import { useState } from "react";

import type { AlertRecord, AlertStats, IncidentRecord } from "../../types/alerts";
import { alertService } from "../../services/alertService";
import { SeverityBadge } from "../ui/SeverityBadge";

export function CriticalAlertBanner({ alerts }: { alerts: AlertRecord[] }) {
  const critical = alerts.filter((alert) => alert.severity === "critical");
  if (!critical.length) return null;
  return (
    <section className="rounded-lg border border-red-400/30 bg-red-500/10 p-5 shadow-panel">
      <div className="flex items-start gap-4">
        <div className="grid h-11 w-11 place-items-center rounded-md bg-red-400/15 text-red-200">
          <Siren size={22} />
        </div>
        <div>
          <p className="text-sm font-semibold text-red-100">{critical.length} critical alert{critical.length === 1 ? "" : "s"} active</p>
          <p className="mt-1 text-sm text-red-100/75">{critical[0].title} for {critical[0].entity_id ?? "unknown entity"}</p>
        </div>
      </div>
    </section>
  );
}

export function AlertStatsStrip({ stats }: { stats: AlertStats }) {
  const items = [
    ["Total alerts", stats.total_alerts],
    ["Open alerts", stats.open_alerts],
    ["Critical", stats.critical_alerts],
    ["High", stats.high_alerts],
    ["Open incidents", stats.open_incidents],
  ];
  return (
    <section className="grid gap-4 md:grid-cols-5">
      {items.map(([label, value]) => (
        <article key={label} className="rounded-lg border border-white/10 bg-surface-900 p-4 shadow-panel">
          <p className="text-xs uppercase text-slate-500">{label}</p>
          <p className="mt-2 text-2xl font-semibold text-white">{value}</p>
        </article>
      ))}
    </section>
  );
}

export function AlertsTable({ alerts }: { alerts: AlertRecord[] }) {
  return (
    <section className="overflow-hidden rounded-lg border border-white/10 bg-surface-900 shadow-panel">
      <div className="border-b border-white/10 px-5 py-4">
        <h2 className="text-sm font-semibold text-white">Auto-refreshing alerts</h2>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[960px] text-left text-sm">
          <thead className="bg-white/[0.03] text-xs uppercase text-slate-500">
            <tr>
              <th className="px-5 py-3">Severity</th>
              <th className="px-5 py-3">Title</th>
              <th className="px-5 py-3">Entity</th>
              <th className="px-5 py-3">Risk</th>
              <th className="px-5 py-3">Status</th>
              <th className="px-5 py-3">Explanation</th>
              <th className="px-5 py-3">Details</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/10">
            {alerts.length ? alerts.map((alert) => <AlertRow key={alert.alert_id} alert={alert} />) : (
              <tr><td colSpan={7} className="px-5 py-10 text-center text-slate-500">No alerts generated yet.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function AlertRow({ alert }: { alert: AlertRecord }) {
  const [open, setOpen] = useState(false);
  const [incident, setIncident] = useState<IncidentRecord | null>(null);
  const [incidentState, setIncidentState] = useState<"idle" | "loading" | "unavailable">("idle");
  const detailId = `alert-detail-${alert.alert_id}`;
  const score = alert.ml_risk_score ?? alert.anomaly_score;
  const rule = alert.correlation_id?.split(":", 1)[0] || alert.title;

  async function toggleDetails() {
    const next = !open;
    setOpen(next);
    if (!next || !alert.incident_id || incident || incidentState === "loading") return;
    setIncidentState("loading");
    try {
      setIncident(await alertService.incident(alert.incident_id));
      setIncidentState("idle");
    } catch {
      setIncidentState("unavailable");
    }
  }

  return (
    <>
      <tr className="text-slate-300">
        <td className="px-5 py-3"><SeverityBadge severity={alert.severity} /></td>
        <td className="px-5 py-3 font-medium text-white">{alert.title}</td>
        <td className="px-5 py-3">{alert.entity_id ?? "unknown"}</td>
        <td className="px-5 py-3">{score === null || score === undefined ? "not provided" : score.toFixed(2)}</td>
        <td className="px-5 py-3 text-slate-400"><StatusEditor kind="alerts" id={alert.alert_id} status={alert.status} /></td>
        <td className="max-w-xl px-5 py-3 text-slate-400">{alert.explanation}</td>
        <td className="px-5 py-3">
          <button aria-controls={detailId} aria-expanded={open} aria-label={`${open ? "Hide" : "Show"} details for ${alert.title}`} className="grid h-8 w-8 place-items-center rounded-md bg-white/5 text-slate-300" onClick={() => void toggleDetails()} type="button">
            <ChevronDown className={open ? "rotate-180" : ""} size={16} />
          </button>
        </td>
      </tr>
      {open ? (
        <tr id={detailId}>
          <td colSpan={7} className="bg-black/20 px-5 py-4">
            <dl className="grid gap-4 text-sm md:grid-cols-2 xl:grid-cols-4">
              <AlertDetail label="Rule" value={rule} />
              <AlertDetail label="Entity" value={alert.entity_id ?? "unknown"} />
              <AlertDetail label="Rule severity" value={alert.severity} />
              <AlertDetail label="Score" value={score === null || score === undefined ? "not provided" : score.toFixed(2)} />
              <AlertDetail label="Explanation" value={alert.explanation || "not provided"} />
              <AlertDetail label="Status" value={alert.status} />
              <AlertDetail label="Created" value={formatAlertTime(alert.created_at ?? alert.timestamp)} />
              <AlertDetail label="Linked incident" value={linkedIncidentText(alert, incident, incidentState)} />
            </dl>
          </td>
        </tr>
      ) : null}
    </>
  );
}

function AlertDetail({ label, value }: { label: string; value: string }) {
  return <div><dt className="text-xs uppercase text-slate-500">{label}</dt><dd className="mt-1 break-words text-slate-200">{value}</dd></div>;
}

function linkedIncidentText(alert: AlertRecord, incident: IncidentRecord | null, state: string) {
  if (!alert.incident_id) return "none";
  if (incident) return `${incident.incident_id} · ${incident.status}`;
  return `${alert.incident_id} · ${state === "unavailable" ? "status unavailable" : "loading status"}`;
}

function formatAlertTime(value: string) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

export function IncidentCards({ incidents }: { incidents: IncidentRecord[] }) {
  return (
    <section className="grid gap-4 xl:grid-cols-2">
      {incidents.length ? incidents.map((incident) => (
        <article key={incident.incident_id} className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="text-xs uppercase text-slate-500">{incident.incident_id}</p>
              <h2 className="mt-2 text-lg font-semibold text-white">{incident.title}</h2>
              <p className="mt-2 text-sm leading-6 text-slate-400">{incident.description}</p>
            </div>
            <SeverityBadge severity={incident.severity} />
          </div>
          <div className="mt-5 grid gap-3 md:grid-cols-3">
            <MiniStat label="Events" value={incident.event_count} />
            <MiniStat label="Escalation" value={incident.escalation_level} />
            <StatusEditor kind="incidents" id={incident.incident_id} status={incident.status} />
          </div>
          <div className="mt-5 border-t border-white/10 pt-4">
            <p className="text-xs uppercase text-slate-500">Affected entities</p>
            <p className="mt-2 text-sm text-slate-300">{incident.entity_ids.join(", ") || "unknown"}</p>
          </div>
        </article>
      )) : (
        <article className="rounded-lg border border-white/10 bg-surface-900 p-8 text-center text-slate-500 shadow-panel">No incidents correlated yet.</article>
      )}
    </section>
  );
}

function MiniStat({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-md bg-white/[0.03] p-3">
      <p className="text-xs uppercase text-slate-500">{label}</p>
      <p className="mt-1 font-semibold text-white">{value}</p>
    </div>
  );
}

