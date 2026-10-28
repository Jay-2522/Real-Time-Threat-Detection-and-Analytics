import { IncidentCards } from "../components/alerts/AlertPanels";
import { ApiError } from "../components/ui/DataState";
import { useAlertsWebSocket } from "../hooks/useAlertsWebSocket";
import { useAsyncData } from "../hooks/useAsyncData";
import { alertService } from "../services/alertService";

export function IncidentsPage() {
  const live = useAlertsWebSocket();
  const incidents = useAsyncData(() => alertService.recentIncidents(50), [], 8_000);
  const open = useAsyncData(() => alertService.openIncidents(50), [], 8_000);
  const incidentRows = live.message?.recent_incidents.length ? live.message.recent_incidents : incidents.data ?? [];

  return (
    <div className="space-y-6">
      <ApiError errors={[incidents.error, open.error]} />
      <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
        <p className="text-sm font-medium text-cyan-200">Incident Intelligence · {open.data ? `${open.data.length} open` : "loading"}</p>
        <h1 className="mt-2 text-2xl font-semibold text-white">Correlated Incidents</h1>
        <p className="mt-2 text-sm text-slate-400">Grouped alert timelines with severity escalation, affected entities, and operational status.</p>
      </section>
      <IncidentCards incidents={incidentRows} />
    </div>
  );
}

