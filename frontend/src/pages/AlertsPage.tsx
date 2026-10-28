import { AlertStatsStrip, AlertsTable, CriticalAlertBanner } from "../components/alerts/AlertPanels";
import { ApiError } from "../components/ui/DataState";
import { useAlertsWebSocket } from "../hooks/useAlertsWebSocket";
import { useAsyncData } from "../hooks/useAsyncData";
import { alertService } from "../services/alertService";

export function AlertsPage() {
  const live = useAlertsWebSocket();
  const alerts = useAsyncData(() => alertService.recentAlerts(50), [], 5_000);
  const stats = useAsyncData(alertService.stats, [], 5_000);
  const rows = live.message?.recent_alerts.length ? live.message.recent_alerts : alerts.data ?? [];
  const liveStats = live.message?.stats ?? stats.data;

  return (
    <div className="space-y-6">
      <ApiError errors={[alerts.error, stats.error]} />
      <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
        <p className="text-sm font-medium text-cyan-200">SOC Alert Engine · {live.status}</p>
        <h1 className="mt-2 text-2xl font-semibold text-white">Real-Time Alerts</h1>
        <p className="mt-2 text-sm text-slate-400">Deduplicated, correlated security alerts generated from ML predictions and streaming analytics.</p>
      </section>
      <CriticalAlertBanner alerts={live.message?.critical_alerts ?? rows} />
      {liveStats ? <AlertStatsStrip stats={liveStats} /> : null}
      <AlertsTable alerts={rows} />
    </div>
  );
}

