import { RecordsTable } from "../components/tables/RecordTables";
import { ApiError } from "../components/ui/DataState";
import { useAsyncData } from "../hooks/useAsyncData";
import { useDashboardWebSocket } from "../hooks/useDashboardWebSocket";
import { dashboardService } from "../services/dashboardService";
import type { AnalyticsRecord } from "../types/dashboard";

export function LiveStreamPage() {
  const events = useAsyncData(() => dashboardService.recentEvents(50), [], 5_000);
  const predictions = useAsyncData(() => dashboardService.recentPredictions(25), [], 5_000);
  const live = useDashboardWebSocket();
  const livePredictions = (live.message?.latest_predictions ?? []).map((payload) => ({
    source_topic: typeof payload.source_topic === "string" ? payload.source_topic : undefined,
    raw_payload: payload,
  })) as AnalyticsRecord[];

  return (
    <div className="space-y-6">
      <ApiError errors={[events.error, predictions.error]} />
      <PageHeader title="Live Stream" detail="Auto-refreshing telemetry and anomaly scores from the local streaming pipeline." status={live.status} />
      <RecordsTable title="Auto-refreshing event feed" records={events.data ?? []} severityLabel="Event label" />
      <RecordsTable title="Auto-refreshing anomaly scores" records={livePredictions.length ? livePredictions : predictions.data ?? []} severityLabel="Model severity" />
    </div>
  );
}

function PageHeader({ title, detail, status }: { title: string; detail: string; status: string }) {
  return (
    <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
      <p className="text-sm font-medium text-cyan-200">{status}</p>
      <h1 className="mt-2 text-2xl font-semibold text-white">{title}</h1>
      <p className="mt-2 text-sm text-slate-400">{detail}</p>
    </section>
  );
}

