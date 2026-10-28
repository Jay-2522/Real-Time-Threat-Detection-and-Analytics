import { RecordsTable } from "../components/tables/RecordTables";
import { ApiError } from "../components/ui/DataState";
import { useAsyncData } from "../hooks/useAsyncData";
import { dashboardService } from "../services/dashboardService";

export function MLPredictionsPage() {
  const predictions = useAsyncData(() => dashboardService.recentPredictions(50), [], 8_000);
  const highRisks = useAsyncData(() => dashboardService.highRisks(25), [], 8_000);

  return (
    <div className="space-y-6">
      <ApiError errors={[predictions.error, highRisks.error]} />
      <Header title="ML Predictions" detail="Isolation Forest anomaly scores, confidence, severity, explanations, and feature payloads." />
      <RecordsTable title="Recent Predictions" records={predictions.data ?? []} severityLabel="Model severity" />
      <RecordsTable title="High-Risk Prediction Queue" records={highRisks.data ?? []} severityLabel="Model severity" />
    </div>
  );
}

function Header({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
      <h1 className="text-2xl font-semibold text-white">{title}</h1>
      <p className="mt-2 text-sm text-slate-400">{detail}</p>
    </section>
  );
}

