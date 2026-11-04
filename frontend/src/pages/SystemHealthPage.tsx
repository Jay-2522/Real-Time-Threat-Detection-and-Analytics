import { CheckCircle2, CircleAlert } from "lucide-react";

import { useAsyncData } from "../hooks/useAsyncData";
import { ApiError, EmptyState } from "../components/ui/DataState";
import { dashboardService } from "../services/dashboardService";
import { monitoringService } from "../services/monitoringService";

export function SystemHealthPage() {
  const health = useAsyncData(dashboardService.systemHealth, [], 10_000);
  const monitoringServices = useAsyncData(monitoringService.services, [], 10_000);
  const components =
    monitoringServices.data?.length
      ? monitoringServices.data.map((service) => ({ name: service.name, status: service.status, detail: service.detail }))
      : health.data?.components ?? [];

  return (
    <div className="space-y-6">
      <ApiError errors={[health.error, monitoringServices.error]} />
      <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
        <p className="text-sm font-medium text-cyan-200">{health.data?.status ?? "checking"}</p>
        <h1 className="mt-2 text-2xl font-semibold text-white">System Health</h1>
        <p className="mt-2 text-sm text-slate-400">Local service health for the free Docker Compose analytics platform.</p>
      </section>
      {!components.length && !health.isLoading && !monitoringServices.isLoading ? <EmptyState /> : null}
      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {components.map((component) => {
          const ok = ["healthy", "online", "active", "observing", "idle", "waiting"].includes(component.status);
          return (
            <article key={component.name} className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <h2 className="font-semibold text-white">{component.name}</h2>
                  <p className="mt-2 text-sm text-slate-400">{component.detail}</p>
                </div>
                <div className={ok ? "text-emerald-300" : "text-amber-300"}>{ok ? <CheckCircle2 size={20} /> : <CircleAlert size={20} />}</div>
              </div>
              <p className="mt-4 text-xs uppercase text-slate-500">{component.status}</p>
            </article>
          );
        })}
      </section>
    </div>
  );
}
