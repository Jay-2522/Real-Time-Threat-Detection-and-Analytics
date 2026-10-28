import { httpClient } from "../api/httpClient";
import type { AlertRecord, AlertStats, IncidentRecord } from "../types/alerts";

export const alertService = {
  async recentAlerts(limit = 50) {
    return get<AlertRecord[]>(`/alerts/recent?limit=${limit}`);
  },
  async highAlerts(limit = 50) {
    return get<AlertRecord[]>(`/alerts/high?limit=${limit}`);
  },
  async criticalAlerts(limit = 25) {
    return get<AlertRecord[]>(`/alerts/critical?limit=${limit}`);
  },
  async recentIncidents(limit = 50) {
    return get<IncidentRecord[]>(`/incidents/recent?limit=${limit}`);
  },
  async openIncidents(limit = 50) {
    return get<IncidentRecord[]>(`/incidents/open?limit=${limit}`);
  },
  async incident(incidentId: string) {
    return get<IncidentRecord>(`/incidents/${encodeURIComponent(incidentId)}`);
  },
  async stats() {
    return get<AlertStats>("/alerts/stats");
  },
  async search(query: string, limit = 25) {
    if (!query.trim()) return [];
    return get<Record<string, unknown>[]>(`/alerts/search?q=${encodeURIComponent(query)}&limit=${limit}`);
  },
};

async function get<T>(url: string): Promise<T> {
  return (await httpClient.get<T>(url)).data;
}

