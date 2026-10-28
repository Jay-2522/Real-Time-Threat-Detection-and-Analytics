const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000/api/v1";
const eventSource = import.meta.env.VITE_EVENT_SOURCE ?? "synthetic";
const enabled = (value: string | undefined) => value === "true";

function dashboardWebSocketUrl() {
  const configured = import.meta.env.VITE_DASHBOARD_WS_URL ?? import.meta.env.VITE_WS_BASE_URL;
  if (configured) {
    return configured.endsWith("/ws/dashboard") ? configured : `${configured.replace(/\/$/, "")}/ws/dashboard`;
  }

  const url = new URL(apiBaseUrl, window.location.origin);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = `${url.pathname.replace(/\/$/, "")}/ws/dashboard`;
  url.search = "";
  url.hash = "";
  return url.toString();
}

function alertsWebSocketUrl() {
  const configured = import.meta.env.VITE_ALERTS_WS_URL;
  if (configured) return configured;
  return dashboardWebSocketUrl().replace("/ws/dashboard", "/ws/alerts");
}

export const config = {
  apiBaseUrl,
  eventSource,
  eventSourceLabel: eventSource === "synthetic" ? "Synthetic generator" : eventSource,
  features: {
    riskAnalytics: enabled(import.meta.env.VITE_ENABLE_RISK_ANALYTICS),
    threatIntelligence: enabled(import.meta.env.VITE_ENABLE_THREAT_INTELLIGENCE),
    mitreAttack: enabled(import.meta.env.VITE_ENABLE_MITRE_ATTACK),
    entities: enabled(import.meta.env.VITE_ENABLE_ENTITIES),
    attackTimeline: enabled(import.meta.env.VITE_ENABLE_ATTACK_TIMELINE),
    search: enabled(import.meta.env.VITE_ENABLE_SEARCH),
    threatHunting: enabled(import.meta.env.VITE_ENABLE_THREAT_HUNTING),
    systemHealth: enabled(import.meta.env.VITE_ENABLE_SYSTEM_HEALTH),
    observability: enabled(import.meta.env.VITE_ENABLE_OBSERVABILITY),
  },
  dashboardWebSocketUrl: dashboardWebSocketUrl(),
  alertsWebSocketUrl: alertsWebSocketUrl(),
};
