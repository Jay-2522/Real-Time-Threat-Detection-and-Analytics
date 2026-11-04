export interface DashboardOverview {
  total_events: number;
  total_records: number;
  total_predictions: number;
  anomaly_count: number;
  high_risk_count: number;
  average_risk_score: number;
  system_status: string;
  generated_at: string;
}

export interface SystemComponentHealth {
  name: string;
  status: string;
  detail: string;
}

export interface SystemHealth {
  status: string;
  components: SystemComponentHealth[];
  generated_at: string;
}

export interface ChartPoint {
  label: string;
  value: number;
  secondary_value?: number | null;
}

export interface EntityRiskPoint {
  entity_id: string;
  risk_score: number;
  event_count: number;
  severity?: string | null;
}

export interface AnalyticsRecord {
  id?: number;
  event_type?: string | null;
  entity_id?: string | null;
  source_topic?: string;
  timestamp?: string | null;
  severity?: string | null;
  risk_score?: number | null;
  ml_risk_score?: number | null;
  prediction_id?: string | null;
  explanation?: string | null;
  raw_payload: Record<string, unknown>;
  created_at?: string;
}

export interface DashboardLiveMessage {
  timestamp: string;
  system_status: string;
  total_events: number;
  total_predictions: number;
  anomaly_count: number;
  high_risk_count: number;
  avg_risk_score: number;
  overview: DashboardOverview;
  latest_predictions: Record<string, unknown>[];
  recent_predictions: Record<string, unknown>[];
  recent_events: Record<string, unknown>[];
  high_risk_events: Record<string, unknown>[];
  event_counters: Record<string, number>;
}
