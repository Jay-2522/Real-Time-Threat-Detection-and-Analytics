export interface AlertRecord {
  id?: number;
  alert_id: string;
  timestamp: string;
  created_at?: string;
  severity: string;
  title: string;
  description?: string;
  source_event_id?: string | null;
  entity_id?: string | null;
  event_type?: string | null;
  source_topic?: string;
  anomaly_score?: number | null;
  ml_risk_score?: number | null;
  correlation_id?: string;
  incident_id?: string | null;
  explanation: string;
  recommended_action: string;
  status: string;
  tags: string[];
}

export interface IncidentRecord {
  id?: number;
  incident_id: string;
  created_at: string;
  updated_at: string;
  severity: string;
  title: string;
  description: string;
  related_alerts: string[];
  entity_ids: string[];
  status: string;
  event_count: number;
  escalation_level: number;
  resolution_notes?: string | null;
  raw_payload?: Record<string, unknown>;
}

export interface AlertStats {
  total_alerts: number;
  open_alerts: number;
  critical_alerts: number;
  high_alerts: number;
  total_incidents: number;
  open_incidents: number;
  severity_counts: Record<string, number>;
}

export interface AlertsLiveMessage {
  timestamp: string;
  recent_alerts: AlertRecord[];
  critical_alerts: AlertRecord[];
  recent_incidents: IncidentRecord[];
  severity_counters: Record<string, number>;
  stats: AlertStats;
}

