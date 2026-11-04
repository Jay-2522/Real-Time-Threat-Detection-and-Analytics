export interface ThreatOverview {
  generated_at: string;
  total_enriched_events: number;
  ioc_matches: number;
  high_risk_entities: number;
  rule_hits: number;
  active_timelines: number;
  severity_counts: Record<string, number>;
}

export interface IOCMatch {
  id: number;
  ioc_value: string;
  ioc_type: string;
  entity_id?: string | null;
  confidence: number;
  severity: string;
  feed_name: string;
  description: string;
  timestamp: string;
}

export interface EntityProfile {
  entity_id: string;
  entity_type: string;
  first_seen: string;
  last_seen: string;
  risk_score: number;
  alert_count: number;
  anomaly_count: number;
  ioc_count: number;
  tactics: string[];
  techniques: string[];
  related_entities: string[];
  risk_trend: { ts: string; score: number }[];
}

export interface MitreTechnique {
  tactic: string;
  technique: string;
  mitre_id: string;
  count?: number;
  avg_threat_score?: number;
}

export interface DetectionRuleSummary {
  rule_id: string;
  name: string;
  severity: string;
  enabled: boolean;
  hit_count: number;
  avg_score: number;
}

export interface LoadedDetectionRule {
  rule_id: string;
  name: string;
  severity: string;
  enabled: boolean;
  tactic?: string | null;
  technique?: string | null;
  mitre_id?: string | null;
  loaded_at?: string | null;
}

export interface AttackTimeline {
  timeline_id: string;
  entity_id: string;
  updated_at: string;
  severity: string;
  event_count: number;
  attack_chain: { timestamp: string; event_id: string; tactic?: string; technique?: string; severity: string; score: number }[];
  nodes: { id: string; label: string; type: string; severity?: string; score?: number }[];
  edges: { id: string; source: string; target: string; label?: string }[];
}

export interface ThreatGraph {
  nodes: { id: string; label: string; type: string; risk_score?: number }[];
  edges: { id: string; source: string; target: string; label?: string; weight?: number }[];
}
