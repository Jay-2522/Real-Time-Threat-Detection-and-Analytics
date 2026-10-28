import { SessionGate } from "./auth/SessionGate";
import { Navigate, Route, Routes } from "react-router-dom";

import { DashboardLayout } from "./layouts/DashboardLayout";
import { AlertsPage } from "./pages/AlertsPage";
import { AttackTimelinePage } from "./pages/AttackTimelinePage";
import { DetectionEngineeringPage } from "./pages/DetectionEngineeringPage";
import { EntitiesPage } from "./pages/EntitiesPage";
import { IncidentsPage } from "./pages/IncidentsPage";
import { LiveStreamPage } from "./pages/LiveStreamPage";
import { MitreAttackPage } from "./pages/MitreAttackPage";
import { MLPredictionsPage } from "./pages/MLPredictionsPage";
import { ObservabilityPage } from "./pages/ObservabilityPage";
import { OverviewPage } from "./pages/OverviewPage";
import { RiskAnalyticsPage } from "./pages/RiskAnalyticsPage";
import { SearchPage } from "./pages/SearchPage";
import { SystemHealthPage } from "./pages/SystemHealthPage";
import { ThreatHuntingPage } from "./pages/ThreatHuntingPage";
import { ThreatIntelligencePage } from "./pages/ThreatIntelligencePage";
import { config } from "./config/environment";

export default function App() {
  return (
    <SessionGate><Routes>
      <Route element={<DashboardLayout />}>
        <Route index element={<OverviewPage />} />
        <Route path="live" element={<LiveStreamPage />} />
        {config.features.riskAnalytics ? <Route path="risk" element={<RiskAnalyticsPage />} /> : null}
        <Route path="predictions" element={<MLPredictionsPage />} />
        {config.features.search ? <Route path="search" element={<SearchPage />} /> : null}
        {config.features.systemHealth ? <Route path="health" element={<SystemHealthPage />} /> : null}
        <Route path="alerts" element={<AlertsPage />} />
        <Route path="incidents" element={<IncidentsPage />} />
        {config.features.observability ? <Route path="observability" element={<ObservabilityPage />} /> : null}
        {config.features.threatIntelligence ? <Route path="threat-intelligence" element={<ThreatIntelligencePage />} /> : null}
        <Route path="detection-engineering" element={<DetectionEngineeringPage />} />
        {config.features.threatHunting ? <Route path="threat-hunting" element={<ThreatHuntingPage />} /> : null}
        {config.features.mitreAttack ? <Route path="mitre-attack" element={<MitreAttackPage />} /> : null}
        {config.features.entities ? <Route path="entities" element={<EntitiesPage />} /> : null}
        {config.features.attackTimeline ? <Route path="attack-timeline" element={<AttackTimelinePage />} /> : null}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes></SessionGate>
  );
}
