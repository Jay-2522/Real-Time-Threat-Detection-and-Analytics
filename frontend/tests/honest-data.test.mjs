import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const dashboardService = await readFile(new URL("../src/services/dashboardService.ts", import.meta.url), "utf8");
const dataState = await readFile(new URL("../src/components/ui/DataState.tsx", import.meta.url), "utf8");
const recordsTable = await readFile(new URL("../src/components/tables/RecordTables.tsx", import.meta.url), "utf8");
const liveStream = await readFile(new URL("../src/pages/LiveStreamPage.tsx", import.meta.url), "utf8");
const dashboardLayout = await readFile(new URL("../src/layouts/DashboardLayout.tsx", import.meta.url), "utf8");
const alertPanels = await readFile(new URL("../src/components/alerts/AlertPanels.tsx", import.meta.url), "utf8");
const overviewPage = await readFile(new URL("../src/pages/OverviewPage.tsx", import.meta.url), "utf8");
const detectionPage = await readFile(new URL("../src/pages/DetectionEngineeringPage.tsx", import.meta.url), "utf8");

test("dashboard service cannot restore the known fabricated fallbacks", () => {
  for (const knownFake of ["fallbackTrend", "getOrFallback", "10.12.4.91", "u-1007", 'label: "00:05"', "value: 42"]) {
    assert.equal(dashboardService.includes(knownFake), false, `fabricated fallback returned: ${knownFake}`);
  }
});

test("empty and failed requests have explicit user-facing states", () => {
  assert.match(dataState, /No data yet/);
  assert.match(dataState, /API unreachable/);
});

test("live records keep provenance and distinguish source labels from model severity", () => {
  assert.doesNotMatch(recordsTable, /\?\? "stream"/);
  assert.match(recordsTable, /"unlabeled"/);
  assert.match(recordsTable, /"Event label" \| "Model severity"/);
  assert.match(liveStream, /payload\.source_topic/);
  assert.match(liveStream, /severityLabel="Event label"/);
  assert.match(liveStream, /severityLabel="Model severity"/);
});

test("the shell contains no inert search, notification, or duplicate SOC feed controls", () => {
  assert.doesNotMatch(dashboardLayout, /Search telemetry, entities, predictions/);
  assert.doesNotMatch(dashboardLayout, /aria-label="Notifications"/);
  assert.doesNotMatch(alertPanels, /Live SOC Feed/);
});

test("default navigation contains exactly the six demo-ready pages", () => {
  const block = dashboardLayout.match(/const defaultNavigationItems = \[(.*?)\];/s);
  assert.ok(block, "default navigation declaration is present");
  const labels = [...block[1].matchAll(/label: "([^"]+)"/g)].map((match) => match[1]);
  assert.deepEqual(labels, [
    "Overview",
    "Live Stream",
    "ML Predictions",
    "Alerts",
    "Incidents",
    "Detection Engineering",
  ]);
});

test("visible dashboard labels describe the local demo and persisted data honestly", () => {
  assert.match(dashboardLayout, /Streaming Security Analytics \(local demo\)/);
  assert.match(overviewPage, /Persisted pipeline records/);
  assert.match(overviewPage, /Counts raw and derived records\./);
  assert.match(overviewPage, /Anomaly scores/);
  assert.match(overviewPage, /High anomaly score/);
  assert.doesNotMatch(overviewPage, /label="System"/);
  assert.match(detectionPage, /Loaded rules/);
  assert.match(detectionPage, /Threat-engine monitoring target reachable/);
});
