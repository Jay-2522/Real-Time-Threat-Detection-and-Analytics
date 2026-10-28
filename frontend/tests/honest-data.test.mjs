import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const dashboardService = await readFile(new URL("../src/services/dashboardService.ts", import.meta.url), "utf8");
const dataState = await readFile(new URL("../src/components/ui/DataState.tsx", import.meta.url), "utf8");
const recordsTable = await readFile(new URL("../src/components/tables/RecordTables.tsx", import.meta.url), "utf8");
const liveStream = await readFile(new URL("../src/pages/LiveStreamPage.tsx", import.meta.url), "utf8");

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
