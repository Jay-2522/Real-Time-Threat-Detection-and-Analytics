import { Search } from "lucide-react";
import type { FormEvent } from "react";
import { useState } from "react";

import { dashboardService } from "../services/dashboardService";
import { describeApiError } from "../utils/apiErrors";

export function SearchPage() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setLoading(true);
    setError(null);
    try { setResults(await dashboardService.searchEvents(query)); }
    catch (failure) { setError(describeApiError(failure)); setResults([]); }
    finally { setLoading(false); }
  }

  return (
    <div className="space-y-6">
      {error ? <div role="alert" className="rounded-md border border-red-400/30 bg-red-400/10 px-4 py-3 text-sm text-red-100">{error}</div> : null}
      <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
        <h1 className="text-2xl font-semibold text-white">Search Events</h1>
        <form onSubmit={submit} className="mt-5 flex flex-col gap-3 md:flex-row">
          <div className="flex flex-1 items-center gap-3 rounded-md border border-white/10 bg-white/5 px-3">
            <Search size={18} className="text-slate-500" />
            <input value={query} onChange={(event) => setQuery(event.target.value)} className="h-11 flex-1 bg-transparent text-sm text-white outline-none" placeholder="Search entity, endpoint, severity, explanation" />
          </div>
          <button className="rounded-md bg-cyan-400 px-5 py-2.5 text-sm font-semibold text-surface-950" type="submit">Search</button>
        </form>
      </section>
      <section className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
        <h2 className="text-sm font-semibold text-white">{loading ? "Searching..." : `${results.length} results`}</h2>
        <div className="mt-4 space-y-3">
          {results.map((result, index) => (
            <pre key={index} className="max-h-64 overflow-auto rounded-md bg-surface-950 p-4 text-xs text-slate-300">{JSON.stringify(result, null, 2)}</pre>
          ))}
          {!results.length && !loading ? <p className="py-10 text-center text-sm text-slate-500">Search results will appear here.</p> : null}
        </div>
      </section>
    </div>
  );
}
