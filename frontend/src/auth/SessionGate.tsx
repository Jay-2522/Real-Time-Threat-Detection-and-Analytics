import { useEffect, useState, type FormEvent, type ReactNode } from "react";
import axios from "axios";
import { httpClient, setCsrfToken } from "../api/httpClient";
import { SessionContext, type User } from "./session";

type SessionResponse = { user: User; csrf_token: string; expires_at: number };
export function SessionGate({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<SessionResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  function accept(data: SessionResponse) { setCsrfToken(data.csrf_token); setSession(data); setMessage(""); }
  useEffect(() => {
    let active = true;
    const expired = () => { setCsrfToken(""); setSession(null); setMessage("Your session has expired. Sign in again."); };
    const forbidden = () => setMessage("You do not have permission to perform this action.");
    window.addEventListener("session-expired", expired);
    window.addEventListener("access-forbidden", forbidden);
    httpClient.get<SessionResponse>("/auth/me").then(({ data }) => { if (active) accept(data); }).catch((error) => {
      if (active) setMessage(axios.isAxiosError(error) && error.response?.status === 401 ? "" : "Unable to check your session. Try signing in again.");
    }).finally(() => { if (active) setLoading(false); });
    return () => { active = false; window.removeEventListener("session-expired", expired); window.removeEventListener("access-forbidden", forbidden); };
  }, []);
  useEffect(() => {
    if (!session) return;
    const timer = window.setTimeout(() => window.dispatchEvent(new Event("session-expired")), Math.max(0, session.expires_at * 1000 - Date.now()));
    return () => window.clearTimeout(timer);
  }, [session]);
  async function login(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    setBusy(true); setMessage("");
    try { accept((await httpClient.post<SessionResponse>("/auth/login", { username: form.get("username"), password: form.get("password") })).data); }
    catch (error) {
      setMessage(axios.isAxiosError(error) && error.response?.status === 429 ? "Too many attempts. Try again in 15 minutes." : "Sign-in failed. Check your credentials and connection.");
    } finally { setBusy(false); }
  }
  async function logout() {
    try { await httpClient.post("/auth/logout"); setCsrfToken(""); setSession(null); setMessage("Signed out."); }
    catch { setMessage("Sign-out could not be confirmed. Please retry."); }
  }
  if (loading) return <main className="p-8 text-slate-100">Checking session…</main>;
  if (session) return <SessionContext.Provider value={{ user: session.user, logout }}>{message && <div role="alert" className="bg-amber-950 p-3 text-amber-100">{message}</div>}{children}</SessionContext.Provider>;
  return <main className="grid min-h-screen place-items-center bg-surface-950 text-slate-100">
    <form onSubmit={login} className="w-full max-w-sm space-y-5 rounded-lg border border-white/10 bg-surface-900 p-8">
      <h1 className="text-xl font-semibold">Sign in to Streaming Security Analytics (local demo)</h1>
      {message && <p role="alert" className="text-sm text-amber-200">{message}</p>}
      <label className="block">Username<input name="username" autoComplete="username" required maxLength={64} className="mt-2 w-full rounded bg-white/10 p-2" /></label>
      <label className="block">Password<input name="password" type="password" autoComplete="current-password" required maxLength={128} className="mt-2 w-full rounded bg-white/10 p-2" /></label>
      <button disabled={busy} type="submit" className="w-full rounded bg-cyan-700 p-2 disabled:opacity-50">{busy ? "Signing in…" : "Sign in"}</button>
    </form>
  </main>;
}
