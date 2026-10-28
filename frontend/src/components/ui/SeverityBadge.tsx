const styles: Record<string, string> = {
  critical: "border-red-400/30 bg-red-400/15 text-red-200",
  high: "border-orange-400/30 bg-orange-400/15 text-orange-200",
  medium: "border-amber-400/30 bg-amber-400/15 text-amber-100",
  low: "border-emerald-400/25 bg-emerald-400/10 text-emerald-200",
  unlabeled: "border-slate-400/25 bg-slate-400/10 text-slate-300",
};

export function SeverityBadge({ severity }: { severity?: string | null }) {
  const value = severity ?? "unlabeled";
  return (
    <span className={`inline-flex items-center rounded-md border px-2 py-1 text-xs font-medium ${styles[value] ?? styles.unlabeled}`}>
      {value}
    </span>
  );
}

