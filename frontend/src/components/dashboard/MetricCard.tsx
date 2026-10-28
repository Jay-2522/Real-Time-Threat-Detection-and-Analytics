import type { LucideIcon } from "lucide-react";

interface MetricCardProps {
  label: string;
  value: string | number;
  detail: string;
  icon: LucideIcon;
  tone?: "cyan" | "emerald" | "amber" | "red";
  tooltip?: string;
}

const tones = {
  cyan: "bg-cyan-400/12 text-cyan-200",
  emerald: "bg-emerald-400/12 text-emerald-200",
  amber: "bg-amber-400/12 text-amber-200",
  red: "bg-red-400/12 text-red-200",
};

export function MetricCard({ label, value, detail, icon: Icon, tone = "cyan", tooltip }: MetricCardProps) {
  return (
    <article className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-sm text-slate-400" title={tooltip}>{label}</p>
          <p className="mt-3 text-3xl font-semibold text-white">{value}</p>
        </div>
        <div className={`grid h-10 w-10 place-items-center rounded-md ${tones[tone]}`}>
          <Icon size={18} />
        </div>
      </div>
      <p className="mt-4 text-sm text-slate-500">{detail}</p>
    </article>
  );
}

