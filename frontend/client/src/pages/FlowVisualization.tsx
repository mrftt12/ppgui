import { useState, useCallback, lazy, Suspense, useMemo } from "react";
import { Link } from "wouter";
import { ArrowLeft, Zap, Info } from "lucide-react";
import { buildFromUIElements, type NetworkGraph } from "@/lib/networkGraph";
import type { NetworkElement, Connection } from "@shared/schema";
import type { LoadFlowResult } from "@shared/schema";

const PowerFlowVisualization = lazy(
  () => import("@/components/PowerFlowVisualization"),
);

interface FlowVisualizationProps {
  embedded?: boolean;
  /** When supplied, the 3D scene renders the given network instead of the IEEE-123 demo. */
  elements?: NetworkElement[];
  connections?: Connection[];
  analysisResult?: LoadFlowResult | null;
}

export default function FlowVisualization({
  embedded = false,
  elements,
  connections,
  analysisResult,
}: FlowVisualizationProps) {
  const [bloomStrength, setBloomStrength] = useState(1.0);
  const [particleSize, setParticleSize]   = useState(0.03);
  const [rotationSpeed, setRotationSpeed] = useState(0);
  const [displayResults, setDisplayResults] = useState(true);

  const graph: NetworkGraph | undefined = useMemo(() => {
    if (!elements || !connections || elements.length === 0) return undefined;
    return buildFromUIElements(elements as any[], connections as any[]);
  }, [elements, connections]);

  return (
    <div className={`relative overflow-hidden bg-black text-white font-sans ${embedded ? "w-full h-full" : "w-screen h-screen"}`}>
      {/* ── 3D canvas ─────────────────────────────────────────── */}
      <Suspense
        fallback={
          <div className="flex items-center justify-center w-full h-full">
            <Zap className="animate-pulse w-12 h-12 text-[#ff5900]" />
          </div>
        }
      >
        <PowerFlowVisualization
          className="absolute inset-0"
          graph={graph}
          loadFlowResult={analysisResult}
          showVoltageLabels={displayResults}
        />
      </Suspense>

      {/* ── title / back button ───────────────────────────────── */}
      <div className="absolute top-5 right-5 flex flex-col items-end z-50 pointer-events-auto gap-3">
        <div className="glass-pane px-5 py-3">
          <h1
            className="text-2xl font-bold"
            style={{
              background: "linear-gradient(45deg, #10b981, #ffd700)",
              WebkitBackgroundClip: "text",
              WebkitTextFillColor: "transparent",
            }}
          >
            {graph ? "Network \u00B7 3D Flow" : "IEEE 123 Bus \u00B7 3D Flow"}
          </h1>
        </div>
        {!embedded && (
          <Link
            href="/"
            className="glass-pane-pill flex items-center gap-2 px-4 py-2 text-sm"
          >
            <ArrowLeft size={16} /> Network Editor
          </Link>
        )}
      </div>

      {/* ── controls ──────────────────────────────────────────── */}
      <div className={`absolute left-5 z-50 pointer-events-auto flex flex-col gap-4 ${embedded ? "top-24" : "top-5"}`}>
        <div className="glass-pane p-4 flex flex-col gap-3 w-56">
          <label className="text-xs text-white/60 block">
            Bloom Strength:{" "}
            <span className="text-white">{bloomStrength.toFixed(1)}</span>
          </label>
          <input
            type="range"
            min="0"
            max="2"
            step="0.1"
            value={bloomStrength}
            onChange={(e) => setBloomStrength(parseFloat(e.target.value))}
            className="w-full"
          />

          <label className="text-xs text-white/60 block">
            Particle Size:{" "}
            <span className="text-white">{particleSize.toFixed(3)}</span>
          </label>
          <input
            type="range"
            min="0.01"
            max="0.08"
            step="0.002"
            value={particleSize}
            onChange={(e) => setParticleSize(parseFloat(e.target.value))}
            className="w-full"
          />
        </div>

        {/* legend – bus types */}
        <div className="glass-pane p-4 flex flex-col gap-2 text-xs w-56">
          <span className="text-white/60 font-semibold mb-1">Bus Types</span>
          <Dot color="#ffd700" label="Source (ext grid)" />
          <Dot color="#ff5900" label="Load bus" />
          <Dot color="#00e5ff" label="Capacitor bank" />
          <Dot color="#667788" label="Junction" />
        </div>

        {/* legend – phase colours */}
        <div className="glass-pane p-4 flex flex-col gap-2 text-xs w-56">
          <span className="text-white/60 font-semibold mb-1">Phase Codes</span>
          <Dot color="#ef4444" label="Phase A" />
          <Dot color="#f59e0b" label="Phase B" />
          <Dot color="#3b82f6" label="Phase C" />
          <Dot color="#f97316" label="Phase AB" />
          <Dot color="#14b8a6" label="Phase AC" />
          <Dot color="#a855f7" label="Phase BC" />
          <Dot color="#10b981" label="Phase ABC" />
        </div>

        <div className="glass-pane p-4 flex items-center justify-between gap-3 w-56">
          <label htmlFor="display-results" className="text-xs text-white/80 font-semibold">
            Display Results
          </label>
          <input
            id="display-results"
            type="checkbox"
            checked={displayResults}
            onChange={(e) => setDisplayResults(e.target.checked)}
            className="h-4 w-4 accent-emerald-500"
          />
        </div>
      </div>

      {/* ── instructions ──────────────────────────────────────── */}
      <div className="absolute bottom-5 left-5 z-50 text-white/50 text-xs flex items-center gap-2 pointer-events-none select-none glass-pane px-4 py-2">
        <Info size={14} /> Drag to rotate &middot; Scroll to zoom &middot;
        Double-click to reset view &middot; IEEE 123-Bus Distribution Feeder
      </div>
    </div>
  );
}

/* tiny legend dot */
function Dot({ color, label }: { color: string; label: string }) {
  return (
    <div className="flex items-center gap-2">
      <span
        className="inline-block w-2.5 h-2.5 rounded-full"
        style={{ backgroundColor: color, boxShadow: `0 0 6px ${color}` }}
      />
      <span className="text-white/80">{label}</span>
    </div>
  );
}
