import type { ElementType } from "@shared/schema";

interface ElementIconProps {
  type: ElementType;
  size?: number;
  className?: string;
}

export function ElementIcon({ type, size = 24, className = "" }: ElementIconProps) {
  const baseClass = `${className}`;
  
  switch (type) {
    case "external_source":
      return (
        <svg width={size} height={size} viewBox="0 0 24 24" className={baseClass} fill="none" stroke="currentColor" strokeWidth="1.5">
          <circle cx="12" cy="12" r="8" />
          <path d="M12 4V2M12 22V20M4 12H2M22 12H20" />
          <path d="M8 12L10 10M10 10L12 12M10 10V14" />
          <path d="M14 10L16 12" />
          <path d="M14 14L16 12" />
        </svg>
      );
    case "bus":
      return (
        <svg width={size} height={size} viewBox="0 0 24 24" className={baseClass} fill="none" stroke="currentColor" strokeWidth="2">
          <rect x="4" y="10" width="16" height="4" rx="1" />
          <path d="M8 10V6M12 10V6M16 10V6" />
          <path d="M8 14V18M12 14V18M16 14V18" />
        </svg>
      );
    case "line":
      return (
        <svg width={size} height={size} viewBox="0 0 24 24" className={baseClass} fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M4 12H20" />
          <circle cx="4" cy="12" r="2" fill="currentColor" />
          <circle cx="20" cy="12" r="2" fill="currentColor" />
          <path d="M8 8L12 12L16 8" strokeWidth="1.5" />
        </svg>
      );
    case "transformer":
      return (
        <svg width={size} height={size} viewBox="0 0 24 24" className={baseClass} fill="none" stroke="currentColor" strokeWidth="1.5">
          <circle cx="8" cy="12" r="5" />
          <circle cx="16" cy="12" r="5" />
          <path d="M3 12H3.5M20.5 12H21" strokeWidth="2" />
        </svg>
      );
    case "load":
      return (
        <svg width={size} height={size} viewBox="0 0 24 24" className={baseClass} fill="none" stroke="currentColor" strokeWidth="1.5">
          <path d="M12 4V8" strokeWidth="2" />
          <polygon points="6,8 18,8 15,20 9,20" fill="none" />
          <path d="M9 13H15M10 16H14" />
        </svg>
      );
    case "generator":
      return (
        <svg width={size} height={size} viewBox="0 0 24 24" className={baseClass} fill="none" stroke="currentColor" strokeWidth="1.5">
          <circle cx="12" cy="12" r="8" />
          <path d="M12 4V2" strokeWidth="2" />
          <text x="12" y="15" textAnchor="middle" fontSize="8" fill="currentColor" stroke="none" fontWeight="bold">G</text>
        </svg>
      );
    case "battery":
      return (
        <svg width={size} height={size} viewBox="0 0 24 24" className={baseClass} fill="none" stroke="currentColor" strokeWidth="1.5">
          <rect x="4" y="8" width="14" height="10" rx="1" />
          <path d="M18 11V15H20V11H18Z" fill="currentColor" />
          <path d="M7 12V14M10 11V15M13 12V14" strokeWidth="2" />
        </svg>
      );
    case "capacitor":
      return (
        <svg width={size} height={size} viewBox="0 0 24 24" className={baseClass} fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M4 12H9" />
          <path d="M15 12H20" />
          <path d="M9 6V18" />
          <path d="M15 6V18" />
        </svg>
      );
    case "switch":
      return (
        <svg width={size} height={size} viewBox="0 0 24 24" className={baseClass} fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="6" cy="12" r="2" />
          <circle cx="18" cy="12" r="2" />
          <path d="M8 12L16 8" />
        </svg>
      );
    case "cable":
      return (
        <svg width={size} height={size} viewBox="0 0 24 24" className={baseClass} fill="none" stroke="currentColor" strokeWidth="1.5">
          <path d="M4 12C4 12 7 8 12 8C17 8 20 12 20 12" strokeWidth="2" />
          <path d="M4 12C4 12 7 16 12 16C17 16 20 12 20 12" strokeWidth="2" />
          <circle cx="4" cy="12" r="2" fill="currentColor" />
          <circle cx="20" cy="12" r="2" fill="currentColor" />
        </svg>
      );
    case "ductbank":
      return (
        <svg width={size} height={size} viewBox="0 0 24 24" className={baseClass} fill="none" stroke="currentColor" strokeWidth="1.5">
          <rect x="4" y="6" width="16" height="12" rx="2" />
          <circle cx="9" cy="10" r="1.5" />
          <circle cx="15" cy="10" r="1.5" />
          <circle cx="9" cy="14" r="1.5" />
          <circle cx="15" cy="14" r="1.5" />
        </svg>
      );
    default:
      return null;
  }
}

export const elementColors: Record<ElementType, { bg: string; border: string; text: string }> = {
  external_source: { bg: "bg-amber-500/20", border: "border-amber-500", text: "text-amber-600 dark:text-amber-400" },
  bus: { bg: "bg-sky-500/20", border: "border-sky-500", text: "text-sky-600 dark:text-sky-400" },
  line: { bg: "bg-emerald-500/20", border: "border-emerald-500", text: "text-emerald-600 dark:text-emerald-400" },
  transformer: { bg: "bg-yellow-500/20", border: "border-yellow-500", text: "text-yellow-600 dark:text-yellow-400" },
  load: { bg: "bg-rose-500/20", border: "border-rose-500", text: "text-rose-600 dark:text-rose-400" },
  generator: { bg: "bg-stone-400/20", border: "border-stone-400", text: "text-stone-700 dark:text-stone-300" },
  battery: { bg: "bg-cyan-500/20", border: "border-cyan-500", text: "text-cyan-600 dark:text-cyan-400" },
  capacitor: { bg: "bg-pink-500/20", border: "border-pink-500", text: "text-pink-600 dark:text-pink-400" },
  switch: { bg: "bg-orange-500/20", border: "border-orange-500", text: "text-orange-600 dark:text-orange-400" },
  cable: { bg: "bg-teal-500/20", border: "border-teal-500", text: "text-teal-600 dark:text-teal-400" },
  ductbank: { bg: "bg-amber-500/20", border: "border-amber-500", text: "text-amber-600 dark:text-amber-400" },
};

export const elementLabels: Record<ElementType, string> = {
  external_source: "External Source",
  bus: "Bus",
  line: "Line",
  transformer: "Transformer",
  load: "Load",
  generator: "Generator",
  battery: "Battery",
  capacitor: "Shunt / Capacitor",
  switch: "Switch",
  cable: "Cable",
  ductbank: "Ductbank",
};
