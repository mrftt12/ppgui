import { Minus, MoveRight, Target } from "lucide-react";

type MapToolId = "line" | "arrow" | "target" | "transformer" | "load";

interface MapToolsPaletteProps {
  activeTool?: MapToolId | "pointer" | "navigate";
  onToolChange?: (toolId: MapToolId | "pointer" | "navigate") => void;
  onCenterNetwork?: () => void;
  onToggleLocations?: () => void;
  onToggleViewMode?: () => void;
  showLocations?: boolean;
  viewMode?: "singleLine" | "spatial";
  collapsed?: boolean;
}

const MAP_TOOLS = [
  { id: "line", icon: Minus, label: "Line", shortcut: "L" },
  { id: "arrow", icon: MoveRight, label: "Arrow / Connect", shortcut: "A" },
  { id: "target", icon: Target, label: "Center / Target", shortcut: "C" },
  { id: "transformer", icon: "trafo", label: "Transformer", shortcut: "T", isCustom: true },
  { id: "load", icon: "load", label: "Load", shortcut: "R", isCustom: true, color: "#ef4444" },
] as const;

export function MapToolsPalette({
  activeTool = "pointer",
  onToolChange,
  onCenterNetwork,
  onToggleLocations,
  onToggleViewMode,
  showLocations = true,
  viewMode = "singleLine",
  collapsed = false,
}: MapToolsPaletteProps) {
  const handleToolClick = (toolId: string) => {
    switch (toolId) {
      case "pointer":
      case "navigate":
      case "line":
      case "arrow":
      case "target":
      case "transformer":
      case "load":
        onToolChange?.(toolId as MapToolId);
        break;
      case "center":
        onCenterNetwork?.();
        break;
      case "location":
        onToggleLocations?.();
        break;
      case "view":
        onToggleViewMode?.();
        break;
      default:
        break;
    }
  };

  const isToolActive = (toolId: string) => {
    if (toolId === "pointer" || toolId === "navigate") {
      return activeTool === toolId;
    }
    if (MAP_TOOLS.some((tool) => tool.id === toolId)) {
      return activeTool === toolId;
    }
    if (toolId === "location") {
      return showLocations;
    }
    if (toolId === "view") {
      return viewMode === "spatial";
    }
    return false;
  };

  if (collapsed) return null;

  const TransformerIcon = () => (
    <svg
      width="20"
      height="20"
      viewBox="0 0 20 20"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M4 10 L6 6 L8 14 L10 6 L12 14 L14 6 L16 10" />
    </svg>
  );

  const LoadIcon = () => (
    <svg
      width="20"
      height="20"
      viewBox="0 0 20 20"
      fill="none"
      stroke="#ef4444"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M4 4 L6 8 L8 4 L10 8 L12 4 L14 8 L16 4" />
      <path d="M10 10 L10 16" />
      <path d="M7 13 L10 16 L13 13" />
    </svg>
  );

  const renderIcon = (tool: (typeof MAP_TOOLS)[number]) => {
    if (tool.isCustom) {
      if (tool.id === "transformer") return <TransformerIcon />;
      if (tool.id === "load") return <LoadIcon />;
    }
    const IconComponent = tool.icon;
    return <IconComponent size={20} />;
  };

  return (
    <div className="absolute left-4 top-16 z-30 flex flex-col gap-2 rounded-xl border border-border bg-card/90 p-2 shadow-lg backdrop-blur">
      {MAP_TOOLS.map((tool) => {
        const active = isToolActive(tool.id);
        return (
          <button
            key={tool.id}
            className={`flex h-9 w-9 items-center justify-center rounded-lg border text-foreground transition ${
              active
                ? "border-primary bg-primary/15 text-primary"
                : "border-transparent bg-muted/40 hover:bg-muted"
            }`}
            onClick={() => handleToolClick(tool.id)}
            title={`${tool.label} (${tool.shortcut})`}
            aria-label={tool.label}
          >
            {renderIcon(tool)}
          </button>
        );
      })}
    </div>
  );
}
