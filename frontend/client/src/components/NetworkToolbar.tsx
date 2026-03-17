import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuLabel,
    DropdownMenuSeparator,
    DropdownMenuTrigger
} from "@/components/ui/dropdown-menu";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Input } from "@/components/ui/input";
import {
    Tooltip,
    TooltipContent,
    TooltipTrigger,
    TooltipProvider
} from "@/components/ui/tooltip";
import {
    FilePlus,
    FolderOpen,
    Save,
    Trash2,
    Play,
    RotateCcw,
    Search,
    Grid3X3,
    LayoutGrid,
    ZoomIn,
    ZoomOut,
    PieChart,
    ZapOff,
    TrendingUp,
    Thermometer,
    LineChart,
    Settings,
    SlidersHorizontal
} from "lucide-react";

export interface LayoutSettings {
    componentSpacing: number;
    ringSpacing: number;
    nodeSpacing: number;
    levelSpacing: number;
    gridSpacing: number;
    circularRadius: number;
    spiralStep: number;
    forceCharge: number;
    forceLinkDistance: number;
    forceCollide: number;
    forceIterations: number;
}

interface NetworkToolbarProps {
    onNew: () => void;
    onOpen: () => void;
    onSave: () => void;
    onDelete: () => void;
    onResetWorkspace: () => void;
    onRunAnalysis: () => void;
    onLoadAllocation?: () => void;
    onShortCircuit?: () => void;
    onHostingCapacity?: () => void;
    onThermalModeling?: () => void;
    onTimeSeries?: () => void;
    onResetLayout: () => void;
    onAutoLayout: (algorithm: "radial" | "layered" | "grid" | "cluster" | "circular" | "concentric" | "spiral" | "tree" | "force" | "hierarchy" | "stratify" | "partition" | "pack" | "treemap") => void;
    onApplyLayout: () => void;
    onZoomIn: () => void;
    onZoomOut: () => void;
    onFitView: () => void;
    onOpenSettings: () => void;
    hasUnsavedChanges: boolean;
    isRunningAnalysis: boolean;
    layoutSettings: LayoutSettings;
    onUpdateLayoutSettings: (updates: Partial<LayoutSettings>) => void;
    onResetLayoutSettings: () => void;
}

export function NetworkToolbar({
    onNew,
    onOpen,
    onSave,
    onDelete,
    onResetWorkspace,
    onRunAnalysis,
    onLoadAllocation,
    onShortCircuit,
    onHostingCapacity,
    onThermalModeling,
    onTimeSeries,
    onResetLayout,
    onAutoLayout,
    onApplyLayout,
    onZoomIn,
    onZoomOut,
    onFitView,
    onOpenSettings,
    hasUnsavedChanges,
    isRunningAnalysis,
    layoutSettings,
    onUpdateLayoutSettings,
    onResetLayoutSettings
}: NetworkToolbarProps) {
    return (
        <div className="h-10 border-b flex items-center px-2 gap-1 bg-card shrink-0 select-none overflow-x-auto no-scrollbar">
            <TooltipProvider>
                {/* File Operations */}
                <div className="flex items-center gap-1 shrink-0">
                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={onNew}>
                                <FilePlus className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>New Network</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={onOpen}>
                                <FolderOpen className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Open Network</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button
                                variant="ghost"
                                size="icon"
                                className={`h-8 w-8 ${hasUnsavedChanges ? "text-amber-500" : ""}`}
                                onClick={onSave}
                            >
                                <Save className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Save Network</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={onResetWorkspace}>
                                <RotateCcw className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Reset Workspace</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button variant="ghost" size="icon" className="h-8 w-8 hover:text-destructive" onClick={onDelete}>
                                <Trash2 className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Delete Network</TooltipContent>
                    </Tooltip>
                </div>

                <Separator orientation="vertical" className="h-6 mx-1" />

                {/* View Operations */}
                <div className="flex items-center gap-1 shrink-0">
                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={onZoomIn}>
                                <ZoomIn className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Zoom In</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={onZoomOut}>
                                <ZoomOut className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Zoom Out</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={onFitView}>
                                <Search className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Fit View</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={onResetLayout}>
                                <Grid3X3 className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Reset Layout</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <DropdownMenu>
                            <DropdownMenuTrigger asChild>
                                <TooltipTrigger asChild>
                                    <Button variant="ghost" size="icon" className="h-8 w-8">
                                        <LayoutGrid className="h-4 w-4" />
                                    </Button>
                                </TooltipTrigger>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="start">
                                <DropdownMenuLabel>Auto Layout</DropdownMenuLabel>
                                <DropdownMenuSeparator />
                                <DropdownMenuItem onClick={() => onAutoLayout("radial")}>Radial</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("cluster")}>Cluster (D3)</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("tree")}>Tree (D3)</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("hierarchy")}>Hierarchy</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("stratify")}>Stratify</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("partition")}>Partition</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("pack")}>Pack</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("treemap")}>Treemap</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("layered")}>Layered</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("grid")}>Grid</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("circular")}>Circular</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("concentric")}>Concentric</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("spiral")}>Spiral</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => onAutoLayout("force")}>Force-Directed</DropdownMenuItem>
                            </DropdownMenuContent>
                            <TooltipContent>Auto Layout</TooltipContent>
                        </DropdownMenu>
                    </Tooltip>

                    <Popover>
                        <PopoverTrigger asChild>
                            <Button variant="ghost" size="icon" className="h-8 w-8" aria-label="Layout settings">
                                <SlidersHorizontal className="h-4 w-4" />
                            </Button>
                        </PopoverTrigger>
                        <PopoverContent align="start" className="w-72 p-3">
                            <div className="space-y-3">
                                <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                                    Layout Settings
                                </div>

                                <div className="grid grid-cols-2 gap-2 text-xs">
                                    <label className="flex flex-col gap-1">
                                        Component Spacing
                                        <Input
                                            type="number"
                                            value={layoutSettings.componentSpacing}
                                            onChange={(e) => onUpdateLayoutSettings({ componentSpacing: Number(e.target.value) })}
                                            className="h-8 text-xs"
                                        />
                                    </label>
                                    <label className="flex flex-col gap-1">
                                        Ring Spacing
                                        <Input
                                            type="number"
                                            value={layoutSettings.ringSpacing}
                                            onChange={(e) => onUpdateLayoutSettings({ ringSpacing: Number(e.target.value) })}
                                            className="h-8 text-xs"
                                        />
                                    </label>
                                    <label className="flex flex-col gap-1">
                                        Node Spacing
                                        <Input
                                            type="number"
                                            value={layoutSettings.nodeSpacing}
                                            onChange={(e) => onUpdateLayoutSettings({ nodeSpacing: Number(e.target.value) })}
                                            className="h-8 text-xs"
                                        />
                                    </label>
                                    <label className="flex flex-col gap-1">
                                        Level Spacing
                                        <Input
                                            type="number"
                                            value={layoutSettings.levelSpacing}
                                            onChange={(e) => onUpdateLayoutSettings({ levelSpacing: Number(e.target.value) })}
                                            className="h-8 text-xs"
                                        />
                                    </label>
                                    <label className="flex flex-col gap-1">
                                        Grid Spacing
                                        <Input
                                            type="number"
                                            value={layoutSettings.gridSpacing}
                                            onChange={(e) => onUpdateLayoutSettings({ gridSpacing: Number(e.target.value) })}
                                            className="h-8 text-xs"
                                        />
                                    </label>
                                    <label className="flex flex-col gap-1">
                                        Circular Radius
                                        <Input
                                            type="number"
                                            value={layoutSettings.circularRadius}
                                            onChange={(e) => onUpdateLayoutSettings({ circularRadius: Number(e.target.value) })}
                                            className="h-8 text-xs"
                                        />
                                    </label>
                                    <label className="flex flex-col gap-1">
                                        Spiral Step
                                        <Input
                                            type="number"
                                            value={layoutSettings.spiralStep}
                                            onChange={(e) => onUpdateLayoutSettings({ spiralStep: Number(e.target.value) })}
                                            className="h-8 text-xs"
                                        />
                                    </label>
                                    <label className="flex flex-col gap-1">
                                        Force Charge
                                        <Input
                                            type="number"
                                            value={layoutSettings.forceCharge}
                                            onChange={(e) => onUpdateLayoutSettings({ forceCharge: Number(e.target.value) })}
                                            className="h-8 text-xs"
                                        />
                                    </label>
                                    <label className="flex flex-col gap-1">
                                        Link Distance
                                        <Input
                                            type="number"
                                            value={layoutSettings.forceLinkDistance}
                                            onChange={(e) => onUpdateLayoutSettings({ forceLinkDistance: Number(e.target.value) })}
                                            className="h-8 text-xs"
                                        />
                                    </label>
                                    <label className="flex flex-col gap-1">
                                        Collide Radius
                                        <Input
                                            type="number"
                                            value={layoutSettings.forceCollide}
                                            onChange={(e) => onUpdateLayoutSettings({ forceCollide: Number(e.target.value) })}
                                            className="h-8 text-xs"
                                        />
                                    </label>
                                    <label className="flex flex-col gap-1">
                                        Force Iterations
                                        <Input
                                            type="number"
                                            value={layoutSettings.forceIterations}
                                            onChange={(e) => onUpdateLayoutSettings({ forceIterations: Number(e.target.value) })}
                                            className="h-8 text-xs"
                                        />
                                    </label>
                                </div>

                                <div className="flex items-center gap-2">
                                    <Button size="sm" onClick={onApplyLayout}>
                                        Apply Layout
                                    </Button>
                                    <Button size="sm" variant="outline" onClick={onResetLayoutSettings}>
                                        Reset
                                    </Button>
                                </div>
                            </div>
                        </PopoverContent>
                    </Popover>
                </div>

                <Separator orientation="vertical" className="h-6 mx-1" />

                {/* Analysis Operations */}
                <div className="flex items-center gap-1 shrink-0">
                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8"
                                onClick={onLoadAllocation}
                            >
                                <PieChart className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Load Allocation</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button
                                variant="ghost"
                                size="sm"
                                className="h-8 gap-2 px-2 text-primary font-medium bg-primary/10 hover:bg-primary/20"
                                onClick={onRunAnalysis}
                                disabled={isRunningAnalysis}
                            >
                                {isRunningAnalysis ? (
                                    <RotateCcw className="h-4 w-4 animate-spin" />
                                ) : (
                                    <Play className="h-4 w-4 fill-current" />
                                )}

                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Run Load Flow Analysis</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8"
                                onClick={onShortCircuit}
                            >
                                <ZapOff className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Short Circuit Duty</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8"
                                onClick={onHostingCapacity}
                            >
                                <TrendingUp className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Hosting Capacity</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8"
                                onClick={onThermalModeling}
                            >
                                <Thermometer className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Thermal Modeling</TooltipContent>
                    </Tooltip>

                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8"
                                onClick={onTimeSeries}
                            >
                                <LineChart className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Time Series Analysis</TooltipContent>
                    </Tooltip>
                </div>

                <Separator orientation="vertical" className="h-6 mx-1" />

                {/* Settings */}
                <div className="flex items-center gap-1 shrink-0 ml-auto">
                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={onOpenSettings}>
                                <Settings className="h-4 w-4" />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Settings</TooltipContent>
                    </Tooltip>
                </div>
            </TooltipProvider>
        </div>
    );
}
