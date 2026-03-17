import { memo } from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import type { NetworkElement } from '@shared/schema';

// Shared styles for all nodes
export const handleStyle = {
    width: 10,
    height: 10,
    borderRadius: '50%',
    border: '2px solid hsl(var(--background))',
};

export const inputHandleStyle = {
    ...handleStyle,
    background: '#3b82f6', // blue
};

export const outputHandleStyle = {
    ...handleStyle,
    background: '#10b981', // green
};

interface PowerNodeData {
    element: NetworkElement;
    isSelected: boolean;
    showLabels?: boolean;
}

// BUS NODE - Horizontal bar with multiple connection points
export const BusNode = memo(({ data, selected }: NodeProps<PowerNodeData>) => {
    const element = data.element;
    const showLabels = data.showLabels ?? true;
    const busHandleStyle = {
        ...handleStyle,
        width: 8,
        height: 8,
        borderRadius: '50%',
    };

    return (
        <div className="relative flex flex-col items-center">
            <Handle
                type="target"
                position={Position.Left}
                id="bus-in"
                style={{ ...inputHandleStyle, ...busHandleStyle }}
            />
            <div
                className={`h-3 w-3 rounded-full transition-all ${selected ? 'ring-2 ring-primary ring-offset-2 ring-offset-background' : ''}`}
                style={{
                    background: 'hsl(var(--primary))',
                }}
            />
            <Handle
                type="source"
                position={Position.Right}
                id="bus-out"
                style={{ ...outputHandleStyle, ...busHandleStyle }}
            />
            {showLabels && (
                <div className="absolute -bottom-5 left-1/2 -translate-x-1/2 text-[10px] font-medium text-foreground/80 bg-background/80 px-1 py-0.5 rounded whitespace-nowrap">
                    {element.name}
                </div>
            )}
        </div>
    );
});

BusNode.displayName = 'BusNode';

// GENERATOR NODE - Circle with output handle
export const GeneratorNode = memo(({ data, selected }: NodeProps<PowerNodeData>) => {
    const element = data.element;
    const showLabels = data.showLabels ?? true;

    return (
        <div className="relative flex flex-col items-center">
            <Handle
                type="source"
                position={Position.Right}
                id="output"
                style={outputHandleStyle}
            />

            <div
                className={`w-14 h-14 rounded-full border-2 flex items-center justify-center transition-all ${selected ? 'ring-2 ring-primary ring-offset-2 ring-offset-background' : ''
                    }`}
                style={{
                    borderColor: '#10b981',
                    background: 'hsl(var(--card))',
                }}
            >
                <span className="text-lg font-bold text-emerald-500">G</span>
            </div>

            {showLabels && (
                <div className="mt-1 text-xs font-medium text-foreground/80 bg-background/80 px-1.5 py-0.5 rounded whitespace-nowrap">
                    {element.name}
                </div>
            )}
        </div>
    );
});

GeneratorNode.displayName = 'GeneratorNode';

// LOAD NODE - Triangle/arrow pointing down with input handle
export const LoadNode = memo(({ data, selected }: NodeProps<PowerNodeData>) => {
    const element = data.element;
    const showLabels = data.showLabels ?? true;

    return (
        <div className="relative flex flex-col items-center">
            <Handle
                type="target"
                position={Position.Left}
                id="input"
                style={inputHandleStyle}
            />

            <div
                className={`flex items-center justify-center transition-all ${selected ? 'ring-2 ring-primary ring-offset-2 ring-offset-background rounded' : ''
                    }`}
            >
                <svg width="48" height="48" viewBox="0 0 48 48">
                    <polygon
                        points="24,4 44,40 4,40"
                        fill="hsl(var(--card))"
                        stroke="#f97316"
                        strokeWidth="2"
                    />
                    <text x="24" y="32" textAnchor="middle" fill="#f97316" fontSize="14" fontWeight="bold">
                        L
                    </text>
                </svg>
            </div>

            {showLabels && (
                <div className="mt-1 text-xs font-medium text-foreground/80 bg-background/80 px-1.5 py-0.5 rounded whitespace-nowrap">
                    {element.name}
                </div>
            )}
        </div>
    );
});

LoadNode.displayName = 'LoadNode';

// TRANSFORMER NODE - Two overlapping circles
export const TransformerNode = memo(({ data, selected }: NodeProps<PowerNodeData>) => {
    const element = data.element;
    const showLabels = data.showLabels ?? true;

    return (
        <div className="relative flex flex-col items-center">
            <Handle
                type="target"
                position={Position.Left}
                id="primary"
                style={inputHandleStyle}
            />
            <Handle
                type="source"
                position={Position.Right}
                id="secondary"
                style={outputHandleStyle}
            />

            <div
                className={`relative transition-all ${selected ? 'ring-2 ring-primary ring-offset-2 ring-offset-background rounded-full' : ''
                    }`}
            >
                <svg width="56" height="40" viewBox="0 0 56 40">
                    {/* Primary winding */}
                    <circle cx="18" cy="20" r="14" fill="hsl(var(--card))" stroke="#3b82f6" strokeWidth="2" />
                    {/* Secondary winding */}
                    <circle cx="38" cy="20" r="14" fill="hsl(var(--card))" stroke="#3b82f6" strokeWidth="2" />
                    <text x="28" y="25" textAnchor="middle" fill="#3b82f6" fontSize="12" fontWeight="bold">
                        T
                    </text>
                </svg>
            </div>

            {showLabels && (
                <div className="mt-1 text-xs font-medium text-foreground/80 bg-background/80 px-1.5 py-0.5 rounded whitespace-nowrap">
                    {element.name}
                </div>
            )}
        </div>
    );
});

TransformerNode.displayName = 'TransformerNode';

// EXTERNAL SOURCE NODE - Grid/Source symbol
export const ExternalSourceNode = memo(({ data, selected }: NodeProps<PowerNodeData>) => {
    const element = data.element;
    const showLabels = data.showLabels ?? true;

    return (
        <div className="relative flex flex-col items-center">
            <Handle
                type="source"
                position={Position.Bottom}
                id="output"
                style={outputHandleStyle}
            />

            <div
                className={`w-14 h-14 border-2 flex items-center justify-center transition-all ${selected ? 'ring-2 ring-primary ring-offset-2 ring-offset-background' : ''
                    }`}
                style={{
                    borderColor: '#8b5cf6',
                    background: 'hsl(var(--card))',
                }}
            >
                <svg width="32" height="32" viewBox="0 0 32 32">
                    <line x1="4" y1="8" x2="28" y2="8" stroke="#8b5cf6" strokeWidth="2" />
                    <line x1="4" y1="16" x2="28" y2="16" stroke="#8b5cf6" strokeWidth="2" />
                    <line x1="4" y1="24" x2="28" y2="24" stroke="#8b5cf6" strokeWidth="2" />
                    <line x1="8" y1="4" x2="8" y2="28" stroke="#8b5cf6" strokeWidth="2" />
                    <line x1="16" y1="4" x2="16" y2="28" stroke="#8b5cf6" strokeWidth="2" />
                    <line x1="24" y1="4" x2="24" y2="28" stroke="#8b5cf6" strokeWidth="2" />
                </svg>
            </div>

            {showLabels && (
                <div className="mt-1 text-xs font-medium text-foreground/80 bg-background/80 px-1.5 py-0.5 rounded whitespace-nowrap">
                    {element.name}
                </div>
            )}
        </div>
    );
});

ExternalSourceNode.displayName = 'ExternalSourceNode';

// BATTERY NODE
export const BatteryNode = memo(({ data, selected }: NodeProps<PowerNodeData>) => {
    const element = data.element;
    const showLabels = data.showLabels ?? true;

    return (
        <div className="relative flex flex-col items-center">
            <Handle type="target" position={Position.Left} id="input" style={inputHandleStyle} />
            <Handle type="source" position={Position.Right} id="output" style={outputHandleStyle} />

            <div
                className={`w-12 h-8 border-2 rounded flex items-center justify-center transition-all ${selected ? 'ring-2 ring-primary ring-offset-2 ring-offset-background' : ''
                    }`}
                style={{
                    borderColor: '#06b6d4',
                    background: 'hsl(var(--card))',
                }}
            >
                <div className="flex items-center gap-0.5">
                    <div className="w-1 h-4 bg-cyan-500" />
                    <div className="w-2 h-6 bg-cyan-500" />
                </div>
            </div>

            {showLabels && (
                <div className="mt-1 text-xs font-medium text-foreground/80 bg-background/80 px-1.5 py-0.5 rounded whitespace-nowrap">
                    {element.name}
                </div>
            )}
        </div>
    );
});

BatteryNode.displayName = 'BatteryNode';

// CAPACITOR NODE
export const CapacitorNode = memo(({ data, selected }: NodeProps<PowerNodeData>) => {
    const element = data.element;
    const showLabels = data.showLabels ?? true;

    return (
        <div className="relative flex flex-col items-center">
            <Handle type="target" position={Position.Left} id="input" style={inputHandleStyle} />

            <div
                className={`flex items-center justify-center transition-all ${selected ? 'ring-2 ring-primary ring-offset-2 ring-offset-background rounded' : ''
                    }`}
            >
                <svg width="40" height="40" viewBox="0 0 40 40">
                    <line x1="8" y1="20" x2="16" y2="20" stroke="#eab308" strokeWidth="2" />
                    <line x1="16" y1="8" x2="16" y2="32" stroke="#eab308" strokeWidth="3" />
                    <line x1="24" y1="8" x2="24" y2="32" stroke="#eab308" strokeWidth="3" />
                    <line x1="24" y1="20" x2="32" y2="20" stroke="#eab308" strokeWidth="2" />
                </svg>
            </div>

            {showLabels && (
                <div className="mt-1 text-xs font-medium text-foreground/80 bg-background/80 px-1.5 py-0.5 rounded whitespace-nowrap">
                    {element.name}
                </div>
            )}
        </div>
    );
});

CapacitorNode.displayName = 'CapacitorNode';

// SWITCH NODE
export const SwitchNode = memo(({ data, selected }: NodeProps<PowerNodeData>) => {
    const element = data.element;
    const showLabels = data.showLabels ?? true;
    const isClosed = (element as { isClosed?: boolean }).isClosed ?? true;
    const phaseColor = (element as { phaseColor?: string }).phaseColor || '#10b981';
    const openColor = '#ef4444';

    return (
        <div className="relative flex flex-col items-center">
            <Handle type="target" position={Position.Left} id="input" style={inputHandleStyle} />
            <Handle type="source" position={Position.Right} id="output" style={outputHandleStyle} />

            <div
                className={`flex items-center justify-center transition-all ${selected ? 'ring-2 ring-primary ring-offset-2 ring-offset-background rounded' : ''
                    }`}
            >
                <svg width="48" height="32" viewBox="0 0 48 32">
                    <circle cx="8" cy="16" r="4" fill={isClosed ? phaseColor : openColor} />
                    <circle cx="40" cy="16" r="4" fill={isClosed ? phaseColor : openColor} />
                    {isClosed ? (
                        <line x1="12" y1="16" x2="36" y2="16" stroke={phaseColor} strokeWidth="2" />
                    ) : (
                        <line x1="12" y1="16" x2="32" y2="6" stroke={openColor} strokeWidth="2" />
                    )}
                </svg>
            </div>

            {showLabels && (
                <div className="mt-1 text-xs font-medium text-foreground/80 bg-background/80 px-1.5 py-0.5 rounded whitespace-nowrap">
                    {element.name}
                </div>
            )}
        </div>
    );
});

SwitchNode.displayName = 'SwitchNode';

// DUCTBANK NODE
export const DuctbankNode = memo(({ data, selected }: NodeProps<PowerNodeData>) => {
    const element = data.element;
    const showLabels = data.showLabels ?? true;
    const rows = Math.max(1, Math.min(8, Number((element as { rows?: number }).rows ?? 2)));
    const columns = Math.max(1, Math.min(8, Number((element as { columns?: number }).columns ?? 2)));
    const radius = 5;
    const spacing = 14;
    const innerPadding = 6;
    const outerPadding = 6;
    const gridWidth = (columns - 1) * spacing + radius * 2;
    const gridHeight = (rows - 1) * spacing + radius * 2;
    const rectWidth = gridWidth + innerPadding * 2;
    const rectHeight = gridHeight + innerPadding * 2;
    const svgWidth = rectWidth + outerPadding * 2;
    const svgHeight = rectHeight + outerPadding * 2;
    const rectX = outerPadding;
    const rectY = outerPadding;
    const startX = rectX + innerPadding + radius;
    const startY = rectY + innerPadding + radius;
    const ducts = Array.from({ length: rows * columns }, (_, index) => {
        const row = Math.floor(index / columns);
        const col = index % columns;
        return {
            key: `${row}-${col}`,
            cx: startX + col * spacing,
            cy: startY + row * spacing,
        };
    });

    return (
        <div className="relative flex flex-col items-center">
            <Handle type="target" position={Position.Left} id="input" style={inputHandleStyle} />
            <Handle type="source" position={Position.Right} id="output" style={outputHandleStyle} />

            <div
                className={`flex items-center justify-center transition-all ${selected ? 'ring-2 ring-primary ring-offset-2 ring-offset-background rounded' : ''}`}
            >
                <svg width={svgWidth} height={svgHeight} viewBox={`0 0 ${svgWidth} ${svgHeight}`}>
                    <rect
                        x={rectX}
                        y={rectY}
                        width={rectWidth}
                        height={rectHeight}
                        rx="3"
                        fill="hsl(var(--card))"
                        stroke="#d97706"
                        strokeWidth="2"
                    />
                    {ducts.map((duct) => (
                        <circle
                            key={duct.key}
                            cx={duct.cx}
                            cy={duct.cy}
                            r={radius}
                            fill="hsl(var(--card))"
                            stroke="hsl(var(--foreground))"
                            strokeWidth="2"
                        />
                    ))}
                </svg>
            </div>

            {showLabels && (
                <div className="mt-1 text-xs font-medium text-foreground/80 bg-background/80 px-1.5 py-0.5 rounded whitespace-nowrap">
                    {element.name}
                </div>
            )}
        </div>
    );
});

DuctbankNode.displayName = 'DuctbankNode';

// Export node types mapping
export const nodeTypes = {
    bus: BusNode,
    generator: GeneratorNode,
    load: LoadNode,
    transformer: TransformerNode,
    external_source: ExternalSourceNode,
    battery: BatteryNode,
    capacitor: CapacitorNode,
    switch: SwitchNode,
    ductbank: DuctbankNode,
};
