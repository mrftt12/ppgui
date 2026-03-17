import { memo, type CSSProperties } from 'react';
import {
    BaseEdge,
    EdgeLabelRenderer,
    getStraightPath,
    type EdgeProps,
} from 'reactflow';

type FlowingEdgeData = {
    flowing?: boolean;
    flowColor?: string;
};

function normalizeTextStyle(style?: CSSProperties): CSSProperties | undefined {
    if (!style) return undefined;
    const next: CSSProperties = { ...style };
    if (typeof next.fill === 'string' && !next.color) {
        next.color = next.fill;
        delete (next as { fill?: string }).fill;
    }
    return next;
}

function buildLabelContainerStyle(
    labelBgStyle?: CSSProperties,
    labelBgPadding?: [number, number] | number,
    labelBgBorderRadius?: number
): CSSProperties {
    const padding = Array.isArray(labelBgPadding)
        ? labelBgPadding
        : [labelBgPadding ?? 0, labelBgPadding ?? 0];
    const [paddingX, paddingY] = padding;
    const backgroundColor =
        labelBgStyle && typeof (labelBgStyle as { fill?: string }).fill === 'string'
            ? (labelBgStyle as { fill?: string }).fill
            : undefined;
    const borderColor =
        labelBgStyle && typeof (labelBgStyle as { stroke?: string }).stroke === 'string'
            ? (labelBgStyle as { stroke?: string }).stroke
            : undefined;
    const borderWidth =
        labelBgStyle && typeof (labelBgStyle as { strokeWidth?: number }).strokeWidth === 'number'
            ? (labelBgStyle as { strokeWidth?: number }).strokeWidth
            : undefined;

    return {
        padding: `${paddingY}px ${paddingX}px`,
        borderRadius: labelBgBorderRadius ?? 0,
        backgroundColor,
        borderColor,
        borderWidth,
        borderStyle: borderColor ? 'solid' : undefined,
        whiteSpace: 'nowrap',
    };
}

export const FlowingEdge = memo(
    ({
        id,
        sourceX,
        sourceY,
        targetX,
        targetY,
        markerEnd,
        markerStart,
        style,
        label,
        labelStyle,
        labelBgStyle,
        labelBgPadding,
        labelBgBorderRadius,
        data,
    }: EdgeProps<FlowingEdgeData>) => {
        const [edgePath, labelX, labelY] = getStraightPath({
            sourceX,
            sourceY,
            targetX,
            targetY,
        });

        const flowColor = data?.flowColor ?? 'hsl(var(--chart-2))';
        const textStyle = normalizeTextStyle(labelStyle as CSSProperties | undefined);
        const containerStyle = buildLabelContainerStyle(
            labelBgStyle as CSSProperties | undefined,
            labelBgPadding as [number, number] | number | undefined,
            labelBgBorderRadius
        );

        return (
            <>
                <BaseEdge
                    id={id}
                    path={edgePath}
                    markerEnd={markerEnd}
                    markerStart={markerStart}
                    style={style}
                />
                {data?.flowing && (
                    <>
                        <path
                            className="rf-edge-flow__glow"
                            d={edgePath}
                            markerEnd={markerEnd}
                            markerStart={markerStart}
                            style={{ stroke: flowColor }}
                        />
                        <path
                            className="rf-edge-flow__animated"
                            d={edgePath}
                            markerEnd={markerEnd}
                            markerStart={markerStart}
                            style={{ stroke: flowColor }}
                        />
                    </>
                )}
                {label && (
                    <EdgeLabelRenderer>
                        <div
                            className="nodrag nopan"
                            style={{
                                position: 'absolute',
                                transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
                                pointerEvents: 'none',
                                ...containerStyle,
                            }}
                        >
                            <span style={textStyle}>{label}</span>
                        </div>
                    </EdgeLabelRenderer>
                )}
            </>
        );
    }
);
