import { useCallback, useEffect, useMemo, DragEvent } from 'react';
import ReactFlow, {
    Node,
    Edge,
    Connection as RFConnection,
    Controls,
    Background,
    BackgroundVariant,
    useNodesState,
    useEdgesState,
    NodeChange,
    EdgeChange,
    ReactFlowProvider,
    useReactFlow,
    Panel,
} from 'reactflow';
import 'reactflow/dist/style.css';
import type { NetworkElement, ElementType, Connection as ModelConnection } from '@shared/schema';
import { nodeTypes } from './nodes/PowerNodes';
import { FlowingEdge } from './edges/FlowingEdge';
import { Button } from '@/components/ui/button';
import { Trash2 } from 'lucide-react';

interface ReactFlowCanvasProps {
    elements: NetworkElement[];
    connections: ModelConnection[];
    selectedElementId: string | null;
    selectedElementIds: string[];
    onSelectElement: (id: string | null) => void;
    onSelectElements: (ids: string[], primaryId: string | null) => void;
    onUpdateElement: (id: string, updates: Partial<NetworkElement>) => void;
    onUpdateElementsPositions?: (positions: Record<string, { x: number; y: number }>) => void;
    onDeleteElement: (id: string) => void;
    onCreateLine: (fromId: string, toId: string) => void;
    onDropElement?: (type: ElementType, x: number, y: number) => void;
    isFlowing?: boolean;
    onOpenUndergroundLine?: (lineId: string) => void;
    showLabels?: boolean;
}

// Convert NetworkElements to React Flow nodes
function elementsToNodes(elements: NetworkElement[], showLabels: boolean): Node[] {
    return elements
        .filter(el => el.type !== 'line' && el.type !== 'cable')
        .map(element => ({
            id: element.id,
            type: element.type,
            position: { x: element.x, y: element.y },
            data: {
                element,
                isSelected: false,
                showLabels,
            },
            selected: false,
        }));
}

// Convert NetworkElements (lines/cables) to React Flow edges
function elementsToEdges(
    elements: NetworkElement[],
    connections: ModelConnection[],
    isFlowing: boolean,
    showLabels: boolean
): Edge[] {
    const lineEdges = elements
        .filter(el => el.type === 'line' || el.type === 'cable')
        .filter(el => {
            const fromId = (el as { fromElementId?: string }).fromElementId;
            const toId = (el as { toElementId?: string }).toElementId;
            return fromId && toId;
        })
        .map(lineEl => {
            const fromId = (lineEl as { fromElementId?: string }).fromElementId!;
            const toId = (lineEl as { toElementId?: string }).toElementId!;
            const shouldFlow = isFlowing || lineEl.type === 'cable';
            const flowColor = isFlowing
                ? 'hsl(var(--chart-2))'
                : lineEl.type === 'cable'
                  ? '#06b6d4'
                  : '#10b981';

            return {
                id: lineEl.id,
                source: fromId,
                target: toId,
                type: 'flowing',
                style: {
                    stroke: lineEl.type === 'cable' ? '#06b6d4' : '#10b981',
                    strokeWidth: 3,
                },
                label: showLabels ? lineEl.name : undefined,
                labelStyle: {
                    fill: 'hsl(var(--foreground))',
                    fontSize: 11,
                    fontWeight: 500,
                },
                labelBgStyle: {
                    fill: 'hsl(var(--card))',
                    stroke: 'hsl(var(--border))',
                    strokeWidth: 1,
                },
                labelBgPadding: [6, 4] as [number, number],
                labelBgBorderRadius: 4,
                data: {
                    flowing: shouldFlow,
                    flowColor,
                    elementType: lineEl.type,
                },
            };
        });

    const nodeElements = elements.filter(el => el.type !== 'line' && el.type !== 'cable');
    const nodeIds = new Set(nodeElements.map(el => el.id));
    const nodeTypes = new Map(nodeElements.map(el => [el.id, el.type]));
    const targetOnly = new Set<ElementType>(['load', 'capacitor']);
    const sourceOnly = new Set<ElementType>(['generator', 'external_source']);

    const connectionEdges = connections
        .filter(conn => nodeIds.has(conn.fromElementId) && nodeIds.has(conn.toElementId))
        .map(conn => {
            let source = conn.fromElementId;
            let target = conn.toElementId;
            const sourceType = nodeTypes.get(source);
            const targetType = nodeTypes.get(target);
            if (sourceType && targetType) {
                if (targetOnly.has(sourceType) || sourceOnly.has(targetType)) {
                    [source, target] = [target, source];
                }
            }

            return {
                id: conn.id,
                source,
                target,
            type: 'straight',
            animated: false,
            style: {
                stroke: 'hsl(var(--muted-foreground))',
                strokeWidth: 1.2,
                strokeDasharray: '4 4',
            },
        };
    });

    return [...lineEdges, ...connectionEdges];
}

function ReactFlowCanvasInner({
    elements,
    connections,
    selectedElementId,
    selectedElementIds,
    onSelectElement,
    onSelectElements,
    onUpdateElement,
    onUpdateElementsPositions,
    onDeleteElement,
    onCreateLine,
    onDropElement,
    isFlowing = false,
    onOpenUndergroundLine,
    showLabels = false,
}: ReactFlowCanvasProps) {
    const reactFlowInstance = useReactFlow();

    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);
    const edgeTypes = useMemo(() => ({ flowing: FlowingEdge }), []);

    // Sync nodes when elements change - use useEffect instead of useMemo
    useEffect(() => {
        setNodes(elementsToNodes(elements, showLabels));
    }, [elements, setNodes, showLabels]);

    useEffect(() => {
        setEdges(elementsToEdges(elements, connections, isFlowing, showLabels));
    }, [elements, connections, isFlowing, setEdges, showLabels]);


    useEffect(() => {
        const selectedSet = new Set(selectedElementIds);
        setNodes((prev) =>
            prev.map((node) => {
                const isSelected = selectedSet.has(node.id);
                if (node.selected === isSelected && node.data?.isSelected === isSelected) return node;
                return {
                    ...node,
                    selected: isSelected,
                    data: { ...node.data, isSelected },
                };
            })
        );
        setEdges((prev) =>
            prev.map((edge) => {
                const isSelected = selectedSet.has(edge.id);
                return edge.selected === isSelected ? edge : { ...edge, selected: isSelected };
            })
        );
    }, [selectedElementIds, setNodes, setEdges]);

    // Handle node position changes
    const handleNodesChange = useCallback(
        (changes: NodeChange[]) => {
            onNodesChange(changes);
        },
        [onNodesChange]
    );

    const handleNodeDragStop = useCallback(
        (_event: React.MouseEvent, _node: Node, nodes: Node[]) => {
            if (!nodes || nodes.length === 0) return;
            const positions: Record<string, { x: number; y: number }> = {};
            nodes.forEach((item) => {
                positions[item.id] = {
                    x: Math.round(item.position.x),
                    y: Math.round(item.position.y),
                };
            });

            if (onUpdateElementsPositions) {
                onUpdateElementsPositions(positions);
                return;
            }

            Object.entries(positions).forEach(([id, pos]) => {
                onUpdateElement(id, pos);
            });
        },
        [onUpdateElement, onUpdateElementsPositions]
    );

    // Handle edge changes
    const handleEdgesChange = useCallback(
        (changes: EdgeChange[]) => {
            onEdgesChange(changes);
        },
        [onEdgesChange]
    );

    // Handle new connections - creates a Line element
    const onConnect = useCallback(
        (connection: RFConnection) => {
            if (connection.source && connection.target) {
                // Create a new Line element with the connection
                onCreateLine(connection.source, connection.target);
            }
        },
        [onCreateLine]
    );

    // Handle node selection
    const onNodeClick = useCallback(
        (event: React.MouseEvent, node: Node) => {
            const isMulti = event.shiftKey || event.metaKey || event.ctrlKey;
            if (isMulti) {
                const next = new Set(selectedElementIds);
                if (next.has(node.id)) {
                    next.delete(node.id);
                } else {
                    next.add(node.id);
                }
                const list = Array.from(next);
                onSelectElements(list, list.length ? node.id : null);
            } else {
                onSelectElements([node.id], node.id);
            }
        },
        [selectedElementIds, onSelectElements]
    );

    // Handle edge selection (clicking on a line)
    const onEdgeClick = useCallback(
        (event: React.MouseEvent, edge: Edge) => {
            const isMulti = event.shiftKey || event.metaKey || event.ctrlKey;
            if (isMulti) {
                const next = new Set(selectedElementIds);
                if (next.has(edge.id)) {
                    next.delete(edge.id);
                } else {
                    next.add(edge.id);
                }
                const list = Array.from(next);
                onSelectElements(list, list.length ? edge.id : null);
            } else {
                onSelectElements([edge.id], edge.id);
            }
        },
        [selectedElementIds, onSelectElements]
    );

    const onEdgeDoubleClick = useCallback(
        (_event: React.MouseEvent, edge: Edge) => {
            const line = elements.find((el) => el.id === edge.id && el.type === "line") as (NetworkElement & { installation?: string }) | undefined;
            if (!line || line.installation !== "underground") return;
            onOpenUndergroundLine?.(line.id);
        },
        [elements, onOpenUndergroundLine]
    );

    // Handle pane click (deselect)
    const onPaneClick = useCallback(() => {
        onSelectElements([], null);
    }, [onSelectElements]);

    // Handle drop from palette
    const onDrop = useCallback(
        (event: DragEvent) => {
            event.preventDefault();

            const type = event.dataTransfer.getData('application/reactflow-type') as ElementType;

            if (!type || !onDropElement) return;

            const position = reactFlowInstance.screenToFlowPosition({
                x: event.clientX,
                y: event.clientY,
            });

            onDropElement(type, Math.round(position.x), Math.round(position.y));
        },
        [reactFlowInstance, onDropElement]
    );

    const onDragOver = useCallback((event: DragEvent) => {
        event.preventDefault();
        event.dataTransfer.dropEffect = 'move';
    }, []);

    // Get selected element for delete button
    const selectedElement = elements.find(el => el.id === selectedElementId);

    return (
        <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={handleNodesChange}
            onEdgesChange={handleEdgesChange}
            onConnect={onConnect}
            onNodeClick={onNodeClick}
            onEdgeClick={onEdgeClick}
            onEdgeDoubleClick={onEdgeDoubleClick}
            onNodeDragStop={handleNodeDragStop}
            onPaneClick={onPaneClick}
            onDrop={onDrop}
            onDragOver={onDragOver}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            selectionOnDrag
            multiSelectionKeyCode="Shift"
            minZoom={0.05}
            snapToGrid
            snapGrid={[10, 10]}
            className="bg-background"
            proOptions={{ hideAttribution: true }}
        >
            <Background variant={BackgroundVariant.Dots} gap={20} size={1} />
            <Controls showInteractive={false} />

            {/* Delete button panel */}
            {selectedElement && (
                <Panel position="top-right" className="flex gap-2">
                    <Button
                        variant="destructive"
                        size="sm"
                        onClick={() => onDeleteElement(selectedElementId!)}
                    >
                        <Trash2 className="h-4 w-4 mr-1" />
                        Delete {selectedElement.name}
                    </Button>
                </Panel>
            )}
        </ReactFlow>
    );
}

// Wrap with ReactFlowProvider
export function ReactFlowCanvas(props: ReactFlowCanvasProps) {
    return (
        <ReactFlowProvider>
            <ReactFlowCanvasInner {...props} />
        </ReactFlowProvider>
    );
}
