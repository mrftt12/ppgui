import React, { useRef, useState, useCallback, useEffect } from "react";
import type { NetworkElement, ElementType, Connection } from "@shared/schema";
import { ElementIcon, elementLabels } from "./ElementIcons";
import { Button } from "@/components/ui/button";
import { useDroppable } from "@dnd-kit/core";
import { ZoomIn, ZoomOut, Maximize2, Trash2, RotateCw } from "lucide-react";

interface NetworkCanvasProps {
  elements: NetworkElement[];
  connections: Connection[];
  selectedElementId: string | null;
  onSelectElement: (id: string | null) => void;
  onUpdateElement: (id: string, updates: Partial<NetworkElement>) => void;
  onDeleteElement: (id: string) => void;
  onAddConnection: (fromId: string, toId: string) => void;
  zoom: number;
  offset: { x: number; y: number };
  onZoomChange: (zoom: number) => void;
  onOffsetChange: (offset: { x: number; y: number }) => void;
}

export function NetworkCanvas({
  elements,
  connections,
  selectedElementId,
  onSelectElement,
  onUpdateElement,
  onDeleteElement,
  onAddConnection,
  zoom,
  offset,
  onZoomChange,
  onOffsetChange,
}: NetworkCanvasProps) {
  const canvasRef = useRef<HTMLDivElement>(null);
  const [isPanning, setIsPanning] = useState(false);
  const [panStart, setPanStart] = useState({ x: 0, y: 0 });
  const [draggedElement, setDraggedElement] = useState<string | null>(null);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const [connectingFrom, setConnectingFrom] = useState<string | null>(null);

  const { setNodeRef, isOver } = useDroppable({
    id: "canvas-drop-zone",
  });

  const handleMouseDown = (e: React.MouseEvent, elementId?: string) => {
    if (elementId) {
      onSelectElement(elementId);
      const element = elements.find(el => el.id === elementId);
      if (element && canvasRef.current) {
        const rect = canvasRef.current.getBoundingClientRect();
        const mouseX = (e.clientX - rect.left - offset.x) / zoom;
        const mouseY = (e.clientY - rect.top - offset.y) / zoom;
        setDragOffset({ x: mouseX - element.x, y: mouseY - element.y });
        setDraggedElement(elementId);
      }
    } else if (e.button === 1 || (e.button === 0 && e.shiftKey)) {
      setIsPanning(true);
      setPanStart({ x: e.clientX - offset.x, y: e.clientY - offset.y });
    } else {
      onSelectElement(null);
    }
  };

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (isPanning) {
      onOffsetChange({
        x: e.clientX - panStart.x,
        y: e.clientY - panStart.y,
      });
    } else if (draggedElement && canvasRef.current) {
      const rect = canvasRef.current.getBoundingClientRect();
      const x = (e.clientX - rect.left - offset.x) / zoom - dragOffset.x;
      const y = (e.clientY - rect.top - offset.y) / zoom - dragOffset.y;
      onUpdateElement(draggedElement, {
        x: Math.round(x / 20) * 20,
        y: Math.round(y / 20) * 20,
      });
    }
  }, [isPanning, panStart, draggedElement, dragOffset, offset, zoom, onUpdateElement, onOffsetChange]);

  const handleMouseUp = () => {
    setIsPanning(false);
    setDraggedElement(null);
  };

  const handleWheel = useCallback((e: React.WheelEvent) => {
    if (e.ctrlKey || e.metaKey) {
      e.preventDefault();
      const delta = e.deltaY > 0 ? 0.9 : 1.1;
      onZoomChange(Math.min(Math.max(zoom * delta, 0.25), 4));
    }
  }, [zoom, onZoomChange]);

  const zoomIn = () => onZoomChange(Math.min(zoom * 1.2, 4));
  const zoomOut = () => onZoomChange(Math.max(zoom * 0.8, 0.25));
  const resetView = () => {
    onZoomChange(1);
    onOffsetChange({ x: 0, y: 0 });
  };

  const handleConnectionStart = (elementId: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setConnectingFrom(elementId);
  };

  const handleConnectionEnd = (elementId: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (connectingFrom && connectingFrom !== elementId) {
      onAddConnection(connectingFrom, elementId);
    }
    setConnectingFrom(null);
  };

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.key === "Delete" || e.key === "Backspace") && selectedElementId) {
        onDeleteElement(selectedElementId);
      }
      if (e.key === "Escape") {
        setConnectingFrom(null);
        onSelectElement(null);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [selectedElementId, onDeleteElement, onSelectElement]);

  // Get element center position based on type
  const getElementCenter = (element: NetworkElement) => {
    if (element.type === "bus") {
      const busWidth = (element as { width?: number }).width || 96;
      return { x: element.x + busWidth / 2, y: element.y + 6 }; // Bus bar center
    }
    return { x: element.x + 40, y: element.y + 30 }; // Default box center
  };

  // Render a line element as a connection between any two elements
  const renderLineElement = (lineElement: NetworkElement) => {
    // Only render lines that have fromElementId and toElementId
    const fromElementId = (lineElement as { fromElementId?: string }).fromElementId;
    const toElementId = (lineElement as { toElementId?: string }).toElementId;

    if (!fromElementId || !toElementId) return null;

    const fromElement = elements.find(e => e.id === fromElementId);
    const toElement = elements.find(e => e.id === toElementId);

    if (!fromElement || !toElement) return null;

    const from = getElementCenter(fromElement);
    const to = getElementCenter(toElement);

    const isSelected = lineElement.id === selectedElementId;
    
    // Use phaseColor if available, otherwise default green
    const lineColor = (lineElement as { phaseColor?: string }).phaseColor || "#10b981";

    // Determine if we need vertical or horizontal connection
    const dx = Math.abs(to.x - from.x);
    const dy = Math.abs(to.y - from.y);
    const isVertical = dy > dx;

    // Calculate midpoint for label
    const midX = (from.x + to.x) / 2;
    const midY = (from.y + to.y) / 2;

    // Create an orthogonal path for cleaner routing
    let pathD: string;
    if (isVertical) {
      // Mostly vertical - go straight down/up
      pathD = `M ${from.x} ${from.y} L ${from.x} ${midY} L ${to.x} ${midY} L ${to.x} ${to.y}`;
    } else {
      // Mostly horizontal - go straight across
      pathD = `M ${from.x} ${from.y} L ${midX} ${from.y} L ${midX} ${to.y} L ${to.x} ${to.y}`;
    }

    return (
      <g
        key={lineElement.id}
        className={`cursor-pointer ${isSelected ? "" : "hover:opacity-80"}`}
        onClick={(e) => {
          e.stopPropagation();
          onSelectElement(lineElement.id);
        }}
      >
        {/* Main line path */}
        <path
          d={pathD}
          stroke={isSelected ? "hsl(var(--primary))" : lineColor}
          strokeWidth={isSelected ? 4 : 2}
          fill="none"
          className="transition-all"
        />
        {/* End circles */}
        <circle cx={from.x} cy={from.y} r="5" fill={lineColor} stroke="white" strokeWidth="2" />
        <circle cx={to.x} cy={to.y} r="5" fill={lineColor} stroke="white" strokeWidth="2" />
        {/* Line label at midpoint */}
        <rect
          x={midX - 25}
          y={midY - 10}
          width="50"
          height="20"
          rx="4"
          fill="hsl(var(--card))"
          stroke={isSelected ? "hsl(var(--primary))" : "hsl(var(--border))"}
          strokeWidth="1"
        />
        <text
          x={midX}
          y={midY + 4}
          textAnchor="middle"
          fontSize="11"
          fill="hsl(var(--foreground))"
          className="select-none pointer-events-none"
        >
          {lineElement.name}
        </text>
      </g>
    );
  };

  // Render connections (generic connections, not line elements)
  const renderConnection = (conn: Connection) => {
    const fromEl = elements.find(e => e.id === conn.fromElementId);
    const toEl = elements.find(e => e.id === conn.toElementId);
    if (!fromEl || !toEl) return null;

    const from = getElementCenter(fromEl);
    const to = getElementCenter(toEl);

    // Simple straight line for generic connections
    return (
      <g key={conn.id}>
        <line
          x1={from.x}
          y1={from.y}
          x2={to.x}
          y2={to.y}
          stroke="hsl(var(--primary))"
          strokeWidth="2"
          strokeDasharray={conn.fromElementId === connectingFrom ? "5,5" : "none"}
        />
        <circle cx={from.x} cy={from.y} r="4" fill="hsl(var(--primary))" />
        <circle cx={to.x} cy={to.y} r="4" fill="hsl(var(--primary))" />
      </g>
    );
  };

  return (
    <div className="relative h-full overflow-hidden bg-muted/30">
      <div className="absolute top-3 right-3 z-20 flex items-center gap-1 bg-card/90 backdrop-blur-sm rounded-md border p-1 shadow-sm">
        <Button size="icon" variant="ghost" onClick={zoomOut} data-testid="button-zoom-out">
          <ZoomOut className="h-4 w-4" />
        </Button>
        <span className="text-xs font-mono w-12 text-center text-muted-foreground">
          {Math.round(zoom * 100)}%
        </span>
        <Button size="icon" variant="ghost" onClick={zoomIn} data-testid="button-zoom-in">
          <ZoomIn className="h-4 w-4" />
        </Button>
        <div className="w-px h-5 bg-border mx-1" />
        <Button size="icon" variant="ghost" onClick={resetView} data-testid="button-reset-view">
          <Maximize2 className="h-4 w-4" />
        </Button>
      </div>

      {selectedElementId && (
        <div className="absolute top-3 left-3 z-20 flex items-center gap-1 bg-card/90 backdrop-blur-sm rounded-md border p-1 shadow-sm">
          <Button
            size="icon"
            variant="ghost"
            onClick={() => {
              const el = elements.find(e => e.id === selectedElementId);
              if (el) {
                onUpdateElement(selectedElementId, { rotation: ((el.rotation || 0) + 90) % 360 });
              }
            }}
            data-testid="button-rotate"
          >
            <RotateCw className="h-4 w-4" />
          </Button>
          <Button
            size="icon"
            variant="ghost"
            onClick={() => onDeleteElement(selectedElementId)}
            className="text-destructive"
            data-testid="button-delete"
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      )}

      <div
        ref={(node) => {
          (canvasRef as React.MutableRefObject<HTMLDivElement | null>).current = node;
          setNodeRef(node);
        }}
        className={`absolute inset-0 canvas-grid cursor-crosshair ${isOver ? "ring-2 ring-inset ring-primary/50" : ""}`}
        onMouseDown={(e) => handleMouseDown(e)}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
        onWheel={handleWheel}
        style={{
          backgroundPosition: `${offset.x}px ${offset.y}px`,
          backgroundSize: `${20 * zoom}px ${20 * zoom}px`,
        }}
        data-testid="network-canvas"
      >
        <svg
          className="absolute inset-0"
          style={{
            transform: `translate(${offset.x}px, ${offset.y}px) scale(${zoom})`,
            transformOrigin: "0 0",
            pointerEvents: "none",
          }}
        >
          <g style={{ pointerEvents: "auto" }}>
            {/* Render connections */}
            {connections.map(renderConnection)}
            {/* Render lines that have element connections */}
            {elements
              .filter(el => (el.type === "line" || el.type === "cable") &&
                (el as { fromElementId?: string }).fromElementId &&
                (el as { toElementId?: string }).toElementId)
              .map(renderLineElement)}
          </g>
        </svg>

        <div
          className="absolute inset-0"
          style={{
            transform: `translate(${offset.x}px, ${offset.y}px) scale(${zoom})`,
            transformOrigin: "0 0",
          }}
        >
          {elements.map((element) => {
            const isSelected = element.id === selectedElementId;
            const isConnecting = connectingFrom === element.id;
            // Render buses as horizontal bars with dynamic width and multiple ports
            if (element.type === "bus") {
              const busWidth = (element as { width?: number }).width || 96;
              // Calculate number of ports based on width (1 port per ~40px, minimum 3)
              const numPorts = Math.max(3, Math.floor(busWidth / 40) + 1);
              const portSpacing = busWidth / (numPorts - 1);

              return (
                <div
                  key={element.id}
                  className={`absolute flex flex-col items-center cursor-move select-none ${isSelected ? "z-10" : ""}`}
                  style={{
                    left: element.x,
                    top: element.y,
                  }}
                  onMouseDown={(e) => handleMouseDown(e, element.id)}
                  data-testid={`canvas-element-${element.id}`}
                >
                  {/* Bus bar - horizontal thick line with dynamic width */}
                  <div
                    className={`relative h-3 rounded-sm ${isSelected ? "ring-2 ring-primary ring-offset-2 ring-offset-background" : ""} ${isConnecting ? "animate-pulse" : ""}`}
                    style={{
                      width: busWidth,
                      backgroundColor: "hsl(var(--primary))"
                    }}
                  >
                    {/* Connection ports distributed along the bus */}
                    {Array.from({ length: numPorts }).map((_, i) => {
                      const portX = i * portSpacing;
                      return (
                        <button
                          key={`port-${i}`}
                          className="absolute top-1/2 -translate-y-1/2 w-3 h-3 rounded-full bg-sky-400 border-2 border-background hover:scale-125 transition-transform z-10"
                          style={{ left: portX - 6 }}
                          onMouseDown={(e) => {
                            e.stopPropagation();
                            handleConnectionStart(element.id, e);
                          }}
                          onMouseUp={(e) => handleConnectionEnd(element.id, e)}
                          data-testid={`connection-port-${element.id}-${i}`}
                        />
                      );
                    })}
                    {/* Top and bottom ports for vertical connections */}
                    {Array.from({ length: Math.max(2, Math.floor(numPorts / 2)) }).map((_, i) => {
                      const portX = (i + 1) * (busWidth / (Math.floor(numPorts / 2) + 1));
                      return (
                        <React.Fragment key={`tb-${i}`}>
                          <button
                            className="absolute -top-1.5 w-2.5 h-2.5 rounded-full bg-sky-400 border-2 border-background hover:scale-125 transition-transform"
                            style={{ left: portX - 5 }}
                            onMouseDown={(e) => {
                              e.stopPropagation();
                              handleConnectionStart(element.id, e);
                            }}
                            onMouseUp={(e) => handleConnectionEnd(element.id, e)}
                          />
                          <button
                            className="absolute -bottom-1.5 w-2.5 h-2.5 rounded-full bg-sky-400 border-2 border-background hover:scale-125 transition-transform"
                            style={{ left: portX - 5 }}
                            onMouseDown={(e) => {
                              e.stopPropagation();
                              handleConnectionStart(element.id, e);
                            }}
                            onMouseUp={(e) => handleConnectionEnd(element.id, e)}
                          />
                        </React.Fragment>
                      );
                    })}
                    {/* Resize handle on the right */}
                    {isSelected && (
                      <div
                        className="absolute -right-2 top-1/2 -translate-y-1/2 w-4 h-6 bg-primary/80 rounded cursor-ew-resize flex items-center justify-center"
                        onMouseDown={(e) => {
                          e.stopPropagation();
                          // Start resize operation
                          const startX = e.clientX;
                          const startWidth = busWidth;

                          const handleResize = (moveEvent: MouseEvent) => {
                            const delta = (moveEvent.clientX - startX) / zoom;
                            const newWidth = Math.max(60, Math.min(400, startWidth + delta));
                            onUpdateElement(element.id, { width: Math.round(newWidth) } as Partial<NetworkElement>);
                          };

                          const handleResizeEnd = () => {
                            document.removeEventListener("mousemove", handleResize);
                            document.removeEventListener("mouseup", handleResizeEnd);
                          };

                          document.addEventListener("mousemove", handleResize);
                          document.addEventListener("mouseup", handleResizeEnd);
                        }}
                      >
                        <div className="w-0.5 h-3 bg-background/80 rounded" />
                      </div>
                    )}
                  </div>
                  <span
                    className="mt-1 text-xs font-medium text-foreground/80 bg-background/80 px-1.5 py-0.5 rounded whitespace-nowrap"
                  >
                    {element.name}
                  </span>
                </div>
              );
            }

            // Render lines/cables as visual line segments ONLY if not connected
            // (Connected lines are rendered in SVG layer)
            if (element.type === "line" || element.type === "cable") {
              const lineEl = element as { fromElementId?: string; toElementId?: string };
              // Skip if both ends are connected - it's rendered in SVG
              if (lineEl.fromElementId && lineEl.toElementId) {
                return null;
              }

              // Show as unconnected line that needs to be configured
              return (
                <div
                  key={element.id}
                  className={`absolute flex flex-col items-center cursor-move select-none ${isSelected ? "z-10" : ""}`}
                  style={{
                    left: element.x,
                    top: element.y,
                  }}
                  onMouseDown={(e) => handleMouseDown(e, element.id)}
                  data-testid={`canvas-element-${element.id}`}
                >
                  <div
                    className={`relative flex items-center ${isSelected ? "ring-2 ring-primary ring-offset-2 ring-offset-background rounded" : ""} ${isConnecting ? "animate-pulse" : ""}`}
                  >
                    {/* Left terminal */}
                    <button
                      className={`w-3 h-3 rounded-full border-2 border-background hover:scale-125 transition-transform ${lineEl.fromElementId ? "bg-emerald-500" : "bg-orange-400"}`}
                      onMouseDown={(e) => handleConnectionEnd(element.id, e)}
                      onMouseUp={(e) => handleConnectionEnd(element.id, e)}
                      data-testid={`connection-port-in-${element.id}`}
                      title={lineEl.fromElementId ? "Connected" : "Set From Element in Properties"}
                    />
                    {/* Line segment */}
                    <div className={`w-16 h-0.5 relative ${lineEl.fromElementId && lineEl.toElementId ? "bg-emerald-500" : "bg-orange-400"}`}>
                      {element.type === "cable" && (
                        <div className="absolute inset-0 flex items-center justify-center">
                          <div className="w-full h-1.5 border-t-2 border-b-2 border-dashed border-orange-500" />
                        </div>
                      )}
                    </div>
                    {/* Right terminal */}
                    <button
                      className={`w-3 h-3 rounded-full border-2 border-background hover:scale-125 transition-transform ${lineEl.toElementId ? "bg-emerald-500" : "bg-orange-400"}`}
                      onMouseDown={(e) => handleConnectionStart(element.id, e)}
                      data-testid={`connection-port-out-${element.id}`}
                      title={lineEl.toElementId ? "Connected" : "Set To Element in Properties"}
                    />
                  </div>
                  <span
                    className="mt-1 text-xs font-medium text-foreground/80 bg-background/80 px-1.5 py-0.5 rounded whitespace-nowrap"
                  >
                    {element.name}
                    {(!lineEl.fromElementId || !lineEl.toElementId) && (
                      <span className="ml-1 text-orange-400" title="Set From/To Element in Properties">⚠</span>
                    )}
                  </span>
                </div>
              );
            }

            // Default rendering for other elements (generators, loads, transformers, etc.)
            return (
              <div
                key={element.id}
                className={`absolute flex flex-col items-center gap-1 cursor-move select-none transition-shadow ${isSelected ? "element-shadow-selected" : "element-shadow"
                  }`}
                style={{
                  left: element.x,
                  top: element.y,
                  transform: `rotate(${element.rotation || 0}deg)`,
                }}
                onMouseDown={(e) => handleMouseDown(e, element.id)}
                data-testid={`canvas-element-${element.id}`}
              >
                <div
                  className={`relative p-3 rounded-md border-2 bg-card border-border ${isSelected ? "ring-2 ring-primary ring-offset-2 ring-offset-background" : ""
                    } ${isConnecting ? "animate-pulse" : ""}`}
                >
                  <div className="text-foreground">
                    <ElementIcon type={element.type} size={28} />
                  </div>

                  <button
                    className="absolute -left-2 top-1/2 -translate-y-1/2 w-3 h-3 rounded-full bg-primary border-2 border-background hover:scale-125 transition-transform"
                    onMouseDown={(e) => handleConnectionEnd(element.id, e)}
                    onMouseUp={(e) => handleConnectionEnd(element.id, e)}
                    data-testid={`connection-port-in-${element.id}`}
                  />
                  <button
                    className="absolute -right-2 top-1/2 -translate-y-1/2 w-3 h-3 rounded-full bg-primary border-2 border-background hover:scale-125 transition-transform"
                    onMouseDown={(e) => handleConnectionStart(element.id, e)}
                    data-testid={`connection-port-out-${element.id}`}
                  />
                </div>
                <span
                  className="text-xs font-medium text-foreground/80 bg-background/80 px-1.5 py-0.5 rounded whitespace-nowrap"
                  style={{ transform: `rotate(-${element.rotation || 0}deg)` }}
                >
                  {element.name}
                </span>
              </div>
            );
          })}
        </div>

        {elements.length === 0 && (
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
            <div className="text-center">
              <div className="text-muted-foreground/50 text-lg font-medium mb-2">
                Network Canvas
              </div>
              <div className="text-muted-foreground/40 text-sm">
                Drag elements from the palette or double-click to add
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
