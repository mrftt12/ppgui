import { type ElementType } from "@shared/schema";
import { ElementPalette } from "./ElementPalette";
import { SpatialPalette } from "./SpatialPalette";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Layers, Map } from "lucide-react";

interface ElementSidebarProps {
    onAddElement: (type: ElementType) => void;
    viewMode: "grid" | "spatial";
}

export function ElementSidebar({ onAddElement, viewMode }: ElementSidebarProps) {
    return (
        <div className="flex flex-col h-full bg-background text-foreground border-r border-border">
            <div className="p-3 border-b border-border bg-muted/10">
                <h2 className="text-sm font-semibold flex items-center gap-2">
                    {viewMode === "spatial" ? <Map className="h-4 w-4" /> : <Layers className="h-4 w-4" />}
                    {viewMode === "spatial" ? "Spatial Map" : "Element Explorer"}
                </h2>
            </div>

            <ScrollArea className="flex-1">
                <div className="p-2 h-full">
                    {viewMode === "spatial" ? (
                        <SpatialPalette />
                    ) : (
                        <ElementPalette onAddElement={onAddElement} />
                    )}
                </div>
            </ScrollArea>
        </div>
    );
}
