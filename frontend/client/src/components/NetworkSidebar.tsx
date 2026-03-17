import { type NetworkElement } from "@shared/schema";
import { NetworkElementTree } from "./NetworkElementTree";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Network } from "lucide-react";

interface NetworkSidebarProps {
    elements: NetworkElement[];
    selectedElementId: string | null;
    onSelectElement: (id: string | null) => void;
}

export function NetworkSidebar({
    elements,
    selectedElementId,
    onSelectElement,
}: NetworkSidebarProps) {
    return (
        <div className="flex flex-col h-full bg-background text-foreground border-r border-border">
            <div className="p-3 border-b border-border bg-muted/10">
                <h2 className="text-sm font-semibold flex items-center gap-2">
                    <Network className="h-4 w-4" />
                    Network
                </h2>
            </div>

            <ScrollArea className="flex-1">
                <NetworkElementTree
                    elements={elements}
                    selectedElementId={selectedElementId}
                    onSelectElement={onSelectElement}
                />
            </ScrollArea>
        </div>
    );
}
