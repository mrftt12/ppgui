import { useMemo } from "react";
import type { NetworkElement, ElementType } from "@shared/schema";
import { elementTypes } from "@shared/schema";
import { ElementIcon, elementColors, elementLabels } from "./ElementIcons";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { ChevronRight, Layers } from "lucide-react";
import { cn } from "@/lib/utils";

interface NetworkElementTreeProps {
  elements: NetworkElement[];
  selectedElementId: string | null;
  onSelectElement: (id: string) => void;
}

export function NetworkElementTree({
  elements,
  selectedElementId,
  onSelectElement,
}: NetworkElementTreeProps) {
  const groupedElements = useMemo(() => {
    const groups: Record<ElementType, NetworkElement[]> = {} as any;
    elementTypes.forEach((type) => {
      groups[type] = [];
    });
    elements.forEach((element) => {
      groups[element.type].push(element);
    });
    return groups;
  }, [elements]);

  const nonEmptyGroups = useMemo(() => {
    return elementTypes.filter((type) => groupedElements[type].length > 0);
  }, [groupedElements]);

  if (elements.length === 0) {
    return (
      <div className="h-full flex items-center justify-center p-4">
        <div className="text-center">
          <Layers className="h-10 w-10 text-muted-foreground/30 mx-auto mb-3" />
          <p className="text-sm text-muted-foreground">No elements in network</p>
          <p className="text-xs text-muted-foreground/70 mt-1">
            Drag elements to the canvas to build your network
          </p>
        </div>
      </div>
    );
  }

  return (
    <ScrollArea className="h-full custom-scrollbar">
      <div className="p-3 space-y-1">
        <div className="flex items-center justify-between px-2 py-1.5 mb-2">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Network Elements
          </h3>
          <Badge variant="secondary" className="text-xs">
            {elements.length}
          </Badge>
        </div>

        {nonEmptyGroups.map((type) => {
          const typeElements = groupedElements[type];
          const colors = elementColors[type];

          return (
            <Collapsible key={type} defaultOpen={false}>
              <CollapsibleTrigger className="flex items-center gap-2 w-full px-2 py-1.5 rounded-md hover-elevate group">
                <ChevronRight className="h-4 w-4 text-muted-foreground transition-transform group-data-[state=open]:rotate-90" />
                <div className={colors.text}>
                  <ElementIcon type={type} size={16} />
                </div>
                <span className="text-sm font-medium flex-1 text-left">
                  {elementLabels[type]}
                </span>
                <Badge variant="outline" className="text-xs">
                  {typeElements.length}
                </Badge>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <div className="ml-6 mt-1 space-y-0.5">
                  {typeElements.map((element) => {
                    const isSelected = element.id === selectedElementId;
                    return (
                      <button
                        key={element.id}
                        className={cn(
                          "flex items-center gap-2 w-full px-2 py-1.5 rounded-md text-left transition-colors",
                          isSelected
                            ? "bg-primary/10 text-primary"
                            : "hover-elevate text-foreground"
                        )}
                        onClick={() => onSelectElement(element.id)}
                        data-testid={`tree-element-${element.id}`}
                      >
                        <div
                          className={cn(
                            "w-1.5 h-1.5 rounded-full",
                            element.enabled ? "bg-green-500" : "bg-muted-foreground"
                          )}
                        />
                        <span className="text-sm truncate flex-1">{element.name}</span>
                        {!element.enabled && (
                          <Badge variant="secondary" className="text-[10px] px-1">
                            Off
                          </Badge>
                        )}
                      </button>
                    );
                  })}
                </div>
              </CollapsibleContent>
            </Collapsible>
          );
        })}
      </div>
    </ScrollArea>
  );
}
