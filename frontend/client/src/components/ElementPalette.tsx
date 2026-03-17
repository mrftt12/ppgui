import { type ElementType } from "@shared/schema";
import { ElementIcon, elementLabels } from "./ElementIcons";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useDraggable } from "@dnd-kit/core";
import { CSS } from "@dnd-kit/utilities";
import { ChevronDown, ChevronRight, Plus, Grid, Zap, Factory, PlugZap, Layers } from "lucide-react";
import { useState, useRef } from "react";

// Define element categories
const elementCategories: { id: string; label: string; icon: React.ElementType; types: ElementType[] }[] = [
  {
    id: "external",
    label: "External",
    icon: PlugZap,
    types: ["external_source"],
  },
  {
    id: "equipment",
    label: "Equipment",
    icon: Grid,
    types: ["bus", "transformer", "switch", "capacitor"],
  },
  {
    id: "generation",
    label: "Generation",
    icon: Zap,
    types: ["generator", "battery"],
  },
  {
    id: "load",
    label: "Load",
    icon: Factory,
    types: ["load"],
  },
  {
    id: "underground",
    label: "Underground",
    icon: Layers,
    types: ["ductbank", "cable"],
  },
];

interface DraggableElementProps {
  type: ElementType;
  onAddElement: (type: ElementType) => void;
}

function DraggableElement({ type, onAddElement }: DraggableElementProps) {
  const clickTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const clickCountRef = useRef(0);

  const { attributes, listeners, setNodeRef, transform, isDragging } = useDraggable({
    id: `palette-${type}`,
    data: { type, source: "palette" },
  });

  const style = {
    transform: CSS.Translate.toString(transform),
    opacity: isDragging ? 0.5 : 1,
  };

  const handleClick = () => {
    clickCountRef.current += 1;

    if (clickCountRef.current === 1) {
      clickTimeoutRef.current = setTimeout(() => {
        clickCountRef.current = 0;
      }, 300);
    } else if (clickCountRef.current === 2) {
      if (clickTimeoutRef.current) {
        clearTimeout(clickTimeoutRef.current);
      }
      clickCountRef.current = 0;
      onAddElement(type);
    }
  };

  // Handle native drag for React Flow compatibility
  const handleDragStart = (event: React.DragEvent) => {
    event.dataTransfer.setData('application/reactflow-type', type);
    event.dataTransfer.effectAllowed = 'move';
  };

  return (
    <div className="flex items-center gap-1 pl-4">
      <Card
        ref={setNodeRef}
        style={style}
        {...listeners}
        {...attributes}
        onClick={handleClick}
        draggable
        onDragStart={handleDragStart}
        className="flex-1 flex items-center gap-2 p-2 border border-border/50 bg-card/50 hover:bg-card hover:border-border transition-all cursor-grab active:cursor-grabbing touch-none text-sm"
        data-testid={`element-palette-${type}`}
      >
        <div className="text-muted-foreground">
          <ElementIcon type={type} size={18} />
        </div>
        <span className="font-medium text-foreground/90">
          {elementLabels[type]}
        </span>
      </Card>
      <Button
        size="icon"
        variant="ghost"
        className="h-7 w-7 shrink-0"
        onClick={() => onAddElement(type)}
        data-testid={`button-add-${type}`}
      >
        <Plus className="h-3.5 w-3.5" />
      </Button>
    </div>
  );
}

interface CategoryTreeProps {
  category: typeof elementCategories[0];
  onAddElement: (type: ElementType) => void;
  defaultExpanded?: boolean;
}

function CategoryTree({ category, onAddElement, defaultExpanded = true }: CategoryTreeProps) {
  const [isExpanded, setIsExpanded] = useState(defaultExpanded);
  const Icon = category.icon;

  return (
    <div className="mb-1">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center gap-2 px-2 py-1.5 text-sm font-semibold text-sidebar-foreground/80 hover:bg-muted/50 rounded transition-colors"
        data-testid={`category-${category.id}`}
      >
        {isExpanded ? (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
        )}
        <Icon className="h-4 w-4 text-primary/70" />
        <span>{category.label}</span>
        <span className="ml-auto text-xs text-muted-foreground">
          {category.types.length}
        </span>
      </button>

      {isExpanded && (
        <div className="mt-1 space-y-1">
          {category.types.map((type) => (
            <DraggableElement
              key={type}
              type={type}
              onAddElement={onAddElement}
            />
          ))}
        </div>
      )}
    </div>
  );
}

interface ElementPaletteProps {
  onAddElement: (type: ElementType) => void;
}

export function ElementPalette({ onAddElement }: ElementPaletteProps) {
  return (
    <div className="flex flex-col gap-1">
      {elementCategories.map((category) => (
        <CategoryTree
          key={category.id}
          category={category}
          onAddElement={onAddElement}
        />
      ))}
    </div>
  );
}
