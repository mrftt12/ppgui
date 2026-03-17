import { useState } from "react";
import type { EquipmentTemplate, ElementType } from "@shared/schema";
import { elementTypes } from "@shared/schema";
import { ElementIcon, elementColors, elementLabels } from "./ElementIcons";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Plus, Database, Search, Package } from "lucide-react";

interface EquipmentDatabaseProps {
  templates: EquipmentTemplate[];
  onAddTemplate: (template: Omit<EquipmentTemplate, "id">) => void;
  onSelectTemplate: (template: EquipmentTemplate) => void;
}

export function EquipmentDatabase({
  templates,
  onAddTemplate,
  onSelectTemplate,
}: EquipmentDatabaseProps) {
  const [dialogOpen, setDialogOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [filterType, setFilterType] = useState<ElementType | "all">("all");
  const [newTemplate, setNewTemplate] = useState({
    name: "",
    manufacturer: "",
    model: "",
    type: "transformer" as ElementType,
    description: "",
  });

  const filteredTemplates = templates.filter((t) => {
    const matchesSearch =
      t.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      t.manufacturer?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      t.model?.toLowerCase().includes(searchQuery.toLowerCase());
    const matchesType = filterType === "all" || t.type === filterType;
    return matchesSearch && matchesType;
  });

  const groupedTemplates = elementTypes.reduce((acc, type) => {
    acc[type] = filteredTemplates.filter((t) => t.type === type);
    return acc;
  }, {} as Record<ElementType, EquipmentTemplate[]>);

  const handleCreate = () => {
    if (newTemplate.name.trim()) {
      onAddTemplate({
        ...newTemplate,
        defaultProperties: getDefaultProperties(newTemplate.type),
      });
      setNewTemplate({
        name: "",
        manufacturer: "",
        model: "",
        type: "transformer",
        description: "",
      });
      setDialogOpen(false);
    }
  };

  const getDefaultProperties = (type: ElementType): Record<string, any> => {
    switch (type) {
      case "transformer":
        return { ratingMVA: 10, primaryVoltageKV: 13.8, secondaryVoltageKV: 0.48, impedancePercent: 5.75 };
      case "generator":
        return { ratingMVA: 5, activePowerMW: 4, voltageSetpointPU: 1.0 };
      case "load":
        return { activePowerKW: 100, reactivePowerKVAR: 30 };
      default:
        return {};
    }
  };

  return (
    <div className="flex flex-col h-full">
      <div className="p-4 border-b space-y-3">
        <div className="flex items-center gap-2">
          <Database className="h-5 w-5 text-primary" />
          <h3 className="font-semibold">Equipment Database</h3>
        </div>

        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search equipment..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-8 h-9"
            data-testid="input-search-equipment"
          />
        </div>

        <div className="flex items-center gap-2">
          <Select
            value={filterType}
            onValueChange={(val) => setFilterType(val as ElementType | "all")}
          >
            <SelectTrigger className="flex-1 h-9" data-testid="select-filter-type">
              <SelectValue placeholder="Filter by type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Types</SelectItem>
              {elementTypes.map((type) => (
                <SelectItem key={type} value={type}>
                  {elementLabels[type]}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
            <DialogTrigger asChild>
              <Button size="icon" variant="outline" data-testid="button-add-equipment">
                <Plus className="h-4 w-4" />
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Add Equipment Template</DialogTitle>
                <DialogDescription>
                  Create a reusable equipment template
                </DialogDescription>
              </DialogHeader>
              <div className="space-y-4 py-4">
                <div className="space-y-2">
                  <Label htmlFor="eq-name">Equipment Name</Label>
                  <Input
                    id="eq-name"
                    value={newTemplate.name}
                    onChange={(e) =>
                      setNewTemplate((prev) => ({ ...prev, name: e.target.value }))
                    }
                    placeholder="e.g., 1000 kVA Pad-Mount"
                    data-testid="input-equipment-name"
                  />
                </div>
                <div className="grid grid-cols-2 gap-3">
                  <div className="space-y-2">
                    <Label htmlFor="eq-manufacturer">Manufacturer</Label>
                    <Input
                      id="eq-manufacturer"
                      value={newTemplate.manufacturer}
                      onChange={(e) =>
                        setNewTemplate((prev) => ({ ...prev, manufacturer: e.target.value }))
                      }
                      placeholder="e.g., ABB"
                      data-testid="input-equipment-manufacturer"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="eq-model">Model</Label>
                    <Input
                      id="eq-model"
                      value={newTemplate.model}
                      onChange={(e) =>
                        setNewTemplate((prev) => ({ ...prev, model: e.target.value }))
                      }
                      placeholder="e.g., PMH-3"
                      data-testid="input-equipment-model"
                    />
                  </div>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="eq-type">Type</Label>
                  <Select
                    value={newTemplate.type}
                    onValueChange={(val) =>
                      setNewTemplate((prev) => ({ ...prev, type: val as ElementType }))
                    }
                  >
                    <SelectTrigger data-testid="select-equipment-type">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {elementTypes.map((type) => (
                        <SelectItem key={type} value={type}>
                          {elementLabels[type]}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="eq-description">Description</Label>
                  <Textarea
                    id="eq-description"
                    value={newTemplate.description}
                    onChange={(e) =>
                      setNewTemplate((prev) => ({ ...prev, description: e.target.value }))
                    }
                    placeholder="Optional description..."
                    rows={2}
                    data-testid="input-equipment-description"
                  />
                </div>
              </div>
              <DialogFooter>
                <Button variant="outline" onClick={() => setDialogOpen(false)}>
                  Cancel
                </Button>
                <Button onClick={handleCreate} disabled={!newTemplate.name.trim()} data-testid="button-create-equipment">
                  Create
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      <ScrollArea className="flex-1 custom-scrollbar">
        <div className="p-4 space-y-4">
          {filteredTemplates.length === 0 ? (
            <div className="text-center py-8">
              <Package className="h-10 w-10 text-muted-foreground/30 mx-auto mb-3" />
              <p className="text-sm text-muted-foreground">No equipment found</p>
              <p className="text-xs text-muted-foreground/70 mt-1">
                Add equipment templates to build your library
              </p>
            </div>
          ) : (
            Object.entries(groupedTemplates).map(([type, items]) => {
              if (items.length === 0) return null;
              const colors = elementColors[type as ElementType];
              return (
                <div key={type}>
                  <div className="flex items-center gap-2 mb-2">
                    <div className={colors.text}>
                      <ElementIcon type={type as ElementType} size={16} />
                    </div>
                    <h4 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                      {elementLabels[type as ElementType]}
                    </h4>
                    <Badge variant="secondary" className="text-xs">
                      {items.length}
                    </Badge>
                  </div>
                  <div className="space-y-2">
                    {items.map((template) => (
                      <Card
                        key={template.id}
                        className={`cursor-pointer hover-elevate border ${colors.border} bg-background`}
                        onClick={() => onSelectTemplate(template)}
                        data-testid={`equipment-template-${template.id}`}
                      >
                        <CardContent className="p-3">
                          <div className="flex items-start gap-3">
                            <div className={`p-2 rounded-md ${colors.bg} ${colors.text}`}>
                              <ElementIcon type={type as ElementType} size={18} />
                            </div>
                            <div className="flex-1 min-w-0">
                              <div className="font-medium text-sm truncate">
                                {template.name}
                              </div>
                              {(template.manufacturer || template.model) && (
                                <div className="text-xs text-muted-foreground mt-0.5">
                                  {[template.manufacturer, template.model]
                                    .filter(Boolean)
                                    .join(" - ")}
                                </div>
                              )}
                              {template.description && (
                                <p className="text-xs text-muted-foreground/70 mt-1 line-clamp-1">
                                  {template.description}
                                </p>
                              )}
                            </div>
                          </div>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                </div>
              );
            })
          )}
        </div>
      </ScrollArea>
    </div>
  );
}
