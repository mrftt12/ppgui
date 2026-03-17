import { useState } from "react";
import type { NetworkModel } from "@shared/schema";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Plus, FolderOpen, Save, Trash2, FileText, Clock, Beaker, ChevronDown } from "lucide-react";

interface NetworkModelManagerProps {
  models: NetworkModel[];
  currentModelId: string | null;
  onNewModel: (
    name: string,
    description: string | undefined,
    baseFrequencyHz: number,
    baseVoltageKV: number
  ) => void;
  onLoadModel: (id: string) => void;
  onSaveModel: () => void;
  onDeleteModel: (id: string) => void;
  onUpdateModelInfo: (name: string, description?: string) => void;
  hasUnsavedChanges: boolean;
  newDialogOpen?: boolean;
  onNewDialogOpenChange?: (open: boolean) => void;
  onLoadSample?: (sampleType: string) => void;
  isLoadingSample?: boolean;
  testNetworks?: Array<{ name: string; displayName: string; hasGeodata: boolean }>;
  onLoadTestNetwork?: (name: string) => void;
  isLoadingTestNetwork?: boolean;
}

export function NetworkModelManager({
  models,
  currentModelId,
  onNewModel,
  onLoadModel,
  onSaveModel,
  onDeleteModel,
  onUpdateModelInfo,
  hasUnsavedChanges,
  newDialogOpen,
  onNewDialogOpenChange,
  onLoadSample,
  isLoadingSample,
  testNetworks = [],
  onLoadTestNetwork,
  isLoadingTestNetwork,
}: NetworkModelManagerProps) {
  const [internalNewDialogOpen, setInternalNewDialogOpen] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDescription, setNewDescription] = useState("");
  const [newBaseFrequency, setNewBaseFrequency] = useState("60");
  const [newBaseVoltage, setNewBaseVoltage] = useState("13.8");
  const [formErrors, setFormErrors] = useState<{
    name?: string;
    baseFrequencyHz?: string;
    baseVoltageKV?: string;
  }>({});
  const [sampleOpen, setSampleOpen] = useState(false);
  const [testOpen, setTestOpen] = useState(false);

  const wp110Networks = testNetworks
    .filter((n) => n.name.toLowerCase().startsWith("wp1.10_"))
    .sort((a, b) => a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: "base" }));

  const geoNetworks = testNetworks
    .filter((n) => n.hasGeodata && !n.name.toLowerCase().startsWith("wp1.10_"))
    .sort((a, b) => a.displayName.localeCompare(b.displayName));

  const [geoOpen, setGeoOpen] = useState(true);

  const formatWp110DisplayName = (name: string) =>
    name.replace(/^wp1\.10_/i, "").replace(/\.pkl$/i, "");

  const dialogOpen = newDialogOpen ?? internalNewDialogOpen;
  const setDialogOpen = onNewDialogOpenChange ?? setInternalNewDialogOpen;

  const currentModel = models.find((m) => m.id === currentModelId);

  const parsedFrequency = Number(newBaseFrequency);
  const parsedVoltage = Number(newBaseVoltage);
  const isFrequencyValid = Number.isFinite(parsedFrequency) && parsedFrequency > 0;
  const isVoltageValid = Number.isFinite(parsedVoltage) && parsedVoltage > 0;
  const isFormValid = Boolean(newName.trim()) && isFrequencyValid && isVoltageValid;

  const resetForm = () => {
    setNewName("");
    setNewDescription("");
    setNewBaseFrequency("60");
    setNewBaseVoltage("13.8");
    setFormErrors({});
  };

  const handleCreate = () => {
    const errors: { name?: string; baseFrequencyHz?: string; baseVoltageKV?: string } = {};

    if (!newName.trim()) {
      errors.name = "Name is required";
    }

    if (!newBaseFrequency.trim()) {
      errors.baseFrequencyHz = "Base frequency is required";
    } else if (!isFrequencyValid) {
      errors.baseFrequencyHz = "Base frequency must be a positive number";
    }

    if (!newBaseVoltage.trim()) {
      errors.baseVoltageKV = "Base voltage is required";
    } else if (!isVoltageValid) {
      errors.baseVoltageKV = "Base voltage must be a positive number";
    }

    if (Object.keys(errors).length > 0) {
      setFormErrors(errors);
      return;
    }

    onNewModel(
      newName.trim(),
      newDescription.trim() || undefined,
      parsedFrequency,
      parsedVoltage
    );
    resetForm();
    setDialogOpen(false);
  };

  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  };

  const handleDialogOpenChange = (open: boolean) => {
    if (open) {
      setFormErrors({});
    }
    setDialogOpen(open);
  };

  return (
    <div className="flex flex-col h-full">
      <div className="p-4 border-b space-y-3">
        <div className="flex items-center gap-2">
          <Dialog open={dialogOpen} onOpenChange={handleDialogOpenChange}>
            <DialogTrigger asChild>
              <Button className="flex-1" data-testid="button-new-model">
                <Plus className="h-4 w-4 mr-2" />
                New Network
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Create New Network</DialogTitle>
                <DialogDescription>
                  Create a new power system network workspace
                </DialogDescription>
              </DialogHeader>
              <div className="space-y-4 py-4">
                <div className="space-y-2">
                  <Label htmlFor="name">Network Name</Label>
                  <Input
                    id="name"
                    value={newName}
                    onChange={(e) => {
                      setNewName(e.target.value);
                      if (formErrors.name) {
                        setFormErrors((prev) => ({ ...prev, name: undefined }));
                      }
                    }}
                    placeholder="e.g., Distribution Network #1"
                    data-testid="input-new-model-name"
                  />
                  {formErrors.name && (
                    <p className="text-xs text-destructive">{formErrors.name}</p>
                  )}
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="base-frequency">Base Frequency (Hz)</Label>
                    <Input
                      id="base-frequency"
                      type="number"
                      min="1"
                      step="1"
                      value={newBaseFrequency}
                      onChange={(e) => {
                        setNewBaseFrequency(e.target.value);
                        if (formErrors.baseFrequencyHz) {
                          setFormErrors((prev) => ({ ...prev, baseFrequencyHz: undefined }));
                        }
                      }}
                      data-testid="input-new-model-frequency"
                    />
                    {formErrors.baseFrequencyHz && (
                      <p className="text-xs text-destructive">{formErrors.baseFrequencyHz}</p>
                    )}
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="base-voltage">Base Voltage (kV)</Label>
                    <Input
                      id="base-voltage"
                      type="number"
                      min="0"
                      step="0.01"
                      value={newBaseVoltage}
                      onChange={(e) => {
                        setNewBaseVoltage(e.target.value);
                        if (formErrors.baseVoltageKV) {
                          setFormErrors((prev) => ({ ...prev, baseVoltageKV: undefined }));
                        }
                      }}
                      data-testid="input-new-model-voltage"
                    />
                    {formErrors.baseVoltageKV && (
                      <p className="text-xs text-destructive">{formErrors.baseVoltageKV}</p>
                    )}
                  </div>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="description">Description (optional)</Label>
                  <Textarea
                    id="description"
                    value={newDescription}
                    onChange={(e) => setNewDescription(e.target.value)}
                    placeholder="Brief description of the network..."
                    rows={3}
                    data-testid="input-new-model-description"
                  />
                </div>
              </div>
              <DialogFooter>
                <Button variant="outline" onClick={() => setDialogOpen(false)}>
                  Cancel
                </Button>
                <Button onClick={handleCreate} disabled={!isFormValid} data-testid="button-create-model">
                  Create
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>

          <Button
            variant="outline"
            onClick={onSaveModel}
            disabled={!currentModelId || !hasUnsavedChanges}
            data-testid="button-save-model"
          >
            <Save className="h-4 w-4" />
          </Button>
        </div>

        {/* Sample Cases */}
        <div className="border rounded-md bg-muted/30 overflow-hidden">
          <button
            type="button"
            className="w-full px-3 py-2 flex items-center justify-between text-xs font-semibold uppercase tracking-wider text-muted-foreground hover:bg-muted/40"
            onClick={() => setSampleOpen((prev) => !prev)}
          >
            Sample Cases
            <ChevronDown className={`h-3.5 w-3.5 transition ${sampleOpen ? "rotate-180" : ""}`} />
          </button>
          {sampleOpen && (
            <div className="p-3 pt-2">
              <Button
                variant="secondary"
                size="sm"
                className="w-full justify-start gap-2"
                onClick={() => onLoadSample?.("ieee-123")}
                disabled={isLoadingSample}
                data-testid="button-load-ieee-123"
              >
                <Beaker className="h-4 w-4" />
                {isLoadingSample ? "Loading..." : "Load IEEE 123-Bus"}
              </Button>
              <p className="text-[10px] text-muted-foreground mt-1.5">
                123 buses, 85 line segments, switches
              </p>
              <div className="mt-2 space-y-1">
                <Button
                  variant="secondary"
                  size="sm"
                  className="w-full justify-start gap-2"
                  onClick={() => onLoadSample?.("ieee-30")}
                  disabled={isLoadingSample}
                  data-testid="button-load-ieee-30"
                >
                  <Beaker className="h-4 w-4" />
                  {isLoadingSample ? "Loading..." : "Load IEEE 30-Bus"}
                </Button>
                <p className="text-[10px] text-muted-foreground">
                  30 buses, 41 branches, 6 generators
                </p>
                <Button
                  variant="secondary"
                  size="sm"
                  className="w-full justify-start gap-2"
                  onClick={() => onLoadSample?.("ieee-9")}
                  disabled={isLoadingSample}
                  data-testid="button-load-ieee-9"
                >
                  <Beaker className="h-4 w-4" />
                  {isLoadingSample ? "Loading..." : "Load IEEE 9-Bus"}
                </Button>
                <p className="text-[10px] text-muted-foreground">
                  9 buses, 9 branches, 3 generators
                </p>
              </div>
            </div>
          )}
        </div>

        {/* Geospatial Networks */}
        <div className="border rounded-md bg-muted/30 overflow-hidden">
          <button
            type="button"
            className="w-full px-3 py-2 flex items-center justify-between text-xs font-semibold uppercase tracking-wider text-muted-foreground hover:bg-muted/40"
            onClick={() => setGeoOpen((prev) => !prev)}
          >
            Geospatial Networks
            <ChevronDown className={`h-3.5 w-3.5 transition ${geoOpen ? "rotate-180" : ""}`} />
          </button>
          {geoOpen && (
            <div className="p-3 pt-2 space-y-1">
              {geoNetworks.length === 0 ? (
                <p className="text-xs text-muted-foreground">No geospatial networks found</p>
              ) : (
                geoNetworks.map((network) => (
                  <Button
                    key={network.name}
                    variant="secondary"
                    size="sm"
                    className="w-full justify-start gap-2"
                    onClick={() => onLoadTestNetwork?.(network.name)}
                    disabled={isLoadingTestNetwork}
                    data-testid={`button-load-geo-${network.name}`}
                  >
                    {isLoadingTestNetwork ? "Loading..." : network.displayName}
                  </Button>
                ))
              )}
            </div>
          )}
        </div>

        {/* wp1.10 cases */}
        <div className="border rounded-md bg-muted/30 overflow-hidden">
          <button
            type="button"
            className="w-full px-3 py-2 flex items-center justify-between text-xs font-semibold uppercase tracking-wider text-muted-foreground hover:bg-muted/40"
            onClick={() => setTestOpen((prev) => !prev)}
          >
            wp1.10 cases
            <ChevronDown className={`h-3.5 w-3.5 transition ${testOpen ? "rotate-180" : ""}`} />
          </button>
          {testOpen && (
            <div className="p-3 pt-2 space-y-1">
              {wp110Networks.length === 0 ? (
                <p className="text-xs text-muted-foreground">No wp1.10 cases found</p>
              ) : (
                wp110Networks.map((network) => (
                  <Button
                    key={network.name}
                    variant="secondary"
                    size="sm"
                    className="w-full justify-start"
                    onClick={() => onLoadTestNetwork?.(network.name)}
                    disabled={isLoadingTestNetwork}
                    data-testid={`button-load-wp110-${network.name}`}
                  >
                    {isLoadingTestNetwork ? "Loading..." : formatWp110DisplayName(network.name)}
                  </Button>
                ))
              )}
            </div>
          )}
        </div>

        {currentModel && (
          <Card className="bg-muted/50">
            <CardContent className="p-3">
              <div className="flex items-start justify-between gap-2">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <FileText className="h-4 w-4 text-primary shrink-0" />
                    <span className="font-medium text-sm truncate">
                      {currentModel.name}
                    </span>
                    {hasUnsavedChanges && (
                      <Badge variant="secondary" className="text-xs">Modified</Badge>
                    )}
                  </div>
                  {currentModel.description && (
                    <p className="text-xs text-muted-foreground mt-1 line-clamp-2">
                      {currentModel.description}
                    </p>
                  )}
                  <div className="flex items-center gap-2 mt-2 text-[11px] text-muted-foreground">
                    <Badge variant="outline" className="text-[10px]">
                      {currentModel.baseFrequencyHz} Hz
                    </Badge>
                    <Badge variant="outline" className="text-[10px]">
                      {currentModel.baseVoltageKV} kV
                    </Badge>
                  </div>
                  <div className="flex items-center gap-1 mt-2 text-xs text-muted-foreground">
                    <Clock className="h-3 w-3" />
                    <span>{formatDate(currentModel.updatedAt)}</span>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>

      <ScrollArea className="flex-1 custom-scrollbar">
        <div className="p-4 space-y-2">
          <h4 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
            Saved Networks
          </h4>
          {models.length === 0 ? (
            <div className="text-center py-8">
              <FolderOpen className="h-10 w-10 text-muted-foreground/30 mx-auto mb-3" />
              <p className="text-sm text-muted-foreground">No saved networks yet</p>
              <p className="text-xs text-muted-foreground/70 mt-1">
                Create a new network to get started
              </p>
            </div>
          ) : (
            models.map((model) => (
              <Card
                key={model.id}
                className={`cursor-pointer transition-all hover-elevate ${model.id === currentModelId
                    ? "ring-2 ring-primary ring-offset-2 ring-offset-background"
                    : ""
                  }`}
                onClick={() => onLoadModel(model.id)}
                data-testid={`model-card-${model.id}`}
              >
                <CardContent className="p-3">
                  <div className="flex items-start justify-between gap-2">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <FileText className="h-4 w-4 text-muted-foreground shrink-0" />
                        <span className="font-medium text-sm truncate">
                          {model.name}
                        </span>
                      </div>
                      {model.description && (
                        <p className="text-xs text-muted-foreground mt-1 line-clamp-1">
                          {model.description}
                        </p>
                      )}
                      <div className="flex items-center gap-3 mt-2 text-xs text-muted-foreground">
                        <span>{model.elements.length} elements</span>
                        <span>{model.connections.length} connections</span>
                      </div>
                    </div>
                    <AlertDialog>
                      <AlertDialogTrigger asChild>
                        <Button
                          size="icon"
                          variant="ghost"
                          className="shrink-0 text-muted-foreground hover:text-destructive"
                          onClick={(e) => e.stopPropagation()}
                          data-testid={`button-delete-model-${model.id}`}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </AlertDialogTrigger>
                      <AlertDialogContent>
                        <AlertDialogHeader>
                          <AlertDialogTitle>Delete Model</AlertDialogTitle>
                          <AlertDialogDescription>
                            Are you sure you want to delete "{model.name}"? This action cannot be undone.
                          </AlertDialogDescription>
                        </AlertDialogHeader>
                        <AlertDialogFooter>
                          <AlertDialogCancel>Cancel</AlertDialogCancel>
                          <AlertDialogAction
                            onClick={() => onDeleteModel(model.id)}
                            className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                          >
                            Delete
                          </AlertDialogAction>
                        </AlertDialogFooter>
                      </AlertDialogContent>
                    </AlertDialog>
                  </div>
                </CardContent>
              </Card>
            ))
          )}
        </div>
      </ScrollArea>
    </div>
  );
}
