import React, { useEffect, useMemo, useState } from "react";
import type { NetworkElement, ElementType } from "@shared/schema";
import { ElementIcon, elementColors, elementLabels } from "./ElementIcons";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { ChartConfig, ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import { CartesianGrid, Legend, Line, LineChart, XAxis, YAxis } from "recharts";

type DuctLoadProfileSeries = {
  key: string;
  label: string;
  values: number[];
};

type DuctTransientSeries = {
  key: string;
  values: number[];
};

interface PropertiesPanelProps {
  element: NetworkElement | null;
  onUpdate: (id: string, updates: Partial<NetworkElement>) => void;
  allElements?: NetworkElement[];
  onThermalResult?: (payload: {
    ductbankId: string;
    ductbankName: string;
    rows: number;
    columns: number;
    temperatures: number[];
    timestamp: string;
  }) => void;
}

export function PropertiesPanel({ element, onUpdate, allElements = [], onThermalResult }: PropertiesPanelProps) {
  const { toast } = useToast();
  const [isCalculating, setIsCalculating] = useState(false);
  const [isGeneratingLoadProfiles, setIsGeneratingLoadProfiles] = useState(false);
  const [isCalculatingTransientTemps, setIsCalculatingTransientTemps] = useState(false);
  const [loadProfiles, setLoadProfiles] = useState<DuctLoadProfileSeries[]>([]);
  const [transientTemperatures, setTransientTemperatures] = useState<DuctTransientSeries[]>([]);

  useEffect(() => {
    setLoadProfiles([]);
    setTransientTemperatures([]);
  }, [element?.id]);

  const loadProfileChartData = useMemo(() => {
    const pointCount = loadProfiles.reduce((maxCount, series) => Math.max(maxCount, series.values.length), 0);
    const length = pointCount > 0 ? pointCount : 24;

    return Array.from({ length }, (_, hour) => {
      const point: Record<string, string | number | null> = {
        time: `${hour}:00`,
      };

      for (const series of loadProfiles) {
        point[series.key] = series.values[hour] ?? null;
      }

      for (const series of transientTemperatures) {
        point[`temp_${series.key}`] = series.values[hour] ?? null;
      }

      return point;
    });
  }, [loadProfiles, transientTemperatures]);

  const loadProfileChartConfig = useMemo<ChartConfig>(() => {
    const config: ChartConfig = {};

    for (let index = 0; index < loadProfiles.length; index += 1) {
      const series = loadProfiles[index];
      const colorSlot = (index % 5) + 1;
      config[series.key] = {
        label: `${series.label} Load (A)`,
        color: `hsl(var(--chart-${colorSlot}))`,
      };
      config[`temp_${series.key}`] = {
        label: `${series.label} Temp (°C)`,
        color: `hsl(var(--chart-${colorSlot}))`,
      };
    }

    return config;
  }, [loadProfiles]);

  if (!element) {
    return (
      <div className="h-full flex items-center justify-center p-4">
        <div className="text-center">
          <div className="text-muted-foreground/50 text-sm">
            Select an element to view properties
          </div>
        </div>
      </div>
    );
  }

  const colors = elementColors[element.type];

  const handleChange = (field: string, value: any) => {
    onUpdate(element.id, { [field]: value } as Partial<NetworkElement>);
  };

  const handleGenerateLoadProfiles = () => {
    if (element.type !== "ductbank") return;

    if (isGeneratingLoadProfiles) return;

    setIsGeneratingLoadProfiles(true);

    try {
      const ducts = Array.isArray((element as any).ducts)
        ? ((element as any).ducts as Array<{ row?: number; column?: number; load?: number; loadFactor?: number }>)
        : [];

      const loadedDucts = ducts.filter((duct) => Number(duct?.load ?? 0) > 0);

      if (!loadedDucts.length) {
        setLoadProfiles([]);
        setTransientTemperatures([]);
        toast({
          title: "No assigned cable loads",
          description: "Assign load values in one or more ducts before generating profiles.",
          variant: "destructive",
        });
        return;
      }

      const generatedProfiles = loadedDucts.map((duct, index) => {
        const row = Number(duct.row ?? 0);
        const column = Number(duct.column ?? 0);
        const baseLoad = Math.max(0, Number(duct.load ?? 0)) * Math.max(0, Number(duct.loadFactor ?? 1));
        const phaseOffset = (index * Math.PI) / 8;

        const values = Array.from({ length: 24 }, (_, hour) => {
          const dailyComponent = 0.7 + 0.25 * Math.sin(((hour - 7) / 24) * 2 * Math.PI);
          const variability = 0.08 * Math.sin(((hour + 1) / 24) * 4 * Math.PI + phaseOffset);
          const profileScale = Math.max(0.2, dailyComponent + variability);
          return Number((baseLoad * profileScale).toFixed(3));
        });

        return {
          key: `cable_${row}_${column}`,
          label: `Cable R${row}C${column}`,
          values,
        };
      });

      setLoadProfiles(generatedProfiles);
      setTransientTemperatures([]);

      toast({
        title: "Load profiles generated",
        description: `Generated ${generatedProfiles.length} cable profile${generatedProfiles.length === 1 ? "" : "s"}.`,
      });
    } finally {
      setIsGeneratingLoadProfiles(false);
    }
  };

  const handleCalculateTransientTemps = () => {
    if (isCalculatingTransientTemps) return;

    if (!loadProfiles.length) {
      toast({
        title: "No load profiles",
        description: "Generate load profiles before calculating transient temperatures.",
        variant: "destructive",
      });
      return;
    }

    setIsCalculatingTransientTemps(true);

    try {
      const ambientTempC = 25;
      const thermalTimeConstant = 0.18;
      const tempRiseAtRatedLoad = 45;

      const calculated = loadProfiles.map((series) => {
        const ratedLoad = Math.max(...series.values, 1);
        let previous = ambientTempC;
        const values = series.values.map((currentLoad) => {
          const loadRatio = currentLoad / ratedLoad;
          const target = ambientTempC + tempRiseAtRatedLoad * loadRatio * loadRatio;
          previous = previous + thermalTimeConstant * (target - previous);
          return Number(previous.toFixed(3));
        });

        return {
          key: series.key,
          values,
        };
      });

      setTransientTemperatures(calculated);

      const maxTemp = calculated.length
        ? Math.max(...calculated.flatMap((entry) => entry.values))
        : null;

      toast({
        title: "Transient temperatures calculated",
        description: maxTemp != null ? `Max transient temperature: ${maxTemp.toFixed(2)} °C` : "No transient values were produced.",
      });
    } finally {
      setIsCalculatingTransientTemps(false);
    }
  };

  const renderField = (label: string, field: string, type: "text" | "number" | "boolean" | "select", options?: { value: string; label: string }[]) => {
    const value = (element as any)[field];

    if (type === "boolean") {
      return (
        <div className="flex items-center justify-between py-2">
          <Label htmlFor={field} className="text-sm text-muted-foreground">
            {label}
          </Label>
          <Switch
            id={field}
            checked={value}
            onCheckedChange={(checked) => handleChange(field, checked)}
            data-testid={`switch-${field}`}
          />
        </div>
      );
    }

    if (type === "select" && options) {
      return (
        <div className="space-y-1.5 py-2">
          <Label htmlFor={field} className="text-sm text-muted-foreground">
            {label}
          </Label>
          <Select
            value={value}
            onValueChange={(val) => handleChange(field, val)}
          >
            <SelectTrigger data-testid={`select-${field}`}>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {options.map((opt) => (
                <SelectItem key={opt.value} value={opt.value}>
                  {opt.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      );
    }

    return (
      <div className="space-y-1.5 py-2">
        <Label htmlFor={field} className="text-sm text-muted-foreground">
          {label}
        </Label>
        <Input
          key={`${element.id}-${field}`}
          id={field}
          type={type}
          value={value ?? ""}
          onChange={(e) => handleChange(field, type === "number" ? parseFloat(e.target.value) || 0 : e.target.value)}
          className="h-8"
          data-testid={`input-${field}`}
        />
      </div>
    );
  };

  const renderTypeSpecificFields = () => {
    switch (element.type) {
      case "external_source":
        return (
          <>
            {renderField("Voltage (kV)", "voltageKV", "number")}
            {renderField("Short Circuit MVA", "shortCircuitMVA", "number")}
            {renderField("X/R Ratio", "xrRatio", "number")}
            {renderField("Phase Angle (deg)", "phaseAngle", "number")}
          </>
        );
      case "bus":
        return (
          <>
            {renderField("Nominal Voltage (kV)", "nominalVoltageKV", "number")}
            {renderField("Bus Type", "busType", "select", [
              { value: "slack", label: "Slack Bus" },
              { value: "pv", label: "PV Bus" },
              { value: "pq", label: "PQ Bus" },
            ])}
            <Separator className="my-2" />
            <div className="text-xs font-medium text-muted-foreground mb-2">Display</div>
            {renderField("Width (px)", "width", "number")}
          </>
        );
      case "line": {
        // Get all connectable elements (excluding lines, cables, and the current element)
        const connectableElements = allElements.filter(
          el => el.id !== element.id && el.type !== "line" && el.type !== "cable" && el.type !== "ductbank"
        );
        // Group by type for better organization
        const elementsByType = connectableElements.reduce((acc, el) => {
          if (!acc[el.type]) acc[el.type] = [];
          acc[el.type].push(el);
          return acc;
        }, {} as Record<string, typeof connectableElements>);

        const typeLabels: Record<string, string> = {
          bus: "Buses",
          transformer: "Transformers",
          load: "Loads",
          generator: "Generators",
          external_source: "External Sources",
          battery: "Batteries",
          capacitor: "Capacitors",
          switch: "Switches",
          ductbank: "Ductbanks",
        };

        return (
          <>
            {renderField("Installation", "installation", "select", [
              { value: "overhead", label: "Overhead" },
              { value: "underground", label: "Underground" },
            ])}
            {(element as { installation?: string }).installation === "underground" && (
              <div className="text-xs text-muted-foreground">
                Double-click the line in the one-line view to open the underground canvas.
              </div>
            )}
            <div className="text-xs font-medium text-muted-foreground mb-2">Connections</div>
            <div className="space-y-1.5 py-2">
              <Label htmlFor="fromElementId" className="text-sm text-muted-foreground">
                From Element
              </Label>
              <Select
                value={(element as any).fromElementId || "__none__"}
                onValueChange={(val) => handleChange("fromElementId", val === "__none__" ? undefined : val)}
              >
                <SelectTrigger data-testid="select-fromElementId">
                  <SelectValue placeholder="Select an element..." />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__none__">None</SelectItem>
                  {Object.entries(elementsByType).map(([type, elements]) => (
                    <React.Fragment key={type}>
                      <div className="px-2 py-1.5 text-xs font-semibold text-muted-foreground bg-muted/50">
                        {typeLabels[type] || type}
                      </div>
                      {elements.map((el) => (
                        <SelectItem key={el.id} value={el.id}>
                          {el.name}
                        </SelectItem>
                      ))}
                    </React.Fragment>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1.5 py-2">
              <Label htmlFor="toElementId" className="text-sm text-muted-foreground">
                To Element
              </Label>
              <Select
                value={(element as any).toElementId || "__none__"}
                onValueChange={(val) => handleChange("toElementId", val === "__none__" ? undefined : val)}
              >
                <SelectTrigger data-testid="select-toElementId">
                  <SelectValue placeholder="Select an element..." />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__none__">None</SelectItem>
                  {Object.entries(elementsByType).map(([type, elements]) => (
                    <React.Fragment key={type}>
                      <div className="px-2 py-1.5 text-xs font-semibold text-muted-foreground bg-muted/50">
                        {typeLabels[type] || type}
                      </div>
                      {elements.map((el) => (
                        <SelectItem key={el.id} value={el.id}>
                          {el.name}
                        </SelectItem>
                      ))}
                    </React.Fragment>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <Separator className="my-2" />
            <div className="text-xs font-medium text-muted-foreground mb-2">Line Parameters</div>
            {renderField("Length (km)", "lengthKm", "number")}
            {renderField("Resistance (Ω/km)", "resistanceOhmPerKm", "number")}
            {renderField("Reactance (Ω/km)", "reactanceOhmPerKm", "number")}
            {renderField("Susceptance (S/km)", "susceptanceSPerKm", "number")}
          </>
        );
      }
      case "transformer":
        return (
          <>
            {renderField("Rating (MVA)", "ratingMVA", "number")}
            {renderField("Primary Voltage (kV)", "primaryVoltageKV", "number")}
            {renderField("Secondary Voltage (kV)", "secondaryVoltageKV", "number")}
            {renderField("Impedance (%)", "impedancePercent", "number")}
            {renderField("X/R Ratio", "xrRatio", "number")}
            {renderField("Tap Position", "tapPosition", "number")}
            {renderField("Connection Type", "connectionType", "select", [
              { value: "Yg-Yg", label: "Yg-Yg" },
              { value: "Yg-D", label: "Yg-D (Delta secondary)" },
              { value: "D-Yg", label: "D-Yg (Delta primary)" },
              { value: "D-D", label: "D-D" },
            ])}
          </>
        );
      case "load":
        return (
          <>
            {renderField("Active Power (kW)", "activePowerKW", "number")}
            {renderField("Reactive Power (kVAR)", "reactivePowerKVAR", "number")}
            {renderField("Load Model", "loadModel", "select", [
              { value: "constant_power", label: "Constant Power" },
              { value: "constant_current", label: "Constant Current" },
              { value: "constant_impedance", label: "Constant Impedance" },
            ])}
            {renderField("Unbalanced", "unbalanced", "boolean")}
            {(element as any).unbalanced && (
              <>
                <Separator className="my-2" />
                <div className="text-xs font-medium text-muted-foreground mb-2">Phase Distribution (%)</div>
                {renderField("Phase A", "phaseAPower", "number")}
                {renderField("Phase B", "phaseBPower", "number")}
                {renderField("Phase C", "phaseCPower", "number")}
              </>
            )}
          </>
        );
      case "generator":
        return (
          <>
            {renderField("Rating (MVA)", "ratingMVA", "number")}
            {renderField("Active Power (MW)", "activePowerMW", "number")}
            {renderField("Voltage Setpoint (p.u.)", "voltageSetpointPU", "number")}
            {renderField("Min Q (MVAR)", "minReactivePowerMVAR", "number")}
            {renderField("Max Q (MVAR)", "maxReactivePowerMVAR", "number")}
          </>
        );
      case "battery":
        return (
          <>
            {renderField("Capacity (kWh)", "capacityKWh", "number")}
            {renderField("Max Power (kW)", "maxPowerKW", "number")}
            {renderField("State of Charge (%)", "stateOfCharge", "number")}
            {renderField("Charging Efficiency", "chargingEfficiency", "number")}
            {renderField("Discharging Efficiency", "dischargingEfficiency", "number")}
          </>
        );
      case "capacitor":
        return (
          <>
            {renderField("Rating (kVAR)", "ratingKVAR", "number")}
            {renderField("Nominal Voltage (kV)", "nominalVoltageKV", "number")}
            {renderField("Steps", "steps", "number")}
            {renderField("Current Step", "currentStep", "number")}
          </>
        );
      case "switch":
        return (
          <>
            {renderField("Closed", "isClosed", "boolean")}
            {renderField("Rated Current (A)", "ratedCurrentA", "number")}
          </>
        );
      case "cable":
        return (
          <>
            {renderField("Length (km)", "lengthKm", "number")}
            {renderField("Resistance (Ω/km)", "resistanceOhmPerKm", "number")}
            {renderField("Reactance (Ω/km)", "reactanceOhmPerKm", "number")}
            {renderField("Capacitance (µF/km)", "capacitanceuFPerKm", "number")}
            {renderField("Rated Current (A)", "ratedCurrentA", "number")}
          </>
        );
      case "ductbank":
        {
          const rows = Math.max(1, Number((element as any).rows ?? 1));
          const columns = Math.max(1, Number((element as any).columns ?? 1));
          const defaultDiameter = Number((element as any).ductDiameterIn ?? 6);
          const defaultThickness = Number((element as any).thickness ?? 6);
          const existingDucts = Array.isArray((element as any).ducts)
            ? (element as any).ducts as Array<{ row: number; column: number; diameter: number; thickness: number; load: number; loadFactor: number }>
            : [];

          const ducts = Array.from({ length: rows * columns }, (_, idx) => {
            const row = Math.floor(idx / columns) + 1;
            const column = (idx % columns) + 1;
            const match = existingDucts.find((duct) => duct.row === row && duct.column === column);
            return {
              row,
              column,
              diameter: match?.diameter ?? defaultDiameter,
              thickness: match?.thickness ?? defaultThickness,
              load: match?.load ?? 0,
              loadFactor: match?.loadFactor ?? 1,
            };
          });

          const updateDuct = (index: number, field: "diameter" | "thickness" | "load" | "loadFactor", value: number) => {
            const next = ducts.map((duct, ductIndex) => {
              if (ductIndex !== index) return duct;
              return { ...duct, [field]: value };
            });
            handleChange("ducts", next);
          };

          return (
            <>
              {renderField("Rows", "rows", "number")}
              {renderField("Columns", "columns", "number")}
              {renderField("Thickness (in)", "thickness", "number")}
              {renderField("Vertical Spacing (in)", "verticalSpacing", "number")}
              {renderField("Horizontal Spacing (in)", "horizontalSpacing", "number")}
              {renderField("Depth (in)", "depth", "number")}
              {renderField("Soil Resistivity (ohm-m)", "soilResistivity", "number")}
              {renderField("Duct Diameter (in)", "ductDiameterIn", "number")}
              <Separator className="my-2" />
              <div className="text-xs font-medium text-muted-foreground mb-2">Ducts</div>
              <div className="rounded-md border border-border/60">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="h-9 px-2 text-xs">Row</TableHead>
                      <TableHead className="h-9 px-2 text-xs">Column</TableHead>
                      <TableHead className="h-9 px-2 text-xs">Diameter</TableHead>
                      <TableHead className="h-9 px-2 text-xs">Thickness</TableHead>
                      <TableHead className="h-9 px-2 text-xs">Load (A)</TableHead>
                      <TableHead className="h-9 px-2 text-xs">Load Factor</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {ducts.map((duct, index) => (
                      <TableRow key={`${duct.row}-${duct.column}`}>
                        <TableCell className="px-2 py-1.5 text-xs text-muted-foreground">{duct.row}</TableCell>
                        <TableCell className="px-2 py-1.5 text-xs text-muted-foreground">{duct.column}</TableCell>
                        <TableCell className="px-2 py-1.5">
                          <Input
                            type="number"
                            value={duct.diameter}
                            onChange={(e) => updateDuct(index, "diameter", parseFloat(e.target.value) || 0)}
                            className="h-7 text-xs"
                          />
                        </TableCell>
                        <TableCell className="px-2 py-1.5">
                          <Input
                            type="number"
                            value={duct.thickness}
                            onChange={(e) => updateDuct(index, "thickness", parseFloat(e.target.value) || 0)}
                            className="h-7 text-xs"
                          />
                        </TableCell>
                        <TableCell className="px-2 py-1.5">
                          <Input
                            type="number"
                            value={duct.load}
                            onChange={(e) => updateDuct(index, "load", parseFloat(e.target.value) || 0)}
                            className="h-7 text-xs"
                          />
                        </TableCell>
                        <TableCell className="px-2 py-1.5">
                          <Input
                            type="number"
                            value={duct.loadFactor}
                            onChange={(e) => updateDuct(index, "loadFactor", parseFloat(e.target.value) || 0)}
                            className="h-7 text-xs"
                          />
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
              <div className="flex justify-end pt-2">
                <Button
                  size="sm"
                  onClick={async () => {
                    if (isCalculating) return;
                    setIsCalculating(true);
                    try {
                      const response = await apiRequest("POST", "/api/ductbank/steady-state", {
                        ductbank: element,
                      });
                      const payload = await response.json();
                      const temps = Array.isArray(payload?.temperatures) ? payload.temperatures : [];
                      const maxTemp = temps.length ? Math.max(...temps) : null;
                      if (onThermalResult) {
                        onThermalResult({
                          ductbankId: element.id,
                          ductbankName: element.name,
                          rows: Number(payload?.rows ?? (element as any).rows ?? 0),
                          columns: Number(payload?.columns ?? (element as any).columns ?? 0),
                          temperatures: temps,
                          timestamp: new Date().toISOString(),
                        });
                      }
                      toast({
                        title: "Temperature calculated",
                        description: maxTemp != null ? `Max steady-state temperature: ${maxTemp.toFixed(2)}` : "No temperature results returned.",
                      });
                    } catch (error: any) {
                      toast({
                        title: "Calculation failed",
                        description: error?.message || "Unable to calculate temperatures.",
                        variant: "destructive",
                      });
                    } finally {
                      setIsCalculating(false);
                    }
                  }}
                  disabled={isCalculating}
                  data-testid="button-ductbank-calc-temperature"
                >
                  {isCalculating ? "Calculating..." : "Calculate Temperature"}
                </Button>
              </div>
            </>
          );
        }
      default:
        return null;
    }
  };

  return (
    <ScrollArea className="h-full custom-scrollbar">
      <div className="p-4 space-y-4">
        <Card>
          <CardHeader className="pb-3">
            <div className="flex items-center gap-3">
              <div className={`p-2 rounded-md ${colors.bg} ${colors.text}`}>
                <ElementIcon type={element.type} size={20} />
              </div>
              <div className="flex-1 min-w-0">
                <CardTitle className="text-base truncate">{element.name}</CardTitle>
                <Badge variant="secondary" className="mt-1">
                  {elementLabels[element.type]}
                </Badge>
              </div>
            </div>
          </CardHeader>
          <CardContent className="pt-0">
            <Separator className="mb-3" />
            <div className="space-y-1">
              {renderField("Name", "name", "text")}
              {renderField("Enabled", "enabled", "boolean")}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Element Parameters
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <div className="space-y-1">
              {renderTypeSpecificFields()}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Position
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-1.5">
                <Label className="text-sm text-muted-foreground">X</Label>
                <Input
                  type="number"
                  value={element.x}
                  onChange={(e) => handleChange("x", parseInt(e.target.value) || 0)}
                  className="h-8"
                  data-testid="input-x"
                />
              </div>
              <div className="space-y-1.5">
                <Label className="text-sm text-muted-foreground">Y</Label>
                <Input
                  type="number"
                  value={element.y}
                  onChange={(e) => handleChange("y", parseInt(e.target.value) || 0)}
                  className="h-8"
                  data-testid="input-y"
                />
              </div>
            </div>
            <div className="space-y-1.5 mt-3">
              <Label className="text-sm text-muted-foreground">Rotation (deg)</Label>
              <Input
                type="number"
                value={element.rotation || 0}
                onChange={(e) => handleChange("rotation", parseInt(e.target.value) || 0)}
                className="h-8"
                step={15}
                data-testid="input-rotation"
              />
            </div>
          </CardContent>
        </Card>

        {element.type === "ductbank" && (
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Load Profile
              </CardTitle>
            </CardHeader>
            <CardContent className="pt-0 space-y-3">
              <div className="rounded-md border border-border/60 p-2">
                {loadProfiles.length > 0 ? (
                  <ChartContainer config={loadProfileChartConfig} className="h-64 w-full">
                    <LineChart data={loadProfileChartData} margin={{ top: 8, right: 8, bottom: 8, left: 8 }}>
                      <CartesianGrid vertical={false} />
                      <XAxis dataKey="time" tickLine={false} axisLine={false} minTickGap={16} />
                      <YAxis
                        yAxisId="load"
                        tickLine={false}
                        axisLine={false}
                        width={48}
                        label={{ value: "Load (A)", angle: -90, position: "insideLeft", offset: -2 }}
                      />
                      <YAxis
                        yAxisId="temperature"
                        orientation="right"
                        tickLine={false}
                        axisLine={false}
                        width={48}
                        label={{ value: "Temp (°C)", angle: 90, position: "insideRight", offset: -2 }}
                      />
                      <ChartTooltip content={<ChartTooltipContent indicator="line" />} />
                      <Legend />
                      {loadProfiles.map((series) => (
                        <Line
                          key={series.key}
                          yAxisId="load"
                          type="monotone"
                          dataKey={series.key}
                          stroke={`var(--color-${series.key})`}
                          strokeWidth={2}
                          dot={false}
                        />
                      ))}
                      {transientTemperatures.map((series) => (
                        <Line
                          key={`temp_${series.key}`}
                          yAxisId="temperature"
                          type="monotone"
                          dataKey={`temp_${series.key}`}
                          stroke={`var(--color-temp_${series.key})`}
                          strokeWidth={2}
                          strokeDasharray="4 3"
                          dot={false}
                        />
                      ))}
                    </LineChart>
                  </ChartContainer>
                ) : (
                  <div className="h-64 w-full flex items-center justify-center text-xs text-muted-foreground">
                    Generate load profiles to display the time series.
                  </div>
                )}
              </div>

              <div className="flex flex-wrap gap-2">
                <Button
                  size="sm"
                  variant="outline"
                  onClick={handleGenerateLoadProfiles}
                  disabled={isGeneratingLoadProfiles}
                  data-testid="button-generate-load-profiles"
                >
                  {isGeneratingLoadProfiles ? "Generating..." : "Generate Load Profiles"}
                </Button>
                <Button
                  size="sm"
                  onClick={handleCalculateTransientTemps}
                  disabled={isCalculatingTransientTemps || loadProfiles.length === 0}
                  data-testid="button-calculate-transient-temps"
                >
                  {isCalculatingTransientTemps ? "Calculating..." : "Calculate Transient Temps"}
                </Button>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </ScrollArea>
  );
}
