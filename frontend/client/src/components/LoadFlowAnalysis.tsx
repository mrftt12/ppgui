import { useState } from "react";
import type { LoadFlowResult, NetworkModel } from "@shared/schema";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Progress } from "@/components/ui/progress";
import {
  Play,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Zap,
  Activity,
} from "lucide-react";

type AnalysisType =
  | "loadflow"
  | "load_allocation"
  | "short_circuit"
  | "hosting_capacity"
  | "thermal";

interface ShortCircuitParams {
  faultType: string;
  caseType: string;
  ip: boolean;
  ith: boolean;
}

interface ShortCircuitResult {
  success: boolean;
  error?: string;
  res_bus_sc?: Array<Record<string, unknown>>;
  res_line_sc?: Array<Record<string, unknown>>;
}

interface LoadFlowAnalysisProps {
  network: NetworkModel | null;
  onRunAnalysis: () => void;
  onRunShortCircuit?: (params: ShortCircuitParams) => void;
  result: LoadFlowResult | null;
  shortCircuitResult?: ShortCircuitResult | null;
  isRunning: boolean;
  isRunningShortCircuit?: boolean;
}

export function LoadFlowAnalysis({
  network,
  onRunAnalysis,
  onRunShortCircuit,
  result,
  shortCircuitResult,
  isRunning,
  isRunningShortCircuit,
}: LoadFlowAnalysisProps) {
  const [activeTab, setActiveTab] = useState("summary");
  const [analysisType, setAnalysisType] = useState<AnalysisType>("loadflow");

  const [loadflowParams, setLoadflowParams] = useState({
    tolVmag: 1e-5,
    tolVang: 1e-5,
    maxIter: 100,
    runControl: false,
  });

  const [loadAllocationParams, setLoadAllocationParams] = useState({
    maxIter: 50,
    tolerance: 1e-4,
    measurementStdDev: 0.01,
    weighting: "balanced",
  });

  const [shortCircuitParams, setShortCircuitParams] = useState<ShortCircuitParams>({
    faultType: "LLL",
    caseType: "max",
    ip: true,
    ith: true,
  });

  const [hostingCapacityParams, setHostingCapacityParams] = useState({
    maxIter: 30,
    voltageLimit: 1.05,
    thermalLimitPct: 100,
    strategy: "streamlined",
  });

  const [thermalParams, setThermalParams] = useState({
    ambientC: 25,
    conductorLimitC: 90,
    timestepMin: 15,
    durationHours: 24,
  });

  const isLoadFlow = analysisType === "loadflow";

  if (!network) {
    return (
      <div className="h-full flex items-center justify-center p-4">
        <div className="text-center">
          <Activity className="h-10 w-10 text-muted-foreground/30 mx-auto mb-3" />
          <p className="text-sm text-muted-foreground">No network loaded</p>
          <p className="text-xs text-muted-foreground/70 mt-1">
            Create or load a network model to run analysis
          </p>
        </div>
      </div>
    );
  }

  const busCount = network.elements.filter((e) => e.type === "bus").length;
  const loadCount = network.elements.filter((e) => e.type === "load").length;
  const generatorCount = network.elements.filter(
    (e) => e.type === "generator" || e.type === "external_source"
  ).length;

  return (
    <div className="flex flex-col h-full">
      <div className="p-4 border-b space-y-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Zap className="h-5 w-5 text-primary" />
            <h3 className="font-semibold">Analysis</h3>
          </div>
          {isLoadFlow && result && (
            <Badge
              variant={result.converged ? "default" : "destructive"}
              className="gap-1"
            >
              {result.converged ? (
                <CheckCircle2 className="h-3 w-3" />
              ) : (
                <XCircle className="h-3 w-3" />
              )}
              {result.converged ? "Converged" : "Failed"}
            </Badge>
          )}
        </div>

        <div className="space-y-2">
          <Label className="text-xs text-muted-foreground">Analysis Type</Label>
          <Select
            value={analysisType}
            onValueChange={(value) =>
              setAnalysisType(value as AnalysisType)
            }
          >
            <SelectTrigger className="h-8 text-xs">
              <SelectValue placeholder="Select analysis" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="loadflow">Load Flow Analysis</SelectItem>
              <SelectItem value="load_allocation">Load Allocation</SelectItem>
              <SelectItem value="short_circuit">Short Circuit Duty</SelectItem>
              <SelectItem value="hosting_capacity">Hosting Capacity Analysis</SelectItem>
              <SelectItem value="thermal">Thermal Analysis</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <Card className="bg-muted/30">
          <CardContent className="p-3 space-y-3">
            {analysisType === "loadflow" && (
              <>
                <div className="grid grid-cols-2 gap-2">
                  <div className="space-y-1">
                    <Label className="text-xs">tol_vmag_pu</Label>
                    <Input
                      type="number"
                      step="0.000001"
                      value={loadflowParams.tolVmag}
                      onChange={(e) =>
                        setLoadflowParams((prev) => ({
                          ...prev,
                          tolVmag: Number(e.target.value),
                        }))
                      }
                    />
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">tol_vang_rad</Label>
                    <Input
                      type="number"
                      step="0.000001"
                      value={loadflowParams.tolVang}
                      onChange={(e) =>
                        setLoadflowParams((prev) => ({
                          ...prev,
                          tolVang: Number(e.target.value),
                        }))
                      }
                    />
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">MaxIter</Label>
                    <Input
                      type="number"
                      step="1"
                      value={loadflowParams.maxIter}
                      onChange={(e) =>
                        setLoadflowParams((prev) => ({
                          ...prev,
                          maxIter: Number(e.target.value),
                        }))
                      }
                    />
                  </div>
                  <div className="flex items-center justify-between gap-2 pt-5">
                    <Label className="text-xs">run_control</Label>
                    <Switch
                      checked={loadflowParams.runControl}
                      onCheckedChange={(checked) =>
                        setLoadflowParams((prev) => ({
                          ...prev,
                          runControl: checked,
                        }))
                      }
                    />
                  </div>
                </div>
              </>
            )}

            {analysisType === "load_allocation" && (
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <Label className="text-xs">MaxIter</Label>
                  <Input
                    type="number"
                    step="1"
                    value={loadAllocationParams.maxIter}
                    onChange={(e) =>
                      setLoadAllocationParams((prev) => ({
                        ...prev,
                        maxIter: Number(e.target.value),
                      }))
                    }
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">tolerance</Label>
                  <Input
                    type="number"
                    step="0.0001"
                    value={loadAllocationParams.tolerance}
                    onChange={(e) =>
                      setLoadAllocationParams((prev) => ({
                        ...prev,
                        tolerance: Number(e.target.value),
                      }))
                    }
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">measurement_std_dev</Label>
                  <Input
                    type="number"
                    step="0.001"
                    value={loadAllocationParams.measurementStdDev}
                    onChange={(e) =>
                      setLoadAllocationParams((prev) => ({
                        ...prev,
                        measurementStdDev: Number(e.target.value),
                      }))
                    }
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">weighting</Label>
                  <Select
                    value={loadAllocationParams.weighting}
                    onValueChange={(value) =>
                      setLoadAllocationParams((prev) => ({
                        ...prev,
                        weighting: value,
                      }))
                    }
                  >
                    <SelectTrigger className="h-8 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="balanced">balanced</SelectItem>
                      <SelectItem value="loads">loads</SelectItem>
                      <SelectItem value="measurements">measurements</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            )}

            {analysisType === "short_circuit" && (
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <Label className="text-xs">fault_type</Label>
                  <Select
                    value={shortCircuitParams.faultType}
                    onValueChange={(value) =>
                      setShortCircuitParams((prev) => ({
                        ...prev,
                        faultType: value,
                      }))
                    }
                  >
                    <SelectTrigger className="h-8 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="LLL">LLL (3-phase)</SelectItem>
                      <SelectItem value="LL">LL (line-to-line)</SelectItem>
                      <SelectItem value="LG">LG (line-to-ground)</SelectItem>
                      <SelectItem value="LLG">LLG (line-line-ground)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">case</Label>
                  <Select
                    value={shortCircuitParams.caseType}
                    onValueChange={(value) =>
                      setShortCircuitParams((prev) => ({
                        ...prev,
                        caseType: value,
                      }))
                    }
                  >
                    <SelectTrigger className="h-8 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="max">max</SelectItem>
                      <SelectItem value="min">min</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="flex items-center justify-between gap-2 pt-5">
                  <Label className="text-xs">ip (peak)</Label>
                  <Switch
                    checked={shortCircuitParams.ip}
                    onCheckedChange={(checked) =>
                      setShortCircuitParams((prev) => ({
                        ...prev,
                        ip: checked,
                      }))
                    }
                  />
                </div>
                <div className="flex items-center justify-between gap-2 pt-5">
                  <Label className="text-xs">ith (thermal)</Label>
                  <Switch
                    checked={shortCircuitParams.ith}
                    onCheckedChange={(checked) =>
                      setShortCircuitParams((prev) => ({
                        ...prev,
                        ith: checked,
                      }))
                    }
                  />
                </div>
              </div>
            )}

            {analysisType === "hosting_capacity" && (
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <Label className="text-xs">MaxIter</Label>
                  <Input
                    type="number"
                    step="1"
                    value={hostingCapacityParams.maxIter}
                    onChange={(e) =>
                      setHostingCapacityParams((prev) => ({
                        ...prev,
                        maxIter: Number(e.target.value),
                      }))
                    }
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">voltage_limit_pu</Label>
                  <Input
                    type="number"
                    step="0.01"
                    value={hostingCapacityParams.voltageLimit}
                    onChange={(e) =>
                      setHostingCapacityParams((prev) => ({
                        ...prev,
                        voltageLimit: Number(e.target.value),
                      }))
                    }
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">thermal_limit_pct</Label>
                  <Input
                    type="number"
                    step="1"
                    value={hostingCapacityParams.thermalLimitPct}
                    onChange={(e) =>
                      setHostingCapacityParams((prev) => ({
                        ...prev,
                        thermalLimitPct: Number(e.target.value),
                      }))
                    }
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">strategy</Label>
                  <Select
                    value={hostingCapacityParams.strategy}
                    onValueChange={(value) =>
                      setHostingCapacityParams((prev) => ({
                        ...prev,
                        strategy: value,
                      }))
                    }
                  >
                    <SelectTrigger className="h-8 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="streamlined">streamlined</SelectItem>
                      <SelectItem value="iterative">iterative</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            )}

            {analysisType === "thermal" && (
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <Label className="text-xs">ambient_c</Label>
                  <Input
                    type="number"
                    step="1"
                    value={thermalParams.ambientC}
                    onChange={(e) =>
                      setThermalParams((prev) => ({
                        ...prev,
                        ambientC: Number(e.target.value),
                      }))
                    }
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">conductor_limit_c</Label>
                  <Input
                    type="number"
                    step="1"
                    value={thermalParams.conductorLimitC}
                    onChange={(e) =>
                      setThermalParams((prev) => ({
                        ...prev,
                        conductorLimitC: Number(e.target.value),
                      }))
                    }
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">timestep_min</Label>
                  <Input
                    type="number"
                    step="1"
                    value={thermalParams.timestepMin}
                    onChange={(e) =>
                      setThermalParams((prev) => ({
                        ...prev,
                        timestepMin: Number(e.target.value),
                      }))
                    }
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">duration_hours</Label>
                  <Input
                    type="number"
                    step="1"
                    value={thermalParams.durationHours}
                    onChange={(e) =>
                      setThermalParams((prev) => ({
                        ...prev,
                        durationHours: Number(e.target.value),
                      }))
                    }
                  />
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <Button
          className="w-full"
          onClick={onRunAnalysis}
          disabled={!isLoadFlow || isRunning || network.elements.length === 0}
          data-testid="button-run-analysis"
        >
          {isRunning ? (
            <>
              <Activity className="h-4 w-4 mr-2 animate-spin" />
              Running Analysis...
            </>
          ) : (
            <>
              <Play className="h-4 w-4 mr-2" />
              {analysisType === "loadflow" && "Run Load Flow Analysis"}
              {analysisType === "load_allocation" && "Run Load Allocation"}
              {analysisType === "short_circuit" && "Run Short Circuit Duty"}
              {analysisType === "hosting_capacity" && "Run Hosting Capacity"}
              {analysisType === "thermal" && "Run Thermal Analysis"}
            </>
          )}
        </Button>
        {!isLoadFlow && (
          <p className="text-xs text-muted-foreground">
            Configuration only. Execution is not wired yet for this analysis.
          </p>
        )}

        {isRunning && (
          <Progress value={65} className="h-1" />
        )}
      </div>

      <ScrollArea className="flex-1 custom-scrollbar">
        <div className="p-4 space-y-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Network Summary
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-3 gap-3">
                <div className="text-center p-2 bg-muted/50 rounded-md">
                  <div className="text-2xl font-bold text-primary">{busCount}</div>
                  <div className="text-xs text-muted-foreground">Buses</div>
                </div>
                <div className="text-center p-2 bg-muted/50 rounded-md">
                  <div className="text-2xl font-bold text-rose-500">{loadCount}</div>
                  <div className="text-xs text-muted-foreground">Loads</div>
                </div>
                <div className="text-center p-2 bg-muted/50 rounded-md">
                  <div className="text-2xl font-bold text-green-500">{generatorCount}</div>
                  <div className="text-xs text-muted-foreground">Sources</div>
                </div>
              </div>
            </CardContent>
          </Card>

          {isLoadFlow && result && (
            <>
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground">
                    Analysis Results
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2 text-sm">
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Status</span>
                      <Badge variant={result.converged ? "default" : "destructive"}>
                        {result.converged ? "Converged" : "Did not converge"}
                      </Badge>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Iterations</span>
                      <span className="font-medium">{result.iterations}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Timestamp</span>
                      <span className="font-mono text-xs">
                        {new Date(result.timestamp).toLocaleTimeString()}
                      </span>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Tabs value={activeTab} onValueChange={setActiveTab}>
                <TabsList className="grid w-full grid-cols-2">
                  <TabsTrigger value="buses" data-testid="tab-bus-results">Bus Results</TabsTrigger>
                  <TabsTrigger value="branches" data-testid="tab-branch-results">Branch Results</TabsTrigger>
                </TabsList>

                <TabsContent value="buses" className="mt-3">
                  <Card>
                    <CardContent className="p-0">
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Bus</TableHead>
                            <TableHead className="text-right">|Va|</TableHead>
                            <TableHead className="text-right">|Vb|</TableHead>
                            <TableHead className="text-right">|Vc|</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {result.busResults.map((bus) => {
                            const lowVoltage =
                              bus.voltagePhaseA.magnitude < 0.95 ||
                              bus.voltagePhaseB.magnitude < 0.95 ||
                              bus.voltagePhaseC.magnitude < 0.95;
                            return (
                              <TableRow key={bus.busId}>
                                <TableCell className="font-medium">
                                  <div className="flex items-center gap-2">
                                    {lowVoltage && (
                                      <AlertTriangle className="h-3 w-3 text-amber-500" />
                                    )}
                                    {bus.busName}
                                  </div>
                                </TableCell>
                                <TableCell className="text-right font-mono text-xs">
                                  {bus.voltagePhaseA.magnitude.toFixed(4)} pu
                                </TableCell>
                                <TableCell className="text-right font-mono text-xs">
                                  {bus.voltagePhaseB.magnitude.toFixed(4)} pu
                                </TableCell>
                                <TableCell className="text-right font-mono text-xs">
                                  {bus.voltagePhaseC.magnitude.toFixed(4)} pu
                                </TableCell>
                              </TableRow>
                            );
                          })}
                        </TableBody>
                      </Table>
                    </CardContent>
                  </Card>
                </TabsContent>

                <TabsContent value="branches" className="mt-3">
                  <Card>
                    <CardContent className="p-0">
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Branch</TableHead>
                            <TableHead className="text-right">P (MW)</TableHead>
                            <TableHead className="text-right">Q (MVAR)</TableHead>
                            <TableHead className="text-right">Loss</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {result.branchResults.map((branch) => (
                            <TableRow key={branch.branchId}>
                              <TableCell className="font-medium">
                                {branch.branchName}
                              </TableCell>
                              <TableCell className="text-right font-mono text-xs">
                                {branch.powerFlowMW.toFixed(3)}
                              </TableCell>
                              <TableCell className="text-right font-mono text-xs">
                                {branch.powerFlowMVAR.toFixed(3)}
                              </TableCell>
                              <TableCell className="text-right font-mono text-xs">
                                {(branch.losses * 100).toFixed(2)}%
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </CardContent>
                  </Card>
                </TabsContent>
              </Tabs>
            </>
          )}

          {!result && !isRunning && (
            <div className="text-center py-8">
              <Activity className="h-10 w-10 text-muted-foreground/30 mx-auto mb-3" />
              <p className="text-sm text-muted-foreground">
                No analysis results yet
              </p>
              <p className="text-xs text-muted-foreground/70 mt-1">
                Click "Run Three-Phase Analysis" to start
              </p>
            </div>
          )}
        </div>
      </ScrollArea>
    </div>
  );
}
