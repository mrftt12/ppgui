import { useEffect, useState } from "react";
import { CheckCircle2, ChevronDown, ChevronUp, XCircle } from "lucide-react";
import type { LoadFlowResult } from "@shared/schema";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";

interface ResultsDockProps {
  result: LoadFlowResult | null;
  thermalResult?: {
    ductbankId: string;
    ductbankName: string;
    rows: number;
    columns: number;
    temperatures: number[];
    timestamp: string;
  } | null;
  collapsed: boolean;
  onToggleCollapse: () => void;
}

function formatNumber(value: number, digits = 3) {
  if (!Number.isFinite(value)) return "-";
  return value.toFixed(digits);
}

export function ResultsDock({ result, thermalResult, collapsed, onToggleCollapse }: ResultsDockProps) {
  const [activeTab, setActiveTab] = useState("summary");

  const busRows = result?.busResults ?? [];
  const branchRows = result?.branchResults ?? [];
  const hasLoadflow = Boolean(result);
  const hasThermal = Boolean(thermalResult);

  const availableTabs = [
    ...(hasLoadflow ? ["summary", "buses", "branches"] : []),
    ...(hasThermal ? ["cables"] : []),
  ];

  useEffect(() => {
    if (availableTabs.length > 0 && !availableTabs.includes(activeTab)) {
      setActiveTab(availableTabs[0]);
    }
  }, [activeTab, availableTabs]);

  return (
    <div className="h-full border-t bg-card/95 backdrop-blur flex flex-col">
      <div className="flex items-center justify-between gap-2 px-4 py-2 text-sm font-semibold border-b">
        <div className="flex items-center gap-2">
          {result ? (
            result.converged ? (
              <CheckCircle2 className="h-4 w-4 text-emerald-500" />
            ) : (
              <XCircle className="h-4 w-4 text-rose-500" />
            )
          ) : (
            <CheckCircle2 className="h-4 w-4 text-muted-foreground" />
          )}
          Results
          {result && (
            <Badge variant={result.converged ? "default" : "destructive"} className="text-[10px]">
              {result.converged ? "Converged" : "Not Converged"}
            </Badge>
          )}
          {hasThermal && (
            <Badge variant="secondary" className="text-[10px]">
              Cable Temps
            </Badge>
          )}
        </div>
        <button
          type="button"
          className="text-muted-foreground hover:text-foreground"
          onClick={onToggleCollapse}
          aria-label={collapsed ? "Expand results panel" : "Collapse results panel"}
        >
          {collapsed ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
        </button>
      </div>

      <div className="flex-1 overflow-hidden">
        {!result && !hasThermal ? (
          <div className="h-full flex items-center justify-center text-sm text-muted-foreground">
            Run an analysis to see results here.
          </div>
        ) : (
          <Tabs value={activeTab} onValueChange={setActiveTab} className="h-full flex flex-col">
            <TabsList className="m-3 mb-2 self-start">
              {hasLoadflow && <TabsTrigger value="summary">Summary</TabsTrigger>}
              {hasLoadflow && <TabsTrigger value="buses">Buses</TabsTrigger>}
              {hasLoadflow && <TabsTrigger value="branches">Branches</TabsTrigger>}
              {hasThermal && <TabsTrigger value="cables">Cable Temps</TabsTrigger>}
            </TabsList>

            {hasLoadflow && (
              <TabsContent value="summary" className="flex-1 overflow-auto px-4 pb-4">
              <div className="grid gap-4 sm:grid-cols-3">
                <div className="rounded-md border bg-background/60 p-3">
                  <div className="text-xs text-muted-foreground">Iterations</div>
                  <div className="text-lg font-semibold">{result.iterations}</div>
                </div>
                <div className="rounded-md border bg-background/60 p-3">
                  <div className="text-xs text-muted-foreground">Buses</div>
                  <div className="text-lg font-semibold">{busRows.length}</div>
                </div>
                <div className="rounded-md border bg-background/60 p-3">
                  <div className="text-xs text-muted-foreground">Branches</div>
                  <div className="text-lg font-semibold">{branchRows.length}</div>
                </div>
              </div>
              </TabsContent>
            )}

            {hasLoadflow && (
              <TabsContent value="buses" className="flex-1 overflow-auto px-4 pb-4">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Bus</TableHead>
                    <TableHead>Va (p.u.)</TableHead>
                    <TableHead>Va (deg)</TableHead>
                    <TableHead>Vb (p.u.)</TableHead>
                    <TableHead>Vb (deg)</TableHead>
                    <TableHead>Vc (p.u.)</TableHead>
                    <TableHead>Vc (deg)</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {busRows.map((bus) => (
                    <TableRow key={bus.busId}>
                      <TableCell className="font-medium">{bus.busName}</TableCell>
                      <TableCell>{formatNumber(bus.voltagePhaseA.magnitude)}</TableCell>
                      <TableCell>{formatNumber(bus.voltagePhaseA.angle)}</TableCell>
                      <TableCell>{formatNumber(bus.voltagePhaseB.magnitude)}</TableCell>
                      <TableCell>{formatNumber(bus.voltagePhaseB.angle)}</TableCell>
                      <TableCell>{formatNumber(bus.voltagePhaseC.magnitude)}</TableCell>
                      <TableCell>{formatNumber(bus.voltagePhaseC.angle)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              </TabsContent>
            )}

            {hasLoadflow && (
              <TabsContent value="branches" className="flex-1 overflow-auto px-4 pb-4">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Branch</TableHead>
                    <TableHead>Ia (A)</TableHead>
                    <TableHead>Ib (A)</TableHead>
                    <TableHead>Ic (A)</TableHead>
                    <TableHead>P (MW)</TableHead>
                    <TableHead>Q (MVAR)</TableHead>
                    <TableHead>Losses</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {branchRows.map((branch) => (
                    <TableRow key={branch.branchId}>
                      <TableCell className="font-medium">{branch.branchName}</TableCell>
                      <TableCell>{formatNumber(branch.currentPhaseA)}</TableCell>
                      <TableCell>{formatNumber(branch.currentPhaseB)}</TableCell>
                      <TableCell>{formatNumber(branch.currentPhaseC)}</TableCell>
                      <TableCell>{formatNumber(branch.powerFlowMW)}</TableCell>
                      <TableCell>{formatNumber(branch.powerFlowMVAR)}</TableCell>
                      <TableCell>{formatNumber(branch.losses)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              </TabsContent>
            )}

            {hasThermal && thermalResult && (
              <TabsContent value="cables" className="flex-1 overflow-auto px-4 pb-4">
                <div className="mb-3 flex flex-wrap gap-2 text-xs text-muted-foreground">
                  <span>Ductbank: {thermalResult.ductbankName}</span>
                  <span>Rows: {thermalResult.rows}</span>
                  <span>Columns: {thermalResult.columns}</span>
                </div>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Row</TableHead>
                      <TableHead>Column</TableHead>
                      <TableHead>Temp (C)</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {thermalResult.temperatures.map((temp, index) => {
                      const row = Math.floor(index / thermalResult.columns) + 1;
                      const column = (index % thermalResult.columns) + 1;
                      return (
                        <TableRow key={`${row}-${column}`}>
                          <TableCell className="font-medium">{row}</TableCell>
                          <TableCell>{column}</TableCell>
                          <TableCell>{formatNumber(temp, 2)}</TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </TabsContent>
            )}
          </Tabs>
        )}
      </div>
    </div>
  );
}
