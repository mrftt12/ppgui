import { useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import type { LoadFlowResult } from "@shared/schema";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { AlertTriangle, CheckCircle2, XCircle } from "lucide-react";

/**
 * Voltage violation thresholds (per-unit)
 */
const VOLTAGE_THRESHOLDS = {
    LOW_CRITICAL: 0.90,
    LOW_WARNING: 0.95,
    HIGH_WARNING: 1.05,
    HIGH_CRITICAL: 1.10,
};

/**
 * Loading violation thresholds (percent)
 */
const LOADING_THRESHOLDS = {
    WARNING: 80,
    CRITICAL: 100,
};

/**
 * Get voltage violation class for styling
 */
function getVoltageClass(magnitude: number): string {
    if (magnitude < VOLTAGE_THRESHOLDS.LOW_CRITICAL) return "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200";
    if (magnitude < VOLTAGE_THRESHOLDS.LOW_WARNING) return "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200";
    if (magnitude > VOLTAGE_THRESHOLDS.HIGH_CRITICAL) return "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200";
    if (magnitude > VOLTAGE_THRESHOLDS.HIGH_WARNING) return "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200";
    return "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200";
}

/**
 * Get loading violation level
 */
function getLoadingLevel(loading: number): "normal" | "warning" | "critical" {
    if (loading >= LOADING_THRESHOLDS.CRITICAL) return "critical";
    if (loading >= LOADING_THRESHOLDS.WARNING) return "warning";
    return "normal";
}

/**
 * Get loading progress color
 */
function getLoadingColor(loading: number): string {
    const level = getLoadingLevel(loading);
    if (level === "critical") return "bg-red-500";
    if (level === "warning") return "bg-yellow-500";
    return "bg-green-500";
}

interface LoadFlowResultsProps {
    result: LoadFlowResult | null;
    isLoading?: boolean;
}

/**
 * LoadFlowResults Component
 * 
 * Displays load flow analysis results with:
 * - Convergence status and summary
 * - Bus voltage table with violation highlighting
 * - Branch loading bar chart
 * - Violation summary
 */
export function LoadFlowResults({ result, isLoading }: LoadFlowResultsProps) {
    // Compute violation summary
    const summary = useMemo(() => {
        if (!result) return null;

        let lowVoltage = 0;
        let highVoltage = 0;
        let overloaded = 0;
        let nearLimit = 0;

        for (const bus of result.busResults) {
            const mag = bus.voltagePhaseA.magnitude;
            if (mag < VOLTAGE_THRESHOLDS.LOW_WARNING) lowVoltage++;
            if (mag > VOLTAGE_THRESHOLDS.HIGH_WARNING) highVoltage++;
        }

        for (const branch of result.branchResults) {
            // Estimate loading from power flow (simplified)
            const loading = (branch.powerFlowMW / 10) * 100; // Rough estimate
            if (loading >= LOADING_THRESHOLDS.CRITICAL) overloaded++;
            else if (loading >= LOADING_THRESHOLDS.WARNING) nearLimit++;
        }

        return { lowVoltage, highVoltage, overloaded, nearLimit };
    }, [result]);

    if (isLoading) {
        return (
            <Card className="animate-pulse">
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <div className="h-5 w-5 bg-muted rounded-full" />
                        <div className="h-5 w-32 bg-muted rounded" />
                    </CardTitle>
                </CardHeader>
                <CardContent>
                    <div className="h-20 bg-muted rounded" />
                </CardContent>
            </Card>
        );
    }

    if (!result) {
        return (
            <Card>
                <CardHeader>
                    <CardTitle className="text-muted-foreground text-sm">
                        No Analysis Results
                    </CardTitle>
                </CardHeader>
                <CardContent>
                    <p className="text-sm text-muted-foreground">
                        Run load flow analysis to see results here.
                    </p>
                </CardContent>
            </Card>
        );
    }

    return (
        <div className="space-y-4">
            {/* Convergence Status */}
            <Card>
                <CardHeader className="pb-2">
                    <CardTitle className="flex items-center gap-2 text-base">
                        {result.converged ? (
                            <>
                                <CheckCircle2 className="h-5 w-5 text-green-500" />
                                Converged
                            </>
                        ) : (
                            <>
                                <XCircle className="h-5 w-5 text-red-500" />
                                Did Not Converge
                            </>
                        )}
                    </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground">
                    <div className="flex flex-wrap gap-4">
                        <span>Iterations: {result.iterations}</span>
                        <span>Buses: {result.busResults.length}</span>
                        <span>Branches: {result.branchResults.length}</span>
                    </div>
                </CardContent>
            </Card>

            {/* Violation Summary */}
            {summary && (summary.lowVoltage > 0 || summary.highVoltage > 0 || summary.overloaded > 0 || summary.nearLimit > 0) && (
                <Card className="border-yellow-200 dark:border-yellow-800">
                    <CardHeader className="pb-2">
                        <CardTitle className="flex items-center gap-2 text-base text-yellow-600 dark:text-yellow-400">
                            <AlertTriangle className="h-5 w-5" />
                            Violations Detected
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="flex flex-wrap gap-2">
                            {summary.lowVoltage > 0 && (
                                <Badge variant="destructive">
                                    {summary.lowVoltage} Low Voltage
                                </Badge>
                            )}
                            {summary.highVoltage > 0 && (
                                <Badge variant="destructive">
                                    {summary.highVoltage} High Voltage
                                </Badge>
                            )}
                            {summary.overloaded > 0 && (
                                <Badge variant="destructive">
                                    {summary.overloaded} Overloaded
                                </Badge>
                            )}
                            {summary.nearLimit > 0 && (
                                <Badge variant="secondary">
                                    {summary.nearLimit} Near Limit
                                </Badge>
                            )}
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Bus Voltage Table */}
            <Card>
                <CardHeader className="pb-2">
                    <CardTitle className="text-base">Bus Voltages</CardTitle>
                </CardHeader>
                <CardContent>
                    <div className="max-h-60 overflow-y-auto">
                        <Table>
                            <TableHeader>
                                <TableRow>
                                    <TableHead className="w-24">Bus</TableHead>
                                    <TableHead className="w-20">V (pu)</TableHead>
                                    <TableHead className="w-20">Angle (°)</TableHead>
                                    <TableHead className="w-16">Status</TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {result.busResults.slice(0, 20).map((bus) => {
                                    const mag = bus.voltagePhaseA.magnitude;
                                    const angle = bus.voltagePhaseA.angle;
                                    const voltageClass = getVoltageClass(mag);

                                    return (
                                        <TableRow key={bus.busId}>
                                            <TableCell className="font-medium truncate max-w-24" title={bus.busName}>
                                                {bus.busName}
                                            </TableCell>
                                            <TableCell>
                                                <span className={`px-1.5 py-0.5 rounded text-xs ${voltageClass}`}>
                                                    {mag.toFixed(3)}
                                                </span>
                                            </TableCell>
                                            <TableCell className="text-muted-foreground">
                                                {angle.toFixed(1)}
                                            </TableCell>
                                            <TableCell>
                                                {mag < VOLTAGE_THRESHOLDS.LOW_WARNING || mag > VOLTAGE_THRESHOLDS.HIGH_WARNING ? (
                                                    <AlertTriangle className="h-4 w-4 text-yellow-500" />
                                                ) : (
                                                    <CheckCircle2 className="h-4 w-4 text-green-500" />
                                                )}
                                            </TableCell>
                                        </TableRow>
                                    );
                                })}
                            </TableBody>
                        </Table>
                        {result.busResults.length > 20 && (
                            <p className="text-xs text-muted-foreground mt-2 text-center">
                                Showing 20 of {result.busResults.length} buses
                            </p>
                        )}
                    </div>
                </CardContent>
            </Card>

            {/* Branch Loading */}
            <Card>
                <CardHeader className="pb-2">
                    <CardTitle className="text-base">Branch Loading</CardTitle>
                </CardHeader>
                <CardContent>
                    <div className="space-y-2 max-h-60 overflow-y-auto">
                        {result.branchResults.slice(0, 15).map((branch) => {
                            // Estimate loading percentage (simplified - use actual rating if available)
                            const loading = Math.min((branch.powerFlowMW / 10) * 100, 150);
                            const loadingColor = getLoadingColor(loading);

                            return (
                                <div key={branch.branchId} className="space-y-1">
                                    <div className="flex items-center justify-between text-xs">
                                        <span className="truncate max-w-32" title={branch.branchName}>
                                            {branch.branchName}
                                        </span>
                                        <span className="text-muted-foreground">
                                            {branch.powerFlowMW.toFixed(2)} MW
                                        </span>
                                    </div>
                                    <Progress
                                        value={Math.min(loading, 100)}
                                        className="h-2"
                                        // @ts-ignore - custom color prop
                                        indicatorClassName={loadingColor}
                                    />
                                </div>
                            );
                        })}
                        {result.branchResults.length > 15 && (
                            <p className="text-xs text-muted-foreground mt-2 text-center">
                                Showing 15 of {result.branchResults.length} branches
                            </p>
                        )}
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}
