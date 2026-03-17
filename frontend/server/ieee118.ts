/**
 * IEEE 118-Bus Test Case Generator
 * 
 * Generates a simplified IEEE 118-bus power system test case for load flow analysis.
 * The IEEE 118-bus system is a classical test case representing a portion of the 
 * American Electric Power System as of December 1962.
 * 
 * Original system specs:
 * - 118 buses
 * - 186 branches (lines + transformers)
 * - 54 generators
 * - 91 loads
 * - Operating at 132kV primary, with some 13.8kV and 1kV substations
 */

import type { NetworkElement, Connection } from "@shared/schema";
import { randomUUID } from "crypto";

interface IEEE118Bus {
    id: number;
    name: string;
    type: "slack" | "pv" | "pq";
    voltageKV: number;
    loadMW: number;
    loadMVAR: number;
    genMW: number;
    genMVAR: number;
    x: number;
    y: number;
}

interface IEEE118Branch {
    from: number;
    to: number;
    r: number; // pu
    x: number; // pu
    b: number; // pu
    ratingMVA: number;
    isTransformer: boolean;
    tapRatio?: number;
}

/**
 * Generate IEEE 118-bus test case data
 * Simplified version with representative topology
 */
export function generateIEEE118Case(): {
    elements: NetworkElement[];
    connections: Connection[];
} {
    const buses = generateBuses();
    const branches = generateBranches();

    const elements: NetworkElement[] = [];
    const connections: Connection[] = [];
    const busIdMap = new Map<number, string>();

    // Create bus elements
    for (const bus of buses) {
        const busId = randomUUID();
        busIdMap.set(bus.id, busId);

        elements.push({
            id: busId,
            name: `Bus ${bus.id}`,
            type: "bus",
            x: bus.x,
            y: bus.y,
            rotation: 0,
            enabled: true,
            nominalVoltageKV: bus.voltageKV,
            busType: bus.type,
        } as NetworkElement);

        // Add external source for slack bus
        if (bus.type === "slack") {
            const sourceId = randomUUID();
            elements.push({
                id: sourceId,
                name: `Grid ${bus.id}`,
                type: "external_source",
                x: bus.x - 60,
                y: bus.y,
                rotation: 0,
                enabled: true,
                voltageKV: bus.voltageKV,
                shortCircuitMVA: 5000,
                xrRatio: 15,
                phaseAngle: 0,
            } as NetworkElement);

            connections.push({
                id: randomUUID(),
                fromElementId: sourceId,
                toElementId: busId,
                fromPort: "output",
                toPort: "input",
            });
        }

        // Add generator for PV buses
        if (bus.type === "pv" && bus.genMW > 0) {
            const genId = randomUUID();
            elements.push({
                id: genId,
                name: `Gen ${bus.id}`,
                type: "generator",
                x: bus.x + 60,
                y: bus.y - 30,
                rotation: 0,
                enabled: true,
                ratingMVA: bus.genMW * 1.2,
                activePowerMW: bus.genMW,
                voltageSetpointPU: 1.0,
                minReactivePowerMVAR: -bus.genMW * 0.5,
                maxReactivePowerMVAR: bus.genMW * 0.6,
                connectedBusId: busId,
            } as NetworkElement);

            connections.push({
                id: randomUUID(),
                fromElementId: genId,
                toElementId: busId,
                fromPort: "output",
                toPort: "input",
            });
        }

        // Add load for PQ buses with load
        if (bus.loadMW > 0) {
            const loadId = randomUUID();
            elements.push({
                id: loadId,
                name: `Load ${bus.id}`,
                type: "load",
                x: bus.x + (bus.type === "pv" ? -60 : 60),
                y: bus.y + 30,
                rotation: 0,
                enabled: true,
                activePowerKW: bus.loadMW * 1000,
                reactivePowerKVAR: bus.loadMVAR * 1000,
                loadModel: "constant_power",
                unbalanced: false,
                phaseAPower: 33.33,
                phaseBPower: 33.33,
                phaseCPower: 33.34,
                connectedBusId: busId,
            } as NetworkElement);

            connections.push({
                id: randomUUID(),
                fromElementId: loadId,
                toElementId: busId,
                fromPort: "output",
                toPort: "input",
            });
        }
    }

    // Create branch elements (lines and transformers)
    for (const branch of branches) {
        const fromBusId = busIdMap.get(branch.from);
        const toBusId = busIdMap.get(branch.to);

        if (!fromBusId || !toBusId) continue;

        const fromBus = buses.find(b => b.id === branch.from);
        const toBus = buses.find(b => b.id === branch.to);
        if (!fromBus || !toBus) continue;

        const branchId = randomUUID();
        const midX = (fromBus.x + toBus.x) / 2;
        const midY = (fromBus.y + toBus.y) / 2;

        if (branch.isTransformer) {
            elements.push({
                id: branchId,
                name: `T${branch.from}-${branch.to}`,
                type: "transformer",
                x: midX,
                y: midY,
                rotation: 0,
                enabled: true,
                ratingMVA: branch.ratingMVA,
                primaryVoltageKV: fromBus.voltageKV,
                secondaryVoltageKV: toBus.voltageKV,
                impedancePercent: branch.x * 100,
                xrRatio: branch.x / (branch.r || 0.001),
                tapPosition: branch.tapRatio ? (branch.tapRatio - 1) * 100 : 0,
                connectionType: "Yg-D",
                fromBusId,
                toBusId,
            } as NetworkElement);
        } else {
            // Estimate line length from impedance (assuming Z ~= 0.4 ohm/km at 132kV)
            const lengthKm = Math.sqrt(branch.r * branch.r + branch.x * branch.x) * 1000 / 0.4;

            elements.push({
                id: branchId,
                name: `L${branch.from}-${branch.to}`,
                type: "line",
                x: midX,
                y: midY,
                rotation: 0,
                enabled: true,
                lengthKm: Math.max(1, lengthKm),
                resistanceOhmPerKm: branch.r * 100 / Math.max(1, lengthKm),
                reactanceOhmPerKm: branch.x * 100 / Math.max(1, lengthKm),
                susceptanceSPerKm: branch.b / Math.max(1, lengthKm),
                fromElementId: fromBusId,
                toElementId: toBusId,
                fromBusId,
                toBusId,
            } as NetworkElement);
        }

        // Connection from first bus to branch
        connections.push({
            id: randomUUID(),
            fromElementId: fromBusId,
            toElementId: branchId,
            fromPort: "output",
            toPort: "input",
        });

        // Connection from branch to second bus
        connections.push({
            id: randomUUID(),
            fromElementId: branchId,
            toElementId: toBusId,
            fromPort: "output",
            toPort: "input",
        });
    }

    return { elements, connections };
}

/**
 * Generate bus data for IEEE 118-bus system
 * Simplified representative data
 */
function generateBuses(): IEEE118Bus[] {
    const buses: IEEE118Bus[] = [];

    // Layout parameters
    const cols = 12;
    const spacing = 120;
    const startX = 100;
    const startY = 100;

    // Representative bus data (simplified from IEEE 118-bus case)
    const busData: Partial<IEEE118Bus>[] = [
        { id: 1, type: "pq", loadMW: 51, loadMVAR: 27 },
        { id: 2, type: "pq", loadMW: 20, loadMVAR: 9 },
        { id: 3, type: "pq", loadMW: 39, loadMVAR: 10 },
        { id: 4, type: "pv", genMW: 0, loadMW: 39, loadMVAR: 12 },
        { id: 5, type: "pq", loadMW: 0, loadMVAR: 0 },
        { id: 6, type: "pv", genMW: 0, loadMW: 52, loadMVAR: 22 },
        { id: 7, type: "pq", loadMW: 19, loadMVAR: 2 },
        { id: 8, type: "pv", genMW: 0, loadMW: 28, loadMVAR: 0 },
        { id: 9, type: "pq", loadMW: 0, loadMVAR: 0 },
        { id: 10, type: "pv", genMW: 450, loadMW: 0, loadMVAR: 0 },
        { id: 11, type: "pq", loadMW: 70, loadMVAR: 23 },
        { id: 12, type: "pv", genMW: 85, loadMW: 47, loadMVAR: 10 },
        { id: 13, type: "pq", loadMW: 34, loadMVAR: 16 },
        { id: 14, type: "pq", loadMW: 14, loadMVAR: 1 },
        { id: 15, type: "pv", genMW: 0, loadMW: 90, loadMVAR: 30 },
        { id: 16, type: "pq", loadMW: 25, loadMVAR: 10 },
        { id: 17, type: "pq", loadMW: 11, loadMVAR: 3 },
        { id: 18, type: "pv", genMW: 0, loadMW: 60, loadMVAR: 34 },
        { id: 19, type: "pv", genMW: 0, loadMW: 45, loadMVAR: 25 },
        { id: 20, type: "pq", loadMW: 18, loadMVAR: 3 },
        { id: 21, type: "pq", loadMW: 14, loadMVAR: 8 },
        { id: 22, type: "pq", loadMW: 10, loadMVAR: 5 },
        { id: 23, type: "pq", loadMW: 7, loadMVAR: 3 },
        { id: 24, type: "pv", genMW: 0, loadMW: 13, loadMVAR: 0 },
        { id: 25, type: "pv", genMW: 220, loadMW: 0, loadMVAR: 0 },
        { id: 26, type: "pv", genMW: 314, loadMW: 0, loadMVAR: 0 },
        { id: 27, type: "pq", loadMW: 71, loadMVAR: 13 },
        { id: 28, type: "pq", loadMW: 17, loadMVAR: 7 },
        { id: 29, type: "pq", loadMW: 24, loadMVAR: 4 },
        { id: 30, type: "pq", loadMW: 0, loadMVAR: 0 },
        { id: 31, type: "pv", genMW: 7, loadMW: 43, loadMVAR: 27 },
        { id: 32, type: "pv", genMW: 0, loadMW: 59, loadMVAR: 23 },
        { id: 33, type: "pq", loadMW: 23, loadMVAR: 9 },
        { id: 34, type: "pv", genMW: 0, loadMW: 59, loadMVAR: 26 },
        { id: 35, type: "pq", loadMW: 33, loadMVAR: 9 },
        { id: 36, type: "pv", genMW: 0, loadMW: 31, loadMVAR: 17 },
        { id: 37, type: "pq", loadMW: 0, loadMVAR: 0 },
        { id: 38, type: "pq", loadMW: 0, loadMVAR: 0 },
        { id: 39, type: "pq", loadMW: 27, loadMVAR: 11 },
        { id: 40, type: "pv", genMW: 0, loadMW: 66, loadMVAR: 23 },
        { id: 41, type: "pq", loadMW: 37, loadMVAR: 10 },
        { id: 42, type: "pv", genMW: 0, loadMW: 96, loadMVAR: 23 },
        { id: 43, type: "pq", loadMW: 18, loadMVAR: 7 },
        { id: 44, type: "pq", loadMW: 16, loadMVAR: 8 },
        { id: 45, type: "pq", loadMW: 53, loadMVAR: 22 },
        { id: 46, type: "pv", genMW: 19, loadMW: 28, loadMVAR: 10 },
        { id: 47, type: "pq", loadMW: 34, loadMVAR: 0 },
        { id: 48, type: "pq", loadMW: 20, loadMVAR: 11 },
        { id: 49, type: "pv", genMW: 204, loadMW: 87, loadMVAR: 30 },
        { id: 50, type: "pq", loadMW: 17, loadMVAR: 4 },
        { id: 51, type: "pq", loadMW: 17, loadMVAR: 8 },
        { id: 52, type: "pq", loadMW: 18, loadMVAR: 5 },
        { id: 53, type: "pq", loadMW: 23, loadMVAR: 11 },
        { id: 54, type: "pv", genMW: 48, loadMW: 113, loadMVAR: 32 },
        { id: 55, type: "pv", genMW: 0, loadMW: 63, loadMVAR: 22 },
        { id: 56, type: "pv", genMW: 0, loadMW: 84, loadMVAR: 18 },
        { id: 57, type: "pq", loadMW: 12, loadMVAR: 3 },
        { id: 58, type: "pq", loadMW: 12, loadMVAR: 3 },
        { id: 59, type: "pv", genMW: 155, loadMW: 277, loadMVAR: 113 },
        { id: 60, type: "pq", loadMW: 78, loadMVAR: 3 },
        { id: 61, type: "pv", genMW: 160, loadMW: 0, loadMVAR: 0 },
        { id: 62, type: "pv", genMW: 0, loadMW: 77, loadMVAR: 14 },
        { id: 63, type: "pq", loadMW: 0, loadMVAR: 0 },
        { id: 64, type: "pq", loadMW: 0, loadMVAR: 0 },
        { id: 65, type: "pv", genMW: 391, loadMW: 0, loadMVAR: 0 },
        { id: 66, type: "pv", genMW: 392, loadMW: 39, loadMVAR: 18 },
        { id: 67, type: "pq", loadMW: 28, loadMVAR: 7 },
        { id: 68, type: "pq", loadMW: 0, loadMVAR: 0 },
        { id: 69, type: "slack", genMW: 513, loadMW: 0, loadMVAR: 0 },
        { id: 70, type: "pv", genMW: 0, loadMW: 66, loadMVAR: 20 },
        { id: 71, type: "pq", loadMW: 0, loadMVAR: 0 },
        { id: 72, type: "pv", genMW: 0, loadMW: 12, loadMVAR: 0 },
        { id: 73, type: "pv", genMW: 0, loadMW: 6, loadMVAR: 0 },
        { id: 74, type: "pv", genMW: 0, loadMW: 68, loadMVAR: 27 },
        { id: 75, type: "pq", loadMW: 47, loadMVAR: 11 },
        { id: 76, type: "pv", genMW: 0, loadMW: 68, loadMVAR: 36 },
        { id: 77, type: "pv", genMW: 0, loadMW: 61, loadMVAR: 28 },
        { id: 78, type: "pq", loadMW: 71, loadMVAR: 26 },
        { id: 79, type: "pq", loadMW: 39, loadMVAR: 32 },
        { id: 80, type: "pv", genMW: 477, loadMW: 130, loadMVAR: 26 },
        { id: 81, type: "pq", loadMW: 0, loadMVAR: 0 },
        { id: 82, type: "pq", loadMW: 54, loadMVAR: 27 },
        { id: 83, type: "pq", loadMW: 20, loadMVAR: 10 },
        { id: 84, type: "pq", loadMW: 11, loadMVAR: 7 },
        { id: 85, type: "pv", genMW: 0, loadMW: 24, loadMVAR: 15 },
        { id: 86, type: "pq", loadMW: 21, loadMVAR: 10 },
        { id: 87, type: "pv", genMW: 4, loadMW: 0, loadMVAR: 0 },
        { id: 88, type: "pq", loadMW: 48, loadMVAR: 10 },
        { id: 89, type: "pv", genMW: 607, loadMW: 0, loadMVAR: 0 },
        { id: 90, type: "pv", genMW: 0, loadMW: 163, loadMVAR: 42 },
        { id: 91, type: "pv", genMW: 0, loadMW: 10, loadMVAR: 0 },
        { id: 92, type: "pv", genMW: 0, loadMW: 65, loadMVAR: 10 },
        { id: 93, type: "pq", loadMW: 12, loadMVAR: 7 },
        { id: 94, type: "pq", loadMW: 30, loadMVAR: 16 },
        { id: 95, type: "pq", loadMW: 42, loadMVAR: 31 },
        { id: 96, type: "pq", loadMW: 38, loadMVAR: 15 },
        { id: 97, type: "pq", loadMW: 15, loadMVAR: 9 },
        { id: 98, type: "pq", loadMW: 34, loadMVAR: 8 },
        { id: 99, type: "pv", genMW: 0, loadMW: 42, loadMVAR: 0 },
        { id: 100, type: "pv", genMW: 252, loadMW: 37, loadMVAR: 18 },
        { id: 101, type: "pq", loadMW: 22, loadMVAR: 15 },
        { id: 102, type: "pq", loadMW: 5, loadMVAR: 3 },
        { id: 103, type: "pv", genMW: 40, loadMW: 23, loadMVAR: 16 },
        { id: 104, type: "pv", genMW: 0, loadMW: 38, loadMVAR: 25 },
        { id: 105, type: "pv", genMW: 0, loadMW: 31, loadMVAR: 26 },
        { id: 106, type: "pq", loadMW: 43, loadMVAR: 16 },
        { id: 107, type: "pv", genMW: 0, loadMW: 50, loadMVAR: 12 },
        { id: 108, type: "pq", loadMW: 2, loadMVAR: 1 },
        { id: 109, type: "pq", loadMW: 8, loadMVAR: 3 },
        { id: 110, type: "pv", genMW: 0, loadMW: 39, loadMVAR: 30 },
        { id: 111, type: "pv", genMW: 36, loadMW: 0, loadMVAR: 0 },
        { id: 112, type: "pv", genMW: 0, loadMW: 68, loadMVAR: 13 },
        { id: 113, type: "pv", genMW: 0, loadMW: 6, loadMVAR: 0 },
        { id: 114, type: "pq", loadMW: 8, loadMVAR: 3 },
        { id: 115, type: "pq", loadMW: 22, loadMVAR: 7 },
        { id: 116, type: "pv", genMW: 0, loadMW: 184, loadMVAR: 0 },
        { id: 117, type: "pq", loadMW: 20, loadMVAR: 8 },
        { id: 118, type: "pq", loadMW: 33, loadMVAR: 15 },
    ];

    for (const data of busData) {
        const row = Math.floor((data.id! - 1) / cols);
        const col = (data.id! - 1) % cols;

        buses.push({
            id: data.id!,
            name: `Bus ${data.id}`,
            type: data.type || "pq",
            voltageKV: 132, // Primary voltage level
            loadMW: data.loadMW || 0,
            loadMVAR: data.loadMVAR || 0,
            genMW: data.genMW || 0,
            genMVAR: 0,
            x: startX + col * spacing,
            y: startY + row * spacing,
        });
    }

    return buses;
}

/**
 * Generate branch data for IEEE 118-bus system
 * Simplified representative topology
 */
function generateBranches(): IEEE118Branch[] {
    // Representative branch connections (simplified from IEEE 118-bus case)
    const branchData: [number, number, number, number, number, number][] = [
        // [from, to, r, x, b, rating]
        [1, 2, 0.0303, 0.0999, 0.0254, 100],
        [1, 3, 0.0129, 0.0424, 0.0108, 100],
        [2, 12, 0.0187, 0.0616, 0.0157, 100],
        [3, 5, 0.0241, 0.0798, 0.0203, 100],
        [3, 12, 0.0484, 0.1600, 0.0406, 100],
        [4, 5, 0.0018, 0.0080, 0.0021, 200],
        [4, 11, 0.0209, 0.0688, 0.0175, 100],
        [5, 6, 0.0119, 0.0540, 0.0143, 200],
        [5, 11, 0.0203, 0.0682, 0.0174, 100],
        [6, 7, 0.0046, 0.0208, 0.0055, 200],
        [7, 12, 0.0086, 0.0340, 0.0087, 100],
        [8, 9, 0.0024, 0.0305, 1.1620, 200],
        [8, 30, 0.0043, 0.0504, 0.0514, 200],
        [9, 10, 0.0026, 0.0322, 1.2300, 200],
        [10, 118, 0.0032, 0.0400, 1.5200, 300],
        [11, 12, 0.0060, 0.0196, 0.0050, 100],
        [11, 13, 0.0022, 0.0073, 0.0019, 100],
        [12, 14, 0.0215, 0.0707, 0.0180, 100],
        [12, 16, 0.0212, 0.0834, 0.0214, 100],
        [13, 15, 0.0744, 0.2444, 0.0627, 100],
        [14, 15, 0.0595, 0.1950, 0.0502, 100],
        [15, 17, 0.0132, 0.0437, 0.0444, 100],
        [15, 19, 0.0120, 0.0394, 0.0101, 100],
        [15, 33, 0.0380, 0.1244, 0.0319, 100],
        [16, 17, 0.0454, 0.1801, 0.0466, 100],
        [17, 18, 0.0123, 0.0505, 0.0130, 100],
        [17, 31, 0.0474, 0.1563, 0.0399, 100],
        [17, 113, 0.0091, 0.0301, 0.0077, 100],
        [18, 19, 0.0112, 0.0493, 0.0114, 100],
        [19, 20, 0.0252, 0.0830, 0.0213, 100],
        [19, 34, 0.0752, 0.2470, 0.0632, 100],
        [20, 21, 0.0183, 0.0600, 0.0153, 100],
        [21, 22, 0.0209, 0.0688, 0.0175, 100],
        [22, 23, 0.0342, 0.1590, 0.0404, 100],
        [23, 24, 0.0135, 0.0492, 0.0498, 100],
        [23, 25, 0.0156, 0.0800, 0.0864, 200],
        [23, 32, 0.0317, 0.1153, 0.1173, 100],
        [24, 70, 0.0022, 0.4115, 0.1020, 200],
        [24, 72, 0.0488, 0.1960, 0.0488, 100],
        [25, 27, 0.0318, 0.1630, 0.1764, 200],
        [26, 25, 0.0000, 0.0382, 0.0000, 200],
        [26, 30, 0.0080, 0.0860, 0.9080, 300],
        [27, 28, 0.0191, 0.0855, 0.0216, 100],
        [27, 32, 0.0229, 0.0755, 0.0193, 100],
        [27, 115, 0.0164, 0.0741, 0.0197, 100],
        [28, 29, 0.0237, 0.0943, 0.0238, 100],
        [29, 31, 0.0108, 0.0331, 0.0083, 100],
        [30, 17, 0.0000, 0.0388, 0.0000, 200],
        [30, 38, 0.0046, 0.0540, 0.4220, 200],
        [31, 32, 0.0298, 0.0985, 0.0251, 100],
        [32, 113, 0.0615, 0.2030, 0.0518, 100],
        [32, 114, 0.0135, 0.0612, 0.0163, 100],
        [33, 37, 0.0415, 0.1420, 0.0366, 100],
        [34, 36, 0.0087, 0.0268, 0.0057, 100],
        [34, 37, 0.0026, 0.0094, 0.0098, 100],
        [34, 43, 0.0413, 0.1681, 0.0423, 100],
        [35, 36, 0.0022, 0.0102, 0.0027, 100],
        [35, 37, 0.0110, 0.0497, 0.0132, 100],
        [37, 39, 0.0321, 0.1060, 0.0270, 100],
        [37, 40, 0.0593, 0.1680, 0.0420, 100],
        [38, 37, 0.0000, 0.0375, 0.0000, 200],
        [38, 65, 0.0090, 0.0986, 1.0460, 300],
        [39, 40, 0.0184, 0.0605, 0.0155, 100],
        [40, 41, 0.0145, 0.0487, 0.0122, 100],
        [40, 42, 0.0555, 0.1830, 0.0466, 100],
        [41, 42, 0.0410, 0.1350, 0.0344, 100],
        [42, 49, 0.0715, 0.3230, 0.0860, 100],
        [43, 44, 0.0608, 0.2454, 0.0607, 100],
        [44, 45, 0.0224, 0.0901, 0.0224, 100],
        [45, 46, 0.0400, 0.1356, 0.0332, 100],
        [45, 49, 0.0684, 0.1860, 0.0444, 100],
        [46, 47, 0.0380, 0.1270, 0.0316, 100],
        [46, 48, 0.0601, 0.1890, 0.0472, 100],
        [47, 49, 0.0191, 0.0625, 0.0160, 100],
        [47, 69, 0.0844, 0.2778, 0.0709, 100],
        [48, 49, 0.0179, 0.0505, 0.0126, 100],
        [49, 50, 0.0267, 0.0752, 0.0187, 100],
        [49, 51, 0.0486, 0.1370, 0.0342, 100],
        [49, 54, 0.0730, 0.2890, 0.0738, 100],
        [49, 69, 0.0985, 0.3240, 0.0828, 100],
        [50, 57, 0.0474, 0.1340, 0.0332, 100],
        [51, 52, 0.0203, 0.0588, 0.0140, 100],
        [51, 58, 0.0255, 0.0719, 0.0179, 100],
        [52, 53, 0.0405, 0.1635, 0.0406, 100],
        [53, 54, 0.0263, 0.1220, 0.0310, 100],
        [54, 55, 0.0169, 0.0707, 0.0202, 100],
        [54, 56, 0.0028, 0.0096, 0.0073, 100],
        [54, 59, 0.0503, 0.2293, 0.0598, 100],
        [55, 56, 0.0049, 0.0151, 0.0037, 100],
        [55, 59, 0.0474, 0.2158, 0.0565, 100],
        [56, 57, 0.0343, 0.0966, 0.0242, 100],
        [56, 58, 0.0343, 0.0966, 0.0242, 100],
        [56, 59, 0.0825, 0.2510, 0.0569, 100],
        [59, 60, 0.0317, 0.1450, 0.0376, 100],
        [59, 61, 0.0328, 0.1500, 0.0388, 100],
        [59, 63, 0.0000, 0.0386, 0.0000, 200],
        [60, 61, 0.0026, 0.0135, 0.0146, 100],
        [60, 62, 0.0123, 0.0561, 0.0147, 100],
        [61, 62, 0.0082, 0.0376, 0.0098, 100],
        [61, 64, 0.0000, 0.0268, 0.0000, 200],
        [62, 66, 0.0482, 0.2180, 0.0578, 100],
        [62, 67, 0.0258, 0.1170, 0.0310, 100],
        [63, 59, 0.0000, 0.0386, 0.0000, 200],
        [63, 64, 0.0017, 0.0200, 0.2160, 300],
        [64, 65, 0.0027, 0.0302, 0.3800, 300],
        [65, 66, 0.0000, 0.0370, 0.0000, 200],
        [65, 68, 0.0014, 0.0160, 0.6380, 500],
        [66, 67, 0.0224, 0.1015, 0.0268, 100],
        [68, 69, 0.0000, 0.0370, 0.0000, 300],
        [68, 81, 0.0018, 0.0202, 0.8080, 500],
        [68, 116, 0.0003, 0.0041, 0.1640, 500],
        [69, 70, 0.0300, 0.1270, 0.1220, 200],
        [69, 75, 0.0405, 0.1220, 0.1240, 100],
        [69, 77, 0.0309, 0.1010, 0.1038, 100],
        [70, 71, 0.0088, 0.0355, 0.0088, 100],
        [70, 74, 0.0401, 0.1323, 0.0337, 100],
        [70, 75, 0.0428, 0.1410, 0.0360, 100],
        [71, 72, 0.0446, 0.1800, 0.0444, 100],
        [71, 73, 0.0087, 0.0454, 0.0118, 100],
        [74, 75, 0.0123, 0.0406, 0.0103, 100],
        [75, 77, 0.0601, 0.1999, 0.0498, 100],
        [75, 118, 0.0145, 0.0481, 0.0120, 100],
        [76, 77, 0.0444, 0.1480, 0.0368, 100],
        [76, 118, 0.0164, 0.0544, 0.0136, 100],
        [77, 78, 0.0038, 0.0124, 0.0126, 100],
        [77, 80, 0.0294, 0.1050, 0.0228, 100],
        [77, 82, 0.0298, 0.0853, 0.0817, 100],
        [78, 79, 0.0055, 0.0244, 0.0065, 100],
        [79, 80, 0.0156, 0.0704, 0.0187, 100],
        [80, 81, 0.0000, 0.0370, 0.0000, 300],
        [80, 96, 0.0356, 0.1820, 0.0494, 100],
        [80, 97, 0.0183, 0.0934, 0.0254, 100],
        [80, 98, 0.0238, 0.1080, 0.0286, 100],
        [80, 99, 0.0454, 0.2060, 0.0546, 100],
        [81, 80, 0.0000, 0.0370, 0.0000, 300],
        [82, 83, 0.0112, 0.0367, 0.0380, 100],
        [82, 96, 0.0162, 0.0530, 0.0544, 100],
        [83, 84, 0.0625, 0.1320, 0.0258, 100],
        [83, 85, 0.0430, 0.1480, 0.0348, 100],
        [84, 85, 0.0302, 0.0641, 0.0123, 100],
        [85, 86, 0.0350, 0.1230, 0.0276, 100],
        [85, 88, 0.0200, 0.1020, 0.0276, 100],
        [85, 89, 0.0239, 0.1730, 0.0470, 200],
        [86, 87, 0.0284, 0.2320, 0.0000, 100],
        [88, 89, 0.0139, 0.0712, 0.0193, 200],
        [89, 90, 0.0518, 0.1880, 0.0528, 100],
        [89, 92, 0.0099, 0.0505, 0.0548, 200],
        [90, 91, 0.0254, 0.0836, 0.0214, 100],
        [91, 92, 0.0387, 0.1272, 0.0327, 100],
        [92, 93, 0.0258, 0.0848, 0.0218, 100],
        [92, 94, 0.0481, 0.1580, 0.0406, 100],
        [92, 100, 0.0648, 0.2950, 0.0472, 100],
        [92, 102, 0.0123, 0.0559, 0.0146, 100],
        [93, 94, 0.0223, 0.0732, 0.0188, 100],
        [94, 95, 0.0132, 0.0434, 0.0111, 100],
        [94, 96, 0.0269, 0.0869, 0.0230, 100],
        [94, 100, 0.0178, 0.0580, 0.0604, 100],
        [95, 96, 0.0171, 0.0547, 0.0147, 100],
        [96, 97, 0.0173, 0.0885, 0.0240, 100],
        [98, 100, 0.0397, 0.1790, 0.0476, 100],
        [99, 100, 0.0180, 0.0813, 0.0216, 100],
        [100, 101, 0.0277, 0.1262, 0.0328, 100],
        [100, 103, 0.0160, 0.0525, 0.0536, 100],
        [100, 104, 0.0451, 0.2040, 0.0541, 100],
        [100, 106, 0.0605, 0.2290, 0.0620, 100],
        [101, 102, 0.0246, 0.1120, 0.0294, 100],
        [103, 104, 0.0466, 0.1584, 0.0407, 100],
        [103, 105, 0.0535, 0.1625, 0.0408, 100],
        [103, 110, 0.0391, 0.1813, 0.0461, 100],
        [104, 105, 0.0099, 0.0378, 0.0099, 100],
        [105, 106, 0.0140, 0.0547, 0.0143, 100],
        [105, 107, 0.0530, 0.1830, 0.0472, 100],
        [105, 108, 0.0261, 0.0703, 0.0184, 100],
        [106, 107, 0.0530, 0.1830, 0.0472, 100],
        [108, 109, 0.0105, 0.0288, 0.0076, 100],
        [109, 110, 0.0278, 0.0762, 0.0202, 100],
        [110, 111, 0.0220, 0.0755, 0.0200, 100],
        [110, 112, 0.0247, 0.0640, 0.0620, 100],
        [114, 115, 0.0023, 0.0104, 0.0028, 100],
        [116, 68, 0.0003, 0.0041, 0.0164, 500],
        [117, 12, 0.0329, 0.1400, 0.0358, 100],
    ];

    return branchData.map(([from, to, r, x, b, rating]) => ({
        from,
        to,
        r,
        x,
        b,
        ratingMVA: rating,
        isTransformer: r === 0 && b === 0, // Transformers have zero resistance and susceptance in per-unit
        tapRatio: r === 0 && b === 0 ? 1.0 : undefined,
    }));
}
