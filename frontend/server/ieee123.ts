import type { Connection, NetworkElement } from "@shared/schema";
import { randomUUID } from "crypto";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

type PhaseCode = "A" | "B" | "C" | "AB" | "AC" | "BC" | "ABC";

type RawCoordinate = {
  busNo: number;
  x: number;
  y: number;
};

type RawLineSegment = {
  from: number;
  to: number;
  lengthFt: number;
  configNo: number;
};

type RawSwitch = {
  from: number;
  to: number;
  status: string;
};

type RawLoad = {
  busNo: number;
  model: string;
  paKw: number;
  qaKvar: number;
  pbKw: number;
  qbKvar: number;
  pcKw: number;
  qcKvar: number;
};

type RawCapacitor = {
  busNo: number;
  phaseA: number;
  phaseB: number;
  phaseC: number;
};

type RawPhaseConfig = {
  configNo: number;
  phaseCode: string;
  phaseA: boolean;
  phaseB: boolean;
  phaseC: boolean;
  noPhases: number;
};

type RawImpedanceConfig = {
  configNo: number;
  resistanceOhmPerKm?: number;
  reactanceOhmPerKm?: number;
  susceptanceSPerKm?: number;
};

type IEEE123Raw = {
  coordinates: RawCoordinate[];
  lineSegments: RawLineSegment[];
  switches: RawSwitch[];
  loads: RawLoad[];
  capacitors: RawCapacitor[];
  phaseConfigs: RawPhaseConfig[];
  impedanceConfigs: RawImpedanceConfig[];
};

type PhaseFlags = {
  phaseCode: PhaseCode;
  phaseA: boolean;
  phaseB: boolean;
  phaseC: boolean;
};

type Impedance = {
  resistanceOhmPerKm: number;
  reactanceOhmPerKm: number;
  susceptanceSPerKm: number;
};

const CURRENT_DIR =
  typeof __dirname !== "undefined"
    ? __dirname
    : dirname(fileURLToPath(import.meta.url));

const RAW_PATH = resolve(CURRENT_DIR, "feeder123", "ieee123_raw.json");
const SOURCE_BUS_NO = 150;
const BASE_VOLTAGE_KV = 4.16;
const FT_TO_KM = 0.0003048;

const PHASE_COLOR: Record<PhaseCode, string> = {
  A: "#ef4444",
  B: "#f59e0b",
  C: "#3b82f6",
  AB: "#f97316",
  AC: "#14b8a6",
  BC: "#a855f7",
  ABC: "#10b981",
};

let rawCache: IEEE123Raw | null = null;

const getRawData = (): IEEE123Raw => {
  if (!rawCache) {
    rawCache = JSON.parse(readFileSync(RAW_PATH, "utf-8")) as IEEE123Raw;
  }
  return rawCache;
};

const toPhaseCode = (phaseCode: string): PhaseCode => {
  const normalized = phaseCode.replace(/[^ABC]/g, "") as PhaseCode;
  if (normalized === "A" || normalized === "B" || normalized === "C") return normalized;
  if (normalized === "AB" || normalized === "AC" || normalized === "BC") return normalized;
  return "ABC";
};

const defaultImpedance: Impedance = {
  resistanceOhmPerKm: 0.287,
  reactanceOhmPerKm: 0.656,
  susceptanceSPerKm: 0.0000035,
};

const modelToLoadType = (model: string): "constant_power" | "constant_current" | "constant_impedance" => {
  const upper = model.toUpperCase();
  if (upper.includes("-I")) return "constant_current";
  if (upper.includes("-Z")) return "constant_impedance";
  return "constant_power";
};

const round = (value: number, digits = 6) => {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
};

const ensurePhaseFlags = (flags?: Partial<PhaseFlags>): PhaseFlags => {
  const a = Boolean(flags?.phaseA);
  const b = Boolean(flags?.phaseB);
  const c = Boolean(flags?.phaseC);
  const code = `${a ? "A" : ""}${b ? "B" : ""}${c ? "C" : ""}`;
  const phaseCode = toPhaseCode(code || "ABC");
  return {
    phaseCode,
    phaseA: phaseCode.includes("A"),
    phaseB: phaseCode.includes("B"),
    phaseC: phaseCode.includes("C"),
  };
};

export function generateIEEE123Case(): {
  elements: NetworkElement[];
  connections: Connection[];
} {
  const raw = getRawData();
  const phaseByConfig = new Map<number, PhaseFlags>();
  const impedanceByConfig = new Map<number, Impedance>();
  const coordinateByBus = new Map<number, RawCoordinate>();

  raw.phaseConfigs.forEach((entry) => {
    phaseByConfig.set(
      entry.configNo,
      ensurePhaseFlags({
        phaseCode: toPhaseCode(entry.phaseCode),
        phaseA: entry.phaseA,
        phaseB: entry.phaseB,
        phaseC: entry.phaseC,
      })
    );
  });

  raw.impedanceConfigs.forEach((entry) => {
    impedanceByConfig.set(entry.configNo, {
      resistanceOhmPerKm: entry.resistanceOhmPerKm ?? defaultImpedance.resistanceOhmPerKm,
      reactanceOhmPerKm: entry.reactanceOhmPerKm ?? defaultImpedance.reactanceOhmPerKm,
      susceptanceSPerKm: entry.susceptanceSPerKm ?? defaultImpedance.susceptanceSPerKm,
    });
  });

  raw.coordinates.forEach((point) => {
    coordinateByBus.set(point.busNo, point);
  });

  const usedBusNos = new Set<number>([SOURCE_BUS_NO]);
  raw.lineSegments.forEach((segment) => {
    usedBusNos.add(segment.from);
    usedBusNos.add(segment.to);
  });
  raw.switches.forEach((sw) => {
    usedBusNos.add(sw.from);
    usedBusNos.add(sw.to);
  });
  raw.loads.forEach((load) => usedBusNos.add(load.busNo));
  raw.capacitors.forEach((cap) => usedBusNos.add(cap.busNo));

  const buses = Array.from(usedBusNos)
    .map((busNo) => coordinateByBus.get(busNo))
    .filter((point): point is RawCoordinate => Boolean(point))
    .sort((a, b) => a.busNo - b.busNo);

  const minX = Math.min(...buses.map((bus) => bus.x));
  const minY = Math.min(...buses.map((bus) => bus.y));
  const maxY = Math.max(...buses.map((bus) => bus.y));
  const scale = 1.8;

  const elements: NetworkElement[] = [];
  const connections: Connection[] = [];
  const busElementByNo = new Map<number, string>();
  const busPhase = new Map<number, { phaseA: boolean; phaseB: boolean; phaseC: boolean }>();

  const toCanvas = (point: RawCoordinate) => {
    const x = round((point.x - minX) * scale + 80, 3);
    const y = round((maxY - point.y) * scale + 80, 3);
    const geoLon = round(-97 + (point.x - minX) * 0.0016, 6);
    const geoLat = round(33 + (point.y - minY) * 0.0013, 6);
    return { x, y, geoLat, geoLon };
  };

  const mergeBusPhase = (busNo: number, phase: PhaseFlags) => {
    const current = busPhase.get(busNo) ?? { phaseA: false, phaseB: false, phaseC: false };
    busPhase.set(busNo, {
      phaseA: current.phaseA || phase.phaseA,
      phaseB: current.phaseB || phase.phaseB,
      phaseC: current.phaseC || phase.phaseC,
    });
  };

  buses.forEach((bus) => {
    const id = randomUUID();
    const point = toCanvas(bus);
    busElementByNo.set(bus.busNo, id);
    busPhase.set(bus.busNo, { phaseA: false, phaseB: false, phaseC: false });

    elements.push({
      id,
      name: `Bus ${bus.busNo}`,
      type: "bus",
      x: point.x,
      y: point.y,
      rotation: 0,
      enabled: true,
      nominalVoltageKV: BASE_VOLTAGE_KV,
      busType: bus.busNo === SOURCE_BUS_NO ? "slack" : "pq",
      geoLat: point.geoLat,
      geoLon: point.geoLon,
    } as NetworkElement);
  });

  raw.lineSegments.forEach((segment, idx) => {
    const fromId = busElementByNo.get(segment.from);
    const toId = busElementByNo.get(segment.to);
    const fromPoint = coordinateByBus.get(segment.from);
    const toPoint = coordinateByBus.get(segment.to);
    if (!fromId || !toId || !fromPoint || !toPoint) return;

    const phase = phaseByConfig.get(segment.configNo) ?? ensurePhaseFlags({ phaseCode: "ABC" });
    const color = PHASE_COLOR[phase.phaseCode];
    const imp = impedanceByConfig.get(segment.configNo) ?? defaultImpedance;
    const lengthKm = round(Math.max(0.005, segment.lengthFt * FT_TO_KM), 6);

    const fromCanvas = toCanvas(fromPoint);
    const toCanvasPoint = toCanvas(toPoint);
    const lineId = randomUUID();

    elements.push({
      id: lineId,
      name: `L${segment.from}-${segment.to} (${phase.phaseCode})`,
      type: "line",
      x: round((fromCanvas.x + toCanvasPoint.x) / 2, 3),
      y: round((fromCanvas.y + toCanvasPoint.y) / 2, 3),
      rotation: 0,
      enabled: true,
      lengthKm,
      resistanceOhmPerKm: round(imp.resistanceOhmPerKm, 6),
      reactanceOhmPerKm: round(imp.reactanceOhmPerKm, 6),
      susceptanceSPerKm: round(imp.susceptanceSPerKm, 9),
      fromElementId: fromId,
      toElementId: toId,
      fromBusId: fromId,
      toBusId: toId,
      phaseCode: phase.phaseCode,
      phaseA: phase.phaseA,
      phaseB: phase.phaseB,
      phaseC: phase.phaseC,
      phaseColor: color,
      configNo: segment.configNo,
      lineIndex: idx + 1,
    } as NetworkElement);

    connections.push({
      id: randomUUID(),
      fromElementId: fromId,
      toElementId: lineId,
      fromPort: "output",
      toPort: "input",
    });
    connections.push({
      id: randomUUID(),
      fromElementId: lineId,
      toElementId: toId,
      fromPort: "output",
      toPort: "input",
    });

    mergeBusPhase(segment.from, phase);
    mergeBusPhase(segment.to, phase);
  });

  raw.switches.forEach((sw) => {
    const fromId = busElementByNo.get(sw.from);
    const toId = busElementByNo.get(sw.to);
    const fromPoint = coordinateByBus.get(sw.from);
    const toPoint = coordinateByBus.get(sw.to);
    if (!fromId || !toId || !fromPoint || !toPoint) return;

    const fromPhase = busPhase.get(sw.from);
    const toPhase = busPhase.get(sw.to);
    const phase = ensurePhaseFlags({
      phaseA: Boolean(fromPhase?.phaseA || toPhase?.phaseA),
      phaseB: Boolean(fromPhase?.phaseB || toPhase?.phaseB),
      phaseC: Boolean(fromPhase?.phaseC || toPhase?.phaseC),
    });
    const color = PHASE_COLOR[phase.phaseCode];
    const fromCanvas = toCanvas(fromPoint);
    const toCanvasPoint = toCanvas(toPoint);
    const switchId = randomUUID();

    elements.push({
      id: switchId,
      name: `SW${sw.from}-${sw.to} ${sw.status.toUpperCase()}`,
      type: "switch",
      x: round((fromCanvas.x + toCanvasPoint.x) / 2, 3),
      y: round((fromCanvas.y + toCanvasPoint.y) / 2, 3),
      rotation: 0,
      enabled: true,
      isClosed: sw.status === "closed",
      ratedCurrentA: 600,
      fromBusId: fromId,
      toBusId: toId,
      phaseCode: phase.phaseCode,
      phaseA: phase.phaseA,
      phaseB: phase.phaseB,
      phaseC: phase.phaseC,
      phaseColor: color,
    } as NetworkElement);

    connections.push({
      id: randomUUID(),
      fromElementId: fromId,
      toElementId: switchId,
      fromPort: "output",
      toPort: "input",
    });
    connections.push({
      id: randomUUID(),
      fromElementId: switchId,
      toElementId: toId,
      fromPort: "output",
      toPort: "input",
    });
  });

  raw.loads.forEach((load, idx) => {
    const busId = busElementByNo.get(load.busNo);
    const busPoint = coordinateByBus.get(load.busNo);
    if (!busId || !busPoint) return;

    const totalKW = load.paKw + load.pbKw + load.pcKw;
    const totalKVAR = load.qaKvar + load.qbKvar + load.qcKvar;
    if (totalKW <= 0 && totalKVAR <= 0) return;

    const busCanvas = toCanvas(busPoint);
    const angle = (idx % 8) * (Math.PI / 4);
    const radius = 34 + (idx % 3) * 8;
    const loadId = randomUUID();
    const phase = ensurePhaseFlags({
      phaseA: load.paKw > 0 || load.qaKvar > 0,
      phaseB: load.pbKw > 0 || load.qbKvar > 0,
      phaseC: load.pcKw > 0 || load.qcKvar > 0,
    });

    const phaseAPower = totalKW > 0 ? (load.paKw / totalKW) * 100 : 33.33;
    const phaseBPower = totalKW > 0 ? (load.pbKw / totalKW) * 100 : 33.33;
    const phaseCPower = totalKW > 0 ? (load.pcKw / totalKW) * 100 : 33.34;
    const isUnbalanced = Math.max(phaseAPower, phaseBPower, phaseCPower) - Math.min(phaseAPower, phaseBPower, phaseCPower) > 1;

    elements.push({
      id: loadId,
      name: `Load ${load.busNo}`,
      type: "load",
      x: round(busCanvas.x + radius * Math.cos(angle), 3),
      y: round(busCanvas.y + radius * Math.sin(angle), 3),
      rotation: 0,
      enabled: true,
      activePowerKW: round(totalKW, 3),
      reactivePowerKVAR: round(totalKVAR, 3),
      loadModel: modelToLoadType(load.model),
      connectedBusId: busId,
      unbalanced: isUnbalanced,
      phaseAPower: round(phaseAPower, 3),
      phaseBPower: round(phaseBPower, 3),
      phaseCPower: round(phaseCPower, 3),
      phaseCode: phase.phaseCode,
      phaseA: phase.phaseA,
      phaseB: phase.phaseB,
      phaseC: phase.phaseC,
      phaseColor: PHASE_COLOR[phase.phaseCode],
      sourceModel: load.model,
    } as NetworkElement);

    connections.push({
      id: randomUUID(),
      fromElementId: loadId,
      toElementId: busId,
      fromPort: "output",
      toPort: "input",
    });
  });

  raw.capacitors.forEach((cap, idx) => {
    const busId = busElementByNo.get(cap.busNo);
    const busPoint = coordinateByBus.get(cap.busNo);
    if (!busId || !busPoint) return;

    const totalKVAR = cap.phaseA + cap.phaseB + cap.phaseC;
    if (totalKVAR <= 0) return;

    const phase = ensurePhaseFlags({
      phaseA: cap.phaseA > 0,
      phaseB: cap.phaseB > 0,
      phaseC: cap.phaseC > 0,
    });
    const busCanvas = toCanvas(busPoint);
    const angle = Math.PI + (idx % 4) * (Math.PI / 6);
    const radius = 42;
    const capacitorId = randomUUID();

    elements.push({
      id: capacitorId,
      name: `Cap ${cap.busNo}`,
      type: "capacitor",
      x: round(busCanvas.x + radius * Math.cos(angle), 3),
      y: round(busCanvas.y + radius * Math.sin(angle), 3),
      rotation: 0,
      enabled: true,
      ratingKVAR: round(totalKVAR, 3),
      nominalVoltageKV: BASE_VOLTAGE_KV,
      steps: 1,
      currentStep: 1,
      connectedBusId: busId,
      phaseCode: phase.phaseCode,
      phaseA: phase.phaseA,
      phaseB: phase.phaseB,
      phaseC: phase.phaseC,
      phaseColor: PHASE_COLOR[phase.phaseCode],
    } as NetworkElement);

    connections.push({
      id: randomUUID(),
      fromElementId: capacitorId,
      toElementId: busId,
      fromPort: "output",
      toPort: "input",
    });
  });

  const sourceBusId = busElementByNo.get(SOURCE_BUS_NO);
  const sourcePoint = coordinateByBus.get(SOURCE_BUS_NO);
  if (sourceBusId && sourcePoint) {
    const sourceCanvas = toCanvas(sourcePoint);
    const sourceId = randomUUID();

    elements.push({
      id: sourceId,
      name: "Substation Source",
      type: "external_source",
      x: round(sourceCanvas.x - 70, 3),
      y: sourceCanvas.y,
      rotation: 0,
      enabled: true,
      voltageKV: BASE_VOLTAGE_KV,
      shortCircuitMVA: 500,
      xrRatio: 12,
      phaseAngle: 0,
    } as NetworkElement);

    connections.push({
      id: randomUUID(),
      fromElementId: sourceId,
      toElementId: sourceBusId,
      fromPort: "output",
      toPort: "input",
    });
  }

  return { elements, connections };
}
