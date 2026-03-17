/**
 * Network graph data structures and the IEEE-123 bus topology
 * used by the 3D power flow visualisation.
 *
 * IEEE 123-bus is a radial distribution feeder with real geographic
 * coordinates, phase-coded branches (A, B, C, AB, AC, BC, ABC),
 * and a tree-like topology typical of medium-voltage feeders.
 */

export type PhaseCode = "A" | "B" | "C" | "AB" | "AC" | "BC" | "ABC";

export type BusType = "source" | "load" | "capacitor" | "junction";

export interface Bus {
  id: number;
  sourceElementId?: string;
  name?: string;
  type: BusType;
  totalLoadKW: number;
  /** normalised 2-D position roughly in [-1,1] */
  x: number;
  y: number;
}

export interface Branch {
  from: number;
  to: number;
  phaseCode: PhaseCode;
  lengthFt: number;
  isSwitch: boolean;
}

export interface NetworkGraph {
  buses: Bus[];
  branches: Branch[];
}

/* ─── phase colours ─────────────────────────────────────────────── */

export const PHASE_COLORS: Record<PhaseCode, string> = {
  A:   "#ef4444", // red
  B:   "#f59e0b", // amber
  C:   "#3b82f6", // blue
  AB:  "#f97316", // orange
  AC:  "#14b8a6", // teal
  BC:  "#a855f7", // purple
  ABC: "#10b981", // emerald
};

/* ------------------------------------------------------------------ */
/*  IEEE 123-bus raw data (from ieee123_raw.json)                     */
/* ------------------------------------------------------------------ */

/** [busNo, x, y] */
const RAW_COORDS: [number, number, number][] = [
  [1, 187.2699, 589.6655],
  [2, 178.5088, 529.4255],
  [3, 184.8805, 684.5891],
  [4, 185.677, 716.5345],
  [5, 229.4823, 686.4145],
  [6, 276.4735, 687.3273],
  [7, 227.0929, 583.2764],
  [8, 278.0664, 571.4109],
  [9, 261.3407, 511.1709],
  [10, 203.1991, 507.52],
  [11, 130.7212, 482.8764],
  [12, 250.9867, 613.3964],
  [13, 327.4469, 564.1091],
  [14, 191.2522, 471.9236],
  [15, 361.6947, 664.5091],
  [16, 375.2345, 708.32],
  [17, 403.1106, 645.3418],
  [18, 266.9159, 368.7855],
  [19, 207.1814, 382.4764],
  [20, 142.6681, 397.9927],
  [21, 249.3938, 306.72],
  [22, 131.5177, 335.9273],
  [23, 226.2965, 233.7018],
  [24, 133.1106, 261.0836],
  [25, 212.7566, 184.4145],
  [26, 148.2434, 197.1927],
  [27, 78.1549, 213.6218],
  [28, 201.6062, 143.3418],
  [29, 186.4735, 100.4436],
  [30, 235.0575, 94.0545],
  [31, 130.7212, 144.2545],
  [32, 119.5708, 98.6182],
  [33, 66.208, 139.6909],
  [34, 344.969, 610.6582],
  [35, 356.1195, 345.9673],
  [36, 444.5265, 371.5236],
  [37, 361.6947, 397.08],
  [38, 499.4823, 354.1818],
  [39, 545.677, 341.4036],
  [40, 344.1726, 305.8073],
  [41, 416.6504, 284.8145],
  [42, 333.0221, 269.2982],
  [43, 422.2257, 239.1782],
  [44, 320.2788, 229.1382],
  [45, 379.2168, 210.8836],
  [46, 438.1549, 190.8036],
  [47, 302.7566, 177.1127],
  [48, 254.969, 190.8036],
  [49, 364.8805, 161.5964],
  [50, 419.0398, 143.3418],
  [51, 469.2168, 129.6509],
  [52, 455.677, 540.3782],
  [53, 507.4469, 532.1636],
  [54, 548.0664, 525.7745],
  [55, 587.8894, 521.2109],
  [56, 631.6947, 513.9091],
  [57, 524.969, 453.6691],
  [58, 479.5708, 468.2727],
  [59, 419.8363, 480.1382],
  [60, 638.0664, 429.9382],
  [61, 657.9779, 483.7891],
  [62, 634.8805, 322.2364],
  [63, 619.7478, 263.8218],
  [64, 602.2257, 217.2727],
  [65, 536.9159, 241.0036],
  [66, 550.4558, 285.7273],
  [67, 771.0752, 414.4218],
  [68, 809.3053, 390.6909],
  [69, 845.146, 366.96],
  [70, 880.9867, 346.88],
  [71, 923.1991, 317.6727],
  [72, 788.5973, 474.6618],
  [73, 830.0133, 450.9309],
  [74, 872.2257, 425.3745],
  [75, 913.6416, 400.7309],
  [76, 806.9159, 543.1164],
  [77, 833.9956, 526.6873],
  [78, 864.2611, 514.8218],
  [79, 890.5442, 496.5673],
  [80, 869.0398, 563.1964],
  [81, 872.2257, 619.7855],
  [82, 874.615, 683.6764],
  [83, 925.5885, 684.5891],
  [84, 930.3673, 581.4509],
  [85, 927.9779, 468.2727],
  [86, 816.4735, 648.9927],
  [87, 730.4558, 655.3818],
  [88, 720.8982, 611.5709],
  [89, 659.5708, 662.6836],
  [90, 655.5885, 614.3091],
  [91, 590.2788, 672.7236],
  [92, 583.1106, 624.3491],
  [93, 528.1549, 676.3745],
  [94, 513.0221, 567.76],
  [95, 476.385, 680.9382],
  [96, 464.4381, 607.0073],
  [97, 743.9956, 354.1818],
  [98, 772.6681, 331.3636],
  [99, 807.7124, 309.4582],
  [100, 860.2788, 268.3855],
  [101, 723.2876, 290.2909],
  [102, 764.7035, 264.7345],
  [103, 813.2876, 232.7891],
  [104, 854.7035, 205.4073],
  [105, 707.3584, 242.8291],
  [106, 751.1637, 215.4473],
  [107, 813.2876, 177.1127],
  [108, 691.4292, 194.4545],
  [109, 733.6416, 167.9855],
  [110, 789.3938, 134.2145],
  [111, 715.323, 131.4764],
  [112, 836.385, 131.4764],
  [113, 890.5442, 131.4764],
  [114, 945.5, 132.3891],
  [135, 313.9071, 356.92],
  [149, 135.5, 589.6655],
  [150, 47.8894, 588.7527],
  [151, 596.6504, 119.6109],
  [152, 327.4469, 552.2436],
  [160, 692.2257, 421.7236],
  [195, 475.5885, 722.0109],
  [197, 733.0, 320.0],
  [250, 280.4558, 87.6655],
  [251, 281.2522, 121.4364],
  [300, 662.7566, 116.8727],
  [350, 688.2434, 92.2291],
  [450, 900.1018, 242.8291],
  [451, 939.9248, 213.6218],
  [610, 725.677, 471.9236],
];

/** configNo → phaseCode */
const PHASE_CONFIG_MAP: Record<number, PhaseCode> = {
  1: "ABC", 2: "ABC", 3: "ABC", 4: "ABC", 5: "ABC", 6: "ABC",
  7: "AC",  8: "AB",  9: "A",   10: "B",  11: "C",  12: "ABC",
};

/** [from, to, lengthFt, configNo] */
const RAW_LINE_SEGMENTS: [number, number, number, number][] = [
  [1, 2, 175.0, 10],
  [1, 3, 250.0, 11],
  [1, 7, 300.0, 1],
  [3, 4, 200.0, 11],
  [3, 5, 325.0, 11],
  [5, 6, 250.0, 11],
  [7, 8, 200.0, 1],
  [8, 12, 225.0, 10],
  [8, 9, 225.0, 9],
  [8, 13, 300.0, 1],
  [9, 14, 425.0, 9],
  [13, 34, 150.0, 11],
  [13, 18, 825.0, 2],
  [14, 11, 250.0, 9],
  [14, 10, 250.0, 9],
  [15, 16, 375.0, 11],
  [15, 17, 350.0, 11],
  [18, 19, 250.0, 9],
  [18, 21, 300.0, 2],
  [19, 20, 325.0, 9],
  [21, 22, 525.0, 10],
  [21, 23, 250.0, 2],
  [23, 24, 550.0, 11],
  [23, 25, 275.0, 2],
  [25, 26, 350.0, 7],
  [25, 28, 200.0, 2],
  [26, 27, 275.0, 7],
  [26, 31, 225.0, 11],
  [27, 33, 500.0, 9],
  [28, 29, 300.0, 2],
  [29, 30, 350.0, 2],
  [30, 250, 200.0, 2],
  [31, 32, 300.0, 11],
  [34, 15, 100.0, 11],
  [35, 36, 650.0, 8],
  [35, 40, 250.0, 1],
  [36, 37, 300.0, 9],
  [36, 38, 250.0, 10],
  [38, 39, 325.0, 10],
  [40, 41, 325.0, 11],
  [40, 42, 250.0, 1],
  [42, 43, 500.0, 10],
  [42, 44, 200.0, 1],
  [44, 45, 200.0, 9],
  [44, 47, 250.0, 1],
  [45, 46, 300.0, 9],
  [47, 48, 150.0, 4],
  [47, 49, 250.0, 4],
  [49, 50, 250.0, 4],
  [50, 51, 250.0, 4],
  [51, 151, 500.0, 4],
  [52, 53, 200.0, 1],
  [53, 54, 125.0, 1],
  [54, 55, 275.0, 1],
  [54, 57, 350.0, 3],
  [55, 56, 275.0, 1],
  [57, 58, 250.0, 10],
  [57, 60, 750.0, 3],
  [58, 59, 250.0, 10],
  [60, 61, 550.0, 5],
  [60, 62, 250.0, 12],
  [62, 63, 175.0, 12],
  [63, 64, 350.0, 12],
  [64, 65, 425.0, 12],
  [65, 66, 325.0, 12],
  [67, 68, 200.0, 9],
  [67, 72, 275.0, 3],
  [67, 97, 250.0, 3],
  [68, 69, 275.0, 9],
  [69, 70, 325.0, 9],
  [70, 71, 275.0, 9],
  [72, 73, 275.0, 11],
  [72, 76, 200.0, 3],
  [73, 74, 350.0, 11],
  [74, 75, 400.0, 11],
  [76, 77, 400.0, 6],
  [76, 86, 700.0, 3],
  [77, 78, 100.0, 6],
  [78, 79, 225.0, 6],
  [78, 80, 475.0, 6],
  [80, 81, 475.0, 6],
  [81, 82, 250.0, 6],
  [81, 84, 675.0, 11],
  [82, 83, 250.0, 6],
  [84, 85, 475.0, 11],
  [86, 87, 450.0, 6],
  [87, 88, 175.0, 9],
  [87, 89, 275.0, 6],
  [89, 90, 225.0, 10],
  [89, 91, 225.0, 6],
  [91, 92, 300.0, 11],
  [91, 93, 225.0, 6],
  [93, 94, 275.0, 9],
  [93, 95, 300.0, 6],
  [95, 96, 200.0, 10],
  [97, 98, 275.0, 3],
  [98, 99, 550.0, 3],
  [99, 100, 300.0, 3],
  [100, 450, 800.0, 3],
  [101, 102, 225.0, 11],
  [101, 105, 275.0, 3],
  [102, 103, 325.0, 11],
  [103, 104, 700.0, 11],
  [105, 106, 225.0, 10],
  [105, 108, 325.0, 3],
  [106, 107, 575.0, 10],
  [108, 109, 450.0, 9],
  [108, 300, 1000.0, 3],
  [109, 110, 300.0, 9],
  [110, 111, 575.0, 9],
  [110, 112, 125.0, 9],
  [112, 113, 525.0, 9],
  [113, 114, 325.0, 9],
  [135, 35, 375.0, 4],
  [149, 1, 400.0, 1],
  [152, 52, 400.0, 1],
  [160, 67, 350.0, 6],
  [197, 101, 250.0, 3],
];

/** [from, to, status] – only "closed" switches are included as energised branches */
const RAW_SWITCHES: [number, number, string][] = [
  [13, 152, "closed"],
  [18, 135, "closed"],
  [60, 160, "closed"],
  [61, 610, "closed"],
  [97, 197, "closed"],
  [150, 149, "closed"],
  [250, 251, "open"],
  [450, 451, "open"],
  [54, 94, "open"],
  [151, 300, "open"],
  [300, 350, "open"],
];

/** Buses that carry load – [busNo, totalKW] */
const RAW_LOADS: [number, number][] = [
  [1, 40], [2, 20], [4, 40], [5, 20], [6, 40], [7, 20], [9, 40],
  [10, 20], [11, 40], [12, 20], [16, 40], [17, 20], [19, 40],
  [20, 40], [22, 40], [24, 40], [28, 40], [29, 40], [30, 40],
  [31, 20], [32, 20], [33, 40], [34, 40], [35, 40], [37, 40],
  [38, 20], [39, 20], [41, 20], [42, 20], [43, 40], [45, 20],
  [46, 20], [47, 35], [48, 70], [49, 35], [50, 40], [51, 20],
  [52, 40], [53, 40], [55, 20], [56, 20], [58, 20], [59, 20],
  [60, 20], [62, 40], [63, 40], [64, 75], [65, 35],
  [66, 75], [68, 20], [69, 40], [70, 20], [71, 40], [73, 40],
  [74, 40], [75, 35], [76, 105], [77, 40], [79, 40], [80, 40],
  [82, 40], [83, 20], [84, 20], [85, 40], [86, 20], [87, 40],
  [88, 40], [90, 40], [92, 40], [94, 40], [95, 20], [96, 20],
  [98, 40], [99, 40], [100, 40], [102, 20], [103, 40], [104, 40],
  [106, 40], [107, 40], [109, 40], [111, 20], [112, 20],
  [113, 40], [114, 20],
];

/** Buses with shunt capacitors */
const CAP_BUSES = new Set([83, 88, 90, 92]);

/* ------------------------------------------------------------------ */
/*  Build the IEEE 123-bus graph                                       */
/* ------------------------------------------------------------------ */

export function buildIEEE123(): NetworkGraph {
  /* ── coordinate normalisation ─────────────────────────────── */
  const xs = RAW_COORDS.map(([, x]) => x);
  const ys = RAW_COORDS.map(([,, y]) => y);
  const xMin = Math.min(...xs), xMax = Math.max(...xs);
  const yMin = Math.min(...ys), yMax = Math.max(...ys);
  const cx = (xMin + xMax) / 2;
  const cy = (yMin + yMax) / 2;
  const halfRange = Math.max(xMax - xMin, yMax - yMin) / 2;

  const loadSet = new Map(RAW_LOADS.map(([b, kw]) => [b, kw]));

  const buses: Bus[] = RAW_COORDS.map(([id, rawX, rawY]) => {
    const totalLoadKW = loadSet.get(id) ?? 0;
    let type: BusType = "junction";
    if (id === 150) type = "source";
    else if (CAP_BUSES.has(id)) type = "capacitor";
    else if (totalLoadKW > 0) type = "load";

    return {
      id,
      name: `Bus ${id}`,
      type,
      totalLoadKW,
      // normalise to roughly [-1, 1]
      x: (rawX - cx) / halfRange,
      // flip Y so the feeder goes top → bottom visually like the one-line
      y: -(rawY - cy) / halfRange,
    };
  });

  /* ── branches (line segments + closed switches) ──────────── */
  const branches: Branch[] = [];

  for (const [from, to, len, cfg] of RAW_LINE_SEGMENTS) {
    branches.push({
      from,
      to,
      phaseCode: PHASE_CONFIG_MAP[cfg] ?? "ABC",
      lengthFt: len,
      isSwitch: false,
    });
  }

  for (const [from, to, status] of RAW_SWITCHES) {
    if (status === "closed") {
      branches.push({
        from,
        to,
        phaseCode: "ABC",
        lengthFt: 0,
        isSwitch: true,
      });
    }
  }

  return { buses, branches };
}

/* keep backward-compat alias so existing import doesn't break */
export { buildIEEE123 as buildIEEE118 };

/* ------------------------------------------------------------------ */
/*  Build a NetworkGraph from UI elements / connections (dynamic nets) */
/* ------------------------------------------------------------------ */

interface UIElement {
  id: string;
  type: string;
  name?: string;
  x: number;
  y: number;
  busType?: string;
  activePowerKW?: number;
  reactivePowerKVAR?: number;
  ratingKVAR?: number;
  phaseCode?: string;
  phaseA?: boolean;
  phaseB?: boolean;
  phaseC?: boolean;
  fromElementId?: string;
  toElementId?: string;
  fromBusId?: string;
  toBusId?: string;
  isClosed?: boolean;
  connectedBusId?: string;
  [key: string]: unknown;
}

interface UIConnection {
  id: string;
  fromElementId: string;
  toElementId: string;
  [key: string]: unknown;
}

function inferPhaseCode(el: UIElement): PhaseCode {
  if (el.phaseCode) {
    const pc = el.phaseCode.toUpperCase();
    if (pc === "A" || pc === "B" || pc === "C" || pc === "AB" || pc === "AC" || pc === "BC" || pc === "ABC") {
      return pc as PhaseCode;
    }
  }
  const a = el.phaseA ?? true;
  const b = el.phaseB ?? true;
  const c = el.phaseC ?? true;
  if (a && b && c) return "ABC";
  if (a && b) return "AB";
  if (a && c) return "AC";
  if (b && c) return "BC";
  if (a) return "A";
  if (b) return "B";
  if (c) return "C";
  return "ABC";
}

/**
 * Convert the flat UI element/connection arrays (as returned by the backend
 * `multiconductor_net_to_ui` converter or edited in the Network Editor) into
 * the `NetworkGraph` format consumed by the 3-D visualisation.
 */
export function buildFromUIElements(
  elements: UIElement[],
  connections: UIConnection[],
): NetworkGraph {
  /* ── index buses by their element id ──────────────────────── */
  const busElements = elements.filter((el) => el.type === "bus");
  const busIdToIdx = new Map<string, number>();
  busElements.forEach((b, i) => busIdToIdx.set(b.id, i));

  /* ── determine which buses carry load / capacitor / source ── */
  const extGridBuses = new Set<string>();
  const loadBuses = new Map<string, number>(); // busId → total kW
  const capBuses = new Set<string>();

  for (const el of elements) {
    if (el.type === "external_source") {
      const busId = el.connectedBusId as string | undefined;
      if (busId) extGridBuses.add(busId);
      // Also check connections
      for (const c of connections) {
        if (c.fromElementId === el.id && busIdToIdx.has(c.toElementId)) extGridBuses.add(c.toElementId);
        if (c.toElementId === el.id && busIdToIdx.has(c.fromElementId)) extGridBuses.add(c.fromElementId);
      }
    } else if (el.type === "load") {
      const busId = el.connectedBusId as string | undefined;
      const kw = (el.activePowerKW as number) ?? 0;
      if (busId) loadBuses.set(busId, (loadBuses.get(busId) ?? 0) + kw);
      for (const c of connections) {
        if (c.fromElementId === el.id && busIdToIdx.has(c.toElementId))
          loadBuses.set(c.toElementId, (loadBuses.get(c.toElementId) ?? 0) + kw);
        if (c.toElementId === el.id && busIdToIdx.has(c.fromElementId))
          loadBuses.set(c.fromElementId, (loadBuses.get(c.fromElementId) ?? 0) + kw);
      }
    } else if (el.type === "capacitor") {
      const busId = el.connectedBusId as string | undefined;
      if (busId) capBuses.add(busId);
      for (const c of connections) {
        if (c.fromElementId === el.id && busIdToIdx.has(c.toElementId)) capBuses.add(c.toElementId);
        if (c.toElementId === el.id && busIdToIdx.has(c.fromElementId)) capBuses.add(c.fromElementId);
      }
    }
  }

  /* ── normalise coordinates ─────────────────────────────────── */
  const xs = busElements.map((b) => b.x);
  const ys = busElements.map((b) => b.y);
  const xMin = Math.min(...xs), xMax = Math.max(...xs);
  const yMin = Math.min(...ys), yMax = Math.max(...ys);
  const cx = (xMin + xMax) / 2;
  const cy = (yMin + yMax) / 2;
  const halfRange = Math.max(xMax - xMin, yMax - yMin, 1) / 2;

  /* ── create buses ──────────────────────────────────────────── */
  const buses: Bus[] = busElements.map((b, i) => {
    const totalLoadKW = loadBuses.get(b.id) ?? 0;
    let type: BusType = "junction";
    if (extGridBuses.has(b.id) || b.busType === "slack") type = "source";
    else if (capBuses.has(b.id)) type = "capacitor";
    else if (totalLoadKW > 0) type = "load";

    return {
      id: i,
      sourceElementId: b.id,
      name: b.name,
      type,
      totalLoadKW,
      x: (b.x - cx) / halfRange,
      y: -(b.y - cy) / halfRange,
    };
  });

  /* ── helper: element-id to bus index ────────────────────────── */
  const eidToBusIdx = new Map<string, number>();
  busElements.forEach((b, i) => eidToBusIdx.set(b.id, i));

  /* ── create branches from line/transformer/switch/cable ─── */
  const branches: Branch[] = [];

  for (const el of elements) {
    let fromId: string | undefined;
    let toId: string | undefined;

    if (el.type === "line" || el.type === "cable") {
      fromId = el.fromElementId as string | undefined;
      toId = el.toElementId as string | undefined;
    } else if (el.type === "transformer" || el.type === "switch") {
      fromId = el.fromBusId as string | undefined;
      toId = el.toBusId as string | undefined;
    }

    if (!fromId || !toId) {
      // Try to resolve from connections
      const conns = connections.filter(
        (c) => c.fromElementId === el.id || c.toElementId === el.id,
      );
      const busConns = conns
        .map((c) =>
          c.fromElementId === el.id ? c.toElementId : c.fromElementId,
        )
        .filter((eid) => eidToBusIdx.has(eid));
      if (busConns.length >= 2) {
        fromId = busConns[0];
        toId = busConns[1];
      } else {
        continue;
      }
    }

    const fromIdx = eidToBusIdx.get(fromId);
    const toIdx = eidToBusIdx.get(toId);
    if (fromIdx === undefined || toIdx === undefined) continue;

    branches.push({
      from: fromIdx,
      to: toIdx,
      phaseCode: inferPhaseCode(el),
      lengthFt: 0,
      isSwitch: el.type === "switch",
    });
  }

  return { buses, branches };
}
