import { z } from "zod";

// Element types for the power system network
export const elementTypes = [
  "external_source",
  "bus",
  "line",
  "transformer",
  "load",
  "generator",
  "battery",
  "capacitor",
  "switch",
  "cable",
  "ductbank"
] as const;

export type ElementType = typeof elementTypes[number];

// Three-phase values for unbalanced analysis
export const phaseComplexSchema = z.object({
  phaseA: z.object({ real: z.number(), imag: z.number() }),
  phaseB: z.object({ real: z.number(), imag: z.number() }),
  phaseC: z.object({ real: z.number(), imag: z.number() }),
});

export type PhaseComplex = z.infer<typeof phaseComplexSchema>;

// Base element properties
export const baseElementSchema = z.object({
  id: z.string(),
  name: z.string(),
  type: z.enum(elementTypes),
  x: z.number(),
  y: z.number(),
  rotation: z.number().default(0),
  enabled: z.boolean().default(true),
  geoLat: z.number().optional(),
  geoLon: z.number().optional(),
  geoX: z.number().optional(),
  geoY: z.number().optional(),
});

// External source (grid connection)
export const externalSourceSchema = baseElementSchema.extend({
  type: z.literal("external_source"),
  voltageKV: z.number().default(13.8),
  shortCircuitMVA: z.number().default(100),
  xrRatio: z.number().default(10),
  phaseAngle: z.number().default(0),
});

// Bus (node)
export const busSchema = baseElementSchema.extend({
  type: z.literal("bus"),
  nominalVoltageKV: z.number().default(13.8),
  busType: z.enum(["slack", "pv", "pq"]).default("pq"),
  width: z.number().default(96), // Width in pixels, default ~100px
});

// Line (overhead transmission/distribution line)
export const lineSchema = baseElementSchema.extend({
  type: z.literal("line"),
  installation: z.enum(["overhead", "underground"]).default("overhead"),
  lengthKm: z.number().default(1),
  resistanceOhmPerKm: z.number().default(0.1),
  reactanceOhmPerKm: z.number().default(0.4),
  susceptanceSPerKm: z.number().default(0),
  fromElementId: z.string().optional(), // Can connect to any element
  toElementId: z.string().optional(),   // Can connect to any element
  phaseCode: z.string().optional(),
  phaseA: z.boolean().optional(),
  phaseB: z.boolean().optional(),
  phaseC: z.boolean().optional(),
  phaseColor: z.string().optional(),
});

// Transformer
export const transformerSchema = baseElementSchema.extend({
  type: z.literal("transformer"),
  ratingMVA: z.number().default(10),
  primaryVoltageKV: z.number().default(13.8),
  secondaryVoltageKV: z.number().default(0.48),
  impedancePercent: z.number().default(5.75),
  xrRatio: z.number().default(10),
  tapPosition: z.number().default(0),
  connectionType: z.enum(["Yg-Yg", "Yg-D", "D-Yg", "D-D"]).default("D-Yg"),
  fromBusId: z.string().optional(),
  toBusId: z.string().optional(),
});

// Load
export const loadSchema = baseElementSchema.extend({
  type: z.literal("load"),
  activePowerKW: z.number().default(100),
  reactivePowerKVAR: z.number().default(30),
  loadModel: z.enum(["constant_power", "constant_current", "constant_impedance"]).default("constant_power"),
  connectedBusId: z.string().optional(),
  unbalanced: z.boolean().default(false),
  phaseAPower: z.number().default(33.33),
  phaseBPower: z.number().default(33.33),
  phaseCPower: z.number().default(33.34),
  phaseCode: z.string().optional(),
  phaseA: z.boolean().optional(),
  phaseB: z.boolean().optional(),
  phaseC: z.boolean().optional(),
  phaseColor: z.string().optional(),
});

// Generator
export const generatorSchema = baseElementSchema.extend({
  type: z.literal("generator"),
  ratingMVA: z.number().default(5),
  activePowerMW: z.number().default(4),
  voltageSetpointPU: z.number().default(1.0),
  minReactivePowerMVAR: z.number().default(-2),
  maxReactivePowerMVAR: z.number().default(3),
  connectedBusId: z.string().optional(),
});

// Battery (energy storage)
export const batterySchema = baseElementSchema.extend({
  type: z.literal("battery"),
  capacityKWh: z.number().default(500),
  maxPowerKW: z.number().default(250),
  stateOfCharge: z.number().default(50),
  chargingEfficiency: z.number().default(0.95),
  dischargingEfficiency: z.number().default(0.95),
  connectedBusId: z.string().optional(),
});

// Capacitor (power factor correction)
export const capacitorSchema = baseElementSchema.extend({
  type: z.literal("capacitor"),
  ratingKVAR: z.number().default(100),
  nominalVoltageKV: z.number().default(0.48),
  steps: z.number().default(1),
  currentStep: z.number().default(1),
  connectedBusId: z.string().optional(),
  phaseCode: z.string().optional(),
  phaseA: z.boolean().optional(),
  phaseB: z.boolean().optional(),
  phaseC: z.boolean().optional(),
  phaseColor: z.string().optional(),
});

// Switch
export const switchSchema = baseElementSchema.extend({
  type: z.literal("switch"),
  isClosed: z.boolean().default(true),
  ratedCurrentA: z.number().default(600),
  fromBusId: z.string().optional(),
  toBusId: z.string().optional(),
  phaseCode: z.string().optional(),
  phaseA: z.boolean().optional(),
  phaseB: z.boolean().optional(),
  phaseC: z.boolean().optional(),
  phaseColor: z.string().optional(),
});

// Cable (underground)
export const cableSchema = baseElementSchema.extend({
  type: z.literal("cable"),
  lengthKm: z.number().default(0.5),
  resistanceOhmPerKm: z.number().default(0.15),
  reactanceOhmPerKm: z.number().default(0.1),
  capacitanceuFPerKm: z.number().default(0.3),
  ratedCurrentA: z.number().default(400),
  fromBusId: z.string().optional(),
  toBusId: z.string().optional(),
});

// Ductbank (underground)
export const ductbankSchema = baseElementSchema.extend({
  type: z.literal("ductbank"),
  parentLineId: z.string().optional(),
  rows: z.number().default(4),
  columns: z.number().default(2),
  thickness: z.number().default(0.25),
  verticalSpacing: z.number().default(6),
  horizontalSpacing: z.number().default(6),
  depth: z.number().default(36),
  soilResistivity: z.number().default(90),
  ductDiameterIn: z.number().default(5),
  ducts: z.array(z.object({
    row: z.number(),
    column: z.number(),
    diameter: z.number(),
    thickness: z.number(),
    load: z.number(),
    loadFactor: z.number(),
  })).default([]),
});

// Union of all element schemas
export const networkElementSchema = z.discriminatedUnion("type", [
  externalSourceSchema,
  busSchema,
  lineSchema,
  transformerSchema,
  loadSchema,
  generatorSchema,
  batterySchema,
  capacitorSchema,
  switchSchema,
  cableSchema,
  ductbankSchema,
]);

export type NetworkElement = z.infer<typeof networkElementSchema>;
export type ExternalSource = z.infer<typeof externalSourceSchema>;
export type Bus = z.infer<typeof busSchema>;
export type Line = z.infer<typeof lineSchema>;
export type Transformer = z.infer<typeof transformerSchema>;
export type Load = z.infer<typeof loadSchema>;
export type Generator = z.infer<typeof generatorSchema>;
export type Battery = z.infer<typeof batterySchema>;
export type Capacitor = z.infer<typeof capacitorSchema>;
export type Switch = z.infer<typeof switchSchema>;
export type Cable = z.infer<typeof cableSchema>;
export type Ductbank = z.infer<typeof ductbankSchema>;

// Connection between elements
export const connectionSchema = z.object({
  id: z.string(),
  fromElementId: z.string(),
  toElementId: z.string(),
  fromPort: z.enum(["input", "output", "terminal"]).default("output"),
  toPort: z.enum(["input", "output", "terminal"]).default("input"),
});

export type Connection = z.infer<typeof connectionSchema>;

// Network model (a complete power system)
export const networkModelSchema = z.object({
  id: z.string(),
  name: z.string(),
  description: z.string().optional(),
  baseFrequencyHz: z.number(),
  baseVoltageKV: z.number(),
  elements: z.array(networkElementSchema),
  connections: z.array(connectionSchema),
  createdAt: z.string(),
  updatedAt: z.string(),
});

export type NetworkModel = z.infer<typeof networkModelSchema>;

// Insert schemas
export const insertNetworkModelSchema = networkModelSchema.omit({ id: true, createdAt: true, updatedAt: true });
export type InsertNetworkModel = z.infer<typeof insertNetworkModelSchema>;

// Equipment database entry (templates)
export const equipmentTemplateSchema = z.object({
  id: z.string(),
  name: z.string(),
  manufacturer: z.string().optional(),
  model: z.string().optional(),
  type: z.enum(elementTypes),
  defaultProperties: z.record(z.any()),
  description: z.string().optional(),
});

export type EquipmentTemplate = z.infer<typeof equipmentTemplateSchema>;

export const insertEquipmentTemplateSchema = equipmentTemplateSchema.omit({ id: true });
export type InsertEquipmentTemplate = z.infer<typeof insertEquipmentTemplateSchema>;

// Load flow results
export const loadFlowResultSchema = z.object({
  networkId: z.string(),
  converged: z.boolean(),
  iterations: z.number(),
  timestamp: z.string(),
  busResults: z.array(z.object({
    busId: z.string(),
    busName: z.string(),
    voltagePhaseA: z.object({ magnitude: z.number(), angle: z.number() }),
    voltagePhaseB: z.object({ magnitude: z.number(), angle: z.number() }),
    voltagePhaseC: z.object({ magnitude: z.number(), angle: z.number() }),
  })),
  branchResults: z.array(z.object({
    branchId: z.string(),
    branchName: z.string(),
    currentPhaseA: z.number(),
    currentPhaseB: z.number(),
    currentPhaseC: z.number(),
    powerFlowMW: z.number(),
    powerFlowMVAR: z.number(),
    losses: z.number(),
  })),
});

export type LoadFlowResult = z.infer<typeof loadFlowResultSchema>;

// Legacy user types for compatibility
export const users = {
  $inferSelect: {} as { id: string; username: string; password: string }
};

export const insertUserSchema = z.object({
  username: z.string(),
  password: z.string(),
});

export type InsertUser = z.infer<typeof insertUserSchema>;
export type User = typeof users.$inferSelect;
