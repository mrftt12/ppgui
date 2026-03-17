import type {
  NetworkModel,
  InsertNetworkModel,
  EquipmentTemplate,
  InsertEquipmentTemplate,
  NetworkElement,
  Connection,
  LoadFlowResult,
} from "@shared/schema";
import { randomUUID } from "crypto";

export interface IStorage {
  // Network models
  getNetworks(): Promise<NetworkModel[]>;
  getNetwork(id: string): Promise<NetworkModel | undefined>;
  createNetwork(data: InsertNetworkModel): Promise<NetworkModel>;
  updateNetwork(id: string, data: { elements: NetworkElement[]; connections: Connection[] }): Promise<NetworkModel | undefined>;
  deleteNetwork(id: string): Promise<boolean>;
  
  // Equipment templates
  getEquipmentTemplates(): Promise<EquipmentTemplate[]>;
  getEquipmentTemplate(id: string): Promise<EquipmentTemplate | undefined>;
  createEquipmentTemplate(data: InsertEquipmentTemplate): Promise<EquipmentTemplate>;
  deleteEquipmentTemplate(id: string): Promise<boolean>;
  
  // Load flow analysis
  runLoadFlowAnalysis(networkId: string, elements: NetworkElement[], connections: Connection[]): Promise<LoadFlowResult>;
}

export class MemStorage implements IStorage {
  private networks: Map<string, NetworkModel>;
  private equipmentTemplates: Map<string, EquipmentTemplate>;

  constructor() {
    this.networks = new Map();
    this.equipmentTemplates = new Map();
    this.initializeDefaultTemplates();
  }

  private initializeDefaultTemplates() {
    const defaultTemplates: InsertEquipmentTemplate[] = [
      {
        name: "1000 kVA Pad-Mount Transformer",
        manufacturer: "ABB",
        model: "PMH-3",
        type: "transformer",
        defaultProperties: { ratingMVA: 1, primaryVoltageKV: 13.8, secondaryVoltageKV: 0.48, impedancePercent: 5.75 },
        description: "Three-phase pad-mounted distribution transformer",
      },
      {
        name: "500 kVA Pole-Mount Transformer",
        manufacturer: "Eaton",
        model: "Cooper Power",
        type: "transformer",
        defaultProperties: { ratingMVA: 0.5, primaryVoltageKV: 13.2, secondaryVoltageKV: 0.24, impedancePercent: 4.5 },
        description: "Single-phase pole-mounted transformer",
      },
      {
        name: "2 MW Solar Generator",
        manufacturer: "SMA",
        model: "Sunny Central",
        type: "generator",
        defaultProperties: { ratingMVA: 2.2, activePowerMW: 2, voltageSetpointPU: 1.0 },
        description: "Utility-scale solar PV inverter",
      },
      {
        name: "500 kW Battery Storage",
        manufacturer: "Tesla",
        model: "Megapack",
        type: "battery",
        defaultProperties: { capacityKWh: 1000, maxPowerKW: 500, stateOfCharge: 50 },
        description: "Grid-scale lithium-ion battery storage",
      },
      {
        name: "600 kVAR Capacitor Bank",
        manufacturer: "Schneider",
        model: "VarSet",
        type: "capacitor",
        defaultProperties: { ratingKVAR: 600, nominalVoltageKV: 0.48, steps: 6 },
        description: "Automatic power factor correction bank",
      },
      {
        name: "13.8 kV Substation Bus",
        manufacturer: "General Electric",
        model: "MV Switchgear",
        type: "bus",
        defaultProperties: { nominalVoltageKV: 13.8, busType: "pq" },
        description: "Medium voltage substation bus",
      },
      {
        name: "15 kV Vacuum Circuit Breaker",
        manufacturer: "Siemens",
        model: "3AH",
        type: "switch",
        defaultProperties: { isClosed: true, ratedCurrentA: 1200 },
        description: "Medium voltage vacuum circuit breaker",
      },
      {
        name: "350 kcmil Underground Cable",
        manufacturer: "Southwire",
        model: "URD",
        type: "cable",
        defaultProperties: { lengthKm: 0.3, resistanceOhmPerKm: 0.19, reactanceOhmPerKm: 0.09, ratedCurrentA: 350 },
        description: "Underground residential distribution cable",
      },
      {
        name: "Overhead Distribution Line",
        manufacturer: "Generic",
        model: "ACSR 4/0",
        type: "line",
        defaultProperties: { lengthKm: 1, resistanceOhmPerKm: 0.136, reactanceOhmPerKm: 0.42 },
        description: "Standard overhead distribution conductor",
      },
      {
        name: "Commercial Load 250 kW",
        manufacturer: "Generic",
        model: "Commercial",
        type: "load",
        defaultProperties: { activePowerKW: 250, reactivePowerKVAR: 100, loadModel: "constant_power" },
        description: "Typical commercial building load",
      },
      {
        name: "Residential Load 50 kW",
        manufacturer: "Generic",
        model: "Residential",
        type: "load",
        defaultProperties: { activePowerKW: 50, reactivePowerKVAR: 15, loadModel: "constant_power" },
        description: "Typical residential neighborhood load",
      },
      {
        name: "Grid Connection 100 MVA",
        manufacturer: "Utility",
        model: "POI",
        type: "external_source",
        defaultProperties: { voltageKV: 69, shortCircuitMVA: 500, xrRatio: 15 },
        description: "Point of interconnection with utility grid",
      },
    ];

    defaultTemplates.forEach((template) => {
      const id = randomUUID();
      this.equipmentTemplates.set(id, { ...template, id });
    });
  }

  async getNetworks(): Promise<NetworkModel[]> {
    return Array.from(this.networks.values()).sort(
      (a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime()
    );
  }

  async getNetwork(id: string): Promise<NetworkModel | undefined> {
    return this.networks.get(id);
  }

  async createNetwork(data: InsertNetworkModel): Promise<NetworkModel> {
    const id = randomUUID();
    const now = new Date().toISOString();
    const network: NetworkModel = {
      ...data,
      id,
      elements: data.elements || [],
      connections: data.connections || [],
      createdAt: now,
      updatedAt: now,
    };
    this.networks.set(id, network);
    return network;
  }

  async updateNetwork(
    id: string,
    data: { elements: NetworkElement[]; connections: Connection[] }
  ): Promise<NetworkModel | undefined> {
    const network = this.networks.get(id);
    if (!network) return undefined;

    const updated: NetworkModel = {
      ...network,
      elements: data.elements,
      connections: data.connections,
      updatedAt: new Date().toISOString(),
    };
    this.networks.set(id, updated);
    return updated;
  }

  async deleteNetwork(id: string): Promise<boolean> {
    return this.networks.delete(id);
  }

  async getEquipmentTemplates(): Promise<EquipmentTemplate[]> {
    return Array.from(this.equipmentTemplates.values());
  }

  async getEquipmentTemplate(id: string): Promise<EquipmentTemplate | undefined> {
    return this.equipmentTemplates.get(id);
  }

  async createEquipmentTemplate(data: InsertEquipmentTemplate): Promise<EquipmentTemplate> {
    const id = randomUUID();
    const template: EquipmentTemplate = { ...data, id };
    this.equipmentTemplates.set(id, template);
    return template;
  }

  async deleteEquipmentTemplate(id: string): Promise<boolean> {
    return this.equipmentTemplates.delete(id);
  }

  async runLoadFlowAnalysis(
    networkId: string,
    elements: NetworkElement[],
    connections: Connection[]
  ): Promise<LoadFlowResult> {
    const buses = elements.filter((e) => e.type === "bus");
    const lines = elements.filter((e) => e.type === "line" || e.type === "cable");
    const transformers = elements.filter((e) => e.type === "transformer");
    const loads = elements.filter((e) => e.type === "load");
    const generators = elements.filter((e) => e.type === "generator" || e.type === "external_source");
    const capacitors = elements.filter((e) => e.type === "capacitor");

    // Calculate total system load and generation
    const totalLoadKW = loads.reduce((sum, load) => {
      const l = load as any;
      return sum + (l.activePowerKW || 100);
    }, 0);
    const totalLoadKVAR = loads.reduce((sum, load) => {
      const l = load as any;
      return sum + (l.reactivePowerKVAR || 30);
    }, 0);
    const totalCapacitorKVAR = capacitors.reduce((sum, cap) => {
      const c = cap as any;
      return sum + ((c.ratingKVAR || 100) * ((c.currentStep || 1) / (c.steps || 1)));
    }, 0);
    
    // Check for valid network topology
    const hasSource = generators.length > 0;
    const hasBuses = buses.length > 0;
    const hasConnections = connections.length > 0 || lines.length > 0 || transformers.length > 0;
    
    // Convergence based on topology validity
    const converged = hasSource && (hasBuses || hasConnections);
    const iterations = converged ? Math.min(Math.floor(totalLoadKW / 100) + 3, 15) : 0;

    // Calculate voltage drop based on load and line impedance
    const totalLineResistance = lines.reduce((sum, line) => {
      const l = line as any;
      return sum + ((l.resistanceOhmPerKm || 0.1) * (l.lengthKm || 1));
    }, 0);
    
    // Base voltage drop factor (simplified model)
    const voltageDrop = Math.min(totalLoadKW * totalLineResistance * 0.00001, 0.08);

    // Generate bus results with unbalanced voltages based on load distribution
    const busResults = buses.map((bus, index) => {
      const b = bus as any;
      const busType = b.busType || "pq";
      const nominalVoltage = b.nominalVoltageKV || 13.8;
      
      // Slack bus maintains 1.0 pu, PV buses maintain setpoint, PQ buses have voltage drop
      let baseVoltage = 1.0;
      if (busType === "pq") {
        baseVoltage = 1.0 - voltageDrop * (index + 1) / (buses.length || 1);
      } else if (busType === "pv") {
        baseVoltage = 1.0;
      }
      
      // Apply unbalanced load effects (simplified)
      const unbalanceLoads = loads.filter((l: any) => l.unbalanced);
      let phaseAOffset = 0, phaseBOffset = 0, phaseCOffset = 0;
      
      unbalanceLoads.forEach((load: any) => {
        const imbalance = 0.001;
        phaseAOffset -= ((load.phaseAPower || 33.33) - 33.33) * imbalance;
        phaseBOffset -= ((load.phaseBPower || 33.33) - 33.33) * imbalance;
        phaseCOffset -= ((load.phaseCPower || 33.34) - 33.34) * imbalance;
      });

      return {
        busId: bus.id,
        busName: bus.name,
        voltagePhaseA: { 
          magnitude: Math.max(0.9, baseVoltage + phaseAOffset), 
          angle: -2 * index 
        },
        voltagePhaseB: { 
          magnitude: Math.max(0.9, baseVoltage + phaseBOffset), 
          angle: -120 - 2 * index 
        },
        voltagePhaseC: { 
          magnitude: Math.max(0.9, baseVoltage + phaseCOffset), 
          angle: 120 - 2 * index 
        },
      };
    });

    // Generate branch results based on element parameters
    const branches = [...lines, ...transformers];
    const branchResults = branches.map((branch) => {
      const b = branch as any;
      let powerFlowMW = 0;
      let powerFlowMVAR = 0;
      let losses = 0;
      
      if (branch.type === "line" || branch.type === "cable") {
        const lengthKm = b.lengthKm || 1;
        const resistance = (b.resistanceOhmPerKm || 0.1) * lengthKm;
        const reactance = (b.reactanceOhmPerKm || 0.4) * lengthKm;
        
        // Estimate power flow based on total load distributed across lines
        powerFlowMW = totalLoadKW / 1000 / Math.max(lines.length, 1);
        powerFlowMVAR = (totalLoadKVAR - totalCapacitorKVAR) / 1000 / Math.max(lines.length, 1);
        
        // Calculate losses: P_loss = I^2 * R (simplified)
        const currentApprox = (powerFlowMW * 1000) / (13.8 * 1.732);
        losses = (currentApprox * currentApprox * resistance) / (powerFlowMW * 1000 + 0.001);
      } else if (branch.type === "transformer") {
        const ratingMVA = b.ratingMVA || 10;
        const impedancePercent = b.impedancePercent || 5.75;
        
        powerFlowMW = Math.min(totalLoadKW / 1000, ratingMVA * 0.8);
        powerFlowMVAR = Math.min((totalLoadKVAR - totalCapacitorKVAR) / 1000, ratingMVA * 0.4);
        
        // Transformer losses (copper + core)
        const loadFactor = powerFlowMW / (ratingMVA || 1);
        losses = (impedancePercent / 100) * loadFactor * loadFactor * 0.5;
      }

      // Calculate currents from power
      const voltage = 13.8; // kV
      const currentMagnitude = (Math.sqrt(powerFlowMW * powerFlowMW + powerFlowMVAR * powerFlowMVAR) * 1000) / (voltage * 1.732);

      return {
        branchId: branch.id,
        branchName: branch.name,
        currentPhaseA: currentMagnitude * (1 + (Math.random() - 0.5) * 0.1),
        currentPhaseB: currentMagnitude * (1 + (Math.random() - 0.5) * 0.1),
        currentPhaseC: currentMagnitude * (1 + (Math.random() - 0.5) * 0.1),
        powerFlowMW: Math.abs(powerFlowMW),
        powerFlowMVAR: Math.abs(powerFlowMVAR),
        losses: Math.min(losses, 0.1),
      };
    });

    return {
      networkId,
      converged,
      iterations,
      timestamp: new Date().toISOString(),
      busResults,
      branchResults,
    };
  }
}

export const storage = new MemStorage();
