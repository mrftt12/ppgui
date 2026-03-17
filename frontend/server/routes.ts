import type { Express } from "express";
import { static as expressStatic } from "express";
import { createServer, type Server } from "node:http";
import { storage } from "./storage";
import { insertNetworkModelSchema, insertEquipmentTemplateSchema } from "@shared/schema";
import { z } from "zod";
import { loadSampleCase } from "./sample-cases";
import { generateIEEE123Case } from "./ieee123";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { log } from "./index";

const CURRENT_DIR =
  typeof __dirname !== "undefined"
    ? __dirname
    : dirname(fileURLToPath(import.meta.url));

// FastAPI backend URL for multiconductor load flow
const FASTAPI_URL = process.env.FASTAPI_URL || "http://localhost:8000";

const DEFAULT_BASE_FREQUENCY_HZ = 60;
const DEFAULT_BASE_VOLTAGE_KV = 13.8;

const resolveBaseVoltage = (payload: { elements?: Array<Record<string, unknown>> }) => {
  const elements = payload.elements ?? [];
  const bus = elements.find(
    (element) => element.type === "bus" && typeof element.nominalVoltageKV === "number"
  );
  if (bus && typeof bus.nominalVoltageKV === "number") {
    return bus.nominalVoltageKV;
  }
  const source = elements.find(
    (element) => element.type === "external_source" && typeof element.voltageKV === "number"
  );
  if (source && typeof source.voltageKV === "number") {
    return source.voltageKV;
  }
  return DEFAULT_BASE_VOLTAGE_KV;
};

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  // Static SVG symbology assets for map markers
  app.use("/svg", expressStatic(resolve(CURRENT_DIR, "..", "public", "svg")));

  // Health check
  app.get("/api/health", (_req, res) => {
    res.json({ status: "ok", timestamp: new Date().toISOString() });
  });

  // Network models CRUD
  app.get("/api/networks", async (req, res) => {
    try {
      const networks = await storage.getNetworks();
      res.json(networks);
    } catch (error) {
      res.status(500).json({ error: "Failed to fetch networks" });
    }
  });

  app.get("/api/networks/:id", async (req, res) => {
    try {
      const network = await storage.getNetwork(req.params.id);
      if (!network) {
        return res.status(404).json({ error: "Network not found" });
      }
      res.json(network);
    } catch (error) {
      res.status(500).json({ error: "Failed to fetch network" });
    }
  });

  app.post("/api/networks", async (req, res) => {
    try {
      const createSchema = z.object({
        name: z.string().min(1, "Name is required"),
        description: z.string().optional(),
        baseFrequencyHz: z.coerce.number().positive("Base frequency must be positive"),
        baseVoltageKV: z.coerce.number().positive("Base voltage must be positive"),
        elements: z.array(z.any()).optional().default([]),
        connections: z.array(z.any()).optional().default([]),
      });
      const validated = createSchema.parse(req.body);
      const network = await storage.createNetwork(validated);
      res.status(201).json(network);
    } catch (error) {
      if (error instanceof z.ZodError) {
        return res.status(400).json({ error: "Invalid network data", details: error.errors });
      }
      res.status(400).json({ error: "Failed to create network" });
    }
  });

  app.put("/api/networks/:id", async (req, res) => {
    try {
      const updateSchema = z.object({
        elements: z.array(z.any()).default([]),
        connections: z.array(z.any()).default([]),
      });
      const validated = updateSchema.parse(req.body);
      const network = await storage.updateNetwork(req.params.id, validated);
      if (!network) {
        return res.status(404).json({ error: "Network not found" });
      }
      res.json(network);
    } catch (error) {
      if (error instanceof z.ZodError) {
        return res.status(400).json({ error: "Invalid network data", details: error.errors });
      }
      res.status(400).json({ error: "Failed to update network" });
    }
  });

  app.delete("/api/networks/:id", async (req, res) => {
    try {
      const deleted = await storage.deleteNetwork(req.params.id);
      if (!deleted) {
        return res.status(404).json({ error: "Network not found" });
      }
      res.status(204).send();
    } catch (error) {
      res.status(500).json({ error: "Failed to delete network" });
    }
  });

  // Load flow analysis - proxy to FastAPI backend for real multiconductor solve
  app.post("/api/networks/:id/analyze", async (req, res) => {
    try {
      const { elements, connections } = req.body;

      // Try to proxy to FastAPI backend first
      try {
        const response = await fetch(`${FASTAPI_URL}/api/loadflow`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            networkId: req.params.id,
            elements: elements || [],
            connections: connections || [],
          }),
        });

        if (response.ok) {
          const result = await response.json();
          log(`Load flow analysis completed via multiconductor backend`);
          return res.json(result);
        }

        // Fall back to local mock if backend returns error
        log(`FastAPI backend returned ${response.status}, using local mock`);
      } catch (proxyError) {
        // FastAPI backend not available, use local mock
        log(`FastAPI backend not available, using local mock analysis`);
      }

      // Fallback to local mock analysis
      const result = await storage.runLoadFlowAnalysis(
        req.params.id,
        elements || [],
        connections || []
      );
      res.json(result);
    } catch (error) {
      res.status(500).json({ error: "Failed to run analysis" });
    }
  });

  // Ductbank steady-state temperature calculation
  app.post("/api/ductbank/steady-state", async (req, res) => {
    try {
      const response = await fetch(`${FASTAPI_URL}/api/ductbank/steady-state`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req.body || {}),
      });

      if (!response.ok) {
        const text = await response.text();
        return res.status(response.status).json({ error: text || "Backend error" });
      }

      const payload = await response.json();
      res.json(payload);
    } catch (error) {
      res.status(502).json({ error: "Failed to reach thermal backend" });
    }
  });

  // ST_CHARLES sample case endpoint (loaded from PKL via FastAPI test networks)
  app.get("/api/samples/st-charles", async (_req, res) => {
    try {
      const response = await fetch(
        `${FASTAPI_URL}/api/test-networks/${encodeURIComponent("ST_CHARLES")}`
      );
      if (!response.ok) {
        return res.status(502).json({ error: "FastAPI backend unavailable for ST_CHARLES sample" });
      }
      const payload = await response.json();
      const network = await storage.createNetwork({
        name: payload.name || "ST_CHARLES",
        description: payload.description || "ST_CHARLES multiconductor sample case.",
        baseFrequencyHz: DEFAULT_BASE_FREQUENCY_HZ,
        baseVoltageKV: resolveBaseVoltage(payload),
        elements: payload.elements,
        connections: payload.connections,
      });

      log(`Created ST_CHARLES sample case with ${payload.elements.length} elements`);
      res.status(201).json(network);
    } catch (error) {
      res.status(500).json({ error: "Failed to generate ST_CHARLES sample" });
    }
  });

  // IEEE 123-bus sample case endpoint
  app.get("/api/samples/ieee-123", async (_req, res) => {
    try {
      const payload = generateIEEE123Case();
      const network = await storage.createNetwork({
        name: "IEEE 123-Bus System",
        description: "IEEE 123-bus test feeder with 123 buses, 85 line segments, and switches.",
        baseFrequencyHz: DEFAULT_BASE_FREQUENCY_HZ,
        baseVoltageKV: resolveBaseVoltage(payload),
        elements: payload.elements,
        connections: payload.connections,
      });
      log(`Created IEEE 123-bus sample case with ${payload.elements.length} elements`);
      res.status(201).json(network);
    } catch (error) {
      res.status(500).json({ error: "Failed to generate IEEE 123-bus sample" });
    }
  });

  // IEEE 9-bus sample case endpoint
  app.get("/api/samples/ieee-9", async (req, res) => {
    try {
      let payload;

      try {
        const response = await fetch(`${FASTAPI_URL}/api/samples/ieee-9`);
        if (!response.ok) {
          throw new Error(`FastAPI sample returned ${response.status}`);
        }
        payload = await response.json();
      } catch (error) {
        payload = loadSampleCase("ieee9");
      }

      const network = await storage.createNetwork({
        name: "IEEE 9-Bus System",
        description: "IEEE 9-bus test case with 9 buses, 9 branches, 3 generators, and 3 loads.",
        baseFrequencyHz: DEFAULT_BASE_FREQUENCY_HZ,
        baseVoltageKV: resolveBaseVoltage(payload),
        elements: payload.elements,
        connections: payload.connections,
      });
      log(`Created IEEE 9-bus sample case with ${payload.elements.length} elements`);
      res.status(201).json(network);
    } catch (error) {
      res.status(500).json({ error: "Failed to generate IEEE 9-bus sample" });
    }
  });

  // IEEE 30-bus sample case endpoint
  app.get("/api/samples/ieee-30", async (req, res) => {
    try {
      let payload;

      try {
        const response = await fetch(`${FASTAPI_URL}/api/samples/ieee-30`);
        if (!response.ok) {
          throw new Error(`FastAPI sample returned ${response.status}`);
        }
        payload = await response.json();
      } catch (error) {
        payload = loadSampleCase("ieee30");
      }

      const network = await storage.createNetwork({
        name: "IEEE 30-Bus System",
        description: "IEEE 30-bus test case with 30 buses, 41 branches, 6 generators, and 21 loads.",
        baseFrequencyHz: DEFAULT_BASE_FREQUENCY_HZ,
        baseVoltageKV: resolveBaseVoltage(payload),
        elements: payload.elements,
        connections: payload.connections,
      });
      log(`Created IEEE 30-bus sample case with ${payload.elements.length} elements`);
      res.status(201).json(network);
    } catch (error) {
      res.status(500).json({ error: "Failed to generate IEEE 30-bus sample" });
    }
  });

  // Test networks list
  app.get("/api/getnetworks", async (_req, res) => {
    try {
      const response = await fetch(`${FASTAPI_URL}/api/getnetworks`);
      if (!response.ok) {
        return res.status(502).json({ error: "FastAPI backend unavailable for test networks" });
      }
      const payload = await response.json();
      res.json(payload);
    } catch (error) {
      res.status(500).json({ error: "Failed to fetch test networks" });
    }
  });

  // ST_CHARLES KML overlay (served as raw KML)
  app.get("/api/kml/st-charles", (_req, res) => {
    try {
      const filePath = resolve(CURRENT_DIR, "samples", "st_charles.kml");
      const kml = readFileSync(filePath, "utf-8");
      res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
      res.send(kml);
    } catch (error) {
      res.status(500).json({ error: "Failed to load ST_CHARLES KML" });
    }
  });

  // Load test network from multiconductor pkl
  app.get("/api/test-networks/:name", async (req, res) => {
    try {
      const response = await fetch(
        `${FASTAPI_URL}/api/test-networks/${encodeURIComponent(req.params.name)}`
      );
      if (!response.ok) {
        return res.status(502).json({ error: "FastAPI backend unavailable for test network" });
      }
      const payload = await response.json();
      const network = await storage.createNetwork({
        name: payload.name || req.params.name,
        description: payload.description || "Multiconductor test network",
        baseFrequencyHz: DEFAULT_BASE_FREQUENCY_HZ,
        baseVoltageKV: resolveBaseVoltage(payload),
        elements: payload.elements,
        connections: payload.connections,
      });
      log(`Created test network ${network.name} with ${payload.elements.length} elements`);
      res.status(201).json(network);
    } catch (error) {
      res.status(500).json({ error: "Failed to load test network" });
    }
  });

  // Equipment templates CRUD
  app.get("/api/equipment-templates", async (req, res) => {
    try {
      const templates = await storage.getEquipmentTemplates();
      res.json(templates);
    } catch (error) {
      res.status(500).json({ error: "Failed to fetch equipment templates" });
    }
  });

  app.get("/api/equipment-templates/:id", async (req, res) => {
    try {
      const template = await storage.getEquipmentTemplate(req.params.id);
      if (!template) {
        return res.status(404).json({ error: "Equipment template not found" });
      }
      res.json(template);
    } catch (error) {
      res.status(500).json({ error: "Failed to fetch equipment template" });
    }
  });

  app.post("/api/equipment-templates", async (req, res) => {
    try {
      const data = insertEquipmentTemplateSchema.parse(req.body);
      const template = await storage.createEquipmentTemplate(data);
      res.status(201).json(template);
    } catch (error) {
      if (error instanceof z.ZodError) {
        return res.status(400).json({ error: "Invalid equipment template data", details: error.errors });
      }
      res.status(400).json({ error: "Failed to create equipment template" });
    }
  });

  app.delete("/api/equipment-templates/:id", async (req, res) => {
    try {
      const deleted = await storage.deleteEquipmentTemplate(req.params.id);
      if (!deleted) {
        return res.status(404).json({ error: "Equipment template not found" });
      }
      res.status(204).send();
    } catch (error) {
      res.status(500).json({ error: "Failed to delete equipment template" });
    }
  });

  return httpServer;
}
