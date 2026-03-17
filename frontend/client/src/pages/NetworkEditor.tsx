import { useState, useCallback, useRef, useEffect, useMemo } from "react";
import type { ImperativePanelHandle } from "react-resizable-panels";
import { useQuery, useMutation } from "@tanstack/react-query";
import { DndContext, DragEndEvent, DragOverlay, useDraggable } from "@dnd-kit/core";
import type {
  NetworkElement,
  NetworkModel,
  Connection,
  ElementType,
  EquipmentTemplate,
  LoadFlowResult,
} from "@shared/schema";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { ThemeToggle } from "@/components/ThemeToggle";
import { NetworkSidebar } from "@/components/NetworkSidebar";
import { ElementSidebar } from "@/components/ElementSidebar";
import { ReactFlowCanvas } from "@/components/ReactFlowCanvas";
import { PropertiesPanel } from "@/components/PropertiesPanel";
import { NetworkModelManager } from "@/components/NetworkModelManager";
import { EquipmentDatabase } from "@/components/EquipmentDatabase";
import { LoadFlowAnalysis } from "@/components/LoadFlowAnalysis";
import { NetworkToolbar, type LayoutSettings } from "@/components/NetworkToolbar";
import { StatusBar } from "@/components/StatusBar";
import { SettingsDialog, type Settings } from "@/components/SettingsDialog";
import { elementLabels, ElementIcon } from "@/components/ElementIcons";
import { Legend } from "@/components/Legend";
import { ResultsDock } from "@/components/ResultsDock";
import { SpatialMapView } from "@/components/map/SpatialMapView";
import { DeckGLMapView } from "@/components/map/DeckGLMapView";
import FlowVisualization from "@/pages/FlowVisualization";
import {
  ResizableHandle,
  ResizablePanel,
  ResizablePanelGroup,
} from "@/components/ui/resizable";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { cluster, hierarchy, pack, partition, stratify, tree, treemap } from "d3-hierarchy";
import {
  forceCenter,
  forceCollide,
  forceLink,
  forceManyBody,
  forceSimulation,
} from "d3-force";
import {
  Network,
  Settings2,
  FolderOpen,
  Database,
  Activity,
  Zap,
} from "lucide-react";

export default function NetworkEditor() {
  const { toast } = useToast();
  const canvasContainerRef = useRef<HTMLDivElement>(null);
  const resultsPanelRef = useRef<ImperativePanelHandle>(null);

  const [elements, setElements] = useState<NetworkElement[]>([]);
  const [connections, setConnections] = useState<Connection[]>([]);
  const [selectedElementId, setSelectedElementId] = useState<string | null>(null);
  const [selectedElementIds, setSelectedElementIds] = useState<string[]>([]);
  const [currentModelId, setCurrentModelId] = useState<string | null>(null);
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
  const [newModelDialogOpen, setNewModelDialogOpen] = useState(false);

  // Right sidebar state
  const [rightTab, setRightTab] = useState("properties");
  const [isRightCollapsed, setIsRightCollapsed] = useState(false);

  // Left sidebar state
  const [isNetworkCollapsed, setIsNetworkCollapsed] = useState(false);
  const [isElementsCollapsed, setIsElementsCollapsed] = useState(false);

  const [analysisResult, setAnalysisResult] = useState<LoadFlowResult | null>(null);
  const [cableTempResult, setCableTempResult] = useState<{
    ductbankId: string;
    ductbankName: string;
    rows: number;
    columns: number;
    temperatures: number[];
    timestamp: string;
  } | null>(null);
  const [isRunningAnalysis, setIsRunningAnalysis] = useState(false);
  const [elementCounter, setElementCounter] = useState<Record<string, number>>({});
  const [activeDragType, setActiveDragType] = useState<ElementType | null>(null);
  const [canvasView, setCanvasView] = useState<"oneLine" | "spatial" | "threeD" | "deckMap">("spatial");
  const showLabels = false;
  const [undergroundLineId, setUndergroundLineId] = useState<string | null>(null);
  const [isResultsCollapsed, setIsResultsCollapsed] = useState(false);
  const [lastLayout, setLastLayout] = useState<LayoutAlgorithm>("radial");
  const [layoutSettings, setLayoutSettings] = useState<LayoutSettings>({
    componentSpacing: 5,
    ringSpacing: 140,
    nodeSpacing: 5,
    levelSpacing: 220,
    gridSpacing: 140,
    circularRadius: 180,
    spiralStep: 12,
    forceCharge: -220,
    forceLinkDistance: 120,
    forceCollide: 30,
    forceIterations: 140,
  });

  // Settings state
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [settings, setSettings] = useState<Settings>({
    frequency: "60",
    units: "metric",
    paletteView: "grid",
  });

  const { data: models = [] } = useQuery<NetworkModel[]>({
    queryKey: ["/api/networks"],
  });

  const { data: templates = [] } = useQuery<EquipmentTemplate[]>({
    queryKey: ["/api/equipment-templates"],
  });

  const { data: testNetworksPayload } = useQuery<{ networks: Array<{ name: string; displayName: string; hasGeodata: boolean }> }>({
    queryKey: ["/api/getnetworks"],
  });
  const testNetworks = testNetworksPayload?.networks || [];

  const migrateDuctbankDefaults = useCallback((items: NetworkElement[]) => {
    let didChange = false;
    const next = items.map((el) => {
      if (el.type !== "ductbank") return el;
      const ductbank = el as any;
      let nextDuctbank = ductbank;

      if (ductbank.thickness == null || ductbank.thickness === 6) {
        nextDuctbank = { ...nextDuctbank, thickness: 0.25 };
        didChange = true;
      }
      if (ductbank.ductDiameterIn == null || ductbank.ductDiameterIn === 6) {
        nextDuctbank = { ...nextDuctbank, ductDiameterIn: 5 };
        didChange = true;
      }

      if (Array.isArray(ductbank.ducts)) {
        let ductsChanged = false;
        const nextDucts = ductbank.ducts.map((duct: any) => {
          let nextDuct = duct;
          let ductChanged = false;

          if (duct.thickness == null || duct.thickness === 6) {
            nextDuct = { ...nextDuct, thickness: 0.25 };
            ductChanged = true;
          }
          if (duct.diameter == null || duct.diameter === 6) {
            nextDuct = { ...nextDuct, diameter: 5 };
            ductChanged = true;
          }

          if (ductChanged) {
            ductsChanged = true;
            return nextDuct;
          }

          return duct;
        });

        if (ductsChanged) {
          nextDuctbank = { ...nextDuctbank, ducts: nextDucts };
          didChange = true;
        }
      }

      return nextDuctbank;
    });

    return { elements: next, didChange };
  }, []);

  const createModelMutation = useMutation({
    mutationFn: async (data: {
      name: string;
      description?: string;
      baseFrequencyHz: number;
      baseVoltageKV: number;
    }) => {
      return apiRequest("POST", "/api/networks", data);
    },
    onSuccess: async (response) => {
      const newModel = await response.json();
      queryClient.invalidateQueries({ queryKey: ["/api/networks"] });
      setCurrentModelId(newModel.id);
      setElements([]);
      setConnections([]);
      setHasUnsavedChanges(false);
      setSelectedElementIds([]);
      setSelectedElementId(null);
      setAnalysisResult(null);
      setCableTempResult(null);
      setElementCounter({});
      toast({ title: "Network created", description: `"${newModel.name}" has been created.` });
    },
  });

  const saveModelMutation = useMutation({
    mutationFn: async () => {
      if (!currentModelId) return;
      return apiRequest("PUT", `/api/networks/${currentModelId}`, {
        elements,
        connections,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/networks"] });
      setHasUnsavedChanges(false);
      toast({ title: "Network saved", description: "Your changes have been saved." });
    },
  });

  const deleteModelMutation = useMutation({
    mutationFn: async (id: string) => {
      return apiRequest("DELETE", `/api/networks/${id}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/networks"] });
      if (currentModelId === deleteModelMutation.variables) {
      setCurrentModelId(null);
      setElements([]);
      setConnections([]);
      setSelectedElementIds([]);
    }
    toast({ title: "Network deleted" });
    },
  });

  const addTemplateMutation = useMutation({
    mutationFn: async (template: Omit<EquipmentTemplate, "id">) => {
      return apiRequest("POST", "/api/equipment-templates", template);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/equipment-templates"] });
      toast({ title: "Equipment template added" });
    },
  });

  const runAnalysisMutation = useMutation({
    mutationFn: async () => {
      if (!currentModelId) throw new Error("No network loaded");
      setIsRunningAnalysis(true);
      return apiRequest("POST", `/api/networks/${currentModelId}/analyze`, {
        elements,
        connections,
      });
    },
    onSuccess: async (response) => {
      const result = await response.json();
      setAnalysisResult(result);
      setIsRunningAnalysis(false);
      toast({
        title: result.converged ? "Analysis complete" : "Analysis failed to converge",
        description: result.converged
          ? `Converged in ${result.iterations} iterations`
          : "The power flow did not converge. Check your network.",
        variant: result.converged ? "default" : "destructive",
      });
    },
    onError: () => {
      setIsRunningAnalysis(false);
      toast({
        title: "Analysis error",
        description: "An error occurred during analysis.",
        variant: "destructive",
      });
    },
  });

  const loadSampleMutation = useMutation({
    mutationFn: async (sampleType: string) => {
      return apiRequest("GET", `/api/samples/${sampleType}`);
    },
    onSuccess: async (response) => {
      const newModel = await response.json();
      const migrated = migrateDuctbankDefaults(newModel.elements);
      queryClient.invalidateQueries({ queryKey: ["/api/networks"] });
      setCurrentModelId(newModel.id);
      setElements(migrated.elements);
      setConnections(newModel.connections);
      setHasUnsavedChanges(migrated.didChange);
      setSelectedElementId(null);
      setSelectedElementIds([]);
      setAnalysisResult(null);
      setCableTempResult(null);
      toast({
        title: "Sample loaded",
        description: `"${newModel.name}" with ${newModel.elements.length} elements loaded successfully.`,
      });
    },
    onError: () => {
      toast({
        title: "Failed to load sample",
        description: "An error occurred while loading the sample case.",
        variant: "destructive",
      });
    },
  });

  const loadTestNetworkMutation = useMutation({
    mutationFn: async (networkName: string) => {
      return apiRequest("GET", `/api/test-networks/${encodeURIComponent(networkName)}`);
    },
    onSuccess: async (response) => {
      const newModel = await response.json();
      const migrated = migrateDuctbankDefaults(newModel.elements);
      queryClient.invalidateQueries({ queryKey: ["/api/networks"] });
      setCurrentModelId(newModel.id);
      setElements(migrated.elements);
      setConnections(newModel.connections);
      setHasUnsavedChanges(migrated.didChange);
      setSelectedElementId(null);
      setSelectedElementIds([]);
      setAnalysisResult(null);
      setCableTempResult(null);

      const hasGeodata = migrated.elements.some((el) => {
        const geoLat = (el as { geoLat?: number }).geoLat;
        const geoLon = (el as { geoLon?: number }).geoLon;
        return Number.isFinite(geoLat) && Number.isFinite(geoLon);
      });
      setCanvasView(hasGeodata ? "spatial" : "oneLine");

      toast({
        title: "Test network loaded",
        description: `"${newModel.name}" loaded successfully.`,
      });
    },
    onError: () => {
      toast({
        title: "Failed to load test network",
        description: "An error occurred while loading the test network.",
        variant: "destructive",
      });
    },
  });

  // Auto-save effect
  useEffect(() => {
    if (hasUnsavedChanges && currentModelId) {
      const timer = setTimeout(() => {
        saveModelMutation.mutate();
      }, 2000); // Auto-save after 2 seconds of inactivity

      return () => clearTimeout(timer);
    }
  }, [hasUnsavedChanges, currentModelId, elements, connections]);

  // Auto-load IEEE 123-bus network on initial mount
  useEffect(() => {
    if (elements.length === 0 && !loadSampleMutation.isPending) {
      loadSampleMutation.mutate("ieee-123");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // Run only once on mount

  const loadModel = useCallback((id: string) => {
    const model = models.find((m) => m.id === id);
    if (model) {
      const migrated = migrateDuctbankDefaults(model.elements);
      setCurrentModelId(model.id);
      setElements(migrated.elements);
      setConnections(model.connections);
      setHasUnsavedChanges(migrated.didChange);
      setSelectedElementId(null);
      setSelectedElementIds([]);
      setAnalysisResult(null);
      setCableTempResult(null);
    }
  }, [models, migrateDuctbankDefaults]);

  const handleNewModel = useCallback(() => {
    setRightTab("models");
    setNewModelDialogOpen(true);
  }, [setRightTab, setNewModelDialogOpen]);

  const handleResetWorkspace = useCallback(() => {
    const hasContent = elements.length > 0 || connections.length > 0;
    if (hasContent) {
      const confirmed = window.confirm(
        "Clear all elements and connections from the workspace? This cannot be undone."
      );
      if (!confirmed) return;
    }
    setElements([]);
    setConnections([]);
    setSelectedElementId(null);
    setSelectedElementIds([]);
    setAnalysisResult(null);
    setCableTempResult(null);
    setElementCounter({});
    setHasUnsavedChanges(hasContent && Boolean(currentModelId));
  }, [elements.length, connections.length, currentModelId]);

  const handleUpdateLayoutSettings = useCallback((updates: Partial<LayoutSettings>) => {
    setLayoutSettings((prev) => ({ ...prev, ...updates }));
  }, []);

  const handleResetLayoutSettings = useCallback(() => {
    setLayoutSettings({
      componentSpacing: 5,
      ringSpacing: 140,
      nodeSpacing: 5,
      levelSpacing: 220,
      gridSpacing: 140,
      circularRadius: 180,
      spiralStep: 12,
      forceCharge: -220,
      forceLinkDistance: 120,
      forceCollide: 30,
      forceIterations: 140,
    });
  }, []);

  useEffect(() => {
    const migrated = migrateDuctbankDefaults(elements);
    if (migrated.didChange) {
      setElements(migrated.elements);
      setHasUnsavedChanges(true);
    }
  }, [elements, migrateDuctbankDefaults]);

  const generateElementName = useCallback((type: ElementType) => {
    const count = (elementCounter[type] || 0) + 1;
    setElementCounter((prev) => ({ ...prev, [type]: count }));
    return `${elementLabels[type]} ${count}`;
  }, [elementCounter]);

  const addElement = useCallback((type: ElementType, x: number, y: number) => {
    const newElement: NetworkElement = {
      id: crypto.randomUUID(),
      name: generateElementName(type),
      type,
      x,
      y,
      rotation: 0,
      enabled: true,
      ...(type === "external_source" && {
        voltageKV: 13.8,
        shortCircuitMVA: 100,
        xrRatio: 10,
        phaseAngle: 0,
      }),
      ...(type === "bus" && {
        nominalVoltageKV: 13.8,
        busType: "pq" as const,
      }),
      ...(type === "line" && {
        installation: "overhead" as const,
        lengthKm: 1,
        resistanceOhmPerKm: 0.1,
        reactanceOhmPerKm: 0.4,
        susceptanceSPerKm: 0,
      }),
      ...(type === "transformer" && {
        ratingMVA: 10,
        primaryVoltageKV: 13.8,
        secondaryVoltageKV: 0.48,
        impedancePercent: 5.75,
        xrRatio: 10,
        tapPosition: 0,
        connectionType: "D-Yg" as const,
      }),
      ...(type === "load" && {
        activePowerKW: 100,
        reactivePowerKVAR: 30,
        loadModel: "constant_power" as const,
        unbalanced: false,
        phaseAPower: 33.33,
        phaseBPower: 33.33,
        phaseCPower: 33.34,
      }),
      ...(type === "generator" && {
        ratingMVA: 5,
        activePowerMW: 4,
        voltageSetpointPU: 1.0,
        minReactivePowerMVAR: -2,
        maxReactivePowerMVAR: 3,
      }),
      ...(type === "battery" && {
        capacityKWh: 500,
        maxPowerKW: 250,
        stateOfCharge: 50,
        chargingEfficiency: 0.95,
        dischargingEfficiency: 0.95,
      }),
      ...(type === "capacitor" && {
        ratingKVAR: 100,
        nominalVoltageKV: 0.48,
        steps: 1,
        currentStep: 1,
      }),
      ...(type === "switch" && {
        isClosed: true,
        ratedCurrentA: 600,
      }),
      ...(type === "cable" && {
        lengthKm: 0.5,
        resistanceOhmPerKm: 0.15,
        reactanceOhmPerKm: 0.1,
        capacitanceuFPerKm: 0.3,
        ratedCurrentA: 400,
      }),
      ...(type === "ductbank" && {
        parentLineId: undefined,
        rows: 4,
        columns: 2,
        thickness: 6,
        verticalSpacing: 6,
        horizontalSpacing: 6,
        depth: 36,
        soilResistivity: 90,
        ductDiameterIn: 6,
        ducts: [],
      }),
    } as NetworkElement;

    setElements((prev) => [...prev, newElement]);
    setHasUnsavedChanges(true);
    // Auto-select and switch tab
    setSelectedElementId(newElement.id);
    setRightTab("properties");
  }, [generateElementName]);

  const addDuctbankToLine = useCallback((lineId: string, x = 200, y = 200) => {
    const newElement: NetworkElement = {
      id: crypto.randomUUID(),
      name: generateElementName("ductbank"),
      type: "ductbank",
      x,
      y,
      rotation: 0,
      enabled: true,
      parentLineId: lineId,
      rows: 4,
      columns: 2,
      thickness: 0.25,
      verticalSpacing: 6,
      horizontalSpacing: 6,
      depth: 36,
      soilResistivity: 90,
      ductDiameterIn: 5,
      ducts: [],
    } as NetworkElement;

    setElements((prev) => [...prev, newElement]);
    setHasUnsavedChanges(true);
    setSelectedElementId(newElement.id);
    setSelectedElementIds([newElement.id]);
    setRightTab("properties");
  }, [generateElementName]);

  const updateElement = useCallback((id: string, updates: Partial<NetworkElement>) => {
    setElements((prev) =>
      prev.map((el) => (el.id === id ? { ...el, ...updates } as NetworkElement : el))
    );
    setHasUnsavedChanges(true);
  }, []);

  const updateElementsPositions = useCallback(
    (positions: Record<string, { x: number; y: number }>) => {
      let didChange = false;
      setElements((prev) => {
        const next = prev.map((el) => {
          const pos = positions[el.id];
          if (!pos) return el;
          if (el.x === pos.x && el.y === pos.y) return el;
          didChange = true;
          return { ...el, x: pos.x, y: pos.y } as NetworkElement;
        });
        return didChange ? next : prev;
      });
      if (didChange) {
        setHasUnsavedChanges(true);
      }
    },
    []
  );

  const visibleElements = useMemo(
    () => elements.filter((el) => el.type !== "ductbank"),
    [elements]
  );

  const undergroundElements = useMemo(
    () =>
      elements.filter(
        (el) => el.type === "ductbank" && (el as { parentLineId?: string }).parentLineId === undergroundLineId
      ),
    [elements, undergroundLineId]
  );

  const undergroundLine = useMemo(
    () => elements.find((el) => el.id === undergroundLineId && el.type === "line"),
    [elements, undergroundLineId]
  );

  type LayoutAlgorithm =
    | "radial"
    | "layered"
    | "grid"
    | "cluster"
    | "circular"
    | "concentric"
    | "spiral"
    | "tree"
    | "force"
    | "hierarchy"
    | "stratify"
    | "partition"
    | "pack"
    | "treemap";

  const autoLayout = useCallback((
    algorithm: LayoutAlgorithm,
    settings: LayoutSettings,
    sourceElements?: NetworkElement[],
    sourceConnections?: Connection[]
  ) => {
    const activeElements = sourceElements ?? elements;
    const activeConnections = sourceConnections ?? connections;

    const busElements = activeElements.filter((el) => el.type === "bus");
    if (busElements.length === 0) return;

    const busIds = new Set(busElements.map((el) => el.id));
    const elementById = new Map(activeElements.map((el) => [el.id, el]));
    const slackBusIds = busElements
      .filter((el) => (el as { busType?: string }).busType === "slack")
      .map((el) => el.id);

    const adjacency = new Map<string, Set<string>>();
    busIds.forEach((id) => adjacency.set(id, new Set()));

    const addEdge = (a?: string, b?: string) => {
      if (!a || !b || a === b || !busIds.has(a) || !busIds.has(b)) return;
      adjacency.get(a)?.add(b);
      adjacency.get(b)?.add(a);
    };

    const busIdsFromConnections = (elementId: string) => {
      const connected: string[] = [];
      activeConnections.forEach((conn) => {
        if (conn.fromElementId === elementId && busIds.has(conn.toElementId)) {
          connected.push(conn.toElementId);
        }
        if (conn.toElementId === elementId && busIds.has(conn.fromElementId)) {
          connected.push(conn.fromElementId);
        }
      });
      return Array.from(new Set(connected));
    };

    const resolveBusPair = (el: NetworkElement): [string | undefined, string | undefined] => {
      let fromBus = (el as { fromBusId?: string }).fromBusId;
      let toBus = (el as { toBusId?: string }).toBusId;
      const fromElementId = (el as { fromElementId?: string }).fromElementId;
      const toElementId = (el as { toElementId?: string }).toElementId;
      if (!fromBus && fromElementId && busIds.has(fromElementId)) fromBus = fromElementId;
      if (!toBus && toElementId && busIds.has(toElementId)) toBus = toElementId;
      if (!fromBus || !toBus) {
        const candidates = busIdsFromConnections(el.id);
        if (!fromBus) fromBus = candidates[0];
        if (!toBus) toBus = candidates[1];
      }
      return [fromBus, toBus];
    };

    activeElements.forEach((el) => {
      if (el.type === "line" || el.type === "cable" || el.type === "transformer") {
        const [a, b] = resolveBusPair(el);
        addEdge(a, b);
      }
    });

    const parseBusSortKey = (id: string) => {
      const name = String(elementById.get(id)?.name ?? "");
      const match = name.match(/\d+/g);
      if (match && match.length > 0) {
        const last = match[match.length - 1];
        return Number.parseInt(last, 10);
      }
      return Number.MAX_SAFE_INTEGER;
    };

    const busPositions = new Map<string, { x: number; y: number }>();
    const busIdList = Array.from(busIds).sort((a, b) => parseBusSortKey(a) - parseBusSortKey(b));

    const components: string[][] = [];
    const visited = new Set<string>();
    for (const startId of busIdList) {
      if (visited.has(startId)) continue;
      const queue = [startId];
      const component: string[] = [];
      visited.add(startId);

      while (queue.length) {
        const current = queue.shift()!;
        component.push(current);
        adjacency.get(current)?.forEach((next) => {
          if (!visited.has(next)) {
            visited.add(next);
            queue.push(next);
          }
        });
      }

      components.push(component);
    }

    const buildDepthLevels = (component: string[]) => {
      const roots = slackBusIds.filter((id) => component.includes(id));
      const rootList = roots.length ? roots : [component[0]];
      const depth = new Map<string, number>();
      const depthQueue = [...rootList];
      rootList.forEach((id) => depth.set(id, 0));

      while (depthQueue.length) {
        const current = depthQueue.shift()!;
        const d = depth.get(current) ?? 0;
        adjacency.get(current)?.forEach((next) => {
          if (!depth.has(next)) {
            depth.set(next, d + 1);
            depthQueue.push(next);
          }
        });
      }

      const levels = new Map<number, string[]>();
      component.forEach((id) => {
        const d = depth.get(id) ?? 0;
        const bucket = levels.get(d) ?? [];
        bucket.push(id);
        levels.set(d, bucket);
      });

      return levels;
    };

    const buildTreeEdges = (component: string[]) => {
      const roots = slackBusIds.filter((id) => component.includes(id));
      const rootList = roots.length ? roots : [component[0]];
      const parent = new Map<string, string | null>();
      const queue = [...rootList];
      rootList.forEach((id) => parent.set(id, null));

      while (queue.length) {
        const current = queue.shift()!;
        adjacency.get(current)?.forEach((next) => {
          if (!parent.has(next)) {
            parent.set(next, current);
            queue.push(next);
          }
        });
      }

      const nodes = component.map((id) => ({
        id,
        parentId: parent.get(id) ?? null,
      }));
      return nodes;
    };

    if (algorithm === "radial") {
      const componentCount = components.length;
      const gridCols = Math.max(1, Math.ceil(Math.sqrt(componentCount)));
      const componentSpacing = settings.componentSpacing;
      const ringSpacing = settings.ringSpacing;
      const baseRadius = Math.max(20, settings.nodeSpacing * 0.3);

      components.forEach((component, index) => {
        const levels = buildDepthLevels(component);
        const componentCol = index % gridCols;
        const componentRow = Math.floor(index / gridCols);
        const centerX = 250 + componentCol * componentSpacing;
        const centerY = 200 + componentRow * componentSpacing;

        Array.from(levels.entries())
          .sort((a, b) => a[0] - b[0])
          .forEach(([level, ids]) => {
            ids.sort((a, b) => parseBusSortKey(a) - parseBusSortKey(b));
            const radius = baseRadius + level * ringSpacing;
            const count = ids.length;
            const angleStep = (Math.PI * 2) / Math.max(count, 1);
            ids.forEach((id, idx) => {
              const angle = angleStep * idx;
              const x = centerX + radius * Math.cos(angle);
              const y = centerY + radius * Math.sin(angle);
              busPositions.set(id, { x: Math.round(x), y: Math.round(y) });
            });
          });
      });
    }

    if (algorithm === "cluster") {
      const componentCount = components.length;
      const gridCols = Math.max(1, Math.ceil(Math.sqrt(componentCount)));
      const componentSpacing = settings.componentSpacing * 1.1;
      const ringSpacing = settings.ringSpacing * 1.4;
      const baseRadius = Math.max(60, settings.nodeSpacing * 0.6);
      const nodeSize = Math.max(12, settings.nodeSpacing * 0.18);
      const separation = Math.max(12, settings.nodeSpacing * 0.2);

      const buildTree = (rootId: string) => {
        const visitedTree = new Set<string>([rootId]);
        const buildNode = (id: string): { id: string; children?: { id: string }[] } => {
          const children: { id: string }[] = [];
          adjacency.get(id)?.forEach((next) => {
            if (!visitedTree.has(next)) {
              visitedTree.add(next);
              children.push(buildNode(next));
            }
          });
          return children.length ? { id, children } : { id };
        };
        return buildNode(rootId);
      };

      components.forEach((component, index) => {
        const levels = buildDepthLevels(component);
        let maxDepth = 0;
        levels.forEach((_ids, level) => {
          maxDepth = Math.max(maxDepth, level);
        });

        const roots = slackBusIds.filter((id) => component.includes(id));
        const rootId = roots.length ? roots[0] : component[0];
        const treeData = buildTree(rootId);
        const root = hierarchy(treeData);
        const radius = Math.max(baseRadius + maxDepth * ringSpacing, 320);

        const layout = cluster<{ id: string }>()
          .nodeSize([nodeSize, nodeSize])
          .separation(() => separation);

        layout(root);

        const componentCol = index % gridCols;
        const componentRow = Math.floor(index / gridCols);
        const centerX = 250 + componentCol * componentSpacing;
        const centerY = 200 + componentRow * componentSpacing;

        const descendants = root.descendants();
        const xs = descendants.map((node) => node.x);
        const ys = descendants.map((node) => node.y);
        const minX = Math.min(...xs);
        const maxX = Math.max(...xs);
        const maxY = Math.max(...ys);
        const angleSpan = maxX - minX || 1;
        const radialScale = (radius / (maxY || 1));

        descendants.forEach((node) => {
          const angle = ((node.x - minX) / angleSpan) * Math.PI * 2 - Math.PI / 2;
          const r = node.y * radialScale;
          const x = centerX + r * Math.cos(angle);
          const y = centerY + r * Math.sin(angle);
          busPositions.set(node.data.id, { x: Math.round(x), y: Math.round(y) });
        });
      });
    }

    if (algorithm === "tree") {
      let componentOffsetX = 160;
      const componentSpacingX = Math.max(220, settings.componentSpacing * 0.35);
      const levelSpacingX = settings.levelSpacing;
      const nodeSpacingY = settings.nodeSpacing;

      const buildTree = (rootId: string) => {
        const visitedTree = new Set<string>([rootId]);
        const buildNode = (id: string): { id: string; children?: { id: string }[] } => {
          const children: { id: string }[] = [];
          adjacency.get(id)?.forEach((next) => {
            if (!visitedTree.has(next)) {
              visitedTree.add(next);
              children.push(buildNode(next));
            }
          });
          return children.length ? { id, children } : { id };
        };
        return buildNode(rootId);
      };

      components.forEach((component) => {
        const roots = slackBusIds.filter((id) => component.includes(id));
        const rootId = roots.length ? roots[0] : component[0];
        const treeData = buildTree(rootId);
        const root = hierarchy(treeData);
        const layout = tree<{ id: string }>().nodeSize([nodeSpacingY, levelSpacingX]);
        layout(root);

        const descendants = root.descendants();
        const ys = descendants.map((node) => node.x);
        const minY = Math.min(...ys);
        const maxY = Math.max(...ys);
        const height = maxY - minY || 1;
        const baseY = 160;

        descendants.forEach((node) => {
          const x = componentOffsetX + node.y;
          const y = baseY + (node.x - minY);
          busPositions.set(node.data.id, { x: Math.round(x), y: Math.round(y) });
        });

        componentOffsetX += root.height * levelSpacingX + componentSpacingX + height * 0.25;
      });
    }

    if (algorithm === "hierarchy") {
      let componentOffsetX = 160;
      const componentSpacingX = Math.max(220, settings.componentSpacing * 0.35);
      const levelSpacingX = settings.levelSpacing;
      const nodeSpacingY = settings.nodeSpacing;

      components.forEach((component) => {
        const roots = slackBusIds.filter((id) => component.includes(id));
        const rootId = roots.length ? roots[0] : component[0];
        const treeData = (() => {
          const visitedTree = new Set<string>([rootId]);
          const buildNode = (id: string): { id: string; children?: { id: string }[] } => {
            const children: { id: string }[] = [];
            adjacency.get(id)?.forEach((next) => {
              if (!visitedTree.has(next)) {
                visitedTree.add(next);
                children.push(buildNode(next));
              }
            });
            return children.length ? { id, children } : { id };
          };
          return buildNode(rootId);
        })();

        const root = hierarchy(treeData);
        const layout = tree<{ id: string }>().nodeSize([nodeSpacingY, levelSpacingX]);
        layout(root);

        const descendants = root.descendants();
        const ys = descendants.map((node) => node.x);
        const minY = Math.min(...ys);
        const baseY = 160;

        descendants.forEach((node) => {
          const x = componentOffsetX + node.y;
          const y = baseY + (node.x - minY);
          busPositions.set(node.data.id, { x: Math.round(x), y: Math.round(y) });
        });

        componentOffsetX += root.height * levelSpacingX + componentSpacingX + nodeSpacingY;
      });
    }

    if (algorithm === "stratify") {
      let componentOffsetX = 160;
      const componentSpacingX = Math.max(220, settings.componentSpacing * 0.35);
      const levelSpacingX = settings.levelSpacing;
      const nodeSpacingY = settings.nodeSpacing;

      components.forEach((component) => {
        const nodes = buildTreeEdges(component);
        const stratifier = stratify<{ id: string; parentId: string | null }>()
          .id((d) => d.id)
          .parentId((d) => d.parentId);

        const root = stratifier(nodes);
        const layout = tree<typeof root.data>().nodeSize([nodeSpacingY, levelSpacingX]);
        layout(root);

        const descendants = root.descendants();
        const ys = descendants.map((node) => node.x);
        const minY = Math.min(...ys);
        const baseY = 160;

        descendants.forEach((node) => {
          const x = componentOffsetX + node.y;
          const y = baseY + (node.x - minY);
          busPositions.set(node.data.id, { x: Math.round(x), y: Math.round(y) });
        });

        componentOffsetX += root.height * levelSpacingX + componentSpacingX + nodeSpacingY;
      });
    }

    if (algorithm === "partition") {
      const componentSpacing = settings.componentSpacing;
      const width = componentSpacing;
      const height = componentSpacing * 0.75;

      components.forEach((component, index) => {
        const componentCol = index % Math.max(1, Math.ceil(Math.sqrt(components.length)));
        const componentRow = Math.floor(index / Math.max(1, Math.ceil(Math.sqrt(components.length))));
        const originX = 200 + componentCol * componentSpacing;
        const originY = 160 + componentRow * componentSpacing;

        const nodes = buildTreeEdges(component);
        const stratifier = stratify<{ id: string; parentId: string | null }>()
          .id((d) => d.id)
          .parentId((d) => d.parentId);
        const root = stratifier(nodes).sum(() => 1);

        const layout = partition<{ id: string }>().size([width, height]);
        layout(root);

        root.descendants().forEach((node) => {
          const x = originX + (node.x0 + node.x1) / 2;
          const y = originY + (node.y0 + node.y1) / 2;
          busPositions.set(node.data.id, { x: Math.round(x), y: Math.round(y) });
        });
      });
    }

    if (algorithm === "pack") {
      const componentSpacing = settings.componentSpacing;
      const size = componentSpacing * 0.8;

      components.forEach((component, index) => {
        const componentCol = index % Math.max(1, Math.ceil(Math.sqrt(components.length)));
        const componentRow = Math.floor(index / Math.max(1, Math.ceil(Math.sqrt(components.length))));
        const originX = 200 + componentCol * componentSpacing;
        const originY = 160 + componentRow * componentSpacing;

        const nodes = buildTreeEdges(component);
        const stratifier = stratify<{ id: string; parentId: string | null }>()
          .id((d) => d.id)
          .parentId((d) => d.parentId);
        const root = stratifier(nodes).sum(() => 1);

        const layout = pack<{ id: string }>()
          .size([size, size])
          .padding(settings.nodeSpacing * 0.25);
        layout(root);

        root.descendants().forEach((node) => {
          const x = originX + node.x;
          const y = originY + node.y;
          busPositions.set(node.data.id, { x: Math.round(x), y: Math.round(y) });
        });
      });
    }

    if (algorithm === "treemap") {
      const componentSpacing = settings.componentSpacing;
      const width = componentSpacing;
      const height = componentSpacing * 0.7;

      components.forEach((component, index) => {
        const componentCol = index % Math.max(1, Math.ceil(Math.sqrt(components.length)));
        const componentRow = Math.floor(index / Math.max(1, Math.ceil(Math.sqrt(components.length))));
        const originX = 200 + componentCol * componentSpacing;
        const originY = 160 + componentRow * componentSpacing;

        const nodes = buildTreeEdges(component);
        const stratifier = stratify<{ id: string; parentId: string | null }>()
          .id((d) => d.id)
          .parentId((d) => d.parentId);
        const root = stratifier(nodes).sum(() => 1);

        const layout = treemap<{ id: string }>()
          .size([width, height])
          .paddingInner(6)
          .paddingOuter(6);
        layout(root);

        root.descendants().forEach((node) => {
          const x = originX + (node.x0 + node.x1) / 2;
          const y = originY + (node.y0 + node.y1) / 2;
          busPositions.set(node.data.id, { x: Math.round(x), y: Math.round(y) });
        });
      });
    }

    if (algorithm === "layered") {
      let componentOffsetX = 80;
      const componentSpacingX = Math.max(220, settings.componentSpacing * 0.35);
      const levelSpacingX = settings.levelSpacing;
      const nodeSpacingY = settings.nodeSpacing;
      const baseY = 80;

      components.forEach((component) => {
        const levels = buildDepthLevels(component);
        let maxDepth = 0;
        let maxLevelSize = 0;
        levels.forEach((ids, level) => {
          maxDepth = Math.max(maxDepth, level);
          maxLevelSize = Math.max(maxLevelSize, ids.length);
        });

        Array.from(levels.entries())
          .sort((a, b) => a[0] - b[0])
          .forEach(([level, ids]) => {
            ids.sort((a, b) => parseBusSortKey(a) - parseBusSortKey(b));
            const yStart = baseY + (maxLevelSize - ids.length) * nodeSpacingY * 0.5;
            ids.forEach((id, index) => {
              const x = componentOffsetX + level * levelSpacingX;
              const y = yStart + index * nodeSpacingY;
              busPositions.set(id, { x: Math.round(x), y: Math.round(y) });
            });
          });

        componentOffsetX += (maxDepth + 1) * levelSpacingX + componentSpacingX;
      });
    }

    if (algorithm === "grid") {
      const componentSpacing = settings.componentSpacing;
      const nodeSpacing = settings.gridSpacing;
      const gridCols = Math.max(1, Math.ceil(Math.sqrt(components.length)));
      components.forEach((component, index) => {
        const componentCol = index % gridCols;
        const componentRow = Math.floor(index / gridCols);
        const originX = 200 + componentCol * componentSpacing;
        const originY = 160 + componentRow * componentSpacing;
        const cols = Math.max(1, Math.ceil(Math.sqrt(component.length)));
        component
          .slice()
          .sort((a, b) => parseBusSortKey(a) - parseBusSortKey(b))
          .forEach((id, idx) => {
            const row = Math.floor(idx / cols);
            const col = idx % cols;
            const x = originX + col * nodeSpacing;
            const y = originY + row * nodeSpacing;
            busPositions.set(id, { x: Math.round(x), y: Math.round(y) });
          });
      });
    }

    if (algorithm === "circular") {
      const componentCount = components.length;
      const gridCols = Math.max(1, Math.ceil(Math.sqrt(componentCount)));
      const componentSpacing = settings.componentSpacing;

      components.forEach((component, index) => {
        const componentCol = index % gridCols;
        const componentRow = Math.floor(index / gridCols);
        const centerX = 250 + componentCol * componentSpacing;
        const centerY = 200 + componentRow * componentSpacing;
        const sorted = component.slice().sort((a, b) => parseBusSortKey(a) - parseBusSortKey(b));
        const radius = Math.max(settings.circularRadius, Math.sqrt(sorted.length) * (settings.nodeSpacing * 0.6));
        const angleStep = (Math.PI * 2) / Math.max(sorted.length, 1);
        sorted.forEach((id, idx) => {
          const angle = idx * angleStep;
          const x = centerX + radius * Math.cos(angle);
          const y = centerY + radius * Math.sin(angle);
          busPositions.set(id, { x: Math.round(x), y: Math.round(y) });
        });
      });
    }

    if (algorithm === "concentric") {
      const componentCount = components.length;
      const gridCols = Math.max(1, Math.ceil(Math.sqrt(componentCount)));
      const componentSpacing = settings.componentSpacing;
      const ringSpacing = settings.ringSpacing;

      components.forEach((component, index) => {
        const componentCol = index % gridCols;
        const componentRow = Math.floor(index / gridCols);
        const centerX = 250 + componentCol * componentSpacing;
        const centerY = 200 + componentRow * componentSpacing;

        const degrees = new Map<string, number>();
        component.forEach((id) => degrees.set(id, adjacency.get(id)?.size ?? 0));
        const maxDegree = Math.max(1, ...Array.from(degrees.values()));
        const ringCount = Math.max(3, Math.min(8, Math.ceil(Math.sqrt(component.length) / 2)));

        const rings = new Map<number, string[]>();
        component.forEach((id) => {
          const degree = degrees.get(id) ?? 0;
          const normalized = degree / maxDegree;
          const ring = Math.min(ringCount - 1, Math.max(0, Math.floor((1 - normalized) * (ringCount - 1))));
          const bucket = rings.get(ring) ?? [];
          bucket.push(id);
          rings.set(ring, bucket);
        });

        Array.from(rings.entries())
          .sort((a, b) => a[0] - b[0])
          .forEach(([ringIndex, ids]) => {
            const radius = Math.max(80, ringIndex * ringSpacing + 60);
            const sorted = ids.slice().sort((a, b) => parseBusSortKey(a) - parseBusSortKey(b));
            const angleStep = (Math.PI * 2) / Math.max(sorted.length, 1);
            sorted.forEach((id, idx) => {
              const angle = idx * angleStep;
              const x = centerX + radius * Math.cos(angle);
              const y = centerY + radius * Math.sin(angle);
              busPositions.set(id, { x: Math.round(x), y: Math.round(y) });
            });
          });
      });
    }

    if (algorithm === "spiral") {
      const componentCount = components.length;
      const gridCols = Math.max(1, Math.ceil(Math.sqrt(componentCount)));
      const componentSpacing = settings.componentSpacing;
      const spiralStep = settings.spiralStep;
      const angleStep = 0.42;

      components.forEach((component, index) => {
        const componentCol = index % gridCols;
        const componentRow = Math.floor(index / gridCols);
        const centerX = 250 + componentCol * componentSpacing;
        const centerY = 200 + componentRow * componentSpacing;
        const sorted = component.slice().sort((a, b) => parseBusSortKey(a) - parseBusSortKey(b));
        sorted.forEach((id, idx) => {
          const angle = idx * angleStep;
          const radius = 40 + idx * spiralStep * 0.4;
          const x = centerX + radius * Math.cos(angle);
          const y = centerY + radius * Math.sin(angle);
          busPositions.set(id, { x: Math.round(x), y: Math.round(y) });
        });
      });
    }

    if (algorithm === "force") {
      const componentCount = components.length;
      const gridCols = Math.max(1, Math.ceil(Math.sqrt(componentCount)));
      const componentSpacing = settings.componentSpacing;

      components.forEach((component, index) => {
        const componentCol = index % gridCols;
        const componentRow = Math.floor(index / gridCols);
        const centerX = 250 + componentCol * componentSpacing;
        const centerY = 200 + componentRow * componentSpacing;

        const nodes = component.map((id) => ({ id }));
        const links: Array<{ source: string; target: string }> = [];
        component.forEach((id) => {
          adjacency.get(id)?.forEach((neighbor) => {
            if (id < neighbor) {
              links.push({ source: id, target: neighbor });
            }
          });
        });

        const sim = forceSimulation(nodes)
          .force("charge", forceManyBody().strength(settings.forceCharge))
          .force("link", forceLink(links).id((d) => (d as { id: string }).id).distance(settings.forceLinkDistance).strength(0.5))
          .force("center", forceCenter(centerX, centerY))
          .force("collide", forceCollide(settings.forceCollide))
          .stop();

        const iterations = Math.min(240, Math.max(40, settings.forceIterations));
        for (let i = 0; i < iterations; i += 1) {
          sim.tick();
        }

        nodes.forEach((node) => {
          busPositions.set(node.id, {
            x: Math.round(node.x ?? centerX),
            y: Math.round(node.y ?? centerY),
          });
        });
      });
    }

    const typeOffsets: Record<ElementType, { x: number; y: number }> = {
      external_source: { x: -90, y: 0 },
      generator: { x: 90, y: -40 },
      load: { x: 90, y: 40 },
      battery: { x: -90, y: -40 },
      capacitor: { x: -90, y: 40 },
      switch: { x: 0, y: -80 },
      line: { x: 0, y: 0 },
      cable: { x: 0, y: 0 },
      transformer: { x: 0, y: 0 },
      bus: { x: 0, y: 0 },
    };

    const resolveConnectedBus = (el: NetworkElement) => {
      const connectedBusId = (el as { connectedBusId?: string }).connectedBusId;
      if (connectedBusId && busPositions.has(connectedBusId)) return connectedBusId;
      const candidates = busIdsFromConnections(el.id);
      return candidates.find((id) => busPositions.has(id));
    };

    const nextElements = activeElements.map((el) => {
      if (el.type === "bus") {
        const pos = busPositions.get(el.id);
        return pos ? { ...el, x: pos.x, y: pos.y } : el;
      }

      if (el.type === "line" || el.type === "cable" || el.type === "transformer") {
        const [a, b] = resolveBusPair(el);
        const posA = a ? busPositions.get(a) : undefined;
        const posB = b ? busPositions.get(b) : undefined;
        if (posA && posB) {
          return {
            ...el,
            x: Math.round((posA.x + posB.x) / 2),
            y: Math.round((posA.y + posB.y) / 2),
          };
        }
        return el;
      }

      const busId = resolveConnectedBus(el);
      if (busId) {
        const pos = busPositions.get(busId);
        if (pos) {
          const offset = typeOffsets[el.type];
          return {
            ...el,
            x: Math.round(pos.x + offset.x),
            y: Math.round(pos.y + offset.y),
          };
        }
      }
      return el;
    });

    setElements(nextElements);
    setHasUnsavedChanges(true);
  }, [elements, connections]);

  const deleteElement = useCallback((id: string) => {
    setElements((prev) => prev.filter((el) => el.id !== id));
    setConnections((prev) =>
      prev.filter((c) => c.fromElementId !== id && c.toElementId !== id)
    );
    if (selectedElementId === id) {
      setSelectedElementId(null);
    }
    setSelectedElementIds((prev) => prev.filter((selected) => selected !== id));
    setHasUnsavedChanges(true);
  }, [selectedElementId]);

  // Create a Line element when connecting nodes (React Flow)
  const createLine = useCallback((fromId: string, toId: string) => {
    // Check if a line already exists between these elements
    const existingLine = elements.find(
      (el) =>
        (el.type === 'line' || el.type === 'cable') &&
        (((el as { fromElementId?: string }).fromElementId === fromId &&
          (el as { toElementId?: string }).toElementId === toId) ||
          ((el as { fromElementId?: string }).fromElementId === toId &&
            (el as { toElementId?: string }).toElementId === fromId))
    );

    if (existingLine) return; // Don't create duplicate lines

    const lineCount = (elementCounter['line'] || 0) + 1;
    setElementCounter((prev) => ({ ...prev, line: lineCount }));

    const newLine: NetworkElement = {
      id: crypto.randomUUID(),
      name: `Line ${lineCount}`,
      type: 'line',
      x: 0,
      y: 0,
      rotation: 0,
      enabled: true,
      installation: "overhead",
      fromElementId: fromId,
      toElementId: toId,
      lengthKm: 1,
      resistanceOhmPerKm: 0.1,
      reactanceOhmPerKm: 0.4,
      susceptanceSPerKm: 0,
    } as NetworkElement;

    setElements((prev) => [...prev, newLine]);
    setHasUnsavedChanges(true);
    setSelectedElementId(newLine.id);
    setRightTab('properties');
    toast({
      title: 'Line created',
      description: `Connected ${elements.find(e => e.id === fromId)?.name || 'element'} to ${elements.find(e => e.id === toId)?.name || 'element'}`,
    });
  }, [elements, elementCounter, toast]);

  const openUndergroundView = useCallback((lineId: string) => {
    const line = elements.find((el) => el.id === lineId && el.type === "line") as (NetworkElement & { installation?: string }) | undefined;
    if (!line || line.installation !== "underground") {
      return;
    }
    setUndergroundLineId(lineId);
    setSelectedElementId(null);
    setSelectedElementIds([]);
    setRightTab("properties");
  }, [elements]);

  useEffect(() => {
    if (!undergroundLineId) return;
    const line = elements.find((el) => el.id === undergroundLineId && el.type === "line") as (NetworkElement & { installation?: string }) | undefined;
    if (!line || line.installation !== "underground") {
      setUndergroundLineId(null);
    }
  }, [elements, undergroundLineId]);

  const handleDragStart = (event: any) => {
    const { active } = event;
    if (active.data.current?.source === "palette") {
      setActiveDragType(active.data.current.type);
    }
  };

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;
    setActiveDragType(null);

    if (over?.id === "canvas-drop-zone" && active.data.current?.source === "palette") {
      const type = active.data.current.type as ElementType;

      // Get the canvas container's bounding rect
      const canvasContainer = canvasContainerRef.current;
      if (!canvasContainer) {
        // Fallback: add at default position
        addElement(type, 200, 200);
        return;
      }

      const rect = canvasContainer.getBoundingClientRect();
      const reactFlowBounds = canvasContainer.getBoundingClientRect();

      // Since React Flow Canvas uses screenToFlowPosition internally in onDrop,
      // here we just use a default position or the drop position if we can calculate it easily.
      // But actually, the ReactFlowCanvas handles the drop event itself for native drag-drop.
      // This handler is for the dnd-kit drag-drop.

      // Calculate basic position relative to canvas container
      const delta = event.delta || { x: 0, y: 0 };
      const activeRect = active.rect.current.translated;

      let x = 200;
      let y = 200;

      if (activeRect) {
        // Create a simple approximation for now as view state (zoom/pan) is managed inside ReactFlow
        // Ideally we should use ReactFlow instance to project, but for now we'll rely on the native drag handler in ReactFlowCanvas
        // which is more accurate.
        // If we are using dnd-kit, we might need access to RF instance.

        // For now, let's just make sure we add the element generally where the mouse is
        // but since we enabled native drag in ElementPalette, ReactFlowCanvas.onDrop will handle it.
        // So we might not need to do anything here if native drag was used.
        // However, dnd-kit is still wrapping it.
      }

      // If native drag didn't fire (because of dnd-kit overlay), we might need this.
      // But we added `draggable` and `onDragStart` to the palette items, so browser native drag should work.
    }
  };

  const selectedElement = elements.find((el) => el.id === selectedElementId) || null;
  const currentModel = models.find((m) => m.id === currentModelId) || null;

  const handleElementSelect = useCallback((id: string | null) => {
    if (id) {
      setSelectedElementIds([id]);
    } else {
      setSelectedElementIds([]);
    }
    setSelectedElementId(id);
    if (id) {
      setRightTab("properties");
      if (document.cookie.includes("react-resizable-panels:collapsed=true")) {
        // Panel state is user-controlled.
      }
    }
  }, []);

  const handleElementSelection = useCallback((ids: string[], primaryId: string | null) => {
    setSelectedElementIds(ids);
    setSelectedElementId(primaryId);
    if (primaryId) {
      setRightTab("properties");
    }
  }, []);

  return (
    <DndContext onDragStart={handleDragStart} onDragEnd={handleDragEnd}>
      <div className="h-screen flex flex-col bg-background">
        <header className="h-14 border-b flex items-center justify-between px-4 bg-card shrink-0">
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <div className="p-1.5 bg-primary/10 rounded-md">
                <Zap className="h-5 w-5 text-primary" />
              </div>
              <div>
                <h1 className="font-semibold text-sm">Power Flow Analyzer</h1>
                <p className="text-[10px] text-muted-foreground">Grid Planning Application Suite</p>
              </div>
            </div>
          </div>

          <div className="flex items-center gap-3">
            {currentModel && (
              <div className="flex items-center gap-2">
                <Badge variant="outline" className="gap-1.5">
                  <Network className="h-3 w-3" />
                  {currentModel.name}
                </Badge>
                {hasUnsavedChanges && (
                  <Badge variant="secondary" className="text-xs">Unsaved</Badge>
                )}
              </div>
            )}
            <div className="w-px h-6 bg-border" />
            <ThemeToggle />
          </div>
        </header>

        <NetworkToolbar
          onNew={handleNewModel}
          onOpen={() => { /* Open modal logic would go here, currently using Right Panel */ setRightTab("models"); }}
          onSave={() => saveModelMutation.mutate()}
          onDelete={() => currentModelId && deleteModelMutation.mutate(currentModelId)}
          onRunAnalysis={() => runAnalysisMutation.mutate()}
          onLoadAllocation={() => toast({ title: "Feature coming soon", description: "Load Allocation analysis is under development." })}
          onShortCircuit={() => toast({ title: "Feature coming soon", description: "Short Circuit Duty analysis is under development." })}
          onHostingCapacity={() => toast({ title: "Feature coming soon", description: "Hosting Capacity analysis is under development." })}
          onThermalModeling={() => toast({ title: "Feature coming soon", description: "Thermal Modeling analysis is under development." })}
          onTimeSeries={() => toast({ title: "Feature coming soon", description: "Time Series analysis is under development." })}
          onResetWorkspace={handleResetWorkspace}
          onResetLayout={() => { /* React Flow handles this via fitView prop usually */ }}
          onAutoLayout={(algorithm) => {
            setLastLayout(algorithm);
            autoLayout(algorithm, layoutSettings);
          }}
          onApplyLayout={() => autoLayout(lastLayout, layoutSettings)}
          onZoomIn={() => { /* Passed down to canvas via context or ref in future */ }}
          onZoomOut={() => { /* Passed down to canvas via context or ref in future */ }}
          onFitView={() => { /* Passed down to canvas via context or ref in future */ }}
          onOpenSettings={() => setSettingsOpen(true)}
          hasUnsavedChanges={hasUnsavedChanges}
          isRunningAnalysis={isRunningAnalysis}
          layoutSettings={layoutSettings}
          onUpdateLayoutSettings={handleUpdateLayoutSettings}
          onResetLayoutSettings={handleResetLayoutSettings}
        />

        <div className="flex-1 overflow-hidden">
          <ResizablePanelGroup direction="horizontal">
            {/* Left Sidebar: Network */}
            <ResizablePanel
              defaultSize={12}
              minSize={0}
              maxSize={20}
              collapsible={true}
              collapsedSize={0}
              onCollapse={() => setIsNetworkCollapsed(true)}
              onExpand={() => setIsNetworkCollapsed(false)}
              className={isNetworkCollapsed ? "min-w-[0px]" : ""}
            >
              <NetworkSidebar
                key={currentModelId ?? "no-model"}
                elements={visibleElements}
                selectedElementId={selectedElementId}
                onSelectElement={handleElementSelect}
              />
            </ResizablePanel>

            <ResizableHandle withHandle />

            {/* Left Sidebar: Element Explorer */}
            <ResizablePanel
              defaultSize={13}
              minSize={0}
              maxSize={20}
              collapsible={true}
              collapsedSize={0}
              onCollapse={() => setIsElementsCollapsed(true)}
              onExpand={() => setIsElementsCollapsed(false)}
              className={isElementsCollapsed ? "min-w-[0px]" : ""}
            >
              <ElementSidebar
                onAddElement={(type) => {
                  if (type === "ductbank") {
                    toast({
                      title: "Underground only",
                      description: "Open an underground line to add ductbanks.",
                    });
                    return;
                  }
                  const x = 100 + (elements.length % 5) * 120;
                  const y = 100 + Math.floor(elements.length / 5) * 100;
                  addElement(type, x, y);
                }}
                viewMode={settings.paletteView}
              />
            </ResizablePanel>

            <ResizableHandle withHandle />

            {/* Main Canvas */}
            <ResizablePanel defaultSize={50}>
              <div className="h-full flex flex-col">
                <ResizablePanelGroup direction="vertical">
                  <ResizablePanel defaultSize={70} minSize={30}>
                    <div ref={canvasContainerRef} className="relative h-full min-h-0">
                      <div className="absolute left-4 top-4 z-[1100] flex overflow-hidden rounded-full border border-border bg-card/90 text-xs shadow-lg backdrop-blur">
                        <button
                          className={`px-3 py-1.5 font-semibold transition ${
                            canvasView === "oneLine"
                              ? "bg-primary text-primary-foreground"
                              : "text-muted-foreground hover:text-foreground"
                          }`}
                          onClick={() => setCanvasView("oneLine")}
                          type="button"
                        >
                          One-Line
                        </button>
                        <button
                          className={`px-3 py-1.5 font-semibold transition ${
                            canvasView === "spatial"
                              ? "bg-primary text-primary-foreground"
                              : "text-muted-foreground hover:text-foreground"
                          }`}
                          onClick={() => setCanvasView("spatial")}
                          type="button"
                        >
                          Spatial
                        </button>
                        <button
                          className={`px-3 py-1.5 font-semibold transition ${
                            canvasView === "threeD"
                              ? "bg-primary text-primary-foreground"
                              : "text-muted-foreground hover:text-foreground"
                          }`}
                          onClick={() => setCanvasView("threeD")}
                          type="button"
                        >
                          3D
                        </button>
                        <button
                          className={`px-3 py-1.5 font-semibold transition ${
                            canvasView === "deckMap"
                              ? "bg-primary text-primary-foreground"
                              : "text-muted-foreground hover:text-foreground"
                          }`}
                          onClick={() => setCanvasView("deckMap")}
                          type="button"
                        >
                          3D Map
                        </button>
                      </div>

                      {undergroundLine && undergroundLineId ? (
                        <div className="absolute inset-0 flex flex-col">
                          <div className="absolute left-4 top-4 z-[1100] flex items-center gap-2 rounded-full border border-border bg-card/95 px-3 py-1.5 text-xs shadow-lg backdrop-blur">
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => setUndergroundLineId(null)}
                              data-testid="button-exit-underground"
                            >
                              Return to Network
                            </Button>
                            <div className="h-4 w-px bg-border" />
                            <span className="font-semibold text-muted-foreground">
                              Underground: {undergroundLine.name}
                            </span>
                            <div className="h-4 w-px bg-border" />
                            <Button
                              size="sm"
                              onClick={() => addDuctbankToLine(undergroundLine.id, 200, 200)}
                              data-testid="button-add-ductbank"
                            >
                              Add Ductbank
                            </Button>
                          </div>

                          <div className="flex-1">
                            <ReactFlowCanvas
                              elements={undergroundElements}
                              connections={[]}
                              selectedElementId={selectedElementId}
                              selectedElementIds={selectedElementIds}
                              onSelectElement={handleElementSelect}
                              onSelectElements={handleElementSelection}
                              onUpdateElement={updateElement}
                              onUpdateElementsPositions={updateElementsPositions}
                              onDeleteElement={deleteElement}
                              onCreateLine={() => {}}
                              onDropElement={(type, x, y) => {
                                if (type === "ductbank") {
                                  addDuctbankToLine(undergroundLine.id, x, y);
                                }
                              }}
                              showLabels={showLabels}
                            />
                          </div>
                        </div>
                      ) : canvasView === "oneLine" ? (
                        <>
                          <ReactFlowCanvas
                            elements={visibleElements}
                            connections={connections}
                            selectedElementId={selectedElementId}
                            selectedElementIds={selectedElementIds}
                            onSelectElement={handleElementSelect}
                            onSelectElements={handleElementSelection}
                            onUpdateElement={updateElement}
                            onUpdateElementsPositions={updateElementsPositions}
                            onDeleteElement={deleteElement}
                            onCreateLine={createLine}
                            onDropElement={addElement}
                            isFlowing={analysisResult?.converged ?? false}
                            onOpenUndergroundLine={openUndergroundView}
                            showLabels={showLabels}
                          />
                          <Legend />
                        </>
                      ) : canvasView === "threeD" ? (
                        <FlowVisualization
                          embedded
                          elements={elements}
                          connections={connections}
                          analysisResult={analysisResult}
                        />
                      ) : canvasView === "deckMap" ? (
                        <DeckGLMapView elements={visibleElements} />
                      ) : (
                        <SpatialMapView elements={visibleElements} />
                      )}
                    </div>
                  </ResizablePanel>

                  <ResizableHandle withHandle />

                  <ResizablePanel
                    ref={resultsPanelRef}
                    defaultSize={30}
                    minSize={10}
                    collapsible
                    collapsedSize={6}
                    onCollapse={() => setIsResultsCollapsed(true)}
                    onExpand={() => setIsResultsCollapsed(false)}
                  >
                    <ResultsDock
                      result={analysisResult}
                      thermalResult={cableTempResult}
                      collapsed={isResultsCollapsed}
                      onToggleCollapse={() => {
                        if (isResultsCollapsed) {
                          resultsPanelRef.current?.expand();
                        } else {
                          resultsPanelRef.current?.collapse();
                        }
                      }}
                    />
                  </ResizablePanel>
                </ResizablePanelGroup>
              </div>
            </ResizablePanel>

            <ResizableHandle withHandle />

            {/* Right Sidebar: Properties & Analysis */}
            <ResizablePanel
              defaultSize={32}
              minSize={0}
              maxSize={40}
              collapsible={true}
              collapsedSize={0}
              onCollapse={() => setIsRightCollapsed(true)}
              onExpand={() => setIsRightCollapsed(false)}
            >
              <div className="h-full flex flex-col border-l bg-card">
                <Tabs value={rightTab} onValueChange={setRightTab} className="flex flex-col h-full">
                  <div className="border-b px-2 pt-2 bg-muted/30">
                    <TabsList className="w-full h-9 bg-muted/50">
                      <TabsTrigger value="properties" className="flex-1 text-xs gap-1.5" data-testid="tab-properties">
                        <Settings2 className="h-3.5 w-3.5" />
                        Props
                      </TabsTrigger>
                      <TabsTrigger value="models" className="flex-1 text-xs gap-1.5" data-testid="tab-models">
                        <FolderOpen className="h-3.5 w-3.5" />
                        Files
                      </TabsTrigger>
                      <TabsTrigger value="equipment" className="flex-1 text-xs gap-1.5" data-testid="tab-equipment">
                        <Database className="h-3.5 w-3.5" />
                        DB
                      </TabsTrigger>
                      <TabsTrigger value="analysis" className="flex-1 text-xs gap-1.5" data-testid="tab-analysis">
                        <Activity className="h-3.5 w-3.5" />
                        Run
                      </TabsTrigger>
                    </TabsList>
                  </div>
                  <TabsContent value="properties" className="flex-1 m-0 overflow-hidden bg-card">
                    <PropertiesPanel
                      element={selectedElement}
                      onUpdate={updateElement}
                      allElements={elements}
                      onThermalResult={(payload) => setCableTempResult(payload)}
                    />
                  </TabsContent>
                  <TabsContent value="models" className="flex-1 m-0 overflow-hidden bg-card">
                    <NetworkModelManager
                      models={models}
                      currentModelId={currentModelId}
                      onNewModel={(name, description, baseFrequencyHz, baseVoltageKV) =>
                        createModelMutation.mutate({ name, description, baseFrequencyHz, baseVoltageKV })
                      }
                      onLoadModel={loadModel}
                      onSaveModel={() => saveModelMutation.mutate()}
                      onDeleteModel={(id) => deleteModelMutation.mutate(id)}
                      onUpdateModelInfo={() => { }}
                      hasUnsavedChanges={hasUnsavedChanges}
                      newDialogOpen={newModelDialogOpen}
                      onNewDialogOpenChange={setNewModelDialogOpen}
                      onLoadSample={(sampleType) => loadSampleMutation.mutate(sampleType)}
                      isLoadingSample={loadSampleMutation.isPending}
                      testNetworks={testNetworks}
                      onLoadTestNetwork={(name) => loadTestNetworkMutation.mutate(name)}
                      isLoadingTestNetwork={loadTestNetworkMutation.isPending}
                    />
                  </TabsContent>
                  <TabsContent value="equipment" className="flex-1 m-0 overflow-hidden bg-card">
                    <EquipmentDatabase
                      templates={templates}
                      onAddTemplate={(template) => addTemplateMutation.mutate(template)}
                      onSelectTemplate={(template) => {
                        toast({
                          title: "Template selected",
                          description: `"${template.name}" ready to use`,
                        });
                      }}
                    />
                  </TabsContent>
                  <TabsContent value="analysis" className="flex-1 m-0 overflow-hidden bg-card">
                    <LoadFlowAnalysis
                      network={currentModel ? { ...currentModel, elements, connections } : null}
                      onRunAnalysis={() => runAnalysisMutation.mutate()}
                      result={analysisResult}
                      isRunning={isRunningAnalysis}
                    />
                  </TabsContent>
                </Tabs>
              </div>
            </ResizablePanel>
          </ResizablePanelGroup>
        </div>
        <StatusBar />
      </div>

      <SettingsDialog
        open={settingsOpen}
        onOpenChange={setSettingsOpen}
        settings={settings}
        onSettingsChange={setSettings}
      />

      <DragOverlay>
        {activeDragType ? (
          <Card className="flex items-center gap-3 p-2.5 border border-primary bg-card shadow-lg">
            <div className="text-muted-foreground">
              <ElementIcon type={activeDragType} size={20} />
            </div>
            <span className="text-sm font-medium text-foreground">
              {elementLabels[activeDragType]}
            </span>
          </Card>
        ) : null}
      </DragOverlay>
    </DndContext>
  );
}
