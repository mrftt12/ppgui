import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import './App.css';
import {
  Zap, Network, Play, FileText, Settings, Plus, Minus, Move, Trash2, RefreshCw,
  ChevronDown, ChevronRight, ChevronLeft, ChevronUp, Circle, Square, Triangle, Box, Activity,
  Database, Cpu, Grid, Download, Upload, Save, FolderOpen, AlertTriangle,
  CheckCircle, XCircle, BarChart2, Table, Info, X, Power, Home, Loader,
  Battery, Radio, Gauge, Layers, GitBranch, Disc, PanelLeftClose, PanelRightClose,
  History, RotateCcw, Layout, Map as MapIcon, List, Maximize2
} from 'lucide-react';
import { MapContainer, TileLayer, CircleMarker, Popup, LayersControl, LayerGroup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

// Extracted modules
import api from './api';
import { colorForLoading, colorForVoltage } from './utils/colorUtils';
import Toast from './components/common/Toast';
import { Modal, HistoryModal } from './components/common/Modal';
import EsriFeatureLayers from './components/EsriFeatureLayers';
import Legend from './components/diagram/Legend';
import ResultsPanel from './components/panels/ResultsPanel';
import MapToolsPalette from './components/diagram/MapToolsPalette';
import LayersPanel from './components/diagram/LayersPanel';
import { applyAutoLayout } from './utils/AutoLayout';
import { SettingsProvider, useSettings } from './contexts/SettingsContext';
import { useUndoRedo, createNodeMoveAction } from './hooks/useUndoRedo';
import SettingsModal from './components/modals/SettingsModal';

const parseTsv = (text) => {
  const lines = text.replace(/\r/g, '').split('\n').filter(Boolean);
  if (lines.length === 0) return [];
  const headers = lines[0].split('\t');
  return lines.slice(1).map(line => {
    const values = line.split('\t');
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = values[idx] ?? '';
    });
    return row;
  });
};

const buildSceTree = (rows, availableModels = new Set()) => {
  const root = new Map();
  const normalize = (value, fallback) => {
    const v = value === null || value === undefined ? '' : String(value).trim();
    return v ? v : fallback;
  };
  const ensureNode = (map, name) => {
    if (!map.has(name)) {
      map.set(name, { name, children: new Map() });
    }
    return map.get(name);
  };

  rows.forEach(row => {
    const region = normalize(row.REGION_NAME, 'Unknown Region');
    const system = normalize(row.SYSTEM_NAME, 'Unknown System');
    const alias = normalize(row.SBTRN_SUB_ALIASNAME, 'Unknown Substation Alias');
    const sub = normalize(row.SUB_NAME, 'Unknown Substation');
    const circuitKv = normalize(row.CIRCUIT_KV, 'Unknown KV');
    const circuit = `${normalize(row.CIRCUIT_NAME, 'Unknown Circuit')}_${circuitKv}KV`;

    const regionNode = ensureNode(root, region);
    const systemNode = ensureNode(regionNode.children, system);
    const aliasNode = ensureNode(systemNode.children, alias);
    const subNode = ensureNode(aliasNode.children, sub);
    const circuitNode = ensureNode(subNode.children, circuit);

    // Check match
    // The availableModels set contains names like "SUNFLOWER_12KV"
    if (availableModels.has(circuit)) {
      circuitNode.hasModel = true;
    }
  });

  const mapToNodes = (map) => {
    return Array.from(map.values())
      .sort((a, b) => a.name.localeCompare(b.name))
      .map(node => ({
        name: node.name,
        hasModel: node.hasModel,
        children: mapToNodes(node.children)
      }));
  };

  return mapToNodes(root);
};

// Main App Component
function App() {
  const [networks, setNetworks] = useState([]);
  const [currentNetwork, setCurrentNetwork] = useState(null);
  const [sceRows, setSceRows] = useState([]);
  const [sceTree, setSceTree] = useState([]);
  const [sceTreeLoading, setSceTreeLoading] = useState(true);
  const [sceTreeError, setSceTreeError] = useState(null);
  const [sceExpanded, setSceExpanded] = useState(new Set());
  const [networkData, setNetworkData] = useState(null);
  const [statistics, setStatistics] = useState(null);
  const [topology, setTopology] = useState(null);
  const [elements, setElements] = useState({
    // Sources
    extGrids: [],
    // Equipment
    buses: [], lines: [], switches: [], transformers: [], trafo3w: [],
    // Generators
    generators: [], staticGens: [], motors: [],
    // Loads
    loads: [], asymmetricLoads: [],
    // DC
    dclines: [], storages: [],
    // Other
    shunts: [], asymmetricSgens: [], svcs: [], tcscs: [], sscs: [], wards: [], xwards: [], impedances: [], measurements: []
  });
  const [elementCounts, setElementCounts] = useState({});
  const [analysisResults, setAnalysisResults] = useState(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [selectedElement, setSelectedElement] = useState(null);
  const [selectedElements, setSelectedElements] = useState(new Set()); // Multi-select: Set of "type_index" keys
  const [clipboard, setClipboard] = useState([]); // For copy/paste
  const [expandedGroups, setExpandedGroups] = useState({
    sources: true, equipment: true, generators: false, loads: true, dc: false, other: false
  });
  const [expandedCategories, setExpandedCategories] = useState({});
  const [activeResultTab, setActiveResultTab] = useState('bus');
  const [modal, setModal] = useState(null);
  const [toast, setToast] = useState(null);

  const [samples, setSamples] = useState([]);
  const [selectedLayoutAlgo, setSelectedLayoutAlgo] = useState('radial');
  const [lineTypes, setLineTypes] = useState([]);
  const [trafoTypes, setTrafoTypes] = useState([]);
  const [apiStatus, setApiStatus] = useState('connecting');
  const [nodePositions, setNodePositions] = useState({});
  const [draggingNode, setDraggingNode] = useState(null);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const [dragStartPosition, setDragStartPosition] = useState(null); // For undo support
  const [svgRef, setSvgRef] = useState(null);
  const [viewTransform, setViewTransform] = useState({ scale: 1, x: 0, y: 0 });
  const [isPanning, setIsPanning] = useState(false);
  const [panStart, setPanStart] = useState(null);
  const [diagramViewMode, setDiagramViewMode] = useState('singleLine'); // 'singleLine' | 'spatial'
  const [availableModels, setAvailableModels] = useState(new Set()); // Set of available model IDs

  // Advanced Interaction State
  const [draggingEdgeHandle, setDraggingEdgeHandle] = useState(null); // { edgeId, type: 'source'|'target'|'control', startX, startY, currentX, currentY }
  const [edgeControlPoints, setEdgeControlPoints] = useState({}); // { edgeId: { x, y } }
  const [hoveredEdge, setHoveredEdge] = useState(null);
  const [marquee, setMarquee] = useState(null); // { startX, startY, currentX, currentY } for drag-select
  const [connectionMode, setConnectionMode] = useState(false); // Toggle for connection drawing mode
  const [drawingConnection, setDrawingConnection] = useState(null); // { fromBusId, fromPos: {x,y}, currentPos: {x,y} }

  // Layers & Visibility
  const [showLayersPanel, setShowLayersPanel] = useState(false);
  const [layerVisibility, setLayerVisibility] = useState({
    buses: true,
    lines: true,
    loads: true,
    generators: true,
    labels: true
  });
  // Map layers
  const [showTransmissionLayer, setShowTransmissionLayer] = useState(false);
  const [showSubstationLayer, setShowSubstationLayer] = useState(false);
  const [tcgmLayers, setTcgmLayers] = useState([]);
  const [tcgmLoading, setTcgmLoading] = useState(true);
  const [tcgmError, setTcgmError] = useState(null);
  // Collapsible panel states - default to open/docked
  const [panelStates, setPanelStates] = useState({
    left: true,      // Element tree panel
    right: true,     // Properties panel
    bottom: true,    // Results panel
    sidebar: true,   // Networks sidebar
    networksPane: true, // Collapsible networks list inside sidebar
  });
  const [resultsHeight, setResultsHeight] = useState(280);
  const [isResizingResults, setIsResizingResults] = useState(false);
  const [resizeStart, setResizeStart] = useState(null);
  const [tableStates, setTableStates] = useState({}); // per-tab sort/filter
  const [tsChartSource, setTsChartSource] = useState('res_bus');
  const [tsChartField, setTsChartField] = useState(null);
  const [tsChartType, setTsChartType] = useState('line'); // line | scatter | fill
  const [measurementElementType, setMeasurementElementType] = useState('line');
  const chartRef = useRef(null);
  const [chartSize, setChartSize] = useState({ w: 720, h: 320 });

  // Scenario/Version Management
  const [networkHistory, setNetworkHistory] = useState([]);
  const [selectedVersionId, setSelectedVersionId] = useState(null);

  // Settings and Undo/Redo
  const { settings, updateSettings } = useSettings();
  const { pushAction, undo, redo, canUndo, canRedo } = useUndoRedo();

  useEffect(() => {
    let active = true;
    setSceTreeLoading(true);
    fetch(`${process.env.PUBLIC_URL}/sce.csv`)
      .then(res => res.text())
      .then(text => {
        if (!active) return;
        const rows = parseTsv(text);
        // We defer building the tree until we have availableModels, 
        // but we can initially build it without availability info if needed.
        // Better to wait for both? Or update when either changes.
        // Let's store rows and rebuild tree when rows or availableModels change.
        setSceRows(rows);
      })
      .catch(err => {
        if (!active) return;
        setSceTreeError(err?.message || 'Failed to load sce.csv');
      })
      .finally(() => {
        if (active) setSceTreeLoading(false);
      });
    return () => {
      active = false;
    };
  }, []);

  // Rebuild tree when rows or available models change
  useEffect(() => {
    if (sceRows.length === 0) return;
    const tree = buildSceTree(sceRows, availableModels);
    setSceTree(tree);
    if (sceExpanded.size === 0) {
      setSceExpanded(new Set(tree.map(node => node.name)));
    }
  }, [sceRows, availableModels]);

  const handleLoadModel = async (modelName) => {
    try {
      showToast(`Loading model ${modelName}...`);
      const result = await api.loadPandapowerModel(modelName);

      setNetworks(prev => [...prev, {
        network_id: result.network_id,
        name: result.name,
        bus_count: result.data?.bus?.length || 0,
        line_count: result.data?.line?.length || 0,
      }]);
      setCurrentNetwork({ network_id: result.network_id, name: result.name });
      showToast(`Model ${modelName} loaded`);
    } catch (err) {
      console.error(err);
      showToast(`Failed to load model ${modelName}`, 'error');
    }
  };

  const toggleSceNode = (path) => {
    setSceExpanded(prev => {
      const next = new Set(prev);
      if (next.has(path)) {
        next.delete(path);
      } else {
        next.add(path);
      }
      return next;
    });
  };

  const renderSceTree = (nodes, level = 0, parentPath = '') => {
    if (!nodes || nodes.length === 0) return null;
    return nodes.map(node => {
      const path = parentPath ? `${parentPath}›${node.name}` : node.name;
      const hasChildren = node.children && node.children.length > 0;
      const expanded = sceExpanded.has(path);
      return (
        <div key={path} className="sce-tree-node">
          <div
            className="sce-tree-row"
            style={{ paddingLeft: 4 + level * 12 }}
            onClick={() => hasChildren && toggleSceNode(path)}
          >
            {hasChildren ? (
              expanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />
            ) : (
              // Leaf node (Circuit)
              node.hasModel ? (
                <button
                  className="icon-btn-mini"
                  style={{ marginRight: 4, color: '#22c55e', cursor: 'pointer' }}
                  onClick={(e) => {
                    e.stopPropagation();
                    handleLoadModel(node.name.replace(/_(\d+)KV$/, '_$1KV')); // Attempt to match expected filename if needed, or just use node.name
                  }}
                  title="Load this model"
                >
                  <Play size={10} fill="currentColor" />
                </button>
              ) : <span className="sce-tree-spacer" />
            )}
            <span
              className={`sce-badge sce-level-${level} ${node.hasModel ? 'sce-model-available' : ''}`}
              style={node.hasModel ? { fontWeight: 'bold', color: '#fff' } : { opacity: 0.6 }}
            >
              {node.name}
            </span>
          </div>
          {hasChildren && expanded && (
            <div className="sce-tree-children">
              {renderSceTree(node.children, level + 1, path)}
            </div>
          )}
        </div>
      );
    });
  };

  // Derived analysis info for colouring lines in the diagram
  const analysisConverged = useMemo(() => {
    if (!analysisResults) return null;
    if (analysisResults.type === 'hosting') {
      return analysisResults.success ?? null;
    }
    if (analysisResults.results?.converged !== undefined) {
      return !!analysisResults.results.converged;
    }
    return analysisResults.success ?? null;
  }, [analysisResults]);

  const lineLoadingMap = useMemo(() => {
    const map = {};
    if (!analysisResults) return map;
    const phaseMax = (row, base) => {
      const vals = ['a', 'b', 'c']
        .map(s => Number(row[`${base}_${s}`]))
        .filter(v => Number.isFinite(v));
      if (vals.length) return Math.max(...vals);
      return Number(row[base]);
    };

    if ((analysisResults.type === 'powerflow' || analysisResults.type === 'load_allocation') && Array.isArray(analysisResults.results?.res_line)) {
      analysisResults.results.res_line.forEach(row => {
        const idx = Number(row.line);
        const loading = Number.isFinite(Number(row.loading_percent))
          ? Number(row.loading_percent)
          : phaseMax(row, 'i_ka');
        if (Number.isFinite(idx) && Number.isFinite(loading)) {
          map[idx] = loading;
        }
      });
    }

    if (analysisResults.type === 'time_series' && Array.isArray(analysisResults.results?.res_line)) {
      analysisResults.results.res_line.forEach(row => {
        const idx = Number(row.line);
        const loading = Number.isFinite(Number(row.loading_percent))
          ? Number(row.loading_percent)
          : phaseMax(row, 'i_ka');
        if (Number.isFinite(idx) && Number.isFinite(loading)) {
          map[idx] = Math.max(map[idx] ?? -Infinity, loading);
        }
      });
    }

    return map;
  }, [analysisResults]);

  const busVoltageMap = useMemo(() => {
    const map = {};
    if (!analysisResults) return map;
    const phaseAvg = (row, base) => {
      const vals = ['a', 'b', 'c']
        .map(s => Number(row[`${base}_${s}`]))
        .filter(v => Number.isFinite(v));
      if (vals.length) return vals.reduce((a, b) => a + b, 0) / vals.length;
      return Number(row[base]);
    };

    const accumulateVm = (bus, vm) => {
      if (!Number.isFinite(vm)) return;
      if (!map[bus]) {
        map[bus] = { min: vm, max: vm };
      } else {
        map[bus].min = Math.min(map[bus].min, vm);
        map[bus].max = Math.max(map[bus].max, vm);
      }
    };

    if ((analysisResults.type === 'powerflow' || analysisResults.type === 'load_allocation') && Array.isArray(analysisResults.results?.res_bus)) {
      analysisResults.results.res_bus.forEach(row => {
        const bus = Number(row.bus ?? row.index ?? row.id);
        const vm = phaseAvg(row, 'vm_pu');
        accumulateVm(bus, vm);
      });
    }

    if (analysisResults.type === 'time_series' && Array.isArray(analysisResults.results?.res_bus)) {
      analysisResults.results.res_bus.forEach(row => {
        const bus = Number(row.bus ?? row.index ?? row.id);
        const vm = phaseAvg(row, 'vm_pu');
        accumulateVm(bus, vm);
      });
    }

    return map;
  }, [analysisResults]);

  const colorForLoading = (loading) => {
    if (!Number.isFinite(loading)) return '#808080';
    const x = Math.min(Math.max(loading / 100, 0), 1);
    let r, g, b;

    if (x <= 0.5) {
      const t = x / 0.5;
      r = Math.round((1 - t) * 33 + t * 255);
      g = Math.round((1 - t) * 102 + t * 255);
      b = Math.round((1 - t) * 172 + t * 255);
    } else {
      const t = (x - 0.5) / 0.5;
      r = Math.round((1 - t) * 255 + t * 178);
      g = Math.round((1 - t) * 255 + t * 24);
      b = Math.round((1 - t) * 255 + t * 43);
    }
    return `rgb(${r}, ${g}, ${b})`;
  };

  const colorForVoltage = (vm) => {
    if (!Number.isFinite(vm)) return '#1a1a2e';
    if (vm < 0.95 || vm > 1.05) return '#ff0000';

    // Gradient: Black (0.95) -> Green (1.05)
    // Script logic: t = (vm - 0.95) / 0.10 => g = 255 * t
    const t = (vm - 0.95) / 0.10;
    const g = Math.floor(255 * Math.max(0, Math.min(1, t)));
    return `rgb(0, ${g}, 0)`;
  };

  const getLineColor = useCallback((edge) => {
    if (edge.type !== 'line') {
      return edge.type === 'transformer' ? '#3b82f6' : '#3b82f6';
    }

    // If the latest analysis did not converge, highlight all lines red
    if (analysisConverged === false) {
      return '#ef4444';
    }

    // Prefer line loading when available
    const lineId = Number(String(edge.id).replace('line_', ''));
    const loading = lineLoadingMap[lineId];
    if (Number.isFinite(loading)) {
      return colorForLoading(loading);
    }

    // Fallback: use bus voltage severity
    const fromVm = busVoltageMap[edge.from];
    const toVm = busVoltageMap[edge.to];

    const pickVm = (stat) => {
      if (!stat) return null;
      const deltaMin = Math.abs((stat.min ?? 1) - 1);
      const deltaMax = Math.abs((stat.max ?? 1) - 1);
      return deltaMax > deltaMin ? stat.max : stat.min;
    };

    const vmCandidates = [pickVm(fromVm), pickVm(toVm)].filter(v => Number.isFinite(v));
    if (vmCandidates.length > 0) {
      const worstVm = vmCandidates.reduce((acc, v) => {
        const currentDev = Math.abs((acc ?? 1) - 1);
        const nextDev = Math.abs(v - 1);
        return nextDev > currentDev ? v : acc ?? v;
      }, null);
      return colorForVoltage(worstVm);
    }

    // Default colouring
    if (analysisResults) {
      return '#22c55e';
    }
    return edge.in_service ? '#3b82f6' : '#6b7280';
  }, [analysisConverged, lineLoadingMap, busVoltageMap, analysisResults]);

  const togglePanel = (panel) => {
    setPanelStates(prev => ({ ...prev, [panel]: !prev[panel] }));
  };

  // Resize handlers for results pane
  useEffect(() => {
    const handleMove = (e) => {
      if (!isResizingResults || !resizeStart) return;
      e.preventDefault();
      const container = document.querySelector('.center-panel');
      const maxHeight = container ? Math.max(220, container.clientHeight - 80) : 800;
      const delta = resizeStart.y - e.clientY;
      const nextHeight = Math.min(maxHeight, Math.max(140, resizeStart.height + delta));
      setResultsHeight(nextHeight);
    };
    const handleUp = () => {
      if (isResizingResults) {
        setIsResizingResults(false);
        setResizeStart(null);
      }
    };
    window.addEventListener('mousemove', handleMove);
    window.addEventListener('mouseup', handleUp);
    return () => {
      window.removeEventListener('mousemove', handleMove);
      window.removeEventListener('mouseup', handleUp);
    };
  }, [isResizingResults, resizeStart]);

  // Resize observer for chart container
  useEffect(() => {
    const el = chartRef.current;
    if (!el) return;
    const update = () => setChartSize({ w: el.clientWidth || 720, h: el.clientHeight || 320 });
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => ro.disconnect();
  }, [chartRef]);

  // Keyboard shortcuts for Undo/Redo and Selection
  useEffect(() => {
    const handleKeyDown = (e) => {
      // Ignore if typing in an input
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') {
        return;
      }

      const isMod = e.ctrlKey || e.metaKey;

      // Ctrl+Z = Undo, Ctrl+Shift+Z = Redo
      if (isMod && e.key === 'z') {
        e.preventDefault();
        if (e.shiftKey) {
          redo();
        } else {
          undo();
        }
        return;
      }

      // Escape = Deselect all
      if (e.key === 'Escape') {
        setSelectedElement(null);
        setSelectedElements(new Set());
        return;
      }

      // Delete/Backspace = Delete selected elements
      if (e.key === 'Delete' || e.key === 'Backspace') {
        if (selectedElements.size > 0 || selectedElement) {
          e.preventDefault();
          handleDeleteSelected();
        }
        return;
      }

      // Ctrl+A = Select all nodes
      if (isMod && e.key === 'a') {
        e.preventDefault();
        handleSelectAll();
        return;
      }

      // Ctrl+C = Copy selected
      if (isMod && e.key === 'c') {
        e.preventDefault();
        handleCopySelected();
        return;
      }

      // Ctrl+V = Paste
      if (isMod && e.key === 'v') {
        e.preventDefault();
        handlePaste();
        return;
      }

      // Ctrl+X = Cut (copy + delete)
      if (isMod && e.key === 'x') {
        e.preventDefault();
        handleCopySelected();
        handleDeleteSelected();
        return;
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [undo, redo, selectedElement, selectedElements]);

  // Selection helper functions
  const handleSelectAll = useCallback(() => {
    if (!topology?.nodes) return;
    const allKeys = new Set(topology.nodes.map(n => `node_${n.id}`));
    setSelectedElements(allKeys);
    showToast(`Selected ${allKeys.size} elements`);
  }, [topology]);

  const handleDeleteSelected = useCallback(async () => {
    const toDelete = selectedElements.size > 0
      ? Array.from(selectedElements)
      : selectedElement ? [`${selectedElement.type}_${selectedElement.index}`] : [];

    if (toDelete.length === 0) return;

    // Confirm if multiple elements
    if (toDelete.length > 1) {
      if (!window.confirm(`Delete ${toDelete.length} selected elements?`)) return;
    }

    for (const key of toDelete) {
      const [type, index] = key.split('_');
      try {
        if (currentNetwork?.network_id) {
          await api.deleteElement(currentNetwork.network_id, type, parseInt(index));
        }
      } catch (err) {
        console.error('Delete failed:', err);
      }
    }

    // Refresh network data
    if (currentNetwork?.network_id) {
      loadNetworkData(currentNetwork.network_id);
    }
    setSelectedElement(null);
    setSelectedElements(new Set());
    showToast(`Deleted ${toDelete.length} element(s)`);
  }, [selectedElements, selectedElement, currentNetwork]);

  const handleCopySelected = useCallback(() => {
    const toCopy = selectedElements.size > 0
      ? Array.from(selectedElements)
      : selectedElement ? [`${selectedElement.type}_${selectedElement.index}`] : [];

    if (toCopy.length === 0) return;

    // Store element data for paste
    const copiedData = toCopy.map(key => {
      const [type, index] = key.split('_');
      // Find element in elements state
      const categoryKey = Object.keys(elements).find(k =>
        elements[k]?.some?.(el => el.type === type && el.index === parseInt(index))
      );
      if (categoryKey) {
        return elements[categoryKey].find(el => el.type === type && el.index === parseInt(index));
      }
      return null;
    }).filter(Boolean);

    setClipboard(copiedData);
    showToast(`Copied ${copiedData.length} element(s)`);
  }, [selectedElements, selectedElement, elements]);

  const handlePaste = useCallback(async () => {
    if (clipboard.length === 0) {
      showToast('Nothing to paste');
      return;
    }

    // Create new elements from clipboard
    for (const el of clipboard) {
      try {
        if (currentNetwork?.network_id && el.type) {
          const newData = { ...el };
          delete newData.index; // Let backend assign new index
          // Offset position if available
          if (typeof newData.x === 'number') newData.x += 50;
          if (typeof newData.y === 'number') newData.y += 50;

          await api.createElement(currentNetwork.network_id, el.type, newData);
        }
      } catch (err) {
        console.error('Paste failed:', err);
      }
    }

    // Refresh network data
    if (currentNetwork?.network_id) {
      loadNetworkData(currentNetwork.network_id);
    }
    showToast(`Pasted ${clipboard.length} element(s)`);
  }, [clipboard, currentNetwork]);

  // Create element from symbol palette (drag-drop or double-click)
  const handleCreateElement = useCallback(async (symbolType, position = { x: 200, y: 200 }) => {
    if (!currentNetwork?.network_id) {
      showToast('Please select a network first');
      return;
    }

    // Map symbol types to API element types and default data
    const elementDefaults = {
      bus: { name: 'New Bus', vn_kv: 20, type: 'b' },
      line: { from_bus: 0, to_bus: 1, length_km: 1, std_type: 'NAYY 4x50 SE' },
      trafo: { hv_bus: 0, lv_bus: 1, sn_mva: 0.4, vn_hv_kv: 20, vn_lv_kv: 0.4 },
      load: { bus: 0, p_mw: 0.1, q_mvar: 0.05, name: 'New Load' },
      gen: { bus: 0, p_mw: 0.5, vm_pu: 1.0, name: 'New Generator' },
      sgen: { bus: 0, p_mw: 0.1, q_mvar: 0, name: 'New Static Gen' },
      ext_grid: { bus: 0, vm_pu: 1.0, name: 'External Grid' },
      switch: { bus: 0, element: 0, et: 'b', closed: true },
      storage: { bus: 0, p_mw: 0, max_e_mwh: 1, name: 'New Storage' },
    };

    const defaults = elementDefaults[symbolType];
    if (!defaults) {
      showToast(`Unknown element type: ${symbolType}`);
      return;
    }

    try {
      const data = { ...defaults, x: position.x, y: position.y };
      await api.createElement(currentNetwork.network_id, symbolType, data);
      await loadNetworkData(currentNetwork.network_id);
      showToast(`Created ${symbolType}`);
    } catch (err) {
      console.error('Create element failed:', err);
      showToast(`Failed to create ${symbolType}: ${err.message}`);
    }
  }, [currentNetwork]);

  // Handle drop on canvas from symbol palette
  const handleCanvasDrop = useCallback((e) => {
    e.preventDefault();
    const symbolType = e.dataTransfer.getData('application/x-symbol-type');
    if (!symbolType) return;

    // Get drop position in SVG coordinates
    const svg = e.currentTarget;
    if (!svg || !svg.getScreenCTM) return;

    const pt = svg.createSVGPoint();
    pt.x = e.clientX;
    pt.y = e.clientY;
    const ctm = svg.getScreenCTM();
    if (!ctm) return;
    const svgP = pt.matrixTransform(ctm.inverse());

    handleCreateElement(symbolType, { x: svgP.x, y: svgP.y });
  }, [handleCreateElement]);

  const handleCanvasDragOver = useCallback((e) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'copy';
  }, []);

  // Connection drawing handlers
  const handleStartConnection = useCallback((busId, pos) => {
    if (!connectionMode) return;
    setDrawingConnection({
      fromBusId: busId,
      fromPos: { x: pos.x, y: pos.y },
      currentPos: { x: pos.x, y: pos.y }
    });
  }, [connectionMode]);

  const handleUpdateConnection = useCallback((pos) => {
    if (!drawingConnection) return;
    setDrawingConnection(prev => ({
      ...prev,
      currentPos: { x: pos.x, y: pos.y }
    }));
  }, [drawingConnection]);

  const handleCompleteConnection = useCallback(async (toBusId) => {
    if (!drawingConnection || !currentNetwork?.network_id) {
      setDrawingConnection(null);
      return;
    }

    const fromBusId = drawingConnection.fromBusId;

    // Don't connect a bus to itself
    if (fromBusId === toBusId) {
      showToast('Cannot connect a bus to itself');
      setDrawingConnection(null);
      return;
    }

    try {
      // Create a new line between the buses
      await api.createElement(currentNetwork.network_id, 'line', {
        from_bus: fromBusId,
        to_bus: toBusId,
        length_km: 1,
        std_type: 'NAYY 4x50 SE',
        name: `Line ${fromBusId}-${toBusId}`
      });
      await loadNetworkData(currentNetwork.network_id);
      showToast(`Created line from Bus ${fromBusId} to Bus ${toBusId}`);
    } catch (err) {
      console.error('Create connection failed:', err);
      showToast('Failed to create connection');
    }

    setDrawingConnection(null);
  }, [drawingConnection, currentNetwork]);

  const handleCancelConnection = useCallback(() => {
    setDrawingConnection(null);
  }, []);

  const toggleLayer = useCallback((layerId) => {
    if (layerId === 'grid') {
      updateSettings({ gridVisible: !settings.gridVisible });
    } else {
      setLayerVisibility(prev => ({
        ...prev,
        [layerId]: !prev[layerId]
      }));
    }
  }, [settings.gridVisible, updateSettings]);


  const startResizeResults = (e) => {
    e.preventDefault();
    setIsResizingResults(true);
    setResizeStart({ y: e.clientY, height: resultsHeight });
  };

  // Element group definitions
  const elementGroups = [
    {
      key: 'sources',
      label: 'Sources',
      color: '#06b6d4',
      icon: Grid,
      elements: [
        { key: 'extGrids', label: 'External Grid', icon: Grid, dataKey: 'ext_grid' },
      ]
    },
    {
      key: 'equipment',
      label: 'Equipment',
      color: '#3b82f6',
      icon: Box,
      elements: [
        { key: 'buses', label: 'Bus', icon: Circle, dataKey: 'bus' },
        { key: 'lines', label: 'Line', icon: Activity, dataKey: 'line' },
        { key: 'switches', label: 'Switch', icon: Power, dataKey: 'switch' },
        { key: 'trafo3w', label: 'Three Winding Transformer', icon: Layers, dataKey: 'trafo3w' },
        { key: 'transformers', label: 'Transformer', icon: Box, dataKey: 'trafo' },
      ]
    },
    {
      key: 'generators',
      label: 'Generators',
      color: '#8b5cf6',
      icon: Cpu,
      elements: [
        { key: 'generators', label: 'Generator', icon: Cpu, dataKey: 'gen' },
        { key: 'staticGens', label: 'Static Generator', icon: Zap, dataKey: 'sgen' },
        { key: 'motors', label: 'Motor', icon: Disc, dataKey: 'motor' },
      ]
    },
    {
      key: 'loads',
      label: 'Loads',
      color: '#ef4444',
      icon: Zap,
      elements: [
        { key: 'loads', label: 'Load', icon: Zap, dataKey: 'load' },
        { key: 'asymmetricLoads', label: 'Asymmetric Load', icon: Triangle, dataKey: 'asymmetric_load' },
      ]
    },
    {
      key: 'dc',
      label: 'DC',
      color: '#3b82f6',
      icon: Battery,
      elements: [
        { key: 'storages', label: 'Storage', icon: Battery, dataKey: 'storage' },
        { key: 'dclines', label: 'DC Line', icon: GitBranch, dataKey: 'dcline' },
      ]
    },
    {
      key: 'other',
      label: 'Other',
      color: '#84cc16',
      icon: Settings,
      elements: [
        { key: 'shunts', label: 'Shunt', icon: Triangle, dataKey: 'shunt' },
        { key: 'asymmetricSgens', label: 'Asymmetric Static Generator', icon: Zap, dataKey: 'asymmetric_sgen' },
        { key: 'svcs', label: 'Static Var Compensator (SVC)', icon: Gauge, dataKey: 'svc' },
        { key: 'tcscs', label: 'Thyristor-Controlled Series Capacitor (TCSC)', icon: Radio, dataKey: 'tcsc' },
        { key: 'sscs', label: 'Static Synchronous Compensator (SSC)', icon: Radio, dataKey: 'ssc' },
        { key: 'wards', label: 'Ward Equivalent', icon: Square, dataKey: 'ward' },
        { key: 'xwards', label: 'Extended Ward Equivalent', icon: Square, dataKey: 'xward' },
        { key: 'impedances', label: 'Impedance', icon: Activity, dataKey: 'impedance' },
        { key: 'measurements', label: 'Measurement', icon: Gauge, dataKey: 'measurement' },
      ]
    },
  ];

  // Load saved networks from localStorage on mount
  useEffect(() => {
    const savedNetworks = localStorage.getItem('pandapower_networks');
    if (savedNetworks) {
      try {
        const parsed = JSON.parse(savedNetworks);
        setNetworks(parsed);
      } catch (e) {
        console.error('Failed to load saved networks:', e);
      }
    }

    // Check API health
    api.checkHealth()
      .then(() => setApiStatus('connected'))
      .catch(() => setApiStatus('disconnected'));

    // Load standard types
    api.getLineTypes().then(data => setLineTypes(data.line_types || [])).catch(() => { });
    api.getTrafoTypes().then(data => setTrafoTypes(data.transformer_types || [])).catch(() => { });
    api.getLineTypes().then(data => setLineTypes(data.line_types || [])).catch(() => { });
    api.getTrafoTypes().then(data => setTrafoTypes(data.transformer_types || [])).catch(() => { });

    // Load available manual models
    api.listPandapowerModels()
      .then(data => setAvailableModels(new Set(data.models || [])))
      .catch(e => console.error("Failed to load available models", e));
  }, []);

  // Load TCGM GIS layers (geo nodes grouped by groupid)
  useEffect(() => {
    let active = true;
    setTcgmLoading(true);
    api.getTcgmNodes()
      .then(data => {
        if (!active) return;
        setTcgmLayers(data.groups || []);
        setTcgmError(null);
      })
      .catch(err => {
        if (!active) return;
        setTcgmError(err?.message || 'Failed to load tcgm-gis.json');
      })
      .finally(() => {
        if (active) setTcgmLoading(false);
      });
    return () => { active = false; };
  }, []);

  // Save networks to localStorage whenever they change
  useEffect(() => {
    if (networks.length > 0) {
      localStorage.setItem('pandapower_networks', JSON.stringify(networks));
    }
  }, [networks]);

  // Load node positions from localStorage when network changes
  useEffect(() => {
    if (currentNetwork?.network_id) {
      const savedPositions = localStorage.getItem(`node_positions_${currentNetwork.network_id}`);
      if (savedPositions) {
        try {
          setNodePositions(JSON.parse(savedPositions));
        } catch (e) {
          setNodePositions({});
        }
      } else {
        setNodePositions({});
      }
      setViewTransform({ scale: 1, x: 0, y: 0 });
      setDraggingNode(null);
      setIsPanning(false);
    }
  }, [currentNetwork?.network_id]);

  // Save node positions to localStorage when they change
  useEffect(() => {
    if (currentNetwork?.network_id && Object.keys(nodePositions).length > 0) {
      localStorage.setItem(`node_positions_${currentNetwork.network_id}`, JSON.stringify(nodePositions));
    }
  }, [nodePositions, currentNetwork?.network_id]);

  // Load network data when current network changes
  const loadNetworkData = useCallback(async (networkId) => {
    if (!networkId) return;
    try {
      const [netData, stats, topo, elemCounts, buses, lines, trafos, trafo3w, switches,
        loads, asymLoads, gens, sgens, motors, extGrids, dclines, storages,
        shunts, asymSgens, svcs, tcscs, sscs, wards, xwards, impedances, measurements] = await Promise.all([
          api.getNetwork(networkId),
          api.getStatistics(networkId),
          api.getTopology(networkId),
          api.getAllElements(networkId),
          api.getBuses(networkId),
          api.getLines(networkId),
          api.getTransformers(networkId),
          api.getTrafo3w(networkId),
          api.getSwitches(networkId),
          api.getLoads(networkId),
          api.getAsymmetricLoads(networkId),
          api.getGenerators(networkId),
          api.getStaticGens(networkId),
          api.getMotors(networkId),
          api.getExtGrids(networkId),
          api.getDclines(networkId),
          api.getStorages(networkId),
          api.getShunts(networkId),
          api.getAsymmetricSgens(networkId),
          api.getSvcs(networkId),
          api.getTcscs(networkId),
          api.getSscs(networkId),
          api.getWards(networkId),
          api.getXwards(networkId),
          api.getImpedances(networkId),
          api.getMeasurements(networkId),
        ]);

      // 1. Try to load layout from backend geodata
      let loadedPositions = {};
      if (netData.bus_geodata && Array.isArray(netData.bus_geodata) && netData.bus_geodata.length > 0) {
        netData.bus_geodata.forEach(bg => {
          if (bg.index !== undefined && bg.x !== null && bg.y !== null) {
            loadedPositions[bg.index] = { x: bg.x, y: bg.y };
          }
        });
      }

      // 2. Fallback to localStorage if backend data empty
      if (Object.keys(loadedPositions).length === 0) {
        const savedPositions = localStorage.getItem(`node_positions_${networkId}`);
        if (savedPositions) {
          try { loadedPositions = JSON.parse(savedPositions); } catch (e) { }
        }
      }

      setNodePositions(loadedPositions);

      // Reset view if no previous positions known
      // setViewTransform({ scale: 1, x: 0, y: 0 }); // Optional: could keep view

      // Use topology from API if available, otherwise construct from buses
      if (topo?.nodes && topo?.edges) {
        setTopology(topo);
      } else {
        setTopology({
          nodes: buses?.buses || [],
          edges: [
            ...(lines?.lines || []),
            ...(trafos?.transformers || []),
            ...(trafo3w?.trafo3w || []),
            ...(switches?.switches || []),
            ...(dclines?.dclines || []),
            ...(impedances?.impedances || [])
          ]
        });
      }

      const counts = { ...(elemCounts?.elements || {}) };
      // ensure all keys exist
      ['bus', 'line', 'load', 'gen', 'sgen', 'ext_grid', 'trafo', 'trafo3w', 'switch', 'shunt', 'motor', 'asymmetric_load', 'asymmetric_sgen', 'dcline', 'storage', 'svc', 'tcsc', 'ssc', 'ward', 'xward', 'impedance', 'measurement'].forEach(k => {
        if (counts[k] === undefined) counts[k] = 0;
      });
      setElementCounts(counts);

      setElements({
        // Sources
        extGrids: extGrids?.external_grids || [],
        // Equipment
        buses: buses?.buses || [],
        lines: lines?.lines || [],
        switches: switches?.switches || [],
        transformers: trafos?.transformers || [],
        trafo3w: trafo3w?.trafo3w || [],
        // Generators
        generators: gens?.generators || [],
        staticGens: sgens?.static_generators || [],
        motors: motors?.motors || [],
        // Loads
        loads: loads?.loads || [],
        asymmetricLoads: asymLoads?.asymmetric_loads || [],
        // DC
        dclines: dclines?.dclines || [],
        storages: storages?.storages || [],
        // Other
        shunts: shunts?.shunts || [],
        asymmetricSgens: asymSgens?.asymmetric_sgens || [],
        svcs: svcs?.svcs || [],
        tcscs: tcscs?.tcscs || [],
        sscs: sscs?.sscs || [],
        wards: wards?.wards || [],
        xwards: xwards?.xwards || [],
        impedances: impedances?.impedances || [],
        measurements: measurements?.measurements || [],
      });
      setAnalysisResults(null);

      // If no saved layout, auto-fit geodata (e.g., mv_oberrhein) into viewport
      const savedPositions = localStorage.getItem(`node_positions_${networkId}`);
      if (!savedPositions && topo?.nodes?.length) {
        const nodesWithGeo = topo.nodes.filter(n => n.x !== undefined && n.y !== undefined);
        if (nodesWithGeo.length > 0) {
          const xs = nodesWithGeo.map(n => n.x);
          const ys = nodesWithGeo.map(n => n.y);
          const minX = Math.min(...xs), maxX = Math.max(...xs);
          const minY = Math.min(...ys), maxY = Math.max(...ys);
          const targetW = 1200, targetH = 800, pad = 40;
          const scaleX = (targetW - 2 * pad) / (maxX - minX || 1);
          const scaleY = (targetH - 2 * pad) / (maxY - minY || 1);
          const scale = Math.min(scaleX, scaleY);
          const fitPositions = {};
          nodesWithGeo.forEach(n => {
            fitPositions[n.id] = {
              x: pad + (n.x - minX) * scale,
              y: pad + (n.y - minY) * scale
            };
          });
          setNodePositions(fitPositions);
          localStorage.setItem(`node_positions_${networkId}`, JSON.stringify(fitPositions));
          setViewTransform({ scale: 1, x: 0, y: 0 });
        }
      }
    } catch (error) {
      console.error('Failed to load network data:', error);
      showToast('Failed to load network data', 'error');
    }
  }, []);

  useEffect(() => {
    if (currentNetwork) {
      loadNetworkData(currentNetwork.network_id);
    }
  }, [currentNetwork, loadNetworkData]);

  const showToast = (message, type = 'success') => {
    setToast({ message, type });
  };

  const handleCreateNetwork = async (name = 'New Network') => {
    try {
      const result = await api.createNetwork({ name, f_hz: 50.0, sn_mva: 1.0 });
      const newNetwork = { network_id: result.network_id, name, bus_count: 0, line_count: 0, load_count: 0, gen_count: 0 };
      setNetworks(prev => [...prev, newNetwork]);
      setCurrentNetwork(newNetwork);
      showToast('Network created successfully');
      setModal(null);
    } catch (error) {
      showToast('Failed to create network', 'error');
    }
  };

  const handleLoadSample = async (sampleId) => {
    try {
      const result = await api.loadSample(sampleId);
      // Clear any old layout so autolayout/geodata can reapply for this instance
      if (result.network_id) {
        localStorage.removeItem(`node_positions_${result.network_id}`);
      }
      // Apply preset layout for IEEE 30-bus samples
      if (['case30', 'case_ieee30'].includes(sampleId) && result.network_id) {
        const preset30 = {
          0: { x: -420, y: 0 },
          1: { x: -360, y: -60 },
          2: { x: -260, y: -60 },
          3: { x: -160, y: -40 },
          4: { x: -60, y: -20 },
          5: { x: 40, y: -20 },
          6: { x: 140, y: -10 },
          7: { x: 240, y: 0 },
          8: { x: 340, y: 10 },
          9: { x: 440, y: 20 },
          10: { x: 540, y: 30 },
          11: { x: 640, y: 30 },
          12: { x: 740, y: 40 },
          13: { x: -140, y: 120 },
          14: { x: -40, y: 120 },
          15: { x: 60, y: 120 },
          16: { x: 160, y: 120 },
          17: { x: 260, y: 120 },
          18: { x: 360, y: 120 },
          19: { x: 460, y: 120 },
          20: { x: 560, y: 120 },
          21: { x: 660, y: 120 },
          22: { x: 760, y: 120 },
          23: { x: 500, y: 200 },
          24: { x: 600, y: 200 },
          25: { x: 700, y: 200 },
          26: { x: 800, y: 200 },
          27: { x: 900, y: 200 },
          28: { x: 820, y: -40 },
          29: { x: 940, y: -40 },
        };
        setNodePositions(preset30);
        localStorage.setItem(`node_positions_${result.network_id}`, JSON.stringify(preset30));
        setViewTransform({ scale: 0.9, x: 300, y: 140 });
      }

      const newNetwork = {
        network_id: result.network_id,
        name: result.name,
        bus_count: result.data?.bus?.length || 0,
        line_count: result.data?.line?.length || 0,
        load_count: result.data?.load?.length || 0,
        gen_count: (result.data?.gen?.length || 0) + (result.data?.sgen?.length || 0) + (result.data?.ext_grid?.length || 0)
      };
      setNetworks(prev => [...prev, newNetwork]);
      setCurrentNetwork(newNetwork);
      showToast('Sample network loaded');
      setModal(null);
    } catch (error) {
      showToast('Failed to load sample network', 'error');
    }
  };

  const handleDeleteNetwork = async (networkId) => {
    try {
      await api.deleteNetwork(networkId);
      setNetworks(prev => prev.filter(n => n.network_id !== networkId));
      if (currentNetwork?.network_id === networkId) {
        setCurrentNetwork(null);
        setNetworkData(null);
        setElements({
          extGrids: [], buses: [], lines: [], switches: [], transformers: [], trafo3w: [],
          generators: [], staticGens: [], motors: [], loads: [], asymmetricLoads: [],
          dclines: [], storages: [], shunts: [], asymmetricSgens: [], svcs: [], tcscs: [], sscs: [],
          wards: [], xwards: [], impedances: []
        });
      }
      showToast('Network deleted');
    } catch (error) {
      showToast('Failed to delete network', 'error');
    }
  }




  const handleRunPowerFlow = async (options = {}) => {
    if (!currentNetwork) return;
    setIsAnalyzing(true);
    try {
      const result = await api.runPowerFlow(currentNetwork.network_id, options);
      setAnalysisResults({ type: 'powerflow', ...result });
      setActiveResultTab('bus');
      if (result.success) {
        showToast(result.results.converged ? 'Power flow converged' : 'Power flow did not converge', result.results.converged ? 'success' : 'error');
      } else {
        showToast(`Analysis failed: ${result.error}`, 'error');
      }
    } catch (error) {
      showToast('Failed to run power flow', 'error');
    }
    setIsAnalyzing(false);
    setModal(null);
  };

  const handleRunLoadAllocation = async (options = {}) => {
    if (!currentNetwork) return;
    setIsAnalyzing(true);
    try {
      const result = await api.runLoadAllocation(currentNetwork.network_id, options);
      setAnalysisResults({ type: 'load_allocation', ...result });
      setActiveResultTab('bus');
      if (result.success) {
        showToast('Load allocation complete', 'success');
      } else {
        showToast(`Load allocation failed: ${result.error}`, 'error');
      }
    } catch (error) {
      showToast('Failed to run load allocation', 'error');
    }
    setIsAnalyzing(false);
    setModal(null);
  };

  const handleRunShortCircuit = async (options = {}) => {
    if (!currentNetwork) return;
    setIsAnalyzing(true);
    try {
      const result = await api.runShortCircuit(currentNetwork.network_id, options);
      setAnalysisResults({ type: 'shortcircuit', ...result });
      setActiveResultTab('bus_sc');
      showToast(result.success ? 'Short circuit analysis complete' : `Analysis failed: ${result.error}`, result.success ? 'success' : 'error');
    } catch (error) {
      showToast('Failed to run short circuit analysis', 'error');
    }
    setIsAnalyzing(false);
    setModal(null);
  };

  const handleRunHostingCapacity = async (options = {}) => {
    setActiveResultTab('hosting_summary');
    setIsAnalyzing(true);
    try {
      const payload = { ...options };
      if (currentNetwork?.network_id) {
        payload.network_id = currentNetwork.network_id;
      }
      const result = await api.runHostingCapacity(payload);
      setAnalysisResults({ type: 'hosting', hosting: result, success: result.success, error: result.error });
      showToast(result.success ? 'Hosting capacity analysis complete' : `Analysis failed: ${result.error}`, result.success ? 'success' : 'error');
    } catch (error) {
      showToast('Failed to run hosting capacity analysis', 'error');
    }
    setIsAnalyzing(false);
    setModal(null);
  };

  const handleRunTimeSeries = async (options = {}) => {
    if (!currentNetwork) return;
    setIsAnalyzing(true);
    try {
      const result = await api.runTimeSeries(currentNetwork.network_id, options);
      setAnalysisResults({ type: 'time_series', ...result });
      setActiveResultTab('ts_inputs');
      // Reset chart selections on new run
      setTsChartSource('res_bus');
      setTsChartField(null);
      setTsChartType('line');
      showToast(result.success ? 'Time series simulation complete' : `Time series issues: ${result.error || 'Check convergence'}`, result.success ? 'success' : 'error');
    } catch (error) {
      showToast('Failed to run time series simulation', 'error');
    }
    setIsAnalyzing(false);
    setModal(null);
  };

  // Auto-Layout Handler
  const handleAutoLayout = async () => {
    if (!topology || !topology.nodes) return;

    // We can use the 'elements' state directly
    // Map App.js state keys to AutoLayout expectation (singular)
    const layoutElements = {
      bus: elements.buses,
      line: elements.lines,
      trafo: elements.transformers
    };

    const newPositions = await applyAutoLayout(layoutElements, nodePositions, selectedLayoutAlgo);
    setNodePositions(prev => ({ ...prev, ...newPositions }));

    // Persist to backend
    if (currentNetwork?.network_id) {
      api.updateLayout(currentNetwork.network_id, newPositions)
        .then(() => showToast('Layout updated and saved'))
        .catch(err => {
          console.error(err);
          showToast('Layout updated locally (save failed)', 'error');
        });
    } else {
      showToast('Layout applied');
    }
  };

  // Generic handler for creating any element
  const handleGenericAddElement = async (type, data = {}) => {
    if (!currentNetwork) return;
    try {
      // Default data if none provided
      const finalData = { ...data };

      // Add defaults based on type if needed, e.g.
      if (!finalData.name) finalData.name = `New ${type}`;

      const result = await api.createElement(currentNetwork.network_id, type, finalData);

      // Refresh data
      loadNetworkData(currentNetwork.network_id);
      showToast(`${type} added`);
      setModal(null);
    } catch (error) {
      showToast(`Failed to add ${type}`, 'error');
    }
  };

  // Generic handler for deleting any element
  const handleGenericDeleteElement = async (type, index) => {
    if (!currentNetwork) return;
    if (!window.confirm(`Are you sure you want to delete ${type} ${index}?`)) return;

    try {
      await api.deleteElement(currentNetwork.network_id, type, index);
      // Refresh data
      loadNetworkData(currentNetwork.network_id);
      showToast(`${type} deleted`);
    } catch (error) {
      showToast(`Failed to delete ${type}`, 'error');
    }
  };

  const handleAddElement = async (type, data) => {
    if (!currentNetwork) return;
    try {
      let result;
      switch (type) {
        case 'bus':
          result = await api.createBus(currentNetwork.network_id, data);
          break;
        case 'line':
          result = await api.createLine(currentNetwork.network_id, data);
          break;
        case 'load':
          result = await api.createLoad(currentNetwork.network_id, data);
          break;
        case 'ext_grid':
          result = await api.createExtGrid(currentNetwork.network_id, data);
          break;
        case 'generator':
          result = await api.createGenerator(currentNetwork.network_id, data);
          break;
        case 'transformer':
          result = await api.createTransformer(currentNetwork.network_id, data);
          break;
        default:
          throw new Error('Unknown element type');
      }
      showToast(`${type} created successfully`);
      await loadNetworkData(currentNetwork.network_id);
      setModal(null);
    } catch (error) {
      showToast(`Failed to create ${type}`, 'error');
    }
  };

  const toggleGroup = (group) => {
    setExpandedGroups(prev => ({ ...prev, [group]: !prev[group] }));
  };

  const toggleCategory = (category) => {
    setExpandedCategories(prev => ({ ...prev, [category]: !prev[category] }));
  };

  const getElementCount = (dataKey) => {
    return elementCounts[dataKey] || 0;
  };

  const getGroupCount = (group) => {
    return group.elements.reduce((sum, el) => sum + getElementCount(el.dataKey), 0);
  };

  const handleSaveVersion = async (description) => {
    if (!currentNetwork) return;
    try {
      await api.saveNetworkVersion(currentNetwork.network_id, description);
      const history = await api.getNetworkHistory(currentNetwork.network_id);
      setNetworkHistory(history);
      showToast('Version saved');
    } catch (err) {
      showToast('Failed to save version', 'error');
    }
  };

  const handleLoadVersion = async (versionId) => {
    if (!currentNetwork) return;
    try {
      const success = await api.restoreNetworkVersion(currentNetwork.network_id, versionId);
      if (success) {
        await loadNetworkData(currentNetwork.network_id);
        showToast(`Loaded version ${versionId.substring(0, 8)}`);
        // Mark as selected
        setSelectedVersionId(versionId);
      }
    } catch (err) {
      showToast('Failed to load version', 'error');
    }
  };

  // Render Welcome Screen
  const renderWelcome = () => (
    <div className="welcome-screen">
      <div className="welcome-content">
        <div className="welcome-icon">🐼⚡</div>
        <h1 className="welcome-title">Panda Power</h1>
        <p className="welcome-subtitle">
          Professional power system modeling and analysis platform powered by pandapower.
          Create networks, run power flow analysis, and visualize results.
        </p>
        <div className="welcome-actions">
          <div className="welcome-card" onClick={() => setModal('create')} data-testid="create-network-card">
            <div className="welcome-card-icon">➕</div>
            <div className="welcome-card-title">New Network</div>
            <div className="welcome-card-desc">Create empty network</div>
          </div>
          <div className="welcome-card" onClick={() => {
            api.listSamples().then(data => setSamples(data.samples || []));
            setModal('samples');
          }} data-testid="load-sample-card">
            <div className="welcome-card-icon">📦</div>
            <div className="welcome-card-title">Load Sample</div>
            <div className="welcome-card-desc">IEEE test cases</div>
          </div>
          <div className="welcome-card" onClick={() => setModal('import')} data-testid="import-network-card">
            <div className="welcome-card-icon">📥</div>
            <div className="welcome-card-title">Import</div>
            <div className="welcome-card-desc">From JSON file</div>
          </div>
        </div>
      </div>
    </div>
  );

  // Render Element Tree with Groups
  const renderElementTree = () => {
    return (
      <div className="element-tree">
        {elementGroups.map(group => {
          const groupCount = getGroupCount(group);
          const isGroupExpanded = expandedGroups[group.key];

          return (
            <div key={group.key} className="element-group">
              <div
                className="element-group-header"
                onClick={() => toggleGroup(group.key)}
                style={{ borderLeft: `3px solid ${group.color}` }}
              >
                {isGroupExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                <group.icon size={14} style={{ color: group.color }} />
                <span className="element-group-label">{group.label}</span>
                <span className="element-group-count">{groupCount}</span>
              </div>

              {isGroupExpanded && (
                <div className="element-group-content">
                  {group.elements.map(el => {
                    const count = getElementCount(el.dataKey);
                    const items = elements[el.key] || [];
                    const isCategoryExpanded = expandedCategories[el.key];

                    return (
                      <div key={el.key} className="tree-category">
                        <div
                          className="tree-category-header"
                          onClick={() => toggleCategory(el.key)}
                        >
                          <div style={{ display: 'flex', alignItems: 'center', flex: 1 }}>
                            {count > 0 ? (
                              isCategoryExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />
                            ) : <span style={{ width: 12 }} />}
                            <el.icon size={12} style={{ color: group.color, opacity: 0.7, marginLeft: 4 }} />
                            <span>{el.label}</span>
                            <span className="tree-category-count">{count}</span>
                          </div>
                          <button
                            className="icon-btn-mini action-btn"
                            onClick={(e) => {
                              e.stopPropagation();
                              if (el.dataKey === 'measurement') {
                                setModal('add-measurement');
                              } else {
                                handleGenericAddElement(el.dataKey);
                              }
                            }}
                            title={`Add ${el.label}`}
                          >
                            <Plus size={12} />
                          </button>
                        </div>

                        {isCategoryExpanded && items.length > 0 && (
                          <div className="tree-items">
                            {items.map((item, idx) => (
                              <div
                                key={idx}
                                className={`tree-item ${selectedElement?.key === el.key && selectedElement?.index === item.index ? 'selected' : ''}`}
                                onClick={() => setSelectedElement({ key: el.key, type: el.label, ...item })}
                              >
                                <span style={{ display: 'flex', alignItems: 'center', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                  <Circle size={6} style={{ color: group.color, marginRight: 6 }} />
                                  {item.name || `${el.label} ${item.index}`}
                                </span>
                                <button
                                  className="icon-btn-mini delete-btn"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    handleGenericDeleteElement(el.dataKey, item.index);
                                  }}
                                  title="Delete"
                                >
                                  <Trash2 size={10} />
                                </button>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>
    );
  };

  // Render Properties Panel
  const renderProperties = () => {
    if (!selectedElement) {
      return (
        <div className="properties-empty">
          <Info size={32} style={{ marginBottom: 8, opacity: 0.5 }} />
          <div>Select an element to view properties</div>
        </div>
      );
    }

    const formatValue = (val) => {
      if (val === null || val === undefined || val === 'None' || val === 'nan') return '-';
      if (typeof val === 'boolean') return val ? 'Yes' : 'No';
      if (typeof val === 'number') {
        if (Number.isInteger(val)) return val.toString();
        return val.toFixed(4);
      }
      return String(val);
    };

    const formatLabel = (key) => {
      // Convert snake_case to Title Case
      return key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    };

    // Categorize properties
    const basicProps = ['name', 'in_service', 'type'];
    const electricalProps = ['vn_kv', 'p_mw', 'q_mvar', 'vm_pu', 'va_degree', 'sn_mva', 'vk_percent', 'vkr_percent',
      'pfe_kw', 'i0_percent', 'max_i_ka', 'r_ohm_per_km', 'x_ohm_per_km', 'c_nf_per_km',
      'length_km', 'parallel', 'df', 'scaling', 'max_p_mw', 'min_p_mw', 'max_q_mvar', 'min_q_mvar'];
    const connectionProps = ['bus', 'from_bus', 'to_bus', 'hv_bus', 'lv_bus', 'element', 'et'];
    const skipProps = ['key', 'index'];

    const allProps = Object.entries(selectedElement).filter(([k]) => !skipProps.includes(k));

    // Inject Latitude/Longitude for Buses if available
    let extraProps = [];
    if ((selectedElement.type === 'Bus' || selectedElement.type === 'bus') && nodePositions) {
      const pos = nodePositions[selectedElement.index];
      if (pos) {
        extraProps.push(['Latitude', pos.y]);
        extraProps.push(['Longitude', pos.x]);
      }
    }

    const basic = allProps.filter(([k]) => basicProps.includes(k));
    const electrical = allProps.filter(([k]) => electricalProps.includes(k));
    const connection = allProps.filter(([k]) => connectionProps.includes(k));
    const other = [
      ...allProps.filter(([k]) => !basicProps.includes(k) && !electricalProps.includes(k) && !connectionProps.includes(k) && k !== 'type'),
      ...extraProps
    ];

    const renderPropertyGroup = (title, props, color = '#00d26a') => {
      if (props.length === 0) return null;
      return (
        <div className="property-group">
          <div className="property-group-title" style={{ color }}>{title}</div>
          {props.map(([key, value]) => (
            <div key={key} className="property-row">
              <span className="property-label">{formatLabel(key)}</span>
              <span className="property-value">{formatValue(value)}</span>
            </div>
          ))}
        </div>
      );
    };

    // Get icon color based on element type
    const getTypeColor = () => {
      const typeColors = {
        'External Grid': '#06b6d4',
        'Bus': '#3b82f6',
        'Line': '#3b82f6',
        'Transformer': '#3b82f6',
        'Load': '#ef4444',
        'Generator': '#8b5cf6',
        'Static Generator': '#8b5cf6',
      };
      return typeColors[selectedElement.type] || '#00d26a';
    };

    return (
      <div className="properties-panel">
        <div className="properties-header" style={{ borderLeft: `3px solid ${getTypeColor()}` }}>
          <div className="properties-title">{selectedElement.name || `${selectedElement.type} ${selectedElement.index}`}</div>
          <div className="properties-subtitle">
            <span className="property-badge" style={{ background: getTypeColor() }}>{selectedElement.type}</span>
            <span style={{ marginLeft: 8 }}>Index: {selectedElement.index}</span>
          </div>
        </div>
        <div className="properties-body">
          {renderPropertyGroup('Basic', basic, '#00d26a')}
          {renderPropertyGroup('Connection', connection, '#3b82f6')}
          {renderPropertyGroup('Electrical Parameters', electrical, '#f59e0b')}
          {renderPropertyGroup('Other', other, '#6b7280')}
        </div>
      </div>
    );
  };


  // Render Interactive Network Diagram with Drag Support
  const renderDiagram = () => {
    if (!topology || topology.nodes?.length === 0) {
      return (
        <div className="diagram-placeholder">
          <Network size={64} className="diagram-placeholder-icon" />
          <div>Add buses and elements to visualize the network</div>
        </div>
      );
    }

    // Calculate node positions - use saved positions or auto-layout
    const nodes = topology.nodes.map((node, i) => {
      const savedPos = nodePositions[node.id];
      const baseX = node.x !== undefined ? node.x : (100 + (i % 5) * 150);
      const baseY = node.y !== undefined ? node.y : (100 + Math.floor(i / 5) * 120);
      return {
        ...node,
        x: savedPos?.x ?? baseX,
        y: savedPos?.y ?? baseY
      };
    });

    const getNodePosition = (id) => {
      const savedPos = nodePositions[id];
      if (savedPos) return savedPos;
      const node = nodes.find(n => n.id === id);
      return node || { x: 0, y: 0 };
    };

    const clampScale = (s) => Math.min(3, Math.max(0.4, s));

    const handleWheel = (e) => {
      e.preventDefault();
      const svg = e.currentTarget;
      if (!svg) return;
      const rect = svg.getBoundingClientRect();
      const cx = e.clientX - rect.left;
      const cy = e.clientY - rect.top;
      const scaleFactor = e.deltaY > 0 ? 0.9 : 1.1;

      setViewTransform(prev => {
        const newScale = clampScale(prev.scale * scaleFactor);
        // Zoom towards cursor position
        const worldX = (cx - prev.x) / prev.scale;
        const worldY = (cy - prev.y) / prev.scale;
        const newX = cx - worldX * newScale;
        const newY = cy - worldY * newScale;
        return { scale: newScale, x: newX, y: newY };
      });
    };

    const handlePanStart = (e) => {
      if (e.button !== 0) return; // only left click
      if (e.target.closest('.node-group')) return; // don't start pan when grabbing node
      setIsPanning(true);
      setPanStart({
        x: e.clientX,
        y: e.clientY,
        originX: viewTransform.x,
        originY: viewTransform.y,
      });
    };

    const handleMouseDown = (e, nodeId) => {
      e.preventDefault();
      e.stopPropagation();
      const svg = e.currentTarget.ownerSVGElement || e.currentTarget.closest('svg');
      if (!svg) return;

      const pt = svg.createSVGPoint();
      pt.x = e.clientX;
      pt.y = e.clientY;
      const ctm = svg.getScreenCTM();
      if (!ctm) return;
      const svgP = pt.matrixTransform(ctm.inverse());

      const nodePos = getNodePosition(nodeId);
      setDraggingNode(nodeId);
      setDragOffset({ x: svgP.x - nodePos.x, y: svgP.y - nodePos.y });
      setDragStartPosition({ nodeId, x: nodePos.x, y: nodePos.y }); // Capture for undo
    };

    // Handle node click with Shift/Ctrl modifiers for multi-select
    const handleNodeClick = (e, nodeId, nodeType = 'bus') => {
      e.stopPropagation();
      const key = `${nodeType}_${nodeId}`;

      if (e.shiftKey) {
        // Shift+click: Add to selection
        setSelectedElements(prev => {
          const next = new Set(prev);
          next.add(key);
          return next;
        });
      } else if (e.ctrlKey || e.metaKey) {
        // Ctrl+click: Toggle selection
        setSelectedElements(prev => {
          const next = new Set(prev);
          if (next.has(key)) {
            next.delete(key);
          } else {
            next.add(key);
          }
          return next;
        });
      } else {
        // Regular click: Select only this element
        setSelectedElements(new Set([key]));
        setSelectedElement({ type: nodeType, index: nodeId });
      }
    };

    const handleMouseMove = (e) => {
      if (isPanning && panStart && draggingNode === null) {
        e.preventDefault();
        setViewTransform(prev => ({
          ...prev,
          x: panStart.originX + (e.clientX - panStart.x),
          y: panStart.originY + (e.clientY - panStart.y),
        }));
        return;
      }

      // Handle connection drawing update
      if (drawingConnection) {
        const svg = e.currentTarget;
        if (svg && svg.getScreenCTM) {
          const pt = svg.createSVGPoint();
          pt.x = e.clientX;
          pt.y = e.clientY;
          const ctm = svg.getScreenCTM();
          if (ctm) {
            const svgP = pt.matrixTransform(ctm.inverse());
            handleUpdateConnection(svgP);
          }
        }
        return;
      }

      if (draggingNode === null) return;
      e.preventDefault();

      const svg = e.currentTarget;
      if (!svg || !svg.getScreenCTM) return;
      const bbox = svg.getBoundingClientRect();
      const pad = 200;
      const minX = -pad;
      const maxX = bbox.width + pad;
      const minY = -pad;
      const maxY = bbox.height + pad;

      const pt = svg.createSVGPoint();
      pt.x = e.clientX;
      pt.y = e.clientY;
      const ctm = svg.getScreenCTM();
      if (!ctm) return;
      const svgP = pt.matrixTransform(ctm.inverse());

      // Calculate distance moved
      const dx = svgP.x - (dragStartPosition?.x || 0); // Note: dragStartPosition stores original node pos, not mouse pos. 
      // We need to compare current mouse pos with mouse start pos.
      // But we don't store mouse start pos in state explicitly for drag check, only panStart.
      // Wait, let's use dragOffset to backtrack or just store mouse start.

      // Better approach: Calculate displacement from original node position
      // The new position would be:
      let newX = Math.max(minX, Math.min(svgP.x - dragOffset.x, maxX));
      let newY = Math.max(minY, Math.min(svgP.y - dragOffset.y, maxY));

      // Check if we have moved enough to consider it a drag (prevention of accidental moves on click)
      if (dragStartPosition) {
        const dist = Math.sqrt(Math.pow(newX - dragStartPosition.x, 2) + Math.pow(newY - dragStartPosition.y, 2));
        if (dist < 5) return; // Ignore movements less than 5 units
      }

      // Apply snap-to-grid if enabled
      if (settings.snapToGrid && settings.gridSize > 0) {
        newX = Math.round(newX / settings.gridSize) * settings.gridSize;
        newY = Math.round(newY / settings.gridSize) * settings.gridSize;
      }

      setNodePositions(prev => ({
        ...prev,
        [draggingNode]: { x: newX, y: newY }
      }));

      // Handle edge handle dragging
      if (draggingEdgeHandle) {
        const pt2 = svg.createSVGPoint();
        pt2.x = e.clientX;
        pt2.y = e.clientY;
        const ctm2 = svg.getScreenCTM();
        if (ctm2) {
          const svgP2 = pt2.matrixTransform(ctm2.inverse());

          if (draggingEdgeHandle.type === 'control') {
            setEdgeControlPoints(prev => ({
              ...prev,
              [draggingEdgeHandle.edgeId]: { x: svgP2.x, y: svgP2.y }
            }));
          } else {
            // For Endpoint drag, just update current pos for visual feedback
            setDraggingEdgeHandle(prev => ({
              ...prev,
              currentX: svgP2.x,
              currentY: svgP2.y
            }));
          }
        }
      }
    };

    const handleMouseUp = () => {
      if (draggingNode !== null) {
        // Save new position for this node
        const pos = nodePositions[draggingNode];
        if (pos && currentNetwork?.network_id) {
          api.updateLayout(currentNetwork.network_id, { [draggingNode]: pos }).catch(console.error);
        }

        // Create undo action if position changed
        if (dragStartPosition && dragStartPosition.nodeId === draggingNode) {
          const fromPos = { x: dragStartPosition.x, y: dragStartPosition.y };
          const toPos = pos || fromPos;
          const nodeId = draggingNode;

          // Only create action if position actually changed
          if (fromPos.x !== toPos.x || fromPos.y !== toPos.y) {
            pushAction(createNodeMoveAction(nodeId, fromPos, toPos, setNodePositions));
          }
        }
        setDragStartPosition(null);
      }
      setDraggingNode(null);

      if (draggingEdgeHandle) {
        // TODO: Implement reconnection logic here
        setDraggingEdgeHandle(null);
      }

      setIsPanning(false);
    };

    const handleResetLayout = () => {
      setNodePositions({});
      setViewTransform({ scale: 1, x: 0, y: 0 });
      if (currentNetwork?.network_id) {
        localStorage.removeItem(`node_positions_${currentNetwork.network_id}`);
      }
      showToast('Layout reset to default');
    };

    return diagramViewMode === 'singleLine' ? (
      <div style={{ position: 'relative', width: '100%', height: '100%' }}>
        {/* View Toggle - Top Left */}
        <div style={{
          position: 'absolute',
          top: 8,
          left: 8,
          zIndex: 10,
          display: 'flex',
          gap: 4,
          background: 'rgba(55, 65, 81, 0.9)',
          borderRadius: 6,
          padding: 2
        }}>
          <button
            className={`btn ${diagramViewMode === 'singleLine' ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => setDiagramViewMode('singleLine')}
            style={{ fontSize: '0.75rem', padding: '4px 10px', display: 'flex', alignItems: 'center', gap: 4 }}
            title="Single Line Diagram"
          >
            <List size={12} /> Single Line
          </button>
          <button
            className={`btn ${diagramViewMode === 'spatial' ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => setDiagramViewMode('spatial')}
            style={{ fontSize: '0.75rem', padding: '4px 10px', display: 'flex', alignItems: 'center', gap: 4 }}
            title="Spatial View (Map)"
          >
            <MapIcon size={12} /> Spatial
          </button>
        </div>

        {/* Zoom/Layout Controls - Top Right */}
        <div style={{
          position: 'absolute',
          top: 8,
          right: 8,
          zIndex: 10,
          display: 'flex',
          gap: 8
        }}>

          <div style={{ display: 'flex', gap: 4, alignItems: 'center', background: '#374151', borderRadius: 4, padding: '2px 4px' }}>
            <select
              value={selectedLayoutAlgo}
              onChange={e => setSelectedLayoutAlgo(e.target.value)}
              style={{
                background: 'transparent',
                border: 'none',
                color: '#e5e7eb',
                fontSize: '0.75rem',
                outline: 'none',
                cursor: 'pointer'
              }}
            >
              <option value="radial">Radial</option>
              <option value="mrtree">Mr. Tree</option>
              <option value="force">Force</option>
              <option value="stress">Stress</option>
            </select>
            <button
              className="btn btn-secondary"
              onClick={handleAutoLayout}
              style={{ fontSize: '0.75rem', padding: '4px 8px', height: 24, display: 'flex', alignItems: 'center' }}
              title="Run Layout"
            >
              <Play size={10} />
            </button>
          </div>
          {/* Connect Mode Toggle */}
          <button
            className={`btn ${connectionMode ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => {
              setConnectionMode(prev => !prev);
              if (connectionMode) setDrawingConnection(null);
            }}
            style={{ fontSize: '0.75rem', padding: '4px 8px' }}
            title={connectionMode ? 'Exit Connect Mode (ESC)' : 'Enter Connect Mode - Click bus to start, click another to connect'}
          >
            <GitBranch size={12} />
          </button>


          {/* Layers Toggle */}
          <button
            className={`btn ${showLayersPanel ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => setShowLayersPanel(prev => !prev)}
            style={{ fontSize: '0.75rem', padding: '4px 8px' }}
            title="Toggle Layers Panel"
          >
            <Layers size={12} />
          </button>

          {/* Zoom Controls */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <button
              className="btn btn-secondary"
              onClick={() => setViewTransform({ scale: 1, x: 0, y: 0 })}
              style={{ fontSize: '0.7rem', padding: '4px 8px' }}
              title="Fit to view (100%)"
            >
              <Maximize2 size={12} />
            </button>
            <button
              className="btn btn-secondary"
              onClick={handleResetLayout}
              style={{ fontSize: '0.75rem', padding: '4px 8px' }}
              title="Reset layout to default"
            >
              <RefreshCw size={12} />
            </button>
            <button
              className="btn btn-secondary"
              onClick={() => setViewTransform(prev => ({ ...prev, scale: Math.max(0.1, prev.scale * 0.8) }))}
              style={{ fontSize: '0.75rem', padding: '4px 6px', minWidth: 24 }}
              title="Zoom Out"
            >
              <Minus size={12} />
            </button>
            <span style={{ fontSize: '0.7rem', color: '#9ca3af', minWidth: 40, textAlign: 'center' }}>
              {Math.round(viewTransform.scale * 100)}%
            </span>
            <button
              className="btn btn-secondary"
              onClick={() => setViewTransform(prev => ({ ...prev, scale: Math.min(4, prev.scale * 1.25) }))}
              style={{ fontSize: '0.75rem', padding: '4px 6px', minWidth: 24 }}
              title="Zoom In"
            >
              <Plus size={12} />
            </button>
            <button
              className="btn btn-secondary"
              onClick={handleResetLayout}
              style={{ fontSize: '0.75rem', padding: '4px 8px', display: 'none' }} // Removing duplicate Reset Layout button if any, wait I don't need this line, I already reordered.
              title="Reset layout to default"
            >
            </button>
          </div>
        </div>
        <svg
          className="diagram-canvas"
          width="100%"
          height="100%"
          ref={setSvgRef}
          onWheel={handleWheel}
          onMouseDown={handlePanStart}
          onMouseMove={handleMouseMove}
          onMouseUp={handleMouseUp}
          onMouseLeave={handleMouseUp}
          onDrop={handleCanvasDrop}
          onDragOver={handleCanvasDragOver}
          style={{ cursor: draggingNode !== null ? 'grabbing' : (isPanning ? 'grabbing' : 'grab') }}
        >
          <defs>
            {/* Grid pattern */}
            <pattern
              id="grid-pattern"
              width={settings.gridSize}
              height={settings.gridSize}
              patternUnits="userSpaceOnUse"
            >
              <path
                d={`M ${settings.gridSize} 0 L 0 0 0 ${settings.gridSize}`}
                fill="none"
                stroke="rgba(42, 58, 92, 0.4)"
                strokeWidth="0.5"
              />
            </pattern>
            <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
              <polygon points="0 0, 10 3.5, 0 7" fill="#3b82f6" />
            </marker>
            <filter id="glow">
              <feGaussianBlur stdDeviation="2.5" result="coloredBlur" />
              <feMerge>
                <feMergeNode in="coloredBlur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
            <filter id="neon-glow" x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="3" result="blur" />
              <feFlood floodColor="#00ff00" result="color" />
              <feComposite in="color" in2="blur" operator="in" result="coloredBlur" />
              <feMerge>
                <feMergeNode in="coloredBlur" />
                <feMergeNode in="coloredBlur" /> {/* Double for intensity */}
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
            <radialGradient id="flowGradient">
              <stop offset="0%" stopColor="#00ff00" stopOpacity="1" />
              <stop offset="50%" stopColor="#00ff00" stopOpacity="0.6" />
              <stop offset="100%" stopColor="#00ff00" stopOpacity="0" />
            </radialGradient>

          </defs>

          <g transform={`translate(${viewTransform.x} ${viewTransform.y}) scale(${viewTransform.scale})`}>
            {/* Large invisible background for panning */}
            <rect
              x={-2000}
              y={-2000}
              width={4000}
              height={4000}
              fill="transparent"
            />

            {/* Grid overlay */}
            {settings.gridVisible && (
              <rect
                x={-2000}
                y={-2000}
                width={4000}
                height={4000}
                fill="url(#grid-pattern)"
                pointerEvents="none"
              />
            )}

            {/* Edges */}
            {layerVisibility.lines && topology?.edges?.map(edge => {
              const from = getNodePosition(edge.from);
              const to = getNodePosition(edge.to);

              let hasFlow = false;
              let lineRes = null;
              let trafoRes = null;

              if (analysisResults?.results) {
                if (edge.type === 'line') {
                  const idx = parseInt(edge.id.replace('line_', ''));
                  lineRes = analysisResults.results.res_line?.find(r => (r.index ?? r.line) === idx);
                  const phaseCurrents = ['a', 'b', 'c'].map(s => Math.abs(lineRes?.[`i_ka_${s}`] || 0));
                  const maxPhaseCurrent = Math.max(...phaseCurrents, 0);
                  if (lineRes && (Math.abs(lineRes.loading_percent || 0) > 0.1 || Math.abs(lineRes.i_ka || 0) > 0.001 || maxPhaseCurrent > 0.001)) {
                    hasFlow = true;
                  }
                } else if (edge.type === 'transformer') {
                  const idx = parseInt(edge.id.replace('trafo_', ''));
                  trafoRes = analysisResults.results.res_trafo?.find(r => (r.index ?? r.trafo) === idx);
                }
              }


              const controlPoint = edgeControlPoints[edge.id];
              const pathData = controlPoint
                ? `M ${from.x} ${from.y} Q ${controlPoint.x} ${controlPoint.y} ${to.x} ${to.y}`
                : `M ${from.x} ${from.y} L ${to.x} ${to.y}`;

              const isHovered = hoveredEdge === edge.id;
              const isSelected = selectedElement && selectedElement.type === 'line' && (selectedElement.index === parseInt(edge.id.replace('line_', '')));
              const showHandles = isHovered || isSelected || controlPoint || draggingEdgeHandle?.edgeId === edge.id;
              const phaseCount = Number(edge.phase_count || 3);
              const phasePattern = phaseCount <= 1
                ? '0'
                : phaseCount === 2
                  ? '8 4 1 4'
                  : '8 4 1 4 1 4';

              return (
                <g
                  key={edge.id}
                  onMouseEnter={() => setHoveredEdge(edge.id)}
                  onMouseLeave={() => setHoveredEdge(null)}
                  onDoubleClick={(e) => {
                    e.stopPropagation();
                    const idx = parseInt((edge.id || '').replace(/\D/g, ''));
                    if (!isNaN(idx)) {
                      setSelectedElement({ type: edge.type, index: idx });
                      setPanelStates(prev => ({ ...prev, right: true }));
                    }
                  }}
                >
                  {edge.type === 'line' && (
                    <title>
                      {`Line ${(edge.id || '').replace('line_', '')}\nFrom Bus ${edge.from} -> To Bus ${edge.to}\nLoading: ${lineRes?.loading_percent?.toFixed(1) ?? '-'}%\nCurrent: ${lineRes?.i_ka?.toFixed(2) ?? '-'} kA\nP: ${lineRes?.p_from_mw?.toFixed(2) ?? '-'} MW\nQ: ${lineRes?.q_from_mvar?.toFixed(2) ?? '-'} Mvar\nLength: ${edge.length_km ?? '-'} km`}
                    </title>
                  )}
                  {/* Main Edge Path */}
                  <path
                    d={pathData}
                    fill="none"
                    stroke={getLineColor(edge)}
                    strokeWidth={edge.type === 'transformer' ? 3 : 2}
                    strokeDasharray={edge.in_service ? phasePattern : '5,5'}
                    style={{ cursor: 'pointer' }}
                    onClick={() => {
                      // Select the line
                      const idx = parseInt((edge.id || '').replace(/\D/g, ''));
                      if (!isNaN(idx)) setSelectedElement({ type: edge.type, index: idx });
                    }}
                  />

                  {/* Handles */}
                  {showHandles && (
                    <>
                      {/* Source Handle */}
                      <circle
                        cx={from.x} cy={from.y} r={4} fill="#fff" stroke="#3b82f6" strokeWidth={1}
                        style={{ cursor: 'move' }}
                        onMouseDown={(e) => {
                          e.stopPropagation();
                          setDraggingEdgeHandle({ edgeId: edge.id, type: 'source', startX: from.x, startY: from.y, currentX: from.x, currentY: from.y });
                        }}
                      />
                      {/* Target Handle */}
                      <circle
                        cx={to.x} cy={to.y} r={4} fill="#fff" stroke="#3b82f6" strokeWidth={1}
                        style={{ cursor: 'move' }}
                        onMouseDown={(e) => {
                          e.stopPropagation();
                          setDraggingEdgeHandle({ edgeId: edge.id, type: 'target', startX: to.x, startY: to.y, currentX: to.x, currentY: to.y });
                        }}
                      />
                      {/* Control Point Handle (Midpoint if not set) */}
                      <circle
                        cx={controlPoint ? controlPoint.x : (from.x + to.x) / 2}
                        cy={controlPoint ? controlPoint.y : (from.y + to.y) / 2}
                        r={4} fill="#3b82f6" stroke="#fff" strokeWidth={1}
                        style={{ cursor: 'pointer' }}
                        onMouseDown={(e) => {
                          e.stopPropagation();
                          const startX = controlPoint ? controlPoint.x : (from.x + to.x) / 2;
                          const startY = controlPoint ? controlPoint.y : (from.y + to.y) / 2;
                          setDraggingEdgeHandle({ edgeId: edge.id, type: 'control', startX, startY, currentX: startX, currentY: startY });
                        }}
                      />
                    </>
                  )}

                  {hasFlow && (
                    <ellipse rx="6" ry="2" fill="url(#flowGradient)">
                      <animateMotion
                        dur="1.5s"
                        repeatCount="indefinite"
                        path={pathData}
                        rotate="auto"
                      />
                    </ellipse>
                  )}
                  {edge.type === 'transformer' && (
                    <g
                      transform={`translate(${(from.x + to.x) / 2}, ${(from.y + to.y) / 2})`}
                    >
                      <title>
                        {`Transformer ${(edge.id || '').replace('trafo_', '')}\nHV Bus ${edge.from} -> LV Bus ${edge.to}\nLoading: ${trafoRes?.loading_percent?.toFixed(1) ?? '-'}%\nP HV: ${trafoRes?.p_hv_mw?.toFixed(2) ?? '-'} MW\nQ HV: ${trafoRes?.q_hv_mvar?.toFixed(2) ?? '-'} Mvar`}
                      </title>
                      {/* Transformer Symbol */}
                      {['Standard', 'ANSI'].includes(settings.symbolStandard) ? (
                        /* Standard/ANSI: Zig-Zag Coil (W stacked vertically) */
                        <path
                          d="M -12 -10 L -6 -5 L 0 -10 L 6 -5 L 12 -10 M -12 0 L -6 5 L 0 0 L 6 5 L 12 0"
                          fill="none"
                          stroke="#3b82f6"
                          strokeWidth="2"
                        />
                      ) : (
                        /* IEC: Two Intersecting Circles */
                        <>
                          <circle cx="-5" cy="0" r="8" fill="none" stroke="#3b82f6" strokeWidth="2" />
                          <circle cx="5" cy="0" r="8" fill="none" stroke="#3b82f6" strokeWidth="2" />
                        </>
                      )}
                    </g>
                  )}
                  {edge.type === 'switch' && (
                    <g
                      transform={`translate(${(from.x + to.x) / 2}, ${(from.y + to.y) / 2})`}
                    >
                      <title>Switch {edge.closed ? 'Closed' : 'Open'}</title>
                      {/* Switch Body */}
                      <line x1="-12" y1="0" x2="-6" y2="0" stroke="#ccc" strokeWidth="2" />
                      <line x1="6" y1="0" x2="12" y2="0" stroke="#ccc" strokeWidth="2" />
                      <circle cx="-6" cy="0" r="2" fill="#ccc" />
                      <circle cx="6" cy="0" r="2" fill="#ccc" />

                      {/* Switch Arm */}
                      {edge.closed ? (
                        <line x1="-6" y1="0" x2="6" y2="0" stroke="#00d26a" strokeWidth="2" />
                      ) : (
                        <line x1="-6" y1="0" x2="4" y2="-6" stroke="#ef4444" strokeWidth="2" />
                      )}
                    </g>
                  )}
                  {/* Line Loading Label */}
                  {layerVisibility.labels && settings.showLoading && (
                    <text
                      x={controlPoint ? controlPoint.x : (from.x + to.x) / 2}
                      y={(controlPoint ? controlPoint.y : (from.y + to.y) / 2) - 8}
                      textAnchor="middle"
                      fill={
                        (edge.type === 'line' && lineRes?.loading_percent > 80) ||
                          (edge.type === 'transformer' && trafoRes?.loading_percent > 80)
                          ? '#ef4444' : '#e8e8e8'
                      }
                      fontSize={settings.labelFontSize || 11}
                      fontWeight="bold"
                      style={{ pointerEvents: 'none', userSelect: 'none', textShadow: '0px 1px 2px rgba(0,0,0,0.8)' }}
                    >
                      {edge.type === 'line' && lineRes?.loading_percent !== undefined ? `${lineRes.loading_percent.toFixed(0)}%` : ''}
                      {edge.type === 'transformer' && trafoRes?.loading_percent !== undefined ? `${trafoRes.loading_percent.toFixed(0)}%` : ''}
                    </text>
                  )}
                </g>
              );
            })}

            {/* Nodes */}
            {layerVisibility.buses && topology?.nodes?.map(topoNode => {
              // Try to find rich bus data from currentNetwork, fall back to topology node
              const busData = currentNetwork?.bus?.find(b => b.id === topoNode.id) || topoNode;
              const node = { ...topoNode, ...busData }; // Merge to ensure we have all props

              const pos = getNodePosition(node.id);
              if (!pos) return null;

              // Force visible if selected
              const isSelected = selectedElements.has(`bus_${node.id}`);
              const isDragging = draggingNode === node.id;

              // Determine component visibility
              const showLoad = (node.has_load || busData.has_load) && layerVisibility.loads;
              const hasGenerator = node.has_gen || node.has_sgen || busData.has_gen || busData.has_sgen;
              const hasExtGrid = node.has_ext_grid || busData.has_ext_grid;
              const showGen = (hasGenerator || hasExtGrid) && layerVisibility.generators;

              // Labels visibility
              const showLabels = layerVisibility.labels;

              // Find bus result
              let busRes = null;
              if (analysisResults?.results?.res_bus) {
                busRes = analysisResults.results.res_bus.find(r => (r.index ?? r.bus) === node.id);
              }

              // Use voltage color if result available, else standard type color
              let fillColor = '#ffffff'; // Default white for bus bar
              let strokeColor = isDragging ? '#00d26a' : (node.in_service ? '#00d26a' : '#6b7280');

              if (busRes && busRes.vm_pu !== undefined) {
                // Override fill with voltage color for nicer visualization
                fillColor = colorForVoltage(busRes.vm_pu);
                // Ensure stroke is consistent
                strokeColor = '#ffffff';
              }

              // Removed duplicate declaration


              return (
                <g
                  key={node.id}
                  transform={`translate(${pos.x}, ${pos.y})`}
                  onMouseDown={(e) => !connectionMode && handleMouseDown(e, node.id)}
                  onClick={(e) => {
                    if (connectionMode) {
                      e.stopPropagation();
                      if (drawingConnection) {
                        // Complete the connection
                        handleCompleteConnection(node.id);
                      } else {
                        // Start a new connection
                        handleStartConnection(node.id, pos);
                      }
                    } else {
                      handleNodeClick(e, node.id, 'bus');
                    }
                  }}
                  onDoubleClick={(e) => {
                    e.stopPropagation();
                    // Find bus name/id to select
                    setSelectedElement({ type: 'bus', index: node.id });
                    setPanelStates(prev => ({ ...prev, right: true }));
                  }}
                  style={{ cursor: connectionMode ? (drawingConnection ? 'crosshair' : 'pointer') : (isDragging ? 'grabbing' : 'grab') }}
                  className={`node-group ${isDragging ? 'dragging' : ''} ${isSelected ? 'selected' : ''} ${connectionMode ? 'connection-target' : ''}`}
                  data-testid={`node-${node.id}`}
                >
                  <title>
                    {`Bus ${node.id}\n${node.name || ''}\nVoltage: ${busRes?.vm_pu?.toFixed(3) ?? '-'} p.u.\nVoltage: ${(busRes?.vm_pu * node.vn_kv)?.toFixed(2) ?? '-'} kV\nBase: ${node.vn_kv} kV`}
                  </title>
                  {/* Invisible hitbox for easier grabbing */}
                  <rect
                    x={-25}
                    y={-35}
                    width={50}
                    height={75}
                    fill="transparent"
                    style={{ cursor: 'grab' }}
                  />
                  {/* Bus Bar */}
                  <rect
                    x={-20}
                    y={-3}
                    width={40}
                    height={6}
                    fill={fillColor}
                    stroke={strokeColor}
                    strokeWidth={isDragging ? 2 : 0}
                    filter={isDragging ? 'url(#glow)' : 'none'}
                    rx={1}
                    className="node-shape"
                  />
                  {/* Labels - Top Left */}
                  {/* Labels - Top Left */}
                  {showLabels && (
                    <>
                      {settings.showBusIds && (
                        <text
                          x={-22}
                          y={-5}
                          textAnchor="end"
                          fill="#e8e8e8"
                          fontSize={settings.labelFontSize || 11}
                          style={{ pointerEvents: 'none', userSelect: 'none' }}
                        >
                          {node.name || `Bus ${node.id}`}
                        </text>
                      )}

                      {settings.showVoltage && (
                        <text
                          x={-22}
                          y={settings.showBusIds ? (settings.labelFontSize || 11) - 3 : -5}
                          textAnchor="end"
                          fill={node.has_ext_grid ? '#0f0f1a' : '#e8e8e8'}
                          fontSize={(settings.labelFontSize || 11) - 1}
                          fontWeight="bold"
                          style={{ pointerEvents: 'none', userSelect: 'none' }}
                        >
                          {/* Show Voltage if analysis ran, else Base kV */}
                          {busRes?.vm_pu ? `${(busRes.vm_pu * node.vn_kv).toFixed(2)} kV` : `${node.vn_kv} kV`}
                        </text>
                      )}
                    </>
                  )}
                  {/* Ext Grid Icon at center if present */}
                  {hasExtGrid && layerVisibility.generators && (
                    <g transform="translate(-12, -35)">
                      <title>External Grid</title>
                      <line x1="12" y1="24" x2="12" y2="35" stroke="#ccc" strokeWidth="2" />
                      {/* White Square with Grid Pattern */}
                      <rect x="0" y="0" width="24" height="24" fill="#fff" stroke="#000" strokeWidth="2" />
                      {/* Grid Lines */}
                      <path d="M 6 0 L 6 24 M 12 0 L 12 24 M 18 0 L 18 24 M 0 6 L 24 6 M 0 12 L 24 12 M 0 18 L 24 18" stroke="#ccc" strokeWidth="1" />
                    </g>
                  )}

                  {showLoad && (
                    <g transform="translate(0, 3)">
                      <title>Load</title>
                      {['Standard', 'ANSI'].includes(settings.symbolStandard) ? (
                        /* Standard/ANSI: Resistor Zig-Zag */
                        <>
                          <line x1="0" y1="0" x2="0" y2="5" stroke="#ef4444" strokeWidth="2" />
                          <path
                            d="M 0 5 L -5 8 L 5 13 L -5 18 L 5 23 L 0 25"
                            fill="none"
                            stroke="#ef4444"
                            strokeWidth="2"
                          />
                        </>
                      ) : (
                        /* IEC: Triangle Arrow */
                        <>
                          <line x1="0" y1="0" x2="0" y2="15" stroke="#ef4444" strokeWidth="2" />
                          <polygon points="0,15 -6,25 6,25" fill="#ef4444" stroke="#ef4444" strokeWidth="1" />
                        </>
                      )}
                    </g>
                  )}
                  {hasGenerator && layerVisibility.generators && (
                    <g transform="translate(0, -9)">
                      <title>Generator</title>
                      {/* Connector */}
                      <line x1="15" y1="6" x2="15" y2="-5" stroke="#3b82f6" strokeWidth="2" />

                      {['Standard', 'ANSI'].includes(settings.symbolStandard) ? (
                        /* Standard/ANSI: Circle with G */
                        <>
                          <circle cx="15" cy="-15" r="10" fill="#fff" stroke="#3b82f6" strokeWidth="2" />
                          <text x="15" y="-11" textAnchor="middle" fill="#3b82f6" fontSize="12" fontWeight="bold" dy=".1em" style={{ pointerEvents: 'none', userSelect: 'none' }}>G</text>
                        </>
                      ) : (
                        /* IEC: Circle with Wave */
                        <>
                          <circle cx="15" cy="-15" r="10" fill="#fff" stroke="#3b82f6" strokeWidth="2" />
                          <path d="M 10 -15 Q 12 -19 15 -15 T 20 -15" stroke="#3b82f6" strokeWidth="1" fill="none" />
                        </>
                      )}
                    </g>
                  )}
                </g>
              );
            })}

            {/* Connection Preview Line */}
            {connectionMode && drawingConnection && (
              <line
                x1={drawingConnection.fromPos.x}
                y1={drawingConnection.fromPos.y}
                x2={drawingConnection.currentPos.x}
                y2={drawingConnection.currentPos.y}
                stroke="#00d26a"
                strokeWidth="2"
                strokeDasharray="5,5"
                pointerEvents="none"
              />
            )}
          </g>
        </svg>

        {/* Layers Panel */}
        {showLayersPanel && (
          <LayersPanel
            layers={{ ...layerVisibility, grid: settings.gridVisible }}
            toggleLayer={toggleLayer}
          />
        )}

        {/* Legends */}
        <div style={{
          position: 'absolute',
          bottom: 30,
          right: 20,
          background: 'rgba(15, 23, 42, 0.9)',
          padding: 10,
          borderRadius: 8,
          border: '1px solid #374151',
          display: 'flex',
          flexDirection: 'column',
          gap: 12,
          color: '#e2e8f0',
          fontSize: '0.75rem',
          backdropFilter: 'blur(4px)',
          zIndex: 50
        }}>
          {/* Voltage Legend */}
          <div>
            <div style={{ fontWeight: 'bold', marginBottom: 4 }}>Bus Voltage (p.u.)</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <div style={{ width: 120, height: 8, background: 'linear-gradient(to right, #ef4444 0%, #ffffff 50%, #ef4444 100%)', borderRadius: 2 }}></div>
              <div style={{ display: 'flex', justifyContent: 'space-between', width: '100%', fontSize: '0.65rem', color: '#94a3b8' }}>
                <span>&lt;0.95</span>
                <span style={{ textAlign: 'center' }}>1.0</span>
                <span>&gt;1.05</span>
              </div>
            </div>
          </div>
          {/* Loading Legend */}
          <div>
            <div style={{ fontWeight: 'bold', marginBottom: 4 }}>Line Loading (%)</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <div style={{ width: 120, height: 8, background: 'linear-gradient(to right, #00d26a 0%, #f59e0b 80%, #ef4444 100%)', borderRadius: 2 }}></div>
              <div style={{ display: 'flex', justifyContent: 'space-between', width: '100%', fontSize: '0.65rem', color: '#94a3b8' }}>
                <span>0%</span>
                <span style={{ textAlign: 'center' }}>80%</span>
                <span>100%</span>
              </div>
            </div>
          </div>
        </div>

        <div style={{
          position: 'absolute',
          bottom: 8,
          left: 8,
          fontSize: '0.7rem',
          color: '#6b7280',
          background: 'rgba(0,0,0,0.5)',
          padding: '4px 8px',
          borderRadius: 4
        }}>
          💡 Drag nodes to rearrange • Scroll to zoom • Drag background to pan • Positions auto-saved
        </div>
      </div>
    ) : (
      /* Spatial View - Leaflet Map */
      <div style={{ position: 'relative', width: '100%', height: '100%' }}>
        {/* View Toggle - Top Left */}
        <div style={{
          position: 'absolute',
          top: 8,
          left: 8,
          zIndex: 1000,
          display: 'flex',
          gap: 4,
          background: 'rgba(55, 65, 81, 0.9)',
          borderRadius: 6,
          padding: 2
        }}>
          <button
            className={`btn ${diagramViewMode === 'singleLine' ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => setDiagramViewMode('singleLine')}
            style={{ fontSize: '0.75rem', padding: '4px 10px', display: 'flex', alignItems: 'center', gap: 4 }}
            title="Single Line Diagram"
          >
            <List size={12} /> Single Line
          </button>
          <button
            className={`btn ${diagramViewMode === 'spatial' ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => setDiagramViewMode('spatial')}
            style={{ fontSize: '0.75rem', padding: '4px 10px', display: 'flex', alignItems: 'center', gap: 4 }}
            title="Spatial View (Map)"
          >
            <MapIcon size={12} /> Spatial
          </button>
        </div>

        <MapContainer
          center={(() => {
            const nodesWithGeo = (topology?.nodes || []).filter(n =>
              n.geo_lat !== undefined && n.geo_lon !== undefined
            );
            if (nodesWithGeo.length > 0) {
              const avgLat = nodesWithGeo.reduce((s, n) => s + n.geo_lat, 0) / nodesWithGeo.length;
              const avgLon = nodesWithGeo.reduce((s, n) => s + n.geo_lon, 0) / nodesWithGeo.length;
              return [avgLat, avgLon];
            }
            // Fallback to tcgm layers
            const allTcgmNodes = tcgmLayers.flatMap(g => g.nodes || []);
            if (allTcgmNodes.length > 0) {
              const avgLat = allTcgmNodes.reduce((s, n) => s + n.lat, 0) / allTcgmNodes.length;
              const avgLon = allTcgmNodes.reduce((s, n) => s + n.lon, 0) / allTcgmNodes.length;
              return [avgLat, avgLon];
            }
            return [37.7749, -122.4194]; // Default to San Francisco
          })()}
          zoom={8}
          style={{ width: '100%', height: '100%' }}
          scrollWheelZoom={true}
        >
          <LayersControl position="topright">
            <LayersControl.BaseLayer checked name="OpenStreetMap">
              <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              />
            </LayersControl.BaseLayer>

            {/* Render tcgm networks grouped by groupid */}
            {tcgmLayers.map((grp, idx) => (
              <LayersControl.Overlay
                key={grp.groupid}
                name={`TCGM ${grp.groupid}`}
                checked
              >
                <LayerGroup>
                  {(grp.nodes || []).map((node, jdx) => (
                    <CircleMarker
                      key={`${grp.groupid}-${jdx}-${node.name || jdx}`}
                      center={[node.lat, node.lon]}
                      radius={7}
                      pathOptions={{
                        fillColor: `hsl(${(idx * 47) % 360}, 70%, 55%)`,
                        fillOpacity: 0.85,
                        color: '#1f2937',
                        weight: 1.5
                      }}
                    >
                      <Popup>
                        <div style={{ fontFamily: 'Inter, sans-serif' }}>
                          <strong>{node.name || 'Node'}</strong><br />
                          Group: {grp.groupid}<br />
                          Voltage: {node.nominal_voltage || 'N/A'}<br />
                          Phases: {node.phases || 'N/A'}<br />
                          Lat/Lon: {node.lat?.toFixed(6)}, {node.lon?.toFixed(6)}
                        </div>
                      </Popup>
                    </CircleMarker>
                  ))}
                </LayerGroup>
              </LayersControl.Overlay>
            ))}

            {/* Render current network nodes as an overlay */}
            <LayersControl.Overlay name="Current Network" checked>
              <LayerGroup>
                {(topology?.nodes || []).filter(n => n.geo_lat !== undefined && n.geo_lon !== undefined).map(node => (
                  <CircleMarker
                    key={node.id}
                    center={[node.geo_lat, node.geo_lon]}
                    radius={node.has_ext_grid ? 12 : node.has_load ? 10 : 8}
                    pathOptions={{
                      fillColor: node.has_ext_grid ? '#06b6d4' : node.has_load ? '#ef4444' : node.has_gen ? '#8b5cf6' : '#00d26a',
                      fillOpacity: 0.8,
                      color: '#ffffff',
                      weight: 2
                    }}
                  >
                    <Popup>
                      <div style={{ fontFamily: 'Inter, sans-serif' }}>
                        <strong>{node.name || `Bus ${node.id}`}</strong><br />
                        Voltage: {node.vn_kv} kV<br />
                        {node.has_ext_grid && <span style={{ color: '#06b6d4' }}>⚡ External Grid</span>}<br />
                        {node.has_load && <span style={{ color: '#ef4444' }}>🔌 Load</span>}<br />
                        {node.has_gen && <span style={{ color: '#8b5cf6' }}>⚙️ Generator</span>}
                      </div>
                    </Popup>
                  </CircleMarker>
                ))}
              </LayerGroup>
            </LayersControl.Overlay>
          </LayersControl>

          {/* ArcGIS feature layers (toggled via map UI) */}
          <EsriFeatureLayers showTransmission={showTransmissionLayer} showSubstations={showSubstationLayer} />
        </MapContainer>

        {/* Map layer toggles (ArcGIS) */}
        <div style={{
          position: 'absolute',
          top: 12,
          right: 12,
          zIndex: 1200,
          background: 'rgba(17,24,39,0.8)',
          color: '#e5e7eb',
          padding: '8px 10px',
          borderRadius: 8,
          fontSize: '0.8rem',
          minWidth: 180,
          boxShadow: '0 8px 16px rgba(0,0,0,0.25)',
          backdropFilter: 'blur(4px)'
        }}>
          <div style={{ fontWeight: 600, marginBottom: 6 }}>Layers</div>
          <label style={{ display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer', marginBottom: 4 }}>
            <input
              type="checkbox"
              checked={showTransmissionLayer}
              onChange={e => setShowTransmissionLayer(e.target.checked)}
            />
            <span>US Transmission Lines</span>
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer' }}>
            <input
              type="checkbox"
              checked={showSubstationLayer}
              onChange={e => setShowSubstationLayer(e.target.checked)}
            />
            <span>Electric Substations</span>
          </label>
        </div>

        {/* Info message for nodes without geo data */}
        {(() => {
          const nodesWithGeo = (topology?.nodes || []).filter(n => n.geo_lat !== undefined && n.geo_lon !== undefined);
          const totalNodes = (topology?.nodes || []).length;
          if (nodesWithGeo.length === 0 && totalNodes > 0) {
            return (
              <div style={{
                position: 'absolute',
                bottom: 60,
                left: '50%',
                transform: 'translateX(-50%)',
                zIndex: 1000,
                background: 'rgba(245, 158, 11, 0.95)',
                color: '#1a1a2e',
                padding: '8px 16px',
                borderRadius: 8,
                fontSize: '0.85rem'
              }}>
                ⚠️ No geographic coordinates available for this network. Switch to Single Line view.
              </div>
            );
          }
          return null;
        })()}

        <div style={{
          position: 'absolute',
          bottom: 8,
          left: 8,
          fontSize: '0.7rem',
          color: '#6b7280',
          background: 'rgba(0,0,0,0.5)',
          padding: '4px 8px',
          borderRadius: 4,
          zIndex: 1000
        }}>
          🗺️ Spatial view • Click markers for details • Nodes require geo_lat/geo_lon data
        </div>
      </div>
    );
  };

  // Modals
  const renderModal = () => {
    if (!modal) return null;

    switch (modal) {
      case 'create':
        return (
          <Modal title="Create New Network" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="form-group">
                <label>Network Name</label>
                <input
                  type="text"
                  id="network-name"
                  defaultValue="New Network"
                  style={{ width: '100%' }}
                  data-testid="network-name-input"
                />
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                onClick={() => handleCreateNetwork(document.getElementById('network-name').value)}
                data-testid="create-network-btn"
              >
                Create Network
              </button>
            </div>
          </Modal>
        );

      case 'samples':
        return (
          <Modal title="Load Sample Network" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="samples-list">
                {samples.map(sample => (
                  <div
                    key={sample.id}
                    className="sample-item"
                    onClick={() => handleLoadSample(sample.id)}
                    data-testid={`sample-${sample.id}`}
                  >
                    <div className="sample-name">{sample.name}</div>
                    <div className="sample-desc">{sample.description}</div>
                  </div>
                ))}
              </div>
            </div>
          </Modal>
        );

      case 'powerflow':
        return (
          <Modal title="Power Flow Analysis" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="analysis-options">
                <div className="analysis-option">
                  <label>Algorithm</label>
                  <select id="pf-algorithm" defaultValue="nr">
                    <option value="nr">Newton-Raphson</option>
                    <option value="bfsw">Backward/Forward Sweep</option>
                    <option value="gs">Gauss-Seidel</option>
                    <option value="fdbx">Fast Decoupled BX</option>
                    <option value="fdxb">Fast Decoupled XB</option>
                  </select>
                </div>
                <div className="analysis-option">
                  <label>Initialization</label>
                  <select id="pf-init" defaultValue="auto">
                    <option value="auto">Auto</option>
                    <option value="flat">Flat Start</option>
                    <option value="dc">DC</option>
                    <option value="results">Previous Results</option>
                  </select>
                </div>
                <div className="analysis-option">
                  <label>Max Iterations</label>
                  <input type="number" id="pf-maxiter" defaultValue={50} min={1} max={500} />
                </div>
                <div className="analysis-option">
                  <label>Tolerance (MVA)</label>
                  <input type="number" id="pf-tol" defaultValue={1e-8} step="1e-9" />
                </div>
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                onClick={() => handleRunPowerFlow({
                  algorithm: document.getElementById('pf-algorithm').value,
                  init: document.getElementById('pf-init').value,
                  max_iteration: parseInt(document.getElementById('pf-maxiter').value),
                  tolerance_mva: parseFloat(document.getElementById('pf-tol').value),
                })}
                disabled={isAnalyzing}
                data-testid="run-powerflow-btn"
              >
                {isAnalyzing ? <><Loader size={14} className="spinner" /> Running...</> : <><Play size={14} /> Run Analysis</>}
              </button>
            </div>
          </Modal>
        );

      case 'load-allocation':
        return (
          <Modal title="Load Allocation" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="analysis-options">
                <div className="analysis-option">
                  <label>Tolerance (%)</label>
                  <input type="number" id="la-tolerance" defaultValue={0.5} step="0.1" />
                </div>
                <div className="analysis-option">
                  <label>Max Iterations</label>
                  <input type="number" id="la-maxiter" defaultValue={8} min={1} max={50} />
                </div>
                <div className="analysis-option">
                  <label>Measurement Indices (comma)</label>
                  <input type="text" id="la-measurements" placeholder="e.g. 0,1,2 (optional)" />
                </div>
                <div className="analysis-option">
                  <label>
                    <input type="checkbox" id="la-adjust-after" defaultChecked /> Adjust After Load Flow
                  </label>
                </div>
                <div className="analysis-option">
                  <label>
                    <input type="checkbox" id="la-ignore-gens" /> Ignore Generators
                  </label>
                </div>
                <div className="analysis-option">
                  <label>
                    <input type="checkbox" id="la-ignore-fixed" /> Ignore Fixed Capacitors
                  </label>
                </div>
                <div className="analysis-option">
                  <label>
                    <input type="checkbox" id="la-ignore-controlled" /> Ignore Controlled Capacitors
                  </label>
                </div>
                <div className="analysis-option">
                  <label>
                    <input type="checkbox" id="la-cap-load" /> Cap to Load Rating
                  </label>
                </div>
                <div className="analysis-option">
                  <label>
                    <input type="checkbox" id="la-cap-trafo" /> Cap to Transformer Rating
                  </label>
                </div>
                <div className="analysis-option">
                  <label>Transformer Overload Factor</label>
                  <input type="number" id="la-trafo-factor" defaultValue={1.3} step="0.1" />
                </div>
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                onClick={() => {
                  const raw = document.getElementById('la-measurements').value.trim();
                  const measurement_indices = raw
                    ? raw.split(',').map(v => parseInt(v.trim(), 10)).filter(v => !isNaN(v))
                    : null;
                  handleRunLoadAllocation({
                    tolerance: parseFloat(document.getElementById('la-tolerance').value),
                    max_iter: parseInt(document.getElementById('la-maxiter').value, 10),
                    measurement_indices,
                    adjust_after_load_flow: document.getElementById('la-adjust-after').checked,
                    ignore_generators: document.getElementById('la-ignore-gens').checked,
                    ignore_fixed_capacitors: document.getElementById('la-ignore-fixed').checked,
                    ignore_controlled_capacitors: document.getElementById('la-ignore-controlled').checked,
                    cap_to_load_rating: document.getElementById('la-cap-load').checked,
                    cap_to_transformer_rating: document.getElementById('la-cap-trafo').checked,
                    trafo_overload_factor: parseFloat(document.getElementById('la-trafo-factor').value)
                  });
                }}
                disabled={isAnalyzing}
              >
                {isAnalyzing ? <><Loader size={14} className="spinner" /> Running...</> : <><Play size={14} /> Run Allocation</>}
              </button>
            </div>
          </Modal>
        );

      case 'shortcircuit':
        return (
          <Modal title="Short Circuit Analysis" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="analysis-options">
                <div className="analysis-option">
                  <label>Fault Type</label>
                  <select id="sc-fault" defaultValue="3ph">
                    <option value="3ph">Three-Phase</option>
                    <option value="2ph">Two-Phase</option>
                    <option value="1ph">Single-Phase</option>
                  </select>
                </div>
                <div className="analysis-option">
                  <label>Case</label>
                  <select id="sc-case" defaultValue="max">
                    <option value="max">Maximum</option>
                    <option value="min">Minimum</option>
                  </select>
                </div>
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                onClick={() => handleRunShortCircuit({
                  fault: document.getElementById('sc-fault').value,
                  case: document.getElementById('sc-case').value,
                })}
                disabled={isAnalyzing}
              >
                {isAnalyzing ? <><Loader size={14} /> Running...</> : <><Play size={14} /> Run Analysis</>}
              </button>
            </div>
          </Modal>
        );

      case 'hosting':
        return (
          <Modal title="Hosting Capacity Analysis" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="analysis-options">
                <div className="analysis-option">
                  <label>Iterations</label>
                  <input type="number" id="hc-iterations" defaultValue={50} min={5} max={500} />
                </div>
                <div className="analysis-option">
                  <label>Voltage Limit (pu)</label>
                  <input type="number" step="0.01" id="hc-voltage" defaultValue={1.04} />
                </div>
                <div className="analysis-option">
                  <label>Loading Limit (%)</label>
                  <input type="number" step="1" id="hc-loading" defaultValue={50} />
                </div>
                <div className="analysis-option">
                  <label>Plant Mean (MW)</label>
                  <input type="number" step="0.01" id="hc-mean" defaultValue={0.5} />
                </div>
                <div className="analysis-option">
                  <label>Plant Std Dev (MW)</label>
                  <input type="number" step="0.01" id="hc-std" defaultValue={0.05} />
                </div>
                <div className="analysis-option">
                  <label>Random Seed (optional)</label>
                  <input type="number" id="hc-seed" placeholder="e.g. 42" />
                </div>
              </div>
              <p style={{ fontSize: '0.85rem', color: '#6b7280', marginTop: 12 }}>
                Uses your current network if selected; otherwise runs on the built-in MV Oberrhein example.
              </p>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                onClick={() => handleRunHostingCapacity({
                  iterations: parseInt(document.getElementById('hc-iterations').value),
                  voltage_limit: parseFloat(document.getElementById('hc-voltage').value),
                  loading_limit: parseFloat(document.getElementById('hc-loading').value),
                  plant_mean_mw: parseFloat(document.getElementById('hc-mean').value),
                  plant_std_mw: parseFloat(document.getElementById('hc-std').value),
                  seed: document.getElementById('hc-seed').value ? parseInt(document.getElementById('hc-seed').value) : null,
                })}
                disabled={isAnalyzing}
              >
                {isAnalyzing ? <><Loader size={14} /> Running...</> : <><Play size={14} /> Run Analysis</>}
              </button>
            </div>
          </Modal>
        );

      case 'timeseries':
        return (
          <Modal title="Run Time Series" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="form-row">
                <div className="form-group">
                  <label>Timesteps</label>
                  <input type="number" id="ts-steps" defaultValue={24} min={1} max={168} />
                </div>
                <div className="form-group">
                  <label>Seed (optional)</label>
                  <input type="number" id="ts-seed" placeholder="e.g. 42" />
                </div>
              </div>
              <p style={{ fontSize: '0.85rem', color: '#6b7280', marginTop: 8 }}>
                Generates load and solar-like profiles (see backend time_series.ipynb) for your current network and runs power flow at each step.
              </p>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                onClick={() => handleRunTimeSeries({
                  timesteps: parseInt(document.getElementById('ts-steps').value || '24', 10),
                  seed: document.getElementById('ts-seed').value ? parseInt(document.getElementById('ts-seed').value, 10) : null,
                })}
                disabled={isAnalyzing}
              >
                {isAnalyzing ? <><Loader size={14} /> Running...</> : <><Play size={14} /> Run Simulation</>}
              </button>
            </div>
          </Modal>
        );

      case 'add-bus':
        return (
          <Modal title="Add Bus" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="form-group">
                <label>Name</label>
                <input type="text" id="bus-name" placeholder="Bus name" style={{ width: '100%' }} />
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Voltage (kV)</label>
                  <input type="number" id="bus-vn" defaultValue={20} min={0.1} step={0.1} />
                </div>
                <div className="form-group">
                  <label>Type</label>
                  <select id="bus-type" defaultValue="b">
                    <option value="b">PQ Bus</option>
                    <option value="n">Auxiliary</option>
                  </select>
                </div>
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                onClick={() => handleAddElement('bus', {
                  vn_kv: parseFloat(document.getElementById('bus-vn').value),
                  name: document.getElementById('bus-name').value || null,
                  type: document.getElementById('bus-type').value,
                })}
                data-testid="add-bus-btn"
              >
                Add Bus
              </button>
            </div>
          </Modal>
        );

      case 'add-line':
        return (
          <Modal title="Add Line" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="form-group">
                <label>Name</label>
                <input type="text" id="line-name" placeholder="Line name" style={{ width: '100%' }} />
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>From Bus</label>
                  <select id="line-from">
                    {elements.buses.map(bus => (
                      <option key={bus.index} value={bus.index}>{bus.name || `Bus ${bus.index}`}</option>
                    ))}
                  </select>
                </div>
                <div className="form-group">
                  <label>To Bus</label>
                  <select id="line-to">
                    {elements.buses.map(bus => (
                      <option key={bus.index} value={bus.index}>{bus.name || `Bus ${bus.index}`}</option>
                    ))}
                  </select>
                </div>
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Length (km)</label>
                  <input type="number" id="line-length" defaultValue={1} min={0.001} step={0.1} />
                </div>
                <div className="form-group">
                  <label>Standard Type</label>
                  <select id="line-type">
                    <option value="">Custom Parameters</option>
                    {lineTypes.slice(0, 30).map(t => (
                      <option key={t} value={t}>{t}</option>
                    ))}
                  </select>
                </div>
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                onClick={() => handleAddElement('line', {
                  from_bus: parseInt(document.getElementById('line-from').value),
                  to_bus: parseInt(document.getElementById('line-to').value),
                  length_km: parseFloat(document.getElementById('line-length').value),
                  std_type: document.getElementById('line-type').value || null,
                  name: document.getElementById('line-name').value || null,
                  r_ohm_per_km: 0.1,
                  x_ohm_per_km: 0.1,
                  c_nf_per_km: 0,
                  max_i_ka: 1.0,
                })}
                data-testid="add-line-btn"
              >
                Add Line
              </button>
            </div>
          </Modal>
        );

      case 'add-load':
        return (
          <Modal title="Add Load" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="form-group">
                <label>Name</label>
                <input type="text" id="load-name" placeholder="Load name" style={{ width: '100%' }} />
              </div>
              <div className="form-group">
                <label>Bus</label>
                <select id="load-bus" style={{ width: '100%' }}>
                  {elements.buses.map(bus => (
                    <option key={bus.index} value={bus.index}>{bus.name || `Bus ${bus.index}`}</option>
                  ))}
                </select>
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Active Power (MW)</label>
                  <input type="number" id="load-p" defaultValue={1} step={0.1} />
                </div>
                <div className="form-group">
                  <label>Reactive Power (Mvar)</label>
                  <input type="number" id="load-q" defaultValue={0} step={0.1} />
                </div>
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                onClick={() => handleAddElement('load', {
                  bus: parseInt(document.getElementById('load-bus').value),
                  p_mw: parseFloat(document.getElementById('load-p').value),
                  q_mvar: parseFloat(document.getElementById('load-q').value),
                  name: document.getElementById('load-name').value || null,
                })}
                data-testid="add-load-btn"
              >
                Add Load
              </button>
            </div>
          </Modal>
        );

      case 'add-extgrid':
        return (
          <Modal title="Add External Grid" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="form-group">
                <label>Name</label>
                <input type="text" id="eg-name" placeholder="External grid name" style={{ width: '100%' }} />
              </div>
              <div className="form-group">
                <label>Bus</label>
                <select id="eg-bus" style={{ width: '100%' }}>
                  {elements.buses.map(bus => (
                    <option key={bus.index} value={bus.index}>{bus.name || `Bus ${bus.index}`}</option>
                  ))}
                </select>
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Voltage (p.u.)</label>
                  <input type="number" id="eg-vm" defaultValue={1.0} step={0.01} min={0.8} max={1.2} />
                </div>
                <div className="form-group">
                  <label>Angle (deg)</label>
                  <input type="number" id="eg-va" defaultValue={0} step={1} />
                </div>
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                onClick={() => handleAddElement('ext_grid', {
                  bus: parseInt(document.getElementById('eg-bus').value),
                  vm_pu: parseFloat(document.getElementById('eg-vm').value),
                  va_degree: parseFloat(document.getElementById('eg-va').value),
                  name: document.getElementById('eg-name').value || null,
                })}
                data-testid="add-extgrid-btn"
              >
                Add External Grid
              </button>
            </div>
          </Modal>
        );

      case 'add-measurement': {
        const measurementItems =
          measurementElementType === 'line'
            ? elements.lines
            : measurementElementType === 'trafo1ph'
              ? elements.transformers
              : elements.extGrids;
        const sideOptions =
          measurementElementType === 'line'
            ? ['from', 'to']
            : measurementElementType === 'trafo1ph'
              ? ['hv', 'lv']
              : [];
        const hasElements = measurementItems && measurementItems.length > 0;
        return (
          <Modal title="Add Measurement" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="form-group">
                <label>Name</label>
                <input type="text" id="meas-name" placeholder="Measurement name" style={{ width: '100%' }} />
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Measurement Type</label>
                  <select id="meas-type" defaultValue="p">
                    <option value="p">Active Power (p)</option>
                    <option value="q">Reactive Power (q)</option>
                    <option value="i">Current (i)</option>
                    <option value="s">Apparent Power (s)</option>
                    <option value="pf">Power Factor (pf)</option>
                  </select>
                </div>
                <div className="form-group">
                  <label>Element Type</label>
                  <select
                    id="meas-element-type"
                    value={measurementElementType}
                    onChange={(e) => setMeasurementElementType(e.target.value)}
                  >
                    <option value="line">Line</option>
                    <option value="trafo1ph">Transformer (1ph)</option>
                    <option value="ext_grid">External Grid</option>
                  </select>
                </div>
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Element</label>
                  <select id="meas-element" disabled={!hasElements}>
                    {hasElements ? (
                      measurementItems.map(item => (
                        <option key={item.index} value={item.index}>
                          {item.name || `${measurementElementType} ${item.index}`}
                        </option>
                      ))
                    ) : (
                      <option value="">No elements available</option>
                    )}
                  </select>
                </div>
                <div className="form-group">
                  <label>Side</label>
                  <select id="meas-side" defaultValue={sideOptions[0] || ''} disabled={sideOptions.length === 0}>
                    {sideOptions.length > 0 ? (
                      sideOptions.map(side => (
                        <option key={side} value={side}>{side}</option>
                      ))
                    ) : (
                      <option value="">N/A</option>
                    )}
                  </select>
                </div>
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Value</label>
                  <input type="number" id="meas-value" defaultValue={1.0} step={0.01} />
                </div>
                <div className="form-group">
                  <label>Std Dev</label>
                  <input type="number" id="meas-stddev" defaultValue={0.01} step={0.001} min={0} />
                </div>
              </div>
              <p style={{ fontSize: '0.85rem', color: '#6b7280', marginTop: 8 }}>
                Measurements are used by load allocation to adjust downstream loads to match observed values.
              </p>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                onClick={() => handleGenericAddElement('measurement', {
                  name: document.getElementById('meas-name').value || null,
                  measurement_type: document.getElementById('meas-type').value,
                  element_type: document.getElementById('meas-element-type').value,
                  element: parseInt(document.getElementById('meas-element').value, 10),
                  value: parseFloat(document.getElementById('meas-value').value),
                  std_dev: parseFloat(document.getElementById('meas-stddev').value || '0'),
                  side: sideOptions.length > 0 ? document.getElementById('meas-side').value : null,
                })}
                disabled={!hasElements}
              >
                Add Measurement
              </button>
            </div>
          </Modal>
        );
      }

      case 'import':
        return (
          <Modal title="Import Network" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="form-group">
                <label>Network Name</label>
                <input type="text" id="import-name" placeholder="Imported Network" style={{ width: '100%' }} />
              </div>
              <div className="form-group">
                <label>JSON Data</label>
                <textarea
                  id="import-data"
                  rows={10}
                  placeholder='Paste exported network JSON here...'
                  style={{ width: '100%', background: '#0f0f1a', border: '1px solid #2a3a5c', color: '#e8e8e8', padding: 12, borderRadius: 4, fontFamily: 'monospace', fontSize: '0.8rem' }}
                />
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                onClick={async () => {
                  try {
                    const name = document.getElementById('import-name').value || 'Imported Network';
                    const data = JSON.parse(document.getElementById('import-data').value);
                    const result = await api.importNetwork({ name, network_data: data });
                    const newNetwork = { network_id: result.network_id, name, bus_count: 0, line_count: 0, load_count: 0, gen_count: 0 };
                    setNetworks(prev => [...prev, newNetwork]);
                    setCurrentNetwork(newNetwork);
                    showToast('Network imported successfully');
                    setModal(null);
                  } catch (e) {
                    showToast('Invalid JSON data', 'error');
                  }
                }}
              >
                Import
              </button>
            </div>
          </Modal>

        );

      case 'save-version':
        return (
          <Modal title="Save Version" onClose={() => setModal(null)}>
            <div className="modal-body">
              <div className="form-group">
                <label>Description (Optional)</label>
                <input type="text" id="save-desc" placeholder="e.g. Added transformer" style={{ width: '100%', padding: 8, background: '#0f172a', border: '1px solid #2a3a5c', color: 'white', borderRadius: 4 }} />
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setModal(null)}>Cancel</button>
              <button className="btn btn-primary" onClick={() => handleSaveVersion(document.getElementById('save-desc').value)}>Save</button>
            </div>
          </Modal>
        );

      case 'history':
        return (
          <HistoryModal networkId={currentNetwork.network_id} onClose={() => setModal(null)} onLoadVersion={handleLoadVersion} />
        );

      case 'settings':
        return <SettingsModal onClose={() => setModal(null)} />;

      default:
        return null;
    }
  };

  const totalElements = Object.values(elementCounts).reduce((a, b) => a + b, 0);

  // Helpers for tables (sorting/filtering)
  const getTableState = (key) => tableStates[key] || { sort: { column: null, dir: 'asc' }, filter: '' };
  const updateTableState = (key, updates) => {
    setTableStates(prev => ({ ...prev, [key]: { ...getTableState(key), ...updates } }));
  };

  const orderedColumns = (data) => {
    if (!data || data.length === 0) return [];
    const cols = Object.keys(data[0]);
    const colSet = new Set(cols);
    const idPriority = ['bus', 'line', 'load', 'gen', 'sgen', 'ext_grid', 'trafo', 'trafo3w', 'switch', 'index', 'id'];
    const phaseMatch = (c) => c.match(/^(.+)_([abc])$/);

    const baseOrder = [];
    cols.forEach((col) => {
      const match = phaseMatch(col);
      const base = match ? match[1] : col;
      if (!baseOrder.includes(base)) baseOrder.push(base);
    });

    const ordered = [];
    if (colSet.has('timestep')) ordered.push('timestep');
    idPriority.forEach((col) => {
      if (colSet.has(col)) ordered.push(col);
    });

    baseOrder.forEach((base) => {
      if (idPriority.includes(base) || base === 'timestep') return;
      if (colSet.has(base)) ordered.push(base);
      ['a', 'b', 'c'].forEach((suffix) => {
        const phased = `${base}_${suffix}`;
        if (colSet.has(phased)) ordered.push(phased);
      });
    });

    cols.forEach((col) => {
      if (!ordered.includes(col)) ordered.push(col);
    });

    return ordered;
  };

  const formatCell = (col, val) => {
    const idLike = ['bus', 'line', 'load', 'gen', 'sgen', 'ext_grid', 'trafo', 'trafo3w', 'switch', 'index', 'id'];
    if (col === 'timestep' && val !== undefined && val !== null) return parseInt(val, 10);
    if (idLike.includes(col) && val !== undefined && val !== null) return parseInt(val, 10);
    if (typeof val === 'number') return val.toFixed(4);
    return String(val ?? '-');
  };

  const renderDataTable = (data, tabKey) => {
    const columns = orderedColumns(data);
    const state = getTableState(tabKey);
    const filter = (state.filter || '').toLowerCase();
    const hasPhaseColumns = columns.some(col => /_([abc])$/.test(col));
    const showPhaseOnly = hasPhaseColumns && state.phaseOnly;
    const phaseColumns = columns.filter(col => col === 'timestep' || /^(bus|line|load|gen|sgen|ext_grid|trafo|trafo3w|switch|index|id)$/.test(col) || /_([abc])$/.test(col));
    const visibleColumns = showPhaseOnly ? phaseColumns : columns;

    let rows = data || [];
    if (filter) {
      rows = rows.filter(row =>
        columns.some(col => String(row[col] ?? '').toLowerCase().includes(filter))
      );
    }

    if (state.sort.column) {
      const { column, dir } = state.sort;
      rows = [...rows].sort((a, b) => {
        const av = a[column];
        const bv = b[column];
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        if (!isNaN(av) && !isNaN(bv)) {
          return dir === 'asc' ? av - bv : bv - av;
        }
        return dir === 'asc'
          ? String(av).localeCompare(String(bv))
          : String(bv).localeCompare(String(av));
      });
    }

    return (
      <>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8, alignItems: 'center' }}>
          <input
            type="text"
            placeholder="Filter rows..."
            value={state.filter || ''}
            onChange={(e) => updateTableState(tabKey, { filter: e.target.value })}
            style={{ width: 200, padding: '6px 8px', background: '#0f172a', border: '1px solid #1f2937', color: '#e5e7eb', borderRadius: 4, fontSize: '0.85rem' }}
          />
          {hasPhaseColumns ? (
            <button
              type="button"
              onClick={() => updateTableState(tabKey, { phaseOnly: !state.phaseOnly })}
              style={{
                padding: '6px 10px',
                background: state.phaseOnly ? '#1d4ed8' : '#0f172a',
                border: '1px solid #1f2937',
                color: '#e5e7eb',
                borderRadius: 4,
                fontSize: '0.8rem'
              }}
            >
              {state.phaseOnly ? 'Show All Columns' : 'Show Phase Columns'}
            </button>
          ) : null}
          {data?.length ? (
            <span style={{ fontSize: '0.8rem', color: '#9ca3af' }}>{rows.length} / {data.length} rows</span>
          ) : null}
        </div>
        {rows.length > 0 ? (
          <table className="data-table">
            <thead>
              <tr>
                {visibleColumns.map(col => (
                  <th
                    key={col}
                    onClick={() => {
                      const dir = state.sort.column === col && state.sort.dir === 'asc' ? 'desc' : 'asc';
                      updateTableState(tabKey, { sort: { column: col, dir } });
                    }}
                    style={{ cursor: 'pointer' }}
                  >
                    {col}{state.sort.column === col ? (state.sort.dir === 'asc' ? ' ▲' : ' ▼') : ''}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, idx) => (
                <tr key={idx}>
                  {visibleColumns.map(col => (
                    <td key={col}>{formatCell(col, row[col])}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div style={{ padding: 20, textAlign: 'center', color: '#6b7280' }}>No data available</div>
        )}
      </>
    );
  };

  const renderTimeSeriesChart = (results) => {
    const sources = [
      { key: 'res_bus', label: 'Bus Voltages' },
      { key: 'res_line', label: 'Line Loading' },
      { key: 'res_load', label: 'Load Results' },
      { key: 'res_sgen', label: 'SGen Results' },
      { key: 'res_ext_grid', label: 'External Grid' },
    ].filter(s => results[s.key]?.length);

    if (sources.length === 0) {
      return <div style={{ padding: 12, color: '#6b7280' }}>No result series available.</div>;
    }

    const sourceKey = sources.some(s => s.key === tsChartSource) ? tsChartSource : sources[0].key;
    const activeSource = sources.find(s => s.key === sourceKey) || sources[0];
    const data = results[activeSource.key] || [];
    if (!data.length) return <div style={{ padding: 12, color: '#6b7280' }}>No data for selected source.</div>;

    const numericFields = Object.keys(data[0]).filter(col => col !== 'timestep' && typeof data[0][col] === 'number');
    if (numericFields.length === 0) {
      return <div style={{ padding: 12, color: '#6b7280' }}>No numeric fields to chart.</div>;
    }

    const field = tsChartField && numericFields.includes(tsChartField) ? tsChartField : numericFields[0];

    const idKeyMap = {
      res_bus: 'bus',
      res_line: 'line',
      res_load: 'load',
      res_sgen: 'sgen',
      res_ext_grid: 'ext_grid'
    };
    const idKey = idKeyMap[activeSource.key] || 'id';

    const grouped = {};
    data.forEach(d => {
      const idVal = d[idKey];
      if (typeof d.timestep === 'undefined' || typeof d[field] !== 'number' || idVal === undefined) return;
      const g = grouped[idVal] || (grouped[idVal] = []);
      g.push({ x: parseInt(d.timestep, 10), y: d[field] });
    });

    const series = Object.entries(grouped).map(([id, pts]) => ({
      id,
      points: pts.sort((a, b) => a.x - b.x)
    }));

    if (series.length === 0) return <div style={{ padding: 12, color: '#6b7280' }}>No numeric points for chart.</div>;

    const width = chartSize.w;
    const height = chartSize.h;
    const padding = { left: 50, right: 20, top: 20, bottom: 36 };
    const xs = series.flatMap(s => s.points.map(p => p.x));
    const ys = series.flatMap(s => s.points.map(p => p.y));
    const xMin = Math.min(...xs);
    const xMax = Math.max(...xs);
    const yMin = Math.min(...ys);
    const yMax = Math.max(...ys);
    const xScale = (x) => padding.left + (width - padding.left - padding.right) * ((x - xMin) / (xMax - xMin || 1));
    const yScale = (y) => height - padding.bottom - (height - padding.top - padding.bottom) * ((y - yMin) / (yMax - yMin || 1));
    const palette = ['#3b82f6', '#00d26a', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#e11d48', '#22d3ee', '#a855f7', '#f97316'];
    const colorFor = (id) => palette[Math.abs(parseInt(id, 10) || 0) % palette.length] || '#3b82f6';


    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginBottom: 12 }}>
          <div>
            <label style={{ fontSize: '0.8rem', color: '#9ca3af' }}>Series</label><br />
            <select
              value={sourceKey}
              onChange={e => { setTsChartSource(e.target.value); setTsChartField(null); }}
              style={{ background: '#0f172a', color: '#e5e7eb', border: '1px solid #1f2937', padding: '6px 8px', borderRadius: 4 }}
            >
              {sources.map(s => <option key={s.key} value={s.key}>{s.label}</option>)}
            </select>
          </div>
          <div>
            <label style={{ fontSize: '0.8rem', color: '#9ca3af' }}>Variable</label><br />
            <select
              value={field}
              onChange={e => setTsChartField(e.target.value)}
              style={{ background: '#0f172a', color: '#e5e7eb', border: '1px solid #1f2937', padding: '6px 8px', borderRadius: 4 }}
            >
              {numericFields.map(f => <option key={f} value={f}>{f}</option>)}
            </select>
          </div>
          <div>
            <label style={{ fontSize: '0.8rem', color: '#9ca3af' }}>Chart</label><br />
            <select
              value={tsChartType}
              onChange={e => setTsChartType(e.target.value)}
              style={{ background: '#0f172a', color: '#e5e7eb', border: '1px solid #1f2937', padding: '6px 8px', borderRadius: 4 }}
            >
              <option value="line">Line</option>
              <option value="scatter">Scatter</option>
              <option value="fill">Area</option>
            </select>
          </div>
          <div style={{ fontSize: '0.85rem', color: '#9ca3af' }}>
            Series: {series.length} &nbsp;|&nbsp; Min: {yMin.toFixed(4)} Max: {yMax.toFixed(4)}
          </div>
        </div>
        <div ref={chartRef} style={{ flex: 1, minHeight: 260 }}>
          <svg
            width="100%"
            height="100%"
            viewBox={`0 0 ${width} ${height}`}
            preserveAspectRatio="none"
            style={{ background: '#0b1224', borderRadius: 8 }}
          >
            {/* Axes */}
            <line x1={padding.left} y1={height - padding.bottom} x2={width - padding.right} y2={height - padding.bottom} stroke="#1f2937" />
            <line x1={padding.left} y1={padding.top} x2={padding.left} y2={height - padding.bottom} stroke="#1f2937" />
            {/* Series */}
            {series.map((s, idx) => {
              const path = s.points.map((p, i) => `${i === 0 ? 'M' : 'L'}${xScale(p.x)},${yScale(p.y)}`).join(' ');
              const areaPath = `${path} L ${xScale(s.points[s.points.length - 1].x)},${height - padding.bottom} L ${xScale(s.points[0].x)},${height - padding.bottom} Z`;
              const color = colorFor(s.id);
              return (
                <g key={idx}>
                  {tsChartType === 'fill' && <path d={areaPath} fill={`${color}30`} stroke="none" />}
                  {tsChartType !== 'scatter' && <path d={path} fill="none" stroke={color} strokeWidth="2" />}
                  {(tsChartType === 'scatter' || tsChartType === 'fill') && s.points.map((p, i) => (
                    <circle key={i} cx={xScale(p.x)} cy={yScale(p.y)} r={3} fill={color} />
                  ))}
                </g>
              );
            })}
            {/* Labels */}
            <text x={width / 2} y={height - 6} fill="#9ca3af" fontSize="10" textAnchor="middle">timestep</text>
            <text x={12} y={padding.top} fill="#9ca3af" fontSize="10">{field}</text>
          </svg>
        </div>
      </div>
    );
  };
  // Close renderTimeSeriesChart

  return (
    <div className="app-container">
      {/* Header */}
      <header className="app-header">
        <div className="logo">
          <div className="logo-icon">🐼</div>
          <span>Panda<span style={{ color: '#00d26a' }}>Power</span></span>
        </div>

        <nav className="header-nav">
          <button className="nav-btn active">
            <Home size={16} /> Home
          </button>
          <button className="nav-btn" onClick={() => currentNetwork && setModal('load-allocation')} disabled={!currentNetwork}>
            <Activity size={16} /> Load Allocation
          </button>
          <button className="nav-btn" onClick={() => currentNetwork && setModal('powerflow')} disabled={!currentNetwork}>
            <Play size={16} /> Power Flow
          </button>
          <button className="nav-btn" onClick={() => currentNetwork && setModal('shortcircuit')} disabled={!currentNetwork}>
            <Zap size={16} /> Short Circuit
          </button>
          <button className="nav-btn" onClick={() => currentNetwork && setModal('timeseries')} disabled={!currentNetwork}>
            <Gauge size={16} /> Time Series
          </button>
          <button className="nav-btn" onClick={() => currentNetwork && setModal('hosting')} disabled={!currentNetwork}>
            <Gauge size={16} /> Hosting Capacity
          </button>


          {/* Scenarios / Versions */}
          {currentNetwork && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, margin: '0 8px' }}>
              <div style={{ width: 1, height: 24, background: '#374151', margin: '0 4px' }}></div>
              <History size={16} color="#9ca3af" />
              <select
                value={selectedVersionId || 'latest'}
                onChange={(e) => {
                  const ver = e.target.value;
                  if (ver === 'latest') {
                    loadNetworkData(currentNetwork.network_id); // Reload current head
                  } else {
                    handleLoadVersion(ver);
                  }
                  setSelectedVersionId(ver);
                }}
                className="select-input"
                style={{ height: '32px', maxWidth: '160px', borderColor: '#374151', background: '#0f172a', color: '#e5e7eb', fontSize: '0.8rem', padding: '0 8px', borderRadius: 4 }}
              >
                <option value="latest">Base Case (Latest)</option>
                {Array.isArray(networkHistory) && networkHistory.map(ver => (
                  <option key={ver.id} value={ver.id}>
                    {ver.description || `Version ${ver.id.substring(0, 6)}`}
                  </option>
                ))}
              </select>
              <button
                className="icon-btn"
                title="Save Scenario / Version"
                style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: '#9ca3af', padding: 4 }}
                onClick={() => {
                  const desc = prompt("Enter scenario description (e.g. 'N-1 Line 5'):");
                  if (desc) handleSaveVersion(desc);
                }}
              >
                <Save size={16} />
              </button>
            </div>
          )}
        </nav>

        <div className="header-actions">
          <button
            className="icon-btn"
            onClick={() => setModal('settings')}
            title="Settings"
            style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: '#9ca3af', padding: 8 }}
          >
            <Settings size={18} />
          </button>
        </div>
      </header>

      {/* Main Content */}
      <div className="main-content">
        {/* Sidebar - Networks List */}
        {/* Sidebar - Networks List */}
        <aside className={`sidebar ${panelStates.sidebar ? '' : 'collapsed'}`}>
          <div className="sidebar-section">
            <div className="sidebar-header">SCE Hierarchy</div>
            <div className="sidebar-content sce-tree-scroll">
              {sceTreeLoading ? (
                <div className="sce-tree-status">Loading sce.csv…</div>
              ) : sceTreeError ? (
                <div className="sce-tree-status error">{sceTreeError}</div>
              ) : (
                renderSceTree(sceTree)
              )}
            </div>
          </div>

          <div className="sidebar-section">
            <div className="sidebar-header" onClick={() => togglePanel('networksPane')} style={{ cursor: 'pointer' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                {panelStates.networksPane ? <ChevronLeft size={14} /> : <ChevronRight size={14} />}
                <span>Networks</span>
              </div>
              <button className="btn-icon" onClick={(e) => {
                e.stopPropagation();
                api.listNetworks().then(data => {
                  setNetworks(data.networks || []);
                });
              }} title="Refresh">
                <RefreshCw size={14} />
              </button>
            </div>
            {panelStates.networksPane && (
              <div className="sidebar-content" style={{ maxHeight: 260, overflow: 'auto' }}>
                {networks.length === 0 ? (
                  <div style={{ padding: 16, color: '#6b7280', fontSize: '0.875rem', textAlign: 'center' }}>
                    No networks yet.<br />Create or load one to start.
                  </div>
                ) : (
                  networks.map(net => (
                    <div
                      key={net.network_id}
                      className={`network-item ${currentNetwork?.network_id === net.network_id ? 'active' : ''}`}
                      onClick={() => setCurrentNetwork(net)}
                      data-testid={`network-item-${net.network_id}`}
                    >
                      <div className="network-icon">
                        <Network size={16} style={{ color: '#00d26a' }} />
                      </div>
                      <div className="network-info">
                        <div className="network-name">{net.name}</div>
                        <div className="network-meta">{net.bus_count || 0} buses • {net.line_count || 0} lines</div>
                      </div>
                      <button
                        className="btn-icon"
                        onClick={(e) => { e.stopPropagation(); handleDeleteNetwork(net.network_id); }}
                        title="Delete"
                      >
                        <Trash2 size={14} />
                      </button>
                    </div>
                  ))
                )}
              </div>
            )}
          </div>

          {/* Quick Stats */}
          {statistics && (
            <div className="sidebar-section">
              <div className="sidebar-header">Statistics</div>
              <div style={{ padding: '8px 16px', fontSize: '0.8125rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                  <span style={{ color: '#6b7280' }}>Total Load</span>
                  <span style={{ color: '#ef4444' }}>{statistics.total_load_mw?.toFixed(2) || 0} MW</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                  <span style={{ color: '#6b7280' }}>Total Gen</span>
                  <span style={{ color: '#00d26a' }}>{((statistics.total_gen_mw || 0) + (statistics.total_sgen_mw || 0)).toFixed(2)} MW</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ color: '#6b7280' }}>Voltage Levels</span>
                  <span>{statistics.voltage_levels?.length || 0}</span>
                </div>
              </div>
            </div>
          )}
        </aside>

        {/* Collapsed Sidebar Toggle */}
        {!panelStates.sidebar && (
          <div
            className="panel-toggle sidebar-toggle"
            onClick={() => togglePanel('sidebar')}
            title="Show Networks Panel"
          >
            <ChevronRight size={16} />
          </div>
        )}

        {/* Work Area */}
        <div className="work-area">
          {!currentNetwork ? renderWelcome() : (
            <>
              {/* Toolbar */}
              <div className="toolbar">
                {/* New Network and Samples buttons */}
                <div className="toolbar-group">
                  <button className="toolbar-btn" onClick={() => setModal('create')} title="New Network" data-testid="toolbar-create-network">
                    <Plus size={16} />
                  </button>
                  <button className="toolbar-btn" onClick={() => {
                    api.listSamples().then(data => setSamples(data.samples || []));
                    setModal('samples');
                  }} title="Load Sample" data-testid="toolbar-load-sample">
                    <Database size={16} />
                  </button>
                </div>
                {/* Divider */}
                <div style={{ width: 1, height: 24, background: '#374151', margin: '0 4px' }}></div>
                <div className="toolbar-group">
                  <button className="toolbar-btn" onClick={() => setModal('add-bus')} title="Add Bus" data-testid="toolbar-add-bus">
                    <Circle size={16} />
                  </button>
                  <button className="toolbar-btn" onClick={() => setModal('add-line')} title="Add Line" disabled={elements.buses.length < 2} data-testid="toolbar-add-line">
                    <Activity size={16} />
                  </button>
                  <button className="toolbar-btn" onClick={() => setModal('add-load')} title="Add Load" disabled={elements.buses.length < 1} data-testid="toolbar-add-load">
                    <Zap size={16} />
                  </button>
                  <button className="toolbar-btn" onClick={() => setModal('add-extgrid')} title="Add External Grid" disabled={elements.buses.length < 1} data-testid="toolbar-add-extgrid">
                    <Grid size={16} />
                  </button>
                </div>
                <div className="toolbar-group">
                  <button
                    className="toolbar-btn"
                    onClick={handleAutoLayout}
                    title="Auto Layout (Radial)"
                    disabled={!currentNetwork}
                  >
                    <Layout size={16} />
                  </button>
                </div>
                <div className="toolbar-group">
                  <button
                    className="toolbar-btn"
                    onClick={() => setModal('load-allocation')}
                    title="Run Load Allocation"
                    disabled={elements.buses.length < 1}
                  >
                    <Activity size={16} />
                  </button>
                  <button
                    className="toolbar-btn"
                    onClick={() => setModal('powerflow')}
                    title="Run Power Flow"
                    disabled={elements.buses.length < 1 || elements.extGrids.length < 1}
                    data-testid="toolbar-powerflow"
                  >
                    <Play size={16} />
                  </button>
                  <button
                    className="toolbar-btn"
                    onClick={() => setModal('shortcircuit')}
                    title="Run Short Circuit"
                    disabled={elements.buses.length < 1 || elements.extGrids.length < 1}
                  >
                    <AlertTriangle size={16} />
                  </button>
                  <button
                    className="toolbar-btn"
                    onClick={() => setModal('timeseries')}
                    title="Run Time Series"
                    disabled={elements.buses.length < 1 || (elements.loads.length < 1 && elements.staticGens.length < 1)}
                  >
                    <Gauge size={16} />
                  </button>
                  <button
                    className="toolbar-btn"
                    onClick={() => setModal('hosting')}
                    title="Run Hosting Capacity"
                  >
                    <BarChart2 size={16} />
                  </button>
                </div>
                <div className="toolbar-group">
                  <button
                    className="toolbar-btn"
                    onClick={async () => {
                      const data = await api.exportNetwork(currentNetwork.network_id);
                      const blob = new Blob([JSON.stringify(data.data, null, 2)], { type: 'application/json' });
                      const url = URL.createObjectURL(blob);
                      const a = document.createElement('a');
                      a.href = url;
                      a.download = `${currentNetwork.name || 'network'}.json`;
                      a.click();
                      showToast('Network exported');
                    }}
                    title="Export Network"
                  >
                    <Download size={16} />
                  </button>
                  <button
                    className="toolbar-btn"
                    onClick={() => setModal('save-version')}
                    title="Save Version"
                  >
                    <Save size={16} />
                  </button>
                  <button
                    className="toolbar-btn"
                    onClick={() => setModal('history')}
                    title="Version History"
                  >
                    <History size={16} />
                  </button>
                  <button className="toolbar-btn" onClick={() => loadNetworkData(currentNetwork.network_id)} title="Refresh">
                    <RefreshCw size={16} />
                  </button>
                </div>
              </div>

              {/* Content Area */}
              <div className="content-area">
                {/* Left Panel - Element Tree */}
                <div className={`left-panel ${panelStates.left ? '' : 'collapsed'}`}>
                  <div className="panel-header" onClick={() => togglePanel('left')} style={{ cursor: 'pointer' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      {panelStates.left ? <ChevronLeft size={14} /> : <ChevronRight size={14} />}
                      <span>Network Elements</span>
                    </div>
                    <span style={{ fontSize: '0.75rem', color: '#6b7280' }}>
                      {totalElements} total
                    </span>
                  </div>
                  {panelStates.left && renderElementTree()}
                </div>

                {/* Collapsed Left Panel Toggle */}
                {!panelStates.left && (
                  <div
                    className="panel-toggle left"
                    onClick={() => togglePanel('left')}
                    title="Show Elements Panel"
                  >
                    <ChevronRight size={16} />
                  </div>
                )}

                {/* Center Panel - Diagram & Results */}
                <div className="center-panel">
                  <div className={`diagram-area ${panelStates.bottom ? '' : 'expanded'}`} data-testid="network-diagram">
                    {/* Map Tools Palette - Floating overlay on diagram */}
                    <MapToolsPalette
                      activeTool="pointer"
                      viewMode={diagramViewMode}
                      onToggleViewMode={() => setDiagramViewMode(prev => prev === 'singleLine' ? 'spatial' : 'singleLine')}
                      collapsed={false}
                    />
                    {renderDiagram()}
                  </div>
                  {panelStates.bottom && (
                    <div
                      className={`results-resizer ${isResizingResults ? 'active' : ''}`}
                      onMouseDown={startResizeResults}
                      title="Drag to resize results"
                    />
                  )}

                  {/* Bottom Results Panel */}
                  <div
                    className={`results-area ${panelStates.bottom ? '' : 'collapsed'}`}
                    style={{ height: panelStates.bottom ? `${resultsHeight}px` : '36px' }}
                  >
                    <div
                      className="results-header"
                      onClick={() => togglePanel('bottom')}
                      style={{ cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '8px 16px', background: '#16213e', borderBottom: '1px solid #2a3a5c' }}
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        {panelStates.bottom ? <ChevronDown size={14} /> : <ChevronUp size={14} />}
                        <BarChart2 size={14} />
                        <span style={{ fontWeight: 500 }}>Analysis Results</span>
                      </div>
                      {analysisResults?.results?.converged !== undefined && (
                        <span className={`converged-badge ${analysisResults.results.converged ? 'success' : 'error'}`} style={{ fontSize: '0.7rem' }}>
                          {analysisResults.results.converged ? <CheckCircle size={10} /> : <XCircle size={10} />}
                          {analysisResults.results.converged ? 'Converged' : 'Failed'}
                        </span>
                      )}
                    </div>
                    {panelStates.bottom && (
                      <ResultsPanel
                        analysisResults={analysisResults}
                        activeResultTab={activeResultTab}
                        setActiveResultTab={setActiveResultTab}
                        renderDataTable={renderDataTable}
                        renderTimeSeriesChart={renderTimeSeriesChart}
                      />
                    )}
                  </div>
                </div>

                {/* Right Panel - Properties */}
                <div className={`right-panel ${panelStates.right ? '' : 'collapsed'}`}>
                  <div className="panel-header" onClick={() => togglePanel('right')} style={{ cursor: 'pointer' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      {panelStates.right ? <ChevronRight size={14} /> : <ChevronLeft size={14} />}
                      <span>Properties</span>
                    </div>
                    {selectedElement && (
                      <span style={{ fontSize: '0.7rem', color: '#00d26a' }}>{selectedElement.type}</span>
                    )}
                  </div>
                  {panelStates.right && renderProperties()}
                </div>

                {/* Collapsed Right Panel Toggle */}
                {!panelStates.right && (
                  <div
                    className="panel-toggle right"
                    onClick={() => togglePanel('right')}
                    title="Show Properties Panel"
                  >
                    <ChevronLeft size={16} />
                  </div>
                )}
              </div>
            </>
          )}
        </div>
      </div>

      {/* Status Bar */}
      <footer className="status-bar">
        <div className="status-item">
          <span className={`status-indicator ${apiStatus}`}></span>
          API: {apiStatus === 'connected' ? 'Connected' : apiStatus === 'connecting' ? 'Connecting...' : 'Disconnected'}
        </div>
        {currentNetwork && (
          <>
            <div className="status-item">Network: {currentNetwork.name}</div>
            <div className="status-item">Elements: {totalElements}</div>
          </>
        )}
        {isAnalyzing && (
          <div className="status-item" style={{ marginLeft: 'auto' }}>
            <span className="status-indicator processing"></span>
            Running analysis...
          </div>
        )}
      </footer>

      {/* Modals */}
      {renderModal()}

      {/* Toast */}
      {toast && <Toast {...toast} onClose={() => setToast(null)} />}
    </div >
  );
}

// Wrap App with SettingsProvider
function AppWithProvider() {
  return (
    <SettingsProvider>
      <App />
    </SettingsProvider>
  );
}

export default AppWithProvider;
