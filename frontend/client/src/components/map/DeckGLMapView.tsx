import { useEffect, useMemo, useState, useCallback } from "react";
import DeckGL from "@deck.gl/react";
import { Map as MaplibreMap } from "react-map-gl/maplibre";
import { ScatterplotLayer, PathLayer } from "@deck.gl/layers";
import { MapView } from "@deck.gl/core";
import type { NetworkElement } from "@shared/schema";
import "maplibre-gl/dist/maplibre-gl.css";

/* ── colour palette (matches SpatialMapView & 3‑D flow) ───────── */

const elementColor: Record<NetworkElement["type"], [number, number, number]> = {
  external_source: [245, 158, 11],
  bus:             [14, 165, 233],
  line:            [16, 185, 129],
  transformer:     [250, 204, 21],
  load:            [239, 68, 68],
  generator:       [214, 198, 165],
  battery:         [6, 182, 212],
  capacitor:       [255, 92, 168],
  switch:          [255, 255, 255],
  cable:           [20, 184, 166],
  ductbank:        [217, 119, 6],
};

const elementRadius: Record<NetworkElement["type"], number> = {
  external_source: 18,
  bus:             6,
  line:            4,
  transformer:     14,
  load:            12,
  generator:       14,
  battery:         12,
  capacitor:       12,
  switch:          10,
  cable:           4,
  ductbank:        10,
};

const DARK_BASEMAP  = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";
const LIGHT_BASEMAP = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";

type GeoPoint = { lat: number; lon: number };

/* ── helpers ───────────────────────────────────────────────────── */

function resolveGeoData(elements: NetworkElement[]) {
  const busLocations = new Map<string, GeoPoint>();
  const directLocations = new Map<string, GeoPoint>();

  for (const el of elements) {
    if (!Number.isFinite(el.geoLat) || !Number.isFinite(el.geoLon)) continue;
    const point = { lat: el.geoLat as number, lon: el.geoLon as number };
    directLocations.set(el.id, point);
    if (el.type === "bus") busLocations.set(el.id, point);
  }

  const getPoint = (id?: string) => {
    if (!id) return null;
    return busLocations.get(id) ?? directLocations.get(id) ?? null;
  };

  const mid = (a: GeoPoint, b: GeoPoint): GeoPoint => ({
    lat: (a.lat + b.lat) / 2,
    lon: (a.lon + b.lon) / 2,
  });

  const resolvePoint = (el: NetworkElement): GeoPoint | null => {
    if (Number.isFinite(el.geoLat) && Number.isFinite(el.geoLon))
      return { lat: el.geoLat as number, lon: el.geoLon as number };
    if (el.type === "bus") return null;
    const connectedBusId = (el as { connectedBusId?: string }).connectedBusId;
    const c = getPoint(connectedBusId);
    if (c) return c;
    const fromBusId = (el as { fromBusId?: string }).fromBusId;
    const toBusId = (el as { toBusId?: string }).toBusId;
    const fb = getPoint(fromBusId);
    const tb = getPoint(toBusId);
    if (fb && tb) return mid(fb, tb);
    if (fb || tb) return (fb ?? tb)!;
    const fromElementId = (el as { fromElementId?: string }).fromElementId;
    const toElementId = (el as { toElementId?: string }).toElementId;
    const fe = getPoint(fromElementId);
    const te = getPoint(toElementId);
    if (fe && te) return mid(fe, te);
    if (fe || te) return (fe ?? te)!;
    return null;
  };

  type NodeDatum  = { id: string; name: string; type: NetworkElement["type"]; position: [number, number]; color: [number, number, number]; radius: number };
  type EdgeDatum  = { id: string; type: NetworkElement["type"]; path: [number, number][] };

  const nodes: NodeDatum[] = [];
  const edges: EdgeDatum[] = [];

  for (const el of elements) {
    if (el.type === "line" || el.type === "cable") {
      const fromBusId = (el as { fromBusId?: string }).fromBusId;
      const toBusId = (el as { toBusId?: string }).toBusId;
      const fromElementId = (el as { fromElementId?: string }).fromElementId;
      const toElementId = (el as { toElementId?: string }).toElementId;
      const start = getPoint(fromBusId) ?? getPoint(fromElementId);
      const end   = getPoint(toBusId)   ?? getPoint(toElementId);
      if (start && end) {
        edges.push({
          id: el.id,
          type: el.type,
          path: [[start.lon, start.lat], [end.lon, end.lat]],
        });
      }
      continue;
    }

    const pt = resolvePoint(el);
    if (!pt) continue;

    nodes.push({
      id: el.id,
      name: el.name,
      type: el.type,
      position: [pt.lon, pt.lat],
      color: elementColor[el.type] ?? [120, 120, 120],
      radius: elementRadius[el.type] ?? 8,
    });
  }

  return { nodes, edges };
}

/* ── initial bounds → view state ───────────────────────────────── */

function computeInitialView(
  nodes: { position: [number, number] }[],
  edges: { path: [number, number][] }[],
) {
  const lons: number[] = [];
  const lats: number[] = [];
  for (const n of nodes) { lons.push(n.position[0]); lats.push(n.position[1]); }
  for (const e of edges) for (const p of e.path) { lons.push(p[0]); lats.push(p[1]); }

  if (lons.length === 0) {
    return { longitude: -118.25, latitude: 34.05, zoom: 12, pitch: 45, bearing: 0 };
  }

  const minLon = Math.min(...lons), maxLon = Math.max(...lons);
  const minLat = Math.min(...lats), maxLat = Math.max(...lats);
  const cLon = (minLon + maxLon) / 2;
  const cLat = (minLat + maxLat) / 2;

  const dLon = maxLon - minLon;
  const dLat = maxLat - minLat;
  const span = Math.max(dLon, dLat, 0.0005);

  // rough heuristic: smaller span → higher zoom
  const zoom = Math.min(20, Math.max(10, Math.round(-Math.log2(span) + 9)));

  return { longitude: cLon, latitude: cLat, zoom, pitch: 45, bearing: -20 };
}

/* ── component ─────────────────────────────────────────────────── */

interface DeckGLMapViewProps {
  elements: NetworkElement[];
}

const MAP_VIEW = new MapView({ id: "main", controller: true });

export function DeckGLMapView({ elements }: DeckGLMapViewProps) {
  const [isDark, setIsDark] = useState(true);

  useEffect(() => {
    const update = () => setIsDark(document.documentElement.classList.contains("dark"));
    update();
    const obs = new MutationObserver(update);
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    return () => obs.disconnect();
  }, []);

  const { nodes, edges } = useMemo(() => resolveGeoData(elements), [elements]);
  const initialViewState = useMemo(() => computeInitialView(nodes, edges), [nodes, edges]);

  const [viewState, setViewState] = useState(initialViewState);

  // Reset view when elements change
  useEffect(() => {
    setViewState(initialViewState);
  }, [initialViewState]);

  const onViewStateChange = useCallback(({ viewState: vs }: { viewState: any }) => {
    setViewState(vs);
  }, []);

  const layers = useMemo(() => [
    new PathLayer({
      id: "network-edges",
      data: edges,
      getPath: (d: (typeof edges)[0]) => d.path,
      getColor: (d: (typeof edges)[0]) => [...(elementColor[d.type] ?? [16, 185, 129]), 200] as [number, number, number, number],
      getWidth: 3,
      widthUnits: "pixels",
      widthMinPixels: 1,
      widthMaxPixels: 6,
      jointRounded: true,
      capRounded: true,
      pickable: true,
    }),
    new ScatterplotLayer({
      id: "network-nodes",
      data: nodes,
      getPosition: (d: (typeof nodes)[0]) => d.position,
      getFillColor: (d: (typeof nodes)[0]) => [...d.color, 230] as [number, number, number, number],
      getLineColor: [255, 255, 255, 80],
      getRadius: (d: (typeof nodes)[0]) => d.radius,
      radiusUnits: "pixels",
      radiusMinPixels: 3,
      radiusMaxPixels: 20,
      lineWidthMinPixels: 1,
      stroked: true,
      pickable: true,
    }),
  ], [nodes, edges]);

  const getTooltip = useCallback(({ object }: { object?: any }) => {
    if (!object) return null;
    if (object.name) return { text: `${object.name} (${object.type})` };
    if (object.id) return { text: `${object.type}: ${object.id}` };
    return null;
  }, []);

  return (
    <div className="absolute inset-0 w-full h-full">
      <DeckGL
        views={MAP_VIEW}
        viewState={viewState}
        onViewStateChange={onViewStateChange}
        layers={layers}
        controller={{ touchRotate: true, inertia: true }}
        getTooltip={getTooltip}
        style={{ position: "absolute", inset: "0" }}
      >
        <MaplibreMap
          mapStyle={isDark ? DARK_BASEMAP : LIGHT_BASEMAP}
          attributionControl={false}
        />
      </DeckGL>

      {/* Legend – glass pane */}
      <div className="absolute bottom-4 left-4 z-50 pointer-events-auto glass-pane p-3 flex flex-col gap-1.5 text-xs">
        <span className="text-white/60 font-semibold mb-0.5">Network Legend</span>
        {(["external_source", "bus", "line", "transformer", "load", "generator", "capacitor", "switch"] as const).map((t) => (
          <div key={t} className="flex items-center gap-2">
            <span
              className="inline-block w-2.5 h-2.5 rounded-full"
              style={{
                backgroundColor: `rgb(${elementColor[t].join(",")})`,
                boxShadow: `0 0 6px rgb(${elementColor[t].join(",")})`,
              }}
            />
            <span className="text-white/80 capitalize">{t.replace("_", " ")}</span>
          </div>
        ))}
      </div>

      {/* Controls – glass pane */}
      <div className="absolute top-4 right-4 z-50 pointer-events-auto glass-pane px-4 py-2 flex items-center gap-3 text-xs text-white/70">
        <span>Drag to pan · Right-drag to rotate · Scroll to zoom · Ctrl+drag to tilt</span>
      </div>
    </div>
  );
}
