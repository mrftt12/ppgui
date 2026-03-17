import { useEffect, useMemo, useState } from "react";
import { CircleMarker, MapContainer, Marker, Polyline, TileLayer, Tooltip, useMap } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import type { NetworkElement } from "@shared/schema";
import L from "leaflet";
import { elementLabels } from "@/components/ElementIcons";

type GeoPoint = { lat: number; lon: number };
type ElementGeo = {
  id: string;
  name: string;
  type: NetworkElement["type"];
  point: GeoPoint;
  isClosed?: boolean;
  sourceType?: string;
  angleDeg?: number;
  pinnedToLine?: boolean;
};

const geoOffsetByType: Partial<Record<NetworkElement["type"], { lat: number; lon: number }>> = {
  load: { lat: -0.00034, lon: 0.0 },
  generator: { lat: -0.00018, lon: 0.00008 },
  capacitor: { lat: 0.00016, lon: -0.0001 },
  switch: { lat: -0.00012, lon: -0.00008 },
  transformer: { lat: 0.0, lon: 0.0 },
  external_source: { lat: 0.00022, lon: 0.0 },
};

const hashId = (id: string) => {
  let hash = 0;
  for (let i = 0; i < id.length; i += 1) {
    hash = (hash * 31 + id.charCodeAt(i)) | 0;
  }
  return Math.abs(hash);
};

const offsetGeoPoint = (point: GeoPoint, type: NetworkElement["type"], id: string): GeoPoint => {
  const base = geoOffsetByType[type];
  if (!base) return point;
  const hash = hashId(id);
  const ring = (hash % 3) + 1;
  const angle = ((hash % 360) * Math.PI) / 180;
  const jitterLat = Math.sin(angle) * 0.00003 * ring;
  const jitterLon = Math.cos(angle) * 0.00003 * ring;
  return {
    lat: point.lat + base.lat + jitterLat,
    lon: point.lon + base.lon + jitterLon,
  };
};


const elementColor: Record<NetworkElement["type"], string> = {
  external_source: "#f59e0b",
  bus: "#0ea5e9",
  line: "#10b981",
  transformer: "#facc15",
  load: "#ef4444",
  generator: "#d6c6a5",
  battery: "#06b6d4",
  capacitor: "#ff5ca8",
  switch: "#ffffff",
  cable: "#14b8a6",
  ductbank: "#d97706",
};

const markerRadius: Record<NetworkElement["type"], number> = {
  external_source: 6,
  bus: 4,
  line: 3,
  transformer: 6,
  load: 6,
  generator: 6,
  battery: 6,
  capacitor: 6,
  switch: 6,
  cable: 3,
  ductbank: 6,
};

const symbolMarkerTypes = new Set<NetworkElement["type"]>([
  "load",
  "generator",
  "transformer",
  "capacitor",
  "switch",
  "external_source",
  "battery",
  "ductbank",
]);

const symbolSizeByType: Partial<Record<NetworkElement["type"], number>> = {
  transformer: 24,
  bus: 18,
  load: 30,
  generator: 30,
  capacitor: 30,
  switch: 28,
  external_source: 30,
  battery: 30,
  ductbank: 26,
};

const symbolScaleByType: Partial<Record<NetworkElement["type"], number>> = {
  load: 2.2,
  generator: 2.0,
  transformer: 2.2,
  capacitor: 2.0,
  switch: 2.1,
  external_source: 2.0,
  battery: 2.0,
  ductbank: 2.0,
};

const svgBasePaths = [
  "/svg/Asset Symbology without BG",
  "/svg/Assets Symbology",
];

const buildSvgPath = (basePath: string, filename: string) => {
  const baseParts = basePath.split("/").filter(Boolean).map(encodeURIComponent);
  return `/${[...baseParts, encodeURIComponent(filename)].join("/")}`;
};

const resolveSvgPaths = (filename: string) => {
  return svgBasePaths.map((basePath) => buildSvgPath(basePath, filename));
};

const resolveSvgFileName = (item: ElementGeo): string | null => {
  switch (item.type) {
    case "load":
      return null;
    case "generator":
      return null;
    case "transformer":
      return "OH_TRANSFORMER.svg";
    case "capacitor":
      return "CAPACITOR_BANK.svg";
    case "external_source":
      return "CIRCUIT_HEAD.svg";
    case "switch":
      return item.isClosed === false ? "OH_SWITCH_OPEN.svg" : "OH_SWITCH_CLOSED.svg";
    case "battery":
      return "IBANK.svg";
    case "ductbank":
      return "JUNCTION_BAR.svg";
    default:
      return null;
  }
};

const buildSymbolSvg = (type: NetworkElement["type"], color: string, size = 32) => {
  const stroke = color;
  const fill = "rgba(15, 23, 42, 0.85)";

  switch (type) {
    case "generator":
      return `<svg viewBox="0 0 24 24" width="${size}" height="${size}"><circle cx="12" cy="12" r="8" fill="${fill}" stroke="${stroke}" stroke-width="2"/><text x="12" y="16" text-anchor="middle" font-size="10" font-weight="700" fill="${stroke}">G</text></svg>`;
    case "load":
      return `<svg viewBox="0 0 24 24" width="${size}" height="${size}"><line x1="12" y1="4" x2="12" y2="13" stroke="${stroke}" stroke-width="2.2" stroke-linecap="round"/><polygon points="6,13 18,13 12,21" fill="${stroke}" stroke="${stroke}" stroke-width="1.4"/></svg>`;
    case "transformer":
      return `<svg viewBox="0 0 24 24" width="${size}" height="${size}"><circle cx="9" cy="12" r="5" fill="${fill}" stroke="${stroke}" stroke-width="2"/><circle cx="15" cy="12" r="5" fill="${fill}" stroke="${stroke}" stroke-width="2"/></svg>`;
    case "capacitor":
      return `<svg viewBox="0 0 24 24" width="${size}" height="${size}"><line x1="4" y1="12" x2="9" y2="12" stroke="${stroke}" stroke-width="2"/><line x1="9" y1="6" x2="9" y2="18" stroke="${stroke}" stroke-width="2.5"/><line x1="15" y1="6" x2="15" y2="18" stroke="${stroke}" stroke-width="2.5"/><line x1="15" y1="12" x2="20" y2="12" stroke="${stroke}" stroke-width="2"/></svg>`;
    case "switch":
      return `<svg viewBox="0 0 24 24" width="${size}" height="${size}"><circle cx="6" cy="12" r="2.2" fill="${stroke}"/><circle cx="18" cy="12" r="2.2" fill="${stroke}"/><line x1="8" y1="12" x2="15.5" y2="8" stroke="${stroke}" stroke-width="2" stroke-linecap="round"/></svg>`;
    case "external_source":
      return `<svg viewBox="0 0 24 24" width="${size}" height="${size}"><rect x="5" y="5" width="14" height="14" fill="${fill}" stroke="${stroke}" stroke-width="2"/><line x1="5" y1="9" x2="19" y2="9" stroke="${stroke}" stroke-width="1.4"/><line x1="5" y1="15" x2="19" y2="15" stroke="${stroke}" stroke-width="1.4"/><line x1="9" y1="5" x2="9" y2="19" stroke="${stroke}" stroke-width="1.4"/><line x1="15" y1="5" x2="15" y2="19" stroke="${stroke}" stroke-width="1.4"/></svg>`;
    default:
      return `<svg viewBox="0 0 24 24" width="${size}" height="${size}"><circle cx="12" cy="12" r="6" fill="${fill}" stroke="${stroke}" stroke-width="2"/></svg>`;
  }
};

const createSymbolIcon = (type: NetworkElement["type"], color: string, size = 32, angleDeg = 0) => {
  const svg = buildSymbolSvg(type, color, size);
  const rotation = type === "switch" ? `transform: rotate(${angleDeg}deg); transform-origin: center center;` : "";
  return L.divIcon({
    className: "",
    html: `<div style="width:${size}px;height:${size}px;display:flex;align-items:center;justify-content:center;${rotation}">${svg}</div>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
};

const createSvgSymbolIcon = (
  svgUrl: string,
  fallbackUrl: string | null,
  size = 32,
  angleDeg = 0,
  glyphScale = 2.0,
  colorFilter = "brightness(0) saturate(0) invert(1)",
) => {
  const rotation = `transform: rotate(${angleDeg}deg); transform-origin: center center;`;
  const fallbackAttr = fallbackUrl ? ` data-fallback-src="${fallbackUrl}"` : "";
  return L.divIcon({
    className: "",
    html: `<div style="width:${size}px;height:${size}px;display:flex;align-items:center;justify-content:center;overflow:visible;${rotation}"><img src="${svgUrl}"${fallbackAttr} onerror="if(this.dataset.fallbackSrc){this.src=this.dataset.fallbackSrc;this.dataset.fallbackSrc='';}else{this.style.display='none';}" style="width:${size}px;height:${size}px;object-fit:contain;transform:scale(${glyphScale});transform-origin:center center;filter: ${colorFilter} drop-shadow(0 0 1.4px rgba(255,255,255,0.95));" /></div>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
};

interface SpatialMapViewProps {
  elements: NetworkElement[];
}

function FitToNetwork({
  markers,
  lines,
}: {
  markers: ElementGeo[];
  lines: Array<{ id: string; type: NetworkElement["type"]; positions: [number, number][] }>;
}) {
  const map = useMap();

  useEffect(() => {
    const points: [number, number][] = [];

    markers.forEach((marker) => {
      points.push([marker.point.lat, marker.point.lon]);
    });

    lines.forEach((line) => {
      line.positions.forEach((position) => points.push(position));
    });

    if (points.length === 0) return;

    if (points.length === 1) {
      map.setView(points[0], 19, { animate: false });
      return;
    }

    const bounds = L.latLngBounds(points);
    map.fitBounds(bounds, {
      padding: [40, 40],
      maxZoom: 19,
      animate: false,
    });
  }, [map, markers, lines]);

  return null;
}

function ResizeMapToContainer() {
  const map = useMap();

  useEffect(() => {
    const container = map.getContainer();
    if (!container) return;

    const onResize = () => {
      map.invalidateSize({ pan: false, animate: false });
    };

    const observer = new ResizeObserver(onResize);
    observer.observe(container);
    window.addEventListener("resize", onResize);

    requestAnimationFrame(onResize);

    return () => {
      observer.disconnect();
      window.removeEventListener("resize", onResize);
    };
  }, [map]);

  return null;
}


export function SpatialMapView({ elements }: SpatialMapViewProps) {
  const [isDark, setIsDark] = useState(false);
  const [symbolIcons] = useState(() => new Map<string, L.DivIcon>());

  const resolveMarkerColor = useMemo(
    () => (item: ElementGeo) => {
      if (item.type === "switch") {
        return item.isClosed === false ? "#ef4444" : "#ffffff";
      }
      if (item.type === "transformer") return "#facc15";
      if (item.type === "generator") return "#d6c6a5";
      if (item.type === "capacitor" && item.sourceType === "shunt") return "#ff5ca8";
      if (item.type === "capacitor") return "#ff5ca8";
      return elementColor[item.type];
    },
    []
  );

  const resolveSymbolIcon = useMemo(
    () => (item: ElementGeo) => {
      const color = resolveMarkerColor(item);
      const size = symbolSizeByType[item.type] ?? 32;
      const glyphScale = symbolScaleByType[item.type] ?? 2.0;
      const angle = item.type === "switch" ? Math.round((item.angleDeg ?? 0) * 10) / 10 : 0;
      const colorFilter = item.type === "transformer"
        ? "brightness(0) saturate(100%) invert(81%) sepia(90%) saturate(1468%) hue-rotate(356deg) brightness(103%) contrast(98%)"
        : "brightness(0) saturate(0) invert(1)";
      const svgFile = resolveSvgFileName(item);
      const svgPaths = svgFile ? resolveSvgPaths(svgFile) : [];
      const svgUrl = svgPaths[0] ?? "";
      const fallbackUrl = svgPaths[1] ?? null;
      const key = svgFile
        ? `${item.type}:svg:${svgFile}:${size}:${glyphScale}:${angle}:${colorFilter}`
        : `${item.type}:fallback:${color}:${size}:${angle}`;
      const cached = symbolIcons.get(key);
      if (cached) return cached;
      const created = svgFile
        ? createSvgSymbolIcon(svgUrl, fallbackUrl, size, angle, glyphScale, colorFilter)
        : createSymbolIcon(item.type, color, size, angle);
      symbolIcons.set(key, created);
      return created;
    },
    [resolveMarkerColor, symbolIcons]
  );

  useEffect(() => {
    const update = () => {
      setIsDark(document.documentElement.classList.contains("dark"));
    };
    update();
    const observer = new MutationObserver(update);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    return () => observer.disconnect();
  }, []);

  const { markers, lines } = useMemo(() => {
    const busLocations = new Map<string, GeoPoint>();
    const directLocations = new Map<string, GeoPoint>();

    elements.forEach((el) => {
      if (!Number.isFinite(el.geoLat) || !Number.isFinite(el.geoLon)) return;
      const point = { lat: el.geoLat as number, lon: el.geoLon as number };
      directLocations.set(el.id, point);
      if (el.type === "bus") {
        busLocations.set(el.id, point);
      }
    });

    const getPointById = (id?: string) => {
      if (!id) return null;
      return busLocations.get(id) ?? directLocations.get(id) ?? null;
    };

    const midpoint = (a: GeoPoint, b: GeoPoint): GeoPoint => ({
      lat: (a.lat + b.lat) / 2,
      lon: (a.lon + b.lon) / 2,
    });

    const resolvePoint = (el: NetworkElement): GeoPoint | null => {
      if (Number.isFinite(el.geoLat) && Number.isFinite(el.geoLon)) {
        return { lat: el.geoLat as number, lon: el.geoLon as number };
      }

      if (el.type === "bus") return null;

      const connectedBusId = (el as { connectedBusId?: string }).connectedBusId;
      const connected = getPointById(connectedBusId);
      if (connected) return connected;

      const fromBusId = (el as { fromBusId?: string }).fromBusId;
      const toBusId = (el as { toBusId?: string }).toBusId;
      const fromBus = getPointById(fromBusId);
      const toBus = getPointById(toBusId);
      if (fromBus && toBus) return midpoint(fromBus, toBus);
      if (fromBus || toBus) return fromBus ?? toBus ?? null;

      const fromElementId = (el as { fromElementId?: string }).fromElementId;
      const toElementId = (el as { toElementId?: string }).toElementId;
      const fromEl = getPointById(fromElementId);
      const toEl = getPointById(toElementId);
      if (fromEl && toEl) return midpoint(fromEl, toEl);
      if (fromEl || toEl) return fromEl ?? toEl ?? null;

      return null;
    };

    const resolveSwitchLineAnchor = (el: NetworkElement): { point: GeoPoint; angleDeg: number } | null => {
      const fromBusId = (el as { fromBusId?: string }).fromBusId;
      const toBusId = (el as { toBusId?: string }).toBusId;
      const fromElementId = (el as { fromElementId?: string }).fromElementId;
      const toElementId = (el as { toElementId?: string }).toElementId;

      const start = getPointById(fromBusId) ?? getPointById(fromElementId);
      const end = getPointById(toBusId) ?? getPointById(toElementId);
      if (!start || !end) return null;

      const point = midpoint(start, end);
      const angleRad = Math.atan2(end.lat - start.lat, end.lon - start.lon);
      const angleDeg = (angleRad * 180) / Math.PI;
      return { point, angleDeg };
    };

    const markerSeeds: ElementGeo[] = [];
    const lines: Array<{
      id: string;
      type: NetworkElement["type"];
      positions: [number, number][];
    }> = [];

    elements.forEach((el) => {
      if (el.type === "line" || el.type === "cable") {
        const fromElementId = (el as { fromElementId?: string }).fromElementId;
        const toElementId = (el as { toElementId?: string }).toElementId;
        const fromBusId = (el as { fromBusId?: string }).fromBusId;
        const toBusId = (el as { toBusId?: string }).toBusId;
        const start = getPointById(fromBusId) ?? getPointById(fromElementId);
        const end = getPointById(toBusId) ?? getPointById(toElementId);
        if (start && end) {
          lines.push({
            id: el.id,
            type: el.type,
            positions: [
              [start.lat, start.lon],
              [end.lat, end.lon],
            ],
          });
        }
        return;
      }

      if (el.type === "switch") {
        const anchor = resolveSwitchLineAnchor(el);
        if (anchor) {
          markerSeeds.push({
            id: el.id,
            name: el.name,
            type: el.type,
            isClosed: (el as { isClosed?: boolean }).isClosed,
            sourceType: String((el as { sourceType?: string }).sourceType ?? ""),
            point: anchor.point,
            angleDeg: anchor.angleDeg,
            pinnedToLine: true,
          });
          return;
        }
      }

      const point = resolvePoint(el);
      if (!point) return;
      markerSeeds.push({
        id: el.id,
        name: el.name,
        type: el.type,
        isClosed: (el as { isClosed?: boolean }).isClosed,
        sourceType: String((el as { sourceType?: string }).sourceType ?? ""),
        point,
        angleDeg: 0,
        pinnedToLine: false,
      });
    });

    const markers: ElementGeo[] = markerSeeds.map((seed) => {
      if (seed.type === "bus" || seed.pinnedToLine) {
        return seed;
      }
      return { ...seed, point: offsetGeoPoint(seed.point, seed.type, seed.id) };
    });

    return { markers, lines };
  }, [elements]);

  return (
    <div className="relative h-full w-full">
      <MapContainer
        center={[39.5, -98.35]}
        zoom={4}
        maxZoom={22}
        className="h-full w-full"
        attributionControl={false}
        zoomControl={false}
      >
        <TileLayer
          attribution='&copy; OpenStreetMap contributors &copy; CARTO'
          url={
            isDark
              ? "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
              : "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
          }
        />
        <ResizeMapToContainer />
        <FitToNetwork markers={markers} lines={lines} />
        {lines.map((line) => (
          <Polyline
            key={`line-${line.id}`}
            positions={line.positions}
            pathOptions={{ color: elementColor[line.type], weight: 2, opacity: 0.7 }}
          />
        ))}
        {markers.map((item) => (
          symbolMarkerTypes.has(item.type) ? (
            <Marker
              key={item.id}
              position={[item.point.lat, item.point.lon]}
              icon={resolveSymbolIcon(item)}
            >
              <Tooltip direction="top" offset={[0, -6]} opacity={0.9}>
                {elementLabels[item.type]}{item.sourceType ? ` (${item.sourceType})` : ""}: {item.name}
              </Tooltip>
            </Marker>
          ) : (
            <CircleMarker
              key={item.id}
              center={[item.point.lat, item.point.lon]}
              radius={markerRadius[item.type]}
              pathOptions={{
                color: resolveMarkerColor(item),
                fillColor: resolveMarkerColor(item),
                fillOpacity: 0.9,
                weight: 1,
              }}
            >
              <Tooltip direction="top" offset={[0, -6]} opacity={0.9}>
                {elementLabels[item.type]}{item.sourceType ? ` (${item.sourceType})` : ""}: {item.name}
              </Tooltip>
            </CircleMarker>
          )
        ))}
      </MapContainer>
    </div>
  );
}
