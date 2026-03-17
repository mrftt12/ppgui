import { useEffect, useRef } from "react";
import { useMap } from "react-leaflet";
import * as esri from "esri-leaflet";
import L from "leaflet";

interface EsriFeatureLayersProps {
  showTransmission?: boolean;
  showSubstations?: boolean;
}

export function EsriFeatureLayers({
  showTransmission = false,
  showSubstations = false,
}: EsriFeatureLayersProps) {
  const map = useMap();
  const transmissionRef = useRef<ReturnType<typeof esri.featureLayer> | null>(null);
  const substationRef = useRef<ReturnType<typeof esri.featureLayer> | null>(null);

  useEffect(() => {
    const getVoltageColor = (v: string | number | null | undefined) => {
      const volt = parseFloat(String(v ?? ""));
      if (!volt) return "#808080";
      if (volt >= 500) return "#8B0000";
      if (volt >= 345) return "#FF0000";
      if (volt >= 230) return "#FFA500";
      if (volt >= 138) return "#0000FF";
      if (volt >= 69) return "#228B22";
      return "#808080";
    };

    const getLineWeight = (v: string | number | null | undefined) => {
      const volt = parseFloat(String(v ?? ""));
      if (!volt) return 1;
      if (volt >= 500) return 5;
      if (volt >= 345) return 4;
      if (volt >= 230) return 3;
      if (volt >= 138) return 2;
      return 1;
    };

    if (!transmissionRef.current) {
      transmissionRef.current = esri.featureLayer({
        url: "https://services2.arcgis.com/FiaPA4ga0iQKduv3/ArcGIS/rest/services/US_Electric_Power_Transmission_Lines/FeatureServer/0",
        style: (feature: any) => {
          const voltage = feature?.properties?.VOLTAGE;
          return {
            color: getVoltageColor(voltage),
            weight: getLineWeight(voltage),
            opacity: 0.8,
          };
        },
        onEachFeature: (feature: any, layer: any) => {
          if (feature?.properties) {
            const props = feature.properties;
            layer.bindPopup(`
              <div style="font-family: Inter, sans-serif;">
                <strong>Transmission Line</strong><br/>
                Voltage: ${props.VOLTAGE || "N/A"} kV<br/>
                Owner: ${props.OWNER || "N/A"}<br/>
                Status: ${props.STATUS || "N/A"}
              </div>
            `);
          }
        },
      });
    }

    if (!substationRef.current) {
      substationRef.current = esri.featureLayer({
        url: "https://services5.arcgis.com/HDRa0B57OVrv2E1q/ArcGIS/rest/services/Electric_Substations/FeatureServer/0",
        pointToLayer: (feature: any, latlng: L.LatLng) => {
          const voltage = feature?.properties?.MAX_VOLT || feature?.properties?.Max_Voltage;
          const color = getVoltageColor(voltage);
          return L.marker(latlng, {
            icon: L.divIcon({
              className: "substation-marker",
              html: `<div style="width:10px;height:10px;background-color:${color};border:1px solid white;box-shadow:0 0 2px rgba(0,0,0,0.5);"></div>`,
              iconSize: [12, 12],
              iconAnchor: [6, 6],
            }),
          });
        },
        onEachFeature: (feature: any, layer: any) => {
          if (feature?.properties) {
            const props = feature.properties;
            layer.bindPopup(`
              <div style="font-family: Inter, sans-serif;">
                <strong>${props.NAME || "Substation"}</strong><br/>
                Voltage (High): ${props.MAX_VOLT || props.Max_Voltage || "N/A"} kV<br/>
                Voltage (Low): ${props.MIN_VOLT || props.Min_Voltage || "N/A"} kV<br/>
                Owner: ${props.OWNER || "N/A"}<br/>
                State: ${props.STATE || "N/A"}
              </div>
            `);
          }
        },
      });
    }

    return () => {
      if (transmissionRef.current) map.removeLayer(transmissionRef.current);
      if (substationRef.current) map.removeLayer(substationRef.current);
    };
  }, [map]);

  useEffect(() => {
    if (!transmissionRef.current) return;
    if (showTransmission) {
      transmissionRef.current.addTo(map);
    } else {
      map.removeLayer(transmissionRef.current);
    }
  }, [showTransmission, map]);

  useEffect(() => {
    if (!substationRef.current) return;
    if (showSubstations) {
      substationRef.current.addTo(map);
    } else {
      map.removeLayer(substationRef.current);
    }
  }, [showSubstations, map]);

  return null;
}
