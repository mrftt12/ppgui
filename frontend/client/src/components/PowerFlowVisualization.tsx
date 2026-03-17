/**
 * PowerFlowVisualization – Three.js component that renders
 * the IEEE 123-bus distribution feeder as a 3D particle-based
 * electricity flow visualisation.
 *
 * Buses are rendered as glowing spheres coloured by type:
 *   • Source (ext_grid)  → gold
 *   • Load               → orange
 *   • Capacitor          → cyan
 *   • Junction           → dim white
 *
 * Branches are tube-like connections coloured by phase code
 * (A=red, B=amber, C=blue, AB=orange, AC=teal, BC=purple, ABC=emerald)
 * with animated particles streaming along them to represent power flow.
 */

import { useRef, useEffect, useCallback } from "react";
import * as THREE from "three";
import { OrbitControls } from "three/examples/jsm/controls/OrbitControls.js";
import { EffectComposer } from "three/examples/jsm/postprocessing/EffectComposer.js";
import { RenderPass } from "three/examples/jsm/postprocessing/RenderPass.js";
import { UnrealBloomPass } from "three/examples/jsm/postprocessing/UnrealBloomPass.js";
import { ShaderPass } from "three/examples/jsm/postprocessing/ShaderPass.js";
import { GammaCorrectionShader } from "three/examples/jsm/shaders/GammaCorrectionShader.js";

import {
  buildIEEE123,
  PHASE_COLORS,
  type Bus,
  type Branch,
  type PhaseCode,
  type NetworkGraph,
} from "@/lib/networkGraph";
import type { LoadFlowResult } from "@shared/schema";

/* ─── colour palette ────────────────────────────────────────────── */

const COL_SOURCE   = new THREE.Color(0xffd700); // gold
const COL_LOAD     = new THREE.Color(0xff5900); // orange
const COL_CAP      = new THREE.Color(0x00e5ff); // cyan
const COL_JUNCTION = new THREE.Color(0x667788); // dim

function busColor(b: Bus): THREE.Color {
  switch (b.type) {
    case "source":    return COL_SOURCE.clone();
    case "load":      return COL_LOAD.clone();
    case "capacitor": return COL_CAP.clone();
    default:          return COL_JUNCTION.clone();
  }
}

/** Get THREE.Color for a phase code */
function phaseColor(pc: PhaseCode): THREE.Color {
  return new THREE.Color(PHASE_COLORS[pc]);
}

type SinglePhase = "A" | "B" | "C";

function phasesFromCode(pc: PhaseCode): SinglePhase[] {
  switch (pc) {
    case "A":
      return ["A"];
    case "B":
      return ["B"];
    case "C":
      return ["C"];
    case "AB":
      return ["A", "B"];
    case "AC":
      return ["A", "C"];
    case "BC":
      return ["B", "C"];
    case "ABC":
      return ["A", "B", "C"];
    default:
      return ["A", "B", "C"];
  }
}

function phaseOffsets(phaseCount: number, separation: number): number[] {
  if (phaseCount === 3) return [-separation, 0, separation];
  if (phaseCount === 2) return [-separation * 0.5, separation * 0.5];
  return [0];
}

/* ─── constants ─────────────────────────────────────────────────── */

const PARTICLES_PER_BRANCH = 50;     // animated electricity particles per branch
const NETWORK_SCALE        = 6;      // world-units for the normalised [-1,1] layout
const BUS_SPHERE_RADIUS    = 0.055;
const TUBE_RADIUS          = 0.014;

/* ================================================================
   React component
   ================================================================ */

interface Props {
  className?: string;
  /** Supply a dynamic network graph; falls back to IEEE-123 demo when omitted. */
  graph?: NetworkGraph;
  loadFlowResult?: LoadFlowResult | null;
  showVoltageLabels?: boolean;
}

function createVoltageBadgeSprite(voltagePU: number): THREE.Sprite {
  const width = 192;
  const height = 48;
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    const fallbackMaterial = new THREE.SpriteMaterial({ color: 0xffffff });
    return new THREE.Sprite(fallbackMaterial);
  }

  const labelColor =
    voltagePU < 0.95 || voltagePU > 1.05
      ? "#ef4444"
      : voltagePU < 0.98 || voltagePU > 1.02
        ? "#f59e0b"
        : "#10b981";

  ctx.clearRect(0, 0, width, height);
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.font = "600 16px Inter, Segoe UI, Arial, sans-serif";
  ctx.fillStyle = labelColor;
  ctx.shadowColor = "rgba(2, 6, 23, 0.95)";
  ctx.shadowBlur = 4;
  ctx.fillText(`${voltagePU.toFixed(3)} pu`, width / 2, height / 2);
  ctx.shadowBlur = 0;

  const texture = new THREE.CanvasTexture(canvas);
  texture.needsUpdate = true;
  texture.minFilter = THREE.LinearFilter;
  texture.magFilter = THREE.LinearFilter;

  const sprite = new THREE.Sprite(
    new THREE.SpriteMaterial({
      map: texture,
      transparent: true,
      depthWrite: false,
      depthTest: false,
    }),
  );
  sprite.renderOrder = 999;
  sprite.scale.set(0.44, 0.11, 1);
  return sprite;
}

export default function PowerFlowVisualization({
  className,
  graph: externalGraph,
  loadFlowResult,
  showVoltageLabels = true,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cleanupRef   = useRef<(() => void) | null>(null);

  /* ── build scene ──────────────────────────────────────────────── */
  const initScene = useCallback(() => {
    const container = containerRef.current;
    if (!container) return;

    // Dispose of any previous scene
    cleanupRef.current?.();

    const graph = externalGraph ?? buildIEEE123();
    const busMap = new Map(graph.buses.map((b) => [b.id, b]));

    /* renderer */
    const renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setSize(container.clientWidth, container.clientHeight);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1;
    container.appendChild(renderer.domElement);

    /* scene + camera */
    const scene  = new THREE.Scene();
    scene.background = new THREE.Color(0x050510);
    scene.fog = new THREE.Fog(0x050510, 25, 50);

    const camera = new THREE.PerspectiveCamera(
      55,
      container.clientWidth / container.clientHeight,
      0.1,
      200,
    );
    camera.position.set(0, 9, 14);

    /* lights */
    scene.add(new THREE.AmbientLight(0xffffff, 0.4));
    const dir = new THREE.DirectionalLight(0xffffff, 0.8);
    dir.position.set(3, 8, 5);
    scene.add(dir);

    /* controls */
    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.08;
    controls.minDistance   = 3;
    controls.maxDistance   = 25;

    /* ── post processing (bloom) ──────────────────────────────── */
    const composer  = new EffectComposer(renderer);
    composer.addPass(new RenderPass(scene, camera));
    const bloom = new UnrealBloomPass(
      new THREE.Vector2(container.clientWidth, container.clientHeight),
      1.0,   // strength
      0.4,   // radius
      0.85,  // threshold
    );
    composer.addPass(bloom);
    composer.addPass(new ShaderPass(GammaCorrectionShader));

    /* ── bus spheres ──────────────────────────────────────────── */
    const busPositions = new Map<number, THREE.Vector3>();

    for (const bus of graph.buses) {
      const pos = new THREE.Vector3(
        bus.x * NETWORK_SCALE,
        0,
        bus.y * NETWORK_SCALE,
      );
      busPositions.set(bus.id, pos);

      // Source bus is larger; loaded buses scale with kW
      const scale = bus.type === "source"
        ? 2.0
        : bus.type === "capacitor"
          ? 1.4
          : bus.totalLoadKW > 50
            ? 1.3
            : bus.totalLoadKW > 0
              ? 1.0
              : 0.7;

      const geo  = new THREE.SphereGeometry(BUS_SPHERE_RADIUS * scale, 16, 16);
      const mat  = new THREE.MeshStandardMaterial({
        color: busColor(bus),
        emissive: busColor(bus),
        emissiveIntensity: bus.type === "source" ? 1.5 : bus.type === "capacitor" ? 1.0 : 0.6,
        roughness: 0.3,
        metalness: 0.7,
      });
      const mesh = new THREE.Mesh(geo, mat);
      mesh.position.copy(pos);
      scene.add(mesh);

      // point-light at source bus and capacitor banks for extra glow
      if (bus.type === "source") {
        const pl = new THREE.PointLight(COL_SOURCE.getHex(), 1.2, 3);
        pl.position.copy(pos);
        scene.add(pl);
      } else if (bus.type === "capacitor") {
        const pl = new THREE.PointLight(COL_CAP.getHex(), 0.6, 1.5);
        pl.position.copy(pos);
        scene.add(pl);
      }
    }

    /* ── branch tubes (per-phase traces for 1φ/2φ/3φ branches) ── */
    for (const br of graph.branches) {
      const pA = busPositions.get(br.from);
      const pB = busPositions.get(br.to);
      if (!pA || !pB) continue;

      const phases = phasesFromCode(br.phaseCode);
      const offsets = phaseOffsets(phases.length, TUBE_RADIUS * 2.5);
      const direction = new THREE.Vector3().subVectors(pB, pA).normalize();
      const lateral = new THREE.Vector3().crossVectors(direction, new THREE.Vector3(0, 1, 0));
      if (lateral.lengthSq() < 1e-8) {
        lateral.set(1, 0, 0);
      } else {
        lateral.normalize();
      }

      for (let i = 0; i < phases.length; i++) {
        const phase = phases[i];
        const offset = lateral.clone().multiplyScalar(offsets[i] ?? 0);
        const fromPoint = pA.clone().add(offset);
        const toPoint = pB.clone().add(offset);
        const col = new THREE.Color(PHASE_COLORS[phase]);
        const curve = new THREE.LineCurve3(fromPoint, toPoint);
        const tubeGeo = new THREE.TubeGeometry(curve, 1, br.isSwitch ? TUBE_RADIUS * 1.5 : TUBE_RADIUS, 6, false);
        const tubeMat = new THREE.MeshStandardMaterial({
          color: col,
          emissive: col,
          emissiveIntensity: 0.3,
          transparent: true,
          opacity: 0.45,
          roughness: 0.5,
        });
        scene.add(new THREE.Mesh(tubeGeo, tubeMat));
      }
    }

    /* ── flow particles (the electricity!) ─────────────────────── */
    const totalParticleStreams = graph.branches.reduce(
      (count, branch) => count + phasesFromCode(branch.phaseCode).length,
      0,
    );
    const totalParticles = totalParticleStreams * PARTICLES_PER_BRANCH;
    const posAttr      = new Float32Array(totalParticles * 3);
    const colorAttr    = new Float32Array(totalParticles * 3);
    const sizeAttr     = new Float32Array(totalParticles);
    const particleData: { from: THREE.Vector3; to: THREE.Vector3; t: number; speed: number; color: THREE.Color }[] = [];

    let idx = 0;
    for (const br of graph.branches) {
      const pA = busPositions.get(br.from)!;
      const pB = busPositions.get(br.to)!;
      if (!pA || !pB) continue;

      const phases = phasesFromCode(br.phaseCode);
      const offsets = phaseOffsets(phases.length, TUBE_RADIUS * 2.5);
      const direction = new THREE.Vector3().subVectors(pB, pA).normalize();
      const lateral = new THREE.Vector3().crossVectors(direction, new THREE.Vector3(0, 1, 0));
      if (lateral.lengthSq() < 1e-8) {
        lateral.set(1, 0, 0);
      } else {
        lateral.normalize();
      }

      for (let phaseIdx = 0; phaseIdx < phases.length; phaseIdx++) {
        const phase = phases[phaseIdx];
        const offset = lateral.clone().multiplyScalar(offsets[phaseIdx] ?? 0);
        const fromPoint = pA.clone().add(offset);
        const toPoint = pB.clone().add(offset);
        const col = new THREE.Color(PHASE_COLORS[phase]).offsetHSL(0, 0, 0.15);

        for (let p = 0; p < PARTICLES_PER_BRANCH; p++) {
          const t     = Math.random();           // position along branch [0,1]
          const speed = 0.15 + Math.random() * 0.25; // units / sec

          const px = fromPoint.x + (toPoint.x - fromPoint.x) * t;
          const py = fromPoint.y + (toPoint.y - fromPoint.y) * t + (Math.random() - 0.5) * 0.015;
          const pz = fromPoint.z + (toPoint.z - fromPoint.z) * t + (Math.random() - 0.5) * 0.015;

          posAttr[idx * 3]     = px;
          posAttr[idx * 3 + 1] = py;
          posAttr[idx * 3 + 2] = pz;

          const c = col.clone().offsetHSL(0, 0, (Math.random() - 0.5) * 0.2);
          colorAttr[idx * 3]     = c.r;
          colorAttr[idx * 3 + 1] = c.g;
          colorAttr[idx * 3 + 2] = c.b;

          sizeAttr[idx] = 0.025 + Math.random() * 0.02;

          particleData.push({ from: fromPoint, to: toPoint, t, speed, color: c });
          idx++;
        }
      }
    }

    const bufGeo = new THREE.BufferGeometry();
    bufGeo.setAttribute("position", new THREE.BufferAttribute(posAttr, 3));
    bufGeo.setAttribute("color",    new THREE.BufferAttribute(colorAttr, 3));
    bufGeo.setAttribute("size",     new THREE.BufferAttribute(sizeAttr, 1));

    const ptsMat = new THREE.PointsMaterial({
      size: 0.03,
      vertexColors: true,
      blending: THREE.AdditiveBlending,
      depthWrite: false,
      transparent: true,
      opacity: 0.9,
      sizeAttenuation: true,
    });

    const points = new THREE.Points(bufGeo, ptsMat);
    scene.add(points);

    /* ── end-of-line load voltage labels (post-convergence) ───── */
    if (loadFlowResult?.converged && showVoltageLabels) {
      const busResultById = new Map(
        loadFlowResult.busResults.map((bus) => [bus.busId.toLowerCase(), bus]),
      );
      const busResultByName = new Map(
        loadFlowResult.busResults.map((bus) => [bus.busName.toLowerCase(), bus]),
      );

      const degree = new Map<number, number>();
      for (const br of graph.branches) {
        degree.set(br.from, (degree.get(br.from) ?? 0) + 1);
        degree.set(br.to, (degree.get(br.to) ?? 0) + 1);
      }

      for (const bus of graph.buses) {
        if (bus.type !== "load") continue;
        const busDegree = degree.get(bus.id) ?? 0;
        if (busDegree > 1) continue;

        const idKeys = [bus.sourceElementId, String(bus.id)]
          .filter((k): k is string => Boolean(k))
          .map((k) => k.toLowerCase());
        const nameKey = (bus.name ?? "").toLowerCase();

        let busResult = undefined as (typeof loadFlowResult.busResults)[number] | undefined;
        for (const key of idKeys) {
          busResult = busResultById.get(key);
          if (busResult) break;
        }
        if (!busResult && nameKey) {
          busResult = busResultByName.get(nameKey);
        }
        if (!busResult) continue;

        const voltagePU = busResult.voltagePhaseA.magnitude;
        if (!Number.isFinite(voltagePU)) continue;

        const busPos = busPositions.get(bus.id);
        if (!busPos) continue;

        const badge = createVoltageBadgeSprite(voltagePU);
        badge.position.set(busPos.x, busPos.y + 0.25, busPos.z);
        scene.add(badge);
      }
    }

    /* ── ground grid ───────────────────────────────────────────── */
    const gridHelper = new THREE.GridHelper(NETWORK_SCALE * 3.5, 50, 0x111144, 0x111122);
    gridHelper.position.y = -0.15;
    scene.add(gridHelper);

    /* ── animation loop ────────────────────────────────────────── */
    const clock    = new THREE.Clock();
    let animId     = 0;
    let disposed   = false;

    function animate() {
      if (disposed) return;
      animId = requestAnimationFrame(animate);

      const dt  = clock.getDelta();
      const arr = (points.geometry.attributes.position as THREE.BufferAttribute).array as Float32Array;

      for (let i = 0; i < particleData.length; i++) {
        const pd = particleData[i];
        pd.t += pd.speed * dt;
        if (pd.t > 1) pd.t -= 1;

        const { from: a, to: b, t } = pd;
        arr[i * 3]     = a.x + (b.x - a.x) * t;
        arr[i * 3 + 1] = a.y + (b.y - a.y) * t + Math.sin(t * Math.PI * 4 + clock.elapsedTime * 3) * 0.008;
        arr[i * 3 + 2] = a.z + (b.z - a.z) * t;
      }

      points.geometry.attributes.position.needsUpdate = true;

      controls.update();
      composer.render();
    }

    animate();

    /* ── resize handler ────────────────────────────────────────── */
    function onResize() {
      if (!container) return;
      const width = container.clientWidth;
      const height = container.clientHeight;
      if (width <= 0 || height <= 0) return;

      camera.aspect = width / height;
      camera.updateProjectionMatrix();
      renderer.setSize(width, height);
      composer.setSize(width, height);
    }
    const resizeObserver = new ResizeObserver(onResize);
    resizeObserver.observe(container);
    window.addEventListener("resize", onResize);

    /* ── double-click reset ────────────────────────────────────── */
    function onDblClick() {
      camera.position.set(0, 9, 14);
      camera.lookAt(0, 0, 0);
      controls.reset();
    }
    renderer.domElement.addEventListener("dblclick", onDblClick);

    /* ── cleanup ───────────────────────────────────────────────── */
    cleanupRef.current = () => {
      disposed = true;
      cancelAnimationFrame(animId);
      resizeObserver.disconnect();
      window.removeEventListener("resize", onResize);
      renderer.domElement.removeEventListener("dblclick", onDblClick);
      controls.dispose();
      renderer.dispose();
      composer.dispose();
      scene.traverse((obj) => {
        if (obj instanceof THREE.Mesh || obj instanceof THREE.Points || obj instanceof THREE.Sprite) {
          obj.geometry.dispose();
          if (Array.isArray(obj.material)) {
            obj.material.forEach((m) => {
              const materialWithMap = m as THREE.Material & { map?: THREE.Texture };
              materialWithMap.map?.dispose();
              m.dispose();
            });
          } else {
            const material = obj.material as THREE.Material & { map?: THREE.Texture };
            material.map?.dispose();
            material.dispose();
          }
        }
      });
      if (container.contains(renderer.domElement)) {
        container.removeChild(renderer.domElement);
      }
    };
  }, [externalGraph, loadFlowResult, showVoltageLabels]);

  useEffect(() => {
    initScene();
    return () => {
      cleanupRef.current?.();
    };
  }, [initScene]);

  return <div ref={containerRef} className={className} style={{ width: "100%", height: "100%" }} />;
}
