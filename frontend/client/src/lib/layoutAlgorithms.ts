/**
 * Network Layout Algorithms
 * Various methods for arranging network nodes for better visualization
 */

import type { NetworkElement } from '@shared/schema';

export type LayoutAlgorithm = 
    | 'force-directed'
    | 'hierarchical-tb'
    | 'hierarchical-lr'
    | 'radial'
    | 'circular'
    | 'grid'
    | 'tree'
    | 'spectral'
    | 'kamada-kawai'
    | 'fruchterman-reingold'
    | 'sugiyama';

export interface LayoutOptions {
    width: number;
    height: number;
    padding: number;
    nodeSpacing: number;
    iterations?: number;
}

interface NodePosition {
    id: string;
    x: number;
    y: number;
}

interface Edge {
    source: string;
    target: string;
}

// Build adjacency list from elements
function buildGraph(elements: NetworkElement[]): { nodes: string[]; edges: Edge[]; adjacency: Map<string, Set<string>> } {
    const nodeElements = elements.filter(el => el.type !== 'line' && el.type !== 'cable');
    const lineElements = elements.filter(el => el.type === 'line' || el.type === 'cable');
    
    const nodes = nodeElements.map(el => el.id);
    const nodeSet = new Set(nodes);
    
    const edges: Edge[] = [];
    const adjacency = new Map<string, Set<string>>();
    
    // Initialize adjacency
    nodes.forEach(n => adjacency.set(n, new Set()));
    
    lineElements.forEach(el => {
        const from = (el as { fromElementId?: string }).fromElementId;
        const to = (el as { toElementId?: string }).toElementId;
        if (from && to && nodeSet.has(from) && nodeSet.has(to)) {
            edges.push({ source: from, target: to });
            adjacency.get(from)?.add(to);
            adjacency.get(to)?.add(from);
        }
    });
    
    return { nodes, edges, adjacency };
}

// Find root nodes (nodes with only outgoing edges or external sources)
function findRoots(elements: NetworkElement[], adjacency: Map<string, Set<string>>): string[] {
    const nodeElements = elements.filter(el => el.type !== 'line' && el.type !== 'cable');
    
    // Prefer external sources as roots
    const sources = nodeElements.filter(el => el.type === 'external_source').map(el => el.id);
    if (sources.length > 0) return sources;
    
    // Otherwise find nodes with minimum incoming edges
    const inDegree = new Map<string, number>();
    nodeElements.forEach(el => inDegree.set(el.id, 0));
    
    adjacency.forEach((neighbors, node) => {
        neighbors.forEach(neighbor => {
            inDegree.set(neighbor, (inDegree.get(neighbor) || 0) + 1);
        });
    });
    
    // Find minimum in-degree
    let minDegree = Infinity;
    inDegree.forEach(degree => {
        if (degree < minDegree) minDegree = degree;
    });
    
    const roots: string[] = [];
    inDegree.forEach((degree, node) => {
        if (degree === minDegree && roots.length < 3) roots.push(node);
    });
    
    return roots.length > 0 ? roots : [nodeElements[0]?.id].filter(Boolean);
}

/**
 * 1. Force-Directed Layout (Spring Embedder)
 * Simulates physical forces between nodes
 */
export function forceDirectedLayout(elements: NetworkElement[], options: LayoutOptions): NodePosition[] {
    const { nodes, edges, adjacency } = buildGraph(elements);
    const nodeElements = elements.filter(el => el.type !== 'line' && el.type !== 'cable');
    
    // Initialize positions randomly
    const positions = new Map<string, { x: number; y: number; vx: number; vy: number }>();
    nodeElements.forEach(el => {
        positions.set(el.id, {
            x: options.padding + Math.random() * (options.width - 2 * options.padding),
            y: options.padding + Math.random() * (options.height - 2 * options.padding),
            vx: 0,
            vy: 0
        });
    });
    
    const iterations = options.iterations || 100;
    const k = Math.sqrt((options.width * options.height) / nodes.length); // Optimal distance
    const cooling = 0.95;
    let temperature = options.width / 10;
    
    for (let iter = 0; iter < iterations; iter++) {
        // Repulsive forces between all pairs
        nodes.forEach(v => {
            const pv = positions.get(v)!;
            pv.vx = 0;
            pv.vy = 0;
            
            nodes.forEach(u => {
                if (u === v) return;
                const pu = positions.get(u)!;
                const dx = pv.x - pu.x;
                const dy = pv.y - pu.y;
                const dist = Math.sqrt(dx * dx + dy * dy) || 0.01;
                const force = (k * k) / dist;
                pv.vx += (dx / dist) * force;
                pv.vy += (dy / dist) * force;
            });
        });
        
        // Attractive forces along edges
        edges.forEach(({ source, target }) => {
            const ps = positions.get(source)!;
            const pt = positions.get(target)!;
            const dx = pt.x - ps.x;
            const dy = pt.y - ps.y;
            const dist = Math.sqrt(dx * dx + dy * dy) || 0.01;
            const force = (dist * dist) / k;
            const fx = (dx / dist) * force;
            const fy = (dy / dist) * force;
            ps.vx += fx;
            ps.vy += fy;
            pt.vx -= fx;
            pt.vy -= fy;
        });
        
        // Apply velocities with temperature limiting
        nodes.forEach(v => {
            const p = positions.get(v)!;
            const speed = Math.sqrt(p.vx * p.vx + p.vy * p.vy) || 0.01;
            const limitedSpeed = Math.min(speed, temperature);
            p.x += (p.vx / speed) * limitedSpeed;
            p.y += (p.vy / speed) * limitedSpeed;
            
            // Keep in bounds
            p.x = Math.max(options.padding, Math.min(options.width - options.padding, p.x));
            p.y = Math.max(options.padding, Math.min(options.height - options.padding, p.y));
        });
        
        temperature *= cooling;
    }
    
    return nodes.map(id => ({
        id,
        x: Math.round(positions.get(id)!.x),
        y: Math.round(positions.get(id)!.y)
    }));
}

/**
 * 2. Hierarchical Layout (Top-to-Bottom)
 * Arranges nodes in layers based on graph depth
 */
export function hierarchicalLayoutTB(elements: NetworkElement[], options: LayoutOptions): NodePosition[] {
    const { nodes, adjacency } = buildGraph(elements);
    const nodeElements = elements.filter(el => el.type !== 'line' && el.type !== 'cable');
    const roots = findRoots(elements, adjacency);
    
    // BFS to assign layers
    const layers = new Map<string, number>();
    const visited = new Set<string>();
    const queue: { id: string; layer: number }[] = roots.map(r => ({ id: r, layer: 0 }));
    roots.forEach(r => visited.add(r));
    
    while (queue.length > 0) {
        const { id, layer } = queue.shift()!;
        layers.set(id, layer);
        
        adjacency.get(id)?.forEach(neighbor => {
            if (!visited.has(neighbor)) {
                visited.add(neighbor);
                queue.push({ id: neighbor, layer: layer + 1 });
            }
        });
    }
    
    // Handle disconnected nodes
    nodeElements.forEach(el => {
        if (!layers.has(el.id)) {
            layers.set(el.id, 0);
        }
    });
    
    // Group nodes by layer
    const layerGroups = new Map<number, string[]>();
    layers.forEach((layer, id) => {
        if (!layerGroups.has(layer)) layerGroups.set(layer, []);
        layerGroups.get(layer)!.push(id);
    });
    
    const numLayers = Math.max(...Array.from(layers.values())) + 1;
    const layerHeight = (options.height - 2 * options.padding) / Math.max(numLayers - 1, 1);
    
    const positions: NodePosition[] = [];
    layerGroups.forEach((nodesInLayer, layer) => {
        const layerWidth = options.width - 2 * options.padding;
        const spacing = layerWidth / (nodesInLayer.length + 1);
        
        nodesInLayer.forEach((id, index) => {
            positions.push({
                id,
                x: options.padding + spacing * (index + 1),
                y: options.padding + layer * layerHeight
            });
        });
    });
    
    return positions;
}

/**
 * 3. Hierarchical Layout (Left-to-Right)
 */
export function hierarchicalLayoutLR(elements: NetworkElement[], options: LayoutOptions): NodePosition[] {
    const tbPositions = hierarchicalLayoutTB(elements, {
        ...options,
        width: options.height,
        height: options.width
    });
    
    // Rotate 90 degrees
    return tbPositions.map(p => ({
        id: p.id,
        x: p.y,
        y: p.x
    }));
}

/**
 * 4. Radial Layout
 * Places nodes in concentric circles based on depth from root
 */
export function radialLayout(elements: NetworkElement[], options: LayoutOptions): NodePosition[] {
    const { nodes, adjacency } = buildGraph(elements);
    const nodeElements = elements.filter(el => el.type !== 'line' && el.type !== 'cable');
    const roots = findRoots(elements, adjacency);
    
    // BFS to assign layers
    const layers = new Map<string, number>();
    const visited = new Set<string>();
    const queue: { id: string; layer: number }[] = roots.map(r => ({ id: r, layer: 0 }));
    roots.forEach(r => visited.add(r));
    
    while (queue.length > 0) {
        const { id, layer } = queue.shift()!;
        layers.set(id, layer);
        
        adjacency.get(id)?.forEach(neighbor => {
            if (!visited.has(neighbor)) {
                visited.add(neighbor);
                queue.push({ id: neighbor, layer: layer + 1 });
            }
        });
    }
    
    // Handle disconnected nodes
    let maxLayer = Math.max(...Array.from(layers.values()), 0);
    nodeElements.forEach(el => {
        if (!layers.has(el.id)) {
            layers.set(el.id, maxLayer + 1);
        }
    });
    maxLayer = Math.max(...Array.from(layers.values()));
    
    const centerX = options.width / 2;
    const centerY = options.height / 2;
    const maxRadius = Math.min(options.width, options.height) / 2 - options.padding;
    const radiusStep = maxRadius / (maxLayer + 1);
    
    // Group by layer
    const layerGroups = new Map<number, string[]>();
    layers.forEach((layer, id) => {
        if (!layerGroups.has(layer)) layerGroups.set(layer, []);
        layerGroups.get(layer)!.push(id);
    });
    
    const positions: NodePosition[] = [];
    layerGroups.forEach((nodesInLayer, layer) => {
        const radius = layer === 0 ? 0 : radiusStep * layer;
        const angleStep = (2 * Math.PI) / nodesInLayer.length;
        
        nodesInLayer.forEach((id, index) => {
            const angle = index * angleStep - Math.PI / 2;
            positions.push({
                id,
                x: Math.round(centerX + radius * Math.cos(angle)),
                y: Math.round(centerY + radius * Math.sin(angle))
            });
        });
    });
    
    return positions;
}

/**
 * 5. Circular Layout
 * Places all nodes in a single circle
 */
export function circularLayout(elements: NetworkElement[], options: LayoutOptions): NodePosition[] {
    const nodeElements = elements.filter(el => el.type !== 'line' && el.type !== 'cable');
    
    const centerX = options.width / 2;
    const centerY = options.height / 2;
    const radius = Math.min(options.width, options.height) / 2 - options.padding;
    const angleStep = (2 * Math.PI) / nodeElements.length;
    
    return nodeElements.map((el, index) => ({
        id: el.id,
        x: Math.round(centerX + radius * Math.cos(index * angleStep - Math.PI / 2)),
        y: Math.round(centerY + radius * Math.sin(index * angleStep - Math.PI / 2))
    }));
}

/**
 * 6. Grid Layout
 * Arranges nodes in a regular grid pattern
 */
export function gridLayout(elements: NetworkElement[], options: LayoutOptions): NodePosition[] {
    const nodeElements = elements.filter(el => el.type !== 'line' && el.type !== 'cable');
    
    const n = nodeElements.length;
    const cols = Math.ceil(Math.sqrt(n * (options.width / options.height)));
    const rows = Math.ceil(n / cols);
    
    const cellWidth = (options.width - 2 * options.padding) / cols;
    const cellHeight = (options.height - 2 * options.padding) / rows;
    
    return nodeElements.map((el, index) => ({
        id: el.id,
        x: Math.round(options.padding + (index % cols) * cellWidth + cellWidth / 2),
        y: Math.round(options.padding + Math.floor(index / cols) * cellHeight + cellHeight / 2)
    }));
}

/**
 * 7. Tree Layout
 * Balanced tree layout for hierarchical networks
 */
export function treeLayout(elements: NetworkElement[], options: LayoutOptions): NodePosition[] {
    const { adjacency } = buildGraph(elements);
    const nodeElements = elements.filter(el => el.type !== 'line' && el.type !== 'cable');
    const roots = findRoots(elements, adjacency);
    
    const positions = new Map<string, { x: number; y: number }>();
    const visited = new Set<string>();
    
    // Recursive tree positioning
    function layoutSubtree(nodeId: string, x: number, y: number, width: number, depth: number): void {
        if (visited.has(nodeId)) return;
        visited.add(nodeId);
        
        positions.set(nodeId, { x, y });
        
        const children = Array.from(adjacency.get(nodeId) || []).filter(c => !visited.has(c));
        if (children.length === 0) return;
        
        const childWidth = width / children.length;
        const nextY = y + options.nodeSpacing * 2;
        
        children.forEach((child, index) => {
            const childX = x - width / 2 + childWidth / 2 + index * childWidth;
            layoutSubtree(child, childX, nextY, childWidth, depth + 1);
        });
    }
    
    // Layout each tree from roots
    const rootWidth = (options.width - 2 * options.padding) / roots.length;
    roots.forEach((root, index) => {
        const rootX = options.padding + rootWidth / 2 + index * rootWidth;
        layoutSubtree(root, rootX, options.padding + 50, rootWidth, 0);
    });
    
    // Handle disconnected nodes
    let yOffset = Math.max(...Array.from(positions.values()).map(p => p.y)) + options.nodeSpacing;
    nodeElements.forEach(el => {
        if (!positions.has(el.id)) {
            positions.set(el.id, { x: options.width / 2, y: yOffset });
            yOffset += options.nodeSpacing;
        }
    });
    
    return nodeElements.map(el => ({
        id: el.id,
        x: Math.round(positions.get(el.id)!.x),
        y: Math.round(positions.get(el.id)!.y)
    }));
}

/**
 * 8. Spectral Layout
 * Uses graph Laplacian eigenvalues for positioning
 * (Simplified version using power iteration)
 */
export function spectralLayout(elements: NetworkElement[], options: LayoutOptions): NodePosition[] {
    const { nodes, adjacency } = buildGraph(elements);
    const n = nodes.length;
    
    if (n === 0) return [];
    if (n === 1) return [{ id: nodes[0], x: options.width / 2, y: options.height / 2 }];
    
    const nodeIndex = new Map<string, number>();
    nodes.forEach((id, i) => nodeIndex.set(id, i));
    
    // Build degree and adjacency matrices
    const degree = nodes.map(id => adjacency.get(id)?.size || 0);
    
    // Power iteration to find eigenvectors
    let x = nodes.map(() => Math.random() - 0.5);
    let y = nodes.map(() => Math.random() - 0.5);
    
    for (let iter = 0; iter < 50; iter++) {
        // Laplacian multiplication for x
        const newX = x.map((_, i) => {
            let sum = degree[i] * x[i];
            adjacency.get(nodes[i])?.forEach(neighbor => {
                const j = nodeIndex.get(neighbor)!;
                sum -= x[j];
            });
            return sum;
        });
        
        // Normalize
        const normX = Math.sqrt(newX.reduce((s, v) => s + v * v, 0)) || 1;
        x = newX.map(v => v / normX);
        
        // Laplacian multiplication for y (orthogonalize to x)
        const newY = y.map((_, i) => {
            let sum = degree[i] * y[i];
            adjacency.get(nodes[i])?.forEach(neighbor => {
                const j = nodeIndex.get(neighbor)!;
                sum -= y[j];
            });
            return sum;
        });
        
        // Orthogonalize y to x
        const dot = newY.reduce((s, v, i) => s + v * x[i], 0);
        const orthY = newY.map((v, i) => v - dot * x[i]);
        
        const normY = Math.sqrt(orthY.reduce((s, v) => s + v * v, 0)) || 1;
        y = orthY.map(v => v / normY);
    }
    
    // Scale to canvas
    const minX = Math.min(...x);
    const maxX = Math.max(...x);
    const minY = Math.min(...y);
    const maxY = Math.max(...y);
    
    const scaleX = (options.width - 2 * options.padding) / (maxX - minX || 1);
    const scaleY = (options.height - 2 * options.padding) / (maxY - minY || 1);
    
    return nodes.map((id, i) => ({
        id,
        x: Math.round(options.padding + (x[i] - minX) * scaleX),
        y: Math.round(options.padding + (y[i] - minY) * scaleY)
    }));
}

/**
 * 9. Kamada-Kawai Layout
 * Energy-based layout minimizing spring energy
 */
export function kamadaKawaiLayout(elements: NetworkElement[], options: LayoutOptions): NodePosition[] {
    const { nodes, adjacency } = buildGraph(elements);
    const n = nodes.length;
    
    if (n === 0) return [];
    
    const nodeIndex = new Map<string, number>();
    nodes.forEach((id, i) => nodeIndex.set(id, i));
    
    // Compute shortest path distances (BFS)
    const dist: number[][] = Array(n).fill(null).map(() => Array(n).fill(Infinity));
    nodes.forEach((_, i) => dist[i][i] = 0);
    
    nodes.forEach((node, i) => {
        adjacency.get(node)?.forEach(neighbor => {
            const j = nodeIndex.get(neighbor)!;
            dist[i][j] = 1;
        });
    });
    
    // Floyd-Warshall for all-pairs shortest paths
    for (let k = 0; k < n; k++) {
        for (let i = 0; i < n; i++) {
            for (let j = 0; j < n; j++) {
                if (dist[i][k] + dist[k][j] < dist[i][j]) {
                    dist[i][j] = dist[i][k] + dist[k][j];
                }
            }
        }
    }
    
    // Desired distance based on graph distance
    const L0 = Math.min(options.width, options.height) / (Math.max(...dist.flat().filter(d => d < Infinity)) || 1);
    
    // Initialize positions
    const pos = nodes.map(() => ({
        x: options.padding + Math.random() * (options.width - 2 * options.padding),
        y: options.padding + Math.random() * (options.height - 2 * options.padding)
    }));
    
    // Iterative optimization
    for (let iter = 0; iter < 50; iter++) {
        for (let i = 0; i < n; i++) {
            let dx = 0, dy = 0;
            
            for (let j = 0; j < n; j++) {
                if (i === j || dist[i][j] === Infinity) continue;
                
                const diffX = pos[i].x - pos[j].x;
                const diffY = pos[i].y - pos[j].y;
                const d = Math.sqrt(diffX * diffX + diffY * diffY) || 0.01;
                const l = dist[i][j] * L0;
                const k = 1 / (dist[i][j] * dist[i][j]);
                
                dx += k * (diffX - l * diffX / d);
                dy += k * (diffY - l * diffY / d);
            }
            
            const step = 5 / (iter + 1);
            pos[i].x -= dx * step;
            pos[i].y -= dy * step;
            
            // Keep in bounds
            pos[i].x = Math.max(options.padding, Math.min(options.width - options.padding, pos[i].x));
            pos[i].y = Math.max(options.padding, Math.min(options.height - options.padding, pos[i].y));
        }
    }
    
    return nodes.map((id, i) => ({
        id,
        x: Math.round(pos[i].x),
        y: Math.round(pos[i].y)
    }));
}

/**
 * 10. Fruchterman-Reingold Layout
 * Popular force-directed algorithm with temperature cooling
 */
export function fruchtermanReingoldLayout(elements: NetworkElement[], options: LayoutOptions): NodePosition[] {
    const { nodes, edges } = buildGraph(elements);
    const n = nodes.length;
    
    if (n === 0) return [];
    
    const nodeIndex = new Map<string, number>();
    nodes.forEach((id, i) => nodeIndex.set(id, i));
    
    const W = options.width - 2 * options.padding;
    const H = options.height - 2 * options.padding;
    const area = W * H;
    const k = Math.sqrt(area / n);
    
    // Initialize positions
    const pos = nodes.map(() => ({
        x: options.padding + Math.random() * W,
        y: options.padding + Math.random() * H
    }));
    
    const iterations = options.iterations || 100;
    let temperature = W / 10;
    
    for (let iter = 0; iter < iterations; iter++) {
        const disp = nodes.map(() => ({ x: 0, y: 0 }));
        
        // Repulsive forces
        for (let i = 0; i < n; i++) {
            for (let j = i + 1; j < n; j++) {
                const dx = pos[i].x - pos[j].x;
                const dy = pos[i].y - pos[j].y;
                const d = Math.sqrt(dx * dx + dy * dy) || 0.01;
                const force = (k * k) / d;
                const fx = (dx / d) * force;
                const fy = (dy / d) * force;
                disp[i].x += fx;
                disp[i].y += fy;
                disp[j].x -= fx;
                disp[j].y -= fy;
            }
        }
        
        // Attractive forces
        edges.forEach(({ source, target }) => {
            const i = nodeIndex.get(source)!;
            const j = nodeIndex.get(target)!;
            const dx = pos[i].x - pos[j].x;
            const dy = pos[i].y - pos[j].y;
            const d = Math.sqrt(dx * dx + dy * dy) || 0.01;
            const force = (d * d) / k;
            const fx = (dx / d) * force;
            const fy = (dy / d) * force;
            disp[i].x -= fx;
            disp[i].y -= fy;
            disp[j].x += fx;
            disp[j].y += fy;
        });
        
        // Apply displacement with temperature
        for (let i = 0; i < n; i++) {
            const d = Math.sqrt(disp[i].x * disp[i].x + disp[i].y * disp[i].y) || 0.01;
            const limitedD = Math.min(d, temperature);
            pos[i].x += (disp[i].x / d) * limitedD;
            pos[i].y += (disp[i].y / d) * limitedD;
            
            // Keep in frame
            pos[i].x = Math.max(options.padding, Math.min(options.width - options.padding, pos[i].x));
            pos[i].y = Math.max(options.padding, Math.min(options.height - options.padding, pos[i].y));
        }
        
        // Cool down
        temperature *= 0.95;
    }
    
    return nodes.map((id, i) => ({
        id,
        x: Math.round(pos[i].x),
        y: Math.round(pos[i].y)
    }));
}

/**
 * 11. Sugiyama Layout (Layered Graph Drawing)
 * Minimizes edge crossings in hierarchical layout
 */
export function sugiyamaLayout(elements: NetworkElement[], options: LayoutOptions): NodePosition[] {
    const { nodes, adjacency } = buildGraph(elements);
    const nodeElements = elements.filter(el => el.type !== 'line' && el.type !== 'cable');
    const roots = findRoots(elements, adjacency);
    
    // Step 1: Assign layers using longest path
    const layers = new Map<string, number>();
    const visited = new Set<string>();
    
    function assignLayer(nodeId: string, layer: number): void {
        if (visited.has(nodeId)) {
            layers.set(nodeId, Math.max(layers.get(nodeId) || 0, layer));
            return;
        }
        visited.add(nodeId);
        layers.set(nodeId, layer);
        
        adjacency.get(nodeId)?.forEach(neighbor => {
            assignLayer(neighbor, layer + 1);
        });
    }
    
    roots.forEach(r => assignLayer(r, 0));
    
    // Handle disconnected
    nodeElements.forEach(el => {
        if (!layers.has(el.id)) layers.set(el.id, 0);
    });
    
    // Step 2: Group by layer
    const layerGroups = new Map<number, string[]>();
    layers.forEach((layer, id) => {
        if (!layerGroups.has(layer)) layerGroups.set(layer, []);
        layerGroups.get(layer)!.push(id);
    });
    
    // Step 3: Order nodes within layers to minimize crossings (barycenter method)
    const numLayers = Math.max(...Array.from(layers.values())) + 1;
    
    for (let sweep = 0; sweep < 5; sweep++) {
        // Forward sweep
        for (let layer = 1; layer < numLayers; layer++) {
            const nodesInLayer = layerGroups.get(layer) || [];
            const prevLayer = layerGroups.get(layer - 1) || [];
            const prevPos = new Map<string, number>();
            prevLayer.forEach((id, i) => prevPos.set(id, i));
            
            // Calculate barycenter for each node
            const barycenters = nodesInLayer.map(id => {
                const neighbors = Array.from(adjacency.get(id) || [])
                    .filter(n => layers.get(n) === layer - 1);
                if (neighbors.length === 0) return { id, bc: 0 };
                const bc = neighbors.reduce((s, n) => s + (prevPos.get(n) || 0), 0) / neighbors.length;
                return { id, bc };
            });
            
            barycenters.sort((a, b) => a.bc - b.bc);
            layerGroups.set(layer, barycenters.map(b => b.id));
        }
        
        // Backward sweep
        for (let layer = numLayers - 2; layer >= 0; layer--) {
            const nodesInLayer = layerGroups.get(layer) || [];
            const nextLayer = layerGroups.get(layer + 1) || [];
            const nextPos = new Map<string, number>();
            nextLayer.forEach((id, i) => nextPos.set(id, i));
            
            const barycenters = nodesInLayer.map(id => {
                const neighbors = Array.from(adjacency.get(id) || [])
                    .filter(n => layers.get(n) === layer + 1);
                if (neighbors.length === 0) return { id, bc: 0 };
                const bc = neighbors.reduce((s, n) => s + (nextPos.get(n) || 0), 0) / neighbors.length;
                return { id, bc };
            });
            
            barycenters.sort((a, b) => a.bc - b.bc);
            layerGroups.set(layer, barycenters.map(b => b.id));
        }
    }
    
    // Step 4: Assign coordinates
    const layerHeight = (options.height - 2 * options.padding) / Math.max(numLayers - 1, 1);
    
    const positions: NodePosition[] = [];
    layerGroups.forEach((nodesInLayer, layer) => {
        const layerWidth = options.width - 2 * options.padding;
        const spacing = layerWidth / (nodesInLayer.length + 1);
        
        nodesInLayer.forEach((id, index) => {
            positions.push({
                id,
                x: Math.round(options.padding + spacing * (index + 1)),
                y: Math.round(options.padding + layer * layerHeight)
            });
        });
    });
    
    return positions;
}

/**
 * Apply a layout algorithm to network elements
 */
export function applyLayout(
    elements: NetworkElement[],
    algorithm: LayoutAlgorithm,
    options?: Partial<LayoutOptions>
): NetworkElement[] {
    const defaultOptions: LayoutOptions = {
        width: 6000,
        height: 4500,
        padding: 100,
        nodeSpacing: 50,
        iterations: 100
    };
    
    const opts = { ...defaultOptions, ...options };
    
    let positions: NodePosition[];
    
    switch (algorithm) {
        case 'force-directed':
            positions = forceDirectedLayout(elements, opts);
            break;
        case 'hierarchical-tb':
            positions = hierarchicalLayoutTB(elements, opts);
            break;
        case 'hierarchical-lr':
            positions = hierarchicalLayoutLR(elements, opts);
            break;
        case 'radial':
            positions = radialLayout(elements, opts);
            break;
        case 'circular':
            positions = circularLayout(elements, opts);
            break;
        case 'grid':
            positions = gridLayout(elements, opts);
            break;
        case 'tree':
            positions = treeLayout(elements, opts);
            break;
        case 'spectral':
            positions = spectralLayout(elements, opts);
            break;
        case 'kamada-kawai':
            positions = kamadaKawaiLayout(elements, opts);
            break;
        case 'fruchterman-reingold':
            positions = fruchtermanReingoldLayout(elements, opts);
            break;
        case 'sugiyama':
            positions = sugiyamaLayout(elements, opts);
            break;
        default:
            return elements;
    }
    
    // Apply positions to elements
    const posMap = new Map(positions.map(p => [p.id, p]));
    
    return elements.map(el => {
        const pos = posMap.get(el.id);
        if (pos) {
            return { ...el, x: pos.x, y: pos.y };
        }
        return el;
    });
}

export const layoutAlgorithms: { value: LayoutAlgorithm; label: string; description: string }[] = [
    { value: 'force-directed', label: 'Force Directed', description: 'Spring-based physics simulation' },
    { value: 'fruchterman-reingold', label: 'Fruchterman-Reingold', description: 'Optimized force-directed with cooling' },
    { value: 'kamada-kawai', label: 'Kamada-Kawai', description: 'Energy minimization based on graph distance' },
    { value: 'hierarchical-tb', label: 'Hierarchical (Top-Down)', description: 'Layered layout from source to loads' },
    { value: 'hierarchical-lr', label: 'Hierarchical (Left-Right)', description: 'Layered layout horizontally' },
    { value: 'sugiyama', label: 'Sugiyama', description: 'Minimizes edge crossings in layers' },
    { value: 'radial', label: 'Radial', description: 'Concentric circles from source' },
    { value: 'circular', label: 'Circular', description: 'All nodes on a single circle' },
    { value: 'tree', label: 'Tree', description: 'Balanced tree structure' },
    { value: 'grid', label: 'Grid', description: 'Regular grid arrangement' },
    { value: 'spectral', label: 'Spectral', description: 'Based on graph Laplacian eigenvectors' },
];
