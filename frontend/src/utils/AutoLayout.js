import ELK from 'elkjs/lib/elk.bundled';

const elk = new ELK();

/**
 * Applies auto-layout using ELK.js
 * @param {Object} elements - The categorized elements object
 * @param {Object} currentPositions - Current node positions (optional, might be used for constraints)
 * @returns {Promise<Object>} - New node positions map { busId: { x, y } }
 */
export const applyAutoLayout = async (elements, currentPositions, algorithm = 'radial') => {
    if (!elements || !elements.bus || elements.bus.length === 0) {
        return currentPositions;
    }

    // 1. Build Graph for ELK
    // Nodes: Buses
    // Edges: Lines and Transformers

    const nodes = elements.bus.map(bus => ({
        id: bus.index.toString(),
        width: 40,
        height: 40,
        // Add layout options for node if needed
    }));

    const edges = [];

    if (elements.line) {
        elements.line.forEach(line => {
            if (line.from_bus !== undefined && line.to_bus !== undefined) {
                edges.push({
                    id: `line-${line.index}`,
                    sources: [line.from_bus.toString()],
                    targets: [line.to_bus.toString()]
                });
            }
        });
    }

    if (elements.trafo) {
        elements.trafo.forEach(trafo => {
            if (trafo.hv_bus !== undefined && trafo.lv_bus !== undefined) {
                edges.push({
                    id: `trafo-${trafo.index}`,
                    sources: [trafo.hv_bus.toString()],
                    targets: [trafo.lv_bus.toString()]
                });
            }
        });
    }

    // Map user friendly names to ELK algorithms
    const algoMap = {
        'radial': 'radial',
        'layered': 'layered',
        'mrtree': 'mrtree',
        'force': 'force',
        'stress': 'stress' // elk.stress usually mapped from force/stress
    };

    const elkAlgo = algoMap[algorithm] || 'radial';

    const graph = {
        id: "root",
        layoutOptions: {
            'elk.algorithm': elkAlgo,
            'elk.direction': 'DOWN',
            'elk.spacing.nodeNode': '80',
            'elk.spacing.edgeNode': '40',
            'elk.layered.spacing.nodeNodeBetweenLayers': '80',
            // Radial specific
            'elk.radial.radius': '300'
        },
        children: nodes,
        edges: edges
    };

    try {
        const layoutedGraph = await elk.layout(graph);

        // Map back to our position format
        const newPositions = {};
        layoutedGraph.children.forEach(node => {
            // Center the node (ELK gives top-left)
            newPositions[parseInt(node.id)] = {
                x: node.x + node.width / 2,
                y: node.y + node.height / 2
            };
        });

        return newPositions;

    } catch (err) {
        console.error("ELK Layout Failed:", err);
        throw err;
    }
};
