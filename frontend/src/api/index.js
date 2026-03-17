// API Service for PandaPower Network Editor
// Extracted from App.js

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || process.env.REACT_APP_API_URL || '';

const api = {
    // Health Check
    checkHealth: () => fetch(`${BACKEND_URL}/api/health`).then(r => r.json()),

    // Networks
    createNetwork: (data) => fetch(`${BACKEND_URL}/api/networks`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data)
    }).then(r => r.json()),

    getNetwork: (id) => fetch(`${BACKEND_URL}/api/networks/${id}`).then(r => r.json()),
    deleteNetwork: (id) => fetch(`${BACKEND_URL}/api/networks/${id}`, { method: 'DELETE' }).then(r => r.json()),
    listNetworks: () => fetch(`${BACKEND_URL}/api/networks`).then(r => r.json()),
    getStatistics: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/statistics`).then(r => r.json()),
    getTopology: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/topology`).then(r => r.json()),
    validateNetwork: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/validate`).then(r => r.json()),
    exportNetwork: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/export`).then(r => r.json()),
    importNetwork: (data) => fetch(`${BACKEND_URL}/api/networks/import`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data)
    }).then(r => r.json()),
    getAllElements: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/elements`).then(r => r.json()),
    getMeasurements: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/measurements`).then(r => r.json()),

    // Sample Networks
    listSamples: () => fetch(`${BACKEND_URL}/api/sample-networks`).then(r => r.json()),
    loadSample: (id) => fetch(`${BACKEND_URL}/api/sample-networks/${id}/load`, { method: 'POST' }).then(r => r.json()),

    // Elements
    getBuses: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/buses`).then(r => r.json()),
    createBus: (id, data) => fetch(`${BACKEND_URL}/api/networks/${id}/buses`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data)
    }).then(r => r.json()),
    deleteBus: (id, busId) => fetch(`${BACKEND_URL}/api/networks/${id}/buses/${busId}`, { method: 'DELETE' }).then(r => r.json()),

    getLines: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/lines`).then(r => r.json()),
    createLine: (id, data) => fetch(`${BACKEND_URL}/api/networks/${id}/lines`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data)
    }).then(r => r.json()),
    deleteLine: (id, lineId) => fetch(`${BACKEND_URL}/api/networks/${id}/lines/${lineId}`, { method: 'DELETE' }).then(r => r.json()),

    getTransformers: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/transformers`).then(r => r.json()),
    createTransformer: (id, data) => fetch(`${BACKEND_URL}/api/networks/${id}/transformers`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data)
    }).then(r => r.json()),

    getTrafo3w: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/trafo3w`).then(r => r.json()),
    getSwitches: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/switches`).then(r => r.json()),

    getLoads: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/loads`).then(r => r.json()),
    createLoad: (id, data) => fetch(`${BACKEND_URL}/api/networks/${id}/loads`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data)
    }).then(r => r.json()),
    deleteLoad: (id, loadId) => fetch(`${BACKEND_URL}/api/networks/${id}/loads/${loadId}`, { method: 'DELETE' }).then(r => r.json()),
    getAsymmetricLoads: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/asymmetric-loads`).then(r => r.json()),

    getGenerators: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/generators`).then(r => r.json()),
    createGenerator: (id, data) => fetch(`${BACKEND_URL}/api/networks/${id}/generators`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data)
    }).then(r => r.json()),

    getStaticGens: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/static-generators`).then(r => r.json()),
    getMotors: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/motors`).then(r => r.json()),

    getExtGrids: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/external-grids`).then(r => r.json()),
    createExtGrid: (id, data) => fetch(`${BACKEND_URL}/api/networks/${id}/external-grids`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data)
    }).then(r => r.json()),

    // DC Elements
    getDclines: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/dclines`).then(r => r.json()),
    getStorages: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/storages`).then(r => r.json()),

    // FACTS/Other
    getShunts: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/shunts`).then(r => r.json()),
    getAsymmetricSgens: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/asymmetric-sgens`).then(r => r.json()),
    getSvcs: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/svcs`).then(r => r.json()),
    getTcscs: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/tcscs`).then(r => r.json()),
    getSscs: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/sscs`).then(r => r.json()),
    getWards: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/wards`).then(r => r.json()),
    getXwards: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/xwards`).then(r => r.json()),
    getImpedances: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/impedances`).then(r => r.json()),

    // TCGM GIS
    getTcgmNodes: (groupid) => {
        const url = groupid ? `${BACKEND_URL}/api/tcgm/nodes?groupid=${encodeURIComponent(groupid)}` : `${BACKEND_URL}/api/tcgm/nodes`;
        return fetch(url).then(r => r.json());
    },

    // Standard Types
    getLineTypes: () => fetch(`${BACKEND_URL}/api/standard-types/lines`).then(r => r.json()),
    getTrafoTypes: () => fetch(`${BACKEND_URL}/api/standard-types/transformers`).then(r => r.json()),

    // Analysis
    runPowerFlow: (id, options = {}) => fetch(`${BACKEND_URL}/api/networks/${id}/analysis/powerflow`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(options)
    }).then(r => r.json()),

    runLoadAllocation: (id, options = {}) => fetch(`${BACKEND_URL}/api/networks/${id}/analysis/load-allocation`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(options)
    }).then(r => r.json()),

    runDCPowerFlow: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/analysis/powerflow-dc`, {
        method: 'POST'
    }).then(r => r.json()),

    runShortCircuit: (id, options = {}) => fetch(`${BACKEND_URL}/api/networks/${id}/analysis/short-circuit`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(options)
    }).then(r => r.json()),

    runOPF: (id, options = {}) => fetch(`${BACKEND_URL}/api/networks/${id}/analysis/opf`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(options)
    }).then(r => r.json()),

    runHostingCapacity: (options = {}) => fetch(`${BACKEND_URL}/api/analysis/hosting-capacity`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(options)
    }).then(r => r.json()),

    runTimeSeries: (id, options = {}) => fetch(`${BACKEND_URL}/api/networks/${id}/analysis/time-series`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(options)
    }).then(r => r.json()),

    // Version Control
    saveNetworkVersion: (id, description) => fetch(`${BACKEND_URL}/api/networks/${id}/save`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ description })
    }).then(r => r.json()),

    getNetworkHistory: (id) => fetch(`${BACKEND_URL}/api/networks/${id}/history`).then(r => r.json()),

    restoreNetworkVersion: (id, versionId) => fetch(`${BACKEND_URL}/api/networks/${id}/load/${versionId}`, {
        method: 'POST'
    }).then(r => r.json()),

    // Generic Element Management
    createElement: (networkId, type, data) => fetch(`${BACKEND_URL}/api/networks/${networkId}/elements/${type}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data)
    }).then(r => r.json()),

    deleteElement: (networkId, type, index) =>
        fetch(`${BACKEND_URL}/api/networks/${networkId}/elements/${type}/${index}`, { method: 'DELETE' }).then(res => res.json()),

    updateLayout: (networkId, layoutData) =>
        fetch(`${BACKEND_URL}/api/networks/${networkId}/layout`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(layoutData),
        }).then(res => res.json()),

    // Pandapower Generated Models
    listPandapowerModels: () => fetch(`${BACKEND_URL}/api/pandapower/models`).then(r => r.json()),
    loadPandapowerModel: (name) => fetch(`${BACKEND_URL}/api/pandapower/load/${name}`, { method: 'POST' }).then(r => r.json()),
};

export default api;
