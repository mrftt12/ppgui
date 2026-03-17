// Color utility functions for power system visualization
// Extracted from App.js

/**
 * Returns a color based on line loading percentage.
 * Green (0%) -> Orange (80%) -> Red (100%+)
 */
export const colorForLoading = (loading) => {
    if (!Number.isFinite(loading)) return '#3b82f6'; // Default blue

    if (loading < 0) return '#3b82f6';
    if (loading <= 80) {
        // Green to Orange gradient
        const t = loading / 80;
        const r = Math.floor(0 + t * 245);
        const g = Math.floor(210 - t * 52);
        const b = Math.floor(106 - t * 89);
        return `rgb(${r}, ${g}, ${b})`;
    }
    if (loading <= 100) {
        // Orange to Red gradient
        const t = (loading - 80) / 20;
        const r = Math.floor(245 - t * 6);
        const g = Math.floor(158 - t * 90);
        const b = Math.floor(17 + t * 51);
        return `rgb(${r}, ${g}, ${b})`;
    }
    return '#ef4444'; // Red for overloaded
};

/**
 * Returns a color based on voltage magnitude (p.u.).
 * Red (<0.95) -> White (1.0) -> Red (>1.05)
 */
export const colorForVoltage = (vm) => {
    if (!Number.isFinite(vm)) return '#1a1a2e';
    if (vm < 0.95 || vm > 1.05) return '#ff0000';

    // Gradient: Red (0.95) -> White (1.0) -> Red (1.05)
    // At 1.0, return white
    const deviation = Math.abs(vm - 1.0);
    const maxDeviation = 0.05;
    const t = Math.min(1, deviation / maxDeviation);
    const r = 255;
    const g = Math.floor(255 * (1 - t));
    const b = Math.floor(255 * (1 - t));
    return `rgb(${r}, ${g}, ${b})`;
};

export default { colorForLoading, colorForVoltage };
