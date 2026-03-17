import React from 'react';
import {
    Minus, MoveRight, Target
} from 'lucide-react';

/**
 * MapToolsPalette - Compact vertical toolbar with map interaction tools
 * 
 * Provides quick access to map/diagram tools:
 * - Line: Horizontal line tool
 * - Arrow: Navigation/directional tool  
 * - Crosshair: Center/aim on element
 * - Transformer: Transformer symbol (zig-zag)
 * - Load: Load symbol (zig-zag red)
 */

const MAP_TOOLS = [
    { id: 'line', icon: Minus, label: 'Line', shortcut: 'L' },
    { id: 'arrow', icon: MoveRight, label: 'Arrow / Connect', shortcut: 'A' },
    { id: 'target', icon: Target, label: 'Center / Target', shortcut: 'C' },
    { id: 'transformer', icon: 'trafo', label: 'Transformer', shortcut: 'T', isCustom: true },
    { id: 'load', icon: 'load', label: 'Load', shortcut: 'R', isCustom: true, color: '#ef4444' },
];

function MapToolsPalette({
    activeTool = 'pointer',
    onToolChange,
    onCenterNetwork,
    onToggleLocations,
    onToggleViewMode,
    showLocations = true,
    viewMode = 'singleLine',
    collapsed = false
}) {

    const handleToolClick = (toolId) => {
        switch (toolId) {
            case 'pointer':
            case 'navigate':
                if (onToolChange) onToolChange(toolId);
                break;
            case 'center':
                if (onCenterNetwork) onCenterNetwork();
                break;
            case 'location':
                if (onToggleLocations) onToggleLocations();
                break;
            case 'view':
                if (onToggleViewMode) onToggleViewMode();
                break;
            default:
                break;
        }
    };

    const isToolActive = (toolId) => {
        if (toolId === 'pointer' || toolId === 'navigate') {
            return activeTool === toolId;
        }
        if (toolId === 'location') {
            return showLocations;
        }
        if (toolId === 'view') {
            return viewMode === 'spatial';
        }
        return false;
    };

    if (collapsed) {
        return null;
    }

    // Custom SVG for transformer (zig-zag / inductor pattern)
    const TransformerIcon = () => (
        <svg width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M4 10 L6 6 L8 14 L10 6 L12 14 L14 6 L16 10" />
        </svg>
    );

    // Custom SVG for load (zig-zag with arrow down)
    const LoadIcon = () => (
        <svg width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="#ef4444" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M4 4 L6 8 L8 4 L10 8 L12 4 L14 8 L16 4" />
            <path d="M10 10 L10 16" />
            <path d="M7 13 L10 16 L13 13" />
        </svg>
    );

    const renderIcon = (tool) => {
        if (tool.isCustom) {
            if (tool.id === 'transformer') {
                return <TransformerIcon />;
            }
            if (tool.id === 'load') {
                return <LoadIcon />;
            }
        }
        const IconComponent = tool.icon;
        return <IconComponent size={20} />;
    };

    return (
        <div className="map-tools-palette">
            {MAP_TOOLS.map(tool => {
                const isActive = isToolActive(tool.id);

                return (
                    <button
                        key={tool.id}
                        className={`map-tool-btn ${isActive ? 'active' : ''} ${tool.id === 'load' ? 'load-symbol' : ''}`}
                        onClick={() => handleToolClick(tool.id)}
                        title={`${tool.label} (${tool.shortcut})`}
                        aria-label={tool.label}
                    >
                        {renderIcon(tool)}
                    </button>
                );
            })}
        </div>
    );
}

export default MapToolsPalette;
