import React from 'react';
import {
    Circle, Square, Zap, Activity, Battery,
    GitBranch, Radio, Box
} from 'lucide-react';

/**
 * SymbolPalette - Dockable panel with draggable power system symbols
 * 
 * Drag symbols onto the canvas to create new elements
 */

const SYMBOL_CATEGORIES = [
    {
        name: 'Sources',
        items: [
            { type: 'ext_grid', label: 'External Grid', icon: Zap, color: '#00d26a' },
        ]
    },
    {
        name: 'Equipment',
        items: [
            { type: 'bus', label: 'Bus', icon: Square, color: '#ffffff' },
            { type: 'line', label: 'Line', icon: GitBranch, color: '#60a5fa' },
            { type: 'trafo', label: 'Transformer', icon: Circle, color: '#f59e0b' },
            { type: 'switch', label: 'Switch', icon: Radio, color: '#a78bfa' },
        ]
    },
    {
        name: 'Loads',
        items: [
            { type: 'load', label: 'Load', icon: Activity, color: '#ef4444' },
        ]
    },
    {
        name: 'Generation',
        items: [
            { type: 'gen', label: 'Generator', icon: Circle, color: '#3b82f6' },
            { type: 'sgen', label: 'Static Gen', icon: Box, color: '#22d3ee' },
        ]
    },
    {
        name: 'Storage',
        items: [
            { type: 'storage', label: 'Storage', icon: Battery, color: '#84cc16' },
        ]
    },
];

function SymbolPalette({ onDragStart, onCreateElement, collapsed = false }) {

    const handleDragStart = (e, symbolType) => {
        e.dataTransfer.setData('application/x-symbol-type', symbolType);
        e.dataTransfer.effectAllowed = 'copy';

        // Optional callback
        if (onDragStart) {
            onDragStart(symbolType);
        }
    };

    const handleDoubleClick = (symbolType) => {
        // Quick-add at default position
        if (onCreateElement) {
            onCreateElement(symbolType, { x: 200, y: 200 });
        }
    };

    if (collapsed) {
        return null;
    }

    return (
        <div className="symbol-palette">
            <div className="symbol-palette-header">
                <span>Symbols</span>
            </div>
            <div className="symbol-palette-content">
                {SYMBOL_CATEGORIES.map(category => (
                    <div key={category.name} className="symbol-category">
                        <div className="symbol-category-label">{category.name}</div>
                        <div className="symbol-grid">
                            {category.items.map(item => {
                                const IconComponent = item.icon;
                                return (
                                    <div
                                        key={item.type}
                                        className="symbol-item"
                                        draggable
                                        onDragStart={(e) => handleDragStart(e, item.type)}
                                        onDoubleClick={() => handleDoubleClick(item.type)}
                                        title={`Drag to add ${item.label}\nDouble-click to quick add`}
                                    >
                                        <div
                                            className="symbol-icon"
                                            style={{ color: item.color }}
                                        >
                                            <IconComponent size={20} />
                                        </div>
                                        <div className="symbol-label">{item.label}</div>
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                ))}
            </div>
            <div className="symbol-palette-footer">
                <span style={{ fontSize: '0.7rem', color: '#6b7280' }}>
                    Drag onto canvas or double-click
                </span>
            </div>
        </div>
    );
}

export default SymbolPalette;
