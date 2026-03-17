import React from 'react';
import { Eye, EyeOff, Layers } from 'lucide-react';

/**
 * LayersPanel - Controls visibility of different diagram layers
 */
function LayersPanel({ layers, toggleLayer }) {
    const layerDefinitions = [
        { id: 'buses', label: 'Buses' },
        { id: 'lines', label: 'Lines' },
        { id: 'loads', label: 'Loads' },
        { id: 'generators', label: 'Generators' },
        { id: 'labels', label: 'Labels' },
        { id: 'grid', label: 'Grid' },
    ];

    return (
        <div className="layers-panel" style={{
            position: 'absolute',
            top: 60,
            right: 20,
            background: '#1a1a2e',
            border: '1px solid #2a3a5c',
            borderRadius: 8,
            padding: 12,
            zIndex: 100,
            width: 180,
            boxShadow: '0 4px 6px rgba(0, 0, 0, 0.3)'
        }}>
            <div style={{
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                marginBottom: 12,
                paddingBottom: 8,
                borderBottom: '1px solid #2a3a5c',
                color: '#e5e7eb',
                fontWeight: 600,
                fontSize: '0.85rem'
            }}>
                <Layers size={16} />
                <span>Layers</span>
            </div>

            <div className="layers-list" style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {layerDefinitions.map(layer => {
                    const isVisible = layers[layer.id];
                    return (
                        <div
                            key={layer.id}
                            className="layer-item"
                            onClick={() => toggleLayer(layer.id)}
                            style={{
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'space-between',
                                padding: '6px 8px',
                                borderRadius: 4,
                                cursor: 'pointer',
                                background: isVisible ? 'rgba(59, 130, 246, 0.1)' : 'transparent',
                                color: isVisible ? '#e5e7eb' : '#6b7280',
                                transition: 'all 0.2s'
                            }}
                        >
                            <span style={{ fontSize: '0.8rem' }}>{layer.label}</span>
                            {isVisible ? <Eye size={14} /> : <EyeOff size={14} />}
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

export default LayersPanel;
