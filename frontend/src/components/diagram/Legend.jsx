import React from 'react';

/**
 * Legend component for the network diagram
 * Shows voltage and loading color scales
 */
function Legend() {
    return (
        <div style={{
            position: 'absolute',
            bottom: 30,
            right: 20,
            background: 'rgba(15, 23, 42, 0.9)',
            padding: 10,
            borderRadius: 8,
            border: '1px solid #374151',
            display: 'flex',
            flexDirection: 'column',
            gap: 12,
            color: '#e2e8f0',
            fontSize: '0.75rem',
            backdropFilter: 'blur(4px)',
            zIndex: 50
        }}>
            {/* Voltage Legend */}
            <div>
                <div style={{ fontWeight: 'bold', marginBottom: 4 }}>Bus Voltage (p.u.)</div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                    <div style={{ width: 120, height: 8, background: 'linear-gradient(to right, #ef4444 0%, #ffffff 50%, #ef4444 100%)', borderRadius: 2 }}></div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', width: '100%', fontSize: '0.65rem', color: '#94a3b8' }}>
                        <span>&lt;0.95</span>
                        <span style={{ textAlign: 'center' }}>1.0</span>
                        <span>&gt;1.05</span>
                    </div>
                </div>
            </div>
            {/* Loading Legend */}
            <div>
                <div style={{ fontWeight: 'bold', marginBottom: 4 }}>Line Loading (%)</div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                    <div style={{ width: 120, height: 8, background: 'linear-gradient(to right, #00d26a 0%, #f59e0b 80%, #ef4444 100%)', borderRadius: 2 }}></div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', width: '100%', fontSize: '0.65rem', color: '#94a3b8' }}>
                        <span>0%</span>
                        <span style={{ textAlign: 'center' }}>80%</span>
                        <span>100%</span>
                    </div>
                </div>
            </div>
        </div>
    );
}

export default Legend;
