import React from 'react';
import { Settings, RotateCcw } from 'lucide-react';
import { Modal } from '../common/Modal';
import { useSettings } from '../../contexts/SettingsContext';

/**
 * Settings Modal - Configure system frequency, violation criteria, grid options
 */
function SettingsModal({ onClose }) {
    const { settings, updateSettings, resetSettings } = useSettings();

    const handleChange = (key) => (e) => {
        const value = e.target.type === 'checkbox'
            ? e.target.checked
            : e.target.type === 'number'
                ? parseFloat(e.target.value)
                : e.target.value;
        updateSettings({ [key]: value });
    };

    return (
        <Modal title="Settings" onClose={onClose}>
            <div className="modal-body" style={{ maxHeight: '70vh', overflow: 'auto' }}>

                {/* System Settings */}
                <div style={{ marginBottom: 20 }}>
                    <h4 style={{ color: '#9ca3af', fontSize: '0.8rem', textTransform: 'uppercase', marginBottom: 12, display: 'flex', alignItems: 'center', gap: 6 }}>
                        <Settings size={14} /> System
                    </h4>

                    <div className="form-group" style={{ marginBottom: 12 }}>
                        <label style={{ fontSize: '0.85rem', color: '#e5e7eb', marginBottom: 4, display: 'block' }}>
                            System Frequency (Hz)
                        </label>
                        <select
                            value={settings.frequency}
                            onChange={handleChange('frequency')}
                            style={{ width: '100%', padding: 8, background: '#0f172a', border: '1px solid #374151', borderRadius: 4, color: '#e5e7eb' }}
                        >
                            <option value={50}>50 Hz</option>
                            <option value={60}>60 Hz</option>
                        </select>
                    </div>
                </div>

                {/* Violation Criteria */}
                <div style={{ marginBottom: 20 }}>
                    <h4 style={{ color: '#9ca3af', fontSize: '0.8rem', textTransform: 'uppercase', marginBottom: 12 }}>
                        Violation Criteria
                    </h4>

                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                        <div className="form-group">
                            <label style={{ fontSize: '0.85rem', color: '#e5e7eb', marginBottom: 4, display: 'block' }}>
                                Min Bus Voltage (p.u.)
                            </label>
                            <input
                                type="number"
                                step="0.01"
                                min="0.8"
                                max="1.0"
                                value={settings.busVoltageMin}
                                onChange={handleChange('busVoltageMin')}
                                style={{ width: '100%', padding: 8, background: '#0f172a', border: '1px solid #374151', borderRadius: 4, color: '#e5e7eb' }}
                            />
                        </div>

                        <div className="form-group">
                            <label style={{ fontSize: '0.85rem', color: '#e5e7eb', marginBottom: 4, display: 'block' }}>
                                Max Bus Voltage (p.u.)
                            </label>
                            <input
                                type="number"
                                step="0.01"
                                min="1.0"
                                max="1.2"
                                value={settings.busVoltageMax}
                                onChange={handleChange('busVoltageMax')}
                                style={{ width: '100%', padding: 8, background: '#0f172a', border: '1px solid #374151', borderRadius: 4, color: '#e5e7eb' }}
                            />
                        </div>
                    </div>

                    <div className="form-group" style={{ marginTop: 12 }}>
                        <label style={{ fontSize: '0.85rem', color: '#e5e7eb', marginBottom: 4, display: 'block' }}>
                            Line Loading Limit (%)
                        </label>
                        <input
                            type="number"
                            step="1"
                            min="50"
                            max="150"
                            value={settings.lineLoadingLimit}
                            onChange={handleChange('lineLoadingLimit')}
                            style={{ width: '100%', padding: 8, background: '#0f172a', border: '1px solid #374151', borderRadius: 4, color: '#e5e7eb' }}
                        />
                    </div>
                </div>

                {/* Grid & Snap */}
                <div style={{ marginBottom: 20 }}>
                    <h4 style={{ color: '#9ca3af', fontSize: '0.8rem', textTransform: 'uppercase', marginBottom: 12 }}>
                        Grid & Alignment
                    </h4>

                    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                        <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
                            <input
                                type="checkbox"
                                checked={settings.gridVisible}
                                onChange={handleChange('gridVisible')}
                            />
                            <span style={{ fontSize: '0.85rem', color: '#e5e7eb' }}>Show Grid</span>
                        </label>

                        <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
                            <input
                                type="checkbox"
                                checked={settings.snapToGrid}
                                onChange={handleChange('snapToGrid')}
                            />
                            <span style={{ fontSize: '0.85rem', color: '#e5e7eb' }}>Snap to Grid</span>
                        </label>

                        <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
                            <input
                                type="checkbox"
                                checked={settings.snapToObject}
                                onChange={handleChange('snapToObject')}
                            />
                            <span style={{ fontSize: '0.85rem', color: '#e5e7eb' }}>Snap to Objects</span>
                        </label>
                    </div>

                    <div className="form-group" style={{ marginTop: 12 }}>
                        <label style={{ fontSize: '0.85rem', color: '#e5e7eb', marginBottom: 4, display: 'block' }}>
                            Grid Size (px)
                        </label>
                        <input
                            type="number"
                            step="5"
                            min="10"
                            max="100"
                            value={settings.gridSize}
                            onChange={handleChange('gridSize')}
                            style={{ width: '100%', padding: 8, background: '#0f172a', border: '1px solid #374151', borderRadius: 4, color: '#e5e7eb' }}
                        />
                    </div>
                </div>

                {/* Display Options */}
                <div style={{ marginBottom: 20 }}>
                    <h4 style={{ color: '#9ca3af', fontSize: '0.8rem', textTransform: 'uppercase', marginBottom: 12 }}>
                        Display Options
                    </h4>

                    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                        <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
                            <input
                                type="checkbox"
                                checked={settings.showBusIds}
                                onChange={handleChange('showBusIds')}
                            />
                            <span style={{ fontSize: '0.85rem', color: '#e5e7eb' }}>Show Bus IDs</span>
                        </label>

                        <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
                            <input
                                type="checkbox"
                                checked={settings.showVoltage}
                                onChange={handleChange('showVoltage')}
                            />
                            <span style={{ fontSize: '0.85rem', color: '#e5e7eb' }}>Show Voltage Values</span>
                        </label>

                        <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
                            <input
                                type="checkbox"
                                checked={settings.showLoading}
                                onChange={handleChange('showLoading')}
                            />
                            <span style={{ fontSize: '0.85rem', color: '#e5e7eb' }}>Show Line Loading</span>
                        </label>
                    </div>

                    <div className="form-group" style={{ marginTop: 12 }}>
                        <label style={{ fontSize: '0.85rem', color: '#e5e7eb', marginBottom: 4, display: 'block' }}>
                            Label Font Size
                        </label>
                        <input
                            type="number"
                            step="1"
                            min="8"
                            max="18"
                            value={settings.labelFontSize}
                            onChange={handleChange('labelFontSize')}
                            style={{ width: '100%', padding: 8, background: '#0f172a', border: '1px solid #374151', borderRadius: 4, color: '#e5e7eb' }}
                        />
                    </div>
                </div>

                {/* Symbol Standard */}
                <div style={{ marginBottom: 20 }}>
                    <h4 style={{ color: '#9ca3af', fontSize: '0.8rem', textTransform: 'uppercase', marginBottom: 12 }}>
                        Symbol Standard
                    </h4>

                    <select
                        value={settings.symbolStandard || 'Standard'}
                        onChange={handleChange('symbolStandard')}
                        style={{ width: '100%', padding: 8, background: '#0f172a', border: '1px solid #374151', borderRadius: 4, color: '#e5e7eb' }}
                    >
                        <option value="Standard">Standard (US Reference)</option>
                        <option value="IEC">IEC (International)</option>
                        <option value="ANSI">ANSI (Legacy)</option>
                    </select>
                </div>

            </div>

            <div className="modal-footer" style={{ display: 'flex', justifyContent: 'space-between' }}>
                <button
                    className="btn btn-secondary"
                    onClick={resetSettings}
                    style={{ display: 'flex', alignItems: 'center', gap: 6 }}
                >
                    <RotateCcw size={14} /> Reset to Defaults
                </button>
                <button className="btn btn-primary" onClick={onClose}>
                    Done
                </button>
            </div>
        </Modal>
    );
}

export default SettingsModal;
