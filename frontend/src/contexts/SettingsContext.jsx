import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';

const STORAGE_KEY = 'loadflow_settings';

// Default settings
const defaultSettings = {
    // System
    frequency: 60, // Hz

    // Violation Criteria
    busVoltageMin: 0.95, // p.u.
    busVoltageMax: 1.05, // p.u.
    lineLoadingLimit: 100, // %

    // Grid & Snap
    gridVisible: false,
    gridSize: 20, // pixels
    snapToGrid: false,
    snapToObject: false,

    // Labels & Display
    showBusIds: true,
    showVoltage: true,
    showLoading: true,
    labelFontSize: 11,

    // Symbol Standard
    symbolStandard: 'Standard', // 'Standard' | 'IEC' | 'ANSI'
};

const SettingsContext = createContext(null);

export function SettingsProvider({ children }) {
    const [settings, setSettings] = useState(() => {
        try {
            const saved = localStorage.getItem(STORAGE_KEY);
            if (saved) {
                return { ...defaultSettings, ...JSON.parse(saved) };
            }
        } catch (e) {
            console.warn('Failed to load settings from localStorage:', e);
        }
        return defaultSettings;
    });

    // Persist to localStorage when settings change
    useEffect(() => {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
        } catch (e) {
            console.warn('Failed to save settings to localStorage:', e);
        }
    }, [settings]);

    const updateSetting = useCallback((key, value) => {
        setSettings(prev => ({ ...prev, [key]: value }));
    }, []);

    const updateSettings = useCallback((updates) => {
        setSettings(prev => ({ ...prev, ...updates }));
    }, []);

    const resetSettings = useCallback(() => {
        setSettings(defaultSettings);
    }, []);

    const value = {
        settings,
        updateSetting,
        updateSettings,
        resetSettings,
        defaultSettings,
    };

    return (
        <SettingsContext.Provider value={value}>
            {children}
        </SettingsContext.Provider>
    );
}

export function useSettings() {
    const context = useContext(SettingsContext);
    if (!context) {
        throw new Error('useSettings must be used within a SettingsProvider');
    }
    return context;
}

export default SettingsContext;
