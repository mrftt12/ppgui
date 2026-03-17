import React from 'react';
import { BarChart2, AlertTriangle, CheckCircle, XCircle } from 'lucide-react';

/**
 * ResultsPanel - Displays analysis results (Power Flow, Short Circuit, Hosting Capacity, Time Series)
 */
function ResultsPanel({
    analysisResults,
    activeResultTab,
    setActiveResultTab,
    renderDataTable,
    renderTimeSeriesChart
}) {
    if (!analysisResults) {
        return (
            <div style={{ padding: 40, textAlign: 'center', color: '#6b7280' }}>
                <BarChart2 size={32} style={{ marginBottom: 8, opacity: 0.5 }} />
                <div>Run an analysis to see results</div>
            </div>
        );
    }

    // Hosting Capacity Results
    if (analysisResults.type === 'hosting') {
        const hosting = analysisResults.hosting || {};
        if (!analysisResults.success || !hosting.success) {
            return (
                <div style={{ padding: 40, textAlign: 'center', color: '#ef4444' }}>
                    <AlertTriangle size={32} style={{ marginBottom: 8 }} />
                    <div style={{ fontWeight: 500, marginBottom: 8 }}>Hosting Capacity Failed</div>
                    <div style={{ fontSize: '0.875rem', color: '#9ca3af' }}>{hosting.error || analysisResults.error || 'Unknown error occurred'}</div>
                </div>
            );
        }

        const stats = hosting.stats || {};
        const violationCounts = hosting.violation_counts || {};
        const imageSrc = hosting.image_base64 ? `data:${hosting.image_mime || 'image/png'};base64,${hosting.image_base64}` : null;
        const installedSeries = Array.isArray(hosting.installed) ? hosting.installed : [];
        const violationSeries = Array.isArray(hosting.violations) ? hosting.violations : [];
        const usedNetworkLabel = hosting.used_network || 'mv_oberrhein';
        const iterationRows = installedSeries.map((val, idx) => ({
            iteration: idx + 1,
            installed: (() => {
                if (typeof val === 'number') return val;
                const parsed = Number(val);
                return Number.isFinite(parsed) ? parsed : val;
            })(),
            violation: violationSeries[idx] || '-',
        }));

        const hostingTabs = [
            { key: 'hosting_summary', label: 'Summary' },
            { key: 'hosting_iterations', label: 'Iterations', count: iterationRows.length },
        ];
        const activeTab = hostingTabs.find(t => t.key === activeResultTab) || hostingTabs[0];

        return (
            <>
                <div className="results-tabs">
                    {hostingTabs.map(tab => (
                        <div
                            key={tab.key}
                            className={`results-tab ${activeTab.key === tab.key ? 'active' : ''}`}
                            onClick={() => setActiveResultTab(tab.key)}
                        >
                            {tab.label}{typeof tab.count === 'number' ? ` (${tab.count})` : ''}
                        </div>
                    ))}
                    <div style={{ marginLeft: 'auto', padding: '8px 16px', color: '#9ca3af', fontSize: '0.85rem' }}>
                        Network: {usedNetworkLabel}
                    </div>
                </div>
                <div className="results-content">
                    {activeTab.key === 'hosting_summary' ? (
                        <div className="hosting-results" style={{ padding: 16, color: '#e5e7eb' }}>
                            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 16 }}>
                                <div style={{ background: '#0f172a', borderRadius: 8, padding: 12 }}>
                                    <div style={{ fontSize: '0.8rem', color: '#9ca3af' }}>Mean</div>
                                    <div style={{ fontSize: '1.25rem', fontWeight: 700 }}>{stats.mean?.toFixed ? stats.mean.toFixed(3) : '-'} MW</div>
                                </div>
                                <div style={{ background: '#0f172a', borderRadius: 8, padding: 12 }}>
                                    <div style={{ fontSize: '0.8rem', color: '#9ca3af' }}>Median</div>
                                    <div style={{ fontSize: '1.25rem', fontWeight: 700 }}>{stats.median?.toFixed ? stats.median.toFixed(3) : '-'} MW</div>
                                </div>
                                <div style={{ background: '#0f172a', borderRadius: 8, padding: 12 }}>
                                    <div style={{ fontSize: '1.25rem', fontWeight: 700 }}>{stats.max?.toFixed ? stats.max.toFixed(3) : '-'} MW</div>
                                </div>
                                <div style={{ background: '#0f172a', borderRadius: 8, padding: 12 }}>
                                    <div style={{ fontSize: '0.8rem', color: '#9ca3af' }}>Min</div>
                                    <div style={{ fontSize: '1.25rem', fontWeight: 700 }}>{stats.min?.toFixed ? stats.min.toFixed(3) : '-'} MW</div>
                                </div>
                                <div style={{ background: '#0f172a', borderRadius: 8, padding: 12 }}>
                                    <div style={{ fontSize: '0.8rem', color: '#9ca3af' }}>Iterations</div>
                                    <div style={{ fontSize: '1.25rem', fontWeight: 700 }}>{stats.iterations || '-'}</div>
                                </div>
                            </div>

                            <div style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: 16, alignItems: 'start' }}>
                                <div style={{ background: '#0f172a', borderRadius: 8, padding: 12, minHeight: 300 }}>
                                    {imageSrc ? (
                                        <img src={imageSrc} alt="Hosting capacity charts" style={{ width: '100%', borderRadius: 6 }} />
                                    ) : (
                                        <div style={{ textAlign: 'center', padding: 24, color: '#6b7280' }}>Chart unavailable</div>
                                    )}
                                </div>
                                <div style={{ background: '#0f172a', borderRadius: 8, padding: 12 }}>
                                    <div style={{ fontWeight: 600, marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}>
                                        <AlertTriangle size={14} /> Violation Breakdown
                                    </div>
                                    {Object.keys(violationCounts).length === 0 ? (
                                        <div style={{ color: '#6b7280' }}>No violations recorded</div>
                                    ) : (
                                        <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
                                            {Object.entries(violationCounts).map(([k, v]) => (
                                                <li key={k} style={{ display: 'flex', justifyContent: 'space-between', padding: '6px 0', borderBottom: '1px solid #1f2937' }}>
                                                    <span>{k}</span>
                                                    <span style={{ color: '#93c5fd' }}>{v}</span>
                                                </li>
                                            ))}
                                        </ul>
                                    )}
                                </div>
                            </div>
                        </div>
                    ) : (
                        <div style={{ padding: 12 }}>
                            {renderDataTable(iterationRows, 'hosting_iterations')}
                        </div>
                    )}
                </div>
            </>
        );
    }

    // Time Series Results
    if (analysisResults.type === 'time_series') {
        const inputs = analysisResults.inputs || {};
        const results = analysisResults.results || {};
        const tsTabs = [
            { key: 'ts_inputs', label: 'Input Profiles', data: inputs.profiles || [] },
            { key: 'ts_bus', label: 'Bus Voltages', data: results.res_bus || [] },
            { key: 'ts_line', label: 'Line Loading', data: results.res_line || [] },
            { key: 'ts_load', label: 'Load Results', data: results.res_load || [] },
            { key: 'ts_sgen', label: 'SGen Results', data: results.res_sgen || [] },
            { key: 'ts_ext', label: 'External Grid', data: results.res_ext_grid || [] },
            { key: 'ts_chart', label: 'Chart', data: [] },
        ].filter(tab => tab.key === 'ts_chart' || tab.key === 'ts_inputs' || (tab.data && tab.data.length > 0));

        const activeTab = tsTabs.find(t => t.key === activeResultTab) || tsTabs[0];
        const activeData = activeTab?.data || [];
        const convergence = results.convergence || [];
        const convergedCount = convergence.filter(c => c.converged).length;

        return (
            <>
                <div className="results-tabs">
                    {tsTabs.map(tab => (
                        <div
                            key={tab.key}
                            className={`results-tab ${activeTab?.key === tab.key ? 'active' : ''}`}
                            onClick={() => setActiveResultTab(tab.key)}
                        >
                            {tab.label} ({tab.data?.length || 0})
                        </div>
                    ))}
                    <div style={{ marginLeft: 'auto', padding: '8px 16px', display: 'flex', alignItems: 'center', gap: 10 }}>
                        <span style={{ fontSize: '0.85rem', color: '#9ca3af' }}>
                            Steps: {inputs.timesteps || activeData.length}
                        </span>
                        <span className={`converged-badge ${results.converged ? 'success' : 'error'}`}>
                            {convergedCount}/{inputs.timesteps || convergence.length || '…'} converged
                        </span>
                    </div>
                </div>
                <div className="results-content">
                    {activeTab?.key === 'ts_chart'
                        ? <div style={{ padding: 12 }}>{renderTimeSeriesChart(results)}</div>
                        : renderDataTable(activeData, activeTab?.key)}
                    {!results.converged && results.errors?.length > 0 && (
                        <div style={{ marginTop: 12, padding: 12, background: '#1b2439', borderRadius: 6, color: '#f87171', fontSize: '0.9rem' }}>
                            Some steps failed: {results.errors.slice(0, 3).map(e => `t=${e.timestep}`).join(', ')}
                        </div>
                    )}
                </div>
            </>
        );
    }

    // Power Flow / Short Circuit Results
    const { results, type, success, error } = analysisResults;

    if (!success || !results) {
        return (
            <div style={{ padding: 40, textAlign: 'center', color: '#ef4444' }}>
                <AlertTriangle size={32} style={{ marginBottom: 8 }} />
                <div style={{ fontWeight: 500, marginBottom: 8 }}>Analysis Failed</div>
                <div style={{ fontSize: '0.875rem', color: '#9ca3af' }}>{error || 'Unknown error occurred'}</div>
            </div>
        );
    }

    const tabs = (type === 'shortcircuit'
        ? [
            { key: 'bus_sc', label: 'Bus SC', data: results?.res_bus_sc || [] },
            { key: 'line_sc', label: 'Line SC', data: results?.res_line_sc || [] },
            { key: 'trafo_sc', label: 'Trafo SC', data: results?.res_trafo_sc || [] },
        ]
        : [
            { key: 'bus', label: 'Bus Results', data: results?.res_bus || [] },
            { key: 'line', label: 'Line Results', data: results?.res_line || [] },
            { key: 'load', label: 'Load', data: results?.res_load || [] },
            { key: 'sgen', label: 'SGen', data: results?.res_sgen || [] },
            { key: 'ext_grid', label: 'Ext Grid', data: results?.res_ext_grid || [] },
        ]).filter(tab => tab.data && tab.data.length > 0);

    if (type === 'load_allocation' && Array.isArray(results?.measurements) && results.measurements.length > 0) {
        tabs.push({ key: 'measurements', label: 'Measurements', data: results.measurements });
    }

    const activeTab = tabs.find(t => t.key === activeResultTab) || tabs[0];
    const activeData = activeTab?.data || [];

    return (
        <>
            <div className="results-tabs">
                {tabs.map(tab => (
                    <div
                        key={tab.key}
                        className={`results-tab ${activeTab?.key === tab.key ? 'active' : ''}`}
                        onClick={() => setActiveResultTab(tab.key)}
                    >
                        {tab.label} ({tab.data?.length || 0})
                    </div>
                ))}
                <div style={{ marginLeft: 'auto', padding: '8px 16px', display: 'flex', alignItems: 'center', gap: 8 }}>
                    {analysisResults.results?.converged !== undefined && (
                        <span className={`converged-badge ${analysisResults.results.converged ? 'success' : 'error'}`}>
                            {analysisResults.results.converged ? <CheckCircle size={12} /> : <XCircle size={12} />}
                            {analysisResults.results.converged ? 'Converged' : 'Not Converged'}
                        </span>
                    )}
                </div>
            </div>
            <div className="results-content">
                {renderDataTable(activeData, activeResultTab)}
            </div>
        </>
    );
}

export default ResultsPanel;
