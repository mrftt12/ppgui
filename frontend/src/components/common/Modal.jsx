import React, { useState, useEffect } from 'react';
import { X } from 'lucide-react';
import api from '../../api';

/**
 * Generic Modal dialog component
 */
export function Modal({ title, onClose, children }) {
    return (
        <div className="modal-overlay" onClick={onClose}>
            <div className="modal" onClick={e => e.stopPropagation()}>
                <div className="modal-header">
                    {title}
                    <button className="btn-icon" onClick={onClose}><X size={18} /></button>
                </div>
                {children}
            </div>
        </div>
    );
}

/**
 * Network Version History Modal
 */
export function HistoryModal({ networkId, onClose, onLoadVersion }) {
    const [history, setHistory] = useState([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        api.getNetworkHistory(networkId).then(data => {
            setHistory(data.history || []);
            setLoading(false);
        }).catch(() => setLoading(false));
    }, [networkId]);

    return (
        <Modal title="Version History" onClose={onClose}>
            <div className="modal-body">
                {loading ? <div style={{ padding: 20, textAlign: 'center', color: '#9ca3af' }}>Loading history...</div> : (
                    history.length === 0 ? <div style={{ padding: 20, textAlign: 'center', color: '#9ca3af' }}>No history available.</div> : (
                        <div className="history-list" style={{ maxHeight: 400, overflow: 'auto' }}>
                            {history.map(ver => (
                                <div key={ver.id} style={{ padding: '12px', borderBottom: '1px solid #2a3a5c', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                    <div>
                                        <div style={{ fontWeight: 'bold', fontSize: '0.9rem', color: '#e5e7eb' }}>v{ver.version}</div>
                                        <div style={{ fontSize: '0.8rem', color: '#9ca3af' }}>{new Date(ver.created_at).toLocaleString()}</div>
                                        <div style={{ fontSize: '0.85rem', marginTop: 4, color: '#d1d5db' }}>{ver.description || 'No description'}</div>
                                    </div>
                                    <button className="btn btn-primary" onClick={() => onLoadVersion(ver.id)} style={{ fontSize: '0.8rem', padding: '4px 8px' }}>
                                        Load
                                    </button>
                                </div>
                            ))}
                        </div>
                    )
                )}
            </div>
        </Modal>
    );
}

export default Modal;
