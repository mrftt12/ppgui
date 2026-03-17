import React, { useEffect } from 'react';
import { CheckCircle, XCircle } from 'lucide-react';

/**
 * Toast notification component
 * Auto-dismisses after 3 seconds
 */
function Toast({ message, type, onClose }) {
    useEffect(() => {
        const timer = setTimeout(onClose, 3000);
        return () => clearTimeout(timer);
    }, [onClose]);

    return (
        <div className={`toast ${type}`}>
            {type === 'success' ? <CheckCircle size={16} /> : <XCircle size={16} />}
            {message}
        </div>
    );
}

export default Toast;
