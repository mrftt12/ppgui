import { useState, useCallback } from 'react';

const MAX_HISTORY = 50;

/**
 * useUndoRedo - Manages undo/redo history for diagram actions
 * 
 * Each action should have:
 * - type: string describing the action
 * - undo: function to reverse the action
 * - redo: function to reapply the action
 * - description: human-readable description (optional)
 */
export function useUndoRedo() {
    const [undoStack, setUndoStack] = useState([]);
    const [redoStack, setRedoStack] = useState([]);

    const pushAction = useCallback((action) => {
        if (!action || typeof action.undo !== 'function' || typeof action.redo !== 'function') {
            console.warn('Invalid action passed to pushAction:', action);
            return;
        }

        setUndoStack(prev => {
            const newStack = [...prev, action];
            // Limit stack size
            if (newStack.length > MAX_HISTORY) {
                newStack.shift();
            }
            return newStack;
        });

        // Clear redo stack when new action is performed
        setRedoStack([]);
    }, []);

    const undo = useCallback(() => {
        setUndoStack(prev => {
            if (prev.length === 0) return prev;

            const newStack = [...prev];
            const action = newStack.pop();

            // Execute undo
            try {
                action.undo();
            } catch (e) {
                console.error('Undo failed:', e);
                return prev; // Don't modify stack if undo fails
            }

            // Move to redo stack
            setRedoStack(redoPrev => [...redoPrev, action]);

            return newStack;
        });
    }, []);

    const redo = useCallback(() => {
        setRedoStack(prev => {
            if (prev.length === 0) return prev;

            const newStack = [...prev];
            const action = newStack.pop();

            // Execute redo
            try {
                action.redo();
            } catch (e) {
                console.error('Redo failed:', e);
                return prev; // Don't modify stack if redo fails
            }

            // Move back to undo stack
            setUndoStack(undoPrev => [...undoPrev, action]);

            return newStack;
        });
    }, []);

    const clear = useCallback(() => {
        setUndoStack([]);
        setRedoStack([]);
    }, []);

    return {
        pushAction,
        undo,
        redo,
        clear,
        canUndo: undoStack.length > 0,
        canRedo: redoStack.length > 0,
        undoCount: undoStack.length,
        redoCount: redoStack.length,
        lastAction: undoStack[undoStack.length - 1] || null,
    };
}

/**
 * Helper to create a node move action
 */
export function createNodeMoveAction(nodeId, fromPos, toPos, setNodePositions) {
    return {
        type: 'MOVE_NODE',
        description: `Move node ${nodeId}`,
        undo: () => {
            setNodePositions(prev => ({ ...prev, [nodeId]: fromPos }));
        },
        redo: () => {
            setNodePositions(prev => ({ ...prev, [nodeId]: toPos }));
        },
    };
}

/**
 * Helper to create an element create action
 */
export function createElementAction(elementType, elementData, apiCreate, apiDelete, refreshData) {
    let createdIndex = null;

    return {
        type: 'CREATE_ELEMENT',
        description: `Create ${elementType}`,
        undo: async () => {
            if (createdIndex !== null) {
                await apiDelete(elementType, createdIndex);
                await refreshData();
            }
        },
        redo: async () => {
            const result = await apiCreate(elementType, elementData);
            createdIndex = result.index;
            await refreshData();
        },
    };
}

/**
 * Helper to create a multi-node move action
 */
export function createMultiNodeMoveAction(moves, setNodePositions) {
    // moves: Array of { nodeId, fromPos, toPos }
    return {
        type: 'MOVE_NODES',
        description: `Move ${moves.length} nodes`,
        undo: () => {
            setNodePositions(prev => {
                const updated = { ...prev };
                moves.forEach(({ nodeId, fromPos }) => {
                    updated[nodeId] = fromPos;
                });
                return updated;
            });
        },
        redo: () => {
            setNodePositions(prev => {
                const updated = { ...prev };
                moves.forEach(({ nodeId, toPos }) => {
                    updated[nodeId] = toPos;
                });
                return updated;
            });
        },
    };
}

export default useUndoRedo;
