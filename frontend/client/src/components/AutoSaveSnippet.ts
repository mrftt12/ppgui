
// Auto-save effect
useEffect(() => {
    if (hasUnsavedChanges && currentModelId) {
        const timer = setTimeout(() => {
            saveModelMutation.mutate();
        }, 2000); // Auto-save after 2 seconds of inactivity

        return () => clearTimeout(timer);
    }
}, [hasUnsavedChanges, currentModelId, elements, connections]); // Depend on data to reset timer on changes
