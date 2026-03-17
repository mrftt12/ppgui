try:
    import coverage.types
except ImportError:
    pass
else:
    from typing import Any

    if not hasattr(coverage.types, "Tracer"):
        # Patch for numba <-> coverage compatibility
        class MockTracer:
            pass

        coverage.types.Tracer = MockTracer

    if not hasattr(coverage.types, "TShouldTraceFn"):
        coverage.types.TShouldTraceFn = Any

    if not hasattr(coverage.types, "TShouldStartContextFn"):
        coverage.types.TShouldStartContextFn = Any

    if not hasattr(coverage.types, "TFileDispositionFn"):
        coverage.types.TFileDispositionFn = Any
