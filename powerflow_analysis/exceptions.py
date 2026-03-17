class MissingLoadDataException(Exception):
    """Exception raised when load data is missing for a power flow analysis."""

    def __init__(self, circuit_key=None, start_period=None, end_period=None, **kwargs):
        self.message=f"Load data is missing for the power flow analysis for {circuit_key} from {start_period} to {end_period}"
        super().__init__(self.message, **kwargs)
