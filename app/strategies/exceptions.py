class StrategyError(Exception):
    """Base class for exceptions in the IndividualStrategy module."""
    def __init__(self, message="An error occurred in the IndividualStrategy module"):
        self.message = message
        super().__init__(self.message)
