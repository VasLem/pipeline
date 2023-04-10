from utils.logging import LOGGER


class PipelineBreak(BaseException):
    """Intentionally break pipeline. This is supposed to be used to non recursively (single level) stop the execution of a pipeline."""

    pass


class PipelineHalted(BaseException):
    """Exception for unhandled error within the pipeline."""

    pass


class InvalidCache(PipelineHalted):
    """Error when cache is not found"""

    pass


class SkipIterationError(PipelineHalted):
    """Exception raised when the current iteration needs to be skipped because of problems"""

    pass


class BlockError(PipelineHalted):
    def __init__(self, name: str, tb: str):
        LOGGER.error(f"Block {name} raised the following error: \n {tb}")
        super().__init__(f"Block {name} raised the following error: \n {tb}")

    pass


class UntilStepReached(BaseException):
    """Exception to be handled as a normal signal that the
    provided pipeline step has been reached.
    """

    def __init__(self, data):
        self.data = data
        super().__init__()

    pass


REGISTERED_EXCEPTIONS = [
    SkipIterationError,
    UntilStepReached,
    PipelineBreak,
]
