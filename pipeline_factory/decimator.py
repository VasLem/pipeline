import inspect
from typing import Callable, Optional, TypeVar, Any

from block import Block
from utils.config import Configuration
from utils.hash import HashFactory

SampleConfiguration = TypeVar("SampleConfiguration", bound = Configuration)

class Decimator(Block):
    """A block that is responsible to split incoming data"""

    def __init__(
        self,
        *args,
        computeOutputRatioFunc: Optional[
            Callable[[Block, Any], int]
        ] = None,
        **kwargs
    ):
        self.hideInShortenedGraph = True
        self.computeOutputRatioFunc = computeOutputRatioFunc

        super().__init__(*args, **kwargs)


HashFactory.registerHasher(
    Decimator,
    lambda d: HashFactory.compute(
        d.name
        + inspect.getsource(d.fn)
        + (
            inspect.getsource(d.computeOutputRatioFunc)
            if d.computeOutputRatioFunc is not None
            else ""
        )
    ),
)
