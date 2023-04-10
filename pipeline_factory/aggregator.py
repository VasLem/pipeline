import inspect
from typing import Callable, Optional, TypeVar

from block import Block
from utils.config import Configuration
from utils.hash import HashFactory

SampleConfiguration = TypeVar("SampleConfiguration", bound = Configuration)

class Aggregator(Block):
    """A block that is responsible to unite incoming data"""

    def __init__(
        self,
        *args,
        computeOutputRatioFunc: Optional[
            Callable[[Block, SampleConfiguration], float]
        ] = None,
        **kwargs
    ):
        self.hideInShortenedGraph = True
        self.computeOutputRatioFunc = computeOutputRatioFunc
        super().__init__(*args, **kwargs)


HashFactory.registerHasher(
    Aggregator,
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
