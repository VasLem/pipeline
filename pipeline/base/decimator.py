from typing import Callable, Optional

from pipeline.base.block import Block
from utils.config import Configuration as SampleConfiguration
from utils.hash import HashFactory
import inspect


class Decimator(Block):
    """A block that is responsible to split incoming data"""

    def __init__(
        self,
        *args,
        computeOutputRatioFunc: Optional[
            Callable[[Block, SampleConfiguration], int]
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
