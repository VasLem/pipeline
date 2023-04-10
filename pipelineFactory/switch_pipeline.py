from typing import Iterable, List, Optional, Tuple, Union


from utils.logging import LOGGER
from pipelineFactory.block import Block
from utils.config import Configuration as RunConfiguration
from pipelineFactory.iterative_pipeline import IterativePipeline
from pipelineFactory.pipeline import Pipeline
import networkx as nx
from networkx import DiGraph
from utils.hash import HashFactory


class SwitchPipeline(IterativePipeline):
    def __init__(
        self,
        name: str,
        stepsLists: List[List[Union[Pipeline, Block]]],
        runConfig: RunConfiguration,
        switchSteps: Union[Iterable[int], int] = [],
        switchStepsEvery: int = 0,
        version: str = "",
        description: str = "",
        instID: Optional[str] = None,
        cache: bool = True,
        hideInShortenedGraph: bool = False,
        **params,
    ):
        """
        This flavor of pipeline enables the exchange of steps after a specific instance. The behavior is controlled either by
        `switchSteps` or `switchStepsEvery` parameters. `switchStepsEvery` allows for loop exchange after n steps, so it useful to perform updates,
        whereas `switchSteps` specifies the exact number of steps required to move from one steps list to another, practical for initialization reasons.
        Only one of those parameters must be supplied (mutually exclusive).
        Args:
            name:str: Specify the name of the pipeline
            stepsLists:List[List[Union[Pipeline,Block]]]: Store the different steps lists that are used to create a pipeline
            runConfig:RunConfiguration: Pass in a RunConfiguration object
            switchSteps:List[int]=[]: Define the steps at which the pipeline will switch from one list of blocks to another
            switchStepsEvery:int=0: Define how often we switch between the different stepslists
            version:str="": the pipeline version
            instID:str=None: Identify the instance of the pipeline
            cache:bool=True: Determine whether the pipeline should cache the output of each step
            hideInShortenedGraph:bool=False: Control whether the pipeline will be shown in the shortened graph view
            **params: Pass in any additional parameters that are required for the steps
        """
        if isinstance(switchSteps, int):
            switchSteps = [switchSteps]
        self._initialized = False
        self._currSwitch = 0
        self.stepsLists = list(stepsLists)
        self.switchSteps = list(switchSteps)
        self.switchStepsEvery = switchStepsEvery
        assert bool(self.switchSteps) ^ bool(
            self.switchStepsEvery
        ), "Either switchSteps or switchStepsEvery needs to be supplied, and not both"
        if self.switchSteps:
            if len(self.switchSteps) == 0:
                self.switchSteps.append(0)
            elif self.switchSteps[0] != 0:
                self.switchSteps.insert(0, 0)

            assert len(self.stepsLists) == len(
                self.switchSteps
            ), "The length of the steps lists provided and the number of switches does not agree"
            assert all(
                isinstance(x, int) for x in switchSteps
            ), "The provided switches need to be a list of integers"
            assert all(
                x == y for x, y in zip(switchSteps, sorted(switchSteps))
            ), "The provided switches must be an incrementing list of integers"

        super().__init__(
            name=name,
            steps=stepsLists[0],
            runConfig=runConfig,
            version=version,
            description=description,
            instID=instID,
            cache=cache,
            hideInShortenedGraph=hideInShortenedGraph,
            **params,
        )
        self._currSwitch = 0
        self._initialized = True

    def updateParams(self, params):
        """Update the parameters defined in the provided dictionary, recursively on all children

        Args:
            params (dict): the parameters to update and their new values
        """
        for self.currentInstCnt in range(len(self.stepsLists)):
            super().updateParams(params)

    def stepsNum(self, sampleConfigs):
        if self.switchSteps:
            switchSteps = self.switchSteps
        else:
            switchSteps = range(0, len(sampleConfigs), self.switchStepsEvery)
        # @TODO: support Decimator and Aggregator
        fullSteps = [len(sampleConfigs) - step > 0 for step in switchSteps]

        if all(fullSteps):
            ind = -1
        else:
            ind = fullSteps.index(False)
        return sum(
            sum(
                step.stepsNum(sampleConfigs[p : n + 1])
                if isinstance(step, Pipeline)
                else 1
                for step in steps
            )
            for p, n, steps in zip(
                switchSteps[:-1], switchSteps[1:], self.stepsLists[:ind]
            )
        ) + sum(
            step.stepsNum(
                sampleConfigs[
                    switchSteps[ind] : (
                        switchSteps[ind + 1]
                        if (ind != -1) and (ind < len(switchSteps) - 1)
                        else len(sampleConfigs)
                    )
                ]
            )
            if isinstance(step, Pipeline)
            else 1
            for step in self.stepsLists[ind]
        )

    @property
    def steps(self):
        if self.switchSteps:
            ind = [
                c for c, t in enumerate(self.switchSteps) if t >= self.currentInstCnt
            ]
            if ind:
                ind = ind[0]
            else:
                ind = len(self.stepsLists) - 1
        else:
            ind = (self.currentInstCnt // self.switchStepsEvery) % len(self.stepsLists)
        # if self._initialized and (ind != self._currSwitch):
        #     LOGGER.debug(
        #         f"Iteration {self.currentInstCnt}.Switching to next set of steps."
        #     )

        self._steps = self.stepsLists[ind]
        self._currSwitch = ind
        return self._steps

    @steps.setter
    def steps(self, val):
        self._steps = val

    def makeGraph(
        self,
        _currGraph: Optional[DiGraph] = None,
        nodeCnt: Optional[int] = 1,
        shortened: bool = False,
    ) -> Tuple[Optional[DiGraph], Optional[int]]:
        """Make the graph of the structure. No arguments are meant to be provided."""
        if self.hideInShortenedGraph and shortened:
            return _currGraph, nodeCnt

        _currGraph, nodeCnt = Block.makeGraph(self, _currGraph, nodeCnt=nodeCnt)
        nx.set_node_attributes(  # type:ignore
            _currGraph, {self.compositeName: "salmon"}, name="color"
        )
        nx.set_node_attributes(  # type:ignore
            _currGraph, {self.compositeName: self.compositeName}, name="rank"
        )
        if self.switchSteps:
            switchSteps = self.switchSteps
        else:
            switchSteps = [
                self.switchStepsEvery * cnt for cnt in range(len(self.stepsLists))
            ]
        edgeAttributes = {}
        for self.currentInstCnt in switchSteps:
            l = 0
            while (
                shortened
                and (l < len(self.steps))
                and self.steps[l].hideInShortenedGraph
            ):
                l += 1
            if l == len(self.steps):
                continue
            nex = self.steps[l]
            if isinstance(nex, Pipeline):
                _, nodeCnt = nex.makeGraph(
                    _currGraph, nodeCnt=nodeCnt, shortened=shortened
                )
            else:
                _, nodeCnt = nex.makeGraph(_currGraph, nodeCnt=nodeCnt)
            _currGraph.add_edge(self.compositeName, nex.compositeName)
            if not l:
                edgeAttributes[(self.compositeName, nex.compositeName)] = "iter=" + str(
                    self.currentInstCnt
                )
            nx.set_node_attributes(  # type:ignore
                _currGraph, {nex.compositeName: self.compositeName}, name="label"
            )

        nx.set_edge_attributes(  # type:ignore
            _currGraph, values=edgeAttributes, name="label"
        )
        return _currGraph, nodeCnt


HashFactory.registerHasher(
    SwitchPipeline,
    lambda d: HashFactory.compute(
        [d.name + d.runConfigId] + [HashFactory.compute(p) for p in d.stepsLists]
    ),
)
