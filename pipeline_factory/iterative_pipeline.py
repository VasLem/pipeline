from __future__ import annotations
import os
import time
import traceback
from collections import OrderedDict
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, TypeVar

from joblib import Parallel, delayed
from tqdm import tqdm

from aggregator import Aggregator
from decimator import Decimator
from exceptions import SkipIterationError, UntilStepReached
from pipeline import Pipeline
from block import Block
from utils.config import Configuration as RunConfiguration
from utils.logging import LOGGER
from utils.hash import HashFactory
from cacher import InstancesCacher


class ChildPipeline(Pipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


HashFactory.registerHasher(
    ChildPipeline,
    lambda d: HashFactory.compute(
        [d.name + d.configID] + [HashFactory.compute(p) for p in d.steps]
    ),
)

Step = TypeVar("Step", Block, "Pipeline", "IterativePipeline")
Leaf = TypeVar("Leaf", bound=Block)
Node = TypeVar("Node", "Pipeline", "IterativePipeline")
Self = TypeVar("Self", bound="IterativePipeline")


class IterativePipeline(InstancesCacher, Pipeline):
    """Extends the notion of pipeline to run multiple instances of the same pipeline with different inputs."""

    _iterativeKeys = None
    doCache = True
    instIDs = None
    instIDsMapping = None
    currentInstCnt = 0

    def __init__(
        self: Self,
        name: str,
        steps: List[Step],
        runConfig: RunConfiguration=None,
        version: str = "",
        description: str = "",
        parent: Optional[Node] = None,
        instID: Optional[str] = None,
        cache: bool = True,
        hideInShortenedGraph: bool = False,
        **params,
    ):
        self.instIDsMapping = {}
        self.doCache = cache
        super().__init__(
            name=name,
            steps=steps,
            runConfig=runConfig,
            version=version,
            description=description,
            parent=parent,
            instID=instID,
            cache=False,
            hideInShortenedGraph=hideInShortenedGraph,
            **params,
        )

    @property
    def cache(self):
        return False

    @cache.setter
    def cache(self, val):
        self.doCache = (
            val  # No caching implemented for IterativePipeline, only children affected
        )
        for step in self.steps:
            step.cache = val
        pass

    @classmethod
    def fromPipeline(cls, obj: Pipeline) -> "IterativePipeline":
        """
        The fromPipeline function is a helper function that allows you to create an IterativePipeline object from a Pipeline object.

        Args:
            obj: Pass the pipeline object to the function

        Returns:
            An IterativePipeline object

        """
        return cls(
            name=obj.name,
            steps=obj.steps,
            runConfig=obj.runConfig,
            version=obj.version,
            description=obj.description,
            instID=obj.instID,
            cache=obj.cache,
            hideInShortenedGraph=obj.hideInShortenedGraph,
            **{k: v for k, v in obj.params.items()},
        )

    def stepsNum(self, samplesInfo: Optional[Union[Any, List[Any]]]=None):
        """The total number of steps, computed recursively.
        Samples information is required by the `computeOutputRatioFunc` of the
        `Decimator` and `Aggregator` steps of the pipeline
        """
        ret = 0
        if not isinstance(samplesInfo, list):
            samplesInfo = [samplesInfo]
        for sampleInfo in samplesInfo:
            sampleInfo = [sampleInfo]
            for s in self.steps:
                if isinstance(s, Pipeline):
                    ret += s.stepsNum(sampleInfo)
                else:
                    ret += 1
                    if (
                        isinstance(s, Decimator)
                        and s.computeOutputRatioFunc is not None
                    ):
                        sampleInfo = sampleInfo * s.computeOutputRatioFunc(
                            s, sampleInfo[0]
                        )
                    if (
                        isinstance(s, Aggregator)
                        and s.computeOutputRatioFunc is not None
                    ):
                        sampleInfo = sampleInfo[
                            : int(
                                1 / s.computeOutputRatioFunc(s, sampleInfo[0])
                            ) : len(sampleInfo)
                        ]

        return ret

    def onSuccessfulCachingLoad(self, inputs=None) -> None:

        if self.previousCollapsedStep is None:  # entry point
            _iterativeKeys = None
            if inputs is not None:  # Better provide inputs for the entry point
                if isinstance(inputs, (tuple, list)):
                    try:
                        _iterativeKeys = list(inputs[0].keys())
                    except (AttributeError, IndexError):
                        if all(isinstance(inp, str) for inp in inputs):
                            _iterativeKeys = list(inputs)
                        else:
                            raise ValueError(f"Inputs {inputs} not understood.")
                elif isinstance(inputs, dict):
                    _iterativeKeys = list(inputs.keys())
                else:
                    raise ValueError(f"Inputs {inputs} not understood.")
                self.initializeIterations(_iterativeKeys)

            obj = self
            while hasattr(obj, "_iterativeKeys") and obj._iterativeKeys is None:
                obj = self.parent
                if obj is None:
                    # Not supposed to ever reach this point, something messy happened if reached.
                    raise ValueError(
                        "Cannot understand how many times the iterative pipeline needs to run"
                    )
            if not hasattr(
                obj, "_iterativeKeys"
            ):  # Is not supposed to ever reach this point
                raise ValueError("Cannot understand inputs")
            inputs = obj._iterativeKeys
        else:
            inputs = self.getChildInstances(self)

        for (
            self.currentInstCnt,
            inp,  # self.currentInstCnt used so that to support SwitchPipeline as well.
        ) in enumerate(inputs):
            for step in self.steps:
                step.onSuccessfulCachingLoad(inputs=[inp])

    def initializeIterations(self, inps: List[str]):
        self.instIDs = [str(x) for x in inps]

    def _run(
        self,
        inp: Union[Dict[str, Tuple], Iterable[str], str] = [],
        untilStep: Optional[str] = None,
        parallel: int = 0,
        maxTries: int = 1,
        sequentialInstances: bool = False,
        _accessPoint: bool = True,
        _fromStep: Optional[str] = None,
        forceDo: bool = False,
        _forceRunSteps: Optional[List] = None,
    ) -> Union[Dict[str, Any], Any]:
        """Runs multiple instances of the same pipeline. Supplied argument is expected to be
        a dictionary with keys instances IDs and values the inputs for each instance.

        Args:
            inp: The dictionary of inputs, or a list of instances IDs, if no inputs are expected.
            untilStep: Until which step to run each instance . Defaults to None.
            parallel: If `parallel` greater than 0, compute tasks using `parallel` jobs. Defaults to 0.
            maxTries: The maximum number of efforts to run a single task. Defaults to 1.
            sequentialInstances: Whether the output of one instance is going to be supplied as input to the next instance. Defaults to False.

            _accessPoint: Whether this is the root pipeline. Not meant to manually provide. Defaults to True.
            _fromStep: The step from which to run the pipeline. Not meant to manually provide. Defaults to None.
            forceDo: Whether to force the pipeline to run all steps. Not meant to manually provide. Defaults to None.
            _forceRunSteps: Whether to force the pipeline to run provided steps. Not meant to manually provide. Defaults to None.
        Returns:
            The returned values dictionary, with keys the instances IDs. If only a single instance is provided,
            the actual result (no dictionary including it) is returned.
        """
        inps = inp
        self.reset()
        os.makedirs(self.resultsDir, exist_ok=True)
        if isinstance(inps, tuple):  # Produced from a previous step
            inps = inps[0]
        if isinstance(inps, str):
            inps = [inps]
        if isinstance(inps, list) and all(isinstance(x, str) for x in inps):
            inps = {x: tuple() for x in inps}
        ret = OrderedDict()
        assert maxTries >= 1
        if _accessPoint:
            LOGGER.debug(
                f"Running with the following run configuration:\n {self.runConfig}"
            )
        if isinstance(inps, list):
            inps = {inp: tuple() for inp in inps}
        self._iterativeKeys = list(inps.keys())

        self.inputsNum = len(inps)
        LOGGER.debug(f"Amount of instances:{len(inps)}")

        self.initializeIterations(list(inps.keys()))
        self.saveInstances()
        self.currentInstCnt = 0
        doParallel = not sequentialInstances and parallel
        self.stepReached = False
        failed = 0
        if doParallel:
            retList = Parallel(
                n_jobs=parallel if parallel else 1,
                prefer="threads",
                require="sharedmem",
            )(
                delayed(self._singleTask)(
                    (
                        os.path.join(str(self.instID), str(key))
                        if self.instID is not None
                        else str(key)
                    ),
                    inp=inps[key],
                    untilStep=untilStep,
                    _fromStep=_fromStep,
                    forceDo=forceDo,
                    maxTries=maxTries,
                )
                for key in tqdm(inps.keys())
            )

            for key, it in zip(inps.keys(), retList):
                self.stepReached = isinstance(it, UntilStepReached)
                if isinstance(it, SkipIterationError):
                    failed += 1
                    continue
                if self.stepReached:
                    ret[key] = it.data
                else:
                    ret[key] = it

        else:
            res = None
            for self.currentInstCnt, key in tqdm(list(enumerate(inps.keys()))):
                if sequentialInstances and res is not None:
                    inp = res
                else:
                    inp = inps[key]
                res = self._singleTask(
                    (
                        os.path.join(str(self.instID), str(key))
                        if self.instID is not None and (self.instID not in inps.keys())
                        else str(key)
                    ),
                    inp,
                    untilStep=untilStep,
                    _fromStep=_fromStep,
                    forceDo=forceDo,
                    maxTries=maxTries,
                )
                if isinstance(res, SkipIterationError):
                    failed += 1
                    continue
                if isinstance(res, UntilStepReached):
                    ret[key] = res.data
                    self.stepReached = True
                    continue
                ret[key] = res
        if failed == len(inps):
            raise SkipIterationError
        ret = (ret,)
        if self.stepReached:
            if self.isRoot:
                return ret
            raise UntilStepReached(ret)
        self.finalize()
        return ret

    @property
    def instances(self) -> Dict[str, ChildPipeline]:
        oldCnt = self.currentInstCnt
        ret = OrderedDict()
        for self.currentInstCnt, instID in enumerate(self.instIDs):
            ret[instID] = ChildPipeline(
                name=self.name,
                steps=[step.copy() for step in self.steps],
                runConfig=self.runConfig.copy(),
                version=self.version,
                description=self.description,
                cache=self.doCache,
                instID=instID,
                currentInstCnt=self.currentInstCnt,
                parent=self.parent,
            )

        self.currentInstCnt = oldCnt
        return ret

    def _singleTask(
        self,
        instID,
        inps,
        untilStep: Optional[str] = None,
        maxTries: int = 1,
        _fromStep: Optional[str] = None,
        forceDo: bool = False,
    ):
        LOGGER.info(f"Processing: {instID}")
        strs = [str(x) for x in inps]
        inputsStr = "\n".join(
            [
                f"Inp. {i + 1}: "
                + (
                    s[:50].strip().replace("\n", " ")
                    + "....."
                    + s[-50:].strip().replace("\n", " ")
                    if len(s) > 100
                    else s[:100]
                )
                for i, s in enumerate(strs)
            ]
        )
        LOGGER.debug(f"Inputs are:\n{inputsStr}")
        retries = 0
        while True:
            try:
                pipeline = ChildPipeline(
                    name=self.name,
                    steps=self.steps,
                    runConfig=self.runConfig,
                    version=self.version,
                    cache=self.doCache,
                    instID=instID,
                    currentInstCnt=self.currentInstCnt,
                    inputsNum=self.inputsNum,
                    parent=self.parent,
                )
                ret = (
                    pipeline._run(
                        inps,
                        untilStep=untilStep,
                        _fromStep=_fromStep,
                        _accessPoint=False,
                        forceDo=forceDo,
                    ),
                    1,
                )
            except UntilStepReached as err:
                return err
            except SkipIterationError as err:
                LOGGER.warning(
                    f"Iteration {instID} ignored because of the following issue:",
                    exc_info=True,
                )
                return err
            except BaseException:
                if retries == maxTries - 1:
                    raise
                LOGGER.warning(
                    "Error:\n", traceback.format_exc(), f"\nRetrying (Try: {retries})"
                )
                retries += 1
                time.sleep(1)
                continue
            finally:
                self.updateParams(self.params)
            break
        return ret

    def clearResults(
        self,
        instances: Optional[str] = None,
        ifEmpty=False,
        selfOnly=False,
        *args,
        **kwargs,
    ) -> None:
        """
        The clearResults function is used to clear the results directory.

        Args:
            ifEmpty: Clear the results directory if it is empty
            oldest100: Clear the oldest 100 files in the results directory
        """

        oldInstIDs = self.instIDs
        if instances is not None:
            self._instIDs = instances
        try:
            super().clearResults(ifEmpty=ifEmpty, selfOnly=selfOnly, *args, **kwargs)
        finally:
            self._instIDs = oldInstIDs

    def updateCache(self, output) -> None:
        return None


HashFactory.registerHasher(
    IterativePipeline,
    lambda d: HashFactory.compute(
        [d.name + d.configID] + [HashFactory.compute(p) for p in d.steps]
    ),
)
