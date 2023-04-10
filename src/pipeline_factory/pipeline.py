from __future__ import annotations
import gc
import os
from typing import List, Optional, Tuple, Union
from typing_extensions import TypeVarTuple, Unpack

from typing import OrderedDict as OrderedDictType
from aggregator import Aggregator
from block import Block
from decimator import Decimator
from exceptions import (
    PipelineBreak,
    PipelineHalted,
    UntilStepReached,
)
from hierarchical_model import HierarchyNode
from . import PIPELINE_CONFIG
from utils.config import Configuration as RunConfiguration
from utils.config import Configuration as SampleConfiguration
import ast

from utils.logging import LOGGER
from utils.path import oldest_files_in_tree
from utils.hash import HashFactory
from traceback import print_exc
import numpy as np
from typing import TypeVar


USE_CACHING = ast.literal_eval(PIPELINE_CONFIG['use_caching'])
InputArgs = TypeVarTuple("InputArgs")
OutputArgs = TypeVar("OutputArgs", bound=tuple)

Step = TypeVar("Step", Block, "Pipeline")
Leaf = TypeVar("Leaf", bound=Block)
Node = TypeVar("Node", bound="Pipeline")
Self = TypeVar("Self", bound="Pipeline")


class Pipeline(Block, HierarchyNode[Self, Node, Leaf, Step]):
    """Allows a list of steps to run sequentially, with intermediate results autosaving and autoloading.
    Allows different instances to be saved, based on the supplied name.
    If the input changes, the whole pipeline is run.
    If the i-th step changes, the pipeline will run for step i and onwards.
    It is possible to supply the name of the step until which the analysis is supposed to run.

    If the pipeline is the root one, results are saved under (the brackets stand for optional):
        pipeline.resultsDir:=`CONFIG['resultsDir']`/`runconfigDir`/`name`[/`version`]/`step.resultsDir`
        and the cache is saved under:
        pipeline.cacheDir:=`CONFIG['cacheDir']`/`runconfigDir`/`name`[/`version`]/`step.resultsDir`
    """

    def __init__(
        self: Self,
        name: str,
        steps: List[Step],
        runConfig: RunConfiguration,
        version: str = "",
        description: str = "",
        parent: Optional[Node] = None,
        instID: Optional[str] = None,
        cache: bool = True,
        hideInShortenedGraph: bool = False,
        **params,
    ):
        """
        Args:
            name (str): configures the results and caching directories identities, so it needs to be unique.
            steps (List[Union[Pipeline;, Block]]): contains a mixture of blocks and possibly pipelines,
                which is accessed sequentially and each step expects as input the output of the previous step
                (this means that the first step usually is loading the data)
            runConfig (RunConfiguration): Configures the results and caching directories provides variables used by the `steps`
            description(str): The description of the pipeline, if provided, adds information to the generated graph.
            parent (Pipeline): The parent of the pipeline, defaults to None.
            version (str, optional): The version of the pipeline, if provided, adds an extra level in the pipeline directory structure. Defaults to ''.
            instID (str, optional): The block instance ID, to differentiate it from same name blocks. Adds an additional directory if given. Defaults to None.
            cache(bool, optional): Whether to cache runs. Defaults to true.
            hideInShortenedGraph (bool, optional): Whether not to show this pipeline in the shortened graph version. Defaults to False.
            **params: Additional parameters can be provided, that are going to propagate to all the steps, recursively.
                For example the sampleName or sampleConfig can be provided.
        """
        if not all(hasattr(x, "_isblock") for x in steps):
            raise ValueError(f"Invalid steps provided: {steps}")
        HierarchyNode.__init__(
            self,
            name=name,
            children=steps,
            parent=parent,
            description=description,
            hideInShortenedGraph=hideInShortenedGraph,
        )
        for key in ("params", "names"):
            if key in params:
                raise ValueError(
                    f"Invalid keyword `{key}` as additional keyword argument"
                )
        params.pop("fn", None)
        Block.__init__(
            self,
            name=name,
            fn=lambda x: x,
            version=version,
            description=description,
            parent=parent,
            runConfig=runConfig,
            instID=instID,
            cache=cache,
            hideInShortenedGraph=hideInShortenedGraph,
            **params,
        )

        self.params = params
        self.runConfig = runConfig  # required for typing

        self.updateParams(params)

    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, val):
        self._cache = val
        for step in self.steps:
            step.cache = val

    def stepsNum(
        self,
        sampleConfig: Optional[
            Union[List[SampleConfiguration], SampleConfiguration]
        ] = None,
    ):
        """The total number of steps, computed recursively"""
        ret = 0
        if sampleConfig is None:
            sampleConfig = SampleConfiguration()

        if isinstance(sampleConfig, list):
            sampleConfig = sampleConfig[0]

        sampleConfig = [sampleConfig]
        for s in self.steps:
            if isinstance(s, Pipeline):
                ret += s.stepsNum(sampleConfig)
            else:
                if isinstance(s, (Decimator)):
                    if s.computeOutputRatioFunc is None:
                        LOGGER.warning(f"computeOutputRatioFunc undefined for step {s}")
                    else:
                        sampleConfig = sampleConfig * s.computeOutputRatioFunc(
                            s, sampleConfig[0]
                        )
                elif isinstance(s, Aggregator):
                    if s.computeOutputRatioFunc is None:
                        LOGGER.warning(f"computeOutputRatioFunc undefined for step {s}")
                    else:

                        sampleConfig = sampleConfig[
                            : int(
                                1 / s.computeOutputRatioFunc(s, sampleConfig[0])
                            ) : len(sampleConfig)
                        ]
                ret += 1
        return ret

    @property
    def resultsDir(self) -> str:
        """The directory where any results are to be saved

        Returns:
            str: the results directory
        """
        return super().resultsDir

    @resultsDir.setter
    def resultsDir(self, value: str):
        self._resultsDir = value
        if value is None:
            return

    @property
    def instID(self) -> Optional[str]:
        """Used to separate results and cache between copies of the same object
        Returns:
            str: The instance ID

        """
        return super().instID

    @instID.setter
    def instID(self, instID: Optional[str]) -> None:
        """To update instance ID, all the dependent values need to be reinitialized as well

        Args:
            instID: the instance ID
        """
        self.updateParams(
            dict(
                _instID=(str(instID) if instID is not None else None),
                resultsDir=None,
                cacheDir=None,
            )
        )

    def updateInstID(self, instID: Optional[str], selfOnly: bool = False) -> None:
        self.updateParams(
            dict(
                _instID=(str(instID) if instID is not None else None),
                resultsDir=None,
                cacheDir=None,
            ),
            selfOnly=selfOnly,
        )

    def updateParams(self, params: dict, selfOnly=False) -> None:
        """Update the parameters defined in the provided dictionary, recursively on all children

        Args:
            params (dict): the parameters to update and their new values
            selfOnly: do not update children if selfOnly is True
        """
        super().updateParams(params)
        if selfOnly:
            return
        params = {k: v for k, v in params.items() if k != "parent"}
        for step in self.steps:
            step.parent = self
            step.updateParams(params)
            step.instID = self.instID
            if step.runConfig is None:
                step.runConfig = self.runConfig

    def onSuccessfulCachingLoad(self, inputs) -> None:
        """
        The onSuccessfulCachingLoad function is used to emit the progress of a step when it is skipped.
        """
        for step in self.steps:
            step.onSuccessfulCachingLoad(inputs)

    def useCache(
        self,
        inp: Tuple,
        untilStep: Optional[str] = None,
        _forceRunSteps: List[str] = [],
        *args,
        **kwargs,
    ) -> bool:
        """
        The useCache function is called by the Pipeline class to determine whether or not a given input is already cached.
        Args:
            inp: Tuple: Pass the input data to the function.
            untilStep: str: The step until which the pipeline is to run. Defaults to None (all steps are run).
            _forceRunSteps: List[str]: Force the pipeline to run a list of steps.

        Returns:
            True whether the existing cache is valid for the given inputs.
        """

        if untilStep and any(x.endswith(untilStep) for x in self.collapsedNamedSteps):
            return False
        if _forceRunSteps and any(
            x.endswith(y) for x in self.collapsedNamedSteps for y in _forceRunSteps
        ):
            return False
        return self.checkInput(inp)

    def runSteps(
        self, names: Union[str, List[str]], inp
    ) -> List[Union[Tuple, OrderedDictType[str, Tuple]]]:
        """
        The runSteps function is a helper function that allows you to run certain steps of the pipeline, and retrieve their results.

        Args:
            names: Specify the names of the steps to run.
            inp: Pass the input data to the pipeline

        Returns:
            A list of cached outputs for the steps that were run. The cached output is going to be a dictionary if
            the step belongs to an iterative pipeline.
        """

        assert USE_CACHING
        collapsedNamedSteps = self.collapsedNamedSteps
        collapsedNamedStepsNames = list(collapsedNamedSteps.keys())
        names = [names] if isinstance(names, str) else names
        inds = [
            [i for i, c in enumerate(collapsedNamedSteps) if c.endswith(x)]
            for x in names
        ]
        if not all(inds):
            invalidSteps = [x for (x, ind) in zip(names, inds) if not ind]
            raise ValueError(
                f"Provided steps {invalidSteps} do not exist in the pipeline"
            )
        inds = [ind[0] for ind in inds]
        for ind in inds:
            collapsedNamedSteps[collapsedNamedStepsNames[ind]].cache = True
        LOGGER.info(f"Running until step {collapsedNamedStepsNames[np.max(inds)]}")
        self._run(inp, untilStep=names[np.argmax(inds)], _forceRunSteps=names)
        selectedSteps = [
            collapsedNamedSteps[collapsedNamedStepsNames[ind]] for ind in inds
        ]
        oldInstIDs = [step._instID for step in selectedSteps]
        try:
            return [
                [
                    step.loadCachedOutput()
                    for step._instID in self.getChildInstances(step)
                ]
                for step in selectedSteps
            ]
        finally:
            for o, step in zip(oldInstIDs, selectedSteps):
                step._instID = o

    def _run(
        self,
        inp: tuple[Unpack[InputArgs]] = tuple(),
        untilStep: Optional[str] = None,
        _fromStep: Optional[str] = None,
        _accessPoint: bool = True,
        forceDo: bool = False,
        _forceRunSteps: List[str] = [],
    ) -> OutputArgs:
        """Runs the pipeline, until a specific step, if provided.

        Args:
            inp: The positional arguments to the first step of the pipeline. Defaults to an empty tuple.
            untilStep: The step until which to run the pipeline. Defaults to None.
            _fromStep: The step from which to run the pipeline. Not meant to be manually provided. Defaults to None.
            _accessPoint: Whether this is the root pipeline. Not meant to be manually provided. Defaults to True.
            forceDo: Whether to force all the steps to be done. Not meant to be manually provided. Defaults to False.
        Returns:
            Any: the result of the final step run.
        """
        self.reset()
        os.makedirs(self.resultsDir, exist_ok=True)
        out = inp
        if _accessPoint:
            LOGGER.debug(
                f"Running with the following run configuration: {self.runConfig}"
            )
        do = forceDo
        if untilStep is not None and self.isRoot:
            collapsedNamedSteps = self.collapsedNamedSteps
            if (
                _fromStep is None
            ):  # Recursion depth 0, check whether provided untilStep exists
                if not any(x.endswith(untilStep) for x in collapsedNamedSteps):
                    raise PipelineHalted(
                        f"Supplied step name {untilStep} does not belong in the pipeline {self.compositeName}. Available steps:\n"
                        + "\n".join(collapsedNamedSteps.keys())
                    )

        if not do and self.useCache(inp, untilStep, _forceRunSteps):
            LOGGER.debug(
                f"Step  {str(self.__class__.__qualname__).split('.')[-1]} {self.compositeName} is cached, skipping.."
            )
            LOGGER.debug(f"Step has instance ID: {self.instID}")
            self.onSuccessfulCachingLoad(inp)
            return self.loadCachedOutput()
        LOGGER.info(
            f"Running {str(self.__class__.__qualname__).split('.')[-1]} {self.compositeName}.."
        )
        for cnt, step in enumerate(self.steps):
            step._instID = self._instID
            self.currStepIndex = cnt
            if not do and _fromStep is not None:
                if (
                    isinstance(step, Pipeline)
                    and (_fromStep != "init")
                    and not step.isParentOf(_fromStep)
                ):  # Check relatedness, if not related, skip
                    continue
                # If related, get in the child
                do = True
                # or if the step matches exactly the given name, load from cache and move to the next step
                if step.compositeName == _fromStep:
                    if step is not None and os.path.exists(step.cachedOutputPath):
                        LOGGER.debug(f"Loading output from step: {_fromStep}")
                        out = step.loadCachedOutput()

                    continue
            step.reset()
            if (
                isinstance(out, tuple)
                and (len(out) > 0)
                and isinstance(out[0], Pipeline)
            ):
                # Handling nested pipelines
                out = out[1:]
            noException = False
            try:
                if isinstance(step, Pipeline):
                    out = step.run(
                        inp=out,
                        _fromStep=_fromStep,
                        _accessPoint=False,
                        untilStep=untilStep,
                        forceDo=(
                            forceDo or _fromStep is not None
                        ),  # It has already been decided that all the steps between _fromStep and untilStep need to be done
                        _forceRunSteps=_forceRunSteps,
                    )
                else:
                    out = step.run(
                        inp=out,
                        forceDo=forceDo
                        or bool(_forceRunSteps)
                        and any(step.compositeName.endswith(x) for x in _forceRunSteps),
                    )

                    gc.collect()
                    try:
                        import torch # type: ignore
                        if torch.cuda.is_available():
                            torch.cuda.empty_cache()
                    except BaseException:
                        pass
            except UntilStepReached as err:
                if self.isRoot:
                    return err.data
                raise err
            except PipelineBreak:
                return out
            except PipelineHalted:
                raise
            except BaseException:

                print_exc()
                raise PipelineHalted("Internal error occurred. Check logs.")
            else:
                noException = True
            finally:
                if (
                    not noException
                ):  # In case there is an exception, finalize before exiting
                    self.finalize()
            if untilStep and step.compositeName.endswith(untilStep):
                if step.compositeName == self.lastStep.compositeName:
                    self.updateCache(out)
                if self.isRoot:
                    return out
                raise UntilStepReached(out)
        if USE_CACHING and self.cache:
            self.updateCache(out)
        self.finalize()
        return out

    def clearCache(
        self,
        instance: Optional[str] = None,
        ifEmpty=False,
        oldest100=False,
        forcedAll: bool = False,
        selfOnly=False,
        *args,
        **kwargs,
    ) -> None:
        """
        The clearCache function is used to clear the cache.

        Args:
            instance: Clear the specified instance.
            ifEmpty: Clear the cache if it is empty.
            oldest100: Clear the oldest 100 files in the cache directory.
            forcedAll: Whether to clear all cache, independently on the current instance ID.
            selfOnly: Whether to clear only entries related to self alone, and not its children. Defaults to False.
        """

        if oldest100:
            [os.remove(x) for x in oldest_files_in_tree(self.cacheDir, count=100)]
            return
        oldInstID = self.instID

        if instance is not None:
            assert isinstance(instance, str)
            self._instID = instance
        try:
            if not selfOnly:
                for step in self.steps:
                    step.clearCache(
                        ifEmpty=ifEmpty,
                        oldest100=oldest100,
                        forcedAll=forcedAll,
                        selfOnly=False,
                        *args,
                        **kwargs,
                    )
            kwargs = kwargs.copy()
            kwargs.update(dict(selfOnly=True))

        finally:
            self._instID = oldInstID
        super().clearCache(
            instance=instance,
            ifEmpty=ifEmpty,
            oldest100=oldest100,
            forcedAll=forcedAll,
            *args,
            **kwargs,
        )

    def clearResults(
        self,
        instance: Optional[str] = None,
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
        oldInstID = self.instID
        if instance is not None:
            assert isinstance(instance, str)
            self._instID = instance
        try:
            if not selfOnly:
                for step in self.steps:
                    instances = self.getChildInstances(step)
                    oldStepInstID = step.instID
                    for step._instID in instances:
                        step.clearResults(
                            ifEmpty=ifEmpty, selfOnly=False, *args, **kwargs
                        )
                    step._instID = oldStepInstID
            super().clearResults(ifEmpty=ifEmpty, *args, **kwargs)
        finally:
            self._instID = oldInstID

    def clearReport(self, instance: Optional[str] = None, *args, **kwargs) -> None:
        """Clear the reports database"""
        oldInstID = self._instID
        if instance is not None:
            assert isinstance(instance, str)
            self._instID = instance
        try:
            for step in self.steps:
                instances = self.getChildInstances(step)
                oldStepInstID = step.instID
                for step._instID in instances:
                    step.clearReport(*args, **kwargs)
                step._instID = oldStepInstID
            super().clearReport(*args, **kwargs)
        finally:
            self._instID = oldInstID

    def __call__(
        self,
        inp=tuple(),
        untilStep: Optional[str] = None,
    ):
        """Runs the pipeline, until a specific step, if provided.

        Args:
            untilStep (str, optional): The step until which to run the pipeline. Defaults to None.
        Returns:
            Any: the result of the final step run.
        """
        return self.run(
            inp=inp,
            untilStep=untilStep,
            _fromStep=None,
            _accessPoint=False,
            forceDo=False,
        )

    def __getstate__(self):
        """Special function to be used by joblib, converts the object to serializable."""
        return self.__dict__

    def insertBefore(self: Self, before: str, step: Step) -> Self:
        """
        In place insertion of the children list, before the denoted name. Does not support the supply of a composite name.

        Args:
            before: the name of the child, before which the supplied child is added, the composite name can also be provided.
            step: the step to add to the pipeline

        Raises:
            PipelineHalted: in case `before` is not found.

        Returns:
            Pipeline: the pipeline itself


        """
        try:
            super().insertBefore(before, step)
        except IndexError as e:
            raise PipelineHalted(e)
        self.updateParams(self.params)
        return self

    def insertAfter(self: Self, after: str, step: Step) -> Self:
        """
        In place insertion of the children list, after the denoted name. Does not support the supply of a composite name.

        Args:
            after: the name of the step, after which the supplied step is added, the composite name can also be provided.
            step: the step to add to the pipeline


        Raises:
            PipelineHalted: in case `after` is not found.

        Returns:
            The pipeline itself
        """
        try:
            super().insertAfter(after, step)
        except IndexError as e:
            raise PipelineHalted(e)
        self.updateParams(self.params)
        return self

    def remove(self: Self, stepName: str, okNotExist: bool = False) -> Self:
        """
        In place Removal of the provided step. It can also not exist.

        Args:
            stepName (str): the step name
            okNotExist (bool, optional): Whether to ignore the fact that the step does not exist. Defaults to False.

        Raises:
            PipelineHalted: If the step does not exist and okNotExist is False.

        Returns: the pipeline.
        """
        try:
            super().remove(stepName, okNotExist=okNotExist)
        except IndexError as e:
            raise PipelineHalted(e)
        self.updateParams(self.params)
        return self

    def replace(self: Self, stepName: str, newStep: Step) -> Self:
        """
        In place replace of the provided step from the pipeline. It can also not exist.

        Args:
            stepName (str): the step name
            newStep (Block): the new step to replace

        Raises:
            PipelineHalter: If the step does not exist.

        Returns:
            The pipeline.
        """
        try:
            super().replace(stepName, newStep)
        except IndexError as e:
            raise PipelineHalted(e)
        self.updateParams(self.params)
        return self

    def append(self: Self, steps: List[Step]) -> Self:
        """Extend the pipeline with steps.

        Args:
            steps (List[Step]): the steps to extend

        Returns:
            The pipeline.
        """
        super().append(steps)
        self.updateParams(self.params)
        return self

    def prepend(self: Self, steps: List[Step]) -> Self:
        """Prepend steps to the pipeline.

        Args:
            steps (List[Step]): the steps to prepend

        Returns:
            The pipeline.
        """
        super().prepend(steps)
        self.updateParams(self.params)
        return self

    @property
    def steps(self) -> List[Step]:
        """The steps of the pipeline"""
        return self.children

    @steps.setter
    def steps(self, val: List[Step]):
        self.children = val

    @property
    def collapsedStepsAndParents(self) -> OrderedDictType[str, Step]:
        """The collapsed steps of the pipeline, parents included."""
        return self.collapsedChildrenAndParents

    @property
    def namedSteps(self) -> OrderedDictType[str, Step]:
        """The named steps of the pipeline."""
        return self.namedChildren

    @property
    def collapsedNamedSteps(
        self,
    ) -> OrderedDictType[str, Leaf]:
        """The collapsed steps of the pipeline, without parents."""
        return self.collapsedChildren

    @property
    def firstStep(self) -> Leaf:
        """The first Block of the pipeline"""
        return self.firstChild

    @property
    def lastStep(self) -> Leaf:
        """The last Block of the pipeline"""
        return self.lastChild

    def copy(self) -> "Pipeline":
        """Returns a copy of the pipeline."""
        return self.__class__(
            name=self.name,
            steps=[step.copy() for step in self.steps],
            runConfig=self.runConfig.copy(),
            version=self.version,
            cache=self.cache,
            instID=self.instID,
        )


HashFactory.registerHasher(
    Pipeline,
    lambda d: HashFactory.compute(
        [d.name + d.runConfigId] + [HashFactory.compute(p) for p in d.steps]
    ),
)
