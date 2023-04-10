import os
import pathlib
import traceback
from copy import deepcopy as copy
from typing import (
    Callable,
    List,
    Union,
    Generic,
    Literal,
    Optional,
    TypeVar,
    TYPE_CHECKING,
)
from typing_extensions import TypeVarTuple, Unpack
import numpy as np
import pandas as pd
import inspect
import ast
from pipeline.base.exceptions import BlockError, REGISTERED_EXCEPTIONS
from pipeline.base.reporter import Reporter
from pipeline.base.writer import Writer
from pipeline.base.hierarchical_model import HierarchyLeaf
from pipeline import PIPELINE_CONFIG
from utils.config import Configuration as RunConfiguration
from utils.logging import LOGGER, emitProgress
from bs4 import BeautifulSoup

InputArgs = TypeVarTuple("InputArgs")
OutputArgs = TypeVar("OutputArgs", bound=tuple)
if TYPE_CHECKING:
    from pipeline.base.pipeline import Pipeline

Node = TypeVar("Node", bound=Union["Block", "Pipeline"])
Leaf = TypeVar("Leaf", bound="Block")
from pipeline.base.cacher import Cacher
from utils.hash import HashFactory


class Block(
    HierarchyLeaf["Pipeline", Leaf, Node],
    Cacher,
    Generic[Leaf, Node, Unpack[InputArgs], OutputArgs],
    # metaclass=TimeRegistration,
):
    """Used to implement a single step in the `pipeline.base.Pipeline`."""

    _isblock = True  # Necessary variable for checking type, in case of class wrapping

    def __init__(
        self,
        name: str,
        fn: Callable[["Block", Unpack[InputArgs]], OutputArgs],
        parent: Optional["Pipeline"] = None,
        version: str = "",
        description: str = "",
        runConfig: Optional[RunConfiguration] = None,
        instID: Optional[str] = None,
        cache: bool = True,
        hideInShortenedGraph: bool = False,
        deletePreviousResult: bool = True,
        **params,
    ):
        """
        Args:
            name (str): Name of the block.
            fn (Callable): The function to be called when the block is run.
            parent (HierarchyLeaf): The parent of the block, defaults to None.
            runConfig (RunConfiguration): Configures the results and caching directories provides variables used by the `steps`
            version (str, optional): The version of the pipeline, if provided, adds an extra level in the pipeline directory structure. Defaults to ''.
            instID (str, optional): The block instance ID, to differentiate it from same name blocks. Adds an additional directory if given. Defaults to None.
            cache (bool, optional): Whether to cache the results. Defaults to true.
            hideInShortenedGraph (bool, optional): Whether not to show this block in the shortened graph version. Defaults to False.
            deletePreviousResult (bool, optional): Whether to clear results folder upon running of the block. Defaults to True.
            params: Additional parameters to add as object attributes
        """

        self.fn = fn
        self._instID = None
        self.version = version
        self.description = description
        self._parallel = None
        self._resultsDir = None
        self._featuresDir = None
        self._reporter = None
        self._writer = None
        self._onCopy = False
        self._cache = (
            ast.literal_eval(PIPELINE_CONFIG["use_caching"])
            and cache  # Do not affect the children as well, unless explicitly stated
        )
        self._runConfig = None
        HierarchyLeaf.__init__(
            self, name=name, parent=parent, hideInShortenedGraph=hideInShortenedGraph
        )
        Cacher.__init__(
            self,
            name=name,
            runConfig=runConfig,
            parent=parent,
            version=version,
            instID=instID,
        )
        self.deletePreviousResult = deletePreviousResult
        [setattr(self, key, params[key]) for key in params]

    @property
    def runConfig(self) -> RunConfiguration:
        return self._runConfig

    @runConfig.setter
    def runConfig(self, value: Optional[RunConfiguration]):
        if value is None:
            return
        self._runConfig = value
        self._cache = self._cache and self.canWriteToCache

    @property
    def nextCollapsedStep(self) -> Optional["Block"]:
        """
        Returns:
            The step after this one, ignoring hierarchies, if it exists
        """
        return self.nextCollapsed

    @property
    def previousCollapsedStep(self) -> Optional["Block"]:
        """
        Returns:
            The step before this one, ignoring hierarchies, if it exists
        """
        return self.previousCollapsed

    @property
    def nextStep(self) -> Optional[Node]:
        """
        Returns:
            The step after this one,  if it exists in the current hierarchy
        """
        return self.next

    @property
    def previousStep(self) -> Optional[Node]:
        """
        Returns:
            The step previous this one,  if it exists in the current hierarchy
        """
        return self.previous

    @property
    def cache(self) -> bool:
        """
        Whether to cache or not the run.
        """
        return self._cache

    @cache.setter
    def cache(self, val: bool):
        self._cache = val

    @property
    def reporter(self) -> Optional[Reporter]:
        if self._reporter is None and not self._onCopy:
            self._reporter = Reporter(
                self,
                dbName=PIPELINE_CONFIG["reports_db_name"],
                reportsDir=PIPELINE_CONFIG["reports_dir"],
            )
        return self._reporter

    @property
    def writer(self) -> Writer:
        if self._writer is None:
            self._writer = Writer(self)
        return self._writer

    def copy(self):
        """
        The copy function is a wrapper around the copy.copy function that
            handles some of the thread-unsafe attributes of this class.
        """
        threadUnsafe = {}
        self.parent = None
        for param in ("parent", "_reporter", "_writer"):
            if hasattr(self, param):
                threadUnsafe[param] = getattr(self, param)
                setattr(self, param, None)
        self._onCopy = True
        try:
            ret = copy(self)
        except:
            raise
        for param, val in threadUnsafe.items():
            setattr(ret, param, val)
            setattr(self, param, val)
        self._onCopy = False
        ret._onCopy = False
        return ret

    def updateParams(self, params: dict):
        """Update object's attributes with the supplied parameters dictionary

        Args:
            params (dict): parameters dictionary
        """
        try:
            [setattr(self, k, v) for k, v in params.items()]
        except:
            raise

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def write(
        self,
        path: Union[str, List[str]],
        data: Optional[Union[pd.DataFrame, List[np.ndarray], np.ndarray]] = None,
        desc="",
        report=True,
        level="info",
        **kwargs,
    ):
        """Using the included reporter, write to disk and update database. Consult :func:`~pipeline.base.reporter.Reporter.write`
        for further assistance with the additional keyword arguments.
        Args:
            path (Union[str, List[str]]): the path to save the input
            data (Union[pd.DataFrame, List[np.ndarray], np.ndarray], optional): the data. Defaults to None.
            desc (str): the description of the input
        """
        if isinstance(path, str):
            LOGGER.debug(f"Writing data to {path}")
        if report:
            if self.reporter is None:
                raise ValueError("_onCopy variable must be set to False")
            return self.reporter.write(
                desc=desc, path=path, data=data, level=level, **kwargs
            )
        return self.writer.write(path=path, data=data, **kwargs)

    def writeResult(
        self,
        fname: Union[str, List[str]],
        data: Optional[Union[pd.DataFrame, List[np.ndarray], np.ndarray]] = None,
        desc="",
        report=True,
        level: Literal["debug", "info"] = "info",
        **kwargs,
    ):
        """Using the included reporter, write to `resultsDir` and update database. Consult :func:`~pipeline.base.reporter.Reporter.write`
        for further assistance with the additional keyword arguments.
        Args:
            fname (Union[str, List[str]]): the name of the file(s) to save the input
            data (Union[pd.DataFrame, List[np.ndarray], np.ndarray], optional): the data. Defaults to None.
            desc (str): the description of the input
        """
        self.write(
            path=(
                os.path.join(self.resultsDir, fname)
                if isinstance(fname, str)
                else [os.path.join(self.resultsDir, f) for f in fname]
            ),
            data=data,
            desc=desc,
            report=report,
            level=level,
            **kwargs,
        )

    def createReport(
        self, samplesNames: List[str] = [], level: Literal["debug", "info"] = "info"
    ) -> Optional[BeautifulSoup]:
        """
        The createReport function creates a report for the samples in the sample set, recursively accessing its descendants.
        It returns a compiled html. If the object has no parent, it tries to open up the report in the user's browser.

        Args:
            samplesNames: Specify which samples to include in the report, leave empty for all samples

        Returns:
            The html report
        """
        if self.reporter is None:
            raise ValueError("_onCopy variable must be set to False")
        report = self.reporter.createReport(samplesNames, level)
        if self.isRoot and report is not None:
            try:
                import webbrowser

                webbrowser.open_new_tab(
                    pathlib.Path(os.path.abspath(self.reporter.reportPath)).as_uri()
                )
            except:
                pass
        return report

    def setName(self, name: str):
        """In place set name"""
        self.name = name
        return self

    def setDescription(self, description: str):
        """In place set description"""
        self.description = description
        return self

    def removeReported(self):
        """Recursively clear reporter register."""
        if self.reporter is not None:
            self.reporter.clear()

    def run(self, inp: "tuple[Unpack[InputArgs]]" = tuple(), *args, **kwargs):
        try:
            return self._run(inp, *args, **kwargs)
        except:
            raise

    @Cacher.cached
    def _run(
        self, inp: "tuple[Unpack[InputArgs]]" = tuple(), *args, **kwargs
    ) -> "OutputArgs":
        ret = tuple()
        if inp is None:
            inp = tuple()
        self.reset()
        if self.deletePreviousResult:
            self.clearResults()
        LOGGER.info(f"Running: {self.compositeName}..")
        os.makedirs(self.resultsDir, exist_ok=True)
        if self.reporter is not None:
            self.reporter.clear()
        try:
            ret = self.fn(self, *inp)
            emitProgress()
        except BaseException as err:
            self.finalize()
            if isinstance(err, tuple(REGISTERED_EXCEPTIONS)):
                raise
            raise BlockError(self.name, traceback.format_exc()) from err
        if ret is not None and not isinstance(ret, tuple):
            ret = (ret,)
        self.finalize()
        return ret

    def finalize(self):
        """
        Cleaning up steps after run.
        """
        # LOGGER.debug(f"Step {self.compositeName} is finalizing..")
        self.clearResults(ifEmpty=True, selfOnly=True)
        # LOGGER.debug(f"Step {self.compositeName} finalized successfully.")

    def onSuccessfulCachingLoad(self, *args, **kwargs) -> None:
        """To emit progress when skipping this step"""
        LOGGER.info(f"Step {self.compositeName} is cached, skipping")
        LOGGER.debug(f"Step has instance ID: {self.instID}")
        emitProgress()

    def reset(self):
        super().reset()
        self.cacheDir = None
        self.resultsDir = None
        self._hash = None

    @property
    def resultsDir(self) -> str:
        """
        Creates  the results directory if it does not exist, upon call.

        :return: the path to a directory where intermediate visualization results files will be written.
        :rtype: str
        """

        if self._resultsDir is None:
            resultsDir = self._getDir("results_dir")
            self._resultsDir = resultsDir

        return self._resultsDir

    @resultsDir.setter
    def resultsDir(self, val: Optional[str]):
        self._resultsDir = val

    def clearResults(self, ifEmpty=False, oldest100=False, *args, **kwargs) -> None:
        """
        The clearResults function is used to clear the results directory of all files.

        Args:
            ifEmpty: Clear the results directory if it is empty
            oldest100: Clear the oldest 100 files in the results directory
        """

        self._clear(
            self.resultsDir, ifEmpty=ifEmpty, oldest100=oldest100, *args, **kwargs
        )

    def clearReport(self, *args, **kwargs) -> None:
        """Clear the reports database"""
        LOGGER.debug(f"Clearing report from {self.compositeName}..")
        if self.reporter is not None:
            self.reporter.clear()
        emitProgress()

    def __str__(self):
        return f"{self.__class__.__qualname__}({self.name}{self.version if self.version else ''})"

    def __repr__(self):
        return self.__str__()


HashFactory.registerHasher(
    Block,
    lambda d: HashFactory.compute(d.name + inspect.getsource(d.fn)),
)
