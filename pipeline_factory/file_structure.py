import ast
import os
import shutil
from hashlib import sha512
from typing import Optional

from . import PIPELINE_CONFIG
from utils.config import Configuration as RunConfiguration
from utils.documentation import DocInherit
from utils.path import fixPath, oldest_files_in_tree
from utils.reload import ReloadCallerFnOnChange
from utils.timing import TimeRegistration


class MetaTimeRegistration(
    metaclass=TimeRegistration,
    logAt=ast.literal_eval(PIPELINE_CONFIG["show_runtime_gt"]),
    logLevel=PIPELINE_CONFIG["show_runtime_log_level"],
):
    ...


class CombinedMeta(MetaTimeRegistration, ReloadCallerFnOnChange, DocInherit):
    ...


class FileStructure(metaclass=CombinedMeta):
    def __init__(
        self,
        name: str,
        runConfig: Optional[RunConfiguration] = None,
        parent: Optional["FileStructure"] = None,
        version: Optional[str] = None,
        instID: Optional[str] = None,
    ):
        self.runConfig = runConfig
        self.name = name
        self.parent = parent
        self.version = version
        self.instID = instID

    def _getDir(self, attr: str, ignoreInstance=False, returnHashed=False) -> str:
        """
        Returns:
            str: the directory that corresponds to this object, given the configuration
        """
        l = [
            fixPath(PIPELINE_CONFIG[attr]),
            self.instID if self.instID is not None else "nonInstanceSpecific",
            str(self.configID),
            self.endpoint,
        ]
        if ignoreInstance:
            l.pop(1)
        if returnHashed:
            l = [l[0]] + [sha512("".join(l[1:]).encode("utf-8")).hexdigest()[:20]]
        baseDir = os.path.join(*l)
        return baseDir

    @property
    def instID(self) -> Optional[str]:
        """

        Returns:
            str: The instance ID
        """
        return self._instID

    @instID.setter
    def instID(self, val: Optional[str]):
        self.reset()
        if val is not None:
            val = str(val)
        self._instID = val

    @property
    def configID(self) -> str:
        """The instance identifier

        Raises:
            ValueError: In case runConfig has not been set

        Returns:
            str: The instance ID
        """
        if self.runConfig is None:
            raise ValueError("runConfig has not been set")
        return self.runConfig.configID

    @property
    def endpoint(self) -> str:
        """
        :return: The endpoint as defined by the hierachical structure, and the versioning.
        :rtype: str
        """
        if self.parent is not None:
            if self.name != "":
                baseDir = os.path.join(self.parent.endpoint, self.name)
            else:
                baseDir = self.parent.endpoint
        else:
            baseDir = self.name
        if self.version:
            baseDir = os.path.join(baseDir, "v" + self.version)

        return baseDir

    def reset(self):
        return

    def _clear(
        self, direc: str, ifEmpty=False, oldest100=False, *args, **kwargs
    ) -> bool:
        """
        The _clear function is used to clear an output directory.

        Args:
            direc: str: Specify the directory to clear
            ifEmpty: Check if the directory is empty or not, delete only if empty.
            oldest100: Delete the oldest 100 files in a directory

        """
        direc = fixPath(direc)
        if not os.path.isdir(direc):
            return False
        if ifEmpty and any(x[2] for x in os.walk(direc)):
            return False
        if oldest100:
            [os.remove(x) for x in oldest_files_in_tree(direc, count=100)]
            return False
        shutil.rmtree(direc, ignore_errors=True)
        return True
