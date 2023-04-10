import os
import shelve
from typing import (
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
)
from typing import OrderedDict as OrderedDictType

import joblib
from typing_extensions import TypeVarTuple, Unpack
from functools import wraps
from pipeline.base.file_structure import FileStructure
from utils.logging import LOGGER
from utils.hash import HashFactory
from pipeline.base.exceptions import InvalidCache

InputArgs = TypeVarTuple("InputArgs")
OutputArgs = TypeVar("OutputArgs", bound=tuple)


class Cacher(FileStructure, Generic[Unpack[InputArgs], OutputArgs]):
    """Class that provides to the inheritor runs independent caching processes."""

    hasher = HashFactory()
    _cacheDir = None
    _cachedInputHash = None
    _cachedOutput = None
    _cachedOutputHash = None
    _cachedInstances = None
    _hash = None
    _inputHash = None
    _maximumSavedNum = 10
    error = None
    cache = True

    @property
    def maximumSavedNum(self) -> int:
        return self._maximumSavedNum

    @maximumSavedNum.setter
    def maximumSavedNum(self, val: int):
        self._maximumSavedNum = val

    @property
    def canWriteToCache(self) -> bool:
        """
        Tries to create the cache directory, if it fails returns False.
        """
        try:
            os.makedirs(self.cacheDir, exist_ok=True)
            with open(os.path.join(self.cacheDir, "tmp"), "w") as out:
                out.write("")
            os.remove(os.path.join(self.cacheDir, "tmp"))
        except OSError:
            LOGGER.warning("Cannot write to cache, reason:", exc_info=True)
            return False
        return True

    def updateCache(self, output: OutputArgs) -> None:
        """Meant to update cache, using the already seen input and the provided output."""
        LOGGER.info(f"Updating cache of {self.name}..")
        self.saveInputHash()
        self.saveOutputToCache(output)
        self.clearOldInstances()

    @property
    def cacheDir(self) -> str:
        """Creates the cache directory upon call

        :return: The cache directory, where the output of the block, or its children is saved
        :rtype: str
        """
        if self._cacheDir is None:
            cacheDir = self._getDir("cache_dir", ignoreInstance=True)
            os.makedirs(cacheDir, exist_ok=True)
            self._cacheDir = cacheDir
        return self._cacheDir

    @cacheDir.setter
    def cacheDir(self, value: Optional[str]):
        self._cacheDir = value

    @property
    def cachedOutputPath(self) -> str:
        """Returns the cache path, where the output is saved. Makes parent folder upon call.

        :return: The cache path, where the output of the block is meant to be saved
        :rtype: str
        """

        return os.path.join(self.cacheDir, "output")

    @property
    def cachedOutputHashPath(self) -> str:
        """Returns the cache path, where the output hash is saved. Makes parent folder upon call.

        :return: The cache path, where the output of the block is meant to be saved
        :rtype: str
        """
        return os.path.join(self.cacheDir, "output_hash")

    @property
    def hashPath(self) -> str:
        """Returns the hash path, where the code hash is saved. Makes parent folder upon call.

        :return: The hash path, where the code hash of the block is meant to be saved
        :rtype: str
        """
        return os.path.join(self.cacheDir, "hash.pkl")

    @property
    def inputHashPath(self) -> str:
        """Creates the hash path, where the input hash is saved. Makes parent folder upon call.

        :return: The hash path, where the code hash of the block is meant to be saved
        :rtype: str
        """
        return os.path.join(self.cacheDir, "input_hash")

    def computeInputHash(self, args):
        self._inputHash = self.hasher.compute(args)
        return self._inputHash

    def loadHash(self) -> Optional[str]:
        """
        The loadHash function loads the hash from a file.
                If the file does not exist, it raises an exception.
        Returns:
            A string
        """

        if not os.path.isfile(self.hashPath):
            self.error = f"Cached code hash file {self.hashPath} does not exist"
            raise InvalidCache(self.error)
        return joblib.load(self.hashPath)

    def loadSavedInstances(self) -> List[str]:
        """
        The loadSavedInstances function is used to load the saved instances of a given input hash.
            It does this by opening the shelve file associated with that input hash and returning a list of all keys in that shelve file.
        Returns:
            A list of keys
        """

        if not os.path.isfile(self.inputHashPath + ".bak"):
            self.error = f"Saved instances file {self.inputHashPath}.bak does not exist"
            raise InvalidCache(self.error)
        with shelve.open(self.inputHashPath) as h:
            return list(h.keys())

    def loadInputHash(self) -> Union[str, List[str]]:
        """
        The loadInputHash function is used to load the input hash from a shelve file.
        Returns:
            The hash of the input file
        """

        if not os.path.isfile(self.inputHashPath + ".bak"):
            self.error = (
                f"Cached input hash file {self.inputHashPath}.bak does not exist"
            )
            raise InvalidCache(self.error)
        with shelve.open(self.inputHashPath) as h:
            if str(self.instID) not in h:
                self.error = f"Instance {self.instID} not found in {list(h.keys())}"
                raise InvalidCache(self.error)
            return h[str(self.instID)]

    def clearOldInstances(self) -> List[str]:
        with shelve.open(self.inputHashPath) as h:
            keys = list(h.keys())
        if len(keys) < self.maximumSavedNum:
            return []

        LOGGER.debug(
            f"Cache exceeding size of {self.maximumSavedNum}, removing: {keys[: -self.maximumSavedNum]}"
        )
        keys = keys[: -self.maximumSavedNum]
        with shelve.open(self.inputHashPath) as h:
            for k in keys:
                try:
                    del h[k]
                except KeyError:
                    pass

        with shelve.open(self.cachedOutputHashPath) as h:
            for k in keys:
                try:
                    del h[k]
                except KeyError:
                    pass
        with shelve.open(self.cachedOutputPath) as h:
            for k in keys:
                try:
                    del h[k]
                except KeyError:
                    pass
        return keys

    def saveInputHash(self) -> None:
        """
        The saveInputHash function saves the hash of the input arguments to a shelve database.
            The function also saves the code hash, which is used to determine if there are any changes in
            code between runs. If there are no changes in either inputs or code, then we can skip running
            this instance and just return its output from cache.

        Args:
            args: Get the hash of the input arguments
        """
        with shelve.open(self.inputHashPath, writeback=True) as h:
            h[str(self.instID)] = self._inputHash

        # Saving code hash as well
        joblib.dump(self.hash, self.hashPath)
        return

    def clearInputHash(self) -> bool:
        """
        The clearInputHash function is used to clear the input hash for a given instance. Returns True if cleared successfully
        """
        if not os.path.isfile(self.inputHashPath + ".bak"):
            return False
        with shelve.open(self.inputHashPath) as h:
            if str(self.instID) not in h:
                return False
            del h[str(self.instID)]
        return True

    def loadCachedOutputHash(self) -> Union[str, List[str]]:
        """
        The loadCachedOutputHash function loads the cached output hash file.
        Returns:
            The cached output hash for the given instance id
        """

        if not os.path.isfile(self.cachedOutputHashPath + ".bak"):
            self.error = f"Cached output hash file {self.cachedOutputHashPath}.bak does not exist"
            raise InvalidCache(self.error)
        with shelve.open(self.cachedOutputHashPath) as h:
            if str(self.instID) not in h:
                self.error = f"Instance ID {self.instID} not found in cached ouput instances {list(h.keys())}"
                raise InvalidCache(self.error)
            return h[str(self.instID)]

    def loadCachedOutput(
        self,
    ) -> Union[OutputArgs, List[OutputArgs], OrderedDictType[str, OutputArgs]]:
        """
        The loadCachedOutput function loads the cached output from a shelve file.

        Args:
            self: Access the instance attributes of the class
        Raises:
            InvalidCache: If output cannot be loaded.
        """

        if not os.path.isfile(self.cachedOutputPath + ".bak"):
            self.error = (
                f"Cached output file {self.cachedOutputPath}.bak does not exist"
            )
            raise InvalidCache(self.error)
        with shelve.open(self.cachedOutputPath) as h:
            if str(self.instID) not in h:
                self.error = f"Instance ID {self.instID} not found in cached ouput instances {list(h.keys())}"
                raise InvalidCache(self.error)
            return h[str(self.instID)]

    def saveOutputToCache(self, output: OutputArgs) -> None:
        """
        The saveOutputToCache function saves the output of a function to a cache.
        """
        LOGGER.debug(f"Saving {self.name}:{self.instID} output to cache...")
        with shelve.open(self.cachedOutputPath, writeback=True) as h:
            h[str(self.instID)] = output
        with shelve.open(self.cachedOutputHashPath, writeback=True) as h:
            h[str(self.instID)] = self.hasher.compute(output)

    def clearOutputCache(self):
        if not os.path.isfile(self.cachedOutputPath + ".bak"):
            return
        with shelve.open(self.cachedOutputPath) as h:
            if not str(self.instID) in h:
                return
            del h[str(self.instID)]
        with shelve.open(self.cachedOutputHashPath) as h:
            del h[str(self.instID)]

    @property
    def cachedOutput(self) -> OutputArgs:
        if self._cachedOutput is None:
            self._cachedOutput = self.loadCachedOutput()
        return self._cachedOutput

    @property
    def cachedOutputHash(self) -> str:
        if self._cachedOutputHash is None:
            self._cachedOutputHash = self.loadCachedOutputHash()
        return self._cachedOutputHash

    @property
    def cachedInputHash(self) -> str:
        if self._cachedInputHash is None:
            self._cachedInputHash = self.loadInputHash()
        return self._cachedInputHash

    @property
    def hash(self) -> str:
        """
        Returns the hash of the current object
        """
        if self._hash is None:
            self._hash = self.hasher.compute(self)
        return self._hash

    def checkInput(self, args, hashGiven=False):
        if not self.cache:
            return False
        self._inputHash = self.hasher.compute(args) if not hashGiven else args
        try:
            loadedInputHash = self.loadInputHash()
        except InvalidCache:
            return False

        ret = self._inputHash == loadedInputHash
        if not ret:
            self.error = (
                f"Input different than the one cached:\nInput: \n"
                + (
                    (str(args)[:50] + "\n...\n" + str(args)[-50:])
                    if len(str(args)) > 100
                    else str(args)
                )
                + f"\n{self._inputHash}\nCached: {loadedInputHash}"
            )
        return ret

    def reset(self):
        super().reset()
        self._cachedOutput = None
        self._cachedOutputHash = None
        self._cachedInputHash = None

    def checkOutput(self):
        try:
            self.cachedOutput
            return True
        except InvalidCache:
            return False

    def checkOutputHash(self):
        try:
            self.cachedOutputHash
            return True
        except InvalidCache:
            return False

    def cacheExists(
        self,
        args=None,
        unknownInput=False,
        loadInputFromCache=False,
    ):
        """
        The cacheExists function checks if the cache is usable.

        Args:
            args: The input to be checked if cached. Defaults to None
            unknownInput: True if the input is unknown.
            loadInputFromCache: Load the input from cache

        Returns:
            True if the cache is usable, false otherwise
        """
        try:

            if loadInputFromCache:
                inputHash = self.cachedInputHash
            else:
                inputHash = self.computeInputHash(
                    args
                )  # necessary to be here, as it initializes self._inputHash, and things can fail afterwards
            loadedHash = self.loadHash()
            if (
                self.hasher.compute(self) != loadedHash
            ):  # The code hash is different to the one loaded
                self.error = "Step code different than the one cached"
                return False
            if not unknownInput and not self.checkInput(inputHash, hashGiven=True):
                return False
            if not self.checkOutputHash():
                return False
            return True
        except InvalidCache:

            return False
        finally:
            if self.error:
                LOGGER.debug(f"Cache of {self.name} not usable, reason: {self.error}")

    def clearCache(
        self,
        instance: Optional[str] = None,
        ifEmpty: bool = False,
        oldest100: bool = False,
        forcedAll: bool = False,
        *args,
        **kwargs,
    ) -> None:
        """
        The clearCache function is used to clear the cache directory of all files.

        Args:
            instance: The instance to clean, optional
            ifEmpty: Clear the cache if it is empty
            oldest100: Clear the oldest 100 files in the cache directory
            forcedAll: Whether to clear all cache, independently on the current instance ID.
        """
        if forcedAll:
            self._clear(self.cacheDir)
            return
        if oldest100:
            if self._clear(
                self.cacheDir, ifEmpty=ifEmpty, oldest100=oldest100, *args, **kwargs
            ):
                self._cacheDir = None
            return
        if instance is not None:
            insts = [instance]
        else:
            insts = self.getChildInstances(self)
        oldInstID = self.instID
        try:
            for self._instID in insts:
                if not ifEmpty:
                    try:
                        if self.clearInputHash():
                            LOGGER.debug(
                                f"Cleared cache from {self.name}, instance ID: {self.instID}"
                            )
                        self.clearOutputCache()
                    except BaseException as err:
                        LOGGER.warning(
                            f"Cache of {self.name} could not be cleared due to the following error",
                            stack_info=True,
                        )
                        pass
                if ifEmpty:
                    self._clear(self.cacheDir, ifEmpty=True)
        finally:
            self._instID = oldInstID

    def getChildInstances(self, obj: "Cacher"):
        from itertools import product

        def canLoad(t: InstancesCacher) -> bool:
            if not os.path.isfile(t.cachedInstancesPath + ".bak"):
                return False
            try:
                t.cachedInstances
                return True
            except KeyError:
                return False

        def getInstIds(t: Cacher) -> List[Optional[str]]:
            if isinstance(t, InstancesCacher):
                if t.instIDs is not None:
                    return t.instIDs
                if canLoad(t):
                    return t.cachedInstances
            if t.parent is None:
                return [t.instID]
            return [None]

        hierarchy = [obj]
        while hierarchy[-1].parent is not None:
            hierarchy.append(hierarchy[-1].parent)
        items = [
            [it for it in items if it is not None]
            for items in product(*[getInstIds(t) for t in hierarchy][::-1])
        ]
        try:
            instances = [os.path.join(*it) if it else None for it in items]
        except:
            raise
        if not instances:
            instances = [None]
        return instances

    def onSuccessfulCachingLoad(self):
        return

    @staticmethod
    def cached(method):
        @wraps(method)
        def inner(self: Cacher, *args, forceDo=False, **kwargs):
            if not forceDo and self.checkInput(args):
                self.onSuccessfulCachingLoad()
                return self.loadCachedOutput()
            retried = False
            while True:
                try:
                    ret = method(self, *args, **kwargs)
                    if self.cache:
                        self.updateCache(ret)
                    return ret
                except OSError as err:
                    if retried or not self.cache:
                        raise
                    if err.errno != 28:  # No space left on device
                        raise
                    LOGGER.warning(
                        "Removing old cache as no space is left in device..."
                    )
                    self.clearCache(oldest100=True)
                    retried = True
                finally:
                    self.clearCache(ifEmpty=True)

        return inner


class InstancesCacher(Cacher):
    instIDs = None

    @property
    def cachedInstancesPath(self):
        return os.path.join(self.cacheDir, "cached_instances")

    def saveInstances(self):
        """Save the instances saved to `instIDs`, if this parameter exists."""
        cachedInstancesPath = self.cachedInstancesPath
        with shelve.open(cachedInstancesPath, writeback=True) as h:
            item = (
                self.instIDs
                if hasattr(self, "instIDs")
                else [self.instID if self.parent is None else None]
            )
            key = str(self.instID)
            if not key:
                key = None
            h[str(key)] = item

    @property
    def cachedInstances(self) -> List[str]:
        if self._cachedInstances is None:
            self._cachedInstances = self.loadCachedInstances()
        return self._cachedInstances

    def loadCachedInstances(self) -> List[str]:
        """Loads the cached instances from previous runs.
        Returns a single item list with the root parent instance ID if no cached instances are found.

        Raises:
            InvalidCache: If cached instances file does not exist.

        Returns:
            Optional[List[str]]: The list of retrieved instance IDs
        """
        cachedInstancesPath = self.cachedInstancesPath
        if not os.path.isfile(cachedInstancesPath + ".bak"):
            self.error = "Cached instances file does not exist"
            raise InvalidCache(self.error)
        with shelve.open(cachedInstancesPath) as h:
            return h[str(self.instID)]

    def clearOldInstances(self) -> List[str]:
        keys = super().clearOldInstances()
        with shelve.open(self.cachedInstancesPath) as h:
            for k in keys:
                try:
                    del h[k]
                except KeyError:
                    pass
        return keys

    def reset(self):
        super().reset()
        self._cachedInstances = None

    def saveInputHash(self):
        super().saveInputHash()
        self.saveInstances()

    def clearCachedInstances(self):
        """Clears the cached instances."""
        cachedInstancesPath = self.cachedInstancesPath
        if not os.path.isfile(cachedInstancesPath + ".bak"):
            return
        with shelve.open(cachedInstancesPath) as h:
            if str(self.instID) not in h:
                return
            LOGGER.debug(
                f"Clearing {self.name} cached instances: {h[str(self.instID)]}"
            )
            del h[str(self.instID)]
        return

    def clearCache(
        self,
        instances: Optional[List[str]] = None,
        ifEmpty: bool = False,
        oldest100: bool = False,
        forcedAll: bool = False,
        *args,
        **kwargs,
    ) -> None:
        oldInstIDs = self.instIDs
        if instances is not None:
            self.instIDs = instances
        try:
            super().clearCache(
                ifEmpty=ifEmpty,
                oldest100=oldest100,
                forcedAll=forcedAll,
                *args,
                **kwargs,
            )
            self.clearCachedInstances()
        finally:
            self.instIDs = oldInstIDs

    def checkInput(self, args, hashGiven=False):
        if not super().checkInput(args, hashGiven):
            return False
        try:
            self.cachedInstances
        except InvalidCache:
            return False
        return True
