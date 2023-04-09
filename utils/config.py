import json
import os
from abc import abstractproperty
from collections import OrderedDict
from copy import deepcopy as copy
from json import JSONEncoder
from typing import Any, Dict, Generic, List, Optional
from typing import OrderedDict as OrderedDictType
from typing import Tuple, Type, TypeVar, Union

from utils.logging import LOGGER


def _default(self, obj):
    return getattr(obj.__class__, "toJson", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


class MissingConfig(BaseException):
    """Raised when configuration is not found"""


T = TypeVar("T", bound="Configuration")


class Configuration(Generic[T]):
    @abstractproperty
    def fields(self) -> Tuple[str, ...]:
        """
        The fields to copy or to convert to dictionary.
        They need to have the exact naming as the one used to supply __init__
        """
        raise NotImplementedError

    @abstractproperty
    def hiddenFields(self) -> Tuple[str, ...]:
        """
        Fields not to be used in __str__ or __repr__
        """
        return []

    @property
    def existingFields(self) -> List[str]:
        exists = lambda x: x is not None and not (
            isinstance(x, (list, tuple, dict)) and not x
        )
        return [x for x in self.fields if exists(getattr(self, x))]

    def toDict(self) -> OrderedDictType[str, Any]:
        return OrderedDict([(x, getattr(self, x)) for x in self.existingFields])

    @classmethod
    def fromDict(cls: Type[T], dict: Dict[str, Any]) -> T:
        return cls(**dict)

    def toJson(self) -> Dict[str, Any]:
        return self.toDict()

    def copy(self) -> T:
        return self.__class__(**{f: copy(getattr(self, f)) for f in self.fields})

    def __copy__(self) -> T:
        return self.copy()

    def __str__(self):
        return (
            self.__class__.__name__
            + "\n"
            + "\n".join(
                [
                    f + ": " + str(getattr(self, f))
                    for f in self.fields
                    if f not in self.hiddenFields
                ]
            )
        )

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, obj: Optional[Union[Dict[str, Any], Any]]):
        if obj is None:
            return False
        if isinstance(obj, dict):
            obj = self.__class__(**obj)
        return all(getattr(self, f) == getattr(obj, f) for f in self.fields)


class ConfigHandlerBase(Generic[T]):
    sampleConfigurationClass: Type[T]
    samplesConfigsFile: str
    samplesConfigs: Dict[str, T] = {}
    _config: Optional[T] = None
    _name: Optional[str] = None

    def loadSamplesConfigs(self):
        """
        The loadSamplesConfigs function loads the sample configuration files.

        Returns:
            A dictionary of the loaded configurations {sampleName: sampleConfiguration}
        """

        if not os.path.isfile(self.samplesConfigsFile):
            raise ValueError(
                f"Sample configuration file {self.samplesConfigsFile} does not exist"
            )
        with open(self.samplesConfigsFile, "r") as inp:
            dat = json.load(inp)

        self.samplesConfigs = {
            entry: self.sampleConfigurationClass.fromDict(info)
            for entry, info in dat.items()
            if info
        }
        return self.samplesConfigs

    @property
    def config(self) -> Optional[T]:
        return self._config

    @property
    def name(self) -> Optional[str]:
        return self._name

    @config.setter
    def config(self, name: Optional[str]):
        if name is None:
            self._config = None
        else:
            if not self.samplesConfigs:
                samplesConfigs = self.loadSamplesConfigs()
            else:
                samplesConfigs = self.samplesConfigs
            self.newConfig = name not in samplesConfigs
            if self.newConfig:
                LOGGER.debug(f"Making new configuration: {name}")
                samplesConfigs[name] = self.sampleConfigurationClass()
            self._config = samplesConfigs[name]
        self._name = name

    def find(self, name: str) -> T:
        try:
            if not self.samplesConfigs:
                samplesConfigs = self.loadSamplesConfigs()
            else:
                samplesConfigs = self.samplesConfigs
            return samplesConfigs[name]
        except KeyError:
            raise MissingConfig

    def addItem(self, name: str, config: Union[Dict[str, Any], T]):
        """
        The addItem function adds a new item to the samplesConfigs dictionary, by also updating the corresponding file.
        It takes as input the name of the sample, and its configuration.
        The function also takes an optional argument, sampleConfigGroup, which is used to group together configurations for different samples.

        Args:
            name:str: Set the name of the sample
            config: the sample configuration of the new item
            sampleConfigGroup:str=None: Specify a sample configuration group

        Returns:
            The config dictionary
        """
        if not self.samplesConfigs:
            samplesConfigs = self.loadSamplesConfigs()
        else:
            samplesConfigs = self.samplesConfigs
        samplesConfigs[name] = (
            config
            if isinstance(config, self.sampleConfigurationClass)
            else self.sampleConfigurationClass.fromDict(config)
        )
        with open(self.samplesConfigsFile, "w") as out:
            json.dump(self.samplesConfigs, out, sort_keys=True, indent=4)

    def deleteItem(self, name: str):
        """
        name is the name of the sample to be deleted, and must be a string. The configuration file is updated.
        Args:
            self: Refer to the object instance itself
            name:str: Specify the name of the sample to be deleted
            sampleConfigGroup:str=None: Specify which sample config file to delete the item from
        """
        if not self.samplesConfigs:
            samplesConfigs = self.loadSamplesConfigs()
        else:
            samplesConfigs = self.samplesConfigs
        del samplesConfigs[name]
        with open(self.samplesConfigsFile, "w") as out:
            json.dump(self.samplesConfigs, out, sort_keys=True, indent=4)

    def renameItem(self, oldName: str, newName: str):
        """
        The renameItem function renames a sample in the samplesConfigs dictionary. The configuration file is updated.
        It also updates the file containing that dictionary.


        Args:
            oldName:str: Specify the name of the sample that is to be renamed
            newName:str: Set the new name of the sample

        Returns:
            The name of the renamed item
        """
        if not self.samplesConfigs:
            samplesConfigs = self.loadSamplesConfigs()
        else:
            samplesConfigs = self.samplesConfigs
        if newName in samplesConfigs:
            raise ValueError(f"{newName} resides already in configuration file")
        if oldName not in samplesConfigs:
            raise MissingConfig(f"Provided name {oldName} not in configuration")
        samplesConfigs[newName] = samplesConfigs.pop(oldName)
        with open(self.samplesConfigsFile, "w") as out:
            json.dump(samplesConfigs, out, sort_keys=True, indent=4)
        return newName

    def duplicateItem(self, name: str):
        """
        The duplicateItem function allows you to duplicate a sample configuration. The sample name will have added an incremental number as a suffix.

        Args:
            self: Reference the object instance of the class
            name:str: Specify the name of the sample
            sampleConfigGroup:str=None: Specify the name of the sample configuration group to which you want to add a duplicate

        Returns:
            The new name of the sample
        """
        import re

        matches = re.findall(r"(.*)\((\d+)\)$", name)
        if matches:
            if matches[0][0] in self.samplesConfigs:
                name = matches[0][0]
        cnt = 1
        newName = name
        if not self.samplesConfigs:
            samplesConfigs = self.loadSamplesConfigs()
        else:
            samplesConfigs = self.samplesConfigs
        while newName in samplesConfigs:
            newName = f"{name}({cnt})"
            cnt += 1
        samplesConfigs[newName] = samplesConfigs[name]
        with open(self.samplesConfigsFile, "w") as out:
            json.dump(self.samplesConfigs, out, sort_keys=True, indent=4)
        return newName

    def __iter__(self):
        if not self.samplesConfigs:
            samplesConfigs = self.loadSamplesConfigs()
        else:
            samplesConfigs = self.samplesConfigs
        for name, config in samplesConfigs.items():
            yield name, config

    def __len__(self):
        if not self.samplesConfigs:
            samplesConfigs = self.loadSamplesConfigs()
        else:
            samplesConfigs = self.samplesConfigs
        return len(samplesConfigs)


class ConfigHandler(ConfigHandlerBase[T], Generic[T]):
    def __init__(
        self,
        sampleConfigurationClass: Type[T],
        samplesConfigsFile: str,
    ):
        self.sampleConfigurationClass = sampleConfigurationClass
        self.samplesConfigsFile = samplesConfigsFile

        self.multipleNames = []
        self.multipleConfigs = []
        self.loadSamplesConfigs()
