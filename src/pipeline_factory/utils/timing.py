from types import FunctionType
from functools import wraps
import time
from utils.logging import LOGGER
import logging
from typing import Any, Tuple, Mapping
from datetime import datetime


def timer(method, logLevel, logAt):
    @wraps(method)
    def wrapped(*args, **kwargs):
        self = args[0]
        if hasattr(self, "name"):
            name = f"{self.__class__.__name__}: {self.name}"
        else:
            name = f"{self.__class__.__name__}"
        t0 = time.time()
        ret = method(*args, **kwargs)
        t1 = time.time()
        if t1 - t0 > logAt:
            LOGGER.log(
                logLevel,
                f"Function {method.__name__} of {name} took {t1-t0} seconds to finish",
            )
        return ret

    return wrapped


class TimeRegistration(type):
    @classmethod
    def __prepare__(
        metacls, __name: str, __bases: Tuple[type, ...], **kwds: Any
    ) -> Mapping[str, object]:
        return super().__prepare__(__name, __bases, **kwds)

    def __new__(meta, classname, bases, classDict, **kwds):
        newClassDict = {}
        for attributeName, attribute in classDict.items():
            if isinstance(attribute, FunctionType) and not attributeName.startswith(
                "_"
            ):
                attribute = timer(
                    attribute,
                    logLevel=kwds.get("logLevel", logging.DEBUG),
                    logAt=kwds.get("logAt", None),
                )
            newClassDict[attributeName] = attribute
        return super().__new__(meta, classname, bases, classDict)

    def __init__(cls, name, bases, namespace, **kwargs):
        super().__init__(name, bases, namespace)


def getCurrentTimeString() -> str:
    """Returns the current date time in the following format: 20230130_22-14-54"""
    return datetime.now().strftime("%Y%m%d_%H-%M-%S")
