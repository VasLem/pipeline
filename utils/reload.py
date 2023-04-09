from importlib import reload
from typing import Callable
import inspect
from utils.logging import LOGGER
from functools import wraps
from types import FunctionType


def reloader(method: Callable):
    methodSource = None

    @wraps(method)
    def wrapped(self, *args, **kwargs):
        nonlocal methodSource
        if methodSource is None:
            newSource = inspect.getsource(self.fn if hasattr(self, "fn") else method)
        if newSource != methodSource:
            LOGGER.warning("Code of method has changed, reloading")
            reload(method.__module__)
            methodSource = newSource
        ret = method(self, *args, **kwargs)
        return ret

    return wrapped


class ReloadCallerFnOnChange(type):
    callerName = "_run"

    def __new__(meta, classname, bases, classDict):
        newClassDict = {}
        for attributeName, attribute in classDict.items():
            if attributeName == meta.callerName and isinstance(attribute, FunctionType):
                attribute = reloader(attribute)
            newClassDict[attributeName] = attribute
        newcls = super(ReloadCallerFnOnChange, meta).__new__(
            meta, classname, bases, classDict
        )
        return newcls
