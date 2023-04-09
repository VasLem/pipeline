HIGH_RUNTIME = 1
from types import FunctionType
from functools import wraps
import time
from utils.logging import LOGGER


def timer(method):
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
        if t1 - t0 > HIGH_RUNTIME:
            LOGGER.debug(
                f"Function {method.__name__} of {name} took {t1-t0} seconds to finish"
            )
        return ret

    return wrapped


class TimeRegistration(type):
    def __new__(meta, classname, bases, classDict):
        newClassDict = {}
        for attributeName, attribute in classDict.items():
            if isinstance(attribute, FunctionType) and not attributeName.startswith(
                "_"
            ):
                attribute = timer(attribute)
            newClassDict[attributeName] = attribute
        newcls = super(TimeRegistration, meta).__new__(meta, classname, bases, classDict)
        return newcls


from datetime import datetime


def getCurrentTimeString() -> str:
    """Returns the current date time in the following format: 20230130_22-14-54"""
    return datetime.now().strftime("%Y%m%d_%H-%M-%S")
