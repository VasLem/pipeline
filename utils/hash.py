"""
Run-independent dynamically extendable hashing factory
"""
import pickle
import types
from hashlib import sha512
from typing import Any, Callable, Dict, Tuple, Union
from types import new_class
from collections import OrderedDict

try:
    import cloudpickle
except ImportError:
    cloudpickle = None
    pass
try:
    import numpy as np
except ImportError:
    np = object
    np.ndarray = new_class("ndarray")
try:
    import pandas as pd
except ImportError:
    pd = object
    pd.DataFrame = new_class("DataFrame")
    pd.Series = new_class("Series")

try:
    import torch
except ImportError:
    torch = object
    torch.is_tensor = lambda x: False

try:
    from utils.config import Configuration as _Configuration
except ImportError:
    _Configuration = lambda x: False
    pass
try:
    from utils.logging import LOGGER
except:
    import logging

    LOGGER = logging.getLogger(__name__)


class HashFactory:
    """
    Builtin supported objects for input/output hashing:
    - Builtin types
    - Block
    - Pipeline
    - Dictionaries
    - Lists and tuples
    - Sets
    - torch Tensors
    - Numpy arrays
    - pandas Dataframes and Series
    - anything else that is picklable or cloud picklable. For that case, a warning will be raised, as the hashes might be different between runs.
    """

    REGISTERED_HASHERS: Dict[Union[type, Tuple[type], Callable], Callable[..., str]] = {
        (lambda x: x is None): (lambda d: "None"),
        (bool, str, int, float): lambda d: sha512(str(d).encode("utf-8")).hexdigest(),
        (lambda x: isinstance(x, (np.integer, np.floating))): lambda d: sha512(
            str(d).encode("utf-8")
        ).hexdigest(),
        (dict, OrderedDict): lambda d: HashFactory.compute(
            sorted(d.items(), key=lambda x: str(x[0]))
        ),
        (list, tuple): lambda d: sha512(
            ",".join(HashFactory.compute(x) for x in d).encode("utf-8")
        ).hexdigest(),
        set: lambda d: HashFactory.compute(sorted(d)),
        _Configuration: lambda d: sha512(pickle.dumps(d)).hexdigest(),
        torch.is_tensor: lambda d: HashFactory.compute(d.cpu().numpy()),
        (np.ndarray, pd.DataFrame, pd.Series): lambda d: sha512(
            pickle.dumps(d)
        ).hexdigest(),
    }

    @classmethod
    def registerHasher(
        cls, key: Union[type, Tuple[type, ...], Callable], val: Callable[..., str]
    ):
        assert (
            isinstance(key, (types.FunctionType, types.BuiltinFunctionType))
            or isinstance(key, tuple)
            or isinstance(key, type)
        )
        assert callable(val)
        cls.REGISTERED_HASHERS[key] = val

    @classmethod
    def compute(cls, d: Any) -> str:
        """Returns a unique identifier for the supplied object"""
        for typOrFun, hasher in cls.REGISTERED_HASHERS.items():
            if (
                isinstance(typOrFun, (types.FunctionType, types.BuiltinFunctionType))
                and typOrFun(d)
                or isinstance(typOrFun, tuple)
                and type(d) in typOrFun
                or type(d) == typOrFun
            ):
                return hasher(d)
        LOGGER.warning(
            f"Object: \n{d}\n of type {type(d)} is not handled, run independent determinism is not ensured, a handler needs to be registered!"
        )
        try:
            return sha512(pickle.dumps(d)).hexdigest()
        except:
            try:
                if cloudpickle is None:
                    raise ValueError
                return sha512(cloudpickle.dumps(d)).hexdigest()
            except:
                raise ValueError(f"Object {d} could not be hashed!")
