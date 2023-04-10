import dataclasses
from typing import Type


def dataclass_from_dict(klass, d, strict=False):
    try:
        if isinstance(d, klass):
            return d
        fieldtypes = {f.name: f.type for f in dataclasses.fields(klass)}
        if not strict:
            return klass(
                **{
                    f: dataclass_from_dict(fieldtypes[f], d[f])
                    for f in fieldtypes
                    if f in d
                }
            )
        return klass(
            **{f: dataclass_from_dict(fieldtypes[f], d[f]) for f in fieldtypes}
        )
    except TypeError:
        return d
