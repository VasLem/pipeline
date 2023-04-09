import itertools
from math import prod

import numpy as np

from typing import Tuple

from utils.math import prime_factors
from utils.lists_operations import common_elems_with_repeats


def getValidResizeRatioForKernelAcceleration(
    im_shape: Tuple[int, int],
    kernelShape: Tuple[int, int],
    maxResizeRatio: int = 50,
    minSizeAfterResize: int = 100,
) -> float:
    """Returns the resize ratio that can get the best reduction on the supplied image, given a certain kernel,
    so that the image can be resized, convolution applied, and then revert it back to original shape.

    Args:
        im_shape (Tuple[int, int]): The image shape
        kernelShape (Tuple[int, int]): The kernel shape. The resize ratio is adjusted so that the minimum dimension of the resized kernel is greater than 3.
        maxResizeRatio (int, optional): The maximum resize ratio. Defaults to 50.
        minSizeAfterResize (int, optional): The minimum size after resize allowed for the image. Defaults to 100.

    Returns:
        float: the computed resize ratio
    """
    resizeRatio = int(
        min(
            im_shape[0] // minSizeAfterResize,
            im_shape[1] // minSizeAfterResize,
            maxResizeRatio,
            min(kernelShape) // 3,
        )
    )

    possibleRatioFactors = common_elems_with_repeats(
        prime_factors(im_shape[0]), prime_factors(im_shape[1])
    )
    possibleRatios = []

    for L in range(1, len(possibleRatioFactors) + 1):
        for subset in itertools.combinations(possibleRatioFactors, L):
            possibleRatios.append(prod(subset) if len(subset) > 1 else subset[0])
    possibleRatios = np.array(
        [p for p in set(possibleRatios) if min(kernelShape) // p >= 3]
    )
    possibleRatios = possibleRatios[possibleRatios < maxResizeRatio]
    if not np.any(possibleRatios):
        return 1
    check = np.abs(possibleRatios - resizeRatio)
    resizeRatio = possibleRatios[np.argmin(check)]
    return resizeRatio
