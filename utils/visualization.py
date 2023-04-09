from math import ceil
import math
from typing import Dict, Tuple
import numpy as np
from matplotlib.widgets import Slider
from utils.image_processing import normalizeIm, automaticBrightnessAndContrast
from utils.logging import LOGGER
import matplotlib.pyplot as plt


def tryexceptpass(func):
    """
    The tryexceptpass function is a decorator that allows the function with a single argument to pass if it fails,
    return the input argument instead of the result.
    This is useful for functions that are not critical to the overall functioning of the program.

    Args:
        func: Pass the function that is being decorated
    """

    def f(inp):
        try:
            return func(inp)
        except:
            LOGGER.debug(f"{func} failed to run with input {inp}")
            return inp

    return f


from typing import Union
import cv2
from utils.path import fixPath
from collections import OrderedDict

IMS_DICT = OrderedDict()
MAX_NUMBER_TO_CACHE = 50


def loadImage(im: Union[np.ndarray, str]):
    """
    The loadImage function loads an image from a file or a numpy array.
       If the input is a string, it loads the image from that file location.
       Otherwise, it just returns the input as-is.

    Args:
        im:Union[np.ndarray,str]: Indicate that the function can accept either a string or an image

    Returns:
        A numpy array of the image
    """

    if isinstance(im, str):
        im = cv2.imread(fixPath(im), -1)
    return im


def cachedImFunc(func):
    """
    The cachedImFunc function is a decorator that caches the results of an image-loading function.
    It takes as input a function that returns an image, and returns another function with the same signature.
    The returned function will cache its results in memory, so that repeated calls to load the same file path
    will return very quickly. If the input is not a path, then caching is not used.

    Args:
        func: Get the image from disk

    Returns:
        A function that takes a path as input and returns the image at that location
    """

    def f(path: Union[np.ndarray, str]):
        if isinstance(path, str):
            if path in IMS_DICT:
                return IMS_DICT[path]
            ret = func(path)
            IMS_DICT[path] = ret
            if len(IMS_DICT) > MAX_NUMBER_TO_CACHE:
                IMS_DICT.popitem(last=False)
        else:
            return func(path)
        return ret

    return f


def preprocess(im, autoContrast=True):
    return cachedImFunc(
        lambda x: tryexceptpass(automaticBrightnessAndContrast)(
            normalizeIm(loadImage(x))
        )
        if autoContrast
        else normalizeIm(loadImage(x))
    )(im)


def plot(
    *ims, nrows=0, ncols=0, activateCarouselIn=11, autoContrast=True, useMatplotlib=True
):
    """
    The plot function takes a list of images and plots them in one figure.
    It can also take a single image, or an array of images, and plot them all on the same figure.
    The function has several options to control how the figures are displayed:
    - nrows: number of rows in which to arrange the subplots (default is 0)
    - ncols: number of columns in which to arrange the subplots (default is 0)
    - activateCarouselIn: if there are more than this many images, it will add a slider at
    the bottom that allows you to navigate between them

    Args:
        *ims: Pass a list of images to the function
        nrows=0: Create a grid of images
        ncols=0: Make the plot as wide as it needs to be to show all of the images
        activateCarouselIn=11: Activate the carousel slider
        autoContrast=True: Automatically adjust the contrast of the image

    Returns:
        The figure and axes of the plot
    """

    if len(ims) > activateCarouselIn:
        fig, axes = plt.subplots()

        # adjust the main plot to make room for the sliders
        fig.subplots_adjust(left=0.25, bottom=0.25)

        # Make a horizontal slider to control the frequency.
        axslid = fig.add_axes([0.25, 0.1, 0.65, 0.03])
        slider = Slider(
            ax=axslid, label="", valmin=1, valmax=len(ims), valinit=1, valstep=1
        )
        autoContrast = False
        preprocessed = preprocess(ims[0], autoContrast=autoContrast)
        artist = axes.imshow(
            preprocessed,
            "gray" if len(preprocessed.shape) == 2 else None,
            interpolation="nearest",
        )
        axes.axis("off")
        axes = [axes]

        def update(val):
            artist.set_data(preprocess(ims[int(val) - 1], autoContrast=autoContrast))
            fig.canvas.draw_idle()

        slider.on_changed(update)

    else:
        assert not nrows or not ncols, "Leave nrows or ncols undefined"
        if not ncols and not nrows:
            ncols = math.ceil(np.sqrt(len(ims)))
            nrows = math.ceil(len(ims) / ncols)
        if not nrows:
            nrows = math.ceil(len(ims) / ncols)
        if not ncols:
            ncols = math.ceil(len(ims) / nrows)
        fig, axes = plt.subplots(
            nrows,
            ncols,
            sharex=True,
            sharey=True,
            gridspec_kw={"wspace": 0, "hspace": 0},
            squeeze=True,
        )

        try:
            axes = axes.flatten()
        except AttributeError:
            axes = [axes]
        [ax.axis("off") for ax in axes]
        for ax, im in zip(axes, ims):
            im = preprocess(im)
            ax.imshow(
                im,
                "gray" if len(im.shape) == 2 else None,
                interpolation="nearest",
            )

    return fig, axes

