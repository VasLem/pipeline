from typing import Tuple
import numpy as np
import os
import struct
import cv2


def scale_min_max(_arr: np.ndarray, M: int = 255, dtype=np.uint8) -> np.ndarray:
    """
    > It takes an array, finds the minimum and maximum values, and then scales the array so that the
    minimum value is 0 and the maximum value is 255

    :param _arr: the array to be scaled
    :type _arr: np.ndarray
    :param M: the maximum value of the output array, defaults to 255
    :type M: int (optional)
    :param dtype: The data type of the output array
    :return: the scaled array.
    """
    mn = _arr.min()
    mx = _arr.max()
    if mx != mn:
        arr = _arr.astype(float)
        return ((arr - mn) * M / (mx - mn)).astype(dtype)
    return (_arr - mn).astype(dtype)


class UnknownImageFormat(Exception):
    pass


def getImageSize(file_path: str) -> Tuple[int, int]:
    """
    Adapted from https://github.com/scardine/image_size
    Return (width, height) for a given img file content - no external
    dependencies except the os and struct modules from core
    """
    size = os.path.getsize(file_path)

    with open(file_path, "rb") as input:
        height = -1
        width = -1
        data = input.read(25)

        if (size >= 10) and data[:6] in (b"GIF87a", b"GIF89a"):
            # GIFs
            w, h = struct.unpack("<HH", data[6:10])
            width = int(w)
            height = int(h)
        elif (
            (size >= 24)
            and data.startswith(b"\211PNG\r\n\032\n")
            and (data[12:16] == b"IHDR")
        ):
            # PNGs
            w, h = struct.unpack(">LL", data[16:24])
            width = int(w)
            height = int(h)
        elif (size >= 16) and data.startswith(b"\211PNG\r\n\032\n"):
            # older PNGs?
            w, h = struct.unpack(">LL", data[8:16])
            width = int(w)
            height = int(h)
        elif (size >= 2) and data.startswith(b"\377\330"):
            # JPEG
            msg = " raised while trying to decode as JPEG."
            input.seek(0)
            input.read(2)
            b = input.read(1)
            try:
                while b and ord(b) != 0xDA:
                    while ord(b) != 0xFF:
                        b = input.read(1)
                    while ord(b) == 0xFF:
                        b = input.read(1)
                    if ord(b) >= 0xC0 and ord(b) <= 0xC3:
                        input.read(3)
                        h, w = struct.unpack(">HH", input.read(4))
                        break
                    else:
                        input.read(int(struct.unpack(">H", input.read(2))[0]) - 2)
                    b = input.read(1)
                width = int(w)
                height = int(h)
            except struct.error:
                raise UnknownImageFormat("StructError" + msg)
            except ValueError:
                raise UnknownImageFormat("ValueError" + msg)
            except Exception as e:
                raise UnknownImageFormat(e.__class__.__name__ + msg)
        else:
            raise UnknownImageFormat(
                "Sorry, don't know how to get information from this file."
            )

    return width, height


import math

# Automatic brightness and contrast optimization with optional histogram clipping
def automaticBrightnessAndContrast(gray, clip_hist_percent=1):
    # Calculate grayscale histogram
    hist = cv2.calcHist([gray], [0], None, [256], [0, 256])
    hist_size = len(hist)

    # Calculate cumulative distribution from the histogram
    accumulator = []
    accumulator.append(float(hist[0]))
    for index in range(1, hist_size):
        accumulator.append(accumulator[index - 1] + float(hist[index]))

    # Locate points to clip
    maximum = accumulator[-1]
    clip_hist_percent *= maximum / 100.0
    clip_hist_percent /= 2.0

    # Locate left cut
    minimum_gray = 0
    while accumulator[minimum_gray] < clip_hist_percent:
        minimum_gray += 1

    # Locate right cut
    maximum_gray = hist_size - 1
    while accumulator[maximum_gray] >= (maximum - clip_hist_percent):
        maximum_gray -= 1

    # Calculate alpha and beta values
    alpha = 255 / max([1, (maximum_gray - minimum_gray)])
    beta = -minimum_gray * alpha

    """
    # Calculate new histogram with desired range and show histogram 
    new_hist = cv2.calcHist([gray],[0],None,[256],[minimum_gray,maximum_gray])
    plt.plot(hist)
    plt.plot(new_hist)
    plt.xlim([0,256])
    plt.show()
    """

    auto_result = cv2.convertScaleAbs(gray, alpha=alpha, beta=beta)
    return auto_result


def normalizeIm(im: np.ndarray, uint16=False) -> np.ndarray:
    ran = float(im.max() - im.min())
    if not ran:
        ran = 1
    if uint16:
        return ((im - im.min()) / ran * 65535).astype(np.uint16)
    return ((im - im.min()) / ran * 255).astype(np.uint8)
