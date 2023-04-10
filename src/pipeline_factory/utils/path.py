import platform
import os
from typing import List
import glob

import time
from utils.logging import LOGGER
from config import PROJ_DIR

PREP = "\\\\?\\"
# Imports required to bypass mocking override during tests, don't change
from os.path import isdir as _isdir
from os.path import isfile as _isfile
from os.path import abspath as _abspath

if platform.system() == "Windows":
    import ctypes
    from ctypes import wintypes

    _GetShortPathNameW = ctypes.windll.kernel32.GetShortPathNameW
    _GetShortPathNameW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
    _GetShortPathNameW.restype = wintypes.DWORD

    def get_short_path_name(long_name):
        """
        Gets the short path name of a given long path.
        http://stackoverflow.com/a/23598461/200291
        """

        toShorten = os.path.dirname(long_name)
        output_buf_size = 0
        while True:
            output_buf = ctypes.create_unicode_buffer(output_buf_size)
            needed = _GetShortPathNameW(toShorten, output_buf, output_buf_size)
            if output_buf_size >= needed:
                return os.path.join(output_buf.value, os.path.basename(long_name))
            else:
                output_buf_size = needed

else:

    def get_short_path_name(long_name):
        return long_name


import os, heapq


def oldest_files_in_tree(rootfolder, count=1, extension=(".bak",)):
    return heapq.nsmallest(
        count,
        (
            os.path.join(dirname, filename)
            for dirname, dirnames, filenames in os.walk(rootfolder)
            for filename in filenames
            if filename.endswith(extension)
        ),
        key=lambda fn: os.stat(fn).st_mtime,
    )


def isNetworkFolder(fPath):
    fPath = fixPath(fPath)
    networkPath = [
        x for x in ["//winbe", "/imec/windows", r"\\winbe"] if fPath.startswith(x)
    ]
    if networkPath:
        return True
    return False


IS_WINDOWS = platform.system() == "Windows"


def fixPath(fPath: str, read=False, checkNetwork=True, isPart=False) -> str:
    """Fixes path, by prepending a special sequence in front of the absolute path, so that Windows
    have no issue writing paths longer than 254 characters.
    :param fPath: the path to fix
    :type fPath: str
    :param read: whether the required operation is read, defaults to False
    :type read: bool, optional
    :return: the fixed path
    :rtype: str
    """
    if not isinstance(fPath, str) or not fPath:
        return fPath
    if fPath.endswith(".py"):  # module loading
        return fPath
    if not IS_WINDOWS:
        fPath = fPath.replace("\\", "/")
        fPath = fPath.replace("//winbe", "/imec/windows")
    else:
        fPath = fPath.replace("/", "\\")
        fPath = fPath.replace(r"\imec\windows", r"\\winbe")
    if any(x in fPath.split(os.sep) for x in ("site-packages", "dist-packages")):
        return fPath
    networkPath = [x for x in ["/imec/windows", r"\\winbe"] if fPath.startswith(x)]
    if networkPath:
        networkPath = networkPath[0]
    if not isPart:
        if (
            not networkPath
            and not (not IS_WINDOWS and fPath.startswith("/"))
            and not (IS_WINDOWS and ((len(fPath) > 1) and (fPath[1] == ":")))
        ):
            fPath = os.path.join(PROJ_DIR, fPath)
    if IS_WINDOWS and (len(fPath) > 200):
        if not isPart:
            if not read:
                if not fPath.startswith(PREP) and not networkPath:
                    fPath = PREP + fPath

    if checkNetwork:

        if networkPath:
            elems = fPath.replace(networkPath + os.sep, "").split(os.sep)
            elems = [networkPath] + elems
            while not any(
                _isdir(os.path.join(*elems[: i + 1])) for i in range(1, len(elems))
            ):
                LOGGER.warning("Cannot connect to network folder...")
                LOGGER.debug(f"Path: {fPath}")
                time.sleep(1)
                LOGGER.debug("Retrying..")
    return fPath


def selectImageType(pattern: str, extensions=("png", "tiff", "tif")) -> List[str]:
    """Return the files that are the most populous for a given extension

    :param pattern: the file pattern to glob
    :type pattern: str
    :param extensions: the extensions to look up, defaults to ('png','tiff','tif')
    :type extensions: tuple, optional
    :return: the list of files of the selected image type
    :rtype: List[str]
    """
    fils = [
        list(glob.glob(fixPath(os.path.join(os.path.splitext(pattern)[0] + f".{ext}"))))
        for ext in extensions
    ]
    ls = [len(f) for f in fils]
    return [f for f in fils if len(f) == max(ls)][0]


def commonPath(paths: List[str]):
    """
    The commonPath function takes a list of paths and returns the longest common path.

    Args:
        paths: Pass a list of paths to the function

    Returns:
        The longest common substring from the input list of strings

    """

    def is_substr(find, data):
        if len(data) < 1 and len(find) < 1:
            return False
        for i in range(len(data)):
            if find not in data[i]:
                return False
        return True

    substr = ""
    if len(paths) > 1 and len(paths[0]) > 0:
        for i in range(len(paths[0])):
            for j in range(len(paths[0]) - i + 1):
                if j > len(substr) and is_substr(paths[0][i : i + j], paths):
                    substr = paths[0][i : i + j]
    return substr


from pathvalidate import sanitize_filepath


def checkPathValidity(filePath: str, raiseError: bool = False):
    if not isinstance(filePath, str):
        ret = False
    elif os.path.isdir(filePath):
        return True
    elif os.path.isfile(filePath):
        return True
    elif filePath == sanitize_filepath(filePath):
        return True
    else:
        ret = False
    if not ret and raiseError:
        raise ValueError(f"Invalid non path-like value provided: {filePath}")
    return ret


def recDeleteEmptyFolders(direc):
    def rec_rm(x):
        if not len(os.listdir(x)):
            os.rmdir(x)
            return True
        else:
            is_empty = True
            for f in os.listdir(x):
                p = os.path.join(x, f)
                if os.path.isdir(p):
                    is_empty = rec_rm(p) and is_empty
                else:
                    is_empty = False
            if is_empty:
                os.rmdir(x)
            return is_empty

    rec_rm(direc)
