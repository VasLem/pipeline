import colorlog
from config import CONFIG
import logging

try:
    from PyQt5 import QtGui
    from PyQt5.QtCore import QThread
except ImportError:
    QtGui = None
    QThread = None


class CustomFormatter(logging.Formatter):
    try:
        FORMATS = {
            logging.ERROR: ("[%(levelname)-8s] %(message)s", QtGui.QColor("red")),
            logging.DEBUG: (
                "[%(levelname)-8s] [%(filename)s:%(lineno)d] %(message)s",
                "green",
            ),
            logging.INFO: ("[%(levelname)-8s] %(message)s", "#0000FF"),
            logging.WARNING: (
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                QtGui.QColor(100, 100, 0),
            ),
        }
    except NameError:
        pass

    def format(self, record):
        last_fmt = self._style._fmt
        opt = CustomFormatter.FORMATS.get(record.levelno)
        if opt:
            fmt, color = opt
            self._style._fmt = '<font color="{}">{}</font>'.format(
                QtGui.QColor(color).name(), fmt
            )
        res = logging.Formatter.format(self, record)
        self._style._fmt = last_fmt
        return res


def updateLoggingHandler(handler, useGUI=False):
    if useGUI:
        formatter = CustomFormatter(
            "%(log_color)s%(levelname)s - %(asctime)s - %(module)s:"
            + ":%(funcName)s():%(lineno)d:%(reset)s %(message)s"
        )
    else:
        formatter = colorlog.ColoredFormatter(
            "%(log_color)s%(levelname)s - %(asctime)s - %(module)s:"
            + ":%(funcName)s():%(lineno)d:%(reset)s %(message)s"
        )
    handler.setFormatter(formatter)


def getLogger():
    """Get logger
    :return: the logger
    :rtype: :meth:`logging.Logger`
    """
    logger = colorlog.getLogger("Cartegena")
    logger.handlers = []
    handler = colorlog.StreamHandler()
    updateLoggingHandler(handler)
    logger.addHandler(handler)
    logger.setLevel(CONFIG["GENERAL"]["verbosity"])
    return logger


EMIT_PROGRESS_HANDLES = {}


def registerProgressSignal(signal):
    if QThread is None:
        return
    EMIT_PROGRESS_HANDLES[(int(QThread.currentThreadId()), "progress")] = signal


def unregisterProgressSignal():
    if QThread is None:
        return
    key = (int(QThread.currentThreadId()), "progress")
    if key in EMIT_PROGRESS_HANDLES:
        del EMIT_PROGRESS_HANDLES[key]


def emitProgress():
    if QThread is None:
        return
    if any(EMIT_PROGRESS_HANDLES):
        _id = int(QThread.currentThreadId())
        if (_id, "progress") in EMIT_PROGRESS_HANDLES:
            EMIT_PROGRESS_HANDLES[(_id, "progress")].emit()


LOGGER = getLogger()
