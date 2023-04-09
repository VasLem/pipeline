import os
from abc import abstractmethod
from config import CONFIG
from PyQt5.QtWidgets import QFileDialog


class Loader(QFileDialog):

    DEFAULT_DIR = CONFIG["GENERAL"]["default_load_dir"]

    @classmethod
    def updateDir(cls, value):
        Loader.DEFAULT_DIR = value

    def __init__(self, title=""):
        super().__init__()
        self.title = title
        self.left = 10
        self.top = 10
        self.width = 640
        self.height = 480
        self._data = None

    def get(self):
        self.setWindowTitle(self.title)
        self.setGeometry(self.left, self.top, self.width, self.height)
        self.setDirectory(Loader.DEFAULT_DIR)
        self._data = self.open()
        if self._data:
            if os.path.isfile(self._data):
                self.updateDir("/".join(self._data.split("/")[:-1]))
            else:
                self.updateDir(self._data)
        return self._data

    @property
    def data(self):
        # The returned information from the dialog
        if self._data is None:
            self._data = self.get()
        return self._data

    @data.setter
    def data(self, val):
        self._data = val

    @abstractmethod
    def open(self):
        return


class DirLoader(Loader):
    def open(self):
        dirName = str(self.getExistingDirectory(self, self.title))
        return dirName

from typing import Union,List
class FileLoader(Loader):
    def __init__(self, fileTypes:Union[str, List[str]]="*", **kwargs):
        super().__init__(**kwargs)
        self.fileTypes = fileTypes
        if self.fileTypes is None:
            self.fileTypes = "*"
        if isinstance(self.fileTypes, list):
            self.fileTypes = " ".join(self.fileTypes)

    def open(self) -> str:
        options = self.Options()
        options |= self.DontUseNativeDialog
        fileName, _ = self.getOpenFileName(
            self, self.title, "", f"Accepted files ({self.fileTypes})", options=options
        )
        self.close()
        return fileName
