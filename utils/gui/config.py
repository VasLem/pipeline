from PyQt5.QtWidgets import (
    QHBoxLayout,
    QVBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QWidget,
    QListWidgetItem,
    QFrame,
    QMenu,
    QAction,
)
from PyQt5 import QtCore

from utils.config import ConfigHandler
from utils.gui.prompt import OkCancelDialog
from typing import Dict, Literal, Union
from utils.gui.custom_widgets import ExtendedListWidget
from collections import OrderedDict


class ConfigPlugin(QWidget, ConfigHandler):
    itemClicked = QtCore.pyqtSignal(str)

    def __init__(
        self,
        sampleConfigurationClass,
        samplesConfigsFile: str,
        allowChoose: bool = True,
        multiple: bool = False,
        modifiable: bool = False,
        addEntryButton: bool = True,
        addMenu: bool = False,
        label="Sample Name",
        modality: Literal["image", "video"] = "image",
        parent=None,
        **kwargs,
    ):
        """
        Main widget to load and show available confgurations.
        Args:
            sampleConfigurationClass (_type_): the class of the sample configuration objects
            samplesConfigsFile (str): the configuration file path
            allowChoose (bool, optional): Whether to show a list of the available configuration to choose from. Defaults to True.
            multiple (bool, optional): If allowChoose, whether to transform the list of available samples to a checkbox list, for multiple configurations selection. Defaults to False.
            modifiable (bool, optional): If modifiable, whether to allow in place editing. Defaults to False.
            addEntryButton (bool, optional): Whether to add a "New Sample" button. Defaults to True.
            addMenu (bool, optional): If allowChoose, whether to add a right click menu to the entries. Defaults to False.
            label (str, optional): The label to use in case allowChoose is False. Defaults to "Sample Name".
            modality (Literal[&quot;image&quot;, &quot;video&quot;], optional): The handled modality. Defaults to "image".
            parent (_type_, optional): The parent layout. Defaults to None.
        """
        self.allowChoose = allowChoose
        self.multiple = multiple
        self.modifiable = modifiable
        self.label = label
        self.addEntryButton = addEntryButton
        self.addMenu = addMenu
        self._modality: Literal["image", "video"] = None
        if modality is None:
            modality = "image"
        super().__init__(
            sampleConfigurationClass=sampleConfigurationClass,
            samplesConfigsFile=samplesConfigsFile,
            parent=parent,
        )
        try:
            self.setFrameShape(QFrame.StyledPanel)
            self.setFrameShadow(QFrame.Plain)
            self.setLineWidth(2)
        except:
            pass

        self.setLayout(self.init())

        self.modality = modality

    def clear(self):
        self.labelBox.clear()

    def addItems(self, *args, **kwargs):
        self.labelBox.addItems(*args, **kwargs)

    def addChild(self, *args):
        self.addItem(*args)

    def addItem(self, *args):
        if len(args) == 2:
            name = args[0]
            config = args[1]
            super().addItem(name, config)
        elif len(args) == 1:
            name = args[0]
        else:
            raise ValueError(
                f"Incorrect number of arguments ({len(args)}) supplied, 1 or 2 allowed."
            )
        self.labelBox.addChild(name)

    @property
    def items(self):
        return self.labelBox.items

    def item(self, index: int):
        return self.labelBox.item(index)

    def getSampleConfig(self, key: str):
        return self.samplesConfigs[key]

    def setCurrentItem(self, val, *args, **kwargs):
        if isinstance(val, str):
            val = [cnt for cnt, x in enumerate(self.items) if x.text() == val][0]
        if not isinstance(val, int):
            val = self.items.index(val)
        self.labelBox.setCurrentItem(val, *args, **kwargs)

    def onItemClicked(self, index):
        try:
            self.itemClicked.emit(index.data())
        except AttributeError:  # TODO: investigate why recursive class child cannot use the same signal as its parent
            pass

    @property
    def modality(self) -> Literal["image", "video"]:
        """The currently handled modality, image or video
        :rtype: str
        """
        return self._modality

    def switchModality(self) -> None:
        self.modality = "image" if self.modality == "video" else "video"

    @modality.setter
    def modality(self, value: Literal["image", "video"]):
        if not value:
            value = "image"
        assert value in ("image", "video")
        self._modality = value
        if self.allowChoose:
            samplesAreVideos = {
                k: v.endLfiPath != v.startLfiPath
                for k, v in self.samplesConfigs.items()
            }
            labelBox = self.labelBox
            labelBox.clear()
            if self._modality == "image":
                labelBox.addItems([k for k, v in samplesAreVideos.items() if not v])
            else:
                labelBox.addItems([k for k, v in samplesAreVideos.items() if v])
            if labelBox.items:
                labelBox.setVisible(True)
                labelBox.update()
            else:
                labelBox.setVisible(False)

    def init(self):
        self.labelBox: Union[ExtendedListWidget, QLineEdit] = None
        if self.allowChoose:
            configLayout = QVBoxLayout()
            self.labelBox = ExtendedListWidget(multiple=self.multiple, fixedSize=False)
            self.labelBox.setObjectName("extendedList")
            self.labelBox.clicked.connect(self.onItemClicked)
        else:
            configLayout = QHBoxLayout()
            self.labelBox = QLineEdit(self)

        if self.label:
            labelLabel = QLabel(f"&{self.label}:")
            labelLabel.setBuddy(self.labelBox)
            configLayout.addWidget(labelLabel)
        configLayout.addWidget(self.labelBox)

        if self.addEntryButton:
            self.entryButton = QPushButton("New sample")
            self.entryButton.setToolTip("Create new sample")
            self.entryButton.clicked.connect(self.onCreateNew)
            configLayout.addWidget(self.entryButton)

        if self.allowChoose:
            self.labelBox.addItems(list(self.samplesConfigs.keys()))

        configLayout.update()

        if not self.modifiable:
            self.modifiable = list(self.samplesConfigs.keys())[-1]

        if self.addMenu:
            self.menu = QMenu(self.parent())
            self.deleteAction = QAction("Delete")
            self.deleteAction.setToolTip(
                "Remove selected sample configuration (cannot be undone!)"
            )
            self.menu.addAction(self.deleteAction)
            self.renameAction = QAction("Rename")
            self.menu.addAction(self.renameAction)
            self.duplicateAction = QAction("Duplicate")
            self.menu.addAction(self.duplicateAction)
            self.exportAction = QAction("Export")
            self.menu.addAction(self.exportAction)
            self.labelBox.installEventFilter(
                self if self.parent() is None else self.parent()
            )

        return configLayout

    def onCreateNew(self):
        handler = ConfigHandlerGUI(
            self.sampleConfigurationClass,
            self.samplesConfigsFile,
            windowName="Create new sample",
            allowChoose=False,
            multiple=False,
            addEntryButton=False,
            parent=self.parent(),
        )
        handler.exec_()
        if handler.cancelled:
            return False
        self.labelBox.addChild(handler.name)
        super().addItem(handler.name, handler.config)
        self.config = handler.name
        self.multipleConfigs = handler.multipleConfigs
        self.multipleNames = handler.multipleNames
        return True

    def eventFilter(self, source, event):
        if event.type() == QtCore.QEvent.ContextMenu and isinstance(
            source, ExtendedListWidget
        ):
            self.menuClick = self.menu.exec(event.globalPos())
            try:
                item = source.itemAt(event.pos())
                if self.menuClick == self.deleteAction:
                    self.onMenuDelete(source, item)
                elif self.menuClick == self.renameAction:
                    self.onMenuRename(source, item)
                elif self.menuClick == self.duplicateAction:
                    self.onMenuDuplicate(source, item)
                elif self.menuClick == self.exportAction:
                    self.onMenuExport(source, item)
            except Exception as e:
                print(f"No item selected {e}")

        return super().eventFilter(source, event)

    def onMenuDelete(self, source, item: QListWidgetItem):
        self.deleteItem(item.text())
        source.takeItem(source.row(item))
        source.update()

    def onMenuRename(self, source, item: QListWidgetItem):
        handler = ConfigHandlerGUI(
            self.sampleConfigurationClass,
            self.samplesConfigsFile,
            windowName="Rename",
            parent=self.parent(),
            allowChoose=False,
            multiple=False,
            label="New name",
        )
        handler.exec_()
        if handler.cancelled:
            return
        self.renameItem(item.text(), handler.name)
        item.setText(handler.name)

    def onMenuDuplicate(self, source, item: QListWidgetItem):
        newName = self.duplicateItem(item.text())
        item = QListWidgetItem(newName)
        source.addItem(item)
        source.update()

    def onMenuExport(self, source, item: QListWidgetItem):
        handler = ConfigHandler(
            self.sampleConfigurationClass,
            self.samplesConfigsFile,
        )
        print(handler.find(item.text()))
        # @TODO: add the export to file part


class ConfigHandlerGUI(OkCancelDialog, ConfigPlugin):
    def __init__(
        self,
        sampleConfigurationClass,
        samplesConfigsFile: str,
        allowChoose: bool = True,
        addEntryButton: bool = True,
        multiple: bool = False,
        addMenu: bool = False,
        modifiable: bool = False,
        label="Sample Name",
        modality: Literal["image", "video"] = "image",
        parent=None,
        windowName: str = "",
        **kwargs,
    ):
        """
        Dialog to load and show available configurations.
        Args:
            sampleConfigurationClass (_type_): the class of the sample configuration objects
            samplesConfigsFile: the configuration file path.
            allowChoose (bool, optional): Whether to show a list of the available configuration to choose from. Defaults to True.
            multiple (bool, optional): If allowChoose, whether to transform the list of available samples to a checkbox list, for multiple configurations selection. Defaults to False.
            modifiable (bool, optional): If modifiable, whether to allow in place editing. Defaults to False.
            addEntryButton (bool, optional): Whether to add a "New Sample" button. Defaults to True.
            addMenu (bool, optional): If allowChoose, whether to add a right click menu to the entries. Defaults to False.
            samplesConfigFileDesc (bool, optional): Whether to show the ids of the configurations, useful for multiple configurations. Defaults to False.
            label (str, optional): The label to use in case allowChoose is False. Defaults to "Sample Name".
            modality (Literal[&quot;image&quot;, &quot;video&quot;], optional): The handled modality. Defaults to "image".
            parent (_type_, optional): The parent layout. Defaults to None.
            windowName (str, optional): The name of the window. Defaults to empty string
        """
        super().__init__(
            parent=parent,
            sampleConfigurationClass=sampleConfigurationClass,
            samplesConfigsFile=samplesConfigsFile,
            allowChoose=allowChoose,
            multiple=multiple,
            addEntryButton=addEntryButton,
            modifiable=modifiable,
            label=label,
            addMenu=addMenu,
            modality=modality,
        )

        self.setWindowTitle(f"{windowName}")
        self.okButtonBox.setDisabled(False)
        self.layout().addLayout(self.okCancelLayout)

    def onCreateNew(self):
        if super().onCreateNew():
            self.close()

    @classmethod
    def fromNonGUI(
        cls,
        configHandler: ConfigHandler,
        windowName: str = "",
        parent=None,
        allowChoose=True,
        multiple=False,
        modifiable=False,
        label="New sample",
    ):
        return ConfigHandlerGUI(
            configHandler.sampleConfigurationClass,
            configHandler.samplesConfigsFile,
            windowName=windowName,
            parent=parent,
            allowChoose=allowChoose,
            multiple=multiple,
            modifiable=modifiable,
            label=label,
        )

    def okCallback(self):
        if not self.multiple or not self.allowChoose:
            item = self.labelBox
            v = item.text()
            if not v:
                msgBox = QMessageBox()
                msgBox.setIcon(QMessageBox.Warning)
                msgBox.setText("The sample name cannot be empty!")
                msgBox.setStandardButtons(QMessageBox.Ok)
                msgBox.exec()
            else:
                self.config = v
        else:
            items = self.labelBox.checkedItems
            if not items:
                msgBox = QMessageBox()
                msgBox.setIcon(QMessageBox.Warning)
                msgBox.setText("At least one of the samples need to be selected!")
                msgBox.setStandardButtons(QMessageBox.Ok)
                msgBox.exec()
            else:
                self.config = items[0]
                self.multipleNames = items
                self.multipleConfigs = [self.samplesConfigs[it] for it in items]
        super().okCallback()
