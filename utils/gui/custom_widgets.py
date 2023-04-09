"""
Contains custom widgets that are based on core ones.
"""
from typing import Dict, List, Tuple, Union, Literal

from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QRect, Qt
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QFrame,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QVBoxLayout,
)
from qtpy.QtWidgets import QStyle
from superqt.sliders import QLabeledRangeSlider

SC_BAR = QStyle.SubControl.SC_ScrollBarSubPage


class Slider(QLabeledRangeSlider):
    savedWheelEvent = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._slider.ovWheelEvent = self._slider.wheelEvent
        self._slider.wheelEvent = self.wheelEvent
        self._slider._offsetAllPositions = self._offsetAllPositions
        self._slider.savedWheelEvent = None

    def wheelEvent(self, e: QtGui.QWheelEvent) -> None:
        self._slider.savedWheelEvent = e
        self._slider.ovWheelEvent(e)
        self._slider.savedWheelEvent = None

    def _offsetAllPositions(self, offset: float, ref=None) -> None:
        self = self._slider
        if ref is None:
            ref = self._position
        if self._bar_is_rigid:
            # NOTE: This assumes monotonically increasing slider positions
            if offset > 0 and ref[-1] + offset > self.maximum():
                offset = self.maximum() - ref[-1]
            elif ref[0] + offset < self.minimum():
                offset = self.minimum() - ref[0]
        if self.savedWheelEvent is not None:
            self._hoverControl, self._hoverIndex = self._getControlAtPos(
                self.savedWheelEvent.pos()
            )
            ref[self._hoverIndex] = ref[self._hoverIndex] + offset
        else:
            ref = [i + offset for i in ref]
        self.setSliderPosition(ref)


class ExtendedListWidget(QListWidget):
    listUpdated = QtCore.pyqtSignal(list)

    def __init__(self, *args, multiple=True, fixedSize=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.multiple = multiple
        self.fixedSize = fixedSize
        self.setProperty("checkedItems", "")
        self.itemChanged.connect(self.onItemChanged)

    def update(self, checkedItems: str = None):
        if not checkedItems:
            checkedItems = self.property("checkedItems")
        else:
            if isinstance(checkedItems, (list, tuple)):
                checkedItems = ",".join(checkedItems)
        if self.multiple:
            for item in self.items:
                if checkedItems and item.text() in checkedItems.split(","):
                    item.setCheckState(Qt.Checked)
                else:
                    item.setCheckState(Qt.Unchecked)

        self.setMinimumWidth(self.sizeHintForColumn(0))
        self.setMinimumHeight(
            self.sizeHintForRow(0) * min(10, self.count()) + 2 * self.frameWidth()
        )
        if self.fixedSize:
            self.setFixedSize(
                self.sizeHintForColumn(0) + 2 * self.frameWidth(),
                self.sizeHintForRow(0) * min(10, self.count()) + 2 * self.frameWidth(),
            )
        # else:
        #     self.setMinimumSize(
        #         self.sizeHintForColumn(0) + 2 * self.frameWidth(),
        #         self.sizeHintForRow(0) * min(10, self.count()) + 2 * self.frameWidth(),
        #     )
        super().update()

    def addItems(self, items: List[str]):
        for item in items:
            item = QListWidgetItem(item)
            self.addItem(item)

        self.update()

    def addChild(self, item: str):
        item = QListWidgetItem(item)
        self.addItem(item)
        self.update()

    def deleteChild(self, item):
        for cnt, it in enumerate(self.items):
            if it.text() == item:
                self.takeItem(cnt)
                self.update()
                return True
        return False

    @property
    def checkedItems(self):
        return [it.text() for it in self.items if it.checkState() == Qt.Checked]

    def onItemChanged(self, item: QListWidgetItem) -> None:
        self.setProperty("checkedItems", ",".join(self.checkedItems))
        self.listUpdated.emit(self.checkedItems)

    @property
    def items(self):
        return [self.item(x) for x in range(self.count())]

    def setCurrentItem(self, index):
        self.setCurrentRow(index)


class ModalitySwitch(QPushButton):
    """Adapted from https://stackoverflow.com/questions/56806987/switch-button-in-pyqt"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setCheckable(True)
        self.setMinimumWidth(66)
        self.setMinimumHeight(22)

    def paintEvent(self, event):
        bg_color = Qt.gray

        radius = 10
        width = 32
        center = self.rect().center()

        painter = QtGui.QPainter(self)
        painter.setRenderHint(QtGui.QPainter.Antialiasing)
        painter.translate(center)
        painter.setBrush(QtGui.QColor(0, 0, 0))

        pen = QtGui.QPen(Qt.black)
        pen.setWidth(2)
        painter.setPen(pen)
        total_rect = QRect(-width, -radius, 2 * width, 2 * radius)
        painter.drawRect(total_rect)
        painter.setBrush(QtGui.QBrush(bg_color))
        sw_rect = QRect(-radius, -radius, width + radius, 2 * radius)
        if not self.isChecked():
            sw_rect.moveLeft(-width)
        painter.drawRect(sw_rect)
        painter.drawText(sw_rect, Qt.AlignCenter, self.label)

    @property
    def label(self):
        return "Video" if self.isChecked() else "Image"

    @property
    def modality(self):
        return "video" if self.isChecked() else "image"

    @modality.setter
    def modality(self, val: Literal["image", "video"]):
        self.setChecked(val == "video")


class ROIBox(QFrame):
    """A widget with two sliders, to define a region of interest"""

    changed = QtCore.pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        labs = ["xRange", "yRange"]
        self.sliders = [
            Slider(
                self,
            )
            for _ in range(2)
        ]
        slidersLayouts = []
        for cellState, s in zip(labs, self.sliders):
            s.setRange(0, 100)
            s.setValue((0, 100))
            s.setOrientation(Qt.Horizontal)
            lab = QLabel("&" + cellState)
            lab.setBuddy(s)
            slidersLayouts.append(QHBoxLayout())
            slidersLayouts[-1].addWidget(lab)
            slidersLayouts[-1].addWidget(s)
            s.valueChanged.connect(self.valueChanged)
            s.setBarMovesAllHandles(False)

        roiSlidersLayout = QVBoxLayout()
        [roiSlidersLayout.addLayout(b) for b in slidersLayouts]
        roiLabel = QLabel("ROI:")
        roiLabel.setToolTip("Region to process")
        roiLayout = QHBoxLayout()
        roiLayout.addWidget(roiLabel)
        roiLayout.addLayout(roiSlidersLayout)

        self.setFrameShape(QFrame.StyledPanel)
        self.setFrameShadow(QFrame.Plain)
        self.setLineWidth(2)
        self.setLayout(roiLayout)

    def valueChanged(self, *args, **kwargs):
        self.changed.emit()

    @property
    def roi(self) -> Union[Tuple[int, int, int, int], None]:
        if self.empty:
            return None
        vs = [s.value() for s in self.sliders]
        return tuple([vs[0][0], vs[1][0], vs[0][1], vs[1][1]])

    @roi.setter
    def roi(self, bbox: Tuple[int, int, int, int]):
        if bbox is None:
            [s.setValue((0, s.maximum())) for s in self.sliders]
        else:
            self.sliders[0].setValue((bbox[0], bbox[2]))
            self.sliders[1].setValue((bbox[1], bbox[3]))

    @property
    def empty(self) -> bool:
        """Returns true if the sliders show to min and max"""
        return all(s.value() == (0, s.maximum()) for s in self.sliders)

    @property
    def default(self) -> Tuple[int, int, int, int]:
        """The default state is when both sliders are set to their maximum range"""
        return (0, 0, self.sliders[0].maximum(), self.sliders[1].maximum())

    def updateDimensions(self, width: int, height: int):
        """Update the ranges, given the supplied width and height. If the range was at default state previously,
        extend it to the new default state.
        """
        extend = self.empty
        self.sliders[0].setRange(0, width)
        self.sliders[1].setRange(0, height)
        if extend:
            self.roi = self.default
        else:
            [
                s.setValue(tuple(min(s.maximum(), x) for x in s.value()))
                for s in self.sliders
            ]


import logging
from utils.logging import updateLoggingHandler


class QTextEditLogger(logging.Handler, QtWidgets.QFrame):
    appendPlainText = QtCore.pyqtSignal(str)

    def __init__(self, parent, logger: logging.Logger):
        super().__init__()
        QtWidgets.QFrame.__init__(self)

        self.widget = QtWidgets.QPlainTextEdit(parent)
        self.widget.setReadOnly(True)
        self.appendPlainText.connect(self.widget.appendHtml)
        logLayout = QHBoxLayout()
        logLayout.addWidget(self.widget)
        self.setLayout(logLayout)
        updateLoggingHandler(self, useGUI=True)
        logger.addHandler(self)

    def emit(self, record):
        msg = self.format(record)
        self.appendPlainText.emit(msg)
