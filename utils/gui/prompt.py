from PyQt5.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
)
from PyQt5 import QtCore


class OkCancelDialog(QDialog):
    def __init__(self, parent=None, **kwargs):
        super().__init__(parent=parent, **kwargs)
        self._parent = parent
        okButton = QDialogButtonBox.Ok
        self.okButtonBox = QDialogButtonBox(okButton)
        self.okButtonBox.clicked.connect(self.okCallback)

        cancelButton = QDialogButtonBox.Cancel
        self.cancelButtonBox = QDialogButtonBox(cancelButton)

        self.cancelButtonBox.clicked.connect(self.cancelCallback)
        self.okCancelLayout = QHBoxLayout()
        self.okCancelLayout.addWidget(self.okButtonBox, alignment=QtCore.Qt.AlignLeft)
        self.okCancelLayout.addWidget(self.cancelButtonBox, alignment=QtCore.Qt.AlignRight)
        self.cancelled = False
        self.okPressed = False

    def keyPressEvent(self, event):
        if event.key() == QtCore.Qt.Key_Escape:
            self.cancelCallback()
        elif event.key() == QtCore.Qt.Key_Enter:
            self.okCallback()

    def cancelCallback(self):
        self.cancelled = True
        self.close()

    def exitCallback(self):
        self.cancelled = True
        self.close()

    def okCallback(self):
        self.okPressed = True
        self.close()
