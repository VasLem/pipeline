# Copied from https://stackoverflow.com/questions/64202927/how-to-save-and-restore-widget-properties-that-is-unique-for-each-instance-of-th
from PyQt5 import QtWidgets, QtCore, QtGui
from utils.logging import LOGGER


def settings_get_all_widgets(parent):
    # Possible fix to the issue:
    # https://stackoverflow.com/questions/64202927/how-to-save-and-restore-widget-properties-that-is-unique-for-each-instance-of-th

    if parent:
        # Find all children inside the given parent that is of type QWidget
        all_widgets = parent.findChildren(QtWidgets.QWidget)
        if parent.isWidgetType():
            # If parent is of type QWidget, add the parent itself to the list
            all_widgets.append(parent)
    else:
        # If no parent is given then get all the widgets from all the PyQt applications
        all_widgets = QtWidgets.qApp.allWidgets()

    return all_widgets


def settings_value_is_valid(val):
    # Originally adapted from:
    # https://stackoverflow.com/a/60028282/4988010
    # https://github.com/eyllanesc/stackoverflow/issues/26#issuecomment-703184281
    if isinstance(val, QtGui.QPixmap):
        return not val.isNull()
    return True


def settings_restore(settings, parent=None):
    # Originally adapted from:
    # https://stackoverflow.com/a/60028282/4988010
    # https://github.com/eyllanesc/stackoverflow/issues/26#issuecomment-703184281

    if not settings:
        return False

    all_widgets = [
        w
        for w in settings_get_all_widgets(parent)
        if w.objectName() and not w.objectName().startswith("qt_")
    ]

    finfo = QtCore.QFileInfo(settings.fileName())
    if not (finfo.exists() and finfo.isFile()):
        return False

    LOGGER.debug("Loading state..")
    for cnt, w in enumerate(all_widgets):
        try:
            # if w.objectName():
            mo = w.metaObject()
            for i in range(mo.propertyCount()):
                prop = mo.property(i)
                name = prop.name()
                last_value = w.property(name)
                key = "{}/{}".format(w.objectName(), name)
                if not settings.contains(key):
                    continue
                val = settings.value(key, type=type(last_value),)
                if (
                    val != last_value
                    and settings_value_is_valid(val)
                    and prop.isValid()
                    and prop.isWritable()
                ):
                    if all(x not in name.lower() for x in ("height", "width")) and all(
                        x != name.lower()
                        for x in (
                            "pos",
                            "geometry",
                            "font",
                            "size",
                            "minimumsize",
                            "maximumsize",
                            "framerect",
                            "iconsize",
                        )
                    ):
                        w.setProperty(name, val)
            for name in w.dynamicPropertyNames():
                name = str(name, "utf-8")
                last_value = w.property(name)
                key = "{}/{}".format(w.objectName(), name)
                if not settings.contains(key):
                    continue
                val = settings.value(key, type=type(last_value))
                w.setProperty(name, val)

        except:
            return False
    return True


def settings_save(settings, parent=None):
    # Originally adapted from:
    # https://stackoverflow.com/a/60028282/4988010
    # https://github.com/eyllanesc/stackoverflow/issues/26#issuecomment-703184281

    if not settings:
        return
    all_widgets = settings_get_all_widgets(parent)
    namedFound = False
    for w in all_widgets:
        if w.objectName() and not w.objectName().startswith("qt_"):
            mo = w.metaObject()
            for i in range(mo.propertyCount()):
                prop = mo.property(i)
                name = prop.name()
                key = "{}/{}".format(w.objectName(), name)
                val = w.property(name)
                if (
                    settings_value_is_valid(val)
                    and prop.isValid()
                    and prop.isWritable()
                ):
                    namedFound = True
                    settings.setValue(key, w.property(name))
            for name in w.dynamicPropertyNames():
                name = str(name, "utf-8")
                namedFound = True
                key = "{}/{}".format(w.objectName(), name)
                settings.setValue(key, w.property(name))

    return namedFound


from PyQt5.QtCore import QSettings
import os


class AppWithState:
    """In order to activate the saving state of a widget, set a name for it,
    using `setObjectName` function.
    """

    def __init__(self, filePath):
        self.uiCachePath = filePath
        os.makedirs(os.path.dirname(filePath), exist_ok=True)
        self.settings = QSettings(filePath, QSettings.IniFormat)

    def saveSettings(self):
        # Write current state to the settings config file
        return settings_save(self.settings, self)

    def loadSettings(self):
        # Load settings config file
        if settings_restore(self.settings, self):
            return True
        if os.path.isfile(self.uiCachePath):
            LOGGER.debug("Restoring settings failed, clearing ui cache..")
            self.clearSettings()
        return False

    def clearSettings(self):
        # Clear the settings config file
        self.settings.clear()

    def closeEvent(self, event):
        LOGGER.debug("Saving state..")
        self.saveSettings()
        event.accept()
