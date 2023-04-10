from configparser import ConfigParser
import os
import logging


CONFIG = ConfigParser()
PROJ_DIR = os.path.dirname(
    (__file__[0].upper() + __file__[1:]) if __file__[0].isalpha() else __file__
)
CONF_PATH = os.path.join(PROJ_DIR, "configuration.ini")
CONFIG.read(CONF_PATH)
CONFIG = {s: dict(CONFIG.items(s)) for s in CONFIG.sections()}


LOGGER = logging.getLogger()
LOGGER.setLevel(CONFIG["GENERAL"]["verbosity"])
