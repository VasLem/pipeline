from config import CONFIG
import os
from utils.logging import LOGGER
CONFIG["PIPELINE"]["cache_dir"] = os.environ.get(
    "WORKDIR",
    os.path.join(CONFIG["PIPELINE"]["root_dir"], CONFIG["PIPELINE"]["cache_dir"]),
)
CONFIG["PIPELINE"]["reports_dir"] = os.path.join(
    CONFIG["PIPELINE"]["root_dir"], CONFIG["PIPELINE"]["reports_dir"]
)
LOGGER.debug(
    "Persistent pipeline configuration: \n"
    + "\n".join([f"{k}: {v}" for k, v in CONFIG["PIPELINE"].items()])
)
PIPELINE_CONFIG = CONFIG["PIPELINE"]
