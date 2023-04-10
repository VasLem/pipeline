"""
Contains the infrastructure to build a pipeline.    
"""
__all__ = ["Block", "Aggregator", "Decimator", "Pipeline", "IterativePipeline", "SwitchPipeline"]

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


from pipelineFactory.block import Block
from pipelineFactory.aggregator import Aggregator
from pipelineFactory.decimator import Decimator
from pipelineFactory.pipeline import Pipeline
from pipelineFactory.iterative_pipeline import IterativePipeline
from pipelineFactory.switch_pipeline import SwitchPipeline
