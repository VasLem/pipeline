from typing import TYPE_CHECKING, List, Union

import cv2
import numpy as np
import pandas as pd

from utils.excel import saveDfToSheet

if TYPE_CHECKING:
    from pipeline.base.block import Block

from utils.timing import TimeRegistration
from matplotlib.figure import Figure
import joblib
from utils.path import fixPath
from typing import Any, Optional
try:
    import torch #type: ignore
except ImportError:
    torch = object
    torch.is_tensor = lambda x: False


class Writer(metaclass=TimeRegistration):
    def __init__(self, parent: "Block" = None):
        """A helper class, to be used as a file writing basis of the parent block."""
        self.parent = parent
        self._samplesNames = None

    def write(
        self,
        path: Union[str, List[str]],
        data: Optional[
            Union[pd.DataFrame, List[np.ndarray], np.ndarray, torch.Tensor]
        ] = None,
        **kwargs,
    ):
        """Writes the data into a the provided file path,
        using the provided description. Accepted data types are: image, images and tabular data.

        The images can be given as a batch, in case they refer to the same description. In that case, the path
        should also be provided as a list of paths to be used to save them.

        Additional keyword arguments to be provided given the file type:
            tabular data:
                sheetName (str): the name of the sheet to write in, defaults to "Sheet1".
                keyword arguments accepted by to_excel pandas action.

        Args:
            desc (str): The description of the file, human readable and concise
            path (Union[str, List[str]]): The path where to save the file. In case it is a video, it is assumed that it has already been saved at that path.
            data (Union[pd.DataFrame, List[np.ndarray], np.ndarray], optional): The data to save, if it is an image or an excel sheet. Defaults to None.
        """
        path = fixPath(path)
        if isinstance(path, (tuple, list)):
            if not (
                isinstance(data, (tuple, list, np.ndarray))
                or (isinstance(data, np.ndarray) and (len(data.shape) > 2))
            ):
                raise ValueError(
                    "List of paths supplied, only list of >2D images are expected as input data"
                )

            return self._imsWrite(path, data, **kwargs)
        if path.endswith(".pkl"):
            return self._binWrite(path, data, **kwargs)
        if isinstance(data, np.ndarray):
            return self._imWrite(path, data, **kwargs)
        if torch.is_tensor(data):
            return self._imWrite(path, data.cpu().numpy(), **kwargs)
        if isinstance(data, pd.DataFrame):
            return self._pdWrite(path, data, **kwargs)
        if isinstance(data, Figure):
            fig = data
            fig.tight_layout()
            fig.canvas.draw()
            data = np.frombuffer(fig.canvas.tostring_rgb(), dtype=np.uint8)
            data = data.reshape(fig.canvas.get_width_height()[::-1] + (3,))
            data = cv2.cvtColor(data, cv2.COLOR_RGB2BGR)
            return self._imWrite(path, data, **kwargs)
        raise ValueError("Data type not understood")

    def _binWrite(self, path: str, obj: Any) -> bool:
        if not path.endswith(".pkl"):
            raise ValueError("Path needs to have a .pkl extension")
        return joblib.dump(obj, path)

    def _imWrite(self, path: str, im: np.ndarray) -> bool:
        """Saves the image to the provided path. Supported types are `.jpg`, `.png`, `.bmp` and `.tiff`

        Args:
            path (str): the path to save the image in.
            im (np.ndarray): the image to save

        Returns:
            bool: whether the operation was successful
        """
        ALLOWED_EXTENSIONS = (".jpg", ".png", ".bmp", ".tiff", ".tif")
        if not any(path.endswith(x) for x in ALLOWED_EXTENSIONS):
            raise ValueError(
                f"Provided image path {path} has not an extension in {ALLOWED_EXTENSIONS}"
            )
        return cv2.imwrite(path, im)

    def _imsWrite(
        self,
        paths: List[str],
        ims: List[np.ndarray],
    ) -> List[str]:
        """Saves the images and returns the paths of the images that were saved successfully

        Args:
            paths (List[str]): the paths of the images to save
            ims (List[np.ndarray]): the list of images

        Returns:
            List[str]: the paths of the images that were saved successfully
        """
        if ims is None:
            raise ValueError("Images data not supplied")
        ret = [cv2.imwrite(path, im) for path, im in zip(paths, ims)]
        paths = [p for p, r in zip(paths, ret) if r]
        return paths

    def _pdWrite(self, path: str, dframe: pd.DataFrame, sheetName="Sheet1", **kwargs):
        """Save the dataframe to the supplied path. The path must end with the extension ".xlsx".
        It also accepts extra kwargs to be passed to `to_excel` pandas action.

        Args:
            path (str): the path to save the dataframe in
            dframe (pd.DataFrame): the dataframe
            sheetName (str, optional): the sheet name to use for the excel file. Defaults to "Sheet1".
        """
        if dframe is None:
            raise ValueError("Pandas data not supplied")
        if path.endswith(".xlsx"):
            return saveDfToSheet(dframe, path, sheetName, **kwargs)
        if path.endswith(".csv"):
            return dframe.to_csv(path, **kwargs)
        raise ValueError(
            "Extension for saving pandas data frame not understood (xlsx or csv are supported)"
        )
