import os
from typing import TYPE_CHECKING, Dict, List, Literal, Union, Tuple
import shutil
import cv2
import numpy as np
import pandas as pd
import pymongo
import regex
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm
try:
    import torch # type: ignore
except ImportError:
    from types import new_class
    torch = object
    torch.Tensor = new_class("torch.Tensor")
from pipeline.base.exceptions import PipelineHalted
from pipeline.base.writer import Writer
from utils.html_table import makeHtmlTable
from utils.video import MP4Writer
from utils.visualization import preprocess
from typing import Optional, Any

if TYPE_CHECKING:
    from pipeline.base.block import Block
    from pipeline.base.pipeline import Pipeline


BASE_DIR = os.path.dirname(
    (__file__[0].upper() + __file__[1:]) if __file__[0].isalpha() else __file__
)
JS_DIR = os.path.join(BASE_DIR, "js")
CSS_DIR = os.path.join(BASE_DIR, "css")

# style to create the effect of collapsible blocks in the html report
COLLAPSIBLE_CSS = open(os.path.join(CSS_DIR, "collapsible.css"), "r").read()
# The javascript required to make the collapsible effect
COLLAPSIBLE_JS = open(os.path.join(JS_DIR, "collapsible.js"), "r").read()
# Style required to make a canvas where the user can zoom in and move the picture, or create a caroussel of images, where left and right arrow are going to change the displayed image.
INTERACTIVE_CANVAS_CSS = open(
    os.path.join(CSS_DIR, "interactive_canvas.css"), "r"
).read()
# Javascript required to be put before the body, to define the function imshow(id_, key, src),
# where id_ is the id of the canvas, which must be unique, the key is the name to be used, or a comma separated list of names to be used for the caroussel case
# and the src is the source to the image, or a comma separated list of sourceds to be used for the caroussel case
PRECANVAS_JS = open(os.path.join(JS_DIR, "precanvas.js"), "r").read()
# Javascript required to be put after the body, to connect the defined canvases to the caroussel callbacks.
POSTCANVAS_JS = open(os.path.join(JS_DIR, "postcanvas.js"), "r").read()
RESIZABLE_IFRAME_CSS = open(os.path.join(CSS_DIR, "resizable_iframe.css"), "r").read()

RESIZABLE_IFRAME_JS = open(os.path.join(JS_DIR, "resizable_iframe.js"), "r").read()
VIDEO_CSS = open(os.path.join(CSS_DIR, "video.css"), "r").read()


def makeId(inp: str) -> str:
    """Helper function to use to normalize the ids for each of the HTML elements.
    Args:
        inp (str): the id to normalize
    """
    return "".join([x for x in inp if x.isalnum()])


from utils.logging import LOGGER


class UnreadableVideo(IOError):
    pass


from urllib.request import pathname2url


def getVideoTag(report: BeautifulSoup, path: str, relPath: str) -> Tag:
    """Creates a tag that contains the video with controls, centered.

    Args:
        report (BeautifulSoup): the BeautifulSoup document
        path (str): the path to the video
    """
    furi = pathname2url(relPath)
    vid = cv2.VideoCapture(path)
    height = vid.get(cv2.CAP_PROP_FRAME_HEIGHT)
    width = vid.get(cv2.CAP_PROP_FRAME_WIDTH)
    vid.release()
    if width == 0:
        raise UnreadableVideo
    ratio = height / width
    videoWidth = min(400, width)
    videoHeight = videoWidth * ratio
    videoDiv = report.new_tag("div", attrs={"class": "video-wrapper"})
    video = report.new_tag(
        "video",
        attrs={
            "width": str(videoWidth),
            "height": str(videoHeight),
            "controls": None,
            "loop": None,
        },
    )
    source = report.new_tag("source", attrs={"src": furi, "type": "video/mp4"})
    video.append(source)
    video.append("Your browser does not support the video format.")
    videoDiv.append(video)
    return videoDiv


CONNECTION_URL = "mongodb://localhost:27017"
CERTIFICATE_PATH = None

def convertDictToTable(report: BeautifulSoup, d: Dict[str, str]) -> Tag:
    table = report.new_tag("table")
    for k, v in d.items():
        if not v:
            continue
        row = report.new_tag("tr")
        kcol = report.new_tag(
            "td", align="left", style="font-size: 12px; font-weight: bold"
        )
        vcol = report.new_tag("td", style="font-size: 10px")
        kcol.append(k)
        vcol.append(str(v))
        row.append(kcol)
        row.append(vcol)
        table.append(row)
    tableDiv = report.new_tag("div")
    tableDiv.append(table)
    return tableDiv


class Reporter(Writer):
    client = pymongo.MongoClient(
        CONNECTION_URL,
        **(
            dict(tls=True, tlsCertificateKeyFile=CERTIFICATE_PATH)
            if CERTIFICATE_PATH is not None
            else dict()
        ),
    )

    SUPPORTED_TYPES = ["figure", "multiFigure", "excel", "video", "binary"]

    def __init__(
        self,
        parent: Union["Block", "Pipeline"],
        dbName: str = "reports",
        reportsDir: str = "reports",
    ):
        """A helper class to write supplied files to the disc, record their metadata in a report database and
        create a html report upon request.

        Args:
            parent (Block, optional): the parent block. Defaults to None.
        """
        self.parent = parent
        self.maindb = self.client[dbName]
        self.reportsDir = reportsDir
        self.content = self.maindb["content"]
        self._counter = None
        self._samplesNames = None
        self._level = None
        self.accessed = []

    def write(
        self,
        desc: str,
        path: Union[str, List[str]],
        data: Optional[
            Union[pd.DataFrame, List[np.ndarray], np.ndarray, torch.Tensor]
        ] = None,
        level: Literal["debug", "info"] = "info",
        **kwargs,
    ):
        """Writes the data into a the provided file path, and inserts an entry in the reports database,
        using the provided description. Accepted data types are: image, excel and video.

        The images can be given as a batch, in case they refer to the same description. In that case, the path
        should also be provided as a list of paths to be used to save them. In the final report, the batch will be converted
        into an image caroussel. The names on the files will be used as titles for each of the images.

        Additional keyword arguments to be provided given the file type:
            image:
                autoContrast (bool): whether to apply autoContrast while creating the report, defaults to False.
            tabular data:
                sheetName (str): the name of the sheet to write in, defaults to "Sheet1".
                keyword arguments accepted by to_excel pandas action.
            images:
                imsTitles (List[str]): the name of each of the images.

        The following keyword arguments can be provided to *override* the saving id:
            stepName (str): the step name
            runConfigID (str): the current RunningConfiguration ID
            instID (str): the instance ID
        Args:
            desc (str): The description of the file, human readable and concise
            path (Union[str, List[str]]): The path where to save the file. In case it is a video, it is assumed that it has already been saved at that path.
            data (Union[pd.DataFrame, List[np.ndarray], np.ndarray], optional): The data to save, if it is an image or an excel sheet. Defaults to None.
            level (Literal['debug','info']): Which verbosity level the entry has, defaults to 'info'.
        """
        try:
            super().write(path=path, data=data, desc=desc, level=level, **kwargs)
        except ValueError as err:
            if "Data type not understood" not in err.args[0]:
                raise
            else:
                if not isinstance(path, str):
                    raise
                self._mp4Write(path, desc, level=level, **kwargs)

    @property
    def idFields(self) -> tuple:
        return ("stepName", "runConfigID", "instID")

    @property
    def id(self) -> dict:
        """The identifier of the entry, including the instID. To be used during saving."""
        return {
            k: v
            for k, v in {
                "stepName": self.parent.compositeName,
                "runConfigID": self.parent.runConfigId
                if self.parent.runConfig is not None
                else "",
                "instID": self.parent.instID,
            }.items()
            if v is not None
        }

    @property
    def idWithoutInstID(self) -> dict:
        """The identifier of the entry, without the instID. To be used during loading."""
        return {
            k: v
            for k, v in {
                "stepName": self.parent.compositeName,
                "runConfigID": self.parent.runConfigId
                if self.parent.runConfig is not None
                else "",
            }.items()
            if v is not None
        }

    @property
    def counter(self) -> int:
        """The number of the existing entries in the database, given the id"""
        if self._counter is None:
            self._counter = self.content.count_documents(self.id)
        return self._counter

    @counter.setter
    def counter(self, val: int):
        self._counter = val

    def writer(func):
        """Wrapper that increments the counter upon call"""

        def wrapper(self: "Reporter", *args, **kwargs):
            self.counter += 1
            return func(self, *args, **kwargs)

        return wrapper

    def deleteContentEntry(self, **kwargs):
        self.content.delete_many(
            {
                **self.id,
                **kwargs,
            }
        )

    @writer
    def _binWrite(
        self,
        path: str,
        data: Any,
        desc: str,
        level: Literal["info", "debug"] = "info",
        **overrides,
    ):

        assert all(
            k in self.idFields for k in overrides
        ), f"Extra key arguments not part of the id: {overrides}"
        if not super()._binWrite(path, data):
            return

        self.deleteContentEntry(key=desc, **overrides)
        self.content.insert_one(
            {
                **{**self.id, **overrides},
                "key": desc,
                "type": "binary",
                "level": level,
                "content": path,
                "index": self.counter,
            }
        )

    @writer
    def _imWrite(
        self,
        path: str,
        im: np.ndarray,
        desc: str,
        level: Literal["info", "debug"] = "info",
        autoContrast=False,
        **overrides,
    ):

        assert all(
            k in self.idFields for k in overrides
        ), f"Extra key arguments not part of the id: {overrides}"
        if not super()._imWrite(path, im):
            return

        self.deleteContentEntry(key=desc, **overrides)
        self.content.insert_one(
            {
                **{**self.id, **overrides},
                "key": desc,
                "type": "figure",
                "content": path,
                "level": level,
                "index": self.counter,
                "meta": dict(autoContrast=autoContrast),
            }
        )

    @writer
    def _imsWrite(
        self,
        paths: List[str],
        ims: List[np.ndarray],
        desc: str,
        level: Literal["info", "debug"] = "info",
        autoContrast=False,
        imsTitles=None,
        **overrides,
    ):
        assert all(
            k in self.idFields for k in overrides
        ), f"Extra key arguments not part of the id: {overrides}"
        saved = super()._imsWrite(paths, ims)
        if not saved:
            return
        if imsTitles:
            imsTitles = [p for p, r in zip(imsTitles, paths) if r in saved]
        self.deleteContentEntry(key=desc, **overrides)
        self.content.insert_one(
            {
                **{**self.id, **overrides},
                "key": desc,
                "type": "multiFigure",
                "content": paths,
                "level": level,
                "index": self.counter,
                "meta": dict(autoContrast=autoContrast, imsTitles=imsTitles),
            }
        )

    @writer
    def _pdWrite(
        self,
        path,
        dframe,
        desc: str,
        level: Literal["info", "debug"] = "info",
        sheetName="Sheet1",
        **kwargs,
    ):
        overrides = {k: v for k, v in kwargs.items() if k in self.idFields}
        kwargs = {k: v for k, v in kwargs.items() if k not in overrides}
        super()._pdWrite(path=path, dframe=dframe, sheetName=sheetName, **kwargs)
        self.deleteContentEntry(key=desc, meta=dict(sheetName=sheetName), **overrides)
        self.content.insert_one(
            {
                **{**self.id, **overrides},
                "key": desc,
                "type": "excel",
                "content": path,
                "level": level,
                "index": self.counter,
                "meta": dict(sheetName=sheetName),
            }
        )

    @writer
    def _mp4Write(
        self,
        path: str,
        desc: str,
        level: Literal["info", "debug"] = "info",
        **overrides,
    ):

        assert all(
            k in self.idFields for k in overrides
        ), f"Extra key arguments not part of the id: {overrides}"
        assert path.endswith(".mp4")
        self.deleteContentEntry(key=desc, **overrides)
        self.content.insert_one(
            {
                **{**self.id, **overrides},
                "key": desc,
                "type": "video",
                "content": path,
                "level": level,
                "index": self.counter,
                "meta": dict(),
            }
        )

    @property
    def reportPath(self) -> str:
        """The path where the report is saved."""
        if self.parent.runConfig is None:
            raise ValueError("runConfig not set in parent")
        runConfig = self.parent.runConfig.toDict()
        if self._samplesNames:
            direc = os.path.join(
                self.reportsDir,
                str(
                    self.parent.name
                    + self.parent.hasher.compute(
                        " ".join([x for x in self._samplesNames if x is not None])
                    )[:10]
                    + self.parent.hasher.compute(
                        str([(k, runConfig[k]) for k in sorted(runConfig)])
                    )[:10]
                ),
            )
        else:
            direc = os.path.join(
                self.reportsDir,
                str(
                    self.parent.name
                    + self.parent.hasher.compute(
                        str([(k, runConfig[k]) for k in sorted(runConfig)])
                    )[:20]
                ),
            )
        if self._level == "debug":
            direc += "_" + self._level
        os.makedirs(direc, exist_ok=True)
        return os.path.join(direc, f"report.html")

    def createReport(
        self, samplesNames: List[str], level: Literal["info", "debug"] = "info"
    ) -> BeautifulSoup:
        ret = self._createReport(samplesNames, level=level)  # type: ignore
        if ret is None:
            raise PipelineHalted("Failed to create report.")
        return ret

    def _createReport(
        self,
        samplesNames: List[Optional[str]] = [],
        level: Literal["info", "debug"] = "info",
        _entryPoint=True,
        _video: bool = True,
        _excel: bool = True,
        _figure: bool = True,
        _multiFigure: bool = True,
        _data: Optional[list] = None,
        _samplesBodies: list = [],
        _reportDir: str = "",
    ) -> Optional[BeautifulSoup]:
        """Recursively access all the children of the parent, and construct the report, given the data contained in the database.

        Returns:
            BeautifulSoup: The final html, which is also saved in the `reportPath`.
        """
        self._level = level
        if self.parent.runConfig is None:
            raise ValueError("runConfig not set in parent")
        self._samplesNames = samplesNames
        report = BeautifulSoup()

        def addCollapsible(
            body, instance: str, type: Literal["sample", "step", "instance"]
        ):

            _id = makeId(
                "div"
                + str(
                    self.parent.hasher.compute(
                        f"div_{instance}_{self.parent.compositeName}"
                    )[:20]
                )
            )
            button = report.new_tag(
                "button",
                attrs={"type": "button", "class": f"collapsible {type}", "id": _id},
            )
            div = report.new_tag(
                "div",
                attrs={
                    "class": f"content {type}",
                    "id": _id,
                },
            )
            button.append(instance)
            body.append(button)
            body.append(div)
            return div

        self._fileCnt = 0
        if _entryPoint:
            reportDir = os.path.dirname(self.reportPath)
            html = report.new_tag("html")
            report.append(html)
            style = report.new_tag("style")
            style.append(COLLAPSIBLE_CSS)
            style.append(INTERACTIVE_CANVAS_CSS)
            style.append(RESIZABLE_IFRAME_CSS)
            style.append(VIDEO_CSS)
            html.append(style)
            jQuery = report.new_tag(
                "script",
                src="https://code.jquery.com/jquery-3.6.1.min.js",
            )
            html.append(jQuery)
            iFrameJQuery = report.new_tag(
                "script",
                src="http://ajax.googleapis.com/ajax/libs/jqueryui/1.8/jquery-ui.min.js",
            )
            html.append(iFrameJQuery)

            iFrameStyleSheet = report.new_tag(
                "link",
                rel="stylesheet",
                type="text/css",
                href="http://ajax.googleapis.com/ajax/libs/jqueryui/1.8/themes/start/jquery-ui.css",
            )
            html.append(iFrameStyleSheet)

            script = report.new_tag("script")
            script.append(PRECANVAS_JS)

            html.append(script)
            body = report.new_tag(
                "body",
                attrs={
                    "id": makeId(
                        f"body_{self.parent.hasher.compute(self.parent.compositeName)[:20]}"
                    )
                },
            )
            html.append(body)
            # Add graph on top
            try:
                graph, _ = self.parent.makeGraph(shortened=True)  # type: ignore
            except:
                graph, _ = self.parent.makeGraph()
            if graph is None:
                raise ValueError("Graph not created")
            graph = self.parent.graphToDot(graph)

            graph.draw(os.path.join(reportDir, "pipeline.svg"), prog="dot")
            graphTag = report.new_tag(
                "object",
                type="image/svg+xml",
                data="pipeline.svg",
                style="width: 100%;",
            )
            body.append(graphTag)

            # Add runConfig subsequently

            body.append(convertDictToTable(report, self.parent.runConfig.toDict()))

            samplesBodies: List[Tag] = []
            data = list(
                self.content.find(
                    {
                        **self.idWithoutInstID,
                        **{
                            "instID": {
                                "$regex": "^("  # match to the beginning of the string
                                + "|".join(
                                    regex.escape(s)
                                    for s in samplesNames
                                    if s is not None
                                )
                                + ")"
                            }
                        },
                        **{
                            "level": {
                                "$regex": ("info" if level == "info" else "info|debug")
                            }
                        },
                        **{
                            "stepName": {"$regex": self.parent.name},
                        },
                    }
                )
            )
            ret: Tuple[List[Optional[str]], List[Tag]] = tuple()
            if data:  # The data is sample/instance specific
                ret = tuple(  # type: ignore
                    map(
                        list,
                        zip(
                            *[
                                (sampleName, addCollapsible(body, sampleName, "sample"))
                                for sampleName in samplesNames
                                if (
                                    sampleName is not None
                                    and any(
                                        elem["instID"].startswith(sampleName)
                                        and (  # do not make collapsibles for time series that only have figures, as they will be merged into videos.
                                            not self.parent.runConfig.onTimeSeries
                                            or (elem["type"] != "figure")
                                        )
                                        for elem in data
                                    )
                                )
                            ]
                        ),
                    )
                )
                # Make a collapsible for videos
                if self.parent.runConfig.onTimeSeries and any(
                    elem["type"] == "figure" for elem in data
                ):
                    if ret:  # There might not be any non-figure data
                        ret[0].insert(0, None)
                        ret[1].insert(0, addCollapsible(body, "TimeSeries", "sample"))
                    else:
                        ret = [None], [addCollapsible(body, "TimeSeries", "sample")]
            else:
                data = list(
                    self.content.find(
                        {
                            **self.idWithoutInstID,
                            **{
                                "instID": None,
                                "stepName": {"$regex": self.parent.name},
                            },
                            **{
                                "level": {
                                    "$regex": (
                                        "info" if level == "info" else "info|debug"
                                    )
                                }
                            },
                        }
                    )
                )
                if not data:
                    return None
                for d in data:
                    d["instID"] = ""
                ret = [None], [addCollapsible(body, "Dataset", "sample")]

            if not ret:
                if _entryPoint:
                    raise PipelineHalted(
                        "No report could be created using the provided samples and running configuration"
                    )
                return None

            samplesNames, samplesBodies = ret

        else:
            data = _data
            samplesBodies = _samplesBodies

            reportDir = _reportDir

        def updateReportData(
            reportDir: str,
            instID: str,
            parent: Tag,
            elems: List[dict],
            figsToVideo=False,
        ):
            """for each instance ID, append the collection of media to the html.

            Args:
                reportDir (str): the directory where the report html resides.
                instID (str): the instance
                parent (Tag): the position to place the media
                elems (List[dict]): the elements found for the specific instance ID.
                figsToVideo (bool): if true, gets all elements marked as figures with same key and converts them into a video.
            """

            import re

            relStepDir = os.path.join(
                "src", f"{self.parent.hasher.compute(self.parent.compositeName)[:20]}"
            )
            if not elems and not self.parent.runConfig.onTimeSeries:
                relStepDir = os.path.join(
                    relStepDir, f"{instID if instID else 'noInstID'}"
                )

            for elem in tqdm(elems) if len(elems) > 1 else elems:
                _id = makeId(
                    f"script_{self.parent.hasher.compute(parent.attrs['id'])[:20]}_{self._fileCnt}"
                )
                if (
                    _figure
                    and (elem["type"] == "figure")
                    and os.path.isfile(elem["content"])
                ):
                    if not self.parent.runConfig.onTimeSeries:
                        fname = f"{self._fileCnt}_{os.path.splitext(os.path.basename(elem['content']))[0]}"
                    else:
                        fname = f"{self._fileCnt}_{elem['key']}"
                    relPath = os.path.join(relStepDir, fname)
                    fPath = os.path.join(reportDir, relPath)
                    os.makedirs(os.path.dirname(fPath), exist_ok=True)
                    if not figsToVideo:
                        fPath += ".jpg"
                        relPath += ".jpg"
                        name = elem["key"]
                        fnameuri = pathname2url(relPath)
                        autoContrast = elem["meta"]["autoContrast"]
                        autoContrast = False  # @TODO: remove this

                        if not autoContrast:
                            shutil.copy(elem["content"], fPath)
                        else:
                            cv2.imwrite(
                                fPath,
                                preprocess(
                                    elem["content"],
                                    autoContrast=elem["meta"]["autoContrast"],
                                ),
                            )

                        canvas = report.new_tag("script")
                        parent.append(report.new_tag("p", attrs={"id": _id}))
                        canvas.append(f"imshow('{_id}', '{name}', '{fnameuri}');")
                        parent.append(canvas)
                    else:
                        fPath += ".mp4"
                        if elem["key"] in self.accessed:
                            continue
                        with MP4Writer(fPath, 10) as writer:
                            [
                                writer.write(
                                    preprocess(
                                        el["content"],
                                        autoContrast=False,  # el["meta"]["autoContrast"],  #@TODO put this back
                                    )
                                )
                                for el in sorted(elems, key=lambda x: x["content"])
                                if el["key"] == elem["key"]
                            ]
                        name = elem["key"]
                        div = report.new_tag(
                            "div",
                            attrs={"text-align": "center", "width": "100%", "id": _id},
                        )

                        a = report.new_tag(
                            "a",
                            attrs={
                                "href": relPath + ".mp4",
                                "download": name + ".mp4",
                                "class": "description",
                                "id": _id,
                            },
                        )

                        a.append(name)
                        div.append(a)
                        parent.append(div)
                        try:
                            parent.append(getVideoTag(report, fPath, relPath + ".mp4"))
                        except UnreadableVideo:
                            LOGGER.debug(f"Video {fPath} could not be read, skipping..")
                        self.accessed.append(elem["key"])
                    self._fileCnt += 1

                if (
                    _multiFigure
                    and (elem["type"] == "multiFigure")
                    and any(os.path.isfile(x) for x in elem["content"])
                ):

                    relDir = os.path.join(
                        relStepDir,
                        f"{self._fileCnt}{elem['key']}",
                    )
                    fdir = os.path.join(reportDir, relDir)
                    div = report.new_tag(
                        "div",
                        attrs={"text-align": "center", "width": "100%", "id": _id},
                    )
                    a = report.new_tag(
                        "a",
                        attrs={
                            "href": relDir + ".zip",
                            "download": elem["key"] + ".zip",
                            "class": "multifig description",
                        },
                    )
                    a.append(elem["key"])
                    div.append(a)
                    parent.append(div)
                    os.makedirs(fdir, exist_ok=True)
                    zeros = len(str(len(elem["content"])))
                    names = (
                        [
                            str(cnt).zfill(zeros)
                            + os.path.basename(os.path.splitext(k)[0])
                            for cnt, k in enumerate(elem["content"])
                        ]
                        if not elem["meta"].get("imsTitles", None)
                        else elem["meta"]["imsTitles"]
                    )

                    fnames = [os.path.join(fdir, key) + ".jpg" for key in names]

                    relFnames = [os.path.join(relDir, key) + ".jpg" for key in names]
                    failed = []
                    for fPath, path in zip(fnames, elem["content"]):
                        if not not os.path.isfile(path):
                            continue
                        autoContrast = elem["meta"]["autoContrast"]
                        autoContrast = False  # @TODO remove this
                        try:
                            if not autoContrast:
                                shutil.copy(path, fPath)
                            else:
                                cv2.imwrite(
                                    fPath,
                                    preprocess(
                                        path,
                                        True,
                                    ),
                                )

                        except:
                            failed.append(fPath)
                    names = [n for f, n in zip(fnames, names) if f not in failed]
                    relfnames = [
                        r for f, r in zip(fnames, relFnames) if f not in failed
                    ]
                    fnames = [f for f in fnames if f not in failed]

                    canvas = report.new_tag("script")
                    furis = [pathname2url(fPath) for fPath in relfnames]
                    names = ",".join(names)
                    furis = ",".join(furis)
                    canvas.append(f"imshow('{_id}', '{names}', '{furis}');")
                    parent.append(report.new_tag("p", attrs={"id": _id}))
                    parent.append(canvas)
                    self._fileCnt += 1
                if (
                    "video"
                    and (elem["type"] == "video")
                    and os.path.isfile(elem["content"])
                ):
                    name = elem["key"]

                    fname = f"{self._fileCnt}_{os.path.splitext(os.path.basename(elem['content']))[0]}.mp4"
                    relPath = os.path.join(relStepDir, fname)
                    div = report.new_tag(
                        "div",
                        attrs={"text-align": "center", "width": "100%", "id": _id},
                    )
                    a = report.new_tag(
                        "a",
                        attrs={
                            "href": relPath + ".mp4",
                            "download": name + ".mp4",
                            "class": "description",
                        },
                    )
                    a.append(name)
                    div.append(a)
                    parent.append(div)

                    fPath = os.path.join(reportDir, relPath)
                    os.makedirs(os.path.dirname(fPath), exist_ok=True)

                    shutil.copyfile(elem["content"], fPath)
                    parent.append(getVideoTag(report, fPath, relPath))
                    self._fileCnt += 1

                if _excel and (
                    (elem["type"] == "excel") and (os.path.isfile(elem["content"]))
                ):

                    relPath = os.path.join(
                        relStepDir,
                        f"{self._fileCnt}_{elem['key']}{elem['meta']['sheetName']}.html",
                    )
                    fPath = os.path.join(reportDir, relPath)
                    os.makedirs(os.path.dirname(fPath), exist_ok=True)
                    name = elem["key"]
                    div = report.new_tag(
                        "div",
                        attrs={"text-align": "center", "width": "100%", "id": _id},
                    )
                    a = report.new_tag(
                        "a",
                        attrs={
                            "href": os.path.splitext(relPath)[0] + ".xlsx",
                            "download": name + ".xlsx",
                            "class": "description",
                        },
                    )
                    if "sheetName" in elem["meta"]:
                        a.append(name + ":" + elem["meta"]["sheetName"])
                    else:
                        a.append(name)
                    div.append(a)
                    parent.append(div)

                    shutil.copyfile(
                        elem["content"], os.path.splitext(fPath)[0] + ".xlsx"
                    )

                    div.append(
                        makeHtmlTable(
                            report,
                            elem["content"],
                            fPath,
                            relPath,
                            sheetName=elem["meta"]["sheetName"]
                            if "sheetName" in elem["meta"]
                            else None,
                        )
                    )

        iterator = (
            (samplesNames, samplesBodies) if data else []
        )  # skip if no data has been previously found
        # For time series, the samples are assumed to be consecutive time series, so all the figures are merged into videos.
        for item in zip(*iterator):
            # The database to be queried multiple times, because the runConfig might be different from step to step
            # In case of a time series, we also need to get the elements that cannot be converted to videos.
            (sampleName, sampleBody) = item
            elems = list(
                self.content.find(
                    {
                        **self.idWithoutInstID,
                        **{"stepName": self.parent.compositeName},
                        **(
                            {"instID": {"$regex": f"^{sampleName}"}}
                            if sampleName is not None
                            else {}
                        ),
                        **{
                            "level": {
                                "$regex": ("info" if level == "info" else "info|debug")
                            }
                        },
                        **(  # Making sure figures will be processed only once for time series. (The first sampleName is None by construction)
                            (
                                {
                                    "type": {
                                        "$regex": "|".join(
                                            t
                                            for t in self.SUPPORTED_TYPES
                                            if t != "figure"
                                        )
                                    }
                                }
                                if sampleName is not None
                                else {"type": "figure"}
                            )
                            if self.parent.runConfig.onTimeSeries
                            else {}
                        ),
                    }
                )
            )
            for d in elems:
                if "instID" not in d:
                    d["instID"] = None
            elems = sorted(elems, key=lambda d: str(d["instID"]) + str(d["index"]))

            if _entryPoint:
                if sampleName:
                    try:
                        sampleConfig = loader.samplesConfigs[sampleName]  # type: ignore
                        sampleBody.append(
                            convertDictToTable(report, sampleConfig.toDict())
                        )
                        LOGGER.debug(f"Processing sample {sampleName}")
                    except KeyError:
                        LOGGER.warning(
                            f"Sample {sampleName} not found in sample configurations, not adding its description"
                        )
                        pass
            if elems:
                LOGGER.debug(f"Processing step: {self.parent.compositeName}")

                stepBody = addCollapsible(sampleBody, self.parent.compositeName, "step")

                instanceContainers = {}
                if self.parent.runConfig.onTimeSeries:
                    import re

                    # process figures
                    figData = [
                        elem
                        for elem in elems
                        if (elem["type"] == "figure")
                        and elem["instID"]
                        and re.findall(r"frame\d+(.*)", elem["instID"])
                    ]
                    videos = {}
                    for el in figData:
                        f = re.findall(r"frame\d+(.*)", el["instID"])[0]
                        f = el["key"] + f
                        if f not in videos:
                            videos[f] = []
                        videos[f].append(el)
                    for video, flagElems in videos.items():
                        LOGGER.debug(f"Processing video: {video}")
                        if video not in instanceContainers:
                            instanceContainers[video] = addCollapsible(
                                stepBody, video, "instance"
                            )
                        updateReportData(
                            reportDir,
                            video,
                            instanceContainers[video],
                            flagElems,
                            figsToVideo=True,
                        )
                    elems = [elem for elem in elems if elem not in figData]
                if (
                    not elems  # for time series, this means that only figure data exists
                ):
                    continue

                # process non figures, or non time series
                instIDs = sorted(set(d["instID"] for d in elems))
                for instID in tqdm(instIDs) if len(instIDs) > 1 else instIDs:
                    if instID:
                        LOGGER.debug(f"Processing instID {sampleName}..")
                    instElems = [elem for elem in elems if (elem["instID"] == instID)]
                    if instID != sampleName:
                        if instID not in instanceContainers:
                            instanceContainers[instID] = addCollapsible(
                                stepBody, instID, "instance"
                            )
                        updateReportData(
                            reportDir, instID, instanceContainers[instID], instElems
                        )
                    else:
                        updateReportData(reportDir, instID, stepBody, instElems)
        if hasattr(self.parent, "namedSteps"):
            for step in self.parent.namedSteps.values():  # type: ignore
                if hasattr(step, "reporter"):
                    t = step.reporter._createReport(  # type: ignore
                        samplesNames,
                        _entryPoint=False,
                        _video=_video,
                        _excel=_excel,
                        _figure=_figure,
                        _multiFigure=_multiFigure,
                        _data=data,
                        _samplesBodies=samplesBodies,
                        _reportDir=reportDir,
                    )
                    if t is not None:
                        sampleBody.append(t)
        if _entryPoint:
            script = report.new_tag("script")
            script.append(COLLAPSIBLE_JS)
            script.append(POSTCANVAS_JS)
            script.append(RESIZABLE_IFRAME_JS)
            report.append(script)
            with open(self.reportPath, "w") as file:
                file.write(str(report))
            import json

            with open(os.path.join(reportDir, "runConfig.json"), "w") as out:
                json.dump(self.parent.runConfig.toDict(), out)

        return report

    def clear(
        self,
        samplesNames=None,
    ):
        """Deletes all content in the database related to the current id"""
        if samplesNames is not None:
            self.content.delete_many(
                {
                    **self.id,
                    **{
                        "instID": {
                            "$regex": "^("
                            + "|".join(regex.escape(s) for s in samplesNames)
                            + ")"
                        }
                    },
                }
            )
        else:
            self.content.delete_many(self.id)
