import numpy as np
import cv2
from utils.logging import LOGGER


class MP4Writer:
    def __init__(self, path: str, frameRate: float):
        """
        A context manager used to save an MP4 video, with H264 encoding. The MPV4 encoding is not supported by the browser.
        In order to make this work, the library openh264 needs to reside in the project's path.
        Args:
            path: str: Specify the path to which the video will be saved
            frameRate: float: Set the frame rate of the video
        """
        self.path = path
        self.frameRate = frameRate
        self.writer = None
        self.frameShape = None
        self.encoding = cv2.VideoWriter_fourcc(*"H264")
        self._ignore = False

    def __enter__(self):
        self.writer = None
        return self

    def write(self, frame: np.ndarray):
        """
        The write function writes a frame to the video file.

        Args:
            frame: np.ndarray: Specify the frame to be written

        """

        if not isinstance(frame, np.uint8):
            frame = ((frame - frame.min()) / (frame.max() - frame.min()) * 255).astype(
                np.uint8
            )
        if len(frame.shape) == 2:
            frame = cv2.cvtColor(frame, cv2.COLOR_GRAY2BGR)
        if frame.shape[2] == 4:
            LOGGER.debug("Removing alpha channel from frame")
            frame = frame[:, :, :3]
        # OPENH264 are limited with maximal resolution of 3Kx2K pixels. https://github.com/opencv/cvat/issues/4527
        ratio = min(2000 / frame.shape[0], 3000 / frame.shape[1])
        if ratio < 1:
            frame = cv2.resize(frame, None, None, ratio, ratio, cv2.INTER_AREA)
        if not self._ignore and (self.writer is None):
            self.frameShape = frame.shape
            self.writer = cv2.VideoWriter(
                self.path,
                self.encoding,
                self.frameRate,
                self.frameShape[:2][::-1],
                (cv2.VIDEOWRITER_PROP_HW_ACCELERATION, cv2.VIDEO_ACCELERATION_ANY),
            )
            if self.writer is None:
                self._ignore = True
        assert self.frameShape == frame.shape
        if not self._ignore:
            self.writer.write(frame)

    def __exit__(self, *args, **kwargs):
        if self.writer is not None:
            self.writer.release()
