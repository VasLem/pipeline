import numpy as np
import imutils
import cv2


def rotate(im: np.ndarray, angle: float, fill: bool = True, keepDims: bool = True):
    """
    The rotate function rotates an image by a given angle.

    Args:
        im: np.ndarray: Specify the image that is to be rotated
        angle: float: Specify the angle of rotation
        fill: bool: Fill the black pixels with the mean value
        keepDims: bool: Keep the dimensions of the image after rotation, else enlarge to the new content

    Returns:
        The rotated image
    """
    if fill:
        dtype = im.dtype
        im = im.astype(float)
        meanVal = np.mean(im)
        im[im == 0] = -1
    if not keepDims:
        im = imutils.rotate_bound(im, angle)
    else:
        image_center = tuple(np.array(im.shape[1::-1]) / 2)
        rot_mat = cv2.getRotationMatrix2D(image_center, angle, 1.0)
        im = cv2.warpAffine(im, rot_mat, im.shape[1::-1], flags=cv2.INTER_LINEAR)
    if fill:
        im[im == 0] = meanVal
        im[im == -1] = 0
        im = im.astype(dtype)
    return im
