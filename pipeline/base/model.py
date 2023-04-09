"""
Contains the structure to be inherited when calling the machine learning models.
"""
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.preprocessing import OneHotEncoder
from typing import Tuple
from pipeline.common import ModelInputData


class Model(BaseEstimator, ClassifierMixin):
    def __init__(self):
        self.labelEncoder = None
        self.model = None

    def preFit(
        self, X: ModelInputData, y: np.ndarray
    ) -> Tuple[ModelInputData, np.ndarray]:
        """Preprocesses input data, by transforming the labels into one hot encodings

        Args:
            X (ModelInputData): the input featuress
            y (np.ndarray): the input labels

        Returns:
            Tuple[ModelInputData, np.ndarray]: the processed input data
        """
        self.featuresNames = X.features.columns.tolist()
        self.labelEncoder = OneHotEncoder(drop="if_binary", sparse=False).fit(y)
        y = self.labelEncoder.transform(y)
        if len(y.shape) == 1:
            y = y.reshape(-1, 1)
        self.distinctLabels = np.unique(y, axis=0)
        if len(self.distinctLabels.shape) == 1:
            self.distinctLabels = self.distinctLabels.reshape(-1, 1)
        from scipy import spatial

        self.kdTree = spatial.KDTree(self.distinctLabels)
        return X, y

    def postPredict(self, preds: np.ndarray) -> np.ndarray:
        """After the model returns the predictions, they are mapped to the corresponding labels

        Args:
            preds (pd.DataFrame): the predictions

        Returns:
            np.ndarray: _description_
        """
        preds = [self.distinctLabels[self.kdTree.query(p)[1]] for p in preds]
        return self.labelEncoder.inverse_transform(preds)

