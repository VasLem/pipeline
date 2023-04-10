from copy import deepcopy
from tkinter.tix import Select

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.decomposition import PCA
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.pipeline import FeatureUnion
from sklearn.preprocessing import PolynomialFeatures, StandardScaler
from utils.logging import LOGGER


class LogFeatures(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, features: pd.DataFrame):
        return features.applymap(lambda x: np.log(max(np.abs(x), 0.0001))).rename(
            columns={col: f"log{col[0].upper()}{col[1:]}" for col in features.columns}
        )


class PolynomialFeaturesWithCols(PolynomialFeatures):
    def get_feature_names(self, X):
        names = super().get_feature_names()
        for cn in range(len(names)):
            for cc, col in enumerate(X.columns):
                names[cn] = names[cn].replace(f"x{cc}", col)
        return names

    def fit(self, X, y=None):
        self._input_features_names = X.columns.tolist()
        super().fit(X, y)
        self._features_names = self.get_feature_names(X)
        return self

    def transform(self, X: pd.DataFrame):
        try:
            ret = super().transform(X)
        except ValueError as err:
            if isinstance(X, pd.DataFrame):
                LOGGER.error(f"Provided input columns:\n{X.columns.tolist()}")
            else:
                LOGGER.error(f"Provided input first 10 rows:\n{X[:10,:]}")
            LOGGER.error(f"Expected input columns: {self.self._input_features_names}")
            raise err
        ret = pd.DataFrame(ret, columns=self._features_names, index=X.index)
        return ret


class FeatureUnionWithCols(FeatureUnion):
    def transform(self, X):
        return pd.concat([t[1].transform(X) for t in self.transformer_list], axis=1)

    def fit_transform(self, X, y=None):
        return pd.concat(
            [t[1].fit_transform(X, y) for t in self.transformer_list], axis=1
        )


class PCAWithCols(PCA):
    def fit(self, X, y=None):
        self.scaler = StandardScaler()
        super().fit(self.scaler.fit_transform(X))
        return self

    def transform(self, X):
        ret = super().transform(self.scaler.transform(X))
        ret = pd.DataFrame(
            ret, columns=[f"PC{cnt + 1}" for cnt in range(ret.shape[1])], index=X.index
        )
        return ret


from typing import Iterable


class SelectKBestWithCols(BaseEstimator, TransformerMixin):
    def __init__(
        self, score_func=f_classif, *args, k=10, toKeep: Iterable[str] = tuple()
    ):
        """
        The __init__ function is called when an object of the class is instantiated.
        The __init__ function can take arguments, but self is always the first one.
        This is a reference to the instance being created.

        Args:
            score_func=f_classif: Specify the function used to calculate the statistical scores
            *args: Additional positional arguments to pass to the SelectKBest initializer
            k=10: Specify the number of features to select
            toKeep: the list of names of the features to keep in addition to the selected ones

        Returns:
            The base model and the k value

        Doc Author:
            Trelent
        """

        self.baseModel = SelectKBest(score_func, *args, k="all")
        self.k = k
        self.toKeep = None
        self.scores_ = None
        self.pvalues_ = None
        self.toKeep = toKeep

    def fit(self, X: pd.DataFrame, y):
        if len(y.shape) == 1:
            y = pd.DataFrame(y).T
        models = []
        for cnt in range(y.shape[1]):
            models.append(deepcopy(self.baseModel).fit(X, y.iloc[:, cnt]))
        self.scores_ = pd.Series(
            np.mean([m.scores_ for m in models], axis=0), index=X.columns.tolist()
        )

        self.pvalues_ = pd.Series(
            np.mean([m.pvalues_ for m in models], axis=0), index=X.columns.tolist()
        )
        self.selected = self.pvalues_.sort_values().index.tolist()
        if isinstance(self.k, int):
            self.selected = self.selected[: self.k]
        if self.toKeep:
            self.selected = self.selected + [
                x for x in X.columns if x in self.toKeep and x not in self.selected
            ]
        return self

    def transform(self, X: pd.DataFrame):
        return X.loc[:, self.selected]
