import abc
from pathlib import Path
from typing import Dict

import pandas as pd
from sklearn import metrics

from taskchain.task import InMemoryData
from taskchain.task.parameter import AutoParameterObject


class RatingModel(AutoParameterObject, InMemoryData, abc.ABC):

    def train(self, X: pd.DataFrame, y: pd.Series):
        self._train(self._process_features(X), self._process_labels(y))

    def eval(self, X: pd.DataFrame, y: pd.Series) -> Dict:
        predicted = self.predict(X)

        return {
            'RMSE': metrics.mean_squared_error(y, predicted, squared=False),
            'MAE': metrics.mean_absolute_error(y, predicted)
        }

    def predict(self, X: pd.DataFrame) -> pd.Series:
        prediction = self._predict(self._process_features(X))
        return pd.Series(index=X.index, data=prediction)

    def _process_features(self, features: pd.DataFrame):
        return features.values

    def _process_labels(self, labels: pd.Series):
        return labels.values

    @abc.abstractmethod
    def _train(self, X, y):
        pass

    @abc.abstractmethod
    def _predict(self, X):
        pass

    @abc.abstractmethod
    def save(self, directory: Path):
        pass

    @abc.abstractmethod
    def load(self, directory: Path):
        pass
