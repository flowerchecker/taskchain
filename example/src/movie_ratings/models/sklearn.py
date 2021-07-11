import abc
import pickle
from pathlib import Path

from sklearn.base import BaseEstimator
from sklearn.linear_model import Ridge
from sklearn.svm import SVR

from movie_ratings.models.core import RatingModel


class SklearnRatingModel(RatingModel, abc.ABC):

    def __init__(self, verbose=True):
        self.verbose = verbose
        super().__init__()

        self.model: BaseEstimator = self.get_model()

    def _train(self, X, y):
        self.model.fit(X, y)

    def _predict(self, X):
        return self.model.predict(X)

    def save(self, directory: Path):
        pickle.dump(self.model, self.model_path(directory).open('wb'))

    def load(self, directory: Path):
        self.model = pickle.load(self.model_path(directory).open('rb'))

    def model_path(self, directory) -> Path:
        return directory / 'model.pickle'

    @abc.abstractmethod
    def get_model(self) -> BaseEstimator:
        pass

    @staticmethod
    def ignore_persistence_args():
        # this is redundant, default returned value already contains verbose
        return ['verbose']


class LinearRegressionRatingModel(SklearnRatingModel):

    def __init__(self, normalize=False, regularization=1., solver='auto', verbose=True):
        self.solver = solver
        self.normalize = normalize
        self.regularization = regularization

        super().__init__(verbose)

    def get_model(self) -> BaseEstimator:
        return Ridge(
            normalize=self.normalize,
            alpha=self.regularization,
            solver=self.solver,
        )


class SVMRatingModel(SklearnRatingModel):

    def __init__(self, kernel, regularization=1., degree=3, verbose=True):
        self.kernel = kernel
        self.degree = degree
        self.regularization = regularization

        super().__init__(verbose)

    def get_model(self) -> BaseEstimator:
        return SVR(
            kernel=self.kernel,
            degree=self.degree,
            C=1. / self.regularization,
            verbose=self.verbose,
        )
