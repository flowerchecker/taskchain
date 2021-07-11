import numpy as np
import pandas as pd

from movie_ratings.tasks.features import Features
from movie_ratings.tasks.movies import Movies
from taskchain.task import ModuleTask
from taskchain.task.parameter import Parameter


class AllX(ModuleTask):

    class Meta:
        input_tasks = [Features]
        parameters = [
            Parameter('user_rating_file', default=None)
        ]

    def run(self, features, user_rating_file) -> pd.DataFrame:
        if user_rating_file is None:
            # no user rating file -> we train on all movies
            return features

        # TODO


class AllY(ModuleTask):

    class Meta:
        input_tasks = [Movies, ]
        parameters = [
            Parameter('user_rating_file', default=None)
        ]

    def run(self, movies, user_rating_file) -> pd.Series:
        if user_rating_file is None:
            # no user rating file -> we train on all movies and use average user rating
            return movies.avg_vote

        # TODO


class TestMoviesMask(ModuleTask):

    class Meta:
        input_tasks = [AllX]
        parameters = [
            Parameter('test_ratio', default=None, dtype=float),
            Parameter('test_count', default=None, dtype=int),
            Parameter('random_state', default=666, dtype=int),
        ]

    def run(self, all_x: pd.DataFrame, test_ratio, test_count, random_state) -> np.ndarray:
        if test_ratio is None and test_count is None:
            raise ValueError('`test_ratio` or `test_value` must be defined')

        if test_ratio is not None:
            test = all_x.sample(frac=test_ratio, random_state=random_state)
        else:
            test = all_x.sample(n=test_count, random_state=random_state)

        selected_movies = test.index.values
        return all_x.index.isin(selected_movies)


class DataSelectionTask(ModuleTask):

    class Meta:
        abstract = True
        train = None

    def run(self):
        mask = self.input_tasks['test_movies_mask'].value
        if self.meta.train:
            mask = ~mask
        return self.input_tasks[0].value.loc[mask]


class TrainX(DataSelectionTask):
    class Meta:
        input_tasks = [AllX, TestMoviesMask]
        train = True
        data_type = pd.DataFrame


class TestX(DataSelectionTask):
    class Meta:
        input_tasks = [AllX, TestMoviesMask]
        train = False
        data_type = pd.DataFrame


class TrainY(DataSelectionTask):
    class Meta:
        input_tasks = [AllY, TestMoviesMask]
        train = True
        data_type = pd.Series


class TestY(DataSelectionTask):
    class Meta:
        input_tasks = [AllY, TestMoviesMask]
        train = False
        data_type = pd.Series


class TrainedModel(ModuleTask):

    class Meta:
        input_tasks = [TrainX, TrainY]
