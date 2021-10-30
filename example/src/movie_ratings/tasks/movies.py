from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Generator

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from taskchain import ModuleTask
from taskchain import Parameter
from taskchain.utils.iter import progress_bar


class AllMovies(ModuleTask):
    """ DataFrame with all movies and their data """

    class Meta:
        input_tasks = []
        parameters = [
            Parameter('source_file', dtype=Path)
        ]

    def run(self, source_file) -> pd.DataFrame:
        df = pd.read_csv(source_file)
        df.set_index(df.imdb_title_id, inplace=True)

        def _extract_year(year):
            try:
                return int(year)
            except ValueError:
                self.logger.warning(f'Invalid year {year}')
                return int(year[-4:])

        df['year'] = df.year.map(_extract_year)
        return df


class Movies(ModuleTask):
    """ DataFrame with all movies and their data """

    class Meta:
        input_tasks = [AllMovies]
        parameters = [
            Parameter('min_vote_count', default=None, dtype=int),
            Parameter('from_year', default=None, dtype=int),
            Parameter('to_year', default=None, dtype=int),
        ]

    def run(self, all_movies) -> pd.DataFrame:
        df = all_movies
        if self.params.min_vote_count is not None:
            df = df.query(f'votes >= {self.params.min_vote_count}')
        if self.params.from_year is not None:
            df = df.query(f'year >= {self.params.from_year}')
        if self.params.to_year is not None:
            df = df.query(f'year <= {self.params.to_year}')
        return df


class Movie_names(ModuleTask):

    class Meta:
        input_tasks = [Movies]
        data_type = Dict

    def run(self):
        return dict(zip(
            self.input_tasks['movies'].value.index,        # access input task by name
            self.input_tasks[0].value.original_title,      # access input task by order defined in meta
        ))


class DurationHistogram(ModuleTask):
    class Meta:
        input_tasks = [Movies]
        parameters = [
            Parameter('max_duration_in_histogram', default=4 * 60)
        ]

    def run(self, movies, max_duration_in_histogram) -> plt.Figure:
        sns.distplot(movies.duration)
        plt.xlim(0, max_duration_in_histogram)
        return plt.gcf()


class YearHistogram(ModuleTask):
    class Meta:
        input_tasks = [Movies]

    def run(self, movies) -> plt.Figure:
        max_year = movies.year.max()
        min_year = movies.year.min()
        sns.distplot(movies.year, bins=max_year - min_year + 1)
        plt.xlim(min_year, max_year)
        return plt.gcf()


class ExtractFeatureTask(ModuleTask):
    """ Universal task for extracting M:N feature to dict feature -> list of movie ids """

    class Meta:
        abstract = True
        input_tasks = [Movies]
        parameters = []
        column_name = None

    def run(self, movies) -> Dict:
        feature_movie_map = defaultdict(list)
        for movie in progress_bar(movies.itertuples(), 'Processing movies'):
            features = getattr(movie, self.meta.column_name)
            if pd.isna(features):
                continue
            for feature in features.split(', '):
                feature_movie_map[feature.strip()].append(movie.Index)

        self.save_to_run_info(f'feature {self.meta.column_name} count: {len(feature_movie_map)}')
        return dict(feature_movie_map)


class Directors(ExtractFeatureTask):

    class Meta:
        input_tasks = ExtractFeatureTask.meta.input_tasks
        column_name = 'director'


class Genres(ExtractFeatureTask):

    class Meta:
        input_tasks = ExtractFeatureTask.meta.input_tasks
        column_name = 'genre'


class Countries(ExtractFeatureTask):

    class Meta:
        input_tasks = ExtractFeatureTask.meta.input_tasks
        column_name = 'country'


class Actors(ExtractFeatureTask):

    class Meta:
        input_tasks = ExtractFeatureTask.meta.input_tasks
        column_name = 'actors'
