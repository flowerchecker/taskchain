from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Generator

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from taskchain.task import ModuleTask
from taskchain.task.parameter import Parameter
from taskchain.utils.iter import progress_bar


class Movies(ModuleTask):
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


class DurationHistogram(ModuleTask):
    class Meta:
        input_tasks = [Movies]
        parameters = [
            Parameter('max_duration_in_histogram', default=4 * 60)
        ]

    def run(self, movies, max_duration_in_histogram) -> plt.Figure:
        figure = plt.Figure()
        sns.distplot(movies.duration)
        plt.xlim(0, max_duration_in_histogram)
        return figure


class YearHistogram(ModuleTask):
    class Meta:
        input_tasks = [Movies]

    def run(self, movies) -> plt.Figure:
        figure = plt.Figure()
        max_year = movies.year.max()
        min_year = movies.year.min()
        sns.distplot(movies.year, bins=max_year - min_year + 1)
        plt.xlim(min_year, max_year)
        return figure


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


class SelectedDirectors(ModuleTask):
    """ List of selected directors - with enough movies with good rating """

    class Meta:
        input_tasks = [Directors, Movies]
        parameters = [
            Parameter('director_minimal_movie_count'),
            Parameter('director_minimal_movie_rating', default=7),
        ]

    def run(self, directors, movies, director_minimal_movie_count, director_minimal_movie_rating) -> Generator:
        selected_directors_count = 0
        for director, directors_movies in directors.items():
            good_movies = [m for m in directors_movies
                           if self.get_movie_rating(movies, m) >= director_minimal_movie_rating]
            if len(good_movies) >= director_minimal_movie_count:
                selected_directors_count += 1
                yield director

        self.save_to_run_info(f'Selected directors count: {selected_directors_count}')

    def get_movie_rating(self, movies, movie):
        return movies.loc[movie].avg_vote


class SelectedActors(ModuleTask):
    """ List of selected actors - with enough movies """

    class Meta:
        input_tasks = [Actors, Movies]
        parameters = [
            Parameter('actor_minimal_movie_count', default=20),
        ]

    def run(self, actors, movies, actor_minimal_movie_count) -> List:
        selected_actors = []
        for actor, actors_movies in actors.items():
            if len(actors_movies) >= actor_minimal_movie_count:
                selected_actors.append(actor)

        self.save_to_run_info(f'Selected directors count: {selected_actors}')
        return selected_actors
