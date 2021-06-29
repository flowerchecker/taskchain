from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Generator

import pandas as pd

from taskchain.task import ModuleTask
from taskchain.task.parameter import Parameter


class Movies(ModuleTask):

    class Meta:
        input_tasks = []
        parameters = [
            Parameter('source_file', dtype=Path)
        ]

    def run(self, source_file) -> pd.DataFrame:
        df = pd.read_csv(source_file)
        df.set_index(df.imdb_title_id, inplace=True)
        return df


class Directors(ModuleTask):
    class Meta:
        input_tasks = [Movies]
        parameters = []

    def run(self, movies) -> Dict:
        directors = defaultdict(list)
        for movie in movies.itertuples():
            if pd.isna(movie.director):
                continue
            for director in movie.director.split(', '):
                directors[director.strip()].append(movie.Index)

        self.save_to_run_info(f'Directors count: {len(directors)}')
        return dict(directors)


class SelectedDirectors(ModuleTask):
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
