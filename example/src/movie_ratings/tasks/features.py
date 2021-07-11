from typing import List, Generator

import pandas as pd

from movie_ratings.tasks.movies import Directors, Movies, Actors, Countries, Genres
from taskchain.task import ModuleTask, InMemoryData
from taskchain.task.parameter import Parameter


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


class AllFeatures(ModuleTask):

    BASIC_FEATURES = ['year', 'duration']

    class Meta:
        input_tasks = [Movies, Genres, Countries, Actors, Directors, SelectedActors, SelectedDirectors]

    def run(self, movies, selected_actors, selected_directors, genres, countries, actors, directors) -> pd.DataFrame:
        features = movies[self.BASIC_FEATURES]

        feature_types = {
            'genre': genres,
            'country': countries,
            'actor': {actor: movies for actor, movies in actors.items() if actor in selected_actors},
            'director': {director: movies for director, movies in actors.items() if director in selected_directors},
        }
        for feature_type, feature_movie_map in feature_types.items():
            for feature, movies in sorted(feature_movie_map.items()):
                feature_name = f'{feature_type}_{feature}'
                features[feature_name] = features.index.isin(movies)

        return features


class FeatureNames(ModuleTask):

    class Meta:
        input_tasks = [AllFeatures]
        parameters = [
            Parameter('feature_types', dtype=list)
        ]

    def run(self, all_features, feature_types) -> List:
        return [column for column in all_features.columns if column.split('_')[0] in feature_types]


class Features(ModuleTask):

    class Meta:
        input_tasks = [AllFeatures, FeatureNames]
        data_class = InMemoryData

    def run(self, all_features, feature_names) -> pd.DataFrame:
        return all_features[feature_names]
