from typing import List, Generator

from movie_ratings.tasks.movies import Directors, Movies, Actors
from taskchain.task import ModuleTask
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
