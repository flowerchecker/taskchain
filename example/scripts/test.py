from taskchain.task import Config

from movie_ratings import config


config_name = 'movies/20210624.imdb.yaml'
chain = Config(config.TASKS_DIR, config.CONFIGS_DIR / config_name, global_vars=config).chain()

print(chain)

print(chain.all_movies.value)
