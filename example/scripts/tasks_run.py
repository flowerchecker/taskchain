from taskchain import Config

from movie_ratings import config


config_name = 'movies/20210624.imdb.yaml'
chain = Config(config.TASKS_DIR, config.CONFIGS_DIR / config_name, global_vars=config).chain()
print(chain)


chain.force('movies')

print(chain.movies.value.columns)
# print(chain.directors.value)
print(chain.directors.run_info['log'])
print(chain.selected_directors.value)
print(chain.selected_directors.run_info['log'])
