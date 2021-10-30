# Example project - movie ratings

[Example project]({{ config.code_url }}/example)
is small demonstration of TaskChain capabilities
and try to show its main features and constructions.

This project allows quick hands-on experience 
and can serve as template for new projects. You can start by running [this notebook]({{config.code_url}}/example/scripts/introduction.ipynb). 

Keep in mind, that goal of project is showcase of various features,
so chosen solutions for given problems in not always optimal.


## Install

```bash
pip install taskchain

git clone {{ config.repo_url }}
cd taskchain/example
python setup.py develop
```


## Description

Project works with IMDB movie dataset downloaded from [Kaggle](https://www.kaggle.com/stefanoleone992/imdb-extensive-dataset/version/2).
Goals of projects is to explore dataset and train a model which is able to predict rating of a new movie. 

Project is to split to 3 pipelines

#### Movies

[tasks]({{config.code_url}}/example/src/movie_ratings/tasks/movies.py),
[configs]({{config.code_url}}/example/configs/movies),
[notebook]({{config.code_url}}/example/scripts/movies.ipynb)

This pipeline has the following function

- load movies data
- filter them
- get basic statistics - year and duration histograms
- extract directors, movies, genres and countries of movies

#### Features

[tasks]({{config.code_url}}/example/src/movie_ratings/tasks/features.py),
[configs]({{config.code_url}}/example/configs/features),
[notebook]({{config.code_url}}/example/scripts/features.ipynb)

This pipeline build on movie pipeline and has the following function

- select the most relevant actors and directors (to use them as features)
- prepare all features - year, duration, and features based on movie's genres, countries, actors and directors (binary features) 
- select requested feature types

#### Rating model

[tasks]({{config.code_url}}/example/src/movie_ratings/tasks/rating_model.py),
[configs]({{config.code_url}}/example/configs/rating_model),
[notebook]({{config.code_url}}/example/scripts/rating_model.ipynb)

This pipeline build on features pipeline and has the following function

- create training and eval data from features
- train a mode - models are defined [here]({{config.code_url}}/example/src/movie_ratings/models)
- evaluate the model


## Project files
```text
example
├── configs
│   ├── features
│   │   ├── all.yaml
│   │   └── basic.yaml
│   ├── movies
│   │   ├── imdb.all.yaml
│   │   └── imdb.filtered.yaml
│   └── rating_model
│       ├── all_features
│       │   ├── baseline.yaml
│       │   ├── linear_regression.yaml
│       │   ├── nn.yaml
│       │   └── tf_linear_regression.yaml
│       └── basic_features
│           ├── baseline.yaml
│           └── linear_regression.yaml
├── data
│   ├── source_data
│   │   ├── IMDB_movies.csv
│   │   └── ratings.Thran.csv
│   └── task_data       # here will be computed data
├── scripts
│   ├── features.ipynb
│   ├── introduction.ipynb
│   ├── movies.ipynb
│   ├── personal_rating_model.ipynb
│   ├── rating_model.ipynb
│   └── tasks_run.py
├── setup.py
└── src
    └── movie_ratings
        ├── config.py
        ├── __init__.py
        ├── models
        │   ├── core.py
        │   ├── __init__.py
        │   ├── sklearn.py
        │   └── tensorflow.py
        └── tasks
            ├── features.py
            ├── __init__.py
            ├── movies.py
            └── rating_model.py

```
