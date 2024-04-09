import abc
import json
from pathlib import Path
from typing import Union

import pandas as pd
import tensorflow as tf

from movie_ratings.models.core import RatingModel
from taskchain.utils.io import NumpyEncoder


class TFRatingModel(RatingModel, abc.ABC):
    def __init__(self, learning_params, batch_size):
        self.learning_params = learning_params
        self.batch_size = batch_size
        super().__init__()

        self.model: Union[tf.keras.Model, None] = None
        self.history = None

    def _train(self, X, y):
        self.model = self.build_model(X.shape[1])
        self.model.compile(
            optimizer=self.learning_params.get('optimizer', 'adam'),
            loss='mean_squared_error',
            metrics=[tf.keras.metrics.RootMeanSquaredError()],
        )

        self.history = self.model.fit(
            X,
            y,
            epochs=self.learning_params['epochs'],
            callbacks=[self.learning_rate_scheduler(self.learning_params['lr_schedule'])],
        ).history

    def learning_rate_scheduler(self, learning_rate_schedule):
        def _lr(epoch, lr):
            for epoch_threshold, learning_rate in learning_rate_schedule[::-1]:
                if epoch + 1 >= epoch_threshold:
                    return learning_rate
            else:
                assert False, f'Learning rate not found in schedule for epoch {epoch}'

        return tf.keras.callbacks.LearningRateScheduler(_lr)

    def _predict(self, X):
        assert self.model is not None, 'Model is not loaded'
        return self.model.predict(X)[:, 0]

    def save(self, directory: Path):
        json.dump(self.history, (directory / 'log.json').open('w'), cls=NumpyEncoder)
        self.model.save(directory)

    def load(self, directory: Path):
        self.history = json.load((directory / 'log.json').open())
        self.model = tf.keras.models.load_model(directory)

    def _process_features(self, features: pd.DataFrame):
        return features.values.astype(float)

    def _process_labels(self, labels: pd.Series):
        return labels.values.astype(float)

    @staticmethod
    def ignore_persistence_args():
        return ['batch_size']

    @abc.abstractmethod
    def build_model(self, feature_count: int) -> tf.keras.Model:
        pass


class LinearRatingModel(TFRatingModel):
    def __init__(self, learning_params, regularization=0.01, batch_size=128):
        self.regularization = regularization

        super().__init__(learning_params, batch_size)

    def build_model(self, feature_count: int) -> tf.keras.Model:
        kernel_regularizer = tf.keras.regularizers.l1_l2(l1=self.regularization, l2=self.regularization)
        input = tf.keras.Input((feature_count,), dtype=float)
        output = 10 * tf.keras.layers.Dense(1, activation='sigmoid', kernel_regularizer=kernel_regularizer)(input)

        return tf.keras.Model(input, output)


class NeuralRatingModel(TFRatingModel):
    def __init__(self, layers, learning_params, dropout=0.5, batch_size=128):
        self.layers = layers
        self.dropout = dropout

        super().__init__(learning_params, batch_size)

    def build_model(self, feature_count: int) -> tf.keras.Model:
        input = tf.keras.Input((feature_count,), dtype=float)
        x = tf.keras.layers.BatchNormalization()(input)
        for layer_size in self.layers:
            x = tf.keras.layers.Dense(layer_size, activation='swish')(x)
            x = tf.keras.layers.Dropout(self.dropout)(x)
        output = 10 * tf.keras.layers.Dense(1, activation='sigmoid')(x)

        return tf.keras.Model(input, output)
