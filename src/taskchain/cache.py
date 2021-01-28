import abc
import json
import logging
import sys
from collections import defaultdict
from hashlib import sha256
from inspect import signature, Parameter
from pathlib import Path
from threading import get_ident
from typing import Callable, Union, Any

import numpy as np
import pandas as pd
from filelock import FileLock

logger = logging.getLogger('cache')
logger.addHandler(logging.StreamHandler(sys.stdout))


class Cache(abc.ABC):

    @abc.abstractmethod
    def get_or_compute(self, key: str, computer: Callable, force: bool = False):
        pass

    @abc.abstractmethod
    def subcache(self, *args):
        pass


class DummyCache(Cache):

    def get_or_compute(self, key: str, computer: Callable, force: bool = False):
        return computer()

    def subcache(self, *args):
        return self


class InMemoryCache(Cache):

    def __init__(self):
        self._memory = defaultdict(dict)

    def get_or_compute(self, key: str, computer: Callable, force: bool = False):
        if key not in self._memory[get_ident()] or force:
            self._memory[get_ident()][key] = computer()
        return self._memory[get_ident()][key]

    def subcache(self, *args):
        return InMemoryCache()

    def __len__(self):
        return len(self._memory[get_ident()])


class FileCache(Cache):
    def __init__(self, directory: Union[str, Path]):
        self.directory = Path(directory)
        self.directory.mkdir(exist_ok=True, parents=True)

    def filepath(self, key: str) -> Path:
        key_hash = sha256(key.encode()).hexdigest()
        directory = self.directory / key_hash[:5]
        directory.mkdir(exist_ok=True)
        return directory / f'{key_hash[5:]}.{self.extension}'

    def get_or_compute(self, key, computer, force=False):
        filepath = self.filepath(key)
        lock = FileLock(str(filepath) + '.lock')
        with lock:
            filepath_exists = filepath.exists()
        if filepath_exists and not force:
            try:
                return self.load_value(filepath, key)
            except CacheException as error:
                raise error
            except Exception as error:
                logger.warning('Cannot load cached value.')
                logger.exception(error)

        with lock:
            logger.debug(f'Computing cache for key {key} | file: {filepath}')
            value = computer()
            self.save_value(filepath, key, value)
        return value

    @abc.abstractmethod
    def save_value(self, filepath: Path, key: str, value: Any):
        pass

    @abc.abstractmethod
    def load_value(self, filepath: Path, key: str) -> Any:
        pass

    def subcache(self, directory: Union[str, Path]):
        return self.__class__(self.directory / directory)

    @property
    @abc.abstractmethod
    def extension(self):
        pass


class JsonCache(FileCache):

    def __init__(self, directory, allow_nones=True):
        super().__init__(directory)
        self.allow_nones = allow_nones

    def save_value(self, filepath: Path, key: str, value: Any):
        if value is None and not self.allow_nones:
            raise CacheException(f'The cache value for key {key} is None')
        with filepath.open('w') as f:
            json.dump({'key': key, 'value': value}, f)

    def load_value(self, filepath: Path, key: str) -> Any:
        with filepath.open('r') as file:
            loaded = json.load(file)
            if key != loaded['key']:
                raise CacheException(
                    f'The expected cache key {key} does not match to the retrieved one {loaded["key"]}')
            if loaded['value'] is None and not self.allow_nones:
                raise CacheException(f'The cache value for key {key} is None, file: {filepath}')
            return loaded['value']

    @property
    def extension(self):
        return 'json'


class DataFrameCache(FileCache):

    def save_value(self, filepath: Path, key: str, value: Any):
        value.to_pickle(filepath)

    def load_value(self, filepath: Path, key: str) -> Any:
        return pd.read_pickle(filepath)

    @property
    def extension(self):
        return 'pd'


class NumpyArrayCache(FileCache):

    def save_value(self, filepath: Path, key: str, value: Any):
        np.save(filepath, value)

    def load_value(self, filepath: Path, key: str) -> Any:
        return np.load(filepath, allow_pickle=True)

    @property
    def extension(self):
        return 'npy'


class CacheException(Exception):

    pass


def cached(cache_object=None, key=None, cache_attr='cache'):
    def _cached(method):
        def _method(self, *args, **kwargs):
            if cache_object is None:
                assert hasattr(self, cache_attr), 'Missing cache argument'
                cache = self.cache
            else:
                cache = cache_object

            if key is None:
                for i, (arg, parameter) in enumerate(signature(method).parameters.items()):
                    if i == 0:
                        # skip self
                        continue
                    if i - 1 < len(args):
                        kwargs[arg] = args[i - 1]
                    if parameter.default != Parameter.empty and arg not in kwargs:
                        kwargs[arg] = parameter.default
                args = []
                cache_key = json.dumps(kwargs, sort_keys=True)
            else:
                cache_key = key(*args, **kwargs)

            return cache.get_or_compute(cache_key, lambda: method(self, *args, **kwargs))
        return _method
    return _cached
