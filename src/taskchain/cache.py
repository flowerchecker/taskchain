import abc
import functools
import json
import logging
import sys
from collections import defaultdict
from hashlib import sha256
from inspect import signature, Parameter
from pathlib import Path
from threading import get_ident
from typing import Callable, Union, Any, List

import numpy as np
import pandas as pd
from filelock import FileLock

logger = logging.getLogger('cache')
logger.addHandler(logging.StreamHandler(sys.stdout))


class Cache(abc.ABC):
    """ Cache interface. """

    @abc.abstractmethod
    def get_or_compute(self, key: str, computer: Callable, force: bool = False) -> Any:
        """
        Get value for given key if cached or compute and cache it.

        Args:
            key: key under which value is cached
            computer: function which returns value if not cached
            force: recompute value even if it is in cache

        Returns:
            cached or computed value
        """
        pass

    @abc.abstractmethod
    def subcache(self, *args) -> 'Cache':
        """ Create separate sub-cache of this cache. """
        pass


class DummyCache(Cache):
    """ No caching. """

    def get_or_compute(self, key: str, computer: Callable, force: bool = False):
        """"""
        return computer()

    def subcache(self, *args):
        """"""
        return self


class InMemoryCache(Cache):
    """ Cache only in memory. """

    def __init__(self):
        self._memory = defaultdict(dict)

    def get_or_compute(self, key: str, computer: Callable, force: bool = False):
        """"""
        if key not in self._memory[get_ident()] or force:
            self._memory[get_ident()][key] = computer()
        return self._memory[get_ident()][key]

    def subcache(self, *args):
        """"""
        return InMemoryCache()

    def __len__(self):
        return len(self._memory[get_ident()])


class FileCache(Cache):
    """ General cache for saving values in files. """

    def __init__(self, directory: Union[str, Path]):
        self.directory = Path(directory)
        self.directory.mkdir(exist_ok=True, parents=True)

    def filepath(self, key: str) -> Path:
        key_hash = sha256(key.encode()).hexdigest()
        directory = self.directory / key_hash[:5]
        directory.mkdir(exist_ok=True)
        return directory / f'{key_hash[5:]}.{self.extension}'

    def get_or_compute(self, key, computer, force=False):
        """"""
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
        """"""
        return self.__class__(self.directory / directory)

    @property
    @abc.abstractmethod
    def extension(self):
        pass


class JsonCache(FileCache):
    """ Cache json-like objects in `.json` files. """

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
    """ Cache pandas DataFrame objects in `.pd` files. """

    def save_value(self, filepath: Path, key: str, value: Any):
        value.to_pickle(filepath)

    def load_value(self, filepath: Path, key: str) -> Any:
        return pd.read_pickle(filepath)

    @property
    def extension(self):
        return 'pd'


class NumpyArrayCache(FileCache):
    """ Cache numpy arrays in `.npy` files. """

    def save_value(self, filepath: Path, key: str, value: Any):
        np.save(filepath, value)

    def load_value(self, filepath: Path, key: str) -> Any:
        return np.load(filepath, allow_pickle=True)

    @property
    def extension(self):
        return 'npy'


class CacheException(Exception):

    pass


class cached:
    """
    Decorator for automatic caching of method results.
    Decorated method is for given arguments called only once a result is cached.
    Cache key is automatically constructed based on method arguments.
    Cache can be defined in decorator or as attribute of object.
    """

    def __init__(self, cache_object: Cache = None, key: Callable = None,
                 cache_attr: str = 'cache', ignore_kwargs: List[str] = None):
        """
        Args:
            cache_object: Cache used for caching.
            key: custom function for computing key from arguments
            cache_attr: if `cache_object` is None, object attribute with this name is used
            ignore_kwargs: kwargs to ignore in key construction, e.g. `verbose`
        """
        if callable(cache_object):
            self.method = cache_object
            cache_object = None
        self.cache_object = cache_object
        self.key = key
        self.cache_attr = cache_attr
        self.ignore_params = ignore_kwargs if ignore_kwargs else []

    def __call__(self, method):
        def decorated(obj, *args, force_cache=False, **kwargs):
            if self.cache_object is None:
                assert hasattr(obj, self.cache_attr), 'Missing cache argument'
                cache = getattr(obj, self.cache_attr)
            else:
                cache = self.cache_object

            if self.key is None:
                for i, (arg, parameter) in enumerate(signature(method).parameters.items()):
                    if i == 0:
                        # skip self
                        continue
                    if i - 1 < len(args):
                        kwargs[arg] = args[i - 1]
                    if parameter.default != Parameter.empty and arg not in kwargs:
                        kwargs[arg] = parameter.default
                args = []
                key_kwargs = {k: v for k, v in kwargs.items() if k not in self.ignore_params}
                cache_key = json.dumps(key_kwargs, sort_keys=True)
            else:
                cache_key = self.key(*args, **kwargs)

            return cache.get_or_compute(cache_key, lambda: method(obj, *args, **kwargs), force=force_cache)
        return decorated

    def __get__(self, instance, instancetype):
        return functools.partial(self(self.method), instance)
