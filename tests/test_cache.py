from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import pytest

from taskchain.cache import DataFrameCache, InMemoryCache, JsonCache, NumpyArrayCache, cached


def test_in_memory_subcache():
    cache = InMemoryCache()
    assert len(cache) == 0
    assert cache.get_or_compute('key', lambda: 1) == 1
    assert len(cache) == 1
    first_subcache = cache.subcache('first')
    second_subcache = cache.subcache('second')
    assert first_subcache.get_or_compute('key', lambda: 2) == 2
    assert len(cache) == 1
    assert len(first_subcache) == 1
    assert len(second_subcache) == 0
    assert second_subcache.get_or_compute('key', lambda: 3) == 3
    assert len(cache) == 1
    assert len(first_subcache) == 1
    assert len(second_subcache) == 1


def test_cache_decorator():
    external_cache = InMemoryCache()
    external_cache2 = InMemoryCache()

    class CachedClass:
        def __init__(self):
            self.cache = InMemoryCache()

        @cached
        def cached_method(self, parameter_1, parameter_2, key_parameter_1=10, key_parameter_2=20):
            return parameter_1 + parameter_2 + key_parameter_1 + key_parameter_2

        @cached(external_cache)
        def cached_method2(self, parameter_1, parameter_2, key_parameter_1=10, key_parameter_2=20):
            return parameter_1 + parameter_2 + key_parameter_1 + key_parameter_2

        @cached(external_cache2)
        def cached_method3(self, parameter_1, parameter_2, key_parameter_1=10, key_parameter_2=20):
            return parameter_1 + parameter_2 + key_parameter_1 + key_parameter_2

    obj = CachedClass()

    for cache, method in [
        (obj.cache.subcache(obj.cached_method.__name__), obj.cached_method),
        (external_cache, obj.cached_method2),
        (external_cache2, obj.cached_method3),
    ]:
        assert len(cache) == 0
        assert method(1, 2) == 33
        assert len(cache) == 1

        assert method(1, 2) == 33
        assert len(cache) == 1

        assert method(1, 2, 10, 20) == 33
        assert len(cache) == 1

        assert method(1, 2, 10, parameter_2=20) == 33
        assert len(cache) == 1

        assert method(1, 2, 20, 10) == 33
        assert len(cache) == 2


def test_cache_decorator_forcing():
    class CachedClass:
        def __init__(self):
            self.cache = InMemoryCache()
            self.calls = 0

        @cached
        def cached_method(self, parameter_1, parameter_2, key_parameter_1=10, key_parameter_2=20):
            self.calls += 1
            return parameter_1 + parameter_2 + key_parameter_1 + key_parameter_2

    obj = CachedClass()
    cache = obj.cache.subcache(obj.cached_method.__name__)

    assert obj.cached_method(1, 2) == 33
    assert len(cache) == 1
    assert obj.calls == 1

    assert obj.cached_method(1, 2) == 33
    assert len(cache) == 1
    assert obj.calls == 1

    assert obj.cached_method(1, 2, force_cache=True) == 33
    assert len(cache) == 1
    assert obj.calls == 2


@pytest.mark.parametrize('cache_class,value,test', [
    (JsonCache, lambda: 'value', lambda v: v == 'value'),
    (JsonCache, lambda: {'a': 3}, lambda v: v['a'] == 3),
    (JsonCache, lambda: None, lambda v: v is None),
    (NumpyArrayCache, lambda: np.zeros(10), lambda v: v.shape == (10,)),
    (DataFrameCache, lambda: pd.DataFrame(np.zeros((10, 10))), lambda v: v.values.shape == (10, 10)),
])
def test_cache(tmp_path, cache_class, value, test):
    class Example:
        counter = 0
        def fce(self):
            self.counter += 1
            return value()

    example = Example()
    cache = cache_class(tmp_path)
    assert test(cache.get_or_compute('key', example.fce))
    assert example.counter == 1
    assert test(cache.get_or_compute('key', example.fce))
    assert example.counter == 1


def test_cache_decorator_ignore_params():
    class CachedClass:
        def __init__(self):
            self.cache = InMemoryCache()

        @cached(ignore_kwargs=['parameter_2', 'key_parameter_2'])
        def cached_method(self, parameter_1, parameter_2, key_parameter_1=10, key_parameter_2=20):
            return parameter_1 + key_parameter_1

    obj = CachedClass()
    cache = obj.cache.subcache(obj.cached_method.__name__)

    assert len(obj.cache._memory) == 0
    assert obj.cached_method(1, 2) == 11
    assert len(cache) == 1

    assert obj.cached_method(1, 666) == 11
    assert len(cache) == 1

    assert obj.cached_method(1, 666, key_parameter_2=123) == 11
    assert len(cache) == 1


def test_cache_decorator_version():
    class CachedClass:
        def __init__(self, cache_dir):
            self.cache = JsonCache(cache_dir)

        @cached(version='v1')
        def method_with_version(self, param):
            return param

        @cached()
        def method_without_version(self, param):
            return param
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        instance = CachedClass(tmpdir)
        instance.method_with_version(None)
        assert (tmpdir / 'method_with_version.v1').exists()
        assert not (tmpdir / 'method_without_version').exists()
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        instance = CachedClass(tmpdir)
        instance.method_without_version(None)
        assert not (tmpdir / 'method_with_version.v1').exists()
        assert (tmpdir / 'method_without_version').exists()
