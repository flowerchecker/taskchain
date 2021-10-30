# Utils

## `ic` ( [IceCream](https://github.com/gruns/icecream) )

If you import taskschain `ic` is installed and you can use it whithout import.


## Caching

For ease up saving "expensive" computation, TaskChain have simple caching tools.
This can be used for saving e.g. api calls.

```python
from taskchain.cache import JsonCache

cache = JsonCache('/path/to/cache_dir')

input_ = 42

def _computation():
    return expensive(input_)

cache_key = f'key_{input_}'
result = cache.get_or_compute(cache_key, _computation)
```

Result is loaded from cache if presented or computed and saved in cache.

You can also use `@cached` decorator which can handle creation of cache key automatically from arguments.

```python
from taskchain.cache import JsonCache, cached

class MyClass:
    @cached(JsonCache('/path/to/cache_dir'))
    def cached_method(self, input_):
        return expensive(input_)

my = MyClass()
result = my.cached_method(42)
```

There are multiple `Cache` classes prepared

- `DummyCache` - no caching
- `InMemoryCache` - values are cached only in memory, all types allowed
- `JsonCache` - saves json-like objects to json
- `DataFrameCache`
- `NumpyArrayCache`
- `FileCache` - abstract class useful for implementing own type of caches


## `@persistent` decorator

This decorator can be used on class methods without arguments.
Result of this method is cached in `self.__method_name` attribute after first call.
On other calls cached value is return. 

!!! Tip
    You can also combine `@persistent` with `@property` decorator, 
    just `@property` is before `@persistent`.

This can be useful lazy behaviour of your classes.

!!! Example
    === "lazy solution"

    ```python
    class MyClass:
        
        @property
        @persistent
        def foo(self):
            return expensive_computation()
    ```

    === "classic solution"

    ```python
    class MyClass:
        
        def __init__(self):
            self.foo = expensive_computation()
        
    ```

## `parallel_map`

You can use `parallel_map` for easy application of threading.


```python
from taskchain.utils.threading import parallel_map


def download_urls(urls: list, threads=2):
    def _fun(url):
        return download(url)

    return parallel_map(_fun, urls, threads=threads, desc='Downloading', total=len(urls))
```

## `@repeat_on_error` decorator

This decorator is useful for calling api or downloading data from internet. 
It only tries to run a method again if error occurs.

```python
from taskchain.utils.clazz import repeat_on_error

class Downloader:
    # first retry is after 2 second, second after 4, third after 8
    @repeat_on_error(waiting_time=2, wait_extension=2, retries=3)
    def download(self, url, exact_match=True):
        ...
```
