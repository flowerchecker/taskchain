import asyncio
import concurrent.futures
import gc
from typing import Callable, Iterable

from tqdm.auto import tqdm

from .iter import chunked


def parallel_map(
    fun: Callable,
    iterable: Iterable,
    threads: int = 10,
    sort: bool = True,
    use_tqdm: bool = True,
    desc: str = 'Running tasks in parallel.',
    total: int = None,
    chunksize: int = 1000,
):
    """
    Map function to iterable in multiple threads.

    Args:
        fun: function to apply
        iterable:
        threads: number of threads
        sort: return values in same order as itarable
        use_tqdm: show progressbar
        desc: text of progressbar
        total: size of iterable to allow show better progressbar

    Returns:
        list: of returned values by fce
    """
    iterable = iterable
    if isinstance(iterable, list) and total is None:
        total = len(iterable)
    if threads == 1:
        return [fun(v) for v in (tqdm(iterable, desc=desc, total=total, maxinterval=2) if use_tqdm else iterable)]

    def _fun(i, arg):
        return i, fun(arg)

    pbar = tqdm(desc=desc, total=total, maxinterval=2) if use_tqdm else None

    async def _run(chunk):
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            loop = asyncio.get_event_loop()
            futures = [loop.run_in_executor(executor, _fun, i, input_value) for i, input_value in enumerate(chunk)]
            result = []
            for output_value in asyncio.as_completed(futures):
                to_append = await output_value
                if use_tqdm:
                    pbar.update()
                result.append(to_append)
            return result
        gc.collect()

    loop = asyncio.get_event_loop()
    result = []
    for chunk in chunked(iterable, chunksize=chunksize):
        chunk_result = loop.run_until_complete(_run(chunk))
        for _, res in sorted(chunk_result, key=lambda ires: ires[0]) if sort else chunk_result:
            result.append(res)
    return result


def parallel_starmap(fun: Callable, iterable: Iterable, **kwargs):
    """
    Allows use `parallel_map` for function with multiple arguments.

    Args:
        fun: function with multiple arguments
        iterable: lists or tuples of arguments
    """

    def _call(d):
        return fun(*d)

    return parallel_map(_call, iterable, **kwargs)
