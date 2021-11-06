from typing import Callable, Iterable

from .iter import progress_bar
import asyncio
import concurrent.futures


def parallel_map(fun: Callable, iterable: Iterable, threads: int = 10, sort: bool = True,
                 use_tqdm: bool = True, desc: str = 'Running tasks in parallel.', total: int = None):
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

    def _fun(i, arg):
        return i, fun(arg)

    async def _run():
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                loop = asyncio.get_event_loop()
                futures = [
                    loop.run_in_executor(
                        executor,
                        _fun,
                        i,
                        input_value
                    )
                    for i, input_value in enumerate(iterable)
                ]
                return [await output_value for output_value in progress_bar(asyncio.as_completed(futures), use_tqdm=use_tqdm, desc=desc, total=total)]

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(_run())
    return [
        res for _, res in (sorted(result, key=lambda ires: ires[0]) if sort else result)
    ]


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
