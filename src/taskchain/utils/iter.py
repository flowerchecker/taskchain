import asyncio
import concurrent.futures
from typing import Union, List

from .jupyter import in_ipynb
from tqdm import tqdm, tqdm_notebook


def progress_bar(data, use_tqdm=True, smoothing=0., **kwargs):
    def iter_fce(iterator, **kwargs):
        return iterator
    if use_tqdm:
        iter_fce = tqdm_notebook if in_ipynb() else tqdm
        kwargs['smoothing'] = smoothing
    return iter_fce(data, **kwargs)


def parallel_map(fun, iterable, threads=2, desc='Running tasks in parallel.', total=None, smoothing=0.3):
    if total is None and hasattr(iterable, '__len__'):
        total = len(iterable)
    if threads == 1:
        return [fun(i) for i in iterable]

    def _fun(i, arg):
        return i, fun(arg)

    async def _run():
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    executor, _fun, i, input_value
                ) for i, input_value in enumerate(iterable) ]
            return [
                await output_value
                for output_value
                in progress_bar(asyncio.as_completed(futures), desc=desc, total=total, smoothing=smoothing)
            ]

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(_run())
    return [res for _, res in sorted(result, key=lambda ires: ires[0])]


def list_or_str_to_list(value: Union[None, List[str], str]) -> List[str]:
    """ Helper function for cases where list of string is expected but single string is also ok.

    Args:
        value:

    Returns:
        original list or original string in list
    """
    if isinstance(value, str):
        return [value]
    return value
