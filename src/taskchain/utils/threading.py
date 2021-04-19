from .iter import progress_bar
import asyncio
import concurrent.futures


def parallel_map(fun, iterable, use_tqdm=True, threads=10, desc='Running tasks in parallel.', total=None, sort=True):

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


def parallel_starmap(fun, iterable, use_tqdm=True, threads=10, desc='Running tasks in parallel.', total=None):

    def _call(d):
        return fun(*d)
    return parallel_map(_call, iterable, use_tqdm=use_tqdm, threads=threads, desc=desc, total=total)
