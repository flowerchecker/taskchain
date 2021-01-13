from .jupyter import in_ipynb
from tqdm import tqdm, tqdm_notebook


def progress_bar(data, use_tqdm=True, smoothing=0, **kwargs):
    def iter_fce(iterator, **kwargs):
        return iterator
    if use_tqdm:
        iter_fce = tqdm_notebook if False else tqdm
        kwargs['smoothing'] = smoothing
    return iter_fce(data, **kwargs)
