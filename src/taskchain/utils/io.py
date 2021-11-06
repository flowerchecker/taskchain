import logging
from pathlib import Path
from typing import Union
import json
import numpy as np

from taskchain.utils.iter import progress_bar


def write_jsons(jsons, filename, use_tqdm=True, overwrite=True, nan_to_null=True, **kwargs):
    """
    Write json-like object to `.jsonl` file (json lines).
    Args:
        jsons (Iterable): Iterable of json-like objects.
        filename (Path | str):
        use_tqdm (bool): Show progress bar.
        overwrite (bool): Overwrite existing file.
        nan_to_null (bool): Change nan values to nulls.
        **kwargs: other arguments to tqdm.
    """
    filename = Path(filename)
    assert not filename.exists() or overwrite, 'File already exists'
    with filename.open('w') as f:
        for j in progress_bar(jsons, disable=not use_tqdm, desc=f'Writing to {f.name}', **kwargs):
            f.write(json.dumps(j, ignore_nan=nan_to_null, cls=NumpyEncoder, ensure_ascii=False) + '\n')


def iter_json_file(filename, use_tqdm=True, **kwargs):
    """
    Yield loaded jsons from `.jsonl` file (json lines).

    Args:
        filename (Path | str):
        use_tqdm (bool):
        **kwargs: additional arguments to tqdm

    Returns:

    """
    filename = Path(filename)
    with filename.open() as f:
        for row in progress_bar(f, disable=not use_tqdm, desc=f'Reading from {f.name}', **kwargs):
            yield json.loads(row.strip())


def check_file_exists(path: Union[Path, str]):
    path = Path(path)
    if not path.exists():
        raise ValueError(f'File `{path}` doesn\'t exists')


class NumpyEncoder(json.JSONEncoder):

    def __init__(self, ignore_nan=True, **kwargs):
        super().__init__(**kwargs)
        self.ignore_nan = ignore_nan

    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return self.default(obj.tolist())
        if isinstance(obj, list):
            return [self.default(x) for x in obj]
        if isinstance(obj, (np.int32, np.int64)):
            return int(obj)
        if isinstance(obj, (np.float32, np.float64)):
            return float(obj)
        if np.isnan(obj) and self.ignore_nan:
            return None
        if isinstance(obj, (int, bool, str, float)):
            return obj
        return json.JSONEncoder.default(self, obj)


class ListHandler(logging.Handler):
    def __init__(self, log_list):
        logging.Handler.__init__(self)
        self.log_list = log_list

    def emit(self, record):
        self.log_list.append(record.msg)
