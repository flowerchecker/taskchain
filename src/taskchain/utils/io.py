from pathlib import Path
from tqdm import tqdm
from typing import Union
import json
import numpy as np


def write_jsons(jsons, filename, use_tqdm=True, overwrite=True, nan_to_null=True, **kwargs):
    filename = Path(filename)
    assert not filename.exists() or overwrite, 'File already exists'
    with filename.open('w') as f:
        for j in tqdm(jsons, disable=not use_tqdm, desc=f'Writing to {f.name}', **kwargs):
            f.write(json.dumps(j, ignore_nan=nan_to_null, cls=NumpyEncoder, ensure_ascii=False) + '\n')


def iter_json_file(filename, use_tqdm=True, **kwargs):
    filename = Path(filename)
    with filename.open() as f:
        for row in tqdm(f, disable=not use_tqdm, desc=f'Reading from {f.name}', **kwargs):
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
            return obj.tolist()
        if isinstance(obj, np.int32):
            return int(obj)
        if np.isnan(obj) and self.ignore_nan:
            return None
        return json.JSONEncoder.default(self, obj)
