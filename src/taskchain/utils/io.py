import json
from pathlib import Path
from typing import Union

from tqdm import tqdm


def write_jsons(jsons, filename, use_tqdm=True, overwrite=True, nan_to_null=True, **kwargs):
    filename = Path(filename)
    assert not filename.exists() or overwrite, 'File already exists'
    with filename.open('w') as f:
        for j in tqdm(jsons, disable=not use_tqdm, desc=f'Writing to {f.name}', **kwargs):
            f.write(json.dumps(j, ignore_nan=nan_to_null) + '\n')


def iter_json_file(filename, use_tqdm=True, **kwargs):
    filename = Path(filename)
    with filename.open() as f:
        for row in tqdm(f, disable=not use_tqdm, desc=f'Reading from {f.name}', **kwargs):
            yield json.loads(row.strip())


def check_file_exists(path: Union[Path, str]):
    path = Path(path)
    if not path.exists():
        raise ValueError(f'File `{path}` doesn\'t exists')
