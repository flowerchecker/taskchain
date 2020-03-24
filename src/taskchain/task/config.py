import json
import yaml
from pathlib import Path
from typing import Union, Dict, Iterable


class Config(dict):

    def __init__(self,
                 base_dir: Union[Path, str, None],
                 filepath: Union[Path, str] = None,
                 name: str = None,
                 data: Dict = None
                 ):
        super().__init__()

        self.base_dir = base_dir
        self.name = None
        self._data = None

        if filepath is not None:
            filepath = Path(filepath)
            name_parts = filepath.name.split('.')
            extension = name_parts[-1]
            self.name = '.'.join(name_parts[:-1])
            if extension == 'json':
                self._data = json.load(filepath.open())
            elif extension == 'yaml':
                self._data = yaml.load(filepath.open(), yaml.CLoader)

        if data is not None:
            self._data = data
        if name is not None:
            self.name = name
        if self.name is None:
            raise ValueError(f'Missing config name')

        self._validate_data()

    def __str__(self):
        return f'{self.name}'

    @property
    def data(self):
        if self._data is None:
            raise ValueError(f'Data of config `{self}` not initialized')
        return self._data

    def __getitem__(self, item):
        return self.data[item]

    def __getattr__(self, item):
        return self.data[item]

    def get(self, item, default=None):
        return self.data.get(item, default)

    def _validate_data(self):
        if self._data is None:
            return

        data = self._data
        uses = data.get('uses', [])
        if not isinstance(uses, Iterable) or isinstance(uses, str):
            raise ValueError(f'`uses` of config `{self}` have to be like')
