import json
from pathlib import Path
from typing import Union, Dict, Iterable, Any

import yaml

from taskchain.utils.clazz import find_and_instancelize_clazz
from taskchain.utils.data import search_and_replace_placeholders


class Config(dict):

    def __init__(self,
                 base_dir: Union[Path, str, None] = None,
                 filepath: Union[Path, str] = None,
                 name: str = None,
                 namespace: str = None,
                 data: Dict = None,
                 global_vars: Union[Any, None] = None,
                 ):
        super().__init__()

        self.base_dir = base_dir
        self._name = None
        self.namespace = namespace
        self._data = None
        self.global_vars = global_vars
        self._filepath = filepath

        if filepath is not None:
            filepath = Path(filepath)
            name_parts = filepath.name.split('.')
            extension = name_parts[-1]
            self._name = '.'.join(name_parts[:-1])
            if extension == 'json':
                self._data = json.load(filepath.open())
            elif extension == 'yaml':
                self._data = yaml.load(filepath.open(), Loader=yaml.Loader)

        if data is not None:
            self._data = data
        if name is not None:
            self._name = name

        self._validate_data()
        if global_vars is not None:
            self.apply_global_vars(global_vars)
        self.prepare_objects()

    @property
    def name(self):
        if self._name is None:
            raise ValueError(f'Missing config name')
        return self._name

    @property
    def fullname(self):
        if self.namespace is None:
            return f'{self.name}'
        return f'{self.namespace}::{self.name}'

    def __str__(self):
        return self.fullname

    def __repr__(self):
        return f'<config: {self}>'

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

    def __contains__(self, item):
        return item in self.data

    def _validate_data(self):
        if self._data is None:
            return

        data = self._data
        uses = data.get('uses', [])
        if not isinstance(uses, Iterable) or isinstance(uses, str):
            raise ValueError(f'`uses` of config `{self}` have to be list or str')

        tasks = data.get('tasks', [])
        if not isinstance(tasks, Iterable) or isinstance(tasks, str):
            raise ValueError(f'`tasks` of config `{self}` have to list or str')

    def apply_global_vars(self, context):
        search_and_replace_placeholders(self._data, context)

    def prepare_objects(self):
        if self._data is None:
            return
        for key, value in self._data.items():
            if isinstance(value, dict) and 'class' in value:
                obj = find_and_instancelize_clazz(value)
                self._data[key] = obj

    def chain(self, **kwargs):
        from taskchain.task import Chain
        return Chain(self, **kwargs)
