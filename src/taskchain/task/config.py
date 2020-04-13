import json
from pathlib import Path
from typing import Union, Dict, Iterable, Any

import yaml

from taskchain.utils.clazz import instancelize_clazz
from taskchain.utils.data import search_and_replace_placeholders


class Config(dict):

    def __init__(self,
                 base_dir: Union[Path, str, None],
                 filepath: Union[Path, str] = None,
                 name: str = None,
                 data: Dict = None,
                 context: Union[Any, None] = None,
                 ):
        super().__init__()

        self.base_dir = base_dir
        self.name = None
        self._data = None
        self.context = context
        self.objects = {}

        if filepath is not None:
            filepath = Path(filepath)
            name_parts = filepath.name.split('.')
            extension = name_parts[-1]
            self.name = '.'.join(name_parts[:-1])
            if extension == 'json':
                self._data = json.load(filepath.open())
            elif extension == 'yaml':
                self._data = yaml.load(filepath.open(), Loader=yaml.Loader)

        if data is not None:
            self._data = data
        if name is not None:
            self.name = name
        if self.name is None:
            raise ValueError(f'Missing config name')

        self._validate_data()
        if context is not None:
            self.apply_context(context)
        self.prepare_objects()

    def __str__(self):
        return f'{self.name}'

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

    def apply_context(self, context):
        search_and_replace_placeholders(self._data, context)

    def prepare_objects(self):
        if self._data is None:
            return
        for key, value in self._data.items():
            if isinstance(value, dict) and 'class' in value:
                obj = instancelize_clazz(
                    value['class'],
                    value.get('args', []),
                    value.get('kwargs', {})
                )
                self._data[key] = obj
                self.objects[key] = obj

    def chain(self):
        from taskchain.task import Chain
        return Chain(self)
